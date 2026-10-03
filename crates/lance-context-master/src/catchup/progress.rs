//! Observe completed work across master failover. Heartbeats never reset idle.
use super::{store::Record, CatchupConfig, Result};
use crate::state::MasterState;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Sample {
    pub execution: Option<String>,
    pub phase: String,
    pub sequence: Option<u64>,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Observation {
    pub sample: Sample,
    pub last_sample_ms: i64,
    pub last_progress_ms: i64,
    pub suspect_since_ms: Option<i64>,
    pub stalled: bool,
}

pub(super) async fn sample(state: &MasterState, record: &Record) -> Result<Sample> {
    let coordinator = state.task_store.merge_coordinator();
    let Some(execution) = coordinator.get(&record.target).await? else {
        return Ok(Sample {
            execution: None,
            phase: "starting_or_finished".into(),
            sequence: None,
        });
    };
    if execution.instance != record.job
        || execution.maintenance != Some(lance_context_merge::MaintenanceKind::Catchup)
    {
        return Err("catch-up progress belongs to another executor; refusing termination".into());
    }
    let progress = coordinator.progress(&execution).await?;
    Ok(Sample {
        execution: Some(execution.id),
        phase: format!("{:?}", execution.phase),
        sequence: progress.map(|p| p.sequence),
    })
}

/// Atomically revoke the exact running execution before asking Kubernetes to
/// terminate it. Slow master replicas cannot act on an obsolete progress sample.
pub(super) async fn revoke(
    state: &MasterState,
    record: &Record,
    observed: &Sample,
) -> Result<bool> {
    let coordinator = state.task_store.merge_coordinator();
    let current = coordinator.get(&record.target).await?;
    match current {
        Some(execution)
            if observed.execution.as_deref() == Some(&execution.id)
                && execution.instance == record.job =>
        {
            if execution.phase == lance_context_merge::Phase::Running {
                coordinator
                    .revoke_stalled(&execution, observed.sequence)
                    .await
            } else if execution.phase == lance_context_merge::Phase::Reserved {
                coordinator.cancel_reserved(&execution).await
            } else {
                // Already cancelled/recovering/terminal work has no ongoing
                // admission to revoke. Recheck phase and progress once more.
                Ok(sample(state, record).await? == *observed)
            }
        }
        None => Ok(observed.execution.is_none()),
        _ => Ok(false),
    }
}

pub(super) fn observe(
    previous: Option<&Observation>,
    sample: Sample,
    now: i64,
    config: &CatchupConfig,
    idle_secs: u64,
) -> Observation {
    // A sampling outage or clock step cannot prove continuous lack of progress.
    let max_gap = config
        .interval_secs
        .saturating_mul(3)
        .max(60)
        .saturating_mul(1000) as i64;
    let previous =
        previous.filter(|p| now >= p.last_sample_ms && now - p.last_sample_ms <= max_gap);
    let advanced = previous.is_none_or(|p| {
        p.sample.execution != sample.execution
            || p.sample.phase != sample.phase
            || sample
                .sequence
                .is_some_and(|current| p.sample.sequence.is_none_or(|old| current > old))
    });
    let last_progress_ms = if advanced {
        now
    } else {
        previous.unwrap().last_progress_ms
    };
    let limit = if sample.execution.is_none() {
        config.startup_timeout_secs
    } else {
        idle_secs
    };
    let idle = now.saturating_sub(last_progress_ms) >= limit.saturating_mul(1000) as i64;
    let suspect_since_ms = if idle {
        Some(previous.and_then(|p| p.suspect_since_ms).unwrap_or(now))
    } else {
        None
    };
    // Give a cooperative executor time to cancel and drain; confirm in a later
    // observation before asking Kubernetes to terminate an unresponsive process.
    let stalled = suspect_since_ms.is_some_and(|since| now.saturating_sub(since) >= 60_000);
    Observation {
        sample,
        last_sample_ms: now,
        last_progress_ms,
        suspect_since_ms,
        stalled,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn running(sequence: u64) -> Sample {
        Sample {
            execution: Some("execution-1".into()),
            phase: "Running".into(),
            sequence: Some(sequence),
        }
    }
    #[test]
    fn progress_outlives_slice_but_heartbeats_eventually_stall() {
        let config = CatchupConfig::default();
        let mut observed = observe(None, running(0), 0, &config, 120);
        for n in 1..=200 {
            observed = observe(Some(&observed), running(n), n as i64 * 30_000, &config, 120);
            assert!(!observed.stalled);
        }
        // Far beyond the 1800-second slice; unchanged publications do not help.
        for n in 1..=6 {
            observed = observe(
                Some(&observed),
                running(200),
                6_000_000 + n * 30_000,
                &config,
                120,
            );
            assert_eq!(observed.stalled, n == 6);
        }
        observed = observe(Some(&observed), running(201), 6_210_000, &config, 120);
        assert!(!observed.stalled);
    }
    #[test]
    fn missing_observations_and_new_executors_receive_a_fresh_grace() {
        let config = CatchupConfig::default();
        let initial = observe(None, running(0), 0, &config, 120);
        let late = observe(Some(&initial), running(0), 1_000_000, &config, 120);
        assert!(!late.stalled);
        assert_eq!(late.last_progress_ms, 1_000_000);
        let mut replacement = running(0);
        replacement.execution = Some("execution-2".into());
        let fresh = observe(Some(&late), replacement, 1_030_000, &config, 120);
        assert_eq!(fresh.last_progress_ms, 1_030_000);
        assert!(!fresh.stalled);
        let backwards = observe(Some(&fresh), running(0), 1, &config, 120);
        assert_eq!(backwards.last_progress_ms, 1);
    }
}
