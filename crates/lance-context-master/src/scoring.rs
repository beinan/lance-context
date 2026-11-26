//! Scoring and class assignment for merge demand (P1.7, shadow).
//!
//! Design: `docs/design/unified-maintenance-scheduler.md` §4.2. Pure
//! functions over `TableDemand` plus a clock and a policy. P1 scores only
//! `MergeWal`; compaction and index demand are folded into `TableDemand`
//! but not yet scored, so the shadow planner can be compared against the
//! merge loops it would eventually replace before anything else moves.
//!
//! Ordering rule (review round 2): class 1 is served **oldest first** by
//! wait time, not by score; score orders classes 2–5 only. Promotion to
//! class 1 is therefore a guarantee of service, not a nudge.

use std::cmp::Ordering;

use lance_context_merge::demand::TableDemand;
use serde::Serialize;

/// Per-table merge policy. Defaults mirror today's sweep thresholds.
#[derive(Debug, Clone, Serialize)]
pub struct MergePolicy {
    /// `merge_score >= 1` at this many pending generations.
    pub min_generations: u64,
    /// `merge_score >= 1` at this many pending bytes (0 = ignore bytes).
    pub min_bytes: u64,
    /// Aging horizon: a backlog this old doubles its score.
    pub max_age_secs: u64,
    /// At or above this, class 1 regardless of score.
    pub critical_generations: u64,
    /// A tail that has waited this long is promoted to class 1.
    pub max_turn_wait_secs: u64,
}

impl Default for MergePolicy {
    fn default() -> Self {
        Self {
            min_generations: 8,
            min_bytes: 0,
            max_age_secs: 3600,
            critical_generations: 256,
            max_turn_wait_secs: 3600,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Class {
    /// Reserved for recovery units; not produced from demand in P1.
    Recovery = 0,
    /// Pending ≥ critical, or promoted for age. Served oldest-first.
    Critical = 1,
    /// Reserved for commit-ready preparations; not produced in P1.
    CommitReady = 2,
    /// `merge_score >= 1`.
    Normal = 3,
    /// Reserved for compaction/index demand; not produced in P1.
    Maintenance = 4,
    /// `0 < merge_score < 1`: low-count tail.
    Tail = 5,
}

/// One scored unit of merge work.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct Scored {
    pub target: String,
    pub class: Class,
    /// Dimensionless; ≥ 1 means "due".
    pub score: f64,
    pub pending_generations: u64,
    pub pending_bytes: u64,
    /// Age of the oldest pending generation, if known.
    pub wait_ms: Option<i64>,
    /// Why this class.
    pub reason: &'static str,
}

/// Score one table's merge demand. `None` when nothing is pending.
pub fn score_merge(
    target: &str,
    demand: &TableDemand,
    policy: &MergePolicy,
    now_ms: i64,
) -> Option<Scored> {
    let pending = demand.pending_generations();
    let bytes = demand.pending_bytes();
    if pending == 0 {
        return None;
    }
    // Promotion uses a *floor* on the wait: an Upper bound on the flush time
    // means the generation flushed no later than that, so it has waited at
    // least this long - sound for "waited >= max_turn_wait". Score aging
    // uses the exact time only, so a bound cannot inflate a score.
    let wait_floor_ms = demand
        .oldest_pending_wait_floor_ms()
        .map(|t| (now_ms - t).max(0));
    let wait_exact_ms = demand
        .oldest_pending_exact_ms()
        .map(|t| (now_ms - t).max(0));
    let wait_ms = wait_floor_ms;
    let by_gens = pending as f64 / policy.min_generations.max(1) as f64;
    let by_bytes = if policy.min_bytes > 0 {
        bytes as f64 / policy.min_bytes as f64
    } else {
        0.0
    };
    let age_boost = match wait_exact_ms {
        Some(w) if policy.max_age_secs > 0 => {
            1.0 + (w as f64 / 1000.0) / policy.max_age_secs as f64
        }
        _ => 1.0,
    };
    let score = by_gens.max(by_bytes) * age_boost;
    let (class, reason) = if pending >= policy.critical_generations {
        (Class::Critical, "pending_at_or_above_critical")
    } else if wait_ms.is_some_and(|w| w >= (policy.max_turn_wait_secs as i64).saturating_mul(1000))
    {
        (Class::Critical, "promoted_for_age")
    } else if score >= 1.0 {
        (Class::Normal, "merge_score_due")
    } else {
        (Class::Tail, "below_threshold")
    };
    Some(Scored {
        target: target.to_string(),
        class,
        score,
        pending_generations: pending,
        pending_bytes: bytes,
        wait_ms,
        reason,
    })
}

/// Planner ordering: class ascending; within class 1, oldest wait first
/// (unknown wait sorts last); within other classes, score descending.
/// Target name breaks remaining ties so the order is total and stable.
pub fn planner_order(a: &Scored, b: &Scored) -> Ordering {
    a.class.cmp(&b.class).then_with(|| {
        if a.class == Class::Critical {
            match (a.wait_ms, b.wait_ms) {
                (Some(x), Some(y)) => y.cmp(&x),
                (Some(_), None) => Ordering::Less,
                (None, Some(_)) => Ordering::Greater,
                (None, None) => Ordering::Equal,
            }
        } else {
            b.score.partial_cmp(&a.score).unwrap_or(Ordering::Equal)
        }
        .then_with(|| a.target.cmp(&b.target))
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use lance_context_merge::demand::{DemandEvent, EventSource, SCHEMA_VERSION};

    fn demand(pending: u64, oldest_ms: i64) -> TableDemand {
        let mut t = TableDemand::default();
        // One shard sealed to `pending`, oldest generation flushed at oldest_ms.
        for g in 1..=pending {
            let _ = t.fold(
                &DemandEvent {
                    v: SCHEMA_VERSION,
                    shard: "s".into(),
                    sealed_through: g,
                    sealed_bytes_through: g * 1_000_000,
                    merged_through: None,
                    flushed_at_ms: oldest_ms + g as i64,
                    writer_epoch: 1,
                    source: EventSource::Writer,
                    merged_epoch: None,
                    sealed_times: Default::default(),
                },
                g as i64,
                0,
            );
        }
        t
    }

    #[test]
    fn classes_follow_thresholds() {
        let p = MergePolicy::default();
        let now = 10_000_000;
        assert!(score_merge("x", &TableDemand::default(), &p, now).is_none());
        let tail = score_merge("x", &demand(3, now - 1000), &p, now).unwrap();
        assert_eq!(tail.class, Class::Tail);
        assert!(tail.score < 1.0);
        let normal = score_merge("x", &demand(16, now - 1000), &p, now).unwrap();
        assert_eq!(normal.class, Class::Normal);
        assert!(normal.score >= 2.0);
        let critical = score_merge("x", &demand(300, now - 1000), &p, now).unwrap();
        assert_eq!(critical.class, Class::Critical);
        assert_eq!(critical.reason, "pending_at_or_above_critical");
    }

    #[test]
    fn age_lifts_score_and_promotes_an_old_tail() {
        let p = MergePolicy {
            max_age_secs: 100,
            max_turn_wait_secs: 200,
            ..Default::default()
        };
        let now = 1_000_000_000;
        let fresh = score_merge("x", &demand(2, now), &p, now).unwrap();
        let aged = score_merge("x", &demand(2, now - 100_000), &p, now).unwrap();
        assert!(aged.score > fresh.score);
        assert!(
            (aged.score / fresh.score - 2.0).abs() < 0.05,
            "doubled at max_age"
        );
        assert_eq!(aged.class, Class::Tail, "aged but below promotion");
        let promoted = score_merge("x", &demand(2, now - 250_000), &p, now).unwrap();
        assert_eq!(promoted.class, Class::Critical);
        assert_eq!(promoted.reason, "promoted_for_age");
    }

    /// Spec test 6 / review round 2: class 1 is oldest-first. A low-score
    /// table promoted for age is placed before a high-score critical merge
    /// that has waited less.
    #[test]
    fn class_one_is_oldest_first_not_score_first() {
        let p = MergePolicy {
            max_turn_wait_secs: 100,
            ..Default::default()
        };
        let now = 1_000_000_000;
        let hot = score_merge("hot", &demand(4000, now - 10_000), &p, now).unwrap();
        let tail = score_merge("tail", &demand(2, now - 500_000), &p, now).unwrap();
        assert_eq!(hot.class, Class::Critical);
        assert_eq!(tail.class, Class::Critical);
        assert!(hot.score > tail.score * 100.0);
        let mut v = [hot.clone(), tail.clone()];
        v.sort_by(planner_order);
        assert_eq!(
            v[0].target, "tail",
            "older wait wins class 1 despite tiny score"
        );
        // Outside class 1, score decides.
        let a = score_merge("a", &demand(32, now - 1000), &p, now).unwrap();
        let b = score_merge("b", &demand(16, now - 900_000_000), &p, now).unwrap();
        assert_eq!((a.class, b.class), (Class::Normal, Class::Critical));
        let mut v = [a.clone(), b.clone()];
        v.sort_by(planner_order);
        assert_eq!(v[0].target, "b", "class before score");
    }

    /// Round-5 finding 6 / round-6 finding 3: when the oldest pending time
    /// is only an upper bound, it is a sound floor on wait and may promote,
    /// but it must not inflate the score.
    #[test]
    fn an_age_bound_promotes_on_wait_but_does_not_boost_score() {
        let p = MergePolicy {
            max_turn_wait_secs: 10,
            max_age_secs: 10,
            ..Default::default()
        };
        let now = 1_000_000_000;
        let mut t = demand(128, now - 500_000);
        let _ = t.fold(
            &DemandEvent {
                v: SCHEMA_VERSION,
                shard: "s".into(),
                sealed_through: 128,
                sealed_bytes_through: 0,
                merged_through: Some(64),
                flushed_at_ms: 0,
                writer_epoch: 1,
                source: EventSource::Executor,
                merged_epoch: None,
                sealed_times: Default::default(),
            },
            999,
            0,
        );
        assert_eq!(t.pending_generations(), 64);
        assert!(t.oldest_pending_ms().is_some(), "a bound is shown");
        assert_eq!(t.oldest_pending_exact_ms(), None, "but is not exact");
        let s = score_merge("x", &t, &p, now).unwrap();
        // The bound is a floor on wait: newest sealed (gen 128) flushed at
        // now - 500_000 + 128, so wait >= ~500 s >= max_turn_wait (10 s) and
        // promotion is sound.
        assert!(s.wait_ms.is_some_and(|w| w >= 499_000), "{:?}", s.wait_ms);
        assert_eq!(s.class, Class::Critical);
        assert_eq!(s.reason, "promoted_for_age");
        // But the score itself carries no age boost from a bound.
        assert!(
            (s.score - 8.0).abs() < 1e-9,
            "no age boost on a bound: {}",
            s.score
        );
    }

    #[test]
    fn ordering_is_total_and_stable() {
        let p = MergePolicy::default();
        let now = 1_000_000_000;
        let mut items: Vec<Scored> = (0..20)
            .map(|i| {
                let pending = [2u64, 16, 300][i % 3];
                score_merge(
                    &format!("t{i:02}"),
                    &demand(pending, now - (i as i64) * 1000),
                    &p,
                    now,
                )
                .unwrap()
            })
            .collect();
        let mut again = items.clone();
        again.reverse();
        items.sort_by(planner_order);
        again.sort_by(planner_order);
        assert_eq!(items, again);
        let classes: Vec<_> = items.iter().map(|s| s.class).collect();
        let mut sorted = classes.clone();
        sorted.sort();
        assert_eq!(classes, sorted, "classes ascend");
    }
}
