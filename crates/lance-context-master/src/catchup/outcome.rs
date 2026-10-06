//! Terminal evidence for controller-owned Jobs. A code 75 alone is not proof
//! that no claim was acquired: require the exact Pod's structured receipt.
use super::{store::Record, CatchupConfig, Result};
use serde::{Deserialize, Serialize};
use serde_json::Value;

pub(super) const RECEIPT_PATH: &str = "/dev/termination-log";

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum JobOutcome {
    Succeeded,
    Deferred,
    Failed,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct DeferredReceipt {
    version: u32,
    outcome: String,
    target: String,
    job: String,
    attempt: String,
    pod_uid: String,
}

/// Called only after every claim RPC returned a definitive None, before any
/// recovery or payload work. Native supervisors without this opt-in keep their
/// existing stdout contract. Failure to write the receipt remains a failure.
pub(super) fn report_deferred(config: &CatchupConfig, target: &str) -> Result<()> {
    let Some(path) = &config.outcome_path else {
        return Ok(());
    };
    let receipt = DeferredReceipt {
        version: 1,
        outcome: "deferred_before_claim".into(),
        target: target.into(),
        job: config.job_name.clone().ok_or("missing receipt Job")?,
        attempt: config.attempt_id.clone().ok_or("missing receipt attempt")?,
        pod_uid: config.pod_uid.clone().ok_or("missing receipt Pod UID")?,
    };
    let bytes = serde_json::to_vec(&receipt).map_err(|e| e.to_string())?;
    // Kubernetes retains at most 4096 bytes per termination message. Leave
    // headroom and fail closed instead of depending on a truncated receipt.
    if bytes.len() > 2048 || receipt.attempt.is_empty() || receipt.pod_uid.is_empty() {
        return Err("invalid catch-up outcome identity or receipt length".into());
    }
    std::fs::write(path, bytes).map_err(|e| format!("write catch-up outcome: {e}"))
}

/// The caller already verified terminal Job state and all listed Pod phases.
/// Missing/ambiguous code-75 evidence retains the reservation for inspection;
/// ordinary failures keep their existing failure policy.
pub(super) fn terminal_outcome(
    record: &Record,
    job: &Value,
    pods: &[Value],
    success: bool,
) -> Result<JobOutcome> {
    let has_busy_exit = pods.iter().any(|pod| {
        pod["status"]["containerStatuses"]
            .as_array()
            .is_some_and(|statuses| {
                statuses
                    .iter()
                    .any(|s| s["state"]["terminated"]["exitCode"] == 75)
            })
    });
    if !has_busy_exit {
        return Ok(if success {
            JobOutcome::Succeeded
        } else {
            JobOutcome::Failed
        });
    }
    let invalid = || "unresolved catch-up deferred receipt; reservation retained".to_string();
    if success || pods.len() != 1 || record.termination_requested {
        return Err(invalid());
    }
    let pod = &pods[0];
    let job_uid = job["metadata"]["uid"].as_str().ok_or_else(invalid)?;
    if !pod["metadata"]["ownerReferences"]
        .as_array()
        .is_some_and(|owners| {
            owners.iter().any(|o| {
                o["uid"] == job_uid
                    && o["name"] == record.job
                    && o["kind"] == "Job"
                    && o["controller"] == true
            })
        })
    {
        return Err(invalid());
    }
    let statuses = pod["status"]["containerStatuses"]
        .as_array()
        .ok_or_else(invalid)?;
    if statuses.len() != 1 {
        return Err(invalid());
    }
    let container = &statuses[0];
    let terminated = &container["state"]["terminated"];
    if pod["status"]["phase"] != "Failed"
        || container["restartCount"] != 0
        || container["name"] != job["spec"]["template"]["spec"]["containers"][0]["name"]
        || terminated["exitCode"] != 75
        || terminated["signal"]
            .as_u64()
            .is_some_and(|signal| signal != 0)
    {
        return Err(invalid());
    }
    let message = terminated["message"].as_str().ok_or_else(invalid)?;
    if message.len() > 2048 {
        return Err(invalid());
    }
    let receipt: DeferredReceipt = serde_json::from_str(message).map_err(|_| invalid())?;
    if receipt.version != 1
        || receipt.outcome != "deferred_before_claim"
        || receipt.target != record.target
        || receipt.job != record.job
        || receipt.attempt != record.attempt.to_string()
        || receipt.pod_uid.is_empty()
        || pod["metadata"]["uid"] != receipt.pod_uid
    {
        return Err(invalid());
    }
    Ok(JobOutcome::Deferred)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn fixture() -> (Record, Value, Value) {
        let record: Record = serde_json::from_value(json!({
            "target":"hot", "job":"lc-job", "job_uid":"job-uid", "slot":0,
            "attempt":7, "active":true, "reason":"test", "pending_at_admission":1000,
            "created_at_ms":0, "finished_at_ms":null, "consecutive_failures":2,
            "needs_attention":true, "next_retry_ms":0, "outcome":null
        }))
        .unwrap();
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("termination");
        report_deferred(
            &CatchupConfig {
                outcome_path: Some(path.to_string_lossy().into()),
                job_name: Some(record.job.clone()),
                attempt_id: Some(record.attempt.to_string()),
                pod_uid: Some("pod-uid".into()),
                ..Default::default()
            },
            &record.target,
        )
        .unwrap();
        let job = json!({"metadata":{"uid":"job-uid"},
            "spec":{"template":{"spec":{"containers":[{"name":"executor"}]}}}});
        let pod = json!({"metadata":{"uid":"pod-uid", "ownerReferences":[{
                "uid":"job-uid", "name":"lc-job", "kind":"Job", "controller":true}]},
            "status":{"phase":"Failed", "containerStatuses":[{"name":"executor",
                "restartCount":0,"state":{"terminated":{"exitCode":75,"signal":0,
                "message":std::fs::read_to_string(path).unwrap()}}}]}});
        (record, job, pod)
    }

    #[test]
    fn exact_receipt_is_deferred_only_after_terminal_failure() {
        let (record, job, pod) = fixture();
        assert_eq!(
            terminal_outcome(&record, &job, std::slice::from_ref(&pod), false).unwrap(),
            JobOutcome::Deferred
        );
        assert!(terminal_outcome(&record, &job, std::slice::from_ref(&pod), true).is_err());
        assert!(terminal_outcome(&record, &job, &[pod.clone(), pod], false).is_err());
    }

    #[test]
    fn copied_truncated_and_unbound_receipts_never_enable_short_retry() {
        let (record, job, pod) = fixture();
        for (pointer, value) in [
            ("/metadata/uid", json!("replacement-pod")),
            ("/metadata/ownerReferences/0/uid", json!("other-job")),
            ("/status/containerStatuses/0/name", json!("other-container")),
            ("/status/containerStatuses/0/restartCount", json!(1)),
            (
                "/status/containerStatuses/0/state/terminated/signal",
                json!(9),
            ),
            (
                "/status/containerStatuses/0/state/terminated/message",
                json!("{}"),
            ),
            (
                "/status/containerStatuses/0/state/terminated/message",
                json!("busy before claim"),
            ),
        ] {
            let mut changed = pod.clone();
            *changed.pointer_mut(pointer).unwrap() = value;
            assert!(
                terminal_outcome(&record, &job, &[changed], false).is_err(),
                "{pointer}"
            );
        }
        for (key, value) in [
            ("version", json!(2)),
            ("target", json!("other")),
            ("job", json!("other")),
            ("attempt", json!("6")),
            ("pod_uid", json!("other")),
            ("outcome", json!("failed_after_claim")),
        ] {
            let mut changed = pod.clone();
            let message =
                &mut changed["status"]["containerStatuses"][0]["state"]["terminated"]["message"];
            let mut receipt: Value = serde_json::from_str(message.as_str().unwrap()).unwrap();
            receipt[key] = value;
            *message = json!(receipt.to_string());
            assert!(
                terminal_outcome(&record, &job, &[changed], false).is_err(),
                "{key}"
            );
        }
        let mut stopped = record;
        stopped.termination_requested = true;
        assert!(terminal_outcome(&stopped, &job, &[pod], false).is_err());
    }

    #[test]
    fn ordinary_terminal_results_and_legacy_executors_remain_supported() {
        let (record, job, mut pod) = fixture();
        pod["status"]["containerStatuses"][0]["state"]["terminated"]["exitCode"] = json!(1);
        assert_eq!(
            terminal_outcome(&record, &job, &[pod], false).unwrap(),
            JobOutcome::Failed
        );
        assert_eq!(
            terminal_outcome(&record, &job, &[], true).unwrap(),
            JobOutcome::Succeeded
        );
        assert!(report_deferred(&CatchupConfig::default(), "hot").is_ok());
        assert!(report_deferred(
            &CatchupConfig {
                outcome_path: Some("/nonexistent/catchup/receipt".into()),
                ..Default::default()
            },
            "hot"
        )
        .is_err());
    }
}
