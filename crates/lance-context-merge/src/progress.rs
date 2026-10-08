//! Execution progress is separate from commit ownership. A heartbeat with an
//! unchanged sequence must never extend a no-progress deadline.
use crate::{encode, execution_key, Coordinator, Execution, Phase, Result};
use etcd_client::{Compare, CompareOp, Txn, TxnOp, TxnOpResponse};

#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct ExecutionProgress {
    pub sequence: u64,
}

/// Execution, progress sequence and work report read together at one etcd
/// revision. `work_report` is only present when it was published for the
/// same sequence as `progress`; a report from an older sequence is dropped
/// rather than shown beside a newer heartbeat.
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct ProgressSnapshot {
    pub execution: Execution,
    pub progress: Option<ExecutionProgress>,
    pub work_report: Option<serde_json::Value>,
    /// etcd revision the snapshot was taken at.
    pub revision: i64,
}

impl Coordinator {
    fn work_key(&self, execution: &Execution) -> String {
        format!(
            "{}/merge-work/{}",
            self.prefix.trim_end_matches('/'),
            execution.id
        )
    }

    /// Publish work details atomically with the existing progress sequence.
    /// Keep the old sequence wire format intact: mixed-version executors use
    /// its exact bytes in the no-progress revocation compare-and-swap.
    pub async fn publish_work_progress(
        &self,
        execution: &Execution,
        sequence: u64,
        report: serde_json::Value,
    ) -> Result<bool> {
        self.transact(
            vec![Compare::value(
                execution_key(&self.prefix, &execution.target),
                CompareOp::Equal,
                encode(execution),
            )],
            vec![
                TxnOp::put(
                    self.progress_key(execution),
                    serde_json::to_vec(&ExecutionProgress { sequence }).unwrap(),
                    None,
                ),
                TxnOp::put(
                    self.work_key(execution),
                    serde_json::to_vec(&report).map_err(|e| e.to_string())?,
                    None,
                ),
            ],
        )
        .await
    }

    pub async fn work_progress(&self, execution: &Execution) -> Result<Option<serde_json::Value>> {
        let response = self
            .client
            .clone()
            .get(self.work_key(execution), None)
            .await
            .map_err(|e| e.to_string())?;
        response
            .kvs()
            .first()
            .map(|kv| serde_json::from_slice(kv.value()).map_err(|e| e.to_string()))
            .transpose()
    }

    /// Read execution, progress and work report for `target` atomically.
    ///
    /// Two round trips: the first learns the execution id (the progress and
    /// work keys are per execution, not per target), the second reads all
    /// three keys in one transaction so they come from a single revision.
    /// If the execution changed between the two reads the transaction's
    /// compare fails and we loop; a target that flips executions faster than
    /// two etcd reads is not something a status endpoint needs to render.
    pub async fn progress_snapshot(&self, target: &str) -> Result<Option<ProgressSnapshot>> {
        let execution_key = execution_key(&self.prefix, target);
        for _ in 0..3 {
            let Some(execution) = self.get(target).await? else {
                return Ok(None);
            };
            let encoded = encode(&execution);
            let response = self
                .client
                .clone()
                .txn(
                    Txn::new()
                        .when(vec![Compare::value(
                            execution_key.clone(),
                            CompareOp::Equal,
                            encoded,
                        )])
                        .and_then(vec![
                            TxnOp::get(self.progress_key(&execution), None),
                            TxnOp::get(self.work_key(&execution), None),
                        ]),
                )
                .await
                .map_err(|e| e.to_string())?;
            if !response.succeeded() {
                continue;
            }
            let revision = response.header().map_or(0, |h| h.revision());
            let mut values = response.op_responses().into_iter().map(|op| match op {
                TxnOpResponse::Get(get) => get.kvs().first().map(|kv| kv.value().to_vec()),
                _ => None,
            });
            let progress: Option<ExecutionProgress> = values
                .next()
                .flatten()
                .map(|v| serde_json::from_slice(&v).map_err(|e| e.to_string()))
                .transpose()?;
            let work_report: Option<serde_json::Value> = values
                .next()
                .flatten()
                .map(|v| serde_json::from_slice(&v).map_err(|e| e.to_string()))
                .transpose()?;
            let work_report = work_report.filter(|report| {
                report["sequence"].as_u64() == progress.as_ref().map(|p| p.sequence)
            });
            return Ok(Some(ProgressSnapshot {
                execution,
                progress,
                work_report,
                revision,
            }));
        }
        Err(format!(
            "merge execution for {target} changed on every read; retry later"
        ))
    }

    pub(crate) fn remove_work_progress(&self, execution: &Execution) -> TxnOp {
        TxnOp::delete(self.work_key(execution), None)
    }

    fn progress_key(&self, execution: &Execution) -> String {
        format!(
            "{}/merge-progress/{}",
            self.prefix.trim_end_matches('/'),
            execution.id
        )
    }

    pub async fn publish_progress(&self, execution: &Execution, sequence: u64) -> Result<bool> {
        self.transact(
            vec![Compare::value(
                execution_key(&self.prefix, &execution.target),
                CompareOp::Equal,
                encode(execution),
            )],
            vec![TxnOp::put(
                self.progress_key(execution),
                serde_json::to_vec(&ExecutionProgress { sequence }).unwrap(),
                None,
            )],
        )
        .await
    }

    pub async fn progress(&self, execution: &Execution) -> Result<Option<ExecutionProgress>> {
        let response = self
            .client
            .clone()
            .get(self.progress_key(execution), None)
            .await
            .map_err(|e| e.to_string())?;
        response
            .kvs()
            .first()
            .map(|kv| serde_json::from_slice(kv.value()).map_err(|e| e.to_string()))
            .transpose()
    }

    /// Close commit admission only if both ownership and the observed completed
    /// work still match. A concurrent progress publication defeats this CAS.
    /// This is not a storage fence: recovery must still cover admitted writes.
    pub async fn revoke_stalled(
        &self,
        execution: &Execution,
        sequence: Option<u64>,
    ) -> Result<bool> {
        if execution.phase != Phase::Running {
            return Ok(false);
        }
        let mut next = execution.finished(Err(
            "confirmed no-progress execution; storage recovery required".into(),
        ));
        next.phase = Phase::Uncertain;
        let progress = match sequence {
            Some(sequence) => Compare::value(
                self.progress_key(execution),
                CompareOp::Equal,
                serde_json::to_vec(&ExecutionProgress { sequence }).unwrap(),
            ),
            None => Compare::version(self.progress_key(execution), CompareOp::Equal, 0),
        };
        self.transact(
            vec![
                Compare::value(
                    execution_key(&self.prefix, &execution.target),
                    CompareOp::Equal,
                    encode(execution),
                ),
                progress,
            ],
            vec![TxnOp::put(
                execution_key(&self.prefix, &execution.target),
                encode(&next),
                None,
            )],
        )
        .await
    }

    pub(crate) fn remove_progress(&self, execution: &Execution) -> TxnOp {
        TxnOp::delete(self.progress_key(execution), None)
    }
}
