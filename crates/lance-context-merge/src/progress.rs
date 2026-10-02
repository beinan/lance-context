//! Execution progress is separate from commit ownership. A heartbeat with an
//! unchanged sequence must never extend a no-progress deadline.
use crate::{encode, execution_key, Coordinator, Execution, Result};
use etcd_client::{Compare, CompareOp, TxnOp};

#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct ExecutionProgress {
    pub sequence: u64,
}

impl Coordinator {
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

    pub(crate) fn remove_progress(&self, execution: &Execution) -> TxnOp {
        TxnOp::delete(self.progress_key(execution), None)
    }
}
