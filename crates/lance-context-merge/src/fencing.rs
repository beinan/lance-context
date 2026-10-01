//! Close commit admission before fencing the finite set of already admitted
//! manifest versions. Lease expiry and process identity are not storage fences.
use crate::{encode, execution_key, ClaimProof, Coordinator, Execution, Phase, Result};
use etcd_client::{Compare, CompareOp, TxnOp};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct CommitWatermarks {
    #[serde(default)]
    pub dataset_uri: Option<String>,
    pub versions: BTreeMap<String, u64>,
}

impl Coordinator {
    fn permits_key(&self, execution: &Execution) -> String {
        format!(
            "{}/merge-commit-permits/{}",
            self.prefix.trim_end_matches('/'),
            execution.id
        )
    }

    /// Every manifest write calls this *after selecting its immutable version*
    /// and before issuing storage I/O. A concurrent freeze either observes this
    /// permit or prevents it; there is no check-then-write ownership gap.
    pub async fn authorize_commit(
        &self,
        running: &Execution,
        dataset_uri: &str,
        resource: &str,
        version: u64,
    ) -> Result<()> {
        if running.protocol != 2 || running.phase != Phase::Running {
            return Err("merge commit requires fencing protocol 2".into());
        }
        if resource != "base"
            && resource
                .strip_prefix("shard:")
                .is_none_or(|s| uuid::Uuid::parse_str(s).is_err())
        {
            return Err("invalid merge manifest resource".into());
        }
        let key = self.permits_key(running);
        for _ in 0..16 {
            let response = self
                .client
                .clone()
                .get(key.clone(), None)
                .await
                .map_err(|e| e.to_string())?;
            let previous = response.kvs().first();
            let mut watermarks: CommitWatermarks = previous
                .map(|kv| serde_json::from_slice(kv.value()).map_err(|e| e.to_string()))
                .transpose()?
                .unwrap_or_default();
            if watermarks
                .dataset_uri
                .as_deref()
                .is_some_and(|uri| uri != dataset_uri)
            {
                return Err("merge execution cannot write multiple datasets".into());
            }
            watermarks.dataset_uri = Some(dataset_uri.into());
            let watermark = watermarks.versions.entry(resource.into()).or_default();
            *watermark = (*watermark).max(version);
            if self
                .transact(
                    vec![
                        Compare::value(
                            execution_key(&self.prefix, &running.target),
                            CompareOp::Equal,
                            encode(running),
                        ),
                        Compare::mod_revision(
                            key.clone(),
                            CompareOp::Equal,
                            previous.map_or(0, |kv| kv.mod_revision()),
                        ),
                    ],
                    vec![TxnOp::put(
                        key.clone(),
                        serde_json::to_vec(&watermarks).unwrap(),
                        None,
                    )],
                )
                .await?
            {
                return Ok(());
            }
            if self.get(&running.target).await?.as_ref() != Some(running) {
                return Err("merge commit ownership revoked for recovery".into());
            }
        }
        Err("merge commit permission contention exceeded limit".into())
    }

    pub async fn freeze(&self, proof: &ClaimProof, old: &Execution) -> Result<Option<Execution>> {
        if old.protocol != 2
            || !matches!(
                old.phase,
                Phase::Running | Phase::Uncertain | Phase::Recovering
            )
        {
            return Err("execution lacks recoverable storage fencing protocol".into());
        }
        let mut frozen = old.clone();
        frozen.phase = Phase::Recovering;
        Ok(self
            .transact(
                vec![
                    Compare::value(proof.key.as_str(), CompareOp::Equal, proof.token.as_bytes()),
                    Compare::value(
                        execution_key(&self.prefix, &old.target),
                        CompareOp::Equal,
                        encode(old),
                    ),
                ],
                vec![TxnOp::put(
                    execution_key(&self.prefix, &old.target),
                    encode(&frozen),
                    None,
                )],
            )
            .await?
            .then_some(frozen))
    }

    pub async fn watermarks(&self, frozen: &Execution) -> Result<CommitWatermarks> {
        if frozen.phase != Phase::Recovering {
            return Err("commit admission must be frozen first".into());
        }
        let response = self
            .client
            .clone()
            .get(self.permits_key(frozen), None)
            .await
            .map_err(|e| e.to_string())?;
        response
            .kvs()
            .first()
            .map(|kv| serde_json::from_slice(kv.value()).map_err(|e| e.to_string()))
            .transpose()
            .map(|v| v.unwrap_or_default())
    }

    /// Only call after storage confirms that every permitted version is fenced.
    pub async fn finish_recovery(&self, proof: &ClaimProof, frozen: &Execution) -> Result<bool> {
        if frozen.phase != Phase::Recovering {
            return Err("execution is not recovering".into());
        }
        let cause = frozen
            .error
            .as_deref()
            .unwrap_or("merge execution deadline or lost worker");
        let cause = cause
            .strip_prefix("merge ownership unresolved: manifest commit result unknown; ")
            .or_else(|| cause.strip_prefix("merge ownership unresolved: "))
            .unwrap_or(cause);
        let mut recovered = frozen.finished(Err(format!(
            "merge storage fenced for recovery; original failure: {cause}"
        )));
        recovered.phase = Phase::Recovered;
        self.transact(
            vec![
                Compare::value(proof.key.as_str(), CompareOp::Equal, proof.token.as_bytes()),
                Compare::value(
                    execution_key(&self.prefix, &frozen.target),
                    CompareOp::Equal,
                    encode(frozen),
                ),
            ],
            vec![TxnOp::put(
                execution_key(&self.prefix, &frozen.target),
                encode(&recovered),
                None,
            )],
        )
        .await
    }

    pub(crate) fn remove_permits(&self, execution: &Execution) -> TxnOp {
        TxnOp::delete(self.permits_key(execution), None)
    }
}
