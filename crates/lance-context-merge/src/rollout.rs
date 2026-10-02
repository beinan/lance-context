//! Explicit per-table rollout. Capability deployment alone changes no merge path.
use crate::{execution_key, Coordinator, Result};
use etcd_client::{Compare, CompareOp, GetOptions, TxnOp};

#[derive(Clone, Debug, Default, clap::Args)]
pub struct MergeRollout {
    /// Exact scheduler targets using owned merges (generic stores: generic:name).
    /// Empty by default. Enable only after the target's legacy writers drain.
    #[arg(
        long = "merge-owned-targets",
        env = "MERGE_OWNED_TARGETS",
        value_delimiter = ','
    )]
    pub owned_targets: Vec<String>,
    /// Drain maintenance for these targets during migration; ingestion and
    /// maintenance of other tables continue. Coordinate all replicas and helpers.
    #[arg(
        long = "merge-drain-targets",
        env = "MERGE_DRAIN_TARGETS",
        value_delimiter = ','
    )]
    pub drain_targets: Vec<String>,
}

impl MergeRollout {
    pub fn owned(&self, target: &str) -> bool {
        self.owned_targets.iter().any(|t| t == target)
    }

    pub fn draining(&self, target: &str) -> bool {
        self.drain_targets.iter().any(|t| t == target)
    }

    pub fn validate(&self) -> Result<()> {
        if self.owned_targets.iter().any(|t| self.draining(t)) {
            return Err("a merge target cannot be both owned and draining".into());
        }
        if self
            .owned_targets
            .iter()
            .chain(&self.drain_targets)
            .any(|t| t.is_empty() || t == "*" || t.starts_with("datagen:"))
        {
            return Err("merge rollout requires explicit rollout/generic targets; wildcards and datagen targets are unsupported".into());
        }
        Ok(())
    }
}

impl Coordinator {
    fn request_key(&self, target: &str) -> String {
        execution_key(&self.prefix, target).replace("/merge-executions/", "/merge-requests/")
    }

    /// Coalesce worker count/timer triggers without resetting failure budgets.
    pub async fn request_merge(&self, target: &str) -> Result<()> {
        let key = self.request_key(target);
        self.transact(
            vec![Compare::version(key.as_str(), CompareOp::Equal, 0)],
            vec![TxnOp::put(key, target, None)],
        )
        .await?;
        Ok(())
    }

    pub async fn request_page(&self, after: Option<&str>) -> Result<(Vec<String>, Option<String>)> {
        let prefix = format!("{}/merge-requests/", self.prefix.trim_end_matches('/'));
        let (start, options) = match after {
            None => (prefix.clone(), GetOptions::new().with_prefix()),
            Some(key) if key.starts_with(&prefix) => {
                let mut end = prefix.as_bytes().to_vec();
                *end.last_mut().unwrap() += 1;
                (format!("{key}\0"), GetOptions::new().with_range(end))
            }
            Some(_) => return Err("invalid merge request cursor".into()),
        };
        let response = self
            .client
            .clone()
            .get(start, Some(options.with_limit(256)))
            .await
            .map_err(|e| e.to_string())?;
        let rows = response
            .kvs()
            .iter()
            .map(|kv| String::from_utf8_lossy(kv.value()).into_owned())
            .collect();
        let next = response
            .more()
            .then(|| {
                response
                    .kvs()
                    .last()
                    .map(|kv| String::from_utf8_lossy(kv.key()).into_owned())
            })
            .flatten();
        Ok((rows, next))
    }

    /// Called only after a durable enqueue. A subsequent flush tick reasserts
    /// demand if the active task already passed this worker's shard.
    pub async fn acknowledge_request(&self, target: &str) -> Result<()> {
        self.client
            .clone()
            .delete(self.request_key(target), None)
            .await
            .map_err(|e| e.to_string())?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn rollout_is_explicit_and_disjoint() {
        let mut config = MergeRollout::default();
        assert!(!config.owned("hot"));
        config.owned_targets.push("hot".into());
        assert!(config.owned("hot"));
        assert!(!config.owned("hot2"));
        config.drain_targets.push("hot".into());
        assert!(config.validate().is_err());
    }
}
