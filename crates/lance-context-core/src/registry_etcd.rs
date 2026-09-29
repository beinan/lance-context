//! [`StoreRegistry`] backed by etcd.
//!
//! One key per store: `<prefix>/registry/<kind>/<name>` holding
//! `{"uri": ..., "created_at": ...}`. A point lookup is one round trip, an
//! upsert is one `put`, and nothing is retried against a shared commit point.
//! Compare `RolloutRegistry`, where every read is a manifest fetch from object
//! storage and every write is two Lance commits contended by every worker.
//!
//! Store names are validated to be portable path segments before they reach a
//! registry, so they are used as key segments unescaped.

use std::sync::Arc;

use etcd_client::{Client, Compare, CompareOp, DeleteOptions, GetOptions, Txn, TxnOp};
use lance::{Error as LanceError, Result as LanceResult};
use serde::{Deserialize, Serialize};

use crate::etcd::etcd_error;
use crate::registry::{RegistryEntry, StoreRegistry};

#[derive(Serialize, Deserialize)]
struct Value {
    uri: String,
    created_at: i64,
}

/// etcd-backed store directory for one store kind.
pub struct EtcdRegistry {
    client: Client,
    /// `<prefix>/registry/<kind>` with no trailing slash.
    prefix: String,
}

impl EtcdRegistry {
    /// `kind` is `rollout`, `generic` or `datagen`; `prefix` is the
    /// deployment-wide etcd namespace (see `EtcdConfig::prefix`).
    pub fn new(client: Client, prefix: &str, kind: &str) -> Arc<Self> {
        Arc::new(Self {
            client,
            prefix: format!("{}/registry/{kind}", prefix.trim_end_matches('/')),
        })
    }

    fn key(&self, name: &str) -> String {
        format!("{}/{name}", self.prefix)
    }

    fn entry(&self, key: &[u8], value: &[u8]) -> LanceResult<RegistryEntry> {
        let key = String::from_utf8_lossy(key);
        let name = key
            .strip_prefix(&format!("{}/", self.prefix))
            .unwrap_or(&key)
            .to_string();
        let value: Value = serde_json::from_slice(value)
            .map_err(|e| LanceError::io(format!("decode registry entry '{name}': {e}")))?;
        Ok(RegistryEntry {
            name,
            uri: value.uri,
            created_at: value.created_at,
        })
    }

    fn encode(uri: &str, created_at: i64) -> LanceResult<Vec<u8>> {
        serde_json::to_vec(&Value {
            uri: uri.to_string(),
            created_at,
        })
        .map_err(|e| LanceError::io(format!("encode registry entry: {e}")))
    }
}

#[async_trait::async_trait]
impl StoreRegistry for EtcdRegistry {
    async fn contains(&self, name: &str) -> LanceResult<bool> {
        let response = self
            .client
            .clone()
            .get(self.key(name), Some(GetOptions::new().with_count_only()))
            .await
            .map_err(etcd_error("registry get"))?;
        Ok(response.count() > 0)
    }

    async fn get(&self, name: &str) -> LanceResult<Option<RegistryEntry>> {
        let response = self
            .client
            .clone()
            .get(self.key(name), None)
            .await
            .map_err(etcd_error("registry get"))?;
        response
            .kvs()
            .first()
            .map(|kv| self.entry(kv.key(), kv.value()))
            .transpose()
    }

    async fn list(&self) -> LanceResult<Vec<RegistryEntry>> {
        let response = self
            .client
            .clone()
            .get(
                format!("{}/", self.prefix),
                Some(GetOptions::new().with_prefix()),
            )
            .await
            .map_err(etcd_error("registry list"))?;
        response
            .kvs()
            .iter()
            .map(|kv| self.entry(kv.key(), kv.value()))
            .collect()
    }

    async fn upsert(&self, name: &str, uri: &str) -> LanceResult<()> {
        // Keep the original created_at when the entry already exists: a
        // retried create must not look like a newer store.
        let created_at = match self.get(name).await? {
            Some(existing) => existing.created_at,
            None => chrono::Utc::now().timestamp_millis(),
        };
        self.client
            .clone()
            .put(self.key(name), Self::encode(uri, created_at)?, None)
            .await
            .map_err(etcd_error("registry put"))?;
        Ok(())
    }

    async fn remove(&self, name: &str) -> LanceResult<()> {
        self.client
            .clone()
            .delete(self.key(name), Some(DeleteOptions::new()))
            .await
            .map_err(etcd_error("registry delete"))?;
        Ok(())
    }

    async fn insert_missing(&self, entries: &[(String, String)]) -> LanceResult<usize> {
        // One put-if-absent transaction per entry, batched so a 10k-row
        // backfill is a few hundred round trips rather than 10k. A txn with
        // several compares is all-or-nothing, which is not what we want here,
        // so each entry is its own txn inside the batch loop.
        const BATCH: usize = 128;
        let now = chrono::Utc::now().timestamp_millis();
        let mut inserted = 0usize;
        let mut seen = std::collections::HashSet::new();
        for chunk in entries.chunks(BATCH) {
            let mut client = self.client.clone();
            for (name, uri) in chunk {
                if !seen.insert(name.clone()) {
                    continue;
                }
                let key = self.key(name);
                let txn = Txn::new()
                    .when([Compare::version(key.as_str(), CompareOp::Equal, 0)])
                    .and_then([TxnOp::put(key.as_str(), Self::encode(uri, now)?, None)]);
                if client
                    .txn(txn)
                    .await
                    .map_err(etcd_error("registry insert_missing"))?
                    .succeeded()
                {
                    inserted += 1;
                }
            }
        }
        Ok(inserted)
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    async fn registry() -> Option<Arc<EtcdRegistry>> {
        let endpoints = std::env::var("ETCD_TEST_ENDPOINTS").ok()?;
        let client = Client::connect(endpoints.split(',').collect::<Vec<_>>(), None)
            .await
            .expect("connect to test etcd");
        let prefix = format!("/lance-context-test/{}", uuid::Uuid::new_v4());
        Some(EtcdRegistry::new(client, &prefix, "rollout"))
    }

    #[tokio::test]
    #[ignore = "requires ETCD_TEST_ENDPOINTS"]
    async fn upsert_get_contains_remove() {
        let r = registry().await.unwrap();
        assert!(!r.contains("a").await.unwrap());
        assert!(r.get("a").await.unwrap().is_none());

        r.upsert("a", "/data/a.lance").await.unwrap();
        assert!(r.contains("a").await.unwrap());
        let first = r.get("a").await.unwrap().unwrap();
        assert_eq!(first.uri, "/data/a.lance");

        // A retried create keeps the original created_at.
        r.upsert("a", "/data/a2.lance").await.unwrap();
        let second = r.get("a").await.unwrap().unwrap();
        assert_eq!(second.uri, "/data/a2.lance");
        assert_eq!(second.created_at, first.created_at);

        r.remove("a").await.unwrap();
        assert!(!r.contains("a").await.unwrap());
        // Removing an absent name is a no-op.
        r.remove("a").await.unwrap();
    }

    #[tokio::test]
    #[ignore = "requires ETCD_TEST_ENDPOINTS"]
    async fn list_and_insert_missing() {
        let r = registry().await.unwrap();
        r.upsert("keep", "/old").await.unwrap();
        let n = r
            .insert_missing(&[
                ("keep".into(), "/new".into()),
                ("x".into(), "/x".into()),
                ("y".into(), "/y".into()),
                ("y".into(), "/dup".into()),
            ])
            .await
            .unwrap();
        assert_eq!(n, 2, "only x and y were missing");
        let mut names: Vec<String> = r
            .list()
            .await
            .unwrap()
            .into_iter()
            .map(|e| e.name)
            .collect();
        names.sort();
        assert_eq!(names, vec!["keep", "x", "y"]);
        assert_eq!(r.get("keep").await.unwrap().unwrap().uri, "/old");
    }

    /// Migration step 1: Lance primary, etcd mirror. Writes reach both, reads
    /// come from Lance, `diff` is empty after a backfill of pre-existing rows.
    #[tokio::test]
    #[ignore = "requires ETCD_TEST_ENDPOINTS"]
    async fn mirrored_registry_keeps_both_backends_in_step() {
        use crate::registry::{
            backfill_registry, diff_registries, LanceRegistry, MirroredRegistry,
        };
        let Some(etcd) = registry().await else { return };
        let dir = tempfile::TempDir::new().unwrap();
        let uri = dir.path().join("_registry.lance");
        let lance = crate::registry::RolloutRegistry::open_or_create(uri.to_str().unwrap(), None)
            .await
            .unwrap();
        let lance: Arc<dyn StoreRegistry> = Arc::new(LanceRegistry::new(lance));
        // Rows that existed before mirroring started.
        lance.upsert("pre-1", "/p1").await.unwrap();
        lance.upsert("pre-2", "/p2").await.unwrap();

        let etcd_dyn: Arc<dyn StoreRegistry> = etcd.clone();
        let (only_lance, only_etcd) = diff_registries(&*lance, &*etcd_dyn).await.unwrap();
        assert_eq!(only_lance, vec!["pre-1", "pre-2"]);
        assert!(only_etcd.is_empty());
        assert_eq!(backfill_registry(&*lance, &*etcd_dyn).await.unwrap(), 2);

        let mirrored = MirroredRegistry {
            primary: lance.clone(),
            mirror: etcd_dyn.clone(),
            label: "rollout",
        };
        mirrored.upsert("new", "/n").await.unwrap();
        mirrored.remove("pre-1").await.unwrap();
        assert!(mirrored.contains("new").await.unwrap());

        let (only_lance, only_etcd) = diff_registries(&*lance, &*etcd_dyn).await.unwrap();
        assert!(only_lance.is_empty(), "{only_lance:?}");
        assert!(only_etcd.is_empty(), "{only_etcd:?}");
        let mut names: Vec<String> = etcd
            .list()
            .await
            .unwrap()
            .into_iter()
            .map(|e| e.name)
            .collect();
        names.sort();
        assert_eq!(names, vec!["new", "pre-2"]);
    }

    /// Names that are prefixes of other names must not collide: `list` is a
    /// prefix scan on `<kind>/`, so `a` and `ab` are distinct keys and a
    /// lookup of `a` must not see `ab`.
    #[tokio::test]
    #[ignore = "requires ETCD_TEST_ENDPOINTS"]
    async fn prefix_names_do_not_collide() {
        let r = registry().await.unwrap();
        r.upsert("ab", "/ab").await.unwrap();
        assert!(!r.contains("a").await.unwrap());
        assert!(r.get("a").await.unwrap().is_none());
        r.upsert("a", "/a").await.unwrap();
        assert_eq!(r.list().await.unwrap().len(), 2);
        r.remove("a").await.unwrap();
        assert!(r.contains("ab").await.unwrap());
    }
}
