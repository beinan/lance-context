//! Per-table maintenance demand folded from per-shard watermarks.
//!
//! Schema: `docs/design/scheduler-p1-data-model.md` §1. The types and
//! `TableDemand::fold` are pure; `Coordinator::publish_demand_event` is the
//! one etcd write, a monotonic put keyed by `(writer_epoch, sealed_through)`.
//! The record under `P/demand/<hex>` is a cache rebuilt from
//! `demand-events/` plus the stats scan, so folding must be idempotent and
//! order-independent.

use std::collections::BTreeMap;

use etcd_client::{Compare, CompareOp, TxnOp};
use serde::{Deserialize, Serialize};

use crate::{Coordinator, Result};

fn chrono_now_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or_default()
}

pub const SCHEMA_VERSION: u32 = 1;

/// Who produced a watermark event.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EventSource {
    /// The shard's own writer, on flush.
    Writer,
    /// An executor that merged generations, written in its release txn.
    Executor,
    /// The periodic stats scan observing the shard from storage.
    Scan,
}

/// One shard's absolute watermarks. Values are absolute, never deltas, so a
/// lost or duplicated event cannot corrupt the fold.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DemandEvent {
    #[serde(default = "schema_version")]
    pub v: u32,
    pub shard: String,
    /// Highest generation sealed (flushed) on this shard.
    pub sealed_through: u64,
    #[serde(default)]
    pub sealed_bytes_through: u64,
    /// Highest generation merged into the base table, if known.
    #[serde(default)]
    pub merged_through: Option<u64>,
    pub flushed_at_ms: i64,
    /// The shard writer's epoch. A lower epoch never overwrites a higher one.
    pub writer_epoch: u64,
    pub source: EventSource,
}

fn schema_version() -> u32 {
    SCHEMA_VERSION
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct ShardDemand {
    pub sealed: u64,
    pub sealed_bytes: u64,
    pub merged: u64,
    pub epoch: u64,
    pub flushed_at_ms: i64,
}

impl ShardDemand {
    pub fn pending_generations(&self) -> u64 {
        self.sealed.saturating_sub(self.merged)
    }
}

/// The per-table record under `P/demand/<hex>`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct TableDemand {
    #[serde(default = "schema_version")]
    pub v: u32,
    pub shards: BTreeMap<String, ShardDemand>,
    #[serde(default)]
    pub fragment_count: u64,
    #[serde(default)]
    pub index_stale: bool,
    #[serde(default)]
    pub uncovered_fragments: u64,
    #[serde(default)]
    pub missing_fragments: bool,
    /// etcd revision of the newest input folded in.
    #[serde(default)]
    pub observed_revision: i64,
    #[serde(default)]
    pub updated_ms: i64,
}

/// Why an event did not change the table record.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Ignored {
    /// A lower writer epoch than the stored one.
    StaleEpoch,
    /// Same epoch, nothing newer than what is stored.
    NotNewer,
    /// A scan tried to lower `sealed_through` without a higher epoch.
    ScanCannotLower,
}

impl TableDemand {
    pub fn pending_generations(&self) -> u64 {
        self.shards
            .values()
            .map(ShardDemand::pending_generations)
            .sum()
    }

    pub fn pending_bytes(&self) -> u64 {
        // Bytes are tracked only as "through sealed"; approximate pending
        // bytes by the sealed-bytes watermark of shards with pending work.
        // A per-generation size table is out of scope for P1.
        self.shards
            .values()
            .filter(|s| s.pending_generations() > 0)
            .map(|s| s.sealed_bytes)
            .sum()
    }

    /// Oldest flush time among shards with pending generations.
    pub fn oldest_pending_ms(&self) -> Option<i64> {
        self.shards
            .values()
            .filter(|s| s.pending_generations() > 0)
            .map(|s| s.flushed_at_ms)
            .min()
    }

    /// Fold one event. Returns `Err(reason)` when the record is unchanged.
    ///
    /// Rules (spec §1.1):
    /// * a strictly higher `writer_epoch` replaces the shard entirely;
    /// * a lower epoch is ignored;
    /// * same epoch: `sealed` and `merged` only move forward, except that a
    ///   `Scan` may never lower `sealed` and no source may lower `merged`;
    /// * `flushed_at_ms` follows the newest `sealed`.
    pub fn fold(
        &mut self,
        event: &DemandEvent,
        revision: i64,
        now_ms: i64,
    ) -> std::result::Result<(), Ignored> {
        let entry = self.shards.entry(event.shard.clone());
        let changed = match entry {
            std::collections::btree_map::Entry::Vacant(slot) => {
                slot.insert(ShardDemand {
                    sealed: event.sealed_through,
                    sealed_bytes: event.sealed_bytes_through,
                    merged: event.merged_through.unwrap_or(0),
                    epoch: event.writer_epoch,
                    flushed_at_ms: event.flushed_at_ms,
                });
                true
            }
            std::collections::btree_map::Entry::Occupied(mut slot) => {
                let shard = slot.get_mut();
                if event.writer_epoch < shard.epoch {
                    return Err(Ignored::StaleEpoch);
                }
                if event.writer_epoch > shard.epoch {
                    // A new writer epoch restarts the shard's numbering
                    // authority; take the event wholesale but never lose a
                    // merged watermark the executor already recorded.
                    *shard = ShardDemand {
                        sealed: event.sealed_through,
                        sealed_bytes: event.sealed_bytes_through,
                        merged: event.merged_through.unwrap_or(0).max(shard.merged),
                        epoch: event.writer_epoch,
                        flushed_at_ms: event.flushed_at_ms,
                    };
                    true
                } else {
                    let mut changed = false;
                    if event.sealed_through > shard.sealed {
                        shard.sealed = event.sealed_through;
                        shard.sealed_bytes = shard.sealed_bytes.max(event.sealed_bytes_through);
                        shard.flushed_at_ms = event.flushed_at_ms;
                        changed = true;
                    } else if event.sealed_through < shard.sealed
                        && event.source == EventSource::Scan
                    {
                        return Err(Ignored::ScanCannotLower);
                    }
                    if let Some(merged) = event.merged_through {
                        if merged > shard.merged {
                            shard.merged = merged;
                            changed = true;
                        }
                    }
                    if !changed {
                        return Err(Ignored::NotNewer);
                    }
                    true
                }
            }
        };
        if changed {
            self.observed_revision = self.observed_revision.max(revision);
            self.updated_ms = now_ms;
        }
        Ok(())
    }
}

/// A shard's merged watermark as observed by an executor after its commit.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ShardMerged {
    pub shard: String,
    pub merged_through: u64,
}

impl Coordinator {
    /// Publish one shard's merged watermark as its own monotonic put.
    ///
    /// Reads the stored record, advances only `merged_through` (never lowers
    /// anything the writer set), and writes under a mod-revision compare. A
    /// writer flush racing on the same key fails the compare; the put is
    /// retried a bounded number of times against the fresh record so a
    /// normal flush cannot make merged progress disappear. A shard with no
    /// stored event gets a fresh executor-sourced record with
    /// `sealed_through = merged_through` at epoch 0, which any real writer
    /// epoch supersedes. Returns `Ok(false)` when the stored merged mark is
    /// already at or past the new one.
    pub async fn publish_merged_watermark(
        &self,
        target: &str,
        shard: &ShardMerged,
    ) -> Result<bool> {
        let key = self.demand_event_key(target, &shard.shard);
        let mut client = self.client.clone();
        for _ in 0..5 {
            let current = client
                .get(key.as_str(), None)
                .await
                .map_err(|e| e.to_string())?;
            let stored: Option<DemandEvent> = current
                .kvs()
                .first()
                .and_then(|kv| serde_json::from_slice(kv.value()).ok());
            // The executor merged generations numbered under the stored
            // writer's epoch, so report under that epoch.
            let epoch = stored.as_ref().map_or(0, |s| s.writer_epoch);
            let event = DemandEvent {
                v: SCHEMA_VERSION,
                shard: shard.shard.clone(),
                sealed_through: stored
                    .as_ref()
                    .map_or(shard.merged_through, |s| s.sealed_through),
                sealed_bytes_through: stored.as_ref().map_or(0, |s| s.sealed_bytes_through),
                merged_through: Some(shard.merged_through),
                flushed_at_ms: stored
                    .as_ref()
                    .map_or_else(chrono_now_ms, |s| s.flushed_at_ms),
                writer_epoch: epoch,
                source: EventSource::Executor,
            };
            let Some(next) = join_events(stored.as_ref(), &event) else {
                return Ok(false);
            };
            let value = serde_json::to_vec(&next).map_err(|e| e.to_string())?;
            let compare = match current.kvs().first() {
                Some(kv) => {
                    Compare::mod_revision(key.as_str(), CompareOp::Equal, kv.mod_revision())
                }
                None => Compare::version(key.as_str(), CompareOp::Equal, 0),
            };
            let response = client
                .txn(
                    etcd_client::Txn::new()
                        .when(vec![compare])
                        .and_then(vec![TxnOp::put(key.as_str(), value, None)]),
                )
                .await
                .map_err(|e| e.to_string())?;
            if response.succeeded() {
                return Ok(true);
            }
        }
        Err(format!(
            "merged watermark for {target}/{} kept losing to concurrent writes",
            shard.shard
        ))
    }

    fn demand_event_key(&self, target: &str, shard: &str) -> String {
        let encoded: String = target.bytes().map(|b| format!("{b:02x}")).collect();
        format!(
            "{}/demand-events/{encoded}/{shard}",
            self.prefix.trim_end_matches('/')
        )
    }

    /// Publish a shard watermark. The stored record is the per-dimension join
    /// of everything published so far: `sealed` and `merged` are each the max
    /// of `(writer_epoch, value)` across stored and new, independently, so a
    /// writer event with `merged_through: None` keeps the executor's merged
    /// mark and an executor event with an older sealed still advances merged.
    /// Writes under a mod-revision compare and retries a bounded number of
    /// times against the fresh record when a concurrent writer wins the race,
    /// so no publisher's progress is lost to another's. Returns `Ok(false)`
    /// when nothing advanced. Never deletes. Safe on every flush.
    pub async fn publish_demand_event(&self, target: &str, event: &DemandEvent) -> Result<bool> {
        let key = self.demand_event_key(target, &event.shard);
        let mut client = self.client.clone();
        for _ in 0..5 {
            let current = client
                .get(key.as_str(), None)
                .await
                .map_err(|e| e.to_string())?;
            let stored: Option<DemandEvent> = current
                .kvs()
                .first()
                .and_then(|kv| serde_json::from_slice(kv.value()).ok());
            let Some(next) = join_events(stored.as_ref(), event) else {
                return Ok(false);
            };
            let value = serde_json::to_vec(&next).map_err(|e| e.to_string())?;
            let compare = match current.kvs().first() {
                Some(kv) => {
                    Compare::mod_revision(key.as_str(), CompareOp::Equal, kv.mod_revision())
                }
                None => Compare::version(key.as_str(), CompareOp::Equal, 0),
            };
            let response = client
                .txn(
                    etcd_client::Txn::new()
                        .when(vec![compare])
                        .and_then(vec![TxnOp::put(key.as_str(), value, None)]),
                )
                .await
                .map_err(|e| e.to_string())?;
            if response.succeeded() {
                return Ok(true);
            }
        }
        Err(format!(
            "demand event for {target}/{} kept losing to concurrent writes",
            event.shard
        ))
    }
}

/// Per-dimension join of a stored event with a new one. Returns `None` when
/// the stored record already dominates on every dimension. `sealed` is
/// ordered by `(writer_epoch, sealed_through)` then bytes; `merged` by
/// `(writer_epoch, merged_through)`; each taken independently. The result
/// keeps the stored record's writer fields unless the new event's sealed
/// mark wins. A merged mark whose epoch differs from the resulting sealed
/// epoch is dropped: the wire record stores merged as a bare value under the
/// record's epoch, and a cross-epoch merged mark says nothing about this
/// epoch's generations anyway.
pub fn join_events(stored: Option<&DemandEvent>, event: &DemandEvent) -> Option<DemandEvent> {
    let Some(stored) = stored else {
        return Some(event.clone());
    };
    let stored_sealed = (stored.writer_epoch, stored.sealed_through);
    let event_sealed = (event.writer_epoch, event.sealed_through);
    let stored_merged = stored.merged_through.map(|v| (stored.writer_epoch, v));
    let event_merged = event.merged_through.map(|v| (event.writer_epoch, v));
    let sealed_wins = event_sealed > stored_sealed
        || (event_sealed == stored_sealed
            && event.sealed_bytes_through > stored.sealed_bytes_through);
    let merged_wins = match (stored_merged, event_merged) {
        (_, None) => false,
        (None, Some(_)) => true,
        (Some(s), Some(e)) => e > s,
    };
    if !sealed_wins && !merged_wins {
        return None;
    }
    let best_merged = match (stored_merged, event_merged) {
        (Some(s), Some(e)) => Some(s.max(e)),
        (s, e) => s.or(e),
    };
    let mut next = if sealed_wins {
        event.clone()
    } else {
        stored.clone()
    };
    next.merged_through = best_merged
        .filter(|(epoch, _)| *epoch == next.writer_epoch)
        .map(|(_, value)| value);
    if merged_wins && !sealed_wins {
        next.source = event.source;
    }
    Some(next)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ev(
        shard: &str,
        sealed: u64,
        merged: Option<u64>,
        epoch: u64,
        source: EventSource,
    ) -> DemandEvent {
        DemandEvent {
            v: 1,
            shard: shard.into(),
            sealed_through: sealed,
            sealed_bytes_through: sealed * 1000,
            merged_through: merged,
            flushed_at_ms: sealed as i64 * 10,
            writer_epoch: epoch,
            source,
        }
    }

    fn fold_all(events: &[DemandEvent]) -> TableDemand {
        let mut table = TableDemand::default();
        for (i, event) in events.iter().enumerate() {
            let _ = table.fold(event, i as i64, 0);
        }
        table.observed_revision = 0;
        table.updated_ms = 0;
        table
    }

    fn permutations<T: Clone>(items: &[T]) -> Vec<Vec<T>> {
        if items.len() <= 1 {
            return vec![items.to_vec()];
        }
        let mut out = Vec::new();
        for i in 0..items.len() {
            let mut rest = items.to_vec();
            let head = rest.remove(i);
            for mut tail in permutations(&rest) {
                tail.insert(0, head.clone());
                out.push(tail);
            }
        }
        out
    }

    #[test]
    fn fold_is_order_independent_and_idempotent() {
        let events = vec![
            ev("a", 10, None, 1, EventSource::Writer),
            ev("a", 12, Some(8), 1, EventSource::Executor),
            ev("a", 11, Some(11), 1, EventSource::Executor),
            ev("b", 3, None, 2, EventSource::Writer),
            ev("b", 5, None, 2, EventSource::Scan),
        ];
        let canonical = fold_all(&events);
        assert_eq!(canonical.shards["a"].sealed, 12);
        assert_eq!(canonical.shards["a"].merged, 11);
        assert_eq!(canonical.shards["b"].sealed, 5);
        assert_eq!(canonical.pending_generations(), 1 + 5);
        for perm in permutations(&events) {
            assert_eq!(fold_all(&perm), canonical, "order {perm:?}");
            // Duplicating every event changes nothing.
            let doubled: Vec<_> = perm.iter().flat_map(|e| [e.clone(), e.clone()]).collect();
            assert_eq!(fold_all(&doubled), canonical);
        }
        // Replaying the final state onto itself is a no-op.
        let mut again = canonical.clone();
        for event in &events {
            let _ = again.fold(event, 99, 99);
        }
        again.observed_revision = 0;
        again.updated_ms = 0;
        assert_eq!(again, canonical);
    }

    #[test]
    fn scan_may_raise_but_not_lower_without_higher_epoch() {
        let mut table = TableDemand::default();
        table
            .fold(&ev("a", 20, None, 3, EventSource::Writer), 1, 0)
            .unwrap();
        assert_eq!(
            table.fold(&ev("a", 15, None, 3, EventSource::Scan), 2, 0),
            Err(Ignored::ScanCannotLower)
        );
        assert_eq!(table.shards["a"].sealed, 20);
        table
            .fold(&ev("a", 25, None, 3, EventSource::Scan), 3, 0)
            .unwrap();
        assert_eq!(table.shards["a"].sealed, 25);
        // A retired shard re-observed by a scan under a higher epoch may lower.
        table
            .fold(&ev("a", 2, None, 4, EventSource::Scan), 4, 0)
            .unwrap();
        assert_eq!(table.shards["a"].sealed, 2);
        assert_eq!(table.shards["a"].epoch, 4);
    }

    #[test]
    fn epoch_rules_and_merged_watermark_preservation() {
        let mut table = TableDemand::default();
        table
            .fold(&ev("a", 50, Some(40), 2, EventSource::Executor), 1, 0)
            .unwrap();
        assert_eq!(
            table.fold(&ev("a", 60, None, 1, EventSource::Writer), 2, 0),
            Err(Ignored::StaleEpoch)
        );
        assert_eq!(
            table.fold(&ev("a", 50, Some(39), 2, EventSource::Executor), 3, 0),
            Err(Ignored::NotNewer),
            "merged never moves backwards"
        );
        // New epoch takes the event but keeps the recorded merged watermark.
        table
            .fold(&ev("a", 5, None, 3, EventSource::Writer), 4, 0)
            .unwrap();
        assert_eq!(table.shards["a"].merged, 40);
        assert_eq!(table.shards["a"].sealed, 5);
        assert_eq!(table.shards["a"].pending_generations(), 0);
    }

    #[test]
    fn aggregates_and_revision_tracking() {
        let mut table = TableDemand::default();
        table
            .fold(&ev("a", 10, Some(4), 1, EventSource::Writer), 7, 100)
            .unwrap();
        table
            .fold(&ev("b", 3, Some(3), 1, EventSource::Writer), 9, 200)
            .unwrap();
        table
            .fold(&ev("c", 8, None, 1, EventSource::Writer), 8, 300)
            .unwrap();
        assert_eq!(table.pending_generations(), 14, "a: 6, b: 0, c: 8");
        assert_eq!(table.pending_bytes(), 10_000 + 8_000);
        assert_eq!(table.oldest_pending_ms(), Some(80));
        assert_eq!(table.observed_revision, 9);
        assert_eq!(table.updated_ms, 300);
        assert_eq!(
            table.fold(&ev("b", 3, None, 1, EventSource::Writer), 50, 400),
            Err(Ignored::NotNewer)
        );
        assert_eq!(table.observed_revision, 9, "ignored events do not advance");
    }

    #[tokio::test]
    #[ignore = "requires isolated local ETCD_TEST_ENDPOINTS"]
    async fn publish_is_monotonic_by_epoch_then_sealed_then_merged() {
        let endpoint = std::env::var("ETCD_TEST_ENDPOINTS").unwrap();
        let client = etcd_client::Client::connect([endpoint], None)
            .await
            .unwrap();
        let coordinator =
            Coordinator::new(client, format!("/demand-publish/{}", uuid::Uuid::new_v4()));
        let publish = |e: DemandEvent| {
            let c = coordinator.clone();
            async move { c.publish_demand_event("t", &e).await.unwrap() }
        };
        assert!(publish(ev("s", 10, None, 1, EventSource::Writer)).await);
        assert!(
            !publish(ev("s", 10, None, 1, EventSource::Writer)).await,
            "duplicate"
        );
        assert!(
            !publish(ev("s", 9, None, 1, EventSource::Scan)).await,
            "lower sealed"
        );
        assert!(
            !publish(ev("s", 50, None, 0, EventSource::Writer)).await,
            "lower epoch"
        );
        assert!(
            publish(ev("s", 10, Some(4), 1, EventSource::Executor)).await,
            "merged advances"
        );
        assert!(
            !publish(ev("s", 10, Some(3), 1, EventSource::Executor)).await,
            "merged regress"
        );
        assert!(
            publish(ev("s", 12, None, 1, EventSource::Writer)).await,
            "sealed advances"
        );
        assert!(
            publish(ev("s", 1, None, 2, EventSource::Scan)).await,
            "higher epoch"
        );
        assert!(
            publish(ev("other", 1, None, 1, EventSource::Writer)).await,
            "separate shard"
        );
    }

    #[tokio::test]
    #[ignore = "requires isolated local ETCD_TEST_ENDPOINTS"]
    async fn merged_publish_advances_only_merged_and_survives_writer_races() {
        let endpoint = std::env::var("ETCD_TEST_ENDPOINTS").unwrap();
        let client = etcd_client::Client::connect([endpoint], None)
            .await
            .unwrap();
        let coordinator = Coordinator::new(
            client.clone(),
            format!("/demand-merged/{}", uuid::Uuid::new_v4()),
        );
        assert!(coordinator
            .publish_demand_event("t", &ev("s", 20, None, 3, EventSource::Writer))
            .await
            .unwrap());
        let shard = |m: u64| ShardMerged {
            shard: "s".into(),
            merged_through: m,
        };
        assert!(coordinator
            .publish_merged_watermark("t", &shard(15))
            .await
            .unwrap());
        let key = coordinator.demand_event_key("t", "s");
        let read = |client: etcd_client::Client, key: String| async move {
            serde_json::from_slice::<DemandEvent>(
                client.clone().get(key, None).await.unwrap().kvs()[0].value(),
            )
            .unwrap()
        };
        let after = read(client.clone(), key.clone()).await;
        assert_eq!(after.merged_through, Some(15));
        assert_eq!(after.sealed_through, 20, "writer's sealed kept");
        assert_eq!(after.writer_epoch, 3, "writer's epoch kept");
        assert_eq!(after.sealed_bytes_through, 20_000, "writer's bytes kept");
        assert!(
            !coordinator
                .publish_merged_watermark("t", &shard(12))
                .await
                .unwrap(),
            "lower merged is a no-op"
        );
        // Race: a writer flush lands between the executor's read and its CAS.
        // Simulate by interleaving: publish writer sealed=25 then merged=18;
        // both must survive regardless of which the stored record saw first.
        let c1 = coordinator.clone();
        let c2 = coordinator.clone();
        let (w, m) = tokio::join!(
            async move {
                c1.publish_demand_event("t", &ev("s", 25, None, 3, EventSource::Writer))
                    .await
                    .unwrap()
            },
            async move { c2.publish_merged_watermark("t", &shard(18)).await.unwrap() }
        );
        assert!(w && m, "both writes must succeed (writer={w}, merged={m})");
        let stored = read(client.clone(), key.clone()).await;
        assert_eq!(
            (stored.sealed_through, stored.merged_through),
            (25, Some(18))
        );
        // Unknown shard: fresh executor record, sealed == merged, epoch 0.
        assert!(coordinator
            .publish_merged_watermark(
                "t",
                &ShardMerged {
                    shard: "new".into(),
                    merged_through: 7
                }
            )
            .await
            .unwrap());
        let fresh = read(client.clone(), coordinator.demand_event_key("t", "new")).await;
        assert_eq!(
            (
                fresh.sealed_through,
                fresh.merged_through,
                fresh.writer_epoch
            ),
            (7, Some(7), 0)
        );
    }

    #[test]
    fn wire_format_is_stable() {
        let event = ev("s", 1, Some(1), 1, EventSource::Writer);
        let json = serde_json::to_value(&event).unwrap();
        assert_eq!(json["v"], 1);
        assert_eq!(json["source"], "writer");
        let back: DemandEvent = serde_json::from_value(json).unwrap();
        assert_eq!(back, event);
        let minimal: DemandEvent = serde_json::from_str(
            r#"{"shard":"s","sealed_through":1,"flushed_at_ms":0,"writer_epoch":1,"source":"scan"}"#,
        )
        .unwrap();
        assert_eq!(minimal.v, 1);
        assert_eq!(minimal.merged_through, None);
    }
}
