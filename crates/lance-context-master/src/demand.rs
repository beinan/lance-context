//! Per-table maintenance demand folded from per-shard watermarks.
//!
//! Schema: `docs/design/scheduler-p1-data-model.md` §1. Everything here is a
//! pure function of its inputs; no etcd. The record under `P/demand/<hex>`
//! is a cache rebuilt from `demand-events/` plus the stats scan, so folding
//! must be idempotent and order-independent.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

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
    pub fn fold(&mut self, event: &DemandEvent, revision: i64, now_ms: i64) -> Result<(), Ignored> {
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
