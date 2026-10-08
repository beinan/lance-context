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

/// A watermark with the epoch that produced it. Ordered lexicographically:
/// a higher epoch wins regardless of value; within an epoch the higher
/// value wins. This is the only ordering used anywhere in the fold.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, Default)]
pub struct Mark {
    pub epoch: u64,
    pub value: u64,
}

/// How many `(generation, flushed_at)` pairs a shard keeps for age tracking.
/// Only the lowest unmerged generations matter for "oldest pending", so the
/// set is truncated to the smallest `SEALED_TIMES_CAP` generation numbers.
/// Union-then-truncate-to-smallest is order independent.
pub const SEALED_TIMES_CAP: usize = 64;

/// One shard's folded state. `sealed`, `merged` and `sealed_bytes` are pure
/// joins (max), so `pending_generations` and `pending_bytes` are exactly
/// order independent (spec invariant 5a). `sealed` and `merged` are
/// independent: nothing about one lowers or replaces the other.
/// `sealed_times` is a join (union with min time) that is truncated to the
/// lowest `SEALED_TIMES_CAP` pending generations; `oldest_pending_ms` is
/// therefore exact while a shard has at most that many pending generations
/// and a best-known lower bound beyond it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct ShardDemand {
    /// Highest `(writer_epoch, sealed_through)` seen.
    pub sealed: Mark,
    /// Highest `(writer_epoch, merged_through)` seen; `None` until an
    /// executor or scan reports one.
    #[serde(default)]
    pub merged: Option<Mark>,
    /// Max `sealed_bytes_through` seen at the current `sealed` epoch.
    #[serde(default)]
    pub sealed_bytes: u64,
    /// Flush times of sealed generations at the current `sealed` epoch, for
    /// the lowest `SEALED_TIMES_CAP` generations above `merged`. Pruned as
    /// `merged` advances. The oldest pending generation's time is the first
    /// entry; a newer flush adds an entry but never changes the first.
    #[serde(default)]
    pub sealed_times: BTreeMap<u64, i64>,
}

impl ShardDemand {
    /// Generations sealed but not merged. Comparable only when both marks
    /// are at the same epoch. A `merged` from an older epoch says nothing
    /// about this epoch's generations; a `merged` from a newer epoch means
    /// the shard was replaced and nothing from the old epoch is pending.
    pub fn pending_generations(&self) -> u64 {
        match self.merged {
            None => self.sealed.value,
            Some(merged) if merged.epoch == self.sealed.epoch => {
                self.sealed.value.saturating_sub(merged.value)
            }
            Some(merged) if merged.epoch > self.sealed.epoch => 0,
            Some(_) => self.sealed.value,
        }
    }

    /// Merged generation at the sealed epoch, if known.
    fn merged_at_sealed_epoch(&self) -> Option<u64> {
        match self.merged {
            Some(m) if m.epoch == self.sealed.epoch => Some(m.value),
            Some(m) if m.epoch > self.sealed.epoch => Some(u64::MAX),
            _ => None,
        }
    }

    fn prune_sealed_times(&mut self) {
        if let Some(merged) = self.merged_at_sealed_epoch() {
            self.sealed_times
                .retain(|generation, _| *generation > merged);
        }
        while self.sealed_times.len() > SEALED_TIMES_CAP {
            self.sealed_times.pop_last();
        }
    }

    /// Flush time of the oldest generation still unmerged, if known.
    pub fn oldest_pending_ms(&self) -> Option<i64> {
        if self.pending_generations() == 0 {
            return None;
        }
        self.sealed_times.values().next().copied()
    }
}

/// The per-table record under `P/demand/<hex>`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TableDemand {
    #[serde(default = "schema_version")]
    pub v: u32,
    #[serde(default)]
    pub shards: BTreeMap<String, ShardDemand>,
    #[serde(default)]
    pub fragment_count: u64,
    #[serde(default)]
    pub index_stale: bool,
    #[serde(default)]
    pub uncovered_fragments: u64,
    #[serde(default)]
    pub missing_fragments: bool,
    /// etcd revision of the newest input folded in. Freshness bookkeeping
    /// only; never an ordering input.
    #[serde(default)]
    pub observed_revision: i64,
    #[serde(default)]
    pub updated_ms: i64,
}

impl Default for TableDemand {
    fn default() -> Self {
        Self {
            v: SCHEMA_VERSION,
            shards: BTreeMap::new(),
            fragment_count: 0,
            index_stale: false,
            uncovered_fragments: 0,
            missing_fragments: false,
            observed_revision: 0,
            updated_ms: 0,
        }
    }
}

/// Why an event did not change the table record.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Ignored {
    /// Neither `sealed` nor `merged` nor bytes advanced.
    NotNewer,
    /// The event's schema major version is not understood.
    UnknownVersion(u32),
}

impl TableDemand {
    /// Reject records from a future schema. `v == 0` is a pre-versioning
    /// record and is read as version 1.
    pub fn check_version(v: u32) -> std::result::Result<(), Ignored> {
        match v {
            0 | SCHEMA_VERSION => Ok(()),
            other => Err(Ignored::UnknownVersion(other)),
        }
    }

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

    /// Flush time of the oldest generation still unmerged on any shard.
    /// Does not move when newer generations arrive.
    pub fn oldest_pending_ms(&self) -> Option<i64> {
        self.shards
            .values()
            .filter_map(ShardDemand::oldest_pending_ms)
            .min()
    }

    /// Fold one event. Returns `Err(reason)` when the record is unchanged.
    ///
    /// `sealed` and `merged` are joined independently by `(epoch, value)`.
    /// Source is irrelevant to ordering: writer, executor and scan all report
    /// absolute watermarks and the max wins, so a stale scan cannot lower
    /// anything - a lower value simply loses the max.
    pub fn fold(
        &mut self,
        event: &DemandEvent,
        revision: i64,
        now_ms: i64,
    ) -> std::result::Result<(), Ignored> {
        Self::check_version(event.v)?;
        let shard = self.shards.entry(event.shard.clone()).or_default();
        let mut changed = false;

        let sealed = Mark {
            epoch: event.writer_epoch,
            value: event.sealed_through,
        };
        // A time entry is only useful for a generation that is still
        // pending; recording one for an already-merged generation would be
        // pruned straight away and must not count as a change.
        let records_time = event.sealed_through > 0
            && shard
                .merged
                .is_none_or(|m| m.epoch != event.writer_epoch || event.sealed_through > m.value);
        match sealed.cmp(&shard.sealed) {
            std::cmp::Ordering::Greater => {
                if sealed.epoch > shard.sealed.epoch {
                    // New writer epoch: byte and age bookkeeping restart.
                    shard.sealed_bytes = event.sealed_bytes_through;
                    shard.sealed_times.clear();
                } else {
                    shard.sealed_bytes = shard.sealed_bytes.max(event.sealed_bytes_through);
                }
                shard.sealed = sealed;
                if records_time {
                    shard
                        .sealed_times
                        .entry(event.sealed_through)
                        .and_modify(|t| *t = (*t).min(event.flushed_at_ms))
                        .or_insert(event.flushed_at_ms);
                }
                changed = true;
            }
            std::cmp::Ordering::Equal => {
                if event.sealed_bytes_through > shard.sealed_bytes {
                    shard.sealed_bytes = event.sealed_bytes_through;
                    changed = true;
                }
                // Same generation seen again: keep the earliest time.
                if records_time {
                    let entry = shard.sealed_times.entry(event.sealed_through);
                    if let std::collections::btree_map::Entry::Occupied(mut slot) = entry {
                        if event.flushed_at_ms < *slot.get() {
                            slot.insert(event.flushed_at_ms);
                            changed = true;
                        }
                    } else {
                        entry.or_insert(event.flushed_at_ms);
                        changed = true;
                    }
                }
            }
            std::cmp::Ordering::Less => {
                // An older sealed watermark at the same epoch still tells us
                // when that generation was flushed, which matters for age.
                if sealed.epoch == shard.sealed.epoch && records_time {
                    let entry = shard.sealed_times.entry(event.sealed_through);
                    match entry {
                        std::collections::btree_map::Entry::Occupied(mut slot) => {
                            if event.flushed_at_ms < *slot.get() {
                                slot.insert(event.flushed_at_ms);
                                changed = true;
                            }
                        }
                        std::collections::btree_map::Entry::Vacant(slot) => {
                            slot.insert(event.flushed_at_ms);
                            changed = true;
                        }
                    }
                }
            }
        }

        if let Some(merged_through) = event.merged_through {
            let merged = Mark {
                epoch: event.writer_epoch,
                value: merged_through,
            };
            if shard.merged.is_none_or(|current| merged > current) {
                shard.merged = Some(merged);
                changed = true;
            }
        }

        if changed {
            shard.prune_sealed_times();
            self.observed_revision = self.observed_revision.max(revision);
            self.updated_ms = now_ms;
            Ok(())
        } else {
            Err(Ignored::NotNewer)
        }
    }
}

/// Per-dimension join of a stored event with a new one. Returns `None` when
/// the stored record already dominates on every dimension. The result keeps
/// the stored record's writer fields unless the new event's sealed mark
/// wins, in which case the new event's epoch, bytes and flush time apply.
pub fn join_events(stored: Option<&DemandEvent>, event: &DemandEvent) -> Option<DemandEvent> {
    let Some(stored) = stored else {
        return Some(event.clone());
    };
    let stored_sealed = Mark {
        epoch: stored.writer_epoch,
        value: stored.sealed_through,
    };
    let event_sealed = Mark {
        epoch: event.writer_epoch,
        value: event.sealed_through,
    };
    let stored_merged = stored.merged_through.map(|v| Mark {
        epoch: stored.writer_epoch,
        value: v,
    });
    let event_merged = event.merged_through.map(|v| Mark {
        epoch: event.writer_epoch,
        value: v,
    });
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
    // The wire record stores merged as a bare value under the record's
    // epoch. A merged mark from a different epoch than the record's sealed
    // epoch cannot be expressed there, and says nothing about this epoch's
    // generations anyway (see ShardDemand::pending_generations), so drop it.
    next.merged_through = best_merged
        .filter(|m| m.epoch == next.writer_epoch)
        .map(|m| m.value);
    if merged_wins && !sealed_wins {
        next.source = event.source;
    }
    Some(next)
}

/// A shard's merged watermark as observed by an executor after its commit.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ShardMerged {
    pub shard: String,
    pub merged_through: u64,
}

impl Coordinator {
    /// Compare/op pairs that publish `merged_through` for each shard, for
    /// inclusion in a larger transaction. Uses the same per-dimension join as
    /// `publish_demand_event`; shards that would not advance emit nothing.
    /// A shard with no stored event gets a fresh executor-sourced record
    /// with `sealed_through = merged_through` (the writer's next flush
    /// raises it) at epoch 0, which any real writer epoch supersedes.
    pub(crate) async fn demand_release_changes(
        &self,
        target: &str,
        merged: &[ShardMerged],
    ) -> Result<(Vec<Compare>, Vec<TxnOp>)> {
        let mut compares = Vec::new();
        let mut operations = Vec::new();
        if merged.is_empty() {
            return Ok((compares, operations));
        }
        let mut client = self.client.clone();
        let now_ms = chrono_now_ms();
        for shard in merged {
            let key = self.demand_event_key(target, &shard.shard);
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
                flushed_at_ms: stored.as_ref().map_or(now_ms, |s| s.flushed_at_ms),
                writer_epoch: epoch,
                source: EventSource::Executor,
            };
            let Some(next) = join_events(stored.as_ref(), &event) else {
                continue;
            };
            let value = serde_json::to_vec(&next).map_err(|e| e.to_string())?;
            compares.push(match current.kvs().first() {
                Some(kv) => {
                    Compare::mod_revision(key.as_str(), CompareOp::Equal, kv.mod_revision())
                }
                None => Compare::version(key.as_str(), CompareOp::Equal, 0),
            });
            operations.push(TxnOp::put(key, value, None));
        }
        Ok((compares, operations))
    }

    fn demand_event_key(&self, target: &str, shard: &str) -> String {
        let encoded: String = target.bytes().map(|b| format!("{b:02x}")).collect();
        format!(
            "{}/demand-events/{encoded}/{shard}",
            self.prefix.trim_end_matches('/')
        )
    }

    /// Publish a shard watermark. The stored record is the per-dimension
    /// join of everything published so far (`join_events`): a writer event
    /// with `merged_through: None` keeps the executor's merged mark, and an
    /// executor event with an older sealed still advances merged. Returns
    /// `Ok(false)` when nothing advanced. Never deletes. Safe on every flush.
    pub async fn publish_demand_event(&self, target: &str, event: &DemandEvent) -> Result<bool> {
        TableDemand::check_version(event.v).map_err(|e| format!("{e:?}"))?;
        let key = self.demand_event_key(target, &event.shard);
        let mut client = self.client.clone();
        let current = client
            .get(key.as_str(), None)
            .await
            .map_err(|e| e.to_string())?;
        let Some(kv) = current.kvs().first() else {
            let value = serde_json::to_vec(event).map_err(|e| e.to_string())?;
            let response = client
                .txn(
                    etcd_client::Txn::new()
                        .when(vec![Compare::version(key.as_str(), CompareOp::Equal, 0)])
                        .and_then(vec![TxnOp::put(key.as_str(), value, None)]),
                )
                .await
                .map_err(|e| e.to_string())?;
            return Ok(response.succeeded());
        };
        // An unreadable record is treated as empty and replaced.
        let stored: Option<DemandEvent> = serde_json::from_slice(kv.value()).ok();
        let Some(next) = join_events(stored.as_ref(), event) else {
            return Ok(false);
        };
        let value = serde_json::to_vec(&next).map_err(|e| e.to_string())?;
        let response = client
            .txn(
                etcd_client::Txn::new()
                    .when(vec![Compare::mod_revision(
                        key.as_str(),
                        CompareOp::Equal,
                        kv.mod_revision(),
                    )])
                    .and_then(vec![TxnOp::put(key.as_str(), value, None)]),
            )
            .await
            .map_err(|e| e.to_string())?;
        Ok(response.succeeded())
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

    fn assert_order_independent(events: &[DemandEvent]) -> TableDemand {
        let canonical = fold_all(events);
        for perm in permutations(events) {
            assert_eq!(fold_all(&perm), canonical, "order {perm:?}");
            let doubled: Vec<_> = perm.iter().flat_map(|e| [e.clone(), e.clone()]).collect();
            assert_eq!(fold_all(&doubled), canonical, "doubled {perm:?}");
        }
        let mut again = canonical.clone();
        for event in events {
            let _ = again.fold(event, 99, 99);
        }
        again.observed_revision = 0;
        again.updated_ms = 0;
        assert_eq!(again, canonical, "replay onto final state");
        canonical
    }

    #[test]
    fn fold_is_order_independent_and_idempotent() {
        let table = assert_order_independent(&[
            ev("a", 10, None, 1, EventSource::Writer),
            ev("a", 12, Some(8), 1, EventSource::Executor),
            ev("a", 11, Some(11), 1, EventSource::Executor),
            ev("b", 3, None, 2, EventSource::Writer),
            ev("b", 5, None, 2, EventSource::Scan),
        ]);
        assert_eq!(table.shards["a"].sealed.value, 12);
        assert_eq!(table.shards["a"].merged.unwrap().value, 11);
        assert_eq!(table.shards["b"].sealed.value, 5);
        assert_eq!(table.pending_generations(), 1 + 5);
    }

    /// Review counterexamples: an old-epoch merge beside a new-epoch flush,
    /// and a scan carrying an older sealed but newer merged. Both must fold
    /// to one answer regardless of order.
    #[test]
    fn review_counterexamples_fold_to_one_answer() {
        let table = assert_order_independent(&[
            ev("s", 100, Some(40), 1, EventSource::Executor),
            ev("s", 60, None, 2, EventSource::Writer),
        ]);
        // Epoch 2 sealed 60; merged is from epoch 1 and says nothing about
        // epoch 2's generations: all 60 pending.
        assert_eq!(table.pending_generations(), 60);

        let table = assert_order_independent(&[
            ev("s", 30, Some(10), 1, EventSource::Writer),
            ev("s", 20, Some(20), 1, EventSource::Scan),
        ]);
        // sealed = max(30, 20) = 30, merged = max(10, 20) = 20: 10 pending.
        assert_eq!(table.pending_generations(), 10);
    }

    /// Review finding 3: a writer event without `merged_through` must not
    /// discard a previously known merged watermark.
    #[test]
    fn writer_event_never_forgets_merged() {
        let table = assert_order_independent(&[
            ev("s", 10, Some(8), 1, EventSource::Executor),
            ev("s", 12, None, 1, EventSource::Writer),
        ]);
        assert_eq!(table.shards["s"].merged.unwrap().value, 8);
        assert_eq!(table.pending_generations(), 4);
    }

    #[test]
    fn scan_cannot_lower_anything() {
        let mut table = TableDemand::default();
        table
            .fold(&ev("a", 20, Some(5), 3, EventSource::Writer), 1, 0)
            .unwrap();
        // Lower sealed and lower merged from a scan at the same epoch: a
        // time entry for generation 15 is learned (useful for age) but no
        // watermark moves.
        table
            .fold(&ev("a", 15, Some(2), 3, EventSource::Scan), 2, 0)
            .unwrap();
        assert_eq!(table.shards["a"].sealed.value, 20);
        assert_eq!(table.shards["a"].merged.unwrap().value, 5);
        assert_eq!(
            table.fold(&ev("a", 15, Some(2), 3, EventSource::Scan), 3, 0),
            Err(Ignored::NotNewer),
            "and the second time it is a pure no-op"
        );
        table
            .fold(&ev("a", 25, None, 3, EventSource::Scan), 4, 0)
            .unwrap();
        assert_eq!(table.shards["a"].sealed.value, 25);
        // A higher epoch from a scan replaces sealed; the epoch-3 merged mark
        // stays recorded but no longer applies to epoch 4 generations.
        table
            .fold(&ev("a", 2, None, 4, EventSource::Scan), 5, 0)
            .unwrap();
        assert_eq!(table.shards["a"].sealed, Mark { epoch: 4, value: 2 });
        assert_eq!(table.shards["a"].pending_generations(), 2);
    }

    #[test]
    fn lower_epoch_events_do_not_move_marks() {
        let mut table = TableDemand::default();
        table
            .fold(&ev("a", 50, Some(40), 2, EventSource::Executor), 1, 0)
            .unwrap();
        assert_eq!(
            table.fold(&ev("a", 60, Some(55), 1, EventSource::Writer), 2, 0),
            Err(Ignored::NotNewer)
        );
        assert_eq!(
            table.shards["a"].sealed,
            Mark {
                epoch: 2,
                value: 50
            }
        );
        assert_eq!(
            table.shards["a"].merged,
            Some(Mark {
                epoch: 2,
                value: 40
            })
        );
    }

    /// Review finding 4: the age clock is set by the oldest pending
    /// generation and does not reset when newer generations arrive.
    #[test]
    fn oldest_pending_does_not_reset_on_newer_flushes() {
        let mut table = TableDemand::default();
        let flush = |g: u64, t: i64| DemandEvent {
            flushed_at_ms: t,
            ..ev("s", g, None, 1, EventSource::Writer)
        };
        table.fold(&flush(1, 1000), 1, 0).unwrap();
        table.fold(&flush(2, 2000), 2, 0).unwrap();
        table.fold(&flush(3, 3000), 3, 0).unwrap();
        assert_eq!(table.oldest_pending_ms(), Some(1000));
        // Merging generation 1 moves the clock to generation 2's time.
        table
            .fold(&ev("s", 3, Some(1), 1, EventSource::Executor), 4, 0)
            .unwrap();
        assert_eq!(table.oldest_pending_ms(), Some(2000));
        table
            .fold(&ev("s", 3, Some(3), 1, EventSource::Executor), 5, 0)
            .unwrap();
        assert_eq!(table.oldest_pending_ms(), None);
        // Independent of order below the cap.
        assert_order_independent(&[
            flush(1, 1000),
            flush(2, 2000),
            flush(3, 3000),
            ev("s", 3, Some(1), 1, EventSource::Executor),
        ]);
    }

    /// Above the cap, pending counts stay exact in every order; the age
    /// clock is a lower bound and may be unknown, never wrong-direction.
    #[test]
    fn beyond_cap_counts_stay_exact_and_age_is_a_bound() {
        let n = (SEALED_TIMES_CAP as u64) + 20;
        let forward: Vec<_> = (1..=n)
            .map(|g| DemandEvent {
                flushed_at_ms: g as i64 * 10,
                ..ev("s", g, None, 1, EventSource::Writer)
            })
            .collect();
        let mut reverse = forward.clone();
        reverse.reverse();
        let f = fold_all(&forward);
        let r = fold_all(&reverse);
        assert_eq!(f.pending_generations(), r.pending_generations());
        assert_eq!(f.pending_generations(), n);
        assert_eq!(f.pending_bytes(), r.pending_bytes());
        assert_eq!(f.oldest_pending_ms(), Some(10));
        assert_eq!(r.oldest_pending_ms(), Some(10));
        assert!(f.shards["s"].sealed_times.len() <= SEALED_TIMES_CAP);
    }

    /// Deterministic pseudo-random streams over two shards, three epochs,
    /// all sources, with and without merged. Small streams: every
    /// permutation. Large streams: forward, reverse and several shuffles.
    /// The exact fields (sealed/merged marks, pending counts, bytes) must
    /// agree; the bounded age field must agree whenever it is exact.
    #[test]
    fn randomized_streams_fold_order_independently() {
        let mut seed: u64 = 0x5eed_1234_abcd_0001;
        let mut next = || {
            seed ^= seed << 13;
            seed ^= seed >> 7;
            seed ^= seed << 17;
            seed
        };
        let gen_event = |r: &mut dyn FnMut() -> u64| {
            let shard = if r().is_multiple_of(2) { "a" } else { "b" };
            let epoch = 1 + r() % 3;
            let sealed = r() % 40;
            let merged = if r().is_multiple_of(3) {
                None
            } else {
                Some(r() % 40)
            };
            let source = match r() % 3 {
                0 => EventSource::Writer,
                1 => EventSource::Executor,
                _ => EventSource::Scan,
            };
            DemandEvent {
                v: 1,
                shard: shard.into(),
                sealed_through: sealed,
                sealed_bytes_through: sealed * 7,
                merged_through: merged,
                flushed_at_ms: (r() % 1000) as i64,
                writer_epoch: epoch,
                source,
            }
        };
        let exact = |t: &TableDemand| {
            t.shards
                .iter()
                .map(|(k, s)| {
                    (
                        k.clone(),
                        s.sealed,
                        s.merged,
                        s.sealed_bytes,
                        s.pending_generations(),
                    )
                })
                .collect::<Vec<_>>()
        };
        for _ in 0..200 {
            let events: Vec<_> = (0..5).map(|_| gen_event(&mut next)).collect();
            let canonical = fold_all(&events);
            for perm in permutations(&events) {
                let folded = fold_all(&perm);
                assert_eq!(exact(&folded), exact(&canonical), "{perm:?}");
                assert_eq!(
                    folded.pending_generations(),
                    canonical.pending_generations()
                );
                assert_eq!(folded.pending_bytes(), canonical.pending_bytes());
                // Below the cap, age is exact too.
                assert_eq!(folded.oldest_pending_ms(), canonical.oldest_pending_ms());
            }
        }
        for _ in 0..50 {
            let events: Vec<_> = (0..200).map(|_| gen_event(&mut next)).collect();
            let canonical = fold_all(&events);
            let mut reversed = events.clone();
            reversed.reverse();
            let mut shuffled = events.clone();
            for i in (1..shuffled.len()).rev() {
                shuffled.swap(i, (next() as usize) % (i + 1));
            }
            for other in [reversed, shuffled] {
                let folded = fold_all(&other);
                assert_eq!(exact(&folded), exact(&canonical));
                assert_eq!(
                    folded.pending_generations(),
                    canonical.pending_generations()
                );
            }
        }
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
        assert_eq!(table.observed_revision, 9);
        assert_eq!(table.updated_ms, 300);
        assert_eq!(
            table.fold(&ev("b", 3, None, 1, EventSource::Writer), 50, 400),
            Err(Ignored::NotNewer)
        );
        assert_eq!(table.observed_revision, 9, "ignored events do not advance");
    }

    /// Review finding 5: default carries the schema version; unknown
    /// versions are rejected rather than silently folded.
    #[test]
    fn schema_version_is_set_and_enforced() {
        assert_eq!(TableDemand::default().v, SCHEMA_VERSION);
        let mut table = TableDemand::default();
        let future = DemandEvent {
            v: 999,
            ..ev("s", 1, None, 1, EventSource::Writer)
        };
        assert_eq!(table.fold(&future, 1, 0), Err(Ignored::UnknownVersion(999)));
        assert!(table.shards.is_empty());
        let legacy = DemandEvent {
            v: 0,
            ..ev("s", 1, None, 1, EventSource::Writer)
        };
        table.fold(&legacy, 1, 0).unwrap();
        let wire = serde_json::to_value(&table).unwrap();
        assert_eq!(wire["v"], SCHEMA_VERSION);
        let back: TableDemand = serde_json::from_value(wire).unwrap();
        assert_eq!(back, table);
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

    /// Review finding 3 at the storage layer: a writer publish that carries
    /// no `merged_through` must keep the stored merged watermark.
    #[tokio::test]
    #[ignore = "requires isolated local ETCD_TEST_ENDPOINTS"]
    async fn writer_publish_preserves_stored_merged() {
        let endpoint = std::env::var("ETCD_TEST_ENDPOINTS").unwrap();
        let client = etcd_client::Client::connect([endpoint], None)
            .await
            .unwrap();
        let coordinator = Coordinator::new(
            client.clone(),
            format!("/demand-preserve/{}", uuid::Uuid::new_v4()),
        );
        assert!(coordinator
            .publish_demand_event("t", &ev("s", 10, Some(8), 1, EventSource::Executor))
            .await
            .unwrap());
        assert!(coordinator
            .publish_demand_event("t", &ev("s", 12, None, 1, EventSource::Writer))
            .await
            .unwrap());
        let key = coordinator.demand_event_key("t", "s");
        let stored: DemandEvent =
            serde_json::from_slice(client.clone().get(key, None).await.unwrap().kvs()[0].value())
                .unwrap();
        assert_eq!(stored.sealed_through, 12);
        assert_eq!(stored.merged_through, Some(8), "merged preserved");
        let mut table = TableDemand::default();
        table.fold(&stored, 1, 0).unwrap();
        assert_eq!(table.pending_generations(), 4);
        // And a later executor publish with a lower sealed but higher
        // merged is accepted on the merged dimension.
        assert!(coordinator
            .publish_demand_event("t", &ev("s", 10, Some(10), 1, EventSource::Executor))
            .await
            .unwrap());
        let key = coordinator.demand_event_key("t", "s");
        let stored: DemandEvent =
            serde_json::from_slice(client.clone().get(key, None).await.unwrap().kvs()[0].value())
                .unwrap();
        assert_eq!(
            (stored.sealed_through, stored.merged_through),
            (12, Some(10))
        );
    }

    #[tokio::test]
    #[ignore = "requires isolated local ETCD_TEST_ENDPOINTS"]
    async fn release_changes_only_advance_merged_and_keep_writer_fields() {
        let endpoint = std::env::var("ETCD_TEST_ENDPOINTS").unwrap();
        let client = etcd_client::Client::connect([endpoint], None)
            .await
            .unwrap();
        let coordinator = Coordinator::new(
            client.clone(),
            format!("/demand-release/{}", uuid::Uuid::new_v4()),
        );
        assert!(coordinator
            .publish_demand_event("t", &ev("s", 20, None, 3, EventSource::Writer))
            .await
            .unwrap());
        let (c, o) = coordinator
            .demand_release_changes(
                "t",
                &[ShardMerged {
                    shard: "s".into(),
                    merged_through: 15,
                }],
            )
            .await
            .unwrap();
        assert_eq!((c.len(), o.len()), (1, 1));
        assert!(coordinator.transact(c, o).await.unwrap());
        let key = coordinator.demand_event_key("t", "s");
        let after: DemandEvent =
            serde_json::from_slice(client.clone().get(key, None).await.unwrap().kvs()[0].value())
                .unwrap();
        assert_eq!(after.merged_through, Some(15));
        assert_eq!(after.sealed_through, 20, "writer's sealed kept");
        assert_eq!(after.writer_epoch, 3, "writer's epoch kept");
        assert_eq!(after.sealed_bytes_through, 20_000, "writer's bytes kept");
        let (c, o) = coordinator
            .demand_release_changes(
                "t",
                &[ShardMerged {
                    shard: "s".into(),
                    merged_through: 12,
                }],
            )
            .await
            .unwrap();
        assert!(c.is_empty() && o.is_empty(), "lower merged emits nothing");
        let (c, o) = coordinator
            .demand_release_changes(
                "t",
                &[ShardMerged {
                    shard: "new".into(),
                    merged_through: 7,
                }],
            )
            .await
            .unwrap();
        assert!(coordinator.transact(c, o).await.unwrap());
        let key = coordinator.demand_event_key("t", "new");
        let fresh: DemandEvent =
            serde_json::from_slice(client.clone().get(key, None).await.unwrap().kvs()[0].value())
                .unwrap();
        assert_eq!((fresh.sealed_through, fresh.merged_through), (7, Some(7)));
        let mut table = TableDemand::default();
        table.fold(&after, 1, 0).unwrap();
        table.fold(&fresh, 2, 0).unwrap();
        assert_eq!(table.pending_generations(), 5);
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
