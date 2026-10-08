//! Leader-elected demand planner, shadow mode (P1.5).
//!
//! Spec: `docs/design/scheduler-p1-data-model.md` §1.2, §3. One master holds
//! `P/leader` under a lease and folds every `P/demand-events/<hex>/<shard>`
//! record into a per-table `P/demand/<hex>` cache. The cache is a pure
//! function of the stored events (see `demand::TableDemand::fold`), so it can
//! be rebuilt from scratch by any leader and is never a source of truth.
//!
//! This module **places nothing and removes nothing**. Every existing loop
//! keeps running for every table. Its only outputs are the `demand/` cache,
//! the `/scheduler/demand` view and metrics. Scores, classes and shadow
//! placements come in later PRs once this cache is trusted.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use etcd_client::{Compare, CompareOp, GetOptions, PutOptions, Txn, TxnOp};
use lance_context_core::generate_id;
use lance_context_merge::demand::{DemandEvent, TableDemand};
use serde::{Deserialize, Serialize};

use crate::state::MasterState;

#[derive(Clone, Debug, clap::Args)]
pub struct PlannerConfig {
    /// Run the demand planner (leader election + demand cache). Shadow only:
    /// nothing is scheduled from it yet.
    #[arg(long, env = "PLANNER_ENABLED", default_value_t = false)]
    pub planner_enabled: bool,
    /// Full reconcile interval; between ticks the leader reacts to changes.
    #[arg(long, env = "PLANNER_RECONCILE_SECS", default_value_t = 30)]
    pub planner_reconcile_secs: u64,
}

impl Default for PlannerConfig {
    fn default() -> Self {
        Self {
            planner_enabled: false,
            planner_reconcile_secs: 30,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct LeaderRecord {
    pub v: u32,
    pub token: String,
    pub instance: String,
    pub since_ms: i64,
}

fn now_ms() -> i64 {
    chrono::Utc::now().timestamp_millis()
}

fn hex(target: &str) -> String {
    target.bytes().map(|b| format!("{b:02x}")).collect()
}

pub(crate) fn unhex(encoded: &str) -> Option<String> {
    if !encoded.len().is_multiple_of(2) {
        return None;
    }
    let bytes: Option<Vec<u8>> = (0..encoded.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(&encoded[i..i + 2], 16).ok())
        .collect();
    String::from_utf8(bytes?).ok()
}

pub(crate) struct Keys {
    prefix: String,
}

impl Keys {
    pub(crate) fn new(prefix: &str) -> Self {
        Self {
            prefix: prefix.trim_end_matches('/').to_string(),
        }
    }
    pub(crate) fn leader(&self) -> String {
        format!("{}/leader", self.prefix)
    }
    /// Bare token beside the record so planner writes can compare on it.
    pub(crate) fn leader_token(&self) -> String {
        format!("{}/leader-token", self.prefix)
    }
    pub(crate) fn events_prefix(&self) -> String {
        format!("{}/demand-events/", self.prefix)
    }
    pub(crate) fn demand_prefix(&self) -> String {
        format!("{}/demand/", self.prefix)
    }
    pub(crate) fn demand(&self, target: &str) -> String {
        format!("{}/demand/{}", self.prefix, hex(target))
    }
    /// `demand-events/<hex>/<shard>` → (target, shard).
    fn parse_event_key(&self, key: &str) -> Option<(String, String)> {
        let rest = key.strip_prefix(&self.events_prefix())?;
        let (encoded, shard) = rest.split_once('/')?;
        Some((unhex(encoded)?, shard.to_string()))
    }
}

/// Fold every stored event into per-table records. Pure; the planner's one
/// unit of work. Events whose key or body cannot be read are counted and
/// skipped, never fatal.
pub(crate) fn fold_events<'a>(
    keys: &Keys,
    kvs: impl Iterator<Item = (&'a [u8], &'a [u8], i64)>,
    now: i64,
) -> (BTreeMap<String, TableDemand>, usize) {
    let mut tables: BTreeMap<String, TableDemand> = BTreeMap::new();
    let mut skipped = 0usize;
    for (key, value, revision) in kvs {
        let Some((target, _shard)) =
            keys.parse_event_key(std::str::from_utf8(key).unwrap_or_default())
        else {
            skipped += 1;
            continue;
        };
        let Ok(event) = serde_json::from_slice::<DemandEvent>(value) else {
            skipped += 1;
            continue;
        };
        let table = tables.entry(target).or_default();
        if let Err(lance_context_merge::demand::Ignored::UnknownVersion(_)) =
            table.fold(&event, revision, now)
        {
            skipped += 1;
        }
    }
    (tables, skipped)
}

pub(crate) fn spawn(state: &Arc<MasterState>) {
    if !state.config.planner.planner_enabled {
        return;
    }
    let weak = Arc::downgrade(state);
    tokio::spawn(async move {
        loop {
            let Some(state) = weak.upgrade() else { return };
            match lead(&state).await {
                Ok(_) => {}
                Err(error) => {
                    tracing::warn!(%error, "planner leadership loop ended");
                    metrics::counter!("planner_leader_errors_total").increment(1);
                }
            }
            drop(state);
            tokio::time::sleep(Duration::from_secs(2)).await;
        }
    });
}

/// Campaign for leadership; while leader, run reconcile cycles until the
/// lease is lost or the process drains. Returns `Ok(true)` if this call held
/// leadership at all, `Ok(false)` if the campaign was lost.
async fn lead(state: &Arc<MasterState>) -> Result<bool, String> {
    let keys = Keys::new(&state.config.etcd.etcd_prefix);
    let mut client = state.task_store.etcd_client().clone();
    let ttl = state.config.etcd_lease_ttl_secs.max(5);
    let lease = client
        .lease_grant(ttl, None)
        .await
        .map_err(|e| e.to_string())?
        .id();
    let token = generate_id();
    let record = LeaderRecord {
        v: 1,
        token: token.clone(),
        instance: state.admission.status().executor_id,
        since_ms: now_ms(),
    };
    let won = client
        .txn(
            Txn::new()
                .when(vec![Compare::version(
                    keys.leader().as_str(),
                    CompareOp::Equal,
                    0,
                )])
                .and_then(vec![
                    TxnOp::put(
                        keys.leader(),
                        serde_json::to_vec(&record).unwrap(),
                        Some(PutOptions::new().with_lease(lease)),
                    ),
                    TxnOp::put(
                        keys.leader_token(),
                        token.clone(),
                        Some(PutOptions::new().with_lease(lease)),
                    ),
                ]),
        )
        .await
        .map_err(|e| e.to_string())?
        .succeeded();
    if !won {
        let _ = client.lease_revoke(lease).await;
        metrics::gauge!("planner_is_leader").set(0.0);
        return Ok(false);
    }
    metrics::gauge!("planner_is_leader").set(1.0);
    tracing::info!(token = %token, "planner leadership acquired");
    let (mut keeper, mut stream) = client
        .lease_keep_alive(lease)
        .await
        .map_err(|e| e.to_string())?;
    let keepalive_every = Duration::from_secs((ttl as u64 / 3).max(1));
    let reconcile_every = Duration::from_secs(state.config.planner.planner_reconcile_secs.max(5));
    let mut reconcile = tokio::time::interval(reconcile_every);
    let mut keepalive = tokio::time::interval(keepalive_every);
    let result = loop {
        tokio::select! {
            _ = keepalive.tick() => {
                if keeper.keep_alive().await.is_err() {
                    break Err("lease keepalive failed".to_string());
                }
                match tokio::time::timeout(Duration::from_secs(5), stream.message()).await {
                    Ok(Ok(Some(response))) if response.ttl() > 0 => {}
                    _ => break Err("lease expired or keepalive stream closed".to_string()),
                }
            }
            _ = reconcile.tick() => {
                if !state.admission.status().accepting {
                    break Ok(true);
                }
                if let Err(error) = reconcile_once(state, &keys, &token).await {
                    tracing::warn!(%error, "planner reconcile failed");
                    metrics::counter!("planner_reconcile_errors_total").increment(1);
                }
            }
        }
    };
    metrics::gauge!("planner_is_leader").set(0.0);
    drop(keeper);
    let _ = client.lease_revoke(lease).await;
    tracing::info!(token = %token, ?result, "planner leadership released");
    result
}

/// One reconcile: read every event, fold, write every changed table record
/// under a leader-token compare, delete records for tables with no events.
pub(crate) async fn reconcile_once(
    state: &Arc<MasterState>,
    keys: &Keys,
    leader_token: &str,
) -> Result<usize, String> {
    let started = std::time::Instant::now();
    let mut client = state.task_store.etcd_client().clone();
    let events = client
        .get(keys.events_prefix(), Some(GetOptions::new().with_prefix()))
        .await
        .map_err(|e| e.to_string())?;
    let header_revision = events.header().map_or(0, |h| h.revision());
    let now = now_ms();
    let (tables, skipped) = fold_events(
        keys,
        events
            .kvs()
            .iter()
            .map(|kv| (kv.key(), kv.value(), kv.mod_revision())),
        now,
    );
    if skipped > 0 {
        metrics::counter!("planner_events_skipped_total").increment(skipped as u64);
    }
    let existing = client
        .get(keys.demand_prefix(), Some(GetOptions::new().with_prefix()))
        .await
        .map_err(|e| e.to_string())?;
    let mut existing_by_key: BTreeMap<Vec<u8>, TableDemand> = BTreeMap::new();
    for kv in existing.kvs() {
        if let Ok(record) = serde_json::from_slice::<TableDemand>(kv.value()) {
            existing_by_key.insert(kv.key().to_vec(), record);
        }
    }
    let mut ops = Vec::new();
    let mut written = 0usize;
    let mut total_pending = 0u64;
    let folded: BTreeMap<String, TableDemand> = tables.clone();
    for (target, mut table) in tables {
        total_pending += table.pending_generations();
        let key = keys.demand(&target);
        // Only the fold's inputs decide equality; the clock does not.
        let unchanged = existing_by_key
            .remove(key.as_bytes())
            .is_some_and(|mut old| {
                old.updated_ms = 0;
                let mut cmp = table.clone();
                cmp.updated_ms = 0;
                old == cmp
            });
        if unchanged {
            continue;
        }
        table.updated_ms = now;
        ops.push(TxnOp::put(key, serde_json::to_vec(&table).unwrap(), None));
        written += 1;
    }
    // Tables that no longer have any events: drop the cache row.
    for stale_key in existing_by_key.into_keys() {
        ops.push(TxnOp::delete(stale_key, None));
        written += 1;
    }
    // Always confirm leadership, even with nothing to write, so a deposed
    // leader learns it is deposed on the very next tick rather than when it
    // next has a change.
    let still_leader = client
        .get(keys.leader_token(), None)
        .await
        .map_err(|e| e.to_string())?
        .kvs()
        .first()
        .is_some_and(|kv| kv.value() == leader_token.as_bytes());
    if !still_leader {
        return Err("leadership lost during reconcile".into());
    }
    // etcd caps a txn at 128 ops by default; chunk, each guarded by the
    // leader token so a deposed leader cannot write a stale cache.
    for chunk in ops.chunks(100) {
        let response = client
            .txn(
                Txn::new()
                    .when(vec![Compare::value(
                        keys.leader_token().as_str(),
                        CompareOp::Equal,
                        leader_token,
                    )])
                    .and_then(chunk.to_vec()),
            )
            .await
            .map_err(|e| e.to_string())?;
        if !response.succeeded() {
            return Err("leadership lost during reconcile".into());
        }
    }
    // Executor view: heartbeats plus reservations, then the shadow pass.
    let executors_keys = crate::executors::Keys::new(&state.config.etcd.etcd_prefix);
    match crate::executors::load_headroom(&client, &executors_keys).await {
        Ok(headroom) => {
            metrics::gauge!("planner_executors").set(headroom.len() as f64);
            let reserved: u64 = headroom.values().map(|h| h.bytes_reserved).sum();
            metrics::gauge!("planner_reserved_bytes_total").set(reserved as f64);
            if let Err(error) = shadow_pass(
                state,
                keys,
                &executors_keys,
                leader_token,
                &folded,
                headroom,
                now,
            )
            .await
            {
                tracing::warn!(%error, "planner shadow pass failed");
                metrics::counter!("planner_shadow_errors_total").increment(1);
            }
        }
        Err(error) => {
            tracing::warn!(%error, "planner could not load executor headroom");
        }
    }
    metrics::gauge!("planner_tables").set(existing.kvs().len() as f64);
    metrics::gauge!("planner_pending_generations_total").set(total_pending as f64);
    metrics::histogram!("planner_reconcile_seconds").record(started.elapsed().as_secs_f64());
    metrics::gauge!("planner_last_revision").set(header_revision as f64);
    tracing::debug!(
        written,
        skipped,
        revision = header_revision,
        "planner reconcile"
    );
    Ok(written)
}

/// Shadow placement: score every table, order per design §4.2, place
/// against headroom, and record what *would* be dispatched as `Shadow`
/// assignments that no executor reads. Then compare with reality: for each
/// table the planner would place, is a MergeWal task already active or
/// queued? For each table it would not, is one running anyway? Each
/// mismatch is a disagreement; the counter has to approach zero before P2.
///
/// Shadow assignments are replaced wholesale every pass (they are a view,
/// not a reservation anyone binds), guarded by the leader token.
async fn shadow_pass(
    state: &Arc<MasterState>,
    keys: &Keys,
    executors_keys: &crate::executors::Keys,
    leader_token: &str,
    tables: &BTreeMap<String, TableDemand>,
    mut headroom: BTreeMap<String, crate::executors::Headroom>,
    now: i64,
) -> Result<(), String> {
    use crate::executors::{Assignment, AssignmentState};
    use crate::scoring::{planner_order, score_merge, Class, MergePolicy};
    use lance_context_api::TaskKind;

    let policy = MergePolicy {
        min_generations: state.config.merge_wal_min_generations.max(1) as u64,
        ..MergePolicy::default()
    };
    let mut scored: Vec<_> = tables
        .iter()
        .filter_map(|(target, demand)| score_merge(target, demand, &policy, now))
        .collect();
    scored.sort_by(planner_order);
    for s in &scored {
        metrics::gauge!("planner_merge_score", "target" => s.target.clone()).set(s.score);
    }
    let by_class = |c: Class| scored.iter().filter(|s| s.class == c).count();
    metrics::gauge!("planner_units", "class" => "critical").set(by_class(Class::Critical) as f64);
    metrics::gauge!("planner_units", "class" => "normal").set(by_class(Class::Normal) as f64);
    metrics::gauge!("planner_units", "class" => "tail").set(by_class(Class::Tail) as f64);

    // Only Shadow records from previous passes are replaced; nothing else
    // under assignments/ is touched.
    let mut client = state.task_store.etcd_client().clone();
    let existing = client
        .get(
            executors_keys.assignments_prefix(),
            Some(GetOptions::new().with_prefix()),
        )
        .await
        .map_err(|e| e.to_string())?;
    let mut ops: Vec<TxnOp> = existing
        .kvs()
        .iter()
        .filter(|kv| {
            serde_json::from_slice::<Assignment>(kv.value())
                .is_ok_and(|a| a.state == AssignmentState::Shadow)
        })
        .map(|kv| TxnOp::delete(kv.key().to_vec(), None))
        .collect();
    // Headroom already counts previous Shadow records; release them in the
    // arithmetic too, since they are about to be replaced.
    for h in headroom.values_mut() {
        h.slots_reserved.clear();
        h.bytes_reserved = 0;
        h.assignments = 0;
    }
    for kv in existing.kvs() {
        if let Ok(a) = serde_json::from_slice::<Assignment>(kv.value()) {
            if a.state != AssignmentState::Shadow && a.holds_capacity() {
                if let Some(h) = headroom.get_mut(&a.executor) {
                    for (k, n) in &a.reserved_slots {
                        *h.slots_reserved.entry(*k).or_insert(0) += n;
                    }
                    h.bytes_reserved += a.reserved_bytes;
                    h.assignments += 1;
                }
            }
        }
    }

    let mut would_place: Vec<(String, String)> = Vec::new();
    let mut unplaceable = 0usize;
    for s in &scored {
        if s.class == Class::Tail {
            continue; // the real sweeps do not touch tails either
        }
        // Estimated cost: staging buffers are ~2x decoded bytes, capped at
        // the per-pass byte limit the append path enforces.
        let expected_bytes = (s.pending_bytes.saturating_mul(2))
            .min(state.config.append.rollout_append_max_bytes as u64 * 2)
            .max(1);
        let pick = headroom
            .values_mut()
            .filter(|h| h.fits(TaskKind::MergeWal, expected_bytes))
            .max_by_key(|h| h.bytes_free());
        let Some(h) = pick else {
            unplaceable += 1;
            continue;
        };
        *h.slots_reserved.entry(TaskKind::MergeWal).or_insert(0) += 1;
        h.bytes_reserved += expected_bytes;
        h.assignments += 1;
        let unit_id = generate_id();
        let a = Assignment {
            v: crate::executors::SCHEMA_VERSION,
            unit_id: unit_id.clone(),
            kind: TaskKind::MergeWal,
            target: s.target.clone(),
            needs_write_turn: true,
            executor: h.executor.clone(),
            planner_token: leader_token.to_string(),
            reserved_slots: [(TaskKind::MergeWal, 1)].into_iter().collect(),
            reserved_bytes: expected_bytes,
            state: AssignmentState::Shadow,
            created_ms: now,
            bind_deadline_ms: now + 30_000,
        };
        ops.push(TxnOp::put(
            executors_keys.assignment(&s.target, &unit_id),
            serde_json::to_vec(&a).unwrap(),
            None,
        ));
        would_place.push((s.target.clone(), h.executor.clone()));
    }
    metrics::gauge!("planner_shadow_placements").set(would_place.len() as f64);
    metrics::gauge!("planner_shadow_unplaceable").set(unplaceable as f64);

    for chunk in ops.chunks(100) {
        let response = client
            .txn(
                Txn::new()
                    .when(vec![Compare::value(
                        keys.leader_token().as_str(),
                        CompareOp::Equal,
                        leader_token,
                    )])
                    .and_then(chunk.to_vec()),
            )
            .await
            .map_err(|e| e.to_string())?;
        if !response.succeeded() {
            return Err("leadership lost during shadow pass".into());
        }
    }

    // Disagreement: planner says place vs. reality has no task; or planner
    // says nothing (no demand scored, or tail) vs. reality is running one.
    let placed: std::collections::BTreeSet<&str> =
        would_place.iter().map(|(t, _)| t.as_str()).collect();
    let mut disagreements = 0u64;
    for s in &scored {
        let active = state
            .task_store
            .get_active_id(TaskKind::MergeWal, &s.target)
            .await
            .map_err(|e| e.to_string())?
            .is_some();
        let planner_wants = placed.contains(s.target.as_str());
        if planner_wants != active {
            disagreements += 1;
            tracing::debug!(
                target = %s.target, class = ?s.class, score = s.score,
                planner_wants, reality_active = active,
                "planner shadow disagreement"
            );
        }
    }
    metrics::counter!("scheduler_shadow_disagreements_total", "kind" => "merge_wal")
        .increment(disagreements);
    metrics::gauge!("planner_shadow_disagreements_last_pass").set(disagreements as f64);
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use lance_context_merge::demand::{EventSource, SCHEMA_VERSION};

    fn event(shard: &str, sealed: u64, merged: Option<u64>) -> Vec<u8> {
        serde_json::to_vec(&DemandEvent {
            v: SCHEMA_VERSION,
            shard: shard.into(),
            sealed_through: sealed,
            sealed_bytes_through: 0,
            merged_through: merged,
            flushed_at_ms: 1,
            writer_epoch: 1,
            source: EventSource::Writer,
            merged_epoch: None,
            sealed_times: Default::default(),
        })
        .unwrap()
    }

    #[test]
    fn fold_events_groups_by_table_and_skips_garbage() {
        let keys = Keys::new("/p");
        let a1 = format!("/p/demand-events/{}/s1", hex("alpha"));
        let a2 = format!("/p/demand-events/{}/s2", hex("alpha"));
        let b1 = format!("/p/demand-events/{}/s1", hex("generic:beta"));
        let bad_key = "/p/demand-events/zz/s1".to_string();
        let bad_body = format!("/p/demand-events/{}/s9", hex("alpha"));
        let future = serde_json::json!({"v": 999, "shard": "s", "sealed_through": 1,
            "flushed_at_ms": 0, "writer_epoch": 1, "source": "writer"})
        .to_string();
        let future_key = format!("/p/demand-events/{}/s1", hex("gamma"));
        let kvs: Vec<(Vec<u8>, Vec<u8>, i64)> = vec![
            (a1.into_bytes(), event("s1", 10, Some(4)), 1),
            (a2.into_bytes(), event("s2", 3, None), 2),
            (b1.into_bytes(), event("s1", 7, Some(7)), 3),
            (bad_key.into_bytes(), event("s1", 1, None), 4),
            (bad_body.into_bytes(), b"not json".to_vec(), 5),
            (future_key.into_bytes(), future.into_bytes(), 6),
        ];
        let (tables, skipped) = fold_events(
            &keys,
            kvs.iter().map(|(k, v, r)| (k.as_slice(), v.as_slice(), *r)),
            0,
        );
        assert_eq!(skipped, 3, "bad key, bad body, future version");
        assert_eq!(tables.len(), 3, "alpha, generic:beta, gamma (empty)");
        assert_eq!(tables["alpha"].pending_generations(), 6 + 3);
        assert_eq!(tables["alpha"].shards.len(), 2);
        assert_eq!(tables["generic:beta"].pending_generations(), 0);
        assert_eq!(tables["alpha"].observed_revision, 2);
        assert!(tables["gamma"].shards.is_empty());
    }

    #[test]
    fn hex_round_trips_and_rejects_odd_input() {
        for s in ["alpha", "generic:beta", "", "ünïcödé"] {
            assert_eq!(unhex(&hex(s)).as_deref(), Some(s));
        }
        assert_eq!(unhex("abc"), None);
        assert_eq!(unhex("zz"), None);
    }

    #[tokio::test]
    #[ignore = "requires ETCD_TEST_ENDPOINTS"]
    async fn single_leader_builds_a_cache_equal_to_a_direct_fold_and_deposed_leader_cannot_write() {
        use crate::state::MasterState;
        use clap::Parser;
        let dir = tempfile::tempdir().unwrap();
        let mut cfg = crate::config::MasterConfig::parse_from([
            "test",
            "--data-dir",
            dir.path().to_str().unwrap(),
        ]);
        cfg.etcd.etcd_endpoints = std::env::var("ETCD_TEST_ENDPOINTS")
            .unwrap()
            .split(',')
            .map(str::to_owned)
            .collect();
        cfg.etcd.etcd_prefix = format!("/planner-test/{}", generate_id());
        cfg.planner.planner_enabled = true;
        cfg.planner.planner_reconcile_secs = 1;
        cfg.stats_scan_interval_secs = 0;
        cfg.compaction_interval_secs = 0;
        cfg.merge_wal_interval_secs = 0;
        let first = MasterState::new(cfg.clone()).await.unwrap();
        let second = MasterState::new(cfg.clone()).await.unwrap();
        let keys = Keys::new(&cfg.etcd.etcd_prefix);

        // Two tables, one with two shards, published through the real path.
        let coordinator = first.task_store.merge_coordinator();
        for (target, shard, sealed, merged) in [
            ("hot", "a", 40u64, Some(10u64)),
            ("hot", "b", 12, None),
            ("cold", "a", 3, Some(3)),
        ] {
            coordinator
                .publish_demand_event(
                    target,
                    &DemandEvent {
                        v: SCHEMA_VERSION,
                        shard: shard.into(),
                        sealed_through: sealed,
                        sealed_bytes_through: sealed * 100,
                        merged_through: merged,
                        flushed_at_ms: 1000,
                        writer_epoch: 1,
                        source: EventSource::Writer,
                        merged_epoch: None,
                        sealed_times: Default::default(),
                    },
                )
                .await
                .unwrap();
        }

        // Both campaign; exactly one wins. The loser returns Ok(false)
        // promptly; the winner keeps leading until aborted.
        let a = tokio::spawn({
            let first = first.clone();
            async move { lead(&first).await }
        });
        let b = tokio::spawn({
            let second = second.clone();
            async move { lead(&second).await }
        });
        let mut client = first.task_store.etcd_client().clone();
        let deadline = std::time::Instant::now() + Duration::from_secs(10);
        let cache = loop {
            let got = client
                .get(keys.demand_prefix(), Some(GetOptions::new().with_prefix()))
                .await
                .unwrap();
            if got.kvs().len() == 2 {
                break got;
            }
            assert!(
                std::time::Instant::now() < deadline,
                "planner never wrote the cache"
            );
            tokio::time::sleep(Duration::from_millis(100)).await;
        };
        let leader_kv = client.get(keys.leader(), None).await.unwrap();
        assert_eq!(leader_kv.kvs().len(), 1, "exactly one leader");
        let leader: LeaderRecord = serde_json::from_slice(leader_kv.kvs()[0].value()).unwrap();

        // Cache equals a direct fold of the events.
        let events = client
            .get(keys.events_prefix(), Some(GetOptions::new().with_prefix()))
            .await
            .unwrap();
        let (direct, skipped) = fold_events(
            &keys,
            events
                .kvs()
                .iter()
                .map(|kv| (kv.key(), kv.value(), kv.mod_revision())),
            0,
        );
        assert_eq!(skipped, 0);
        for kv in cache.kvs() {
            let mut stored: TableDemand = serde_json::from_slice(kv.value()).unwrap();
            let target = unhex(
                String::from_utf8_lossy(kv.key())
                    .strip_prefix(&keys.demand_prefix())
                    .unwrap(),
            )
            .unwrap();
            stored.updated_ms = 0;
            let mut expect = direct[&target].clone();
            expect.updated_ms = 0;
            assert_eq!(stored, expect, "{target}");
        }
        let hot: TableDemand = serde_json::from_slice(
            client.get(keys.demand("hot"), None).await.unwrap().kvs()[0].value(),
        )
        .unwrap();
        assert_eq!(hot.pending_generations(), 30 + 12);

        // Via the HTTP view.
        let axum::Json(report) = crate::routes::scheduler_demand(
            axum::extract::State(first.clone()),
            axum::extract::Query(crate::routes::DemandQuery {
                target: Some("hot".into()),
            }),
        )
        .await
        .unwrap();
        assert_eq!(
            report.leader.as_ref().map(|l| &l.token),
            Some(&leader.token)
        );
        assert_eq!(report.tables.len(), 1);
        assert_eq!(report.tables[0].pending_generations, 42);
        assert_eq!(report.tables[0].shards, 2);

        // Shadow placement: with this master's heartbeat live, the hot table
        // (42 pending >= 8) gets exactly one Shadow assignment against it;
        // cold (0 pending) gets none. No real MergeWal task exists, so the
        // pass records one disagreement for hot.
        let hb_runner = tokio::spawn({
            let first = first.clone();
            async move { crate::executors::heartbeat_loop_for_test(&first).await }
        });
        let ekeys = crate::executors::Keys::new(&cfg.etcd.etcd_prefix);
        let deadline = std::time::Instant::now() + Duration::from_secs(15);
        let shadows: Vec<crate::executors::Assignment> = loop {
            let got = client
                .get(
                    ekeys.assignments_prefix(),
                    Some(GetOptions::new().with_prefix()),
                )
                .await
                .unwrap();
            let v: Vec<crate::executors::Assignment> = got
                .kvs()
                .iter()
                .filter_map(|kv| serde_json::from_slice(kv.value()).ok())
                .collect();
            if !v.is_empty() {
                break v;
            }
            assert!(
                std::time::Instant::now() < deadline,
                "no shadow assignment written"
            );
            tokio::time::sleep(Duration::from_millis(100)).await;
        };
        assert_eq!(shadows.len(), 1, "{shadows:?}");
        assert_eq!(shadows[0].target, "hot");
        assert_eq!(shadows[0].state, crate::executors::AssignmentState::Shadow);
        assert_eq!(shadows[0].kind, lance_context_api::TaskKind::MergeWal);
        assert_eq!(shadows[0].executor, first.admission.status().executor_id);
        assert!(shadows[0].reserved_bytes > 0);
        // Headroom reflects the shadow reservation.
        let h = crate::executors::load_headroom(&client, &ekeys)
            .await
            .unwrap();
        assert_eq!(h[&shadows[0].executor].assignments, 1);
        // Enqueue a real MergeWal task for hot: on the next pass the planner
        // and reality agree, and the shadow row is replaced, not duplicated.
        first
            .task_store
            .enqueue(lance_context_api::TaskKind::MergeWal, "hot", vec![])
            .await
            .unwrap();
        tokio::time::sleep(Duration::from_millis(1500)).await;
        let got = client
            .get(
                ekeys.assignments_prefix(),
                Some(GetOptions::new().with_prefix()),
            )
            .await
            .unwrap();
        assert_eq!(
            got.kvs().len(),
            1,
            "shadow rows are replaced, never accumulated"
        );
        hb_runner.abort();

        // A deposed leader's reconcile is refused: forge a different token.
        let err = reconcile_once(&first, &keys, "not-the-leader")
            .await
            .unwrap_err();
        assert!(err.contains("leadership lost"), "{err}");

        // Dropping the cold table's events removes its cache row on the next
        // reconcile; hot stays.
        client
            .delete(
                format!("{}{}/", keys.events_prefix(), hex("cold")),
                Some(etcd_client::DeleteOptions::new().with_prefix()),
            )
            .await
            .unwrap();
        let deadline = std::time::Instant::now() + Duration::from_secs(10);
        loop {
            let got = client
                .get(keys.demand_prefix(), Some(GetOptions::new().with_prefix()))
                .await
                .unwrap();
            if got.kvs().len() == 1 {
                break;
            }
            assert!(
                std::time::Instant::now() < deadline,
                "stale cache row not dropped"
            );
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        // Exactly one campaigner lost (finished with Ok(false)); the other is
        // still leading. If both led, both would still be running.
        tokio::time::sleep(Duration::from_millis(200)).await;
        let finished = [a.is_finished(), b.is_finished()];
        assert_eq!(
            finished.iter().filter(|f| **f).count(),
            1,
            "exactly one campaigner must have lost and returned (finished={finished:?})"
        );
        let (loser, winner) = if finished[0] { (a, b) } else { (b, a) };
        assert_eq!(loser.await.unwrap(), Ok(false));
        winner.abort();
    }
}
