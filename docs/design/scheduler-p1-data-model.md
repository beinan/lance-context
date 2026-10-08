# Unified scheduler — P1 data model

Status: proposal for the first implementation phase. Parent design:
`unified-maintenance-scheduler.md`; tracking: #333. This document fixes the
etcd records P1 introduces so that the shadow planner, executor heartbeats and
later phases build on one schema. Nothing here changes scheduling behaviour;
P1 only writes and reads these records beside the existing loops.

`P` = `ETCD_PREFIX`. `<hex>` = hex of target bytes. All values are JSON.
Every record carries `v: 1`; readers reject unknown major versions.

## 1. Demand: per-shard watermarks, never deltas

WAL generations are already numbered per shard as `u64` (`FlushedGeneration`
in `store_base.rs`; merged watermarks in `rollout_append::watermarks`). Demand
reuses that numbering so it is idempotent by construction.

### 1.1 Event record — written by writers and executors

```
P/demand-events/<hex>/<shard-uuid>
{
  "v": 1,
  "sealed_through": 1742,        // highest generation sealed (flushed) on this shard
  "sealed_bytes_through": 913_000_000,
  "merged_through": 1690,        // highest generation merged into base, if the writer knows it
  "flushed_at_ms": 1791480000000,
  "writer_epoch": 7,             // the shard writer's epoch; a lower epoch never overwrites a higher one
  "source": "writer" | "executor" | "scan"
}
```

Rules:
* `put` only; no deletes by writers. Each key is overwritten with a strictly
  monotonic `(writer_epoch, sealed_through)`; a writer compares before writing
  and skips if the stored pair is already ≥ its own. Loss or duplication of a
  put cannot corrupt the count because the value is absolute.
* Executors write `merged_through` for the shards they merged **in the same
  txn as the merge-crate `release`**, so demand and ownership agree.
* The stats scan (source `scan`) writes the same record when it observes a
  shard, but may only **lower** `sealed_through` if `writer_epoch` is strictly
  greater than the stored one (a retired shard whose writer is gone). A scan
  with an equal epoch may raise but never lower a watermark: a stale scan must
  not hide a fresh flush (review point 3).

### 1.2 Table record — maintained by the planner

```
P/demand/<hex>
{
  "v": 1,
  "pending_generations": 52,       // Σ_shards max(0, sealed_through − merged_through)
  "pending_bytes": 221_000_000,
  "oldest_pending_ms": 1791470000000,
  "shards": { "<uuid>": { "sealed": 1742, "merged": 1690, "epoch": 7 }, ... },
  "fragment_count": 118, "small_fragment_bytes": 0,   // from scan
  "index_stale": true, "uncovered_fragments": 9,       // from scan / release
  "missing_fragments": false,                          // from repair / failure code
  "observed_revision": 48213,      // etcd revision of the newest folded input
  "updated_ms": 1791480005000
}
```

The planner folds `demand-events/` into this record; it is a cache, not a
source of truth, and may be rebuilt from events + scan at any time. Folding is
a pure function `fold(table, event) -> table` and is tested for commutativity
and idempotency (design invariant 5a).

### 1.3 What writes what in P1

| Source | Writes | Change from today |
|---|---|---|
| Rollout server flush (`sweeper.rs::route_merge`) | `demand-events/<hex>/<shard>` **in addition to** `merge-requests/<hex>` | additive |
| Master append merge release (`rollout_append.rs`, `merge_execution.rs`) | `merged_through` per shard in the release txn | additive |
| Stats scanner (`scanner.rs`) | `demand-events` with `source: scan` for every shard it observes, plus `fragment_count`/`index_stale` into `demand/` | additive; existing enqueue/retire paths untouched |
| Planner (new, leader only) | `demand/<hex>` | new |

No existing key is removed or re-purposed in P1.

## 2. Executors and reservations

### 2.1 Executor heartbeat — leased (`ETCD_LEASE_TTL_SECS`)

```
P/executors/<executor-id>
{
  "v": 1,
  "kind": "resident_master" | "worker" | "catchup_job" | "legacy_rpc",
  "instance": "<pod>/<uid>", "version": "0.6.4",
  "kinds": ["merge_wal", "compact", "index_id", "repair"],
  "slots_total": { "merge_wal": 2, "compact": 1, "index_id": 2, "repair": 2 },
  "bytes_total": 2147483648,
  "bytes_free_sample": 1900000000,   // diagnostic only; never used for placement
  "target_filter": { "owned": [...], "draining": [...], "append_local": [...] },
  "draining": false,
  "heartbeat_ms": 1791480005000
}
```

`executor-id` is stable per process (`admission.rs` already has
`executor_id`). Catch-up Jobs heartbeat for their lifetime and disappear with
their lease.

### 2.2 Assignment — the durable reservation (unleased)

```
P/assignments/<hex>/<unit-id>
{
  "v": 1,
  "unit": { "kind": "merge_wal", "target": "exp-3", "needs_write_turn": true, "preparing": false },
  "executor": "<executor-id>",
  "planner_token": "<leader token>",
  "reserved_slots": { "merge_wal": 1 },
  "reserved_bytes": 268435456,
  "class": 3, "score": 6.5, "reason": "merge_score",
  "created_ms": ..., "bind_deadline_ms": ...,
  "state": "assigned" | "bound" | "released",
  "execution_id": null | "<merge-execution id>"   // set at bind
}
```

Rules (review point 4):
* The planner's view of an executor's headroom is
  `bytes_total − Σ reserved_bytes` and `slots_total − Σ reserved_slots` over
  assignments in state `assigned` or `bound` on that executor — **never**
  `bytes_free_sample`. Unbound assignments count.
* A new leader rebuilds the reservation table from `assignments/` before its
  first cycle; nothing lives only in the old leader's memory.
* An assignment not bound by `bind_deadline_ms` is moved to `released` by the
  planner and its reservation returned; this is a backoff event, not a failure.
* `released` is written in the executor's merge-crate `release` txn (same txn
  as `merged_through`, §1.1), so reservation, ownership and demand change
  together. The planner garbage-collects `released` records after 1 h.
* Executors still enforce `MergeMemoryBudget` locally. The reservation
  prevents over-dispatch; the local budget prevents OOM when an estimate is
  wrong.

### 2.3 P1 scope for executors

P1 writes heartbeats from every master (all are executors regardless of
leadership) and from catch-up Jobs. The planner computes placements and
writes assignments **only in shadow mode**: state `shadow`, never read by any
executor, garbage-collected after 10 min. No executor binds anything in P1.

## 3. Leader

```
P/leader   (leased)   { "v": 1, "token": "<uuid>", "instance": "...", "since_ms": ... }
```

Campaign with `Version(P/leader) == 0`; every planner write compares
`P/leader == own token`. Followers watch `demand-events/`, `demand/`,
`executors/`, `assignments/`, `merge-executions/`, `target-locks/` to keep a
warm cache. The leader's cycle is triggered by those watches plus a 1 s tick.

## 4. Policy (read-only in P1)

```
P/policies/_default  and  P/policies/<hex>
{ "v": 1, "mode": "legacy", ... }     // see design §4.8
```

P1 ships the schema and `GET /api/v1/policies/{target}` returning the
effective merged policy. Nothing consults `mode` yet; it defaults to `legacy`
for every table. The per-table switch to `planner` is P2 and requires every
master in the fleet to run a policy-aware binary (review point 5).

## 5. Observability shipped with P1

* `GET /api/v1/scheduler/demand[?target=]` — `demand/` record, computed
  scores and class, and the planner's current verdict for the table
  (`would_place {executor, reason}` or `would_skip {reason}`), from the
  shadow cycle.
* `GET /api/v1/scheduler/executors` — heartbeats with computed headroom from
  reservations.
* Metrics: `scheduler_demand_pending_generations{target}` (top‑N only),
  `scheduler_shadow_placements_total{kind,executor_kind,reason}`,
  `scheduler_shadow_disagreements_total{kind}` — the planner would have placed
  something the real loops did not within 60 s, or vice versa. This is the
  number that has to go to ~0 before P2.

## 6. Key families added (and none removed)

`demand-events/`, `demand/`, `executors/`, `assignments/`, `leader`,
`policies/`. All are ignored by older binaries. Review point 5 holds: every
existing loop keeps running for every table throughout P1.

## 7. Tests P1 must land with

1. `fold` is idempotent and commutative over any permutation/duplication of
   events (property test, no etcd).
2. A scan with an equal epoch cannot lower `sealed_through`; a higher epoch can.
3. Headroom is computed from assignments, not samples: an executor whose
   `bytes_free_sample` says 2 GiB but with 2 GiB reserved gets no placement.
4. Leader failover rebuilds reservations and reaches the same placement
   decision as the old leader on the same inputs.
5. Shadow mode writes nothing an executor reads (grep‑level test on the
   executor code paths plus an etcd test that binds nothing).
