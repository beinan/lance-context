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
* **Generation numbers are monotonic per shard across writer epochs.** Lance
  keeps `current_generation` and `flushed_generations` when a new writer claims
  a shard (`ShardManifest { writer_epoch: next, ..base }`). The epoch is a
  writer fence and a tiebreak, never a numbering restart. A `merged_through`
  reported under epoch 1 therefore still counts against a `sealed_through`
  flushed under epoch 2 (merged 40, new writer flushes to 60 ⇒ 20 pending).
* `put` only; no deletes by writers. The stored record is the **per‑dimension
  join** of every event published so far: `sealed` and `merged` are each the
  max of `(value, epoch)` across stored and new, taken independently. A writer
  event with `merged_through: None` keeps the executor's merged mark; an
  executor event with an older `sealed_through` still advances `merged`.
  Nothing about one dimension ever lowers or replaces the other. The join is
  idempotent and commutative, so loss, duplication and reordering cannot
  corrupt the count, and a rebuild from stored records equals the in‑memory
  fold.
* Every publisher does read‑join‑CAS with a bounded retry on a lost race, so
  no publisher's progress is lost to another's. Executors publish **after**
  release, one shard per put, detached and bounded; the release transaction
  carries no demand keys (no cross‑key contention with writer flushes, no etcd
  txn‑size limit for many‑shard tables, no storage read on the release path).
* Source is irrelevant to ordering: a lower value from any source simply loses
  the max. A stale scan cannot lower anything. `merged_epoch` records the
  epoch a merged mark was reported under when it differs from the record's
  `writer_epoch`; provenance and tiebreak only.
* `oldest_pending_ms` is the flush time of the lowest unmerged generation. The
  stored record carries `sealed_times` (bounded to `SEALED_TIMES_CAP = 64`
  lowest unmerged generations, min time per generation, pruned as `merged`
  advances) so the age clock **survives a rebuild from etcd**. A newer flush
  adds an entry and never resets the clock. Counts and bytes are exact in every
  order; the age field is exact up to the cap and a lower bound beyond it.
* Records carry `v`. A reader rejects an event it does not understand
  (`Ignored::UnknownVersion`) and a publisher **refuses to overwrite a stored
  record from a newer schema**; `v == 0` is read as version 1.

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
leadership). The planner scores `MergeWal` demand only (design §4.2: classes
Critical / Normal / Tail; class 1 oldest‑first), places against headroom, and
writes assignments **only in shadow mode**: state `shadow`, replaced wholesale
every pass, never read by any executor (a source‑level test enforces this), no
binding. Tails are not placed, matching the real sweeps. Each pass then
compares its placements with whether a real `MergeWal` task is active per
table and increments `scheduler_shadow_disagreements_total{kind}`; the
per‑pass count is `planner_shadow_disagreements_last_pass`. Compaction and
index scoring, commit‑ready class 2 and `max_consecutive_turns` come with P2's
preparation integration; catch‑up Job heartbeats come when Jobs become an
executor kind.

## 3. Leader

```
P/leader         (leased)  { "v": 1, "token": "<uuid>", "instance": "...", "since_ms": ... }
P/leader-token   (leased)  "<uuid>"      // bare token so writes can compare on it
```

Campaign with `Version(P/leader) == 0`, writing both keys under one lease in
one txn. Every planner write txn compares `P/leader-token == own token`, and
every reconcile reads it first even when there is nothing to write, so a
deposed leader notices on its next tick, not on its next change. Keepalive
every `TTL/3`; a failed or expired keepalive ends leadership and the loop
re-campaigns after a short pause. `PLANNER_ENABLED` (default off) gates all of
this; `PLANNER_RECONCILE_SECS` (default 30) sets the full-fold cadence. Followers watch `demand-events/`, `demand/`,
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
6. Class 1 is served oldest‑first: with two class‑1 tables, a low‑score table
   promoted for age is placed before a high‑score critical merge that has
   waited less, given one feasible executor.
7. A late scan snapshot with an equal epoch and a lower `sealed_through`
   leaves the table record unchanged; only a strictly higher epoch may lower
   it (already covered by `scan_may_raise_but_not_lower_without_higher_epoch`
   in #344; keep it).
8. Backpressure shaping never raises the writer's un‑flushed row count or
   delays a flush past the configured interval; under `critical` the
   observable effect is a slower accept rate, not a larger memtable.
