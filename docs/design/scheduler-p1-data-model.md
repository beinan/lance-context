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
* `put` only; no deletes by writers. The stored record is the **per‑dimension
  join** of every event published so far: `sealed` and `merged` are each the
  max of `(writer_epoch, value)` across stored and new, taken independently.
  A writer event with `merged_through: None` therefore keeps the executor's
  merged mark, and an executor event with an older `sealed_through` still
  advances `merged`. Nothing about one dimension ever lowers or replaces the
  other. Loss or duplication of a put cannot corrupt the count because every
  value is absolute and the join is idempotent and commutative.
* Folding (`TableDemand::fold`) applies the same join. `pending` for a shard is
  `sealed − merged` only when both marks are at the same epoch; a `merged`
  from an older epoch says nothing about the current epoch's generations
  (all pending), and a `merged` from a newer epoch means the shard was
  replaced (none pending).
* `oldest_pending_ms` is the flush time of the lowest unmerged generation,
  kept in a bounded per‑shard map (`SEALED_TIMES_CAP = 64`) that is pruned as
  `merged` advances. A newer flush adds an entry and never resets the age
  clock. Pending counts and bytes are exact in every order; the age field is
  exact up to the cap and a lower bound beyond it.
* Executors write `merged_through` for the shards they merged **in the same
  txn as the merge-crate `release`**, so demand and ownership agree.
* The stats scan (source `scan`) writes the same record when it observes a
  shard. Source is irrelevant to ordering: writer, executor and scan all report
  absolute watermarks and the per‑dimension max wins, so a stale scan cannot
  lower anything — a lower value simply loses the max. etcd revisions and
  manifest versions are different clocks and are never compared.
  `observed_revision` on the table record is bookkeeping for "how fresh is
  this cache", not an ordering input.
* Records carry `v`; `TableDemand::default()` sets it, a reader rejects any
  version it does not understand (`Ignored::UnknownVersion`), and `v == 0` is
  read as version 1.

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
   events (property test, no etcd): every permutation of small streams and
   forward/reverse/shuffled large randomized streams across shards, epochs,
   sources and missing `merged` agree on sealed/merged marks, pending counts
   and bytes; the age field agrees below the cap. Includes the review
   counterexamples (old‑epoch merge beside new‑epoch flush; scan with older
   sealed and newer merged) and "writer event never forgets merged".
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
