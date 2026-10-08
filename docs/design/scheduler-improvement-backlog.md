# Scheduler Improvement Backlog

Companion to `unified-maintenance-scheduler.md`. Tracking issue: #333. This document lists what is wrong with the
current maintenance scheduling layer, grouped by severity, with file references, and the
order in which to fix it. Evidence comes from the code inventory (Oct 2026, `main` at
`6e513ce`) and from PRs #280–#331.

## 0. The headline numbers

| Metric | Value |
|---|---|
| Independent scheduling/discovery loops | 11 |
| Distinct pacing mechanisms | 4 (tokio `interval`, `sleep`, etcd‑persisted `next_batch_ms` cursors, K8s Job lifecycle) |
| Failure/backoff systems | 5 (task cooldown, merge‑failures ledger, catch‑up record backoff, compact‑noops, repair chaining) |
| Admission surfaces | 3 (tokio semaphores ×5, `Admission` drain gate, etcd ownership predicates ×7 keys) |
| etcd key families under the prefix | 27 |
| Scheduling‑related env knobs | ≈71 |
| Per‑table allowlists | 6 (`MERGE_OWNED_TARGETS`, `MERGE_DRAIN_TARGETS`, `ROLLOUT_APPEND_TARGETS`, `COMPACTION_PREPARE_TARGETS`, `INDEX_PREPARE_TARGETS`, `MAINTENANCE_CATCHUP_TARGETS`, + `CATCHUP_CONTINUOUS_TARGETS`) |
| Largest files | `task_store.rs` 3557, `scheduler.rs` 3251, `core/rollout_append.rs` 2283 lines |
| Merged PRs 2026‑09‑30 → 10‑08 that fix this layer | ~35 of 50 |
| Tests requiring real etcd | 137 of 647; most scheduling logic has *no* etcd‑free unit test |

## 1. Architectural defects (cause of the churn)

### A1. No single owner of "what runs next"
Six producers enqueue into `P/queue/` (L3, L4, L5, L7, L8→L3, `schedule_repair`, HTTP
routes, dedicated executor), each with its own eligibility rules. Ordering of the queue is
by random id (`generate_id()`, `task_store.rs:1885`), so despite the docs it is **not FIFO**
and has no priority. The only priority signals are stats‑desc ordering at enqueue time (lost
once in the queue), the MergeWal→commit‑ready yield (#324), and catch‑up rotation.
→ Design §4.1–4.5.

### A2. Capacity modelled as semaphores, not resources
Four semaphores + `compaction_permits` (`scheduler.rs:1107‑1137`, `state.rs:240`) know
nothing about memory; memory is a separate `MergeMemoryBudget` enforced inside the task.
PR #331 adds a fifth pool as a workaround for legacy RPCs holding merge slots; the depth
gauge ignores it (`report_depth=false`). `MERGE_WAL_CONCURRENCY=N` now means N or N+1.
→ Design §4.4 (slots per kind + bytes per process, published via heartbeat).

### A3. Admission does a 7‑key etcd snapshot per candidate per claim
`claim_next` (`task_store.rs:900‑916`) reads merge‑execution, target‑lock, catchup‑active,
merge‑claim, claim, compact‑preparation, compact‑commit‑ready for every candidate, after a
full `recover_orphaned` page scan of `P/running/` on every call (`:1226‑1260`). #307 already
had to add a bypass for dedicated executors ("avoid fleet scans during dedicated merge
admission"). Cost grows with queue × running.
→ Design §4.5: one cached watch on fence keys, one‑key reserve txn.

### A4. Discovery duplicated five ways
Merge sweep (600 s, stats), wal‑tail sweep (30 s, stats + etcd cursor), resident discovery
(5 s, manifest probe + etcd cursor), catch‑up admission (30 s, stats), demand loop (15 s,
`merge-requests`). Each has its own freshness threshold (`MERGE_WAL_TAIL_STATS_MAX_AGE_SECS`,
`CATCHUP_STATS_MAX_AGE_SECS`, `STATS_SCAN_INTERVAL_SECS`, `ROLLOUT_APPEND_RECONCILE_INTERVAL_SECS`)
and its own cursor key. Two masters with slightly different config get different
`scope()` hashes in `resident_recovery.rs:109` and run independent cursors.
→ Design §4.1: one demand table fed by push + one reconcile scan.

### A5. Dependencies as task edges instead of demand state
`Compact → IndexId` and `Repair → kind` are task DAG edges requiring `dependency_status`
GETs per candidate and `fail_dependency` CAS (`task_store.rs:1460‑1498`). Dependency‑
bearing tasks are exempt from dedupe (`:1789‑1798`), so duplicates are possible.
→ Design §4.3: post‑conditions update demand (`index_stale`, `missing_fragments`).

### A6. Stats scanner mutates tables directly
`scanner.rs:774‑878`: cold‑retirement runs `cleanup_own_shard` and compaction *inside the
scanner*, holding `compaction_permits` but bypassing the queue, dedupe, cooldown and the
failure ledger.
→ Design §4.3 `Retire` kind.

### A7. Per‑table behaviour via six global allowlists
Whether a table is owned/draining/append‑capable/prepare‑enabled/catchup‑shared/continuous
is the intersection of six env lists parsed at startup; #326 has to reject conflicting
combinations at boot. No API returns the effective mode for a table.
→ Design §4.8 policy document + `GET /scheduler/demand?target=`.

## 2. Correctness / robustness defects

### C1. Error classification by message text (#328)
`merge/failure.rs` strips `"invalid user input: "` and matches on
`"index metadata changed; reprepare, crates/lance-context-core/src/store_base.rs:"`. A line
renumber or a Lance error‑format change silently reverts to a 1‑hour permanent cooldown —
exactly the bug #328 fixed. **Fix**: typed `enum MaintenanceError` in core with stable codes;
classify by code. Also required by the unified ledger (Design §4.6).

### C2. Retry storm across #327 + #330 + #328
Index preparation has no fine‑grained progress (Lance 9 trainers ignore callbacks,
#327 message); the idle watchdog (600 s) cancels it; the result is classified `Retryable`
and retried with 2 s backoff for the first 3 attempts. No test covers a large index build
that is slow‑but‑alive. **Fix**: treat "IO bytes completed" as progress for preparation
units (already partially done), and add an e2e test; longer term, Design §4.6 `Deadline`
class with per‑kind thresholds.

### C3. Starvation accepted as expected behaviour (#331)
The regression test asserts `legacy-queued` remains `Queued` indefinitely while legacy RPCs
hold the pool. The real fault — legacy worker RPCs have no bound on slot hold time (#316/#318
fixed body/response stalls, not total duration) — is unaddressed. **Fix**: legacy RPC gets
its own executor kind with its own slots (Design §4.4), not a side door for resident tasks.

### C4. Fairness hint without version or metric (#324)
`compact-commit-ready` is honoured only by upgraded binaries; nothing records
`hint_present && ignored`. **Fix**: add `scheduler_turn_wait_seconds{kind}` and a counter
for commit‑ready requests served vs. expired; include `protocol` in the hint value.

### C5. Progress endpoint pushes consistency onto the client (#330)
`/merge-progress` does four reads and returns `progress_changed_retry` /
`execution_changed_retry` when they disagree. **Fix**: one range read at a single etcd
revision (`WithRevision`) or a txn; return a typed struct, not `serde_json::json!`.

### C6. Random‑id queue order makes "oldest first" impossible
Any starvation fix today must scan the whole queue. **Fix**: Design §4.2 scoring; interim:
prefix queue keys with `{class:1}{enqueue_ms:013}` so lexicographic order is meaningful.

### C7a. Backpressure is a cliff, not a slope
The only write‑side signal today is read‑side: 503 at 4096 pending generations, warn at
256. Every mature LSM (RocksDB write stall, TiKV `soft-pending-compaction-bytes-limit`,
ClickHouse `parts_to_delay_insert`, Pebble L0 sublevels) throttles *proportionally* before
rejecting. **Fix**: Design §4.7 `backlog_class` driving flush‑interval stretch /
generation coalescing in the rollout server.

### C7. Assignments without executor liveness
Today a claim is leased by the *claimer*, but nothing verifies the claimer can actually run
the work (memory, kinds) before the claim txn; failures surface as cancelled executions and
ledger entries. **Fix**: Design §4.4 heartbeat with capacity; §6 invariant 2.

## 3. Operability defects

### O1. Configuration surface
≈71 knobs; 20 of them `CATCHUP_*`. Target ≤ 12 global + per‑table policy (Design §4.8).

### O2. Docs are per‑PR, not per‑system
`docs/` has 11 topic files (`master-catchup`, `wal-tail-sweep`, `resident-wal-recovery`,
`rollout-parallel-append`, `merge-task-admission`, `merge-recovery`,
`compaction-commit-fairness`, `merge-work-progress`, `owned-maintenance-recovery`,
`concurrent-compaction`, `concurrent-indexing`) and no overview of the WAL→merge lifecycle
or an operator runbook ("backlog is growing — which endpoint do I look at first?").
**Fix**: `docs/design/unified-maintenance-scheduler.md` + one runbook; fold topic docs into
sections or delete after migration.

### O3. No "why not now" surface
Operators cannot ask why table X has pending WAL but no running task. Every `continue` in
`claim_next` is silent. **Fix**: Design §4.9 reason codes.

### O4. Commit messages are acceptance reports
Recent messages list CI runs and test counts but not alternatives considered or trade‑offs.
Adopt: Problem / Approach / Alternatives rejected / Risks / Validation.

### O5. Review bandwidth
15 PRs in 4 days from one author into 3 k‑line files, no second reviewer visible. With the
planner extracted as pure functions (§4 P0.1), reviews become tractable.

## 4. Code‑health defects

* `task_store.rs` mixes etcd queue, dependency graph, preparation protocol, commit‑turn
  fairness and resident filtering — split into `queue.rs`, `fence.rs`, `eligibility.rs`.
* `claim_next` takes three orthogonal optional parameters (`kinds`, `only_id`,
  `resident_targets`) and re‑derives `preparing` with a copy of the same predicate.
* Hard‑coded constants with no metric: `BATCH=4`, 25 s tick timeout, 5 s sleep, 10 s probe
  (`resident_recovery.rs`), `MAX_SWEEP_ENQUEUE=256`, `TASK_POLL_BATCH=256`, 500 ms poller
  sleep, 2 s/1 s watchdogs.
* #327 bundles a `Cargo.lock` dependency bump (110 lines) with a 1 k‑line feature.
* Scheduling logic tested only via etcd integration tests; eligibility and scoring have no
  property or table‑driven unit tests.

## 5. Ordered plan

### P0 — No behaviour change (1–2 weeks)
1. Extract `eligibility(task, fence_snapshot, config) -> Decision{Run|Skip(reason)}` and
   unit‑test it; `claim_next` calls it. Log skip reasons at debug.
2. Typed `MaintenanceError` with codes; `failure.rs` classifies by code (fixes C1).
3. Add metrics: `schedule_to_bind_seconds`, `turn_wait_seconds{kind}`, commit‑ready
   served/expired, per‑pool depth incl. resident pool (fixes C4, half of O3).
4. Slow‑but‑alive index build e2e test (C2).
5. `/merge-progress` single‑revision read + typed response (C5).
6. Write the operator runbook (O2).

### P1 — Demand table + shadow planner (2–3 weeks)
7. `P/demand/` records; stats scanner writes demand only; rollout server flush signal
   generalised to `demand-events`.
8. Leader election; planner computes scores and *logs* placements it would make; compare
   against actual queue behaviour for a week. Ship `GET /scheduler/demand`.
9. Executor heartbeats with (slots, bytes).

### P2 — Planner owns MergeWal (2–3 weeks)
10. Per‑table policy document; `mode: active` tables are placed by the planner; L3/L5/L7/L8
    skip those tables. Legacy workers become an executor kind (C3).
11. Delete resident‑only pool (#331), wal‑tail sweep, resident discovery for migrated tables.

### P3 — Compaction / Index / Repair / Retire (2 weeks)
12. Preparation‑outside‑turn becomes default; `compact-commit-ready` becomes a class‑2
    demand; dependency edges replaced by demand post‑conditions (A5); retirement is a kind (A6).
13. Catch‑up Jobs become an executor type; delete `catchup-slots`, `catchup-records`,
    `catchup-active`, controller admission.

### P4 — Cleanup
14. Delete: 4 pool pollers, `claim_next` multi‑key snapshot, 5 cursor/sweep loops, six
    allowlists, ~55 env knobs, 15 etcd key families (`queue`, `running`, `claims`,
    `dedupe`, `cooldown`, `repairs`, `compact-preparations`, `compact-commit-ready`,
    `compact-noops`, `merge-claims`, `merge-requests`, `catchup-*` ×4,
    `wal-tail-cursor`, `resident-wal-discovery*`). Split `task_store.rs`.

### Mixed‑version rules for the whole migration
* Old masters ignore unknown key families and continue to honour `target-locks` /
  `merge-executions`; the planner treats a legacy `claims/` + `target-locks` pair as
  `Holding‑turn`.
* Per‑table `mode` gates which code path acts, so rollout is per table, never fleet‑wide.
* No phase deletes a key family until no binary in the fleet reads it.

## 6. Open questions to answer before P1

1. Actual table count and hot‑table fraction in production (sets `max_placements_per_cycle`
   and whether a 1 s cycle is even needed).
2. Real generation size distribution (bytes/rows) — drives `expected_bytes` estimates.
3. p99 compaction and index‑build duration on the largest tables — drives `Deadline`
   thresholds and whether K8s Jobs remain necessary at all.
4. Whether the rollout server can be asked to coalesce generations under `critical`
   backlog (Design §4.7), or whether backpressure must stay read‑side only.
