# Unified Maintenance Scheduler — Design

Status: proposal. Scope: `lance-context-master` + `lance-context-merge`. Companion
document: `scheduler-improvement-backlog.md` (what is wrong today and the migration order).

## 1. Problem statement

The master runs maintenance for N Lance tables (rollout + generic stores): WAL merge,
compaction, key-index build, repair, cold retirement. Today this is done by **eleven
independent loops** feeding **one etcd task queue**, each with its own pacing, filtering,
backoff and admission rules (inventory in §Appendix A). The result is that nobody can state,
for a given table, *which component will act next, on which replica, and why*. ~35 of the
50 PRs merged between 2026‑09‑30 and 2026‑10‑08 were fixes to this layer.

This document proposes one scheduling model to replace those loops. It deliberately does
**not** touch the merge-crate execution/fencing protocol (`merge-executions`,
`target-locks`, `merge-commit-permits`, version barriers, progress watchdog). That protocol
is the correctness core and has been hardened; the scheduler sits above it and only decides
*what* to run *where* and *when*.

## 2. Workload characteristics that drive the design

Numbers from `docs/*.md`, config defaults and benchmark notes; unknowns are called out.

| Dimension | Value | Consequence |
|---|---|---|
| Tables | tens → low hundreds active, long cold tail (7‑day retire) | A single planner can hold the entire demand set in memory; no need for distributed scheduling. |
| Writers | thousands of generation workers → 1–N shards/table, 1 generation / shard / 30 s on hot tables | Demand is continuous for hot tables, bursty-then-idle for most. Hot/cold is bimodal. |
| Backlog | 10³–10⁴ pending generations per shard has happened; reads 503 at 4096 | Backlog depth is the primary SLO signal; must be priority‑ordered, not FIFO. |
| Task duration | MergeWal pass 1–3 s/64 MiB but slices run minutes–hours; compaction/index "many minutes" with opaque progress | Tasks are long and few; scheduling latency of ~1 s is irrelevant; **placement and memory** are what matter. |
| Memory | master pod 2 GiB limit, append budget 2 GiB, worker 20 GiB/6 slots, catch‑up Job 8 GiB | Capacity must be modelled as (slots, bytes) per executor, not a single semaphore. |
| Replicas | 3–6 masters, 4 K8s catch‑up Jobs, workers as StatefulSet | Conflict rate between replicas is low; optimistic multi‑scheduler (Omega‑style) buys nothing and costs the 7‑key claim txn. |
| Coordination | etcd only, 30 s leases | Watches + leases are cheap; full prefix scans per claim are not. |
| Correctness | exactly one publisher per table; progress‑based liveness; mixed‑version rollout | Keep the fence; schedule by table *write turn*, not by task. |
| Fairness | compaction starves behind continuous merge; low‑count tails never merge; alphabet‑late tables hidden | Need aging/round‑robin at the *planner*, not ad‑hoc cursors in every loop. |

The closest analogues are therefore **not** cluster schedulers (Borg/K8s/Omega scale) but
**storage‑engine background schedulers over many tables**: RocksDB column families,
ClickHouse MergeTree tables, Hudi table services. Those share: per‑table scores, a small
number of pools, age‑based priority lifting, write‑side backpressure, and plan/execute
separation. We borrow the K8s scheduling *framework shape* (single queue → filter → score
→ reserve → bind, backoff queue) because it is a clean way to structure the planner, not
because of scale.

## 3. Model in one paragraph

A **single leader planner** (etcd‑lease elected among masters) maintains an in‑memory
**demand table**: one `TableDemand` per table, fed by push signals (writer flush, merge
completion) and reconciled by one pull scanner. Every planning cycle it computes a **score
per (table, kind)**, picks the best feasible unit of work, selects an **executor** with free
(slot, memory) capacity, and writes one **assignment** record. Executors (resident masters,
workers, catch‑up Jobs) watch for assignments addressed to them and run them under the
existing merge‑crate fence. Per table, exactly one **write turn** is active; preparation‑
only work (compaction/index prepare) runs outside the turn and requests the next turn when
ready. One **failure ledger** with typed error classes drives backoff and aging. All
per‑table behaviour comes from one **policy** document; global knobs shrink to ~12.

```
 writers ──flush signal──┐
 merge done ─────────────┤                 ┌──────────────┐
 stats scan (reconcile) ─┴─► DemandTable ─►│   Planner    │ (leader)
                                           │ score→filter │
 executors/<id> (leased heartbeat:         │ →place→reserve│
   kinds, free slots, free bytes) ────────►└──────┬───────┘
                                                  │ assignments/<table>  (one CAS)
            ┌─────────────────────────────────────┼─────────────────────┐
            ▼                                     ▼                     ▼
   resident master                           worker pod            catch‑up Job
   (bind → merge‑crate reserve → run → release → report outcome)
```

## 4. Components

### 4.1 Demand table (replaces L3 request scan, L4, L5, L6 side‑effects, L7, L8, L9 admission)

`TableDemand` per table, persisted at `P/demand/<hex>` (unleased, small JSON) and cached by
the leader:

```
pending_generations, pending_bytes, oldest_pending_ms      // WAL merge demand
fragment_count, small_fragment_bytes                        // compaction demand
index_stale: bool, uncovered_fragments                      // index demand
missing_fragments: bool                                     // repair demand
last_success_ms[kind], last_observed_ms, observed_by
```

Sources, in order of freshness:
1. **Push**: the rollout server already notifies on flush (`merge-requests`). Generalise to
   per‑shard **watermarks**, not deltas: `PUT P/demand-events/<hex>/<shard>` carrying
   `{sealed_through_seq, bytes_through_seq, flushed_at_ms}`. Watermarks are idempotent:
   the planner keeps the max `sealed_through_seq` per shard and sums across shards, so a
   lost or duplicated event cannot corrupt the count. Merge release writes the merged
   watermark per shard in the same `release` txn; pending = sealed − merged per shard.
2. **Pull reconciliation**: the existing stats scanner (L6) continues at 300 s and writes
   the same per‑shard watermark records as writers do, with `source: scan`. etcd
   revisions and manifest versions are different clocks and are never compared with each
   other. The only ordering used is the shard's own: `(writer_epoch, sealed_through)`. A
   scan may **raise** a shard's `sealed_through` at an equal epoch and may **replace** the
   shard record only with a strictly higher `writer_epoch` (a retired writer whose shard
   was re‑observed); it may never lower a watermark at an equal epoch, however late the
   snapshot was written. After migration the scanner no longer enqueues, retires or
   compacts directly (retirement becomes a `Retire` kind, see 4.3).
3. **Targeted probe**: when `last_observed_ms` is older than `demand_stale_secs` for a
   table with recent demand, the planner schedules a cheap `Observe` unit (manifest‑only
   read) instead of running a discovery loop.

This removes the need for five discovery loops: hot tables are kept current by push, cold
tails by the scanner, and nothing waits on a 600 s sweep.

### 4.2 Scoring (RocksDB/ClickHouse style)

Scores are dimensionless, computed on every cycle, deterministic, and exposed on
`GET /api/v1/scheduler/demand?target=`.

```
merge_score   = max(pending_generations / merge_min_generations,
                    pending_bytes / merge_min_bytes)
              × age_boost(oldest_pending_ms, merge_max_age_secs)
compact_score = fragment_count / min_fragments
              × (0.5 if merge_score ≥ 1 else 1)        // don't compact under a hot WAL
index_score   = uncovered_fragments / index_min_fragments  (0 if !index_stale)
repair_score  = ∞ if missing_fragments
retire_score  = 1 if idle > retire_secs else 0
age_boost(t)  = 1 + (now − t) / max_age                // linear lift so tails cannot starve
```

Priority classes (strict ordering, then score within class):

| Class | Members | Rationale |
|---|---|---|
| 0 Recovery | unresolved `merge-executions` with `maintenance` set, `Repair` | Unblocks everything else on that table. |
| 1 Critical merge | `merge_score` with `pending_generations ≥ critical_generations` (default 256) | Read amplification / 503 risk; mirrors `CATCHUP_MIN_PENDING`. |
| 2 Commit‑ready | prepared compaction/index requesting its turn | Generalises PR #324's hint: the table's next write turn goes to the preparer. |
| 3 Normal merge | `merge_score ≥ 1` | |
| 4 Compaction / Index | `compact_score ≥ 1`, `index_score ≥ 1` | |
| 5 Tail / housekeeping | `0 < merge_score < 1` after aging, `Retire`, `Observe` | Replaces wal‑tail sweep. |

Class ordering alone starves: a hot table can sit above `critical_generations` for hours,
so a class‑1 merger would always beat a class‑2 commit‑ready compaction, and in‑class aging
never lifts anything across a class boundary. Two **hard bounds** make fairness a guarantee
rather than a tendency:

* `max_consecutive_turns[kind]` (default merge = 2): after N consecutive write turns of the
  same kind on one table, the next turn goes to the highest‑scored *other* kind with a
  pending request (typically commit‑ready compaction/index). This applies across classes,
  including class 1.
* `max_turn_wait_secs[kind]` (default compaction/index commit = 900, tail merge = 3600):
  a unit that has waited longer is promoted to class 1 regardless of score.

Promotion to class 1 is not by itself a guarantee: class 1 is still sorted by score, so
a low‑backlog table promoted for age can keep losing to hot tables whose critical merges
are also class 1. The cross‑table rule is therefore **oldest‑first within class 1**: class
1 sorts by `wait_since_ms` ascending, not by score, and the planner must place the
oldest class‑1 unit before any other unit whenever a feasible executor exists (invariant
4 is restated accordingly). Score ordering applies only in classes 2–5. A table can
starve only if no executor is ever feasible for it, which is a capacity fault surfaced
by `scheduler_schedule_to_bind_seconds`, not a fairness fault.

All three are per‑table policy fields and are exported as `scheduler_turn_wait_seconds`.

### 4.3 Units of work and the per‑table write‑turn state machine

Kinds: `MergeWal`, `Compact`, `IndexId`, `Repair`, `Retire`, `Observe`. Each kind declares
`needs_write_turn: bool` and a `prepare` phase flag. Per table:

```
Idle ──plan──► Assigned ──bind──► Holding‑turn ──release──► Cooldown(kind) ──► Idle
                 │                     ▲
                 └─(prepare‑only)──► Preparing ──commit‑ready──► (class‑2 request)
```

* Exactly one **write‑turn holder** per table at any time; this *is* the merge‑crate
  `target-locks`/`merge-executions` pair — the scheduler does not add a second lock.
* `Preparing` units never hold the turn and run **concurrently with the turn holder and
  with each other** (bounded only by executor capacity and one preparation slot per
  (table, kind)). Today's `COMPACTION_PREPARE_TARGETS` / `INDEX_PREPARE_TARGETS` behaviour
  becomes the default. The scheduler must never re‑serialise merge behind compaction or
  index preparation; #308 and #327 removed exactly that.
* `Cooldown(kind)` is produced by the failure ledger (4.6); successes write
  `last_success_ms` and no cooldown except a configurable `min_interval[kind]`.
* The dependency DAG (`Compact → IndexId`, `Repair → X`) is expressed as *demand*, not as
  task dependencies: after compaction, `index_stale=true`; after repair,
  `missing_fragments=false`. This removes `dependency_status` probes and `fail_dependency`.

### 4.4 Executors and capacity (replaces 4 semaphores, `compaction_permits`, `catchup-slots`)

Every process that can run work publishes a leased heartbeat `P/executors/<id>`:

```
{ kind: resident|worker|catchup_job, kinds: [MergeWal, Compact, …],
  slots_total[kind], slots_free[kind], bytes_total, bytes_free,
  owned_targets_filter, draining: bool, version, instance }
```

Capacity is a **slot supplier per kind plus one byte budget per process** (Temporal's model;
also what `MergeMemoryBudget` already is). The heartbeat's `bytes_free` is a *sample* and is
never used for placement arithmetic. Instead each assignment record carries
`reserved_slots` and `reserved_bytes`, and the planner's view of an executor is
`bytes_total − Σ reserved_bytes(live assignments on it)` — including assignments that have
not yet bound. Reservations are therefore durable in etcd, rebuilt from `assignments/` on
leader failover, and released in the executor's `release` txn. Executors still enforce
`MergeMemoryBudget` locally; the planner's reservation prevents over‑dispatch, the local
budget prevents OOM when an estimate is wrong. A unit's declared cost is
`(1 slot[kind], expected_bytes)` where `expected_bytes = min(pending_bytes, max_bytes)×2`
for staging, `1 GiB` default for compaction/index.

Placement score (K8s "score" phase), evaluated only on feasible executors:
`prefer resident master already caching the table` > `worker that owns the shard` >
`spawn catch‑up Job` (only when `pending_bytes` > `job_threshold_bytes` and no resident
capacity for `job_wait_secs`). K8s Jobs become an *executor type* with on‑demand capacity,
not a separate controller with its own admission. Legacy worker RPCs likewise become an
executor type with their own slots. Isolation bounds blast radius only: a stuck execution is
still recognised by the merge‑crate watchdog from that execution's actual read/encode/commit
progress, then cancelled, fenced and taken over. Heartbeats, retry backoff and total run time
are not progress.

### 4.5 Planner cycle (leader only)

Triggered by etcd watch events on `demand-events/`, `executors/`, `assignments/`,
`merge-executions/`, plus a 1 s tick. One cycle:

1. Fold events into the demand table.
2. Build the candidate list: all `(table, kind)` with score ≥ threshold, not in cooldown,
   not already assigned/holding, policy allows kind.
3. Sort by (class, score desc, round‑robin tiebreak).
4. For the top `max_placements_per_cycle` (default 8): **Filter** (ownership, draining,
   fence state from a *single* cached watch on `target-locks`/`merge-executions`, failure
   ledger) → **Place** (4.4) → **Reserve**: one etcd txn writing
   `P/assignments/<hex>/<unit-id> = {unit, executor, token, deadline, reserved_bytes}`
   guarded by `executors/<id>.lease alive`, `leader == own token`, and — for write‑turn
   units — no other write‑turn assignment on the table. Preparation units need only a free
   preparation slot for their (table, kind).
5. Expire assignments not bound within `bind_timeout_secs` (default 30 s) → backoff queue
   with exponential delay (K8s `backoffQ`), not a failure.

Complexity per cycle is O(tables log tables) in memory; etcd traffic is one txn per
placement. No prefix scans on the hot path.

**Leadership**: `P/leader` leased key, campaign on startup, TTL = `ETCD_LEASE_TTL_SECS`.
Non‑leaders keep the demand cache warm via watches so failover is sub‑second. All
masters remain executors regardless of leadership.

### 4.6 Failure ledger (replaces task cooldown, merge‑failures, catch‑up record backoff, compact‑noops)

One key family `P/failures/<hex>/<kind>`:

```
{ class: Retryable|Ownership|DataOrConfig|Deadline|NoOp,
  attempts, first_ms, last_ms, next_retry_ms, last_error_code, last_error }
```

Errors must carry a **stable machine code** (e.g. `LC_STALE_PREPARATION`,
`LC_MISSING_FRAGMENT`), emitted by `lance-context-core` as a typed enum and classified by
code — never by message text. Policy is one function:
`backoff(class, attempts)`; `needs_attention` at a per‑class threshold; `NoOp` (compaction
that changed nothing) is a 900 s cooldown, replacing `compact-noops`. The ledger entry is
written in the executor's `release` txn so it is atomic with the fence.

### 4.7 Backpressure

The planner publishes per‑table `backlog_class ∈ {ok, warn, critical}` into the demand
record. The rollout server already 503s reads past 4096 pending generations; it should also
read `backlog_class` and export it as a metric, and may use it to shape ingestion. Any
shaping must respect two constraints that reducing generation *count* alone does not:

* **Memory**: stretching the flush interval or coalescing generations keeps more rows in
  the writer's memtable. The server's existing `ROLLOUT_WAL_PENDING_MAX_*` and memtable
  budget remain the hard caps; shaping may only move work *between* a longer flush interval
  and a slower admission rate, never past those caps. If the memtable budget would be
  exceeded, the server must flush (producing a generation) rather than hold rows.
* **Durability latency**: rows are durable only when flushed. Shaping must not extend the
  time between a write being acknowledged and being flushed beyond the server's configured
  durability bound (`ROLLOUT_FLUSH_INTERVAL_SECS` is that bound today). The alternative
  lever under `critical` is therefore admission delay (ClickHouse `parts_to_delay_insert`,
  TiKV flow control): slow the writer's accept rate proportionally, keep flush cadence.

Backpressure therefore converts backlog into **slower ingestion**, never into memory
pressure or longer un‑flushed windows. This is the only place where the scheduler
influences writers, and it is gated per table by policy (`backpressure: off | metric |
delay_admission`), default `metric`.

### 4.8 Policy and configuration

Per‑table policy document `P/policies/<hex>` (default inherited from `P/policies/_default`),
editable via `PUT /api/v1/policies/{target}`:

```
mode: active | drain | frozen            // replaces OWNED/DRAIN allowlists
executors_allowed: [resident, worker, catchup_job]
kinds_allowed: [MergeWal, Compact, IndexId, Repair, Retire]
merge: {min_generations, min_bytes, max_age_secs, critical_generations, continuous: bool}
compact: {min_fragments, max_source_fragments}
index: {type: btree|zonemap}
```

Global env knobs (target ≤ 12): `ETCD_*`, `PLANNER_CYCLE_MS`, `PLANNER_MAX_PLACEMENTS`,
`BIND_TIMEOUT_SECS`, `EXECUTOR_SLOTS_<KIND>`, `EXECUTOR_MEMORY_BYTES`,
`DEMAND_STALE_SECS`, `STATS_SCAN_INTERVAL_SECS`, `CATCHUP_POD_TEMPLATE`, `CATCHUP_MAX_JOBS`.
Everything in today's `ROLLOUT_APPEND_*`, `CATCHUP_*`, `MERGE_WAL_TAIL_*`,
`*_PREPARE_TARGETS`, `*_COMMIT_WAIT_SECS` either becomes policy or is deleted.

### 4.9 Observability contract

* `GET /api/v1/scheduler/demand[?target=]` — demand, scores, class, cooldown, why‑not‑now.
* `GET /api/v1/scheduler/assignments` — live assignments with executor and bind age.
* `GET /api/v1/scheduler/executors` — capacity view.
* Metrics: `scheduler_demand_score{kind}`, `scheduler_backlog_class`,
  `scheduler_placements_total{kind,executor_kind,result}`,
  `scheduler_schedule_to_bind_seconds` (Temporal's schedule‑to‑start — the single best
  "are we capacity‑bound" signal), `scheduler_turn_wait_seconds{kind}` (fairness).
* Every planner decision logs one structured line
  `{table, kind, class, score, executor, reason}`; every *skip* in filter logs a reason
  code at debug.

### 4.10 Rate limits per executor (TiKV PD "store limit")

PD throttles how many operators it dispatches to one store per minute so a planner with
a full view cannot flood a single node. We adopt the same: `placements_per_executor_per_min`
(default 6) and `bytes_in_flight_per_executor ≤ bytes_total`. Both are checked in Filter
before Score. This makes the planner incapable of producing the "24 merges on each
worker" failure mode described in `server/config.rs:76`.

## 5. Risks of a single leader, and mitigations

Search of production systems (TiKV PD, Pulsar load manager, YugabyteDB master, Vitess
VTOrc, Pebble) gives a consistent list of failure modes for leader‑driven schedulers.
Each is addressed here explicitly.

| Risk | Mitigation in this design |
|---|---|
| **Leader hot spot / HoL blocking** | Planner only *decides* (O(tables log tables) in memory, ≤ 8 one‑key txns per cycle); it never executes. Executors do all IO. |
| **Split brain on lease expiry** | The planner's assignment is *advisory*; the merge‑crate fence (`target-locks`, `merge-executions`, version barriers) is what guarantees one publisher. Two planners for a few seconds can at worst double‑assign; the second bind fails at `reserve`. Assignment txn is guarded by `leader` key value == own token. |
| **Stale in‑memory state after failover** | Everything the planner needs is in etcd (`demand/`, `executors/`, `assignments/`, `failures/`, `policies/`, fence keys). Followers keep a warm cache via watches; a new leader reconciles from a single revision snapshot before its first cycle. No decision depends on state that only lived in the old leader's memory (YugabyteDB pattern: durable, replicated state). |
| **Thundering herd on leader change** | Assignments outlive the leader; executors keep running. New leader honours existing assignments and `placements_per_executor_per_min` caps catch‑up bursts (PD store‑limit pattern). |
| **Planner bug stalls fleet** | `GET /scheduler/demand` exposes why‑not‑now; a `mode: legacy` per‑table policy falls back to the old loops during migration; after P4, a manual `POST /scheduler/assign` bypass exists. |
| **Scoring badly tuned** | Scores are pure functions with table‑driven tests; shadow mode in P1 compares planner decisions against actual behaviour for a week before anything is placed. |

Why not leaderless (VTOrc / today's optimistic `claim_next`)? Leaderless works when each
decision is local and idempotent. Our fairness goals (aging, class ordering, round‑robin
between merger and preparer, per‑executor rate limits) require a global view; emulating
that leaderlessly is exactly what produced five cursor loops and a 7‑key claim txn.

## 6. What this is not

* Not a redesign of the merge‑crate fence, version barriers or progress watchdog.
* Not a general job system; six kinds, fixed.
* Not a multi‑scheduler. One leader is sufficient for ≤ 10⁴ tables; if that ever changes,
  shard the planner by table hash — the model is unchanged.

## 7. Invariants (to be enforced by tests)

1. At most one write‑turn assignment per table; preparation assignments are bounded only
   by capacity and one preparation slot per (table, kind). Merge is never queued behind a
   preparation.
2. An assignment is never written to an executor whose heartbeat lease is dead.
3. Sum of `reserved_bytes` over live assignments on an executor ≤ its `bytes_total`,
   computed from `assignments/`, never from sampled `bytes_free`.
4. Class 1 is served oldest‑first: the class‑1 unit with the smallest `wait_since_ms` is
   placed before any other unit whenever a feasible executor exists. Within that rule a
   table with `pending_generations ≥ critical_generations` is placed before any class ≥ 3
   unit.
5. A commit‑ready request is satisfied within `max_consecutive_turns` write turns of the
   same table, and any unit waits at most `max_turn_wait_secs` before promotion to class 1.
5a. Demand folding is idempotent: replaying any subset of `demand-events` in any order
   yields the same `TableDemand`.
6. No planner cycle issues an etcd range read larger than one page of `demand-events`.
7. All scheduling decisions are a pure function of `(DemandTable, ExecutorSet, FenceState,
   FailureLedger, Policies, now)` and are unit‑tested without etcd.

## 8. Migration (summary; detail in the backlog doc)

Phase 0 extract pure eligibility/scoring functions + typed errors (no behaviour change) →
Phase 1 demand table + leader planner running in *shadow mode* (logs decisions, enqueues
nothing; **all existing loops keep running unchanged**) → Phase 2 planner owns MergeWal placement for `mode: planner` tables; old loops
disabled per table by policy, and only once every master runs a policy‑aware binary → Phase 3 compaction/index/repair/retire → Phase 4 delete
loops, pools, allowlists, and 15 of 27 etcd key families. Mixed‑version safety: old masters
treat unknown `assignments/` keys as absent and still respect `target-locks`; new planner
treats legacy `claims/`+`target-locks` as `Holding‑turn`.

## Appendix A — Today's loops (from the code inventory)

| # | Loop | File | Pacing | Feeds | Replaced by |
|---|---|---|---|---|---|
| L1 | 4 pool pollers | `scheduler.rs:1155` | 500 ms + semaphores | claims | executor bind watch |
| L2 | `claim_next` scan | `task_store.rs:808` | per claim, 7‑key txn | — | planner reserve (1 key) |
| L3 | demand/recovery/failure scan | `scheduler.rs:965` | 15 s, 3 prefix scans | queue | demand events + ledger watch |
| L4 | compaction sweep | `scheduler.rs:1043` | 600 s | queue | compact_score |
| L5 | merge sweep | `scheduler.rs:1061` | 600 s | queue | merge_score |
| L6 | stats scanner (+retire/compact side effects) | `scanner.rs:921` | 300 s | direct mutation | demand reconcile only |
| L7 | wal‑tail sweep | `wal_tail.rs:72` | 30 s + etcd cursor | queue | age_boost class 5 |
| L8 | resident discovery | `resident_recovery.rs:73` | 5 s + etcd cursor | merge‑requests | `Observe` unit |
| L9 | catch‑up controller | `catchup/mod.rs:342` | 30 s | K8s Jobs | catchup_job executor |
| L10 | append staging continuation | `rollout_append.rs:215` | in‑task | merge‑requests | demand update in release |
| L11 | merge‑crate coordinator | `merge/lib.rs` | — | fence | **kept** |

## Appendix B — Prior art consulted

* RocksDB compaction: per‑column‑family scores (`level0_file_num_compaction_trigger`,
  pending bytes), low/high priority pools, write stall/slowdown backpressure.
* ClickHouse MergeTree: `SimpleMergeSelector` with age‑lowered thresholds,
  `merge_selecting_sleep_ms` × slowdown factor backoff, `parts_to_delay_insert` /
  `parts_to_throw_insert`, server‑wide pool with per‑table overrides.
* Apache Hudi table services: plan written to timeline, execute async; trigger strategies
  `NUM_COMMITS | TIME_ELAPSED | NUM_OR_TIME`.
* Kubernetes scheduling framework: activeQ/backoffQ, QueueSort → Filter → Score →
  Reserve → Permit → Bind, one‑pod‑at‑a‑time optimistic cycle, QueueingHint.
* Omega: shared‑state optimistic multi‑scheduler — the pattern today's per‑replica
  `claim_next` approximates; justified only at Google scale and for heterogeneous
  schedulers, neither of which applies here.
* Temporal workers: per‑task‑type slot suppliers, resource‑based auto‑tuning,
  schedule‑to‑start latency as the backlog metric.
  https://docs.temporal.io/develop/worker-performance
* CockroachDB Pebble compaction picker: per‑level score (L0 file count ÷ threshold, Ln
  bytes ÷ target), tombstone compensation, low‑priority maintenance compactions, L0
  sublevel count as write throttle. https://deepwiki.com/cockroachdb/pebble/5.1-compaction-picking
* TiKV: flow control proportional to pending compaction bytes
  (`soft-pending-compaction-bytes-limit`); PD scheduler **store limit** and operator
  queue rate‑limit how much background work one node receives.
  https://docs.pingcap.com/best-practices/pd-scheduling-best-practices/
* Apache Pulsar load manager: only the elected leader makes placement decisions;
  coarse‑grained bundles reduce churn. https://pulsar.apache.org/docs/4.0.x/develop-load-manager/
* YugabyteDB master: Raft‑replicated state so failover does not lose scheduler state.
  https://docs.yugabyte.com/stable/architecture/yb-master/
* Vitess VTOrc: leaderless, idempotent agents over the topology server — the
  counter‑example we reject (§5).
* Databricks predictive optimization: cost/benefit telemetry decides *whether* to
  compact at all, not just when. https://www.databricks.com/blog/predictive-optimization-scale-year-innovation-and-whats-next
* Kubernetes QueueingHint KEP‑4247: event‑driven requeue instead of periodic retry.
  https://github.com/kubernetes/enhancements/blob/master/keps/sig-scheduling/4247-queueinghint/README.md
* RocksDB compaction priority blog (kMinOverlappingRatio):
  https://rocksdb.org/blog/2016/01/29/compaction_pri.html
* ClickHouse merge‑selecting settings:
  https://clickhouse.com/docs/reference/settings/merge-tree-settings/merge-selecting
