# Owned merge recovery

A serial merge fan-out previously had no request deadline and could report
success after one worker failed if another reclaimed generations. A client-side
HTTP timeout alone is unsafe: the worker may still be committing after the
master moves to the next shard.

This change admits a durable execution before sending the worker request. The
worker owns the merge independently of that request, cancels preparation at its
deadline, and joins already-started base/shard manifest commits before publishing
its terminal result. The master advances only after this acknowledgement. Before
retrying a cancelled commit, the store refreshes its base manifest. WAL drain
remains a relative edit preserving generations flushed during recovery.

## Ownership

- `MergeWal`, `Compact`, `IndexId`, and `Repair` share the target lock.
- Admission atomically replaces the lease-backed target lock with a persistent
  execution marker. Existing helpers that respect target locks remain excluded.
- A separate leased merge claim ensures only one scheduler reconciles an
  execution, including non-deduplicated tasks in dependency chains.
- Worker execution and the persistent target lock survive task lease loss.
  Only a terminal execution can restore the target lock to a live task lease.
  Ambiguous manifest errors/panics publish `uncertain`, which cannot release
  ownership: an ended Rust future is not evidence that remote storage rejected
  its write. Definite conditional-commit conflicts are treated separately.
- Cancelling an unstarted request uses CAS, preventing a delayed POST from
  starting after another shard has been admitted.
- Graceful worker shutdown cancels and drains owned executions before closing
  resident stores. SIGKILL/OOM is **not** a graceful acknowledgement.

## Repeated failures

Failures persist under `merge-failures/<target-hex>/<endpoint-hex>` in the master
etcd namespace. The record includes total consecutive failed attempts, diagnostic
class, last error (capped at 4096 characters), next retry time, and attention flag.
Recreating a task or master does not reset this budget. Executed failures are
recorded atomically with releasing terminal execution ownership; a crash in the
master between these two steps cannot lose the failure.

The first three transient failures use two-second targeted retries, with healthy
shards visited before retrying failed shards. After that, each due task makes
one probe, backing off from 30 seconds to at most 15 minutes. At 15 consecutive
failures, probes are hourly and `needs_attention` is set. Missing data, corruption,
configuration/authentication errors, and unresolved execution ownership set the
attention flag immediately and use hourly probes. Classification uses diagnostic
text conservatively; it never authorizes data removal.

Successful probes clear the shard's failure record. Failed/deferred shards keep
the overall task failed even if healthy shards reclaim WAL. Per-table merge
cooldown is replaced by per-endpoint backoff so healthy shards can process new
WAL. A 15-second scheduler poll walks at most 256 failure records per page and
enqueues due targets through existing dedupe. It reads metadata only and does
not wait for the 600-second stats sweep. For a large failure ledger, polling
latency grows with page count. Removed endpoints are not automatically probed.

`GET /api/v1/scheduler/merge-failures?after=<cursor>` returns at most 256 records
and a `next` cursor. `needs_attention`, `last_error`, and `next_retry_ms` distinguish
failed shards from recovered ones. Existing task error/counter metrics also show
failure. This is an observable circuit state, not an external alert integration.

The scheduler does **not** automatically enqueue destructive fragment repair for
a merge error. Missing/corrupt files require diagnosis and restoration or an
explicitly reviewed repair; retaining WAL is mandatory. Other maintenance task
repair behavior is unchanged.

## Limits and required rollout validation

This is not yet a complete automatic crash-recovery protocol. If a process dies, a storage commit cannot be joined, or a commit returns an
ambiguous result, there may be no terminal acknowledgement.
After the merge deadline plus a 60-second reconciliation allowance (or 60 seconds
for inherited work), the master records unresolved ownership, fails that task,
and releases its scheduler slot **without deleting the execution or target
lock**. Other tables continue. The affected table remains fenced until the old
executor acknowledges termination. A changed worker UUID, a Ready replacement,
an expired lease, and elapsed time are not sufficient proof.

Automatic recovery of that orphan requires a storage fencing/barrier protocol
and authoritative termination evidence; neither is implemented here. Do not
manually delete the fence or deploy this as a claimed complete fix for OOM/node
loss. The worker deadline bounds its scoped merge future; draining a backend
that never completes its commit is not bounded by that deadline.

The execution fence protects scheduler-coordinated maintenance and helpers that
honor target locks. It does not retrofit storage fencing onto arbitrary manual
writers or solve lease loss during the master's own local compaction/index job.

This wire protocol requires coordinated master/worker rollout. There is no
fallback to unbounded legacy HTTP merges:

- Workers need `ETCD_ENDPOINTS` and the **same** `ETCD_PREFIX` and credentials as
  masters. `MERGE_EXECUTION_TIMEOUT_SECS` defaults to 600; the master caps the
  requested deadline at 600. Zero is rejected.
- With the coordinator enabled, legacy merge routes reject admission. Disable
  worker self-merge thresholds and cleanup timers; startup checks enforce this.
- `INDEX_BEFORE_MERGE` remains accepted for compatibility, but index preparation
  is done by the worker inside the owned execution.
- Drain/reconcile legacy in-flight merges before enabling the new protocol.
  Masters must continue their other jobs. Recovery helpers must preserve target
  locks; do not clear a keeper's pause while its delegate is alive.
- Keep ordinary workers at six merge slots and 20 GiB. This patch increases
  neither memory budgets nor same-table commit concurrency.

Before production rollout, staging must cover realistic blobs, sustained writes,
WAL pending/reclamation rates, memory/OOM counts, commit conflicts, and p95/p99
latency. Local cancellation and HTTP-disconnect tests are necessary but cannot
substitute for this soak or the missing orphan recovery protocol.
