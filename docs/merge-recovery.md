# Owned merge recovery

A serial merge fan-out previously had no request deadline and could report
success after one worker failed if another reclaimed generations. An HTTP
timeout alone cannot hand off a table: the worker or object store may still be
committing after the master advances.

Protocol 2 owns the worker execution independently of its HTTP connection,
records every admitted manifest version, and can fence those versions in storage
before handing off a stuck or crashed executor. Recovery does not depend on a
replacement pod being Ready or on guessing that an old process has died.

## Admission, cancellation, and recovery

`MergeWal`, `Compact`, `IndexId`, and `Repair` share the existing target lock.
Merge admission replaces that lease-backed lock with a persistent execution
marker. A separate leased merge claim permits only one scheduler to reconcile
it, including tasks that bypass dedupe through dependency chains. Helpers that
respect the existing target lock stay excluded throughout recovery.

Before **each** base-manifest commit and **each** shard-drain write attempt, the
worker atomically checks that its exact execution is still Running and records
the resource's maximum admitted immutable version. The permit also binds the
worker's dataset URI. A shard drain guards each conditional write, not a whole
retry loop that could acquire fresh versions after revocation.

Normal cancellation drops preparation and joins already-started leaf manifest
writes before publishing Finished. Worker merge slots remain held during that
join. A definite conditional-write conflict is distinguished from an ambiguous
storage error or panic. An ambiguous outcome publishes Uncertain and retains
ownership, even though the Rust future has ended.

For a missing/stuck executor or an uncertain commit result, the master:

1. Atomically changes the execution to Recovering, closing further commit
   admission. A racing permit either precedes this transaction and is included
   in the durable maximum, or fails its ownership comparison.
2. Reads the closed set of admitted versions and checks the worker/master URI.
3. Uses metadata-only conditional writes to advance the base and affected shard
   manifests beyond those versions. The base barrier updates reserved table
   metadata `lance-context.merge-recovery`. Shard barriers copy the current
   manifest without changing the WAL list or writer epoch. They do not open a
   shard writer, scan payloads, drop fragments, or fence ongoing ingestion.
4. Publishes Recovered only after every barrier is confirmed. Only then can a
   live task release the target lock and continue merging. A delayed old PUT
   targets an occupied immutable version, and any new old-executor commit is
   denied by the closed admission gate.

A surviving executor may abort/join its pending leaf futures after observing a
completed barrier or a subsequent execution. Before that evidence it retains
those futures. This allows a healthy shard to progress without waiting forever
for a stalled old storage request. The master permits one recovery/restart of
fan-out within a task; further faults are durably queued for another task so a
single target cannot monopolize a scheduler slot indefinitely.

Reserved work is cancelled by CAS, so a delayed HTTP POST cannot resurrect it.
The base handle refreshes before retrying, and WAL drain remains a relative edit
that preserves generations flushed during recovery. Graceful worker shutdown
cancels/drains owned actors before closing resident stores.

## Repeated failures

Per-target/endpoint records survive task recreation and master restart. They
contain consecutive failed attempt counts, diagnostic class, last error (capped
at 4096 characters), next retry time, and `needs_attention`. Executed failures
are recorded atomically with releasing terminal execution ownership; a crash
between release and bookkeeping cannot erase the retry budget.

The first three transient failures use two-second targeted retries, with healthy
shards visited before failed shards are retried. After that, each due task makes
one probe, backing off from 30 seconds to at most 15 minutes. At 15 consecutive
failures, probes are hourly and `needs_attention` is set. Missing data,
corruption, and configuration/authentication errors require attention immediately
and use hourly probes. Diagnostic text classification is conservative and never
authorizes data removal.

A failed **storage barrier** retains the execution/target lock and releases the
scheduler slot. Metadata recovery probes back off from 30 seconds to at most
five minutes, with attention flagged immediately. They keep trying to establish
a safe handoff when storage recovers. They cannot authorize a new writer while
the barrier is incomplete. Other tables continue running.

Successful shard merges clear their failure records. Failed/deferred shards keep
the overall task failed even if healthy shards reclaim WAL. Per-table merge
cooldown is replaced by per-endpoint backoff so healthy shards can process new
WAL. A 15-second scheduler poll reads at most 256 failure records per page and
enqueues due targets through existing dedupe. It does not wait for the
600-second stats sweep. Large ledgers add page traversal latency; removed worker
endpoints are not automatically probed.

`GET /api/v1/scheduler/merge-failures?after=<cursor>` returns up to 256 records
and a `next` cursor. `needs_attention`, `last_error`, and `next_retry_ms` expose
the circuit state. `master_merge_storage_recoveries_total{result}` records
barrier outcomes alongside existing task and reclaimed-generation metrics. This
is an observable circuit state, not an external notification integration.

Merge errors do **not** enqueue destructive fragment repair. Missing/corrupt
files require diagnosis and restoration or an explicitly reviewed repair.
Other maintenance tasks retain their existing repair policy.

## Storage and rollout requirements

- Masters/workers must use the same physical storage namespace and etcd prefix,
  with consistent storage configuration/credentials. URI mismatch fails closed.
- Version fencing supports Lance's immutable conditional manifest handlers:
  local files, S3, GCS, Azure, memory, OSS, COS, TOS, and shared memory. Fallback
  unsafe handlers and external catalogues such as `s3+ddb` are rejected for
  owned merging/recovery. Existing manifest immutability/retention guarantees
  must be preserved; never manually remove fencing/version files to unblock a
  live writer.
- Workers need `ETCD_ENDPOINTS` and the same `ETCD_PREFIX` as masters.
  `MERGE_EXECUTION_TIMEOUT_SECS` defaults to 600 and the master caps the requested
  deadline at 600. Zero is rejected. Cancellation reconciliation has a 60-second
  allowance; a metadata recovery attempt has a 120-second deadline.
- Enable protocol 2 on both sides. There is no fallback to unbounded legacy
  merge HTTP calls. Old executions without version admission cannot be
  automatically fenced using an empty watermark set; they remain protected.
- With the coordinator enabled, legacy merge routes reject admission. Disable
  worker self-merge thresholds and cleanup timers; startup checks enforce this.
- `INDEX_BEFORE_MERGE` remains accepted for compatibility; workers prepare their
  key indexes inside owned execution. Keep ordinary workers at six merge slots
  and 20 GiB; this change increases neither limit.
- Drain/reconcile legacy in-flight merges before enabling protocol 2. Masters
  must continue other jobs; recovery helpers must retain target locks. Do not
  clear a keeper's pause while its delegate is alive.

The fence covers scheduler-coordinated maintenance and helpers that honor target
locks. It does not retrofit lease-loss protection onto the master's own local
compaction/index jobs or arbitrary manual writers. It cannot repair missing data
or guarantee a WAL pending ceiling while storage remains unavailable.

Before production rollout, staging must exercise realistic blobs, sustained
writes, forced worker death and delayed storage responses. Measure pending and
reclaimed generations, memory/OOM counts, commit conflicts, etcd overhead, and
p95/p99 task latency. Local fault tests do not replace that soak.
