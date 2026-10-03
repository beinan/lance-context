# Recovering stalled local table maintenance

Index, compaction and repair tasks can hold the same table lock needed by WAL
merging. On explicitly owned merge targets, these local mutations now reserve a
durable execution using the existing version-fencing protocol. Legacy targets
retain their previous execution path. Worker merge fan-out remains serial;
compaction concurrency and all merge memory/byte budgets are unchanged.

## Cancellation and takeover

Owned maintenance has no total runtime ceiling while making progress. It uses a
600-second no-progress deadline (`MAINTENANCE_IDLE_TIMEOUT_SECS`) and 30-second
commit drain grace (`MAINTENANCE_DRAIN_TIMEOUT_SECS`). `MAINTENANCE_TIMEOUT_SECS`
remains a positive legacy wire field; older binaries may still enforce it during
a rolling upgrade. All timeout configuration fields must remain positive.
Progress counts completed scoped processing steps or manifest commits, never
heartbeats. Lance index building and individual compaction rewrites do not
expose fine-grained scan progress here: a healthy long operation can reach the
idle deadline. Size that deadline above measured large-table operation times;
it is an execution bound, not proof of an internal deadlock.

Cancellation drops the local work future and closes commit admission. If every
manifest commit has a definite result, the task can complete and release its
lock. Ambiguous or still-running commits require durable admission revocation
and metadata-only conditional manifest writes beyond every admitted version.
Only a successful barrier permits another writer to take over. Newly opened
maintenance handles retain their execution guard even when passed to a child
task. Cached worker handles continue using the current execution dynamically.

If storage fencing fails, keep the durable table ownership, return the scheduler
slot, and retry recovery with persistent backoff. A small supervisor retains any
unfinished manifest leaves until they drain or a later reconciler proves the
storage fence. Other tables keep running. A master crash or an uncertain
admission response is recoverable from the durable execution inventory, scanned
in pages of 256 every 15 seconds independently of stats sweeps.

## Repeated failures and diagnostics

Failures use the existing `/merge-failures` API and durable per-target ledger,
with endpoints `master:compact`, `master:index_id`, and `master:repair`. A new
task or process does not reset the attempt count. A repair that actually commits
a changed manifest clears the corresponding missing-file failures so dependent
work can resume; no-op repairs and unrelated failures keep their budget.
Retryable failures back off;
persistent failures get `needs_attention` and hourly probes. An unresolved
storage barrier retains ownership and probes at up to five-minute intervals.
An unavailable dataset, bad configuration or repeatable corruption requires
repair of that cause; retrying cannot manufacture a successful storage fence.

## Activation and compatibility

This uses the existing owned-target configuration, not a new rollout switch.
Deploy compatible masters before activating more owned targets. Include every
auxiliary maintenance master in that inventory. Old readers cannot CAS records
containing the maintenance discriminator, so they fail closed rather than
acting as a compatible recovery controller. Worker execution admission rejects
local maintenance records. Existing worker execution JSON remains unchanged.

The existing protocol transition still requires legacy writes to be drained:
this change cannot retroactively fence an old unguarded write. It does not enable
owned targets, alter production pods, or complete registry migration. Recovery
continues on draining targets while an owned execution remains. Validate large
table timings and fault recovery before selecting tighter deadlines.

## Prompt worker loss detection

For owned WAL merges, the master probes the executor identity every 10 seconds
with a two-second request timeout. A changed process incarnation triggers storage
recovery on the next probe. Three consecutive failed probes without observed
merge progress also trigger recovery (normally about 30–36 seconds). A successful
probe or a newly completed merge step clears the failed-probe count. These are
recovery triggers, not evidence that a remote storage commit has stopped: the
existing version barrier must still succeed before a replacement can write.

Workers independently check execution ownership every two seconds, including
while waiting for a merge slot and before the first processing checkpoint. A
revoked execution cancels its work even when its cancel HTTP request was lost.
The merge slot remains held until admitted storage writes drain or the durable
storage barrier permits their cancellation. This terminates the individual merge
execution, not the worker process or other tables' work.

A live worker with no completed processing steps still uses
`MERGE_IDLE_TIMEOUT_SECS` (default 600). Identity probes are not merge progress
and cannot extend this deadline. Reduce that setting only after measuring the
longest individual generation read or base-table write for the target workload;
there is no sub-operation progress for a single long storage call. These changes
do not activate ownership fencing for legacy tables, launch replacement pods, or
provide an OS-level watchdog for a completely wedged worker process.
