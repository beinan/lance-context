# Recovering stalled local table maintenance

Index, compaction and repair tasks can hold the same table lock needed by WAL
merging. On explicitly owned merge targets, these local mutations now reserve a
durable execution using the existing version-fencing protocol. Legacy targets
retain their previous execution path. Worker merge fan-out remains serial;
compaction concurrency and all merge memory/byte budgets are unchanged.

## Cancellation and takeover

The default total deadline is 3600 seconds (`MAINTENANCE_TIMEOUT_SECS`), with a
600-second no-progress deadline (`MAINTENANCE_IDLE_TIMEOUT_SECS`) and 30-second
commit drain grace (`MAINTENANCE_DRAIN_TIMEOUT_SECS`). All must be positive.
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
task or process does not reset the attempt count. Retryable failures back off;
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
