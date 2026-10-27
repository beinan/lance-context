# Single-shard annotation application

The annotation service separates durable WAL acceptance from application success.
A background driver processes accepted records in order, retains the latest
result per bounded producer slot, and signals waiters only after both the complete
annotation state and the result are installed. Missing turns reject the whole
batch and advance progress; temporary staging pressure retains the decoded batch.
The driver skips rebuilding that batch while the materialized cut and retained
byte/entry usage are unchanged. Publication or release of retained readers and
old generation handles enables another attempt without submitting a new mutation.
Storage or protocol errors stop the driver and make reads fail.

The first binding has exactly one virtual shard and an explicit `TurnAnnotations`
policy. Its Arrow schema contains `id`, label additions/removals, and separate
status/score operation columns: 0 means keep, 1 means set, and 2 means clear. Keep
and clear require a null value; set requires a non-null value. Empty status and
zero score remain valid values. Repeated turn IDs in a batch apply in row order.
Rejection-affecting limits are fingerprinted in the schema so replay and separate
ingresses cannot silently choose different outcomes.

The upload is validated before submission. The original IPC bytes are retained
for exact retries and replicated opaquely. A background applier decodes each fetched
batch once and retains its narrow patches through pressure; replicas do not decode
Arrow. This is not end-to-end zero copy: turning patches into current owned values
still allocates narrow strings/sets. Mutation uploads and patch application do not
copy a turn body.
Upload reservations and apply workspace use independent budgets: clients waiting
for application cannot occupy the memory needed to complete their own writes.

An exact retry under the latest producer identity returns the original result.
Changed bytes or old sequences are rejected by the existing WAL producer fence.
There is at most one unresolved request per producer. A deadline is ambiguous and
requires retaining the original identity and bytes; it does not authorize moving
to a new sequence. Results contain application status and a source position, not a
historical annotation value.

An independently connected `Service` has a bounded view and background driver.
Its point reads obtain an O(1) scoped quorum barrier, wait for the background
applied position, then resolve active/frozen/Lance from a compatible view. The
point-read path does not enumerate or fetch the WAL suffix. A lagging service
view waits or fails its deadline instead of returning its old base. This qualifies read
correctness; it does not establish the final multi-shard placement or throughput.
The deployed HTTP ingresses forward to one fenced owner and hold no
staging view. That owner's acquisition cut covers recovery, and every successful
write waits for apply; see [HTTP routing](annotation-owner-http.md) for its read
barriers and authority checks.

## Runtime and application integration

The `annotation-owner` application role runs this service with the publication
runtime. Native result checkpoints bind values and request outcomes to one cut,
including result-only batches.
Recovery validates the catalog-selected checkpoint before replaying its WAL suffix.
See [the result checkpoint protocol](annotation-result-checkpoint.md).

The [runtime](annotation-runtime.md) drives publication, confirmation and source
closure; [application ownership](annotation-application.md) fences takeover and
routes multiple ingresses to the current owner. The service-level RF3 restart
test uses orderly shutdown/reopen. Separate runtime and application process tests
exercise SIGKILL recovery, unknown publication results and collection faults.
Those correctness fixtures do not qualify sustained throughput or mixed ingestion.
