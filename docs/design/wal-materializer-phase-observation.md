# Materializer phase observations

Worker diagnostics report ten fixed cumulative phase counters: scheduling claim,
publisher acquisition, publisher-state query, preparation, publication, source
retirement, pinned-record fetch, IPC decoding and row validation, Lance file
writing, and materializer memory admission. A direct publisher exposes the same
phase snapshot. Worker counters retain observations across successive claims;
they do not introduce per-table or per-record metric labels.

Each counter records attempts started, attempts completed, returned errors,
cancelled or unwound attempts, and cumulative elapsed microseconds. Failed
attempts are a subset of completed attempts. Elapsed time includes errors and
cancellations, but excludes operations still in flight. Snapshot fields are read
independently and do not form an atomic state or define readiness.

Preparation contains state queries, source selection and pinning, memory
admission, fetching, decoding, writing, and durable preparation. Publication and
retirement also contain state queries and other WAL operations. These parent
times overlap their child phases; concurrent operations overlap each other.
Do not sum the counters into wall time or CPU time, or interpret them as latency
percentiles. Compare counter deltas over the same observed interval. The elapsed
sum covers completed plus cancelled attempts; it cannot yield a success-only
mean. Prefer quiescent snapshots when calculating a mean across terminal attempts.

Fetch covers attempts across the configured voters for one exact pinned record.
Decode/validation timing starts inside the admitted blocking closure, excluding
the blocking-pool queue. File-writing timing covers the Lance uncommitted writer,
including encoding and object-store I/O; it does not isolate network upload or
device service time. Acquisition includes its initial state validation; worker
state counters start after the acquired publisher adopts the shared observer.

Counters do not affect source pins, byte admission, retry identities, publication
authority, or safe reclamation. An admitted materialization task continues to own
its observation and memory reservation if its caller is cancelled. Its actual
fetch/decode/write completion remains observable independently of cancellation of
the enclosing preparation future.

This instrumentation is for locating waits and expensive phases. It is not a
throughput improvement and provides no proof of a disk, CPU, or network ceiling.

The annotation owner also reports five apply-path phases. `annotation_observe`
includes its quorum barrier and accepted-source query, including idle/control
polls. `annotation_fetch` times retrieval of one committed record. Unlike the
materializer's `decode_validate`, `annotation_decode` includes blocking-pool
queueing as well as IPC decoding and validation. `annotation_create` and
`annotation_patch` cover the respective staging operations after acquiring the
owner's ordered gate, including any cold Lance lookup and staging admission.
They count attempts: an ordinary backpressure or business rejection result is
not a returned error and does not mean a write was acknowledged. A retained
decoded request can make multiple apply attempts without another fetch/decode.
The decode interval follows the awaited blocking task; shutdown can cancel that
interval while already-started blocking work finishes independently.

These owner phases do not measure ingress-side append/quorum durability latency,
time waiting for the owner's ordered gate, or end-to-end ACK queueing. They can
overlap the publisher's phases. Compare their counts and elapsed deltas with the
client's create/patch ACK timing; do not treat phase sums as a latency percentile
or interpret an absent phase sample as zero I/O cost.
