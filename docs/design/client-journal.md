# Durable client state with group commit

Tracked by [#378](https://github.com/lance-format/lance-context/issues/378).
The optional `client-journal` feature provides a local, bounded latest-value
journal for a recoverable ingestion client. Independent writers can share one
physical synchronization barrier while retaining exact pending request bytes and
ACK checkpoints. It uses the existing pinned raft-engine dependency.

This component does not replicate client state, allocate producers, implement an
HTTP SDK or enable the distributed ingestion path. Callers supply the protocol
state, single-owner scope and recovery validation. The remote WAL still needs its
own durable quorum; a local journal completion cannot substitute for that ACK.

## API contract

`Journal::open(path, scope, keys, settings)` opens the engine and returns a handle
plus the latest optional value for each fixed key. It binds version, scope, key
count and settings durably; reopening with a different binding fails. The caller
must provide an existing parent directory; a bare relative name uses the current
directory. Open synchronizes the engine directory and that parent, and rejects
missing parents without creating an unsynchronized ancestor chain. The caller
must distinguish first initialization from a missing/lost journal and validate
every recovered state before sending any network operation. The engine locks its
directory, while the caller owns any surrounding state and ownership protocol.

`put(key, prefix, body).await` persists the concatenated bytes. The two `Bytes`
parts remain shared until the disk worker assembles an engine value. This avoids
caller-side payload concatenation; engine serialization, checksums and recovery
still copy. The latest value replaces the old value for that key. Concurrent
updates of the same key require the caller's ordering discipline.

A recoverable client normally writes the exact intent before sending, then writes
the ACK checkpoint before advancing the producer sequence. Multiple such writes
may share a disk barrier. This does not make independent network operations
atomic. A cancelled or failed wait can have an unknown durable outcome: recover
and retry the original protocol operation, never invent a new identity because
the wait ended.

Open performs blocking disk I/O. Dropping the last handle joins the disk worker
and can wait for pending disk work; place these operations appropriately in an
async application. Keep ownership until the last handle has closed. Copying the
directory does not transfer or fence producer ownership in another process.

## Durability and bounds

The dedicated worker gathers requests up to a byte/count limit, waits at most the
configured intentional linger (0–25 ms), and writes a nonempty synced engine
batch. Actual completion can take longer due to queueing and storage. The pinned
engine's empty `sync()` is a no-op and is not used for completion.

Queued request bytes retain semaphore permits through completion. A separate
latest-state limit bounds logical live values. Disk admission measures regular
files and reserves live-state, rewrite and segment headroom. Engine-managed purge
reclaims obsolete segments; the journal never deletes them itself. These are
admission limits, not an OS disk quota or an RSS bound: engine/encoding buffers,
metadata, allocator overhead and filesystem behavior need separate measurement.
Caller-owned buffers waiting to enter admission are also outside this bound;
applications should bound producers before constructing those buffers.

Mutation errors and unwinding dependency panics permanently fence the journal
before successful completion can escape. All handles must be dropped and recovery
performed on a fresh engine; a fence cannot be reset. The owner retains the engine
until its worker has stopped, avoiding destruction during mutation unwinding.
Dependency destructor failures or panic-abort builds can still terminate the
process. Process restart and storage power-loss guarantees are distinct.

Statistics report successful groups/requests/bytes, maximum group size, engine
write time, maintenance and admission wait. Write time includes engine encoding,
append and synchronization; admission wait excludes subsequent worker/linger
waits. These counters do not establish end-to-end ingestion throughput.

## Validation

Real-file tests cover concurrent durable replacements, repeated reclamation,
complete latest-value recovery, immutable binding and exclusive ownership,
admission limits, cancellation during actual sync, and injected sync failure with
no premature successful waiter. A parent test invokes the ignored Rust child
fixture, kills it immediately before and after Rewrite-queue sync, then reopens
the files and compares every acknowledged value, including cold retained state.

CI runs the optional feature explicitly. Run it locally with:

```sh
cargo test -p lance-context-wal --features client-journal --lib client_journal
```

These tests qualify local process recovery on retained files, not volume loss,
distributed producer ownership or a production performance result. Protocol-level
ingress integration and performance comparisons remain tracked separately in #378.
