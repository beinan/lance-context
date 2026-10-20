# Durable local Raft checkpoint boundary

Refs https://github.com/lance-format/lance-context/issues/378.

The optional `raft-log` storage supports a local application checkpoint at a
durable committed cut. `GroupLog::checkpoint` persists its bounded Raft Snapshot,
the retained term ranges and logical prefix compaction in one engine batch,
then waits for the existing shared physical sync barrier. A checkpoint does not
change HardState or remove any suffix entries, including uncommitted entries.
Only an identical descriptor can be retried at the same cut.

The caller must first validate the complete application state at that exact
applied cut. If Snapshot.data references files, it must validate their scope,
contents and dependencies, fsync those immutable files and their directory, and
retain both previous and new dependencies after an uncertain write. The storage
layer deliberately treats Snapshot.data as opaque. The entire encoded Snapshot
is limited to 4 KiB; bulk payloads belong in separately managed files. This API
does not verify Lance publication or authorize deletion of those files.

Recovery checks the snapshot membership and boundary against HardState, retained
term ranges and the physical engine's entry bounds. The compacted boundary's term
remains readable; earlier terms and entries return `Compacted`. An empty suffix
still has the snapshot's index as its logical last index. A newer requested
snapshot returns `SnapshotTemporarilyUnavailable`. Logs without a checkpoint
continue to start at index 1. An application binding must precede checkpointing.
Missing, corrupt or inconsistent checkpoint metadata fails recovery.

Compaction uses an explicit `Command::Compact` in the same synced batch as its
metadata. The pinned engine's convenience `compact_to` method is insufficient:
it does not sync and does not return failures. Shared engine failures invalidate
all group handles before any subsequent successful operation.

This increment supplies the storage boundary only. The current
`replication::Node` still retains its complete log and does not invoke this API.
Application state/payload packs, remote snapshot installation and transfer,
reader/transfer pins, physical reclamation, admission during maintenance and
Lance publication remain subsequent integration work. No production ingestion
path or retention default changes.

Tests cover a committed cut inside a term, preserved votes and suffixes, an
empty suffix followed by append, a sibling group, repeated checkpoints, RawNode
replay from the retained suffix, invalid descriptors and corrupt recovery
metadata. Real child processes are killed before and after installation; an
injected filesystem sync failure checks the shared failure fence and complete
recovery boundary. These are storage correctness tests, not power-loss testing,
snapshot network recovery or a throughput benchmark.
