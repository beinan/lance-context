# Durable Raft checkpoint storage

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

`GroupLog::install_snapshot` supplies the different transition needed after Raft
accepts a received snapshot. Its cut must advance beyond the previous durable
commit. It replaces the entire prior log, including any divergent uncommitted
tail, and installs the new descriptor, terms, HardState and optional suffix in
one synced batch. Suffix entries begin immediately after the new cut. The
snapshot cannot change static membership, move committed history backwards or
change a vote within the same term. Repeated/stale installs are rejected.

The engine's `Clean` command removes group metadata as well as entries. The
install batch restores the immutable application binding and every owned group
metadata key; the separate node/layout identity and sibling groups stay intact.
Before giving a snapshot to Raft, the caller must validate and durably stage its
complete application state. After receiving Ready, it must wait for this sync
before releasing responses or applying suffix entries. On an uncertain write,
retain both sets of application dependencies and recover the physical log.

These APIs supply the storage boundary. The `replication::Node` integration
uses validated [application images](replicated-wal-images.md) before invoking
them and supports explicit transfer of those immutable files. Production
transport scheduling, collector pins, physical reclamation, disk admission and
Lance publication remain subsequent integration work. No production ingestion
path or retention default changes.

Tests cover a committed cut inside a term, preserved votes and suffixes, an
empty suffix followed by append, a sibling group, repeated checkpoints, RawNode
replay from the retained suffix, invalid descriptors and corrupt recovery
metadata. Real child processes are killed before and after installation; an
injected filesystem sync failure checks the shared failure fence and complete
recovery boundary. These are storage correctness tests, not power-loss testing,
snapshot network recovery or a throughput benchmark.

Received-snapshot tests additionally exercise actual RawNode Ready output with
a new suffix, replacement below the old uncommitted last index, an empty suffix,
application-binding preservation, stale/invalid installs, process kills and
injected sync failure. Recovery must expose the complete old state or complete
new state. These tests do not authorize bootstrapping a lost disk under an old
voter identity; replica replacement still needs a separate membership protocol.
