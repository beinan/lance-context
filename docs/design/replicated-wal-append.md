# Replicated WAL append component

Tracked by [#378](https://github.com/lance-format/lance-context/issues/378).
The optional `lance-context-wal/replication` feature connects raft-rs to the shared
durable log from #399. It is an embeddable Rust component for fixed three-voter
groups. The existing production ingestion path is not changed.

## Scheduling and ownership

Open a `replication::Node` with a fresh directory, immutable `placement::Layout`,
node ID and `Limits`. The component binds its application format and limits in
the durable group log. Reopen requires identical layout, identity and policy;
unidentified logs containing entries cannot be imported implicitly. Directories
from the earlier experimental service have a different format.

The owner supplies ticks, bounded network queues and a worker for blocking log
I/O. `into_groups` consumes the aggregate owner and transfers each RawNode to an
independent worker while keeping the physical engine and sync/failure fence
shared. It never clones a group's state machine. Transport routes `Packet`s by
group and recipient, preserving the cluster/layout/policy scope. Dropping a Raft
packet is safe; retry/retransmission is Raft's responsibility. Packet scope checks
are not authenticated peer identity, nor a wire-level allocation limit.

## Append and ACK boundary

`propose(group, request, batch)` returning `Ok` means that the local leader queued
the input. It is not an ACK. `poll` performs this sequence for each Ready:

1. Persist and sync entries and HardState through the shared engine boundary.
2. Collect Ready messages and apply its committed entries only through the
   durable commit index.
3. Advance Ready, persist any LightReady commit-index update, then apply that
   committed suffix and advance the applied position.
4. Return transport messages and request completions only after all storage
   operations in this poll have succeeded.

A committed completion is consequently backed by Raft's majority replication
and the local durable commit boundary. An engine error fences every group sharing
that engine. An internal poll error also fences its driver; the owner must stop,
reconcile unknown requests and reopen. A failed poll returns no partially built
message/ACK output. Losing leadership returns `Outcome::Unknown` for unresolved
requests; it does not claim their entries were rejected or cannot later commit.

Raft stores the original shared `Bytes` in `Entry.data`; it does not copy the
body into the `Vec` accepted by `RawNode::propose`. Only bounded metadata is
encoded separately. Ingress/peer admission verifies the payload digest without
decoding Arrow. Replay validates persisted records again. This is not a claim
that protobuf networking, raft-engine or kernel I/O perform no copies.

## Retry and recovery

The replicated state machine tracks the latest `(metadata, accepted index)` for
each `(table, bucket, producer)`. New producers start at sequence one; advancing
requires the next sequence. An exact latest retry returns the original index.
A changed payload at the same sequence, a stale sequence, or a gap is rejected.
The table and bucket are part of identity, so one stream cannot consume another
stream's retry state.

Every retry in this initial component is proposed through consensus again. This
costs an extra log entry/body write, but prevents an isolated former leader from
answering directly from local state. A later quorum-read optimization must
preserve that boundary. Pending local requests bind both log index and term;
replacement of an uncommitted suffix cannot complete a different request.

Reopen reconstructs accepted metadata and latest producer state by replaying
only the durable committed prefix. The uncommitted suffix remains available to
Raft for reconciliation. Accepted pages and payload reads expose a **local
durable applied view**, not a quorum read, authenticated receipt or proof of Lance
visibility. Consumers must preserve the group/layout scope. Their use by a fenced
Lance publisher is subsequent integration.

`accepted(group, after, limit)` captures `Page { entries, through }` together.
After consuming the page, use its `through` as the next cursor; do not substitute
a separately read status position, which may include appends absent from the page.
A truncated page advances only through its last returned record. Otherwise the
cursor advances through the captured applied position, including rejected,
duplicate and blank entries. A cursor beyond this replica's applied position is
rejected, so a lagging replica cannot make the consumer move backward.

## Resource limits and unfinished integration

The immutable policy bounds record size, accepted metadata, tracked producers,
pending local requests and Raft's uncommitted-data budget. Capacity rejection is
itself deterministic after commit; an exact latest retry can still succeed when
metadata capacity is full. Historical accepted metadata and all raw log entries
are retained in this increment, including rejected or duplicate proposals.
These are not bounded physical disk usage or an unlimited-duration retention
policy. Transport must also bound frame decoding and queued messages.

Producer slot reuse, authenticated ownership/revoke, snapshot transfer, replica
replacement, disk credits, log reclamation and fenced Lance publication remain
subsequent increments. This feature is not a deployment-ready WAL service and
does not establish the complete ingestion goal or a performance result.

## Verification

Run `cargo test -p lance-context-wal --features replication --lib --test replication_process`.
The process test starts actual Rust peers with independent raft-engine files and
relays their Raft packets over TCP. It withholds links, kills a leader after an
ACK backed by only one follower, retries after failover, isolates a leader during
retry, replaces its uncommitted suffix, and restarts every process. Both groups'
accepted metadata and complete opaque bodies are compared after recovery.

The unit tests inject a real file-sync failure after a follower has received an
append, verify that no follower response or leader ACK is released, and check
the shared engine fence. They also check original body allocation ownership,
exact retries, changed/gapped sequences, metadata capacity and immutable policy
binding. These exercise real raft-rs and raft-engine, not a consensus model.
