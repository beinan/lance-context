# Application images for replicated WAL recovery

Refs https://github.com/lance-format/lance-context/issues/378.

The optional replication component can explicitly checkpoint a committed applied
cut while retaining its accepted payloads and exact producer retry state. A
lagging member of the same static group can receive that image and then replay
the suffix. No existing production writer or periodic maintenance setting changes.

## Build and install

`Node::prepare_checkpoint(group, id)` captures bounded metadata, the committed
cut and its term, plus handles to the original log and previous immutable image.
It does not copy payloads. Run the resulting `CheckpointTask` on a blocking
worker, while continuing to tick and poll the node. A single permit shared by
all groups on the physical node limits concurrent image builders/receivers and
prevents a second checkpoint from compacting the task's captured inputs. The
permit remains held until the prepared candidate is installed or dropped.

The worker streams metadata and one bounded record at a time into immutable
files created with `create_new`. It validates each body against its captured
metadata, syncs both files and their directory, and returns a non-forgeable
`PreparedCheckpoint`. File names use only a group number and caller-supplied
fresh UUID; a failed attempt cannot overwrite an installed image or orphan.

`Node::install_checkpoint` validates ownership and durably installs the local
descriptor through the shared Raft log checkpoint boundary. The cut may precede
the node's current applied index: intervening committed and uncommitted entries
remain in the log. `read_accepted` uses packed bytes for accepted prefix entries
and ordinary log reads for suffix entries, validating their body digest through
the same boundary. A failed installation fences the node. Retain all old/new
dependencies until reopen resolves an uncertain engine write.

## Transfer before Raft acceptance

When `poll` emits `MsgSnapshot`, immediately capture its `Image` using
`snapshot_image` before another local checkpoint replaces the current image.
Keep this handle for the transfer. A later obsolete message can be discarded and
reported with `report_snapshot(..., false)` so Raft retries. Ordinary `step`
continues to reject snapshot messages.

At the recipient, `receive_snapshot` checks the packet's static scope, policy,
route and bounded descriptor before creating a receiver with a fresh local UUID.
Send the two files through sequential chunks of at most one MiB. File I/O and
network waits stay outside the Raft owner; the owner can keep processing ordinary
traffic during the transfer. Supply transport authentication, deadlines and
node-wide byte/disk admission separately. Scope equality is not authentication.

`SnapshotReceiver::finish` requires exact lengths and hashes, validates metadata,
record hashes and producer tails, and syncs files and their directory. Only then
does `step_snapshot` pass the locally named descriptor to RawNode. Raft can ignore
an image that became stale during download. If it accepts the image, `poll`
atomically persists the descriptor, replacement suffix and HardState before
releasing messages, swaps application state, and applies later committed entries.
Old unresolved local requests return `Unknown` and require exact retries.

A lost volume must not reuse an old Raft voter identity with empty state. This
static-membership transfer API recovers lagging retained replicas; allocating a
new voter identity and committing its membership transition remain separate work.

## Recovery and format bounds

The versioned descriptor binds cluster, layout fingerprint, application policy,
group, static voters, index and term, file identity, counts, lengths and hashes.
The entire Raft Snapshot fits in four KiB. Its metadata file repeats the binding
and frames each record independently. Counts are validated before iteration;
frames have a fixed small maximum and never deserialize a payload-sized vector.
Recovery streams payload verification through a 64 KiB scratch buffer.

Format v1 retains all accepted metadata and stores exact producer tails. It
checks that each producer's accepted sequences start at one and are contiguous,
and that its saved tail is precisely the latest accepted index and metadata.
Blank, duplicate and rejected log entries may leave gaps between accepted indexes.
Missing files, wrong policy, duplicate indexes, unexpected lengths, trailing
bytes or mismatched checksums fail recovery; orphan images never become fallback
state. Reopen loads only the descriptor selected by durable Raft storage.

This representation intentionally precedes publication-aware retirement. Retiring
accepted records will require a new version that validates independently retained
producer tails and publication/source state. Full images currently copy retained
payloads again; they are not incremental checkpoints. Reusing immutable segments,
budgeting rewrite headroom, tracking collector pins and safe physical collection
remain necessary for sustained throughput and bounded disk usage. No Arrow
decoding occurs in the image path, but pack construction and file reads still
copy bytes; this is not an end-to-end zero-copy claim.

## Verification

Run the replication library and real-process suite with:

```bash
cargo test --locked -p lance-context-wal --features replication --lib --test replication_process
```

The suite exercises append progress after a captured cut, exact prefix/suffix
payload recovery, producer retries after packing, shared builder admission,
checksum/scope/length failures, a sibling group and sync-failure fencing. Actual
Rust child processes transfer bounded chunks over TCP; the parent kills a process
after file sync but before installation, interrupts a follower's transfer with
SIGKILL, retries using fresh file identities, and kills/restarts all peers after
successful installation. These tests must pass before this increment is shipped;
they do not measure power-loss durability or production throughput.
