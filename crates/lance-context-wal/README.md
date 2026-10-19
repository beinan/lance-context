# WAL component experiments

Feature-gated Rust measurements for choosing a WAL payload path and durable
local log engine. This crate does not change production ingestion or Lance OSS.

Implementation and validation are tracked in
[#378](https://github.com/lance-format/lance-context/issues/378). Related PRs should
include `Refs #378`; individual steps should leave the tracking issue open.

The independent `contract` feature defines ingestion routing keys, the schema
fingerprint, immutable-ID policy and record byte/row limits. Validation checks
the representation after decoding; callers must durably bind and compare the
complete contract for each table generation. The ingress still validates actual
Arrow input, and the materializer must support the registered maximum record.
This component does not enforce persistence, authentication or ID uniqueness.
Schema hashing accepts already canonical bytes; it does not normalize JSON or
parse Arrow. The module requires neither Arrow nor a replication/log engine.

```bash
cargo test -p lance-context-wal --features contract --lib
```

The optional `arrow-contract` feature adds
`ingest_contract::arrow_schema_fingerprint(&Schema)` for ingress and materializers.
It recursively sorts JSON object keys, including nested metadata, and preserves
field order, types, nullability and serialized dictionary attributes. Calculate
it when binding a table generation, then retain the fingerprint on replicas.
Only Arrow schema metadata is required; this feature does not decode IPC or load
a replication engine. Arrow serialization changes require compatibility review
before upgrading writers of existing contracts.

```bash
cargo test -p lance-context-wal --features arrow-contract --lib
```

The optional `record` feature provides opaque `Batch`/`BatchMeta` envelopes with
shared `Bytes` payload ownership. `BatchMeta::check_body_length` validates frame
lengths and a caller-supplied data/control limit before payload allocation;
`Batch::validate` also checks the complete SHA-256 digest. Construction and cloning
retain the input allocation. Digest computation scans the payload; this is not
end-to-end zero-copy. Replication can use this feature without Arrow or a log engine.

Routing, producer sequencing/fencing, control commands and durable commit still
require separate validation. The record domain tags reserve the single-group,
multi-group and source-control formats used by the ingestion experiments. This
PR adds the envelope, not their state machines. Serialize metadata separately
from bulk bytes in network paths; `Debug` excludes the body.

```bash
cargo test -p lance-context-wal --features record --lib
```

The optional `ipc` feature exposes `ipc::decode(Bytes, expected_schema)` for
materializers. It decodes multiple uncompressed record batches and dictionaries
while sharing binary payload ownership with the input. Numeric alignment repair,
schema parsing and Arrow metadata still allocate. The codec validates footer and
block/array-buffer ranges, native endianness, schema equality, row/count limits
and unsupported expansion before constructing arrays. Compressed batches and
dictionary deltas are rejected.
Callers still reserve input and decoding workspace within their own memory budget.
Replication does not need to enable this module or decode Arrow. The `lab` codec
measurement now uses the same validated path, including its validation overhead.
Arrow dependency panics from malformed metadata become errors in panic-unwind
builds; panic-abort builds cannot recover from dependency panics.

```bash
cargo test -p lance-context-wal --features ipc --lib
```

The independent `placement` feature provides `placement::Layout`, session bucket
hashing and scoped `Receipt` types. A validated layout assigns each stable virtual
bucket to one RF3 group, with explicit voters and StatefulSet DNS addresses. Use a
canonical immutable tenant/table identity in the routing key, not a mutable display
name. Worker count does not participate in routing. Persist the layout and its
fingerprint; changing it requires a migration protocol, which is not implemented
here. A receipt identifies a position and is not authentication or proof of commit.

```bash
cargo test -p lance-context-wal --features placement --lib
```

Compatibility tests preserve a synthetic layout fingerprint and a fixed
session-hash vector. Other tests reject ambiguous ownership
and ensure routes survive JSON serialization and restart.

The independent `admission` feature provides one proposal budget shared by all
groups on a node. Data proposals leave 25% of the configured budget (up to
256 MiB) for control traffic. Reservations release on drop; encoded engine-write
counters account for peer, control and rewrite traffic between directory scans.
Refresh measures file lengths and allocated blocks, rather than estimating
segment sizes. Successful admission does not walk the directory.

`Budget::limit_data` can lower the shared producer ceiling to leave more space
for checkpoint copies and log rewrites. It never raises the total budget or
cancels outstanding reservations. Maintenance callers use the total-budget
reservation path; using producer admission for checkpoints can prevent recovery
of space precisely when producers are throttled. The default 25% control window
alone does not guarantee enough temporary space for full-copy maintenance.

This is proposal admission, not filesystem space reservation or a limit on
already accepted replication traffic. Callers must supply a monotonic write
counter covering every group and protect file lifecycle changes with the shared
lock. Snapshot collection and replica-pressure coordination are still required
for sustained operation. No production writer is connected to this module yet.

```bash
cargo test -p lance-context-wal --features admission --lib
```

```bash
cargo build --release -p lance-context-wal --features lab --bin context-wal-lab
target/release/context-wal-lab codec --payload-bytes 262144 --rows 8 --iterations 30
target/release/context-wal-lab disk --directory /tmp/wal-disk-new \
  --payload-bytes 262144 --batch 4 --writers 4 --groups-per-writer 4 --rounds 64
```

`codec` measures encoding, Arrow FileReader, shared-buffer FileDecoder, an
unaligned shared buffer, and decoding followed by per-row payload copies. All
paths use the same IPC bytes; data generation and correctness checks are outside
timing. JSON reports time, allocation count, allocated bytes and logical IPC
MiB/s. Shared-buffer decoding may inspect only metadata and retain payload
buffers: logical IPC MiB/s is not memory-copy bandwidth or a full payload scan.
Unaligned numeric buffers may need repair copies. The allocator counts allocation
requests, including reallocations, rather than peak or retained memory.

`disk` rotates each writer over separate Raft groups sharing one real raft-engine.
Every nonempty write uses `sync=true`; JSON includes batch latency, payload
throughput, engine bytes, reopen time and separate verification time. It reopens
the engine and compares every persisted payload. Inputs are deterministic bytes,
prepared before timing and reused across rounds. Compression is disabled unless
`--compression-threshold` is supplied. The directory must be new and remains for
inspection. This measures local durable writes, not replicated ACKs, crashes,
power loss or Blob/Lance publication.

The pinned raft-engine 0.4.2 has a separate `sync-probe` command to inspect empty
sync versus a nonempty synced barrier:

```bash
strace -f -e trace=fsync,fdatasync target/release/context-wal-lab \
  sync-probe --directory /tmp/wal-sync-new
```

Use the printed markers to associate syscalls with each phase. Do not infer a
durability boundary from an empty batch without checking the engine's behavior.
For comparisons, preserve the source commit, binary hash, release/debug mode,
command, filesystem/storage placement and JSON. CI runs tiny correctness smoke
cases; their numbers are not performance evidence.
