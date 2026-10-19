# WAL component experiments

Feature-gated Rust measurements for choosing a WAL payload path and durable
local log engine. This crate does not change production ingestion or Lance OSS.

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

Compatibility tests preserve a layout fingerprint recorded by a real three-node
experiment and a fixed session-hash vector. Other tests reject ambiguous ownership
and ensure routes survive JSON serialization and restart.

The independent `admission` feature provides one proposal budget shared by all
groups on a node. Data proposals leave 25% of the configured budget (up to
256 MiB) for control traffic. Reservations release on drop; encoded engine-write
counters account for peer, control and rewrite traffic between directory scans.
Refresh measures file lengths and allocated blocks, rather than estimating
segment sizes. Successful admission does not walk the directory.

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
