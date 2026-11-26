# Physical Raft log maintenance

Tracking: [#378](https://github.com/lance-format/lance-context/issues/378).

Logical checkpointing removes entries from the retained index, but old native
segments can still occupy disk. `GroupLog::maintenance` and
`Node::prepare_log_maintenance` return one physical-engine handle. Run its `run`
method on a bounded blocking worker, once per node rather than once per group.
It asks raft-engine to rewrite retained entries and metadata before deleting
obsolete native segments. Suggested checkpoint group IDs do not authorize
logical truncation; application snapshot validation and durability still decide
that boundary. Application images and Lance objects are outside this operation.

A checkpoint may already have changed the native in-memory index while waiting
for its physical sync. Maintenance therefore drains writes through a separate
node-wide gate, acquires the shared sync gate, and persists a nonempty marker
before rewriting or deleting segments. The maintenance gate excludes payload
reads as well. Ordinary payload reads and ordinary sync barriers can still run
concurrently; readers do not take the append gate used to coalesce syncs.
The engine's empty `sync()` call does not provide that barrier. Native errors or
panics fence every group sharing the physical engine. Drop every storage and
maintenance handle before reopening after an uncertain outcome.

`LogOptions` controls local rotation and rewrite thresholds without changing the
replicated policy. Defaults rotate near 64 MiB and make explicit maintenance
eligible above 256 MiB in the append queue, with a 64 MiB rewrite threshold.
The native trigger estimates size from file count times target size. Oversized
batches can make actual file lengths larger than that estimate. These are
triggers, not hard disk limits; capacity decisions need actual disk observations.
No maintenance runs automatically.

The native pass has no per-call byte or time budget. It can delay reads and
persistence while holding the gate. A production scheduler must reserve rewrite
headroom, measure this delay and coordinate admission and maintenance. This
initial boundary does not establish bounded steady-state capacity or improve
measured throughput by itself.

The storage tests use real files and repeated append/checkpoint/rewrite cycles,
check actual retained file lengths and removed paths, preserve an uncheckpointed
sibling group, and reopen to verify every retained entry. An injected rewrite
sync failure exercises the shared failure fence and recovery. The TCP process
fixture removes native segments before transferring an application snapshot to a
lagging replica, interrupts that transfer, then restarts all replicas and checks
the snapshot, suffix, sibling data and original retry receipt.
