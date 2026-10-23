# Managed session indexes

Part of [#378](https://github.com/lance-format/lance-context/issues/378).

Session reads filter published Lance rows by `rollout_id` and combine them with
the acknowledged WAL tail. A native BTree index lets Lance skip unrelated
published fragments. Unindexed fragments and the WAL tail remain queryable.

Set the materializer's optional `session_index` to
`{"min_fragments": 16, "max_fragments": 128, "max_segments": 16}` to enable it.
It is disabled by default and requires a table created with stable row IDs.
Existing tables are not migrated. Upgrade every catalog voter and publisher
before enabling descriptor format 3, and keep scheduling options on workers that
may resume selected work which has not yet prepared its output.

Each build indexes only uncovered fragments, up to the configured fragment
count and 1,048,576 rows. Fragments with deletion files, unknown row counts or
too many rows for the selection are skipped. Fewer than `min_fragments` eligible
fragments wait for later publications; they remain readable by scanning. The
native build reads the session key and row IDs, not the payload columns.
At `max_segments`, native index files are consolidated before adding another
segment. Consolidation reads the existing indexes and can grow with indexed key
volume. Both operations reserve an estimated 512 MiB from the shared workspace;
this is admission accounting, not a hard limit on native allocations.

Index maintenance uses the existing source-free, fenced publication flow.
Preparation writes native files without changing the selected manifest. Format
3 records their native metadata, fixed read version and replaced segment IDs.
Publication validates coverage and commits `CreateIndex` at the fixed target.
Prepared output survives takeover with new index scheduling disabled. Completion
advances the Lance version without advancing WAL cursors. Existing snapshots
retain their index files; this path does not delete old objects.

When both policies remain eligible, maintenance preference rotates between
index work and small-file compaction using the fixed read version divided by
two. Each append/maintenance pair advances two versions, so neither policy
can continually displace the other during backlog; takeover preserves the
preference. There is at most one maintenance operation between backlogged
appends. Indexing and compaction share the active publisher, so measure their
effect on ingestion before enabling them for a workload. Indexed compaction
groups fragments by coverage in every segment and preserves stable row IDs.

The Rust integration test exercises preparation, two WAL-directory restarts,
recovery after manifest selection, concurrent session reads including an
unpublished tail, indexed compaction, incremental index creation and native
segment consolidation. It checks full rows, unchanged WAL cursors, preserved old
snapshots and rejection of incorrect replacement fragment IDs.
