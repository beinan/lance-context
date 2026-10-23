# Managed Lance compaction

Part of [#378](https://github.com/lance-format/lance-context/issues/378).

Compacting a managed table must publish through its catalog, just as ingestion
does. Calling an ordinary URI-based compactor would bypass the selected manifest
and publisher fencing.

The catalog represents compaction as a publication plan with no WAL sources.
It retains the existing fixed read version, transaction, target version and
publisher authority. Only descriptor format 2, the JSON representation of a
Lance 13 `RewriteResult`, may prepare this plan. Append plans require format 1
fragment descriptors. Replicas check this distinction and the descriptor bounds
and digest; the materializer must validate the actual fragment rewrite.

Preparation, manifest selection and confirmation retain their existing exact
identities across takeover and restart. Completion requires an empty closure set,
advances the minimum Lance read version, and leaves every WAL source cursor
unchanged. A compaction plan cannot issue a source closure permit. Append
completion still requires every selected source's signed closure. The retained
completion digest distinguishes an empty source set from a damaged append receipt.

Workers can opt into automatic small-file rewriting with the materializer's
`compaction` option (`null` by default). For example, `{"target_file_bytes":
134217728, "min_fragments": 4}` targets 128 MiB output files. Each selection
contains at most 64 fragments and twice the target's stored bytes. Only sized,
nonempty fragments smaller than half the target qualify; deletion files and
indexed tables are unsupported by this initial policy.

The worker first scans every source group for pending appends. Compaction runs
only after that scan catches up: an incomplete metadata scan or an append waiting
for the normal batch threshold takes priority. A bounded rewrite already in
progress completes before later appends publish; those appends remain queryable
in the WAL. A constantly backlogged table can postpone compaction, so large append
batches remain the first defense against small files.

Preparation uses Lance 13's `CompactionTask` with `TryBinaryCopy`, which copies
encoded pages when supported and otherwise uses Lance's normal rewrite path.
The worker reserves shared workspace for the task, limits scanner batches to one
row, and retains admission if the caller is cancelled. File size and workspace
values are targets, not hard limits on Lance allocations. Publication validates
the fixed original fragments and conserved row counts, then commits the rewrite
through the same managed manifest as appends. Prepared output is reused after
takeover, including when new compaction scheduling has been disabled.

Old files remain available to captured snapshots; this path does not perform
object garbage collection. Upgrade all WAL nodes and publishers for a table
before enabling compaction: previous binaries reject the new plan and descriptor
values and cannot resume them. Keep the limits enabled on workers that may resume
a selected but not yet prepared task. Existing append commands and checkpoint
encodings remain unchanged.

Catalog tests cover prepare-format mismatches, empty-proof rejection for append,
takeover with retained prepared output, checkpoint round trips through publication,
unchanged source cursors, and a completion retry while the next append is pending.
Ingest tests use actual TLS replicas and Lance files to exercise prepared and
published recovery, concurrent session queries over published rows and pending
WAL appends, old-snapshot reads, and append batching priority.
