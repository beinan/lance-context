# Native turn ID index integration experiment

Refs #378. This candidate is an opt-in implementation under validation, not a
throughput or production-readiness claim. It does not change the WAL payload
format or Lance OSS.

The annotation owner can schedule `turn_id_index` maintenance on the immutable
`id` column of its narrow table. It uses native BTree files and the existing
format-3 descriptors, empty-source plan, fixed target version and fenced
publication/recovery path. Installation preserves the materialized cut, request
results, body reference, active updates and captured old readers. Session index
scheduling remains separate and cannot be enabled on this owner.

The initial policy builds at most 1,048,576 rows from at most 128 fragments.
It starts when the uncovered tail reaches `min_rows` (default 65,536), or 128
fragments. Four segments in the same factor-of-four row-size class may merge,
with at most 16,777,216 input rows. It never selects all historical segments just
because a global segment count was reached. The admitted 512 MiB workspace is
an estimate, not an allocator/RSS guarantee.

Narrow compaction selects groups totaling at most 1,048,576 rows while this option
is enabled. This keeps future fragments eligible for indexing without creating
an ineffective split disguised as compaction. Enabling the option requires
stable row IDs, no deletions, existing fragments within that row bound, and an
annotation entry budget at most that bound. Existing oversized fragments need a
separate migration before this option can be enabled.

The owner derives index/compaction preference from the selected durable version.
A pending maintenance plan resolves before another plan is chosen. The existing
maintenance time window still gives waiting annotation publication its next
turn after a long operation; the window does not preempt a native build.

Qualification must include real owner create/patch traffic, native build and
merge transactions, compaction, exact request-result recovery and lost Prepare
replies. This candidate also extends the existing closed-page collector to
`_indices/<uuid>/page_lookup.lance` and `page_data.lance` in the narrow table.
It protects raw index UUIDs from retained manifests and locally held read views,
including native versions hidden from query planning. Index-root decoding has
separate manifest-size admission and shares the existing weighted session cache.
Unfamiliar index layouts and body-table indexes keep their previous retention.
No index candidate is classified during pending preparation/publication.

Long-running storage bounds, cloud throughput, cold-read cost, capped-segment
growth at large shard sizes, and multi-shard scaling remain unproven. Keep the
option disabled by default; this candidate alone does not close those tasks.
