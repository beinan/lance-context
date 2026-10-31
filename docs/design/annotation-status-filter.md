# Materialized status queries

`POST /api/v1/annotations/{table}/filter-status` accepts a configured `bucket`, a
nullable `status` and a `limit` from 1 to 1024. It returns matching turn IDs from
one captured Lance snapshot. `status: null` selects cleared status values. String
predicates use typed expressions, including strings containing SQL punctuation.

The owner captures the selected base, `materialized_through` and
`applied_through_at_capture` together. The returned scope identifies the table
instance, WAL group and shard. These are log positions, not row counts or seconds
of delay. The result also reports BTree-covered/uncovered fragment counts and unindexed
physical rows (null if native counts are unknown). Native filtering includes the
unindexed Lance tail, so index catch-up affects query cost, not snapshot accuracy.

Active and frozen updates become filter-visible after materialization. Immediate
point reads continue to include them. A filter keeps its captured native files
protected through concurrent publication and collection. Replaced owners fence
their results after query I/O, and replacements wait for their acquisition cut
before capturing query progress. Query handlers do not scan or decode the WAL.

Existing ingress routes select the requested physical shard. Queries for distinct
buckets can run concurrently through either ingress; each result has its own
scope and cut. This endpoint does not imply a cross-shard atomic snapshot or
automatic all-shard aggregation. IDs are unordered; `truncated` means the row or
response byte bound was reached. Cross-request pagination is not provided.

Only IDs are projected; predicates read status, never turn bodies. Existing
control admission bounds concurrent requests. A query reserves shared workspace
without queuing ahead of ingestion at either ingress or owner, retains its reader
and workspace through native work and its response workspace through
response delivery, and fails promptly if capacity is unavailable. Row and escaped
JSON byte limits bound responses. Scanner vectors and read-ahead are limited;
the workspace estimate is not a hard cap on native index allocations or total
scan I/O. Large-shard query cost and sustained filtering/ingestion coexistence
still need measured qualification.
