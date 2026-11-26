# Session/turn lookup on the owner main

Full-body owners can opt into bounded native ZoneMap maintenance for an existing
derived key column. This is a runtime option, not a new business identity:

```rust
use lance_context_ingest::materializer::SessionTurnIndex;

configuration.publication.session_turn_index = Some(SessionTurnIndex::hex_utf8_v1());
// Pass this configuration to the existing Runtime::connect_body_shard(..., &spec).
// Keep spec = (original body schema, [session_id, id], routing=session_id).
```

The original schema must already contain non-null UTF8 `session_turn_key` and
metadata `trace.session_turn_key=hex-utf8-v1`. Its value is `v1:` followed by
lowercase UTF8 hex of session_id, `/`, and lowercase UTF8 hex of id. No fields,
schema metadata, durable specification, fingerprint, producer state or WAL scope
are added or rewritten. Other full-body schemas keep the original lookup path.

The owner builds `session_turn_zonemap_v1` on its authoritative selected main.
An independent search or detached version does not supply coverage for this
lookup. Each build validates the three narrow identity columns, processes at
most 128 fragments and 1048576 physical rows, and uses 1024 rows per zone. The
fragment limit can be lowered through `SessionTurnIndex.max_fragments`.
Four similarly sized segments can merge, capped at 16777216 represented rows.
The existing 512 MiB index reservation is an admission estimate, not a cap on
native allocations. The publication budget must accommodate it; background
preparation additionally leaves an ordinary publication's workspace available.
Current fragments and retained generations must satisfy the 1048576-row bound.

Lookup prunes validated main coverage using the derived-key IN predicate. It
probes uncovered fragments using original session/id predicates, checks exact
original tuples in every result, and captures both parts from one compatible
base/staging view. New writes and WAL replay validate derived values using
borrowed Arrow strings. The first-create identity and request results are
unchanged. No body projection or WAL-tail scan is added to existence lookup.
If native summaries are incomplete, combined counters remain unavailable.

Preparation uses the existing bounded maintenance scheduling, frozen-state
guards, fenced publication, retained descriptors and recovery. Configuration
can be disabled after restart without losing the ability to resolve an already
prepared index publication. An index prepared in the background is rebased only
onto unchanged physical fragments; append tails remain uncovered until a later
maintenance turn. Compaction withdraws coverage of rewritten ZoneMap fragments;
the new fragments use original-key scans until indexed again. Old snapshots
retain their original index coverage. The existing raw manifest protection
also protects ZoneMap files before collection.

ZoneMap ranges can overlap heavily on unordered keys. Correctness tests and
local pruning examples do not establish production throughput; measure scanned
rows/ranges, lookup latency, materialization and backlog on the real workload.
