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
most 32 fragments and 262144 physical rows, and uses 1024 rows per zone. The
fragment limit can be lowered through `SessionTurnIndex.max_fragments`.
Four similarly sized segments can merge, capped at 4194304 represented rows and
1024 physical fragments. Larger existing segments remain readable and are not
selected for these merges.

The ZoneMap uses a full 256 MiB reservation from the existing shared publication
budget; the other index types keep their 512 MiB reservations. Validation and
training share one ordered narrow scan: 2048-row/1 MiB batch targets, an 8 MiB I/O
buffer, one fragment and one decode batch in flight, and a 32 MiB DataFusion
pool. The native trainer receives shared Arrow key/address arrays; there is no
second scan, sort, body read, or value-buffer copy. These controls and the
reservation are admission estimates, not an allocator/RSS cap on native pages,
variable-width values, caches, or index summaries. Oversized native pages may
exceed scanner byte targets.

A 256 MiB owner performs this work serially. Background preparation additionally
requires an ordinary publication's workspace to remain available. New compaction
groups obey the smaller build row bound. Existing fragments larger than 262144
rows remain on the correct raw lookup path; enabling this option does not split
them. Current fragments and retained generations still must satisfy the original
1048576-row validation bound.

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
