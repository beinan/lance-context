# Upgrading lance-context to Lance 13

Rust Lance dependencies and Python's optional `pylance` dependency now use 13.0.0.
The Rust object_store dependency moves to 0.14.2; Arrow remains at 58 and
DataFusion at 54. Python's matching lance-namespace range is 0.11.x.

## Prepare existing MemWAL data before deployment

Lance 13 reads flushed SSTables by field ID. Older flushed generations can use
positional column numbers; when schema field IDs have gaps, the old files cannot
be identified or interpreted safely by the new reader. See the upstream
[MemWAL upgrading notes](https://docs.rs/lance/13.0.0/src/lance/dataset/mem_wal/api.rs.html).

Before deploying the new binaries, stop writes and use the **old Lance version**
to flush and merge all shards into the base dataset. Confirm no old pending
generations remain, then upgrade readers and writers together. Validate rows and
payloads before resuming writes. Avoid a mixed-version rolling upgrade while old
flushed generations remain. The upstream field-ID change does not affect durable
WAL entries themselves. This code upgrade does not migrate existing datasets.

## Compatibility changes

- Use SSTable names for upstream generation metadata; context's public API names
  remain unchanged.
- Refresh authoritative shard manifests explicitly, bypassing the new cache when
  observing concurrent writer or merge progress.
- Wait on the SealFence returned by flush, propagating its errors and bounding
  the wait to the captured set of flushes.
- Preserve exact-version merge authorization across Lance's internal commit
  retries. If the predecessor changed, refresh and authorize again before any
  write at a newer version. A concurrent-commit regression test covers revocation.
- Adapt object-store pagination wrappers and retain GetResult extensions.
- Increase affected crate recursion limits to accommodate the new async types.
