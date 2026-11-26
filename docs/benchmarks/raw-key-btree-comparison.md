# Native BTree versus raw composite-key lookup

Refs #378. This local experiment evaluates an existing Lance BTree on `id`
through the actual composite `(session_id, id)` lookup. It adds no runtime policy,
index-maintenance path, schema or production configuration.

The table has 1M rows in 62 fragments, stable row IDs and one-byte unprojected
bodies. Session keys use a 65,536-value space; IDs are a deterministic 64-bit
permutation rendered as hexadecimal strings. Absent IDs interleave with present
IDs rather than lying above their maximum. Each fragment is sorted by session
and ID. Both raw and indexed sessions use admitted readahead 8, a shared 256 MiB
native/DataFusion pool, 16 MiB index and metadata caches per session, and a
separate 16 MiB key workspace. The index build uses a separate, sequential
256 MiB shared pool. Fixture creation is outside those accounting intervals.

The experiment changes query keys each round and alternates execution order.
It checks every membership result, rejects a nonexistent tuple whose individual
column values exist, finds an appended 64-row unindexed tail and preserves the
older captured snapshot. All tracked reservations release after their owning
sessions drop. It does not force index-specific admission failure.

## Result

The final test passed in 37.87 seconds on 2026-11-26; featured all-target ingest
Clippy passed in 23.16 seconds. These are **local-disk, unoptimized debug**
measurements, not release throughput or remote-store latency. Table values are
medians of three later rounds after the first query. Cache state carries across
query sizes, and the OS cache is not cleared.

| Keys | Present | Raw ms | BTree ms | Raw store bytes | BTree store bytes | Raw / BTree reads |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 32 | 0 | 445.357 | 150.740 | 22,345,464 | 1,703,360 | 62 / 40 |
| 32 | 16 | 447.776 | 110.604 | 22,345,464 | 824,690 | 62 / 72 |
| 256 | 0 | 476.823 | 679.653 | 22,345,464 | 7,276,872 | 62 / 130 |
| 256 | 128 | 496.268 | 700.093 | 22,345,464 | 7,510,112 | 62 / 360 |
| 1,024 | 0 | 511.265 | 1,336.100 | 22,345,464 | 16,444,656 | 62 / 305 |
| 1,024 | 512 | 529.623 | 1,387.640 | 22,345,464 | 20,447,400 | 62 / 1,084 |

Small probes benefit in this fixture. Larger random batches touch enough index
pages that the byte reduction shrinks and the number of reads increases. The
debug timings do not establish the release crossover point. This evidence does
not justify enabling this BTree policy for large ingestion batches; it motivates
measuring the actual key distribution, cache capacity and remote-store costs.

## Count index reads as well as data reads

`TurnLookupObservation` contains the native execution scan summary. An indexed
all-miss lookup can report zero scan bytes, requests and rows while its planning
and index search read substantial data. In the 1,024-key all-miss samples, that
summary is zero but the store observes a median 16,444,656 bytes and 305 reads.
Zero scan rows also does not establish that no index-page comparisons occurred.

The experiment therefore snapshots `dataset.object_store(None).await?.io_stats_snapshot()`
before and after each complete lookup, with no concurrent store users. These
counters include reads outside scan execution, including index loading. They
measure Lance store reads, not physical disk traffic after OS caching or network
wire bytes. The printed scan summary remains available alongside them. Do not
use execution-only bytes as a complete storage cost for an indexed query.

## Build, memory and scale limits

The index built in 3,603 ms, reading 14,484,377 store bytes in 187 reads and
writing 18,959,917 bytes including publication metadata. Index files occupy
18,950,768 bytes. Joint tracked build allocations peaked at 140,632,606 bytes;
native backed allocations peaked at 33,196,996 bytes, while the separate
DataFusion peak was 107,943,532 bytes. Lookup joint/backed peak was 20,799,864
bytes. These are not RSS limits, and category peaks must not be added.

For a capacity illustration only, this fixture's 18.95 index bytes per row would
be about 5.69 TB at 300B rows, before retained versions, cache copies and storage
system overhead. Actual key lengths, compression and fragment layouts change
that result. This test proves neither 300B-scale maintenance nor recovery time;
the 1M-row build timing must not be extrapolated into a rebuild guarantee.

```bash
CARGO_INCREMENTAL=0 RUST_MIN_STACK=16777216 \
  cargo +1.99.0 test -p lance-context-ingest \
  --features service,http-client,lance-io-metrics --lib \
  capped_btree_random_key_lookup_experiment \
  -- --ignored --test-threads=1 --nocapture
```

Use a target directory dedicated to the checkout. The fixture and index are
temporary. Every sample, store delta, native scan observation and accounting
peak is printed for reproducibility.
