# Capped native raw-key lookup scale experiment

Refs #378. This experiment exercises the actual Rust/Lance lookup against local
datasets with 1 million and 4 million rows. It adds no runtime behavior or defaults.
The tested runtime is `c1f18c5a115166437d3894ad4b99dddbb0517b4a`.

Both dataset sessions use the same DataFusion memory pool. One also attaches that
pool as its native memory allocator, enabling the existing admitted readahead
path. That path uses fragment/batch readahead 8, a 1 MiB decoded-batch target and
an 8 MiB I/O target; the session without native admission stays at readahead 1.
The targets are not memory caps. The test pool admits at most 256 MiB across
tracked native and DataFusion allocations; key workspace has a separate 16 MiB
budget. Neither budget bounds process RSS or allocations outside those pools.

The fixture streams batches of 16,384 rows into equally bounded fragments, with
stable row IDs and 65,536 deterministic pseudorandom session names sorted within
each fragment. It has no index and a one-byte body that lookup never projects.
Each lookup requests 1,024 composite keys, either all missing or half present.
Both modes warm their own sessions, followed by three rounds with alternating
execution order. The test verifies every returned membership result. It also
sets native admission to one byte, verifies a typed backpressure error instead
of false absence, restores capacity and verifies successful lookup. Reservations
must be released after every lookup and the forced refusal.

## Observed results

The two selected tests passed in 90.79 seconds on 2026-11-26. Timings below are
medians of three warm samples, in an **unoptimized debug build on local disk**.
They are diagnostic comparisons, not release throughput, production Blob
measurements, p99 estimates or end-to-end ingestion capacity.

| Rows | Present / requested | Serial ms | Admitted ms | Serial / admitted |
| ---: | ---: | ---: | ---: | ---: |
| 1,000,000 | 0 / 1,024 | 1,345.800 | 530.520 | 2.54 |
| 1,000,000 | 512 / 1,024 | 1,254.995 | 536.582 | 2.34 |
| 4,000,000 | 0 / 1,024 | 4,691.608 | 1,998.852 | 2.35 |
| 4,000,000 | 512 / 1,024 | 4,754.530 | 2,069.106 | 2.30 |

Every warm 1M scan read 17,517,120 bytes in 62 native requests across 62
fragments. Every warm 4M scan read 70,041,032 bytes in 245 native requests across
245 fragments. These counters matched between modes and hit ratios. All scans
examined the full dataset: concurrency reduces waiting without reducing the
amount of historical key data scanned. This remains a cost that grows with the
table. An effective index or partitioning strategy requires separate evidence.

| Rows | Joint accounted peak, bytes | Native backed peak, bytes | Read peak | Decode peak |
| ---: | ---: | ---: | ---: | ---: |
| 1,000,000 | 3,258,749 | 3,248,333 | 2,582,920 | 743,137 |
| 4,000,000 | 3,325,121 | 3,306,413 | 2,584,336 | 756,192 |

Category peaks occur at different times and must not be added. Tracked Index
and DataFusion operator peaks were zero in this plain scan, so this does not
exercise simultaneous native/DF operator pressure. Both scales returned all
reservations and backed allocations to zero. The refusal is deliberately forced;
this experiment does not establish a safe production capacity or saturation
behavior under mixed ingestion, query, compaction and indexing work.

## Reproduce

Use a Cargo target directory dedicated to this checkout. Reusing the same target
between worktrees with different path-patched Lance sources can reuse stale
dependency artifacts. Fixture directories are temporary and removed on exit.

```bash
CARGO_INCREMENTAL=0 RUST_MIN_STACK=16777216 \
  cargo +1.99.0 test -p lance-context-ingest \
  --features service,http-client,lance-io-metrics --lib readahead:: \
  -- --include-ignored --test-threads=1 --nocapture
```

The existing small test additionally checks early-hit cancellation. The scale
fixture is ignored by default and prints every sample's native scan counters
plus peak accounting. Do not compare its debug elapsed times directly with
release experiments or substitute them for sustained publication/drain results.
