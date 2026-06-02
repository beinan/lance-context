//! Benchmarks comparing MemWAL writes vs direct Lance Dataset::append() writes.

use std::sync::Arc;
use std::time::Duration;

use arrow_array::RecordBatchIterator;
use arrow_schema::ArrowError;
use chrono::Utc;
use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion};
use lance::dataset::{Dataset, WriteMode, WriteParams};
use lance_context_core::{CompactionConfig, ContextRecord, ContextStore, StateMetadata};
use tempfile::TempDir;

fn make_record(i: usize, bot_id: &str, session_id: &str) -> ContextRecord {
    // ~800KB text payload to simulate real context messages
    let base = format!(
        "Benchmark record {i} with realistic content. This simulates a large context message \
         containing code snippets, documentation, conversation history, and other artifacts \
         that agents typically store. "
    );
    let text = base.repeat(800_000 / base.len() + 1);
    let text = text[..800_000].to_string();

    ContextRecord {
        id: format!("rec-{i}"),
        run_id: "bench-run".to_string(),
        bot_id: Some(bot_id.to_string()),
        session_id: Some(session_id.to_string()),
        created_at: Utc::now(),
        role: "user".to_string(),
        state_metadata: Some(StateMetadata {
            step: Some(i as i32),
            active_plan_id: None,
            tokens_used: None,
            custom: None,
        }),
        content_type: "text/plain".to_string(),
        text_payload: Some(text),
        binary_payload: None,
        embedding: Some(vec![0.0f32; 1536]),
    }
}

fn make_records(count: usize, bot_id: &str, session_id: &str) -> Vec<ContextRecord> {
    (0..count)
        .map(|i| make_record(i, bot_id, session_id))
        .collect()
}

fn runtime() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap()
}

// ---------------------------------------------------------------------------
// Group 1: single_writer — 100 records total, parameterized by batch size
// ---------------------------------------------------------------------------

fn bench_single_writer(c: &mut Criterion) {
    let rt = runtime();
    let mut group = c.benchmark_group("single_writer");
    group.sample_size(10);
    group.measurement_time(Duration::from_secs(10));

    let total = 100usize;

    for batch_size in [1, 10, 100] {
        // MemWAL path via ContextStore::add()
        group.bench_with_input(
            BenchmarkId::new("wal_write", batch_size),
            &batch_size,
            |b, &bs| {
                b.iter(|| {
                    rt.block_on(async {
                        let dir = TempDir::new().unwrap();
                        let uri = dir.path().to_string_lossy().to_string();
                        let mut store = ContextStore::open(&uri).await.unwrap();
                        let records = make_records(total, "bench-bot", "s1");
                        for chunk in records.chunks(bs) {
                            store.add(chunk).await.unwrap();
                        }
                    });
                });
            },
        );

        // Direct Dataset::write/append path (no MemWAL)
        group.bench_with_input(
            BenchmarkId::new("direct_append", batch_size),
            &batch_size,
            |b, &bs| {
                b.iter(|| {
                    rt.block_on(async {
                        let dir = TempDir::new().unwrap();
                        let uri = dir.path().to_string_lossy().to_string();
                        let schema = Arc::new(ContextStore::schema());
                        let records = make_records(total, "bench-bot", "s1");
                        let mut chunks = records.chunks(bs);

                        // Create dataset with first chunk
                        let first = chunks.next().unwrap();
                        let batch = ContextStore::records_to_batch(first).unwrap();
                        let reader = RecordBatchIterator::new(
                            vec![Ok::<_, ArrowError>(batch)],
                            schema.clone(),
                        );
                        Dataset::write(
                            reader,
                            &uri,
                            Some(WriteParams {
                                mode: WriteMode::Create,
                                ..Default::default()
                            }),
                        )
                        .await
                        .unwrap();

                        // Append remaining chunks
                        for chunk in chunks {
                            let batch = ContextStore::records_to_batch(chunk).unwrap();
                            let reader = RecordBatchIterator::new(
                                vec![Ok::<_, ArrowError>(batch)],
                                schema.clone(),
                            );
                            Dataset::write(
                                reader,
                                &uri,
                                Some(WriteParams {
                                    mode: WriteMode::Append,
                                    ..Default::default()
                                }),
                            )
                            .await
                            .unwrap();
                        }
                    });
                });
            },
        );
    }

    group.finish();
}

// ---------------------------------------------------------------------------
// Group 2: multi_session — 100 records across N sessions
// ---------------------------------------------------------------------------

fn bench_multi_session(c: &mut Criterion) {
    let rt = runtime();
    let mut group = c.benchmark_group("multi_session");
    group.sample_size(10);
    group.measurement_time(Duration::from_secs(10));

    let total = 100usize;

    for num_sessions in [1, 4, 16] {
        let per_session = total / num_sessions;

        group.bench_with_input(
            BenchmarkId::new("sessions", num_sessions),
            &num_sessions,
            |b, &ns| {
                b.iter(|| {
                    rt.block_on(async {
                        let dir = TempDir::new().unwrap();
                        let uri = dir.path().to_string_lossy().to_string();
                        let mut store = ContextStore::open(&uri).await.unwrap();

                        // Build all records grouped by session, add in one call
                        // so ContextStore::add() distributes across shards
                        let mut all_records = Vec::with_capacity(total);
                        for s in 0..ns {
                            let session_id = format!("session-{s}");
                            let mut batch =
                                make_records(per_session, "bench-bot", &session_id);
                            all_records.append(&mut batch);
                        }
                        store.add(&all_records).await.unwrap();
                    });
                });
            },
        );
    }

    group.finish();
}

// ---------------------------------------------------------------------------
// Group 3: concurrent_writers — scale test with up to 2000 parallel sessions
// Each writer has its own (bot_id, session_id) = its own WAL shard.
// Total records = 10 per writer × N writers.
// ---------------------------------------------------------------------------

fn bench_concurrent_writers(c: &mut Criterion) {
    let rt = runtime();
    let mut group = c.benchmark_group("concurrent_writers");
    group.sample_size(10);
    group.measurement_time(Duration::from_secs(30));

    let records_per_writer = 10usize;

    for num_writers in [1usize, 10, 100, 500, 2000] {
        group.bench_with_input(
            BenchmarkId::new("writers", num_writers),
            &num_writers,
            |b, &nw| {
                b.iter(|| {
                    rt.block_on(async {
                        let dir = TempDir::new().unwrap();
                        let uri = dir.path().to_string_lossy().to_string();

                        // Create dataset and initialize MemWAL with a seed write
                        let mut store = ContextStore::open(&uri).await.unwrap();
                        let seed = make_records(1, "seed-bot", "seed-session");
                        store.add(&seed).await.unwrap();
                        drop(store);

                        // Spawn N writers in parallel, each with a unique shard
                        let mut handles = Vec::with_capacity(nw);
                        for w in 0..nw {
                            let uri = uri.clone();
                            handles.push(tokio::spawn(async move {
                                let mut store = ContextStore::open(&uri).await.unwrap();
                                let bot_id = format!("bot-{w}");
                                let session_id = format!("session-{w}");
                                let records =
                                    make_records(records_per_writer, &bot_id, &session_id);
                                store.add(&records).await.unwrap();
                            }));
                        }

                        for h in handles {
                            h.await.unwrap();
                        }
                    });
                });
            },
        );
    }

    group.finish();
}

// ---------------------------------------------------------------------------
// Group 4: full_cycle — Write → Compact → Read (list + search)
// Uses direct append (non-WAL) since WAL reads require LsmScanner
// integration which is not yet wired into ContextStore.
// This benchmarks the complete lifecycle with readable data.
// ---------------------------------------------------------------------------

fn bench_full_cycle(c: &mut Criterion) {
    let rt = runtime();
    let mut group = c.benchmark_group("full_cycle");
    group.sample_size(10);
    group.measurement_time(Duration::from_secs(30));

    let records_per_session = 10usize;

    for num_sessions in [1usize, 10, 100] {
        let total = num_sessions * records_per_session;

        // Phase 1: Direct write only (no WAL, immediately readable)
        group.bench_with_input(
            BenchmarkId::new("write", num_sessions),
            &num_sessions,
            |b, &ns| {
                b.iter(|| {
                    rt.block_on(async {
                        let dir = TempDir::new().unwrap();
                        let uri = dir.path().to_string_lossy().to_string();
                        let schema = Arc::new(ContextStore::schema());

                        let mut all_records = Vec::with_capacity(ns * records_per_session);
                        for s in 0..ns {
                            all_records.extend(make_records(
                                records_per_session,
                                &format!("bot-{s}"),
                                &format!("session-{s}"),
                            ));
                        }

                        let batch = ContextStore::records_to_batch(&all_records).unwrap();
                        let reader = RecordBatchIterator::new(
                            vec![Ok::<_, ArrowError>(batch)],
                            schema.clone(),
                        );
                        Dataset::write(
                            reader,
                            &uri,
                            Some(WriteParams {
                                mode: WriteMode::Create,
                                ..Default::default()
                            }),
                        )
                        .await
                        .unwrap();
                    });
                });
            },
        );

        // Phase 2: Write + Compact
        group.bench_with_input(
            BenchmarkId::new("write_compact", num_sessions),
            &num_sessions,
            |b, &ns| {
                b.iter(|| {
                    rt.block_on(async {
                        let dir = TempDir::new().unwrap();
                        let uri = dir.path().to_string_lossy().to_string();
                        let schema = Arc::new(ContextStore::schema());

                        // Write records across multiple appends (one per session)
                        for s in 0..ns {
                            let records = make_records(
                                records_per_session,
                                &format!("bot-{s}"),
                                &format!("session-{s}"),
                            );
                            let batch = ContextStore::records_to_batch(&records).unwrap();
                            let reader = RecordBatchIterator::new(
                                vec![Ok::<_, ArrowError>(batch)],
                                schema.clone(),
                            );
                            let mode = if s == 0 {
                                WriteMode::Create
                            } else {
                                WriteMode::Append
                            };
                            Dataset::write(
                                reader,
                                &uri,
                                Some(WriteParams {
                                    mode,
                                    ..Default::default()
                                }),
                            )
                            .await
                            .unwrap();
                        }

                        // Compact
                        let mut store = ContextStore::open(&uri).await.unwrap();
                        store
                            .compact(Some(CompactionConfig {
                                min_fragments: 0,
                                ..Default::default()
                            }))
                            .await
                            .unwrap();
                    });
                });
            },
        );

        // Phase 3: Write + Compact + List (read all records back)
        group.bench_with_input(
            BenchmarkId::new("write_compact_list", num_sessions),
            &num_sessions,
            |b, &ns| {
                b.iter(|| {
                    rt.block_on(async {
                        let dir = TempDir::new().unwrap();
                        let uri = dir.path().to_string_lossy().to_string();
                        let schema = Arc::new(ContextStore::schema());

                        for s in 0..ns {
                            let records = make_records(
                                records_per_session,
                                &format!("bot-{s}"),
                                &format!("session-{s}"),
                            );
                            let batch = ContextStore::records_to_batch(&records).unwrap();
                            let reader = RecordBatchIterator::new(
                                vec![Ok::<_, ArrowError>(batch)],
                                schema.clone(),
                            );
                            let mode = if s == 0 {
                                WriteMode::Create
                            } else {
                                WriteMode::Append
                            };
                            Dataset::write(
                                reader,
                                &uri,
                                Some(WriteParams {
                                    mode,
                                    ..Default::default()
                                }),
                            )
                            .await
                            .unwrap();
                        }

                        let mut store = ContextStore::open(&uri).await.unwrap();
                        store
                            .compact(Some(CompactionConfig {
                                min_fragments: 0,
                                ..Default::default()
                            }))
                            .await
                            .unwrap();

                        let results = store.list(None, None).await.unwrap();
                        assert_eq!(results.len(), total);
                    });
                });
            },
        );

        // Phase 4: Write + Compact + Search (nearest-neighbor query)
        group.bench_with_input(
            BenchmarkId::new("write_compact_search", num_sessions),
            &num_sessions,
            |b, &ns| {
                b.iter(|| {
                    rt.block_on(async {
                        let dir = TempDir::new().unwrap();
                        let uri = dir.path().to_string_lossy().to_string();
                        let schema = Arc::new(ContextStore::schema());

                        for s in 0..ns {
                            let records = make_records(
                                records_per_session,
                                &format!("bot-{s}"),
                                &format!("session-{s}"),
                            );
                            let batch = ContextStore::records_to_batch(&records).unwrap();
                            let reader = RecordBatchIterator::new(
                                vec![Ok::<_, ArrowError>(batch)],
                                schema.clone(),
                            );
                            let mode = if s == 0 {
                                WriteMode::Create
                            } else {
                                WriteMode::Append
                            };
                            Dataset::write(
                                reader,
                                &uri,
                                Some(WriteParams {
                                    mode,
                                    ..Default::default()
                                }),
                            )
                            .await
                            .unwrap();
                        }

                        let mut store = ContextStore::open(&uri).await.unwrap();
                        store
                            .compact(Some(CompactionConfig {
                                min_fragments: 0,
                                ..Default::default()
                            }))
                            .await
                            .unwrap();

                        let query = vec![0.0f32; 1536];
                        let results = store.search(&query, Some(10)).await.unwrap();
                        assert!(!results.is_empty());
                    });
                });
            },
        );
    }

    group.finish();
}

criterion_group!(
    benches,
    bench_single_writer,
    bench_multi_session,
    bench_concurrent_writers,
    bench_full_cycle
);
criterion_main!(benches);
