//! Explicit local-disk experiment; timings are not production throughput gates.
use super::*;

const LIMIT: usize = 256 << 20;
const FRAGMENT_ROWS: usize = 16_384;
const KEYS: usize = 1024;

fn session(row: usize) -> String {
    let mixed = (row as u64).wrapping_mul(0x9e37_79b9_7f4a_7c15);
    format!("session-{:08x}", (mixed ^ (mixed >> 32)) & 0xffff)
}

fn fixture_batch(
    start: usize,
    end: usize,
) -> std::result::Result<RecordBatch, arrow_schema::ArrowError> {
    let mut order = (start..end)
        .map(|row| (session(row), row))
        .collect::<Vec<_>>();
    order.sort_unstable();
    RecordBatch::try_new(
        schema(),
        vec![
            Arc::new(StringArray::from_iter_values(
                order.iter().map(|(key, _)| key),
            )),
            Arc::new(StringArray::from_iter_values(
                order.iter().map(|(_, row)| format!("turn-{row:08x}")),
            )),
            Arc::new(StringArray::from(vec![None::<&str>; order.len()])),
            // Only identity columns are scanned. Keep fixture creation bounded.
            Arc::new(LargeBinaryArray::from_iter_values(
                order.iter().map(|_| b"x".as_slice()),
            )),
            Arc::new(Float64Array::from(vec![None; order.len()])),
        ],
    )
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "local scale experiment writes 1M and 4M rows; run explicitly"]
async fn capped_native_readahead_scale_experiment() -> Result<()> {
    for rows in [1_000_000usize, 4_000_000] {
        run_scale(rows).await?;
    }
    Ok(())
}

async fn run_scale(rows: usize) -> Result<()> {
    let root = tempfile::tempdir()?;
    let uri = root.path().join("body.lance");
    let uri = uri.to_str().unwrap();
    let input = (0..rows)
        .step_by(FRAGMENT_ROWS)
        .map(move |start| fixture_batch(start, (start + FRAGMENT_ROWS).min(rows)));
    let dataset = Dataset::write(
        arrow_array::RecordBatchIterator::new(input, schema()),
        uri,
        Some(WriteParams {
            max_rows_per_file: FRAGMENT_ROWS,
            max_rows_per_group: FRAGMENT_ROWS,
            enable_stable_row_ids: true,
            skip_auto_cleanup: true,
            ..Default::default()
        }),
    )
    .await?;
    let fragments = dataset.fragments().len();
    assert_eq!(fragments, rows.div_ceil(FRAGMENT_ROWS));
    drop(dataset);

    // Both paths share the same capped DataFusion pool. Only the admitted path
    // attaches native accounting, exactly as the production lookup gate does.
    let memory = Arc::new(Memory::new(LIMIT));
    let runtime = RuntimeEnvBuilder::new()
        .with_memory_pool(memory.clone())
        .build_arc()?;
    let mut bases = Vec::new();
    for native in [false, true] {
        let session = Session::default().with_runtime_env(runtime.clone());
        let session = if native {
            session.with_native_memory(memory.clone())
        } else {
            session
        };
        bases.push(Arc::new(
            DatasetBuilder::from_uri(uri)
                .with_session(Arc::new(session))
                .load()
                .await?,
        ));
    }
    let spec = spec()?;
    // Key-window admission is separate from the native/DF budget and is not RSS.
    let workspace = MergeMemoryBudget::new(16 << 20);
    for hits in [0, KEYS / 2] {
        let keys = (0..KEYS)
            .map(|index| {
                let row = index * 3919 % rows;
                let id = if index < hits { row } else { rows + index };
                spec.business_key(&[&session(row), &format!("turn-{id:08x}")], 1024)
            })
            .collect::<Result<Vec<_>>>()?;
        let keys = keys.iter().map(String::as_str).collect::<Vec<_>>();
        if hits == 0 {
            memory.limit.store(1, Ordering::SeqCst);
            let refused = spec
                .lookup_existing(bases[1].clone(), &keys, &workspace)
                .await
                .err()
                .expect("a native refusal must not report absent keys");
            assert!(refused.chain().any(|cause| cause
                .downcast_ref::<lance::Error>()
                .is_some_and(lance::Error::is_backpressure)));
            memory.released().await?;
            memory.limit.store(LIMIT, Ordering::SeqCst);
        }
        // Warm both sessions; alternate order afterwards. Do not label these
        // cold reads or flush the host's shared filesystem cache.
        for round in 0..4 {
            for mode in if round % 2 == 0 { [0, 1] } else { [1, 0] } {
                let started = std::time::Instant::now();
                let lookup = spec
                    .lookup_existing(bases[mode].clone(), &keys, &workspace)
                    .await?
                    .expect("key workspace admitted");
                for (index, key) in keys.iter().enumerate() {
                    assert_eq!(lookup.get(key), Some(index < hits));
                }
                eprintln!(
                    "scale_lookup rows={rows} fragments={fragments} keys={KEYS} hits={hits} native={} warm={} round={round} wall_us={} observation={:?}",
                    mode == 1,
                    round > 0,
                    started.elapsed().as_micros(),
                    lookup.observation()
                );
                drop(lookup);
                memory.released().await?;
            }
        }
    }
    drop(bases);
    drop(runtime);
    memory.released().await?;
    memory.report(LIMIT, rows, fragments);
    Ok(())
}
