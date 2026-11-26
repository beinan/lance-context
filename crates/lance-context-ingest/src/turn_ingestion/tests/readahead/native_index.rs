//! Compare an existing native BTree with the actual raw-key lookup path.
//! This is local diagnostic evidence, not an index maintenance implementation.
use super::*;
use lance::index::DatasetIndexExt;
use lance_index::{
    scalar::{BuiltinIndexType, ScalarIndexParams},
    IndexType,
};

const ROWS: usize = 1_000_000;
const FRAGMENT_ROWS: usize = 16_384;
const LIMIT: usize = 256 << 20;

// A permutation makes absent IDs interleave with present IDs instead of giving
// the BTree an unrealistically favorable out-of-range sequential-ID workload.
fn mixed(mut value: u64) -> u64 {
    value = (value ^ (value >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    value = (value ^ (value >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    value ^ (value >> 31)
}

fn identity(row: usize) -> (String, String) {
    (
        format!("session-{:04x}", mixed(row as u64 ^ 0x1234_5678) & 0xffff),
        format!("turn-{:016x}", mixed(row as u64)),
    )
}

fn batch(start: usize, end: usize) -> std::result::Result<RecordBatch, arrow_schema::ArrowError> {
    let mut keys = (start..end).map(identity).collect::<Vec<_>>();
    keys.sort_unstable();
    RecordBatch::try_new(
        schema(),
        vec![
            Arc::new(StringArray::from_iter_values(keys.iter().map(|key| &key.0))),
            Arc::new(StringArray::from_iter_values(keys.iter().map(|key| &key.1))),
            Arc::new(StringArray::from(vec![None::<&str>; keys.len()])),
            Arc::new(LargeBinaryArray::from_iter_values(
                keys.iter().map(|_| b"x".as_slice()),
            )),
            Arc::new(Float64Array::from(vec![None; keys.len()])),
        ],
    )
}

fn session(memory: &Arc<Memory>) -> Result<Arc<Session>> {
    let runtime = RuntimeEnvBuilder::new()
        .with_memory_pool(memory.clone())
        .build_arc()?;
    Ok(Arc::new(
        Session::new(16 << 20, 16 << 20, Default::default())
            .with_runtime_env(runtime)
            .with_native_memory(memory.clone()),
    ))
}

fn file_bytes(path: &std::path::Path) -> std::io::Result<u64> {
    std::fs::read_dir(path)?.try_fold(0, |total, entry| {
        let entry = entry?;
        let metadata = entry.metadata()?;
        Ok(total
            + if metadata.is_dir() {
                file_bytes(&entry.path())?
            } else {
                metadata.len()
            })
    })
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "local native index experiment writes and indexes 1M rows; run explicitly"]
async fn capped_btree_random_key_lookup_experiment() -> Result<()> {
    let root = tempfile::tempdir()?;
    let path = root.path().join("body.lance");
    let uri = path.to_str().unwrap();
    let input = (0..ROWS)
        .step_by(FRAGMENT_ROWS)
        .map(|start| batch(start, (start + FRAGMENT_ROWS).min(ROWS)));
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
    let original_version = dataset.manifest().version;
    let fragments = dataset.fragments().len();
    drop(dataset);

    let build_memory = Arc::new(Memory::new(LIMIT));
    let mut dataset = DatasetBuilder::from_uri(uri)
        .with_session(session(&build_memory)?)
        .load()
        .await?;
    let build_store = dataset.object_store(None).await?;
    let build_before = build_store.io_stats_snapshot();
    let started = std::time::Instant::now();
    dataset
        .create_index(
            &["id"],
            IndexType::BTree,
            Some("diagnostic_id".into()),
            &ScalarIndexParams::for_builtin(BuiltinIndexType::BTree),
            false,
        )
        .await?;
    let indexed_version = dataset.manifest().version;
    let build_after = build_store.io_stats_snapshot();
    eprintln!(
        "native_index_build rows={ROWS} fragments={fragments} wall_ms={} index_file_bytes={} store_read_bytes={} store_read_iops={} store_written_bytes={}",
        started.elapsed().as_millis(),
        file_bytes(&path.join("_indices"))?,
        build_after.read_bytes - build_before.read_bytes,
        build_after.read_iops - build_before.read_iops,
        build_after.written_bytes - build_before.written_bytes,
    );
    drop(dataset);
    build_memory.released().await?;
    eprintln!("native_index_experiment phase=build");
    build_memory.report(LIMIT, ROWS, fragments);

    let memory = Arc::new(Memory::new(LIMIT));
    let mut bases = Vec::new();
    for version in [original_version, indexed_version] {
        bases.push(Arc::new(
            DatasetBuilder::from_uri(uri)
                .with_version(version)
                .with_session(session(&memory)?)
                .load()
                .await?,
        ));
    }
    let spec = spec()?;
    let workspace = MergeMemoryBudget::new(16 << 20);
    for count in [32, 256, 1024] {
        for hits in [0, count / 2] {
            for round in 0..4 {
                // Change keys each round. Warm sessions must not mean repeatedly
                // querying an identical finite set of index pages.
                let keys = (0..count)
                    .map(|index| {
                        let row = if index < hits {
                            (index * 7919 + round * 104729) % ROWS
                        } else {
                            ROWS + round * 2048 + index
                        };
                        let (session, id) = identity(row);
                        spec.business_key(&[&session, &id], 1024)
                    })
                    .collect::<Result<Vec<_>>>()?;
                let refs = keys.iter().map(String::as_str).collect::<Vec<_>>();
                for mode in if round % 2 == 0 { [0, 1] } else { [1, 0] } {
                    // ScanStats describe execution. Store deltas also include
                    // index planning/loading, with no concurrent store users.
                    let store = bases[mode].object_store(None).await?;
                    let before = store.io_stats_snapshot();
                    let started = std::time::Instant::now();
                    let lookup = spec
                        .lookup_existing(bases[mode].clone(), &refs, &workspace)
                        .await?
                        .expect("key workspace admitted");
                    for (index, key) in keys.iter().enumerate() {
                        assert_eq!(lookup.get(key), Some(index < hits));
                    }
                    let after = store.io_stats_snapshot();
                    eprintln!("native_index_lookup rows={ROWS} keys={count} hits={hits} indexed={} round={round} wall_us={} store_read_bytes={} store_read_iops={} observation={:?}",
                        mode == 1, started.elapsed().as_micros(), after.read_bytes - before.read_bytes,
                        after.read_iops - before.read_iops, lookup.observation());
                }
            }
        }
    }

    // Both individual column values exist; their composite key does not.
    let cross = spec.business_key(&[&identity(0).0, &identity(1).1], 1024)?;
    assert_ne!(identity(0).0, identity(1).0);
    let lookup = spec
        .lookup_existing(bases[1].clone(), &[&cross], &workspace)
        .await?
        .unwrap();
    assert_eq!(lookup.get(&cross), Some(false));
    drop(lookup);

    // Newly appended keys must still be found outside existing index coverage.
    let mut appended = (*bases[1]).clone();
    appended
        .append(
            arrow_array::RecordBatchIterator::new(vec![batch(ROWS, ROWS + 64)], schema()),
            None,
        )
        .await?;
    let (tail_session, tail_id) = identity(ROWS + 63);
    let tail = spec.business_key(&[&tail_session, &tail_id], 1024)?;
    let old = spec
        .lookup_existing(bases[1].clone(), &[&tail], &workspace)
        .await?
        .unwrap();
    assert_eq!(old.get(&tail), Some(false));
    drop(old);
    let lookup = spec
        .lookup_existing(Arc::new(appended), &[&tail, &cross], &workspace)
        .await?
        .unwrap();
    assert_eq!(lookup.get(&tail), Some(true));
    assert_eq!(lookup.get(&cross), Some(false));
    eprintln!(
        "native_index_uncovered_tail observation={:?}",
        lookup.observation()
    );
    drop(lookup);
    drop(bases);
    memory.released().await?;
    eprintln!("native_index_experiment phase=lookup");
    memory.report(LIMIT, ROWS, fragments);
    Ok(())
}
