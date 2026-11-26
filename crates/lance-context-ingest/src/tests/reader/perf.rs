//! Opt-in local TLS experiment, not a multi-process or cloud throughput claim.
use super::*;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Instant;

const PAYLOAD: usize = 256 << 10;
const ROWS: usize = 64;
const APPENDS: usize = 256;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "local TLS metadata scaling experiment; run explicitly with --ignored --nocapture"]
async fn session_metadata_under_unrelated_backlog() -> Result<()> {
    // Bucket 2 shares group 10 with the target bucket 0; bucket 1 uses group 20.
    // Fresh stores per arm avoid accumulated retained records from earlier arms.
    for (trial, (unrelated, bucket)) in [(0, 2), (128, 2), (2048, 2), (2048, 1), (128, 2), (0, 2)]
        .into_iter()
        .enumerate()
    {
        let root = tempfile::tempdir()?;
        let path = root.path().join("rollouts.lance");
        let c = network::Cluster::open_signed(root.path(), layout()).await?;
        bootstrap(&c, &path, &binding()).await?;
        let storage = storage(&path).await?;
        let ingress = connect(&c, binding())
            .await?
            .with_session_read_concurrency(2)?;
        let s = session(0);
        let target = producer(&ingress, 0).await?;
        let mut expected = Vec::new();
        for i in 0..16 {
            let b = batch(&[&format!("target-{i}")], &[&s])?;
            append(&ingress, &target, i + 1, b.clone()).await?;
            expected.push(b);
        }
        let other = producer(&ingress, bucket).await?;
        for i in 0..unrelated {
            append(
                &ingress,
                &other,
                i + 1,
                batch(&[&format!("unrelated-{i}")], &[&session(bucket)])?,
            )
            .await?;
        }
        // One warm-up is excluded, all measured reads still verify every column.
        verify(
            reader::read_session(&ingress, &storage, &s).await?,
            &expected,
        )
        .await?;
        let mut metadata_ms = Vec::new();
        let mut complete_ms = Vec::new();
        for _ in 0..16 {
            let start = Instant::now();
            let read = reader::read_session(&ingress, &storage, &s).await?;
            metadata_ms.push(start.elapsed().as_secs_f64() * 1000.0);
            verify(read, &expected).await?;
            complete_ms.push(start.elapsed().as_secs_f64() * 1000.0);
        }
        metadata_ms.sort_by(f64::total_cmp);
        complete_ms.sort_by(f64::total_cmp);
        println!(
            "SESSION_METADATA_EXPERIMENT {}",
            serde_json::json!({
                "trial":trial,"unrelated_records":unrelated,"unrelated_bucket":bucket,
                "same_group":bucket==2,"target_records":16,"payload_bytes_per_row":16<<10,
                "queries":metadata_ms.len(),"metadata_p50_ms":metadata_ms[8],
                "metadata_max_ms":metadata_ms[15],"complete_p50_ms":complete_ms[8],
                "complete_max_ms":complete_ms[15],"fetch_concurrency":2,
                "scope":"three real TLS replicas in one process, local disk; metadata scaling only, no publication or concurrent writer"
            })
        );
        ensure!(ingress.budget.reserved() == 0, "query leaked reservation");
        c.shutdown().await?;
    }
    Ok(())
}

fn row(id: &str, session: &str) -> Result<RecordBatch> {
    let b = batch(&[id], &[session])?;
    let mut columns = b.columns().to_vec();
    columns[b.schema().index_of("binary_payload")?] =
        Arc::new(LargeBinaryArray::from_iter_values([vec![0x5a; PAYLOAD]]));
    Ok(RecordBatch::try_new(b.schema(), columns)?)
}

fn rss_bytes() -> Option<u64> {
    std::fs::read_to_string("/proc/self/status")
        .ok()?
        .lines()
        .find(|line| line.starts_with("VmRSS:"))?
        .split_whitespace()
        .nth(1)?
        .parse::<u64>()
        .ok()?
        .checked_mul(1024)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "local TLS throughput experiment; run explicitly with --ignored --nocapture"]
async fn concurrent_session_read_and_append() -> Result<()> {
    // Reverse the second pass to expose order/cache effects. Each arm has fresh
    // Raft stores and the same retained tail and concurrent append workload.
    for (trial, concurrency) in [1, 2, 4, 4, 2, 1].into_iter().enumerate() {
        let root = tempfile::tempdir()?;
        let path = root.path().join("rollouts.lance");
        let c = network::Cluster::open_signed(root.path(), layout()).await?;
        bootstrap(&c, &path, &binding()).await?;
        let storage = storage(&path).await?;
        let ingress = Ingress::connect(
            c.client.clone(),
            layout(),
            binding(),
            MergeMemoryBudget::new(32 << 20),
        )
        .await?
        .with_session_read_concurrency(concurrency)?;
        let s = session(0);
        let p = producer(&ingress, 0).await?;
        for i in 0..ROWS {
            append(&ingress, &p, i as u64 + 1, row(&format!("read-{i}"), &s)?).await?;
        }
        let writer = producer(&ingress, 2).await?;
        let done = Arc::new(AtomicBool::new(false));
        let gate = Arc::new(tokio::sync::Barrier::new(2));
        let ingest = ingress.clone();
        let writer_done = done.clone();
        let writer_gate = gate.clone();
        let writer = tokio::spawn(async move {
            writer_gate.wait().await;
            let start = Instant::now();
            let result = async {
                for seq in 1..=APPENDS {
                    append(
                        &ingest,
                        &writer,
                        seq as u64,
                        row(&format!("write-{seq}"), &session(2))?,
                    )
                    .await?;
                }
                Ok::<_, anyhow::Error>(start.elapsed().as_secs_f64())
            }
            .await;
            writer_done.store(true, Ordering::Release);
            result
        });
        let mut metadata_ms = Vec::new();
        let mut query_ms = Vec::new();
        let mut peak_reserved = 0;
        let mut peak_rss = 0;
        let wall = Instant::now();
        gate.wait().await;
        while !done.load(Ordering::Acquire) || query_ms.is_empty() {
            let start = Instant::now();
            let mut read = reader::read_session(&ingress, &storage, &s).await?;
            metadata_ms.push(start.elapsed().as_secs_f64() * 1000.0);
            let mut ids = HashSet::new();
            while let Some(batch) = read.batches.try_next().await? {
                peak_reserved = peak_reserved.max(ingress.budget.reserved());
                peak_rss = peak_rss.max(rss_bytes().unwrap_or(0));
                let keys = batch
                    .column_by_name("id")
                    .unwrap()
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .unwrap();
                let payload = batch
                    .column_by_name("binary_payload")
                    .unwrap()
                    .as_any()
                    .downcast_ref::<LargeBinaryArray>()
                    .unwrap();
                for i in 0..batch.num_rows() {
                    ensure!(ids.insert(keys.value(i).to_owned()), "duplicate read row");
                    ensure!(
                        payload.value(i).len() == PAYLOAD
                            && payload.value(i).iter().all(|&b| b == 0x5a),
                        "payload mismatch"
                    );
                }
            }
            ensure!(
                ids == (0..ROWS).map(|i| format!("read-{i}")).collect(),
                "incomplete session read"
            );
            query_ms.push(start.elapsed().as_secs_f64() * 1000.0);
        }
        let write_seconds = writer.await??;
        let wall_seconds = wall.elapsed().as_secs_f64();
        query_ms.sort_by(f64::total_cmp);
        metadata_ms.sort_by(f64::total_cmp);
        println!(
            "SESSION_READ_EXPERIMENT {}",
            serde_json::json!({
                "trial":trial,"concurrency":concurrency,"payload_bytes":PAYLOAD,
                "retained_rows":ROWS,"concurrent_appends":APPENDS,"queries":query_ms.len(),
                "query_p50_ms":query_ms[query_ms.len()/2],"query_max_ms":query_ms[query_ms.len()-1],
                "metadata_p50_ms":metadata_ms[metadata_ms.len()/2],
                "append_mib_per_second":(APPENDS*PAYLOAD) as f64 / (1<<20) as f64 / write_seconds,
                "query_mib_per_second":(query_ms.len()*ROWS*PAYLOAD) as f64 / (1<<20) as f64 / wall_seconds,
                "sampled_peak_reserved_bytes":peak_reserved,"sampled_process_peak_rss_bytes":peak_rss,
                "scope":"three real TLS replicas in one process, local disk, no materializer or HTTP encoding"
            })
        );
        ensure!(
            ingress.budget.reserved() == 0,
            "query leaked memory reservation"
        );
        c.shutdown().await?;
    }
    Ok(())
}
