use super::*;
use crate::annotations::Completed;
use std::path::{Path as FsPath, PathBuf};
use tokio::sync::oneshot;

mod fault_store;
use fault_store::{Action, Faults};

fn limits() -> Limits {
    Limits {
        retained_bytes: 256 << 10,
        retained_entries: 128,
        value_bytes: 4096,
        readers: 16,
    }
}

fn root() -> Result<tempfile::TempDir> {
    let mut builder = tempfile::Builder::new();
    builder.prefix("annotation-delete-fault-");
    Ok(if let Ok(parent) = std::env::var("INGEST_TEST_ARTIFACTS") {
        // Preserve WAL, blob files and original request evidence when requested.
        builder.disable_cleanup(true).tempdir_in(parent)?
    } else {
        builder.tempdir()?
    })
}

async fn acknowledged_orphans(
    c: &network::Cluster,
    storage: &Storage,
    path: &FsPath,
) -> Result<(PreparedAppend, Completed, Vec<PathBuf>)> {
    let limits = limits();
    annotation_publication::seed_contract(c, storage, Service::contract(&binding(), &limits)?)
        .await?;
    let original = files(path)?;
    let mut first = None;
    for _ in 0..2 {
        let (service, driver) = connect(c, storage, &limits).await?;
        let (stop, task) = run(driver);
        let producer = producer(&service, 1).await?;
        let request = request(
            &service,
            &producer,
            1,
            wire(&limits, &["T"], 1, Some("acknowledged"), 0, None)?,
        )
        .await?;
        let completed = service.submit(&request, Duration::from_secs(30)).await?;
        if let Some((_, expected)) = &first {
            assert_eq!(&completed, expected);
        }
        let frozen = service.freeze(Uuid::new_v4()).await?.unwrap();
        let prepared = frozen
            .prepare(&MergeMemoryBudget::new(8 << 20), 8192)
            .await?;
        stop.send(()).unwrap();
        task.await??;
        drop((prepared, frozen, service));
        first = Some((request, completed));
    }
    let abandoned: Vec<_> = files(path)?.difference(&original).cloned().collect();
    assert_eq!(abandoned.len(), 2, "two real abandoned native preparations");
    let (request, completed) = first.unwrap();
    Ok((request, completed, abandoned))
}

async fn cold_check(c: &network::Cluster, storage: &Storage, score: Option<f32>) -> Result<()> {
    let base = annotation_publication::read(c, storage, None).await?;
    assert!(base.read_transaction().await?.is_some());
    let batches = base
        .scan()
        .try_into_stream()
        .await?
        .try_collect::<Vec<_>>()
        .await?;
    assert_eq!(batches.iter().map(|b| b.num_rows()).sum::<usize>(), 2);
    let mut found = false;
    for batch in batches {
        let ids = batch
            .column_by_name("id")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let bodies = batch
            .column_by_name("body")
            .unwrap()
            .as_any()
            .downcast_ref::<LargeBinaryArray>()
            .unwrap();
        let statuses = batch
            .column_by_name("status")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let scores = batch
            .column_by_name("score")
            .unwrap()
            .as_any()
            .downcast_ref::<arrow_array::Float32Array>()
            .unwrap();
        for row in 0..batch.num_rows() {
            let body = bodies.value(row);
            assert_eq!(body.len(), 256 << 10);
            assert!(body
                .iter()
                .enumerate()
                .all(|(i, b)| *b == ((i * 31 + i / 17) % 251) as u8));
            if ids.value(row) == "T" {
                found = true;
                assert_eq!(statuses.value(row), "acknowledged");
                assert_eq!(scores.iter().nth(row).unwrap(), score);
            }
        }
    }
    assert!(found);
    Ok(())
}

async fn wait_files(path: &FsPath, count: usize) -> Result<()> {
    tokio::time::timeout(Duration::from_secs(30), async {
        loop {
            if files(path)?.len() == count {
                return Ok::<_, anyhow::Error>(());
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await??;
    Ok(())
}

#[tokio::test]
async fn annotation_collector_slow_pass_leaves_a_full_publication_cooldown() -> Result<()> {
    let root = root()?;
    let path = root.path().join("turns.lance");
    let c = network::Cluster::open_signed(root.path(), annotation_publication::one_shard()).await?;
    let faults = Arc::new(Faults::default());
    let storage = fault_store::storage(&path, faults.clone()).await?;
    let (first, completed, abandoned) = acknowledged_orphans(&c, &storage, &path).await?;
    let (entered, reached) = oneshot::channel();
    let (resume, resumed) = oneshot::channel();
    faults.arm(
        &abandoned[0],
        Action::Pause {
            entered,
            resume: resumed,
        },
    )?;
    let interval = Duration::from_millis(500);
    let (service, runtime) = owned(&c, &storage, &limits()).await?;
    let runtime =
        runtime.with_exclusive_reclamation(crate::annotations::runtime::reclamation::Options {
            interval,
            max_objects: 64,
            max_bytes: 16 << 10,
            delete_parallelism: 2,
        })?;
    faults.collection_starts.lock().unwrap().clear();
    let (stop, task) = run_owner(runtime);
    tokio::time::timeout(Duration::from_secs(30), reached).await??;
    assert_eq!(
        service.submit(&first, Duration::from_secs(30)).await?,
        completed
    );
    let producer = producer(&service, 2).await?;
    let suffix = request(
        &service,
        &producer,
        1,
        wire(&limits(), &["T"], 0, None, 1, Some(0.9))?,
    )
    .await?;
    let suffix_result = service.submit(&suffix, Duration::from_secs(30)).await?;
    let current = service.get("T", Duration::from_secs(30)).await?.unwrap();
    assert_eq!(current.status.as_deref(), Some("acknowledged"));
    assert_eq!(current.score, Some(0.9));
    // A real sealed DELETE keeps the publisher occupied longer than its interval.
    // The next pass must leave a full interval *after* that I/O finishes.
    tokio::time::sleep(interval + Duration::from_millis(50)).await;
    assert_eq!(faults.collection_starts.lock().unwrap().len(), 1);
    let released = tokio::time::Instant::now();
    resume.send(()).unwrap();
    faults.finish_remote().await?;
    drained(&service, suffix_result.index).await?;
    tokio::time::timeout(Duration::from_secs(30), async {
        while faults.collection_starts.lock().unwrap().len() < 2 {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await?;
    let next_started = faults.collection_starts.lock().unwrap()[1];
    // Finish cleanup before asserting the regression, so failure preserves a
    // complete protocol shutdown and does not leave background tasks running.
    wait_files(&path, 3).await?;
    cold_check(&c, &storage, Some(0.9)).await?;
    stop.send(()).unwrap();
    task.await??;
    c.shutdown().await?;
    assert!(
        next_started >= released + interval,
        "collection restarted {:?} after releasing slow I/O; expected at least {interval:?}",
        next_started.duration_since(released)
    );
    Ok(())
}
