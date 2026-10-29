//! A pending listing response must retain its page while publication proceeds.
use super::*;

#[tokio::test]
async fn annotation_collector_recovers_failed_listing() -> Result<()> {
    failed_listing(None, false).await
}

#[tokio::test]
async fn annotation_collector_discards_failed_partial_listing() -> Result<()> {
    failed_listing(None, true).await
}

#[tokio::test]
async fn annotation_collector_recovers_failed_tag_listing() -> Result<()> {
    failed_listing(Some("_refs/tags"), false).await
}

#[tokio::test]
async fn annotation_collector_recovers_failed_branch_listing() -> Result<()> {
    failed_listing(Some("_refs/branches"), false).await
}

async fn failed_listing(suffix: Option<&str>, partial: bool) -> Result<()> {
    use std::sync::atomic::{AtomicBool, Ordering};
    let root = root()?;
    let path = root.path().join("turns.lance");
    let c = network::Cluster::open_signed(root.path(), annotation_publication::one_shard()).await?;
    let faults = Arc::new(Faults::default());
    let storage = fault_store::storage(&path, faults.clone()).await?;
    let (first, completed, abandoned) = acknowledged_orphans(&c, &storage, &path).await?;
    let (service, runtime) = owned(&c, &storage, &limits()).await?;
    let active = Arc::new(AtomicBool::new(true));
    let prefix = suffix.map_or_else(|| path.clone(), |suffix| path.join(suffix));
    faults.fail_listing(
        &prefix,
        partial.then_some(abandoned[0].as_path()),
        active.clone(),
    )?;
    let (stop, task) = run_owner(whole_page(runtime)?);
    let progress = async {
        tokio::time::timeout(Duration::from_secs(5), async {
            while service.reclamation_observation().list_failures == 0 {
                ensure!(!task.is_finished(), "listing error stopped owner");
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
            Ok::<_, anyhow::Error>(())
        })
        .await??;
        let producer = producer(&service, 2).await?;
        let update = request(
            &service,
            &producer,
            1,
            wire(&limits(), &["T"], 0, None, 1, Some(0.9))?,
        )
        .await?;
        let result = service.submit(&update, Duration::from_secs(5)).await?;
        let value = service.get("T", Duration::from_secs(5)).await?.unwrap();
        ensure!(value.status.as_deref() == Some("acknowledged") && value.score == Some(0.9));
        tokio::time::sleep(Duration::from_millis(40)).await;
        ensure!(service.reclamation_observation().listing_blocked);
        ensure!(
            !task.is_finished(),
            "failed LIST killed the applied read view"
        );
        ensure!(
            abandoned.iter().all(|p| p.exists()),
            "partial failed page was deleted"
        );
        let (staging, _) = service.observation().await;
        ensure!(staging.retained_bytes <= limits().retained_bytes);
        ensure!(service.source_observation().records <= 2);
        Ok::<_, anyhow::Error>(result.index)
    }
    .await;
    // Release even for the old-code negative control, preserving the original
    // error while allowing the owned fixture to shut down completely.
    active.store(false, Ordering::SeqCst);
    if let Ok(through) = progress {
        drained(&c, &service, through).await?;
        wait_files(&path, 3).await?;
        assert_eq!(
            service.submit(&first, Duration::from_secs(5)).await?,
            completed
        );
        ensure!(!service.reclamation_observation().listing_blocked);
        cold_check(&c, &storage, Some(0.9)).await?;
        ensure!(abandoned.iter().all(|p| !p.exists()));
    }
    let _ = stop.send(());
    let runtime = task.await?;
    c.shutdown().await?;
    progress?;
    runtime
}

#[tokio::test]
async fn annotation_collector_pending_listing_yields_before_any_candidate() -> Result<()> {
    pending_listing(false, false).await
}

#[tokio::test]
async fn annotation_collector_pending_listing_preserves_partial_page() -> Result<()> {
    pending_listing(true, false).await
}

#[tokio::test]
async fn annotation_collector_pending_listing_drains_source_window() -> Result<()> {
    pending_listing(false, true).await
}

#[tokio::test]
async fn annotation_collector_pending_listing_drains_source_window_and_partial_page() -> Result<()>
{
    pending_listing(true, true).await
}

async fn pending_listing(partial: bool, burst: bool) -> Result<()> {
    let root = root()?;
    eprintln!(
        "pending LIST fixture: {} partial={partial} burst={burst}",
        root.path().display()
    );
    let path = root.path().join("turns.lance");
    let c = network::Cluster::open_signed(root.path(), annotation_publication::one_shard()).await?;
    let faults = Arc::new(Faults::default());
    let storage = fault_store::storage(&path, faults.clone()).await?;
    let (first, completed, abandoned) = acknowledged_orphans(&c, &storage, &path).await?;
    let (service, runtime) = owned(&c, &storage, &limits()).await?;
    let (entered, reached) = oneshot::channel();
    let (resume, resumed) = oneshot::channel();
    faults.pause_listing(
        &path,
        partial.then_some(abandoned[0].as_path()),
        entered,
        resumed,
    )?;
    let runtime = if burst {
        runtime.with_exclusive_reclamation(crate::annotations::runtime::reclamation::Options {
            max_objects: 16,
            max_bytes: 16 << 10,
            delete_parallelism: 2,
            interval: Duration::from_secs(1),
        })?
    } else {
        whole_page(runtime)?
    };
    let (stop, task) = run_owner(runtime);
    tokio::time::timeout(Duration::from_secs(30), reached).await??;
    let producer = producer(&service, 2).await?;
    let mut last = completed.index;
    let mut score = None;
    let progress = async {
        // More updates than the two-record source window. A burst does not
        // wait for drains between requests, exercising admission under pressure.
        // No listing response is released and no request is resent.
        for sequence in 1..=4 {
            let update = request(
                &service,
                &producer,
                sequence,
                wire(&limits(), &["T"], 0, None, 1, Some(sequence as f32))?,
            )
            .await?;
            let result = service.submit(&update, Duration::from_secs(30)).await?;
            last = result.index;
            score = Some(sequence as f32);
            let current = service.get("T", Duration::from_secs(30)).await?.unwrap();
            ensure!(current.status.as_deref() == Some("acknowledged"));
            ensure!(current.score == score);
            if !burst || sequence == 4 {
                tokio::time::timeout(Duration::from_secs(5), drained(&c, &service, last))
                    .await
                    .context("publication remained blocked on the held listing")??;
            }
            let (observed, _) = service.observation().await;
            ensure!(observed.retained_bytes <= limits().retained_bytes);
            ensure!(
                faults.data_listings.lock().unwrap().len() == 1,
                "pending listing was discarded and restarted"
            );
        }
        if partial {
            ensure!(
                !abandoned[0].exists(),
                "partial candidate page did not drain"
            );
        }
        Ok::<_, anyhow::Error>(())
    }
    .await;
    // Release and shut down even on the negative control, preserving a complete
    // fixture rather than abandoning the pending response or background tasks.
    let original_response_retained = resume.send(()).is_ok();
    drained(&c, &service, last).await?;
    wait_files(&path, 3).await?;
    assert_eq!(
        service.submit(&first, Duration::from_secs(30)).await?,
        completed
    );
    assert_eq!(
        service
            .get("T", Duration::from_secs(30))
            .await?
            .unwrap()
            .score,
        score
    );
    stop.send(()).unwrap();
    task.await??;
    cold_check(&c, &storage, score).await?;
    assert!(abandoned.iter().all(|p| !p.exists()));
    c.shutdown().await?;
    ensure!(
        original_response_retained,
        "original listing response was dropped"
    );
    progress
}
