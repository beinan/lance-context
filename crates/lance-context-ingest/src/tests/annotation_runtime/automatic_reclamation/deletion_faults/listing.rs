//! A pending listing response must retain its page while publication proceeds.
use super::*;

#[tokio::test]
async fn annotation_collector_pending_listing_yields_before_any_candidate() -> Result<()> {
    pending_listing(false).await
}

#[tokio::test]
async fn annotation_collector_pending_listing_preserves_partial_page() -> Result<()> {
    pending_listing(true).await
}

async fn pending_listing(partial: bool) -> Result<()> {
    let root = root()?;
    eprintln!(
        "pending LIST fixture: {} partial={partial}",
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
    let (stop, task) = run_owner(whole_page(runtime)?);
    tokio::time::timeout(Duration::from_secs(30), reached).await??;
    let producer = producer(&service, 2).await?;
    let mut last = completed.index;
    let mut score = None;
    let progress = async {
        // More updates than the two-record source window, with a full drain
        // between them. No response is released and no request is resent.
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
            tokio::time::timeout(Duration::from_secs(5), drained(&service, last))
                .await
                .context("publication remained blocked on the held listing")??;
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
    drained(&service, last).await?;
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
