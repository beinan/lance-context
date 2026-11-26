use super::*;

#[tokio::test]
async fn cancelled_local_submission_falls_back_to_wal_under_apply_budget() -> Result<()> {
    let root = tempfile::tempdir()?;
    let c = network::Cluster::open_signed(root.path(), annotation_publication::one_shard()).await?;
    let storage = Storage::resolve(
        root.path().join("turns.lance").to_str().unwrap().into(),
        Default::default(),
        Arc::new(lance::session::Session::default()),
    )
    .await?;
    let limits = Limits::default();
    annotation_publication::seed_contract(&c, &storage, Service::contract(&binding(), &limits)?)
        .await?;
    let apply = MergeMemoryBudget::new(8 << 20);
    let upload = MergeMemoryBudget::new(8 << 20);
    let (a, mut driver) = Service::connect(
        c.client.clone(),
        annotation_publication::one_shard(),
        binding(),
        &storage,
        limits.clone(),
        upload.clone(),
        apply.clone(),
    )
    .await?;
    let p = producer(&a, 1).await?;
    let input = request(
        &a,
        &p,
        1,
        wire(&limits, &["T"], 1, Some("kept"), 1, Some(0.9))?,
    )
    .await?;
    let held = apply.reserve(apply.limit()).await;
    let initial = a.observation().await.0.applied;
    {
        let submitting = a.submit(&input, Duration::from_secs(20));
        tokio::pin!(submitting);
        tokio::time::timeout(Duration::from_secs(10), async {
            tokio::select! {
                result = &mut submitting => {
                    result?;
                    bail!("submission ACK preceded application")
                }
                () = async {
                    loop {
                        if a.phase_observation().iter().any(|p| p.phase == "annotation_apply_wait" && p.started == 1) {
                            break;
                        }
                        tokio::task::yield_now().await;
                    }
                } => Ok::<_, anyhow::Error>(())
            }
        }).await??;
        driver.step_for_test().await?;
        assert_eq!(a.observation().await.0.applied, initial);
        assert!(a.observation().await.1);
        assert!(a
            .phase_observation()
            .iter()
            .filter(|p| p.phase == "annotation_fetch" || p.phase == "annotation_decode")
            .all(|p| p.started == 0));
        // Cancel after quorum acceptance but before application. Do not retry
        // the mutation; the existing committed record remains authoritative.
    }
    drop(input);
    assert_eq!(upload.reserved(), 0);
    drop(held);
    driver.step_for_test().await?;
    assert!(a.observation().await.0.applied > initial);
    assert_eq!(apply.reserved(), 0);
    assert_eq!(
        a.phase_observation()
            .iter()
            .find(|p| p.phase == "annotation_fetch")
            .unwrap()
            .completed,
        1
    );
    let (stop, task) = run(driver);
    let value = a.get("T", Duration::from_secs(5)).await?.unwrap();
    assert_eq!(value.status.as_deref(), Some("kept"));
    assert_eq!(value.score, Some(0.9));
    stop.send(()).unwrap();
    task.await??;
    c.shutdown().await
}
