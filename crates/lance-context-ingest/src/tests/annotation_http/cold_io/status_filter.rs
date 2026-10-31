use super::*;

#[tokio::test]
async fn status_filter_timeout_keeps_native_workspace_and_base_until_io_finishes() -> Result<()> {
    let root = tempfile::tempdir()?;
    let c = network::Cluster::open_signed(root.path(), annotation_publication::one_shard()).await?;
    let gates = Arc::new(store::Gates::default());
    let storage = store::storage(&root.path().join("turns.lance"), gates.clone()).await?;
    let limits = Limits {
        retained_bytes: 256 << 10,
        retained_entries: 128,
        value_bytes: 4096,
        readers: 16,
    };
    annotation_publication::seed_contract(
        &c,
        &storage,
        annotations::Service::contract(&binding(), &limits)?,
    )
    .await?;
    let (service, runtime) = annotation_runtime::owner(&c, &storage, &limits).await?;
    let budget = service.ingress().budget.clone();
    assert_eq!(budget.reserved(), 0);
    let mut cold = gates.pause_get();
    let reader = service.clone();
    let task = tokio::spawn(async move {
        reader
            .filter_status_owned(
                Some("pending".into()),
                16,
                64 << 10,
                0,
                Duration::from_millis(100),
            )
            .await
    });
    cold.entered().await?;
    let error = task.await?.err().context("paused query did not time out")?;
    assert!(format!("{error:#}").contains("status query deadline expired"));
    assert!(budget.reserved() > 0);
    assert_eq!(service.observation().await.0.readers, 1);
    cold.release();
    tokio::time::timeout(Duration::from_secs(10), async {
        while budget.reserved() != 0 || service.observation().await.0.readers != 0 {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await?;
    drop(runtime);
    c.shutdown().await
}
