use super::*;
use crate::tests::annotations as driver;
use crate::{
    annotations::Service, bootstrap::annotations::mixed, tests::mixed_annotations as fixture,
};
use lance_context_core::annotation_staging::BodySnapshot;

async fn provision(c: &network::Cluster, narrow: &Storage, body: &Storage) -> Result<()> {
    mixed::provision(
        c.client.clone(),
        annotation_publication::one_shard(),
        binding(),
        narrow.clone(),
        body.clone(),
        identity(),
        fixture::limits(),
    )
    .await?;
    Ok(())
}

#[tokio::test]
async fn mixed_bootstrap_recovers_unselected_pair_and_selected_restart() -> Result<()> {
    let root = artifact_root()?;
    let c = network::Cluster::open_signed(root.path(), annotation_publication::one_shard()).await?;
    let narrow = fixture::storage(root.path(), "annotations.lance").await?;
    let body = fixture::storage(root.path(), "body.lance").await?;
    let layout = annotation_publication::one_shard();
    let binding = binding();
    let scope = Scope {
        cluster: layout.cluster,
        layout: layout.fingerprint()?,
        table: binding.table,
        destination: binding.destination.clone(),
        incarnation: binding.incarnation,
        contract: Service::mixed_contract(&binding, &fixture::limits())?,
    };
    // Files persisted, but no catalog selection: restart must reuse this pair.
    let unselected =
        mixed::create_pair(&narrow, &body, &scope, &identity(), layout.buckets[0]).await?;
    let transaction = unselected.read_transaction().await?.unwrap();
    let reference = BodySnapshot::read(&unselected).await?.unwrap();
    drop(unselected);
    c.shutdown().await?;
    let c = network::Cluster::open_signed(root.path(), layout.clone()).await?;
    provision(&c, &narrow, &body).await?;
    let original = view(&c, binding.table).await?;
    let selected = annotation_publication::read(&c, &narrow, None).await?;
    assert_eq!(
        selected.read_transaction().await?.as_ref(),
        Some(&transaction)
    );
    assert_eq!(
        BodySnapshot::read(&selected).await?,
        Some(reference.clone())
    );
    assert!(selected.manifest().uses_stable_row_ids());
    let body_base =
        crate::annotations::mixed::load_body(&body, &reference, &binding.incarnation.to_string())
            .await?;
    assert!(body_base.manifest().uses_stable_row_ids());
    assert_eq!(selected.count_rows(None).await?, 0);
    assert_eq!(body_base.count_rows(None).await?, 0);
    provision(&c, &narrow, &body).await?;
    assert_eq!(view(&c, binding.table).await?, original);
    drop((selected, body_base));
    c.shutdown().await?;
    let c = network::Cluster::open_signed(root.path(), layout).await?;
    provision(&c, &narrow, &body).await?;
    assert_eq!(view(&c, binding.table).await?, original);
    // Also prove the resulting public binding can open staging, not merely files.
    let (service, driver) = Service::connect_mixed(
        c.client.clone(),
        annotation_publication::one_shard(),
        binding.clone(),
        &narrow,
        body.clone(),
        fixture::limits(),
        MergeMemoryBudget::new(8 << 20),
        MergeMemoryBudget::new(8 << 20),
    )
    .await?;
    let (stop, task) = driver::run(driver);
    let p = driver::producer(&service, 1).await?;
    let request = driver::request(
        &service,
        &p,
        1,
        fixture::encoded(fixture::create(&["T"], &["session"], &[7])?)?,
    )
    .await?;
    service.submit(&request, Duration::from_secs(10)).await?;
    fixture::check(&service, "T", "session", 7, "pending", None).await?;
    stop.send(()).unwrap();
    task.await??;
    drop(service);
    c.shutdown().await
}

#[tokio::test]
async fn mixed_bootstrap_refuses_changed_identity_destination_and_overlapping_roots() -> Result<()>
{
    let root = artifact_root()?;
    let c = network::Cluster::open_signed(root.path(), annotation_publication::one_shard()).await?;
    let narrow = fixture::storage(root.path(), "annotations.lance").await?;
    let body = fixture::storage(root.path(), "body.lance").await?;
    provision(&c, &narrow, &body).await?;
    let before = view(&c, binding().table).await?;
    let mut changed = identity();
    changed.owner = Uuid::new_v4();
    assert!(mixed::provision(
        c.client.clone(),
        annotation_publication::one_shard(),
        binding(),
        narrow.clone(),
        body.clone(),
        changed,
        fixture::limits()
    )
    .await
    .is_err());
    let other = fixture::storage(root.path(), "other-body.lance").await?;
    assert!(provision(&c, &narrow, &other).await.is_err());
    assert!(!root.path().join("other-body.lance").exists());
    let nested = Storage::resolve(
        format!("{}/nested.lance", narrow.uri),
        Default::default(),
        narrow.session.clone(),
    )
    .await?;
    assert!(provision(&c, &narrow, &nested).await.is_err());
    assert_eq!(view(&c, binding().table).await?, before);
    c.shutdown().await
}

#[tokio::test]
async fn mixed_bootstrap_refuses_foreign_empty_body_without_selecting_narrow() -> Result<()> {
    let root = artifact_root()?;
    let c = network::Cluster::open_signed(root.path(), annotation_publication::one_shard()).await?;
    let narrow = fixture::storage(root.path(), "annotations.lance").await?;
    let body = fixture::storage(root.path(), "body.lance").await?;
    let schema = Arc::new(rollout_schema());
    Dataset::write(
        RecordBatchIterator::new(vec![Ok(RecordBatch::new_empty(schema.clone()))], schema),
        &body.uri,
        Some(WriteParams {
            enable_stable_row_ids: true,
            skip_auto_cleanup: true,
            ..Default::default()
        }),
    )
    .await?;
    // Compare persisted transactions on both sides: the newly written Dataset
    // can cache dictionary values that are absent from a reopened transaction.
    let original_transaction = Dataset::open(&body.uri).await?.read_transaction().await?;
    assert!(provision(&c, &narrow, &body).await.is_err());
    assert!(view(&c, binding().table).await?.manifests.is_none());
    assert!(!root.path().join("annotations.lance").exists());
    assert_eq!(
        Dataset::open(&body.uri).await?.read_transaction().await?,
        original_transaction
    );
    c.shutdown().await
}

fn artifact_root() -> Result<tempfile::TempDir> {
    if let Ok(parent) = std::env::var("INGEST_TEST_ARTIFACTS") {
        Ok(tempfile::Builder::new()
            .prefix("mixed-bootstrap-")
            .disable_cleanup(true)
            .tempdir_in(parent)?)
    } else {
        Ok(tempfile::tempdir()?)
    }
}
