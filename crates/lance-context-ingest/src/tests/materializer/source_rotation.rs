use super::*;
use crate::materializer::SessionIndex;
use lance::index::DatasetIndexExt;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn index_only_maintenance_keeps_source_rotation_across_restart() -> Result<()> {
    let root = tempfile::tempdir()?;
    let path = root.path().join("rotation.lance");
    let cluster = network::Cluster::open_signed(root.path(), layout()).await?;
    bootstrap(&cluster, &path, &binding()).await?;
    let ingress = connect(&cluster, binding()).await?;
    let storage = Storage::resolve(
        path.to_str().unwrap().into(),
        Default::default(),
        Arc::default(),
    )
    .await?;
    let budget = MergeMemoryBudget::new(768 << 20);
    let mut options = compaction::options(false);
    options.max_records = 1;
    options.session_index = Some(SessionIndex {
        min_fragments: 1,
        max_fragments: 1,
        max_segments: 16,
    });
    let mut expected = BTreeMap::new();
    for bucket in 0..2 {
        let open = ProducerOpen {
            bucket,
            slot: bucket,
            expected_generation: 0,
            operation: Uuid::new_v4(),
        };
        for sequence in 1..=4 {
            let (body, _) = append(
                &ingress,
                &open,
                sequence,
                &[&format!("{bucket}-{sequence}")],
            )
            .await?;
            remember(&mut expected, body)?;
        }
    }
    let mut writer = compaction::acquire(&ingress, &storage, &budget, 1, options.clone()).await?;
    let first = writer.run_once().await?.context("first append missing")?;
    assert_eq!(first.sources.keys().copied().collect::<Vec<_>>(), vec![10]);
    let index = writer
        .run_once()
        .await?
        .context("index maintenance missing")?;
    assert!(index.sources.is_empty());
    assert_eq!(
        writer.read_dataset(None).await?.load_indices().await?.len(),
        1
    );
    compaction::query_all(&ingress, &storage, &expected).await?;
    drop((writer, ingress));
    cluster.shutdown().await?;

    // Neither a volatile cursor nor another producer append may repair fairness.
    // Both groups retain their original backlog across the full WAL restart.
    let cluster = network::Cluster::open_signed(root.path(), layout()).await?;
    let ingress = connect(&cluster, binding()).await?;
    let mut writer = compaction::acquire(&ingress, &storage, &budget, 2, options).await?;
    let next = writer.run_once().await?.context("second append missing")?;
    assert_eq!(
        next.sources.keys().copied().collect::<Vec<_>>(),
        vec![20],
        "index-only maintenance reset source rotation after restart"
    );
    let mut drained = false;
    for _ in 0..16 {
        if writer.run_once().await?.is_none() {
            drained = true;
            break;
        }
    }
    assert!(drained, "original backlog or index work did not drain");
    verify(&writer, &expected, 17).await?;
    compaction::query_all(&ingress, &storage, &expected).await?;
    assert_eq!(budget.reserved(), 0);
    drop((writer, ingress));
    cluster.shutdown().await
}
