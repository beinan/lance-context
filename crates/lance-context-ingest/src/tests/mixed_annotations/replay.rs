//! Rebuild current bodies/annotations from the selected pair and its WAL suffix.
use super::*;

#[tokio::test]
async fn mixed_driver_replays_creates_and_updates_then_drains_without_resetting_annotations(
) -> Result<()> {
    let root = artifact_root()?;
    let c = network::Cluster::open_signed(root.path(), annotation_publication::one_shard()).await?;
    let narrow = storage(root.path(), "annotations.lance").await?;
    let body = storage(root.path(), "body.lance").await?;
    seed(&c, &narrow, &body).await?;
    let (a, driver) = connect(&c, &narrow, &body).await?;
    let (stop, task) = run(driver);
    let p = producer(&a, 1).await?;
    let initial = request(
        &a,
        &p,
        1,
        encoded(create(&["A", "A", "B"], &["s", "wrong", "s"], &[1, 9, 2])?)?,
    )
    .await?;
    a.submit(&initial, Duration::from_secs(10)).await?;
    let patch = request(
        &a,
        &p,
        2,
        encoded(mixed::wire::patches(
            &patch(&["A"], Some("reviewed"), Some(0.9))?,
            &limits(),
        )?)?,
    )
    .await?;
    let outcome = a.submit(&patch, Duration::from_secs(10)).await?;
    check(&a, "A", "s", 1, "reviewed", Some(0.9)).await?;
    // A separate apply service reconstructs the acknowledged state. It has no
    // shared staging and performs no materialization or client mutation retry.
    let (other, driver) = connect(&c, &narrow, &body).await?;
    let (stop_other, other_task) = run(driver);
    check(&other, "A", "s", 1, "reviewed", Some(0.9)).await?;
    stop_other.send(()).unwrap();
    other_task.await??;
    drop(other);
    stop.send(()).unwrap();
    task.await??;
    drop(a);
    c.shutdown().await?;

    // No application checkpoint was published before this restart.
    let c = network::Cluster::open_signed(root.path(), annotation_publication::one_shard()).await?;
    let (a, driver) = connect(&c, &narrow, &body).await?;
    let (stop, task) = run(driver);
    check(&a, "A", "s", 1, "reviewed", Some(0.9)).await?;
    assert_eq!(a.submit(&patch, Duration::from_secs(10)).await?, outcome);
    pressure::publish(&c, &a, &narrow, &body).await?;
    assert_eq!(a.retained_body_frames(), (0, 0));

    // Re-import after materialization must preserve the first body and current
    // annotation. New keys in the same record still become immediately visible.
    let q = producer(&a, 2).await?;
    let reimport = request(
        &a,
        &q,
        1,
        encoded(create(
            &["A", "C", "C"],
            &["wrong", "t", "wrong"],
            &[9, 3, 8],
        )?)?,
    )
    .await?;
    let imported = a.submit(&reimport, Duration::from_secs(10)).await?;
    check(&a, "A", "s", 1, "reviewed", Some(0.9)).await?;
    check(&a, "C", "t", 3, "pending", None).await?;
    stop.send(()).unwrap();
    task.await??;
    drop(a);
    c.shutdown().await?;

    // Reopen the selected Lance pair, replay only its retained suffix and drain.
    let c = network::Cluster::open_signed(root.path(), annotation_publication::one_shard()).await?;
    let (a, driver) = connect(&c, &narrow, &body).await?;
    let (stop, task) = run(driver);
    check(&a, "A", "s", 1, "reviewed", Some(0.9)).await?;
    check(&a, "C", "t", 3, "pending", None).await?;
    assert_eq!(
        a.submit(&reimport, Duration::from_secs(10)).await?,
        imported
    );
    pressure::publish(&c, &a, &narrow, &body).await?;
    assert_eq!(a.retained_body_frames(), (0, 0));
    let selected = annotation_publication::read(&c, &narrow, None).await?;
    assert_eq!(selected.count_rows(None).await?, 3);
    let reference = BodySnapshot::read(&selected).await?.unwrap();
    let selected_body =
        mixed::load_body(&body, &reference, &binding().incarnation.to_string()).await?;
    assert_eq!(selected_body.count_rows(None).await?, 3);
    check(&a, "A", "s", 1, "reviewed", Some(0.9)).await?;
    check(&a, "B", "s", 2, "pending", None).await?;
    check(&a, "C", "t", 3, "pending", None).await?;
    stop.send(()).unwrap();
    task.await??;
    c.shutdown().await
}
