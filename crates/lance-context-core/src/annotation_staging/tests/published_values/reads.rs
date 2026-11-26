//! Published point reads reuse bounded values without changing captured views.
use super::*;

#[tokio::test]
async fn published_point_reads_avoid_data_io_after_staging_drains() -> Result<()> {
    let root = tempfile::tempdir()?;
    let (base, throttle) = opened(&root.path().join("turns.lance"), 8 << 20).await?;
    let staging = AnnotationStaging::new(base, scope(), 0, Limits::default())?;
    staging
        .apply(
            0,
            1,
            "T",
            &TurnAnnotationPatch {
                status: Set("reviewed".into()),
                ..Default::default()
            },
        )
        .await?;
    publish(&staging).await?;
    let observed = staging.observation().await;
    assert_eq!(observed.active_entries + observed.frozen_entries, 0);
    assert_eq!(observed.retained_bytes + observed.retained_entries, 0);

    let before = throttle.ops.load(Ordering::Relaxed);
    throttle.frozen.store(true, Ordering::Relaxed);
    let read = tokio::time::timeout(Duration::from_secs(5), current(&staging, "T")).await;
    // Always release the held real data store, including on the negative control.
    throttle.frozen.store(false, Ordering::Relaxed);
    let value = read.map_err(|_| invalid("published point read waited for data IO"))??;
    assert_eq!(throttle.ops.load(Ordering::Relaxed), before);
    assert_eq!(value.status.as_deref(), Some("reviewed"));
    assert_eq!(value.score, None);
    assert_eq!(value.labels, BTreeSet::from(["original".into()]));
    Ok(())
}

#[tokio::test]
async fn older_point_view_does_not_read_a_newer_cached_publication() -> Result<()> {
    let root = tempfile::tempdir()?;
    let (base, throttle) = opened(&root.path().join("turns.lance"), 8 << 20).await?;
    let staging = AnnotationStaging::new(base, scope(), 0, Limits::default())?;
    staging
        .apply(
            0,
            1,
            "T",
            &TurnAnnotationPatch {
                status: Set("reviewed".into()),
                ..Default::default()
            },
        )
        .await?;
    publish(&staging).await?;
    let old = staging.capture("T").await?;
    staging
        .apply(
            1,
            2,
            "T",
            &TurnAnnotationPatch {
                status: Clear,
                score: Set(0.9),
                ..Default::default()
            },
        )
        .await?;
    publish(&staging).await?;

    // The latest cache entry belongs to a later cut. The old handle must use
    // its original Lance base, whose files remain protected by that handle.
    let previous = old.resolve().await?.unwrap();
    assert_eq!(previous.status.as_deref(), Some("reviewed"));
    assert_eq!(previous.score, None);
    let before = throttle.ops.load(Ordering::Relaxed);
    throttle.frozen.store(true, Ordering::Relaxed);
    let read = tokio::time::timeout(Duration::from_secs(5), current(&staging, "T")).await;
    throttle.frozen.store(false, Ordering::Relaxed);
    let current = read.map_err(|_| invalid("current point read missed its published value"))??;
    assert_eq!(throttle.ops.load(Ordering::Relaxed), before);
    assert_eq!(current.status, None);
    assert_eq!(current.score, Some(0.9));
    Ok(())
}

#[tokio::test]
async fn unrelated_publication_keeps_an_unchanged_cached_turn_readable() -> Result<()> {
    let root = tempfile::tempdir()?;
    let (base, throttle) = opened(&root.path().join("turns.lance"), 8 << 20).await?;
    let staging = AnnotationStaging::new(base, scope(), 0, Limits::default())?;
    staging
        .apply(
            0,
            1,
            "T",
            &TurnAnnotationPatch {
                status: Set("reviewed".into()),
                ..Default::default()
            },
        )
        .await?;
    publish(&staging).await?;
    staging
        .apply(
            1,
            2,
            "U",
            &TurnAnnotationPatch {
                score: Set(0.9),
                ..Default::default()
            },
        )
        .await?;
    publish(&staging).await?;

    let before = throttle.ops.load(Ordering::Relaxed);
    throttle.frozen.store(true, Ordering::Relaxed);
    let reads = tokio::time::timeout(Duration::from_secs(5), async {
        tokio::try_join!(current(&staging, "T"), current(&staging, "U"))
    })
    .await;
    throttle.frozen.store(false, Ordering::Relaxed);
    let (t, u) =
        reads.map_err(|_| invalid("unrelated publication discarded cached point reads"))??;
    assert_eq!(throttle.ops.load(Ordering::Relaxed), before);
    assert_eq!(t.status.as_deref(), Some("reviewed"));
    assert_eq!(t.score, None);
    assert_eq!(u.score, Some(0.9));
    Ok(())
}
