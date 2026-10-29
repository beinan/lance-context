//! An uncertain Plan must not discard its frozen prefix or the ACKed suffix.
use super::*;
use std::sync::atomic::Ordering;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn annotation_unknown_plan_keeps_concurrent_updates_and_drains() -> Result<()> {
    tokio::time::timeout(Duration::from_secs(60), exercise()).await?
}

async fn exercise() -> Result<()> {
    let root = tempfile::tempdir()?;
    let storage = Storage::resolve(
        root.path().join("turns.lance").to_str().unwrap().into(),
        Default::default(),
        Arc::default(),
    )
    .await?;
    let limits = Limits {
        retained_bytes: 256 << 10,
        retained_entries: 128,
        value_bytes: 4096,
        readers: 16,
    };
    let mut c =
        network::Cluster::open_signed(root.path(), annotation_publication::one_shard()).await?;
    annotation_publication::seed_contract(&c, &storage, Service::contract(&binding(), &limits)?)
        .await?;
    let proxy = lost_prepare::proxy::Proxy::open_plan(&c, root.path(), true).await?;
    c.client = proxy.client.clone();
    let (service, runtime) = owner(&c, &storage, &limits).await?;
    let (stop, task) = run_owner(runtime);
    let a = producer(&service, 1).await?;
    let b = producer(&service, 2).await?;
    let d = producer(&service, 3).await?;
    let first = request(
        &service,
        &a,
        1,
        wire(&limits, &["T"], 1, Some("reviewed"), 0, None)?,
    )
    .await?;
    let first = service.submit(&first, Duration::from_secs(30)).await?;
    assert_eq!(first.result, MutationResult::Applied);
    tokio::time::timeout(Duration::from_secs(10), proxy.committed.notified()).await?;
    let pending = annotation_publication::state(&c)
        .await?
        .progress
        .pending
        .clone()
        .unwrap();
    assert_eq!(
        pending.plan.sources[&10].records,
        vec![(first.index, first.meta.clone())]
    );
    // Plan has committed while its reply is withheld. The frozen prefix is
    // unchanged; both different-field and same-field suffix updates are ACKed.
    let score = request(
        &service,
        &b,
        1,
        wire(&limits, &["T"], 0, None, 1, Some(0.9))?,
    )
    .await?;
    let status = request(
        &service,
        &d,
        1,
        wire(&limits, &["T"], 1, Some("approved"), 0, None)?,
    )
    .await?;
    let (score, status) = tokio::join!(
        service.submit(&score, Duration::from_secs(30)),
        service.submit(&status, Duration::from_secs(30))
    );
    let (score, status) = (score?, status?);
    assert_eq!(score.result, MutationResult::Applied);
    assert_eq!(status.result, MutationResult::Applied);
    let current = service.get("T", Duration::from_secs(10)).await?.unwrap();
    assert_eq!(current.status.as_deref(), Some("approved"));
    assert_eq!(current.score, Some(0.9));
    proxy.release.notify_one();
    drained(&c, &service, score.index.max(status.index)).await?;
    assert_eq!(
        service.get("T", Duration::from_secs(10)).await?.unwrap(),
        current
    );
    let selected =
        annotation_publication::read(&c, &storage, Some(pending.plan.target_version()?)).await?;
    assert_eq!(
        selected.read_transaction().await?.unwrap().uuid,
        pending.plan.transaction.to_string()
    );
    assert_eq!(proxy.dropped.load(Ordering::SeqCst), 1);
    assert_eq!(
        proxy.plans.load(Ordering::SeqCst),
        2,
        "expected original plan plus suffix, without resubmission"
    );
    stop.send(()).unwrap();
    task.await??;
    drop(proxy);
    c.shutdown().await
}
