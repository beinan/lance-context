//! Quorum acceptance resolves an unknown append, but never substitutes for apply.
use super::*;
use crate::tests::annotation_runtime::lost_prepare::proxy::Proxy;
use std::sync::atomic::Ordering;

#[derive(Clone, Copy, PartialEq)]
enum Case {
    Accepted,
    Absent,
    DifferentBytes,
    RejectedMutation,
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn unknown_append_observes_exact_acceptance_and_waits_for_apply() -> Result<()> {
    tokio::time::timeout(Duration::from_secs(60), exercise(Case::Accepted)).await?
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn unknown_append_without_acceptance_is_not_resubmitted() -> Result<()> {
    tokio::time::timeout(Duration::from_secs(60), exercise(Case::Absent)).await?
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn unknown_append_does_not_accept_same_sequence_with_different_bytes() -> Result<()> {
    tokio::time::timeout(Duration::from_secs(60), exercise(Case::DifferentBytes)).await?
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn unknown_append_preserves_the_applied_mutation_rejection() -> Result<()> {
    tokio::time::timeout(Duration::from_secs(60), exercise(Case::RejectedMutation)).await?
}

async fn exercise(case: Case) -> Result<()> {
    let mut builder = tempfile::Builder::new();
    builder.prefix("annotation-uncertain-append-");
    let root = match std::env::var("INGEST_TEST_ARTIFACTS") {
        Ok(parent) => builder.tempdir_in(parent)?,
        Err(_) => builder.tempdir()?,
    }
    .keep();
    let mut c = network::Cluster::open_signed(&root, annotation_publication::one_shard()).await?;
    let storage = Storage::resolve(
        root.join("turns.lance").to_str().unwrap().into(),
        Default::default(),
        Arc::new(lance::session::Session::default()),
    )
    .await?;
    let limits = Limits::default();
    annotation_publication::seed_contract(&c, &storage, Service::contract(&binding(), &limits)?)
        .await?;
    let execute = matches!(case, Case::Accepted | Case::RejectedMutation);
    let proxy = Proxy::open_append(&c, &root, execute).await?;
    let direct = std::mem::replace(&mut c.client, proxy.client.clone());
    let (service, driver) = connect(&c, &storage, &limits).await?;
    let producer = producer(&service, 1).await?;
    let turn = if case == Case::RejectedMutation {
        "missing"
    } else {
        "T"
    };
    let input = Arc::new(
        request(
            &service,
            &producer,
            1,
            wire(&limits, &[turn], 1, Some("reviewed"), 0, None)?,
        )
        .await?,
    );
    let submit = tokio::spawn({
        let service = service.clone();
        let input = input.clone();
        async move { service.submit(&input, Duration::from_secs(30)).await }
    });
    tokio::time::timeout(Duration::from_secs(20), proxy.committed.notified()).await?;
    if case == Case::DifferentBytes {
        let other = request(
            &service,
            &producer,
            1,
            wire(&limits, &[turn], 1, Some("other"), 0, None)?,
        )
        .await?;
        assert!(matches!(
            direct
                .request_routed(Request::Append {
                    group: 10,
                    batch: other.batch.clone(),
                })
                .await?
                .reply,
            Reply::Commit(Outcome::Committed(Applied::Accepted { .. }))
        ));
    }
    proxy.release.notify_one();
    if execute {
        tokio::time::sleep(Duration::from_millis(30)).await;
        assert!(
            !submit.is_finished(),
            "quorum acceptance must not acknowledge an unapplied mutation"
        );
    }
    let (stop, task) = run(driver);
    let result = submit.await?;
    if execute {
        let completed = result?;
        assert_eq!(completed.meta, input.batch.meta);
        if case == Case::RejectedMutation {
            assert!(matches!(
                completed.result,
                MutationResult::Rejected(ref r) if r.row == 0 && r.reason == RejectionReason::MissingTurn
            ));
            assert!(service.get(turn, Duration::from_secs(10)).await?.is_none());
        } else {
            assert_eq!(completed.result, MutationResult::Applied);
            assert_eq!(
                service.get(turn, Duration::from_secs(10)).await?.unwrap().status.as_deref(),
                Some("reviewed")
            );
        }
    } else {
        let error = result.unwrap_err();
        assert!(
            matches!(error.downcast_ref::<AppendFailure>(), Some(AppendFailure::Unknown)),
            "{error:#}"
        );
        let current = service.get(turn, Duration::from_secs(10)).await?.unwrap();
        assert_eq!(current.status.as_deref(), if case == Case::DifferentBytes { Some("other") } else { Some("pending") });
    }
    assert_eq!(proxy.appends.load(Ordering::SeqCst), usize::from(execute));
    assert_eq!(proxy.dropped.load(Ordering::SeqCst), 1);
    stop.send(()).unwrap();
    task.await??;
    drop(service);
    c.client = direct;
    drop(proxy);
    c.shutdown().await
}
