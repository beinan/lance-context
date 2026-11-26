use super::*;
use lance_context_core::annotation_staging::StatusMatches;

async fn filter(client: &reqwest::Client, server: &Server) -> Result<StatusMatches> {
    Ok(post(client, server, "filter-status")
        .json(&http::annotations::StatusRequest {
            bucket: 0,
            status: Some("pending".into()),
            limit: 16,
        })
        .send()
        .await?
        .error_for_status()?
        .json()
        .await?)
}

/// The replacement must replay its predecessor's ACKs before reading, while a
/// later unacknowledged WAL proposal must not block the already visible value.
#[tokio::test]
async fn owned_reads_replay_predecessor_acks_without_waiting_for_later_proposals() -> Result<()> {
    let root = tempfile::tempdir()?;
    let c = network::Cluster::open_signed(root.path(), annotation_publication::one_shard()).await?;
    let storage = Storage::resolve(
        root.path().join("turns.lance").to_str().unwrap().into(),
        Default::default(),
        Arc::new(lance::session::Session::default()),
    )
    .await?;
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
    let (predecessor, driver) = super::super::annotations::connect(&c, &storage, &limits).await?;
    let (stop, task) = super::super::annotations::run(driver);
    let producer = super::super::annotations::producer(&predecessor, 1).await?;
    let first = super::super::annotations::request(
        &predecessor,
        &producer,
        1,
        super::super::annotations::wire(&limits, &["T"], 1, Some("acknowledged"), 0, None)?,
    )
    .await?;
    let first_ack = predecessor.submit(&first, Duration::from_secs(30)).await?;
    stop.send(()).unwrap();
    task.await??;
    drop(predecessor);

    // No publisher ran: the predecessor's acknowledged value exists only in
    // its WAL. Start the replacement's HTTP listener before polling its runtime.
    let (service, runtime) = annotation_runtime::owner(&c, &storage, &limits).await?;
    let owner = Owner::new(&runtime, keys()?, options())?;
    let server = Server::start(owner.router(), None).await?;
    let (ga, a) = frontend(&c, &limits, &server.uri()).await?;
    let (gb, b) = frontend(&c, &limits, &server.uri()).await?;
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(30))
        .build()?;
    let (stop, stopped) = oneshot::channel();
    let work = runtime.run(async {
        let _ = stopped.await;
    });
    tokio::pin!(work);
    let (first_value, first_filter) = {
        let initial = async { tokio::try_join!(point(&client, &b), filter(&client, &a)) };
        tokio::pin!(initial);
        assert!(
            tokio::time::timeout(Duration::from_millis(100), &mut initial)
                .await
                .is_err(),
            "read escaped before predecessor replay"
        );
        tokio::select! {
            result = &mut work => anyhow::bail!("runtime stopped: {result:?}"),
            result = &mut initial => result?,
        }
    };
    let first_value = first_value.unwrap();
    assert_eq!(first_value.status.as_deref(), Some("acknowledged"));
    assert!(first_filter.applied_through_at_capture >= first_ack.index);

    // Stop polling the runtime, but leave RF3 and both ingresses running.
    // Commit the next proposal without issuing a successful annotation ACK.
    let later = super::super::annotations::request(
        &service,
        &producer,
        2,
        super::super::annotations::wire(&limits, &["T"], 1, Some("later"), 0, None)?,
    )
    .await?;
    let accepted = service.ingress().append(&later).await?;
    assert!(service.observation().await.0.applied < accepted.position.index);
    let visible = tokio::time::timeout(Duration::from_secs(1), async {
        tokio::try_join!(point(&client, &a), filter(&client, &b))
    })
    .await;

    // Resume application and reconcile that exact proposal; its original WAL
    // position must be returned and the new value becomes immediately visible.
    let completed = tokio::select! {
        result = &mut work => anyhow::bail!("runtime stopped: {result:?}"),
        result = service.submit(&later, Duration::from_secs(30)) => result?,
    };
    assert_eq!(completed.index, accepted.position.index);
    let current = tokio::select! {
        result = &mut work => anyhow::bail!("runtime stopped: {result:?}"),
        result = point(&client, &b) => result?.unwrap(),
    };
    assert_eq!(current.status.as_deref(), Some("later"));
    stop.send(()).unwrap();
    work.await?;
    ga.shutdown().await;
    gb.shutdown().await;
    a.shutdown().await?;
    b.shutdown().await?;
    owner.shutdown().await;
    server.shutdown().await?;
    c.shutdown().await?;
    // Assert after clean shutdown so the expected old-behavior failure leaves
    // no running owner, HTTP request, or WAL fixture behind.
    let (point, filtered) = visible??;
    assert_eq!(point, Some(first_value));
    assert!(filtered.applied_through_at_capture >= first_ack.index);
    assert!(filtered.applied_through_at_capture < accepted.position.index);
    assert_eq!(
        filtered.ids.iter().any(|id| id == "T"),
        filtered.materialized_through < first_ack.index
    );
    Ok(())
}
