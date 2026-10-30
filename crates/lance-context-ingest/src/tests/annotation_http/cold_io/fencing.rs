//! A takeover while a real cold point read is in flight must fence its result.
use super::*;

#[tokio::test]
async fn point_read_fences_after_storage_io_and_rejects_an_already_stale_owner() -> Result<()> {
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
    let (old_service, runtime) = annotation_runtime::owner(&c, &storage, &limits).await?;
    let old_owner = Owner::new(&runtime, keys()?, options())?;
    let old_server = Server::start(old_owner.router(), None).await?;
    let (old_gateway, old_ingress) = frontend(&c, &limits, &old_server.uri()).await?;
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(30))
        .build()?;
    let (stop_old, stopped_old) = oneshot::channel();
    let old_work = runtime.run(async {
        let _ = stopped_old.await;
    });
    tokio::pin!(old_work);

    let mut cold = gates.pause_get();
    let pending = tokio::spawn(
        post(&client, &old_ingress, "read")
            .json(&PointRequest { turn: "T".into() })
            .send(),
    );
    tokio::select! {
        result = &mut old_work => anyhow::bail!("old runtime stopped: {result:?}"),
        entered = cold.entered() => entered?,
    }
    assert!(!pending.is_finished());
    // Keep the old runtime alive but unpolled from here. It cannot detect the
    // takeover and stop its applier on our behalf: the HTTP result must fence.
    // Independent owner processes have independent native metadata caches.
    // Sharing the old Session would let its deliberately paused metadata GET
    // stall the successor's read of the same file before it can apply a write.
    let successor_storage = store::storage(
        &root.path().join("turns.lance"),
        Arc::new(store::Gates::default()),
    )
    .await?;
    let (new_service, runtime) = annotation_runtime::owner(&c, &successor_storage, &limits).await?;
    let new_owner = Owner::new(&runtime, keys()?, options())?;
    let new_server = Server::start(new_owner.router(), None).await?;
    let (new_gateway, new_ingress) = frontend(&c, &limits, &new_server.uri()).await?;
    let (stop_new, new_work) = annotation_runtime::run_owner(runtime);
    let producer = open(&client, &new_ingress, 1).await?;
    let body = super::super::super::annotations::wire(
        &limits,
        &["T"],
        1,
        Some("successor-acknowledged"),
        1,
        Some(0.9),
    )?;
    let reply: MutationReply = update(&client, &new_ingress, &producer, 1, body.len())
        .body(body)
        .send()
        .await?
        .error_for_status()?
        .json()
        .await?;
    assert!(matches!(
        reply.completion.result,
        annotations::MutationResult::Applied
    ));
    cold.release();
    let in_flight = pending.await?.context("in-flight predecessor point read")?;
    let in_flight_status = in_flight.status();
    let in_flight_body: serde_json::Value = in_flight.json().await?;
    // Another request begins only after the successor's write ACK. The old
    // listener is still running, and its own apply position has not advanced.
    assert!(old_service.observation().await.0.applied < reply.completion.index);
    let stale = post(&client, &old_ingress, "read")
        .json(&PointRequest { turn: "T".into() })
        .send()
        .await?;
    let stale_status = stale.status();
    let stale_body: serde_json::Value = stale.json().await?;
    let current = point(&client, &new_ingress)
        .await
        .context("successor point read")?
        .unwrap();
    annotation_runtime::drained(&c, &new_service, reply.completion.index).await?;

    let _ = stop_old.send(());
    let _ = old_work.await;
    stop_new.send(()).unwrap();
    new_work.await??;
    old_gateway.shutdown().await;
    new_gateway.shutdown().await;
    old_ingress.shutdown().await?;
    new_ingress.shutdown().await?;
    old_owner.shutdown().await;
    new_owner.shutdown().await;
    old_server.shutdown().await?;
    new_server.shutdown().await?;
    c.shutdown().await?;

    for (status, body) in [
        (in_flight_status, in_flight_body),
        (stale_status, stale_body),
    ] {
        assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(body["error"], "annotation_owner_changed");
        assert_eq!(body["operation_outcome"], "not_submitted");
    }
    assert_eq!(current.status.as_deref(), Some("successor-acknowledged"));
    assert_eq!(current.score, Some(0.9));
    Ok(())
}
