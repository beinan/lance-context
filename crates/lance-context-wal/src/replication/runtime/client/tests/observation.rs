use super::*;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn append_diagnostics_distinguish_admission_failures_without_changing_replies() -> Result<()>
{
    let dir = tempfile::tempdir()?;
    let c = RpcCluster::open(dir.path(), Options::default()).await?;
    let leader = c.leader().await?;
    let handle = &c.net.nodes[&leader].handle;
    let request = Request::Append {
        group: 10,
        batch: batch(1),
    };

    let slots = handle
        .shared
        .data
        .slots
        .clone()
        .try_acquire_many_owned(handle.shared.data.capacity_slots as u32)?;
    assert!(matches!(
        c.client.request(leader, request.clone()).await?.reply,
        Reply::Unknown
    ));
    assert_eq!(handle.append_observation()["request_credits"], 1);
    drop(slots);

    let bytes = handle
        .shared
        .data
        .bytes
        .clone()
        .try_acquire_many_owned(handle.shared.data.capacity_bytes as u32)?;
    assert!(matches!(
        c.client.request(leader, request.clone()).await?.reply,
        Reply::Unknown
    ));
    assert_eq!(handle.append_observation()["byte_credits"], 1);
    drop(bytes);

    let first = c.client.request(leader, request.clone()).await?;
    let index = accepted(&first);
    drop(first);
    let retry = c.client.request(leader, request.clone()).await?;
    assert_eq!(accepted(&retry), index);
    drop(retry);

    let follower = *c.net.nodes.keys().find(|&&id| id != leader).unwrap();
    assert!(matches!(
        c.client.request(follower, request.clone()).await?.reply,
        Reply::Redirect { .. }
    ));
    assert_eq!(
        c.net.nodes[&follower].handle.append_observation()["redirect"],
        1
    );

    handle.disk.as_ref().unwrap().limit_data(1)?;
    assert!(matches!(
        c.client.request(leader, request).await?.reply,
        Reply::Unknown
    ));
    let observed = handle.append_observation();
    assert_eq!(observed["disk_budget"], 1);
    assert_eq!(observed["accepted"], 2);
    assert_eq!(observed["started"], 5);
    assert_eq!(observed["execution_other"], 0);
    assert_eq!(observed["cancelled"], 0);
    assert_eq!(observed.values().sum::<u64>(), 2 * observed["started"]);
    c.shutdown().await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cancelled_append_execution_is_not_reported_as_rejected_or_accepted() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let c = RpcCluster::open_with_options(
        dir.path(),
        Options {
            timeout: Duration::from_millis(300),
            ..Options::default()
        },
        Options::default(),
    )
    .await?;
    let leader = c.leader().await?;
    let handle = &c.net.nodes[&leader].handle;
    let (entered, arrived) = tokio::sync::oneshot::channel();
    let (release, resumed) = std::sync::mpsc::channel::<()>();
    // Keep the actual group owner from processing the queued append until the
    // RPC deadline expires. Dropping the RPC future must not discard its intent.
    handle.enqueue(
        handle.route(10)?,
        Work::Local(Box::new(move |_| {
            let _ = entered.send(());
            let _ = resumed.recv();
            Box::new(|| {})
        })),
        false,
    )?;
    arrived.await?;
    let request = Request::Append {
        group: 10,
        batch: batch(1),
    };
    let expired = c.client.request(leader, request.clone()).await;
    drop(release);
    assert!(expired.is_err());
    assert_eq!(handle.append_observation()["cancelled"], 1);
    assert_eq!(handle.append_observation()["accepted"], 0);
    // The timed-out operation can still commit. Retry the original bytes and
    // verify one accepted record and the original durable position.
    let retried = c.client.request_routed(request).await?;
    let index = accepted(&retried);
    drop(retried);
    assert_eq!(producer(&c.client, leader, &batch(1)).await?, Some(index));
    assert_eq!(handle.observation(10)?.status.accepted_records, 1);
    assert_eq!(handle.append_observation()["accepted"], 1);
    c.shutdown().await
}
