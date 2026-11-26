//! Slow body readers must backpressure the same durable create, not stop apply.
use super::*;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn full_body_reader_pressure_retains_pending_create_and_resumes_after_release() -> Result<()>
{
    let input = bodies()?;
    let spec = TurnIngestionSpec::new(
        input[0].schema(),
        vec!["session_id".into(), "id".into()],
        "session_id",
    )?;
    let key = spec.prepare(input[0].clone(), 4096)?.keys()[0].clone();
    let root = tempfile::tempdir()?;
    let c = network::Cluster::open_signed(root.path(), annotation_publication::one_shard()).await?;
    let storage = fixture::storage(root.path(), "full-body.lance").await?;
    crate::bootstrap::provision_body_shard(
        c.client.clone(),
        annotation_publication::one_shard(),
        binding(),
        storage.clone(),
        Identity {
            owner: Uuid::new_v4(),
            acquisition: Uuid::new_v4(),
            manifest: Uuid::new_v4(),
        },
        &fixture::limits(),
        Shard { bucket: 0 },
        &spec,
    )
    .await?;
    let encode = |i| {
        fixture::encoded(single_body::batch(
            &spec,
            &fixture::limits(),
            Shard { bucket: 0 },
            &input[i],
        )?)
    };
    // Keep the first frame staged. Reading it consumes reader permits without
    // competing for cold-scan memory or relying on storage timing.
    let (service, runtime) = owner_with_wait(&c, &storage, &spec, Duration::from_secs(60)).await?;
    let (stop, task) = run_owner(runtime);
    let lane = producer(&service, 1).await?;
    let first = request(&service, &lane, 1, encode(0)?).await?;
    service.submit(&first, DEADLINE).await?;
    let mut readers = Vec::new();
    for _ in 0..fixture::limits().readers {
        readers.push(service.get_full_body(&key, DEADLINE).await?.unwrap());
    }
    assert_eq!(
        service.observation().await.0.readers,
        fixture::limits().readers
    );
    let pending = request(&service, &lane, 2, encode(2)?).await?;
    let reply = c
        .client
        .request_routed(Request::Append {
            group: 10,
            batch: pending.batch.clone(),
        })
        .await?;
    let Reply::Commit(Outcome::Committed(Applied::Accepted { index })) = reply.reply else {
        bail!("pending create was not quorum durable")
    };
    tokio::time::timeout(DEADLINE, async {
        while service.apply_backpressure().is_none() {
            assert!(!task.is_finished(), "reader pressure stopped the owner");
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await?;
    assert!(!task.is_finished());
    assert!(service.observation().await.0.applied < index);
    // A quorum-durable but unapplied create cannot produce a success response.
    assert!(service
        .submit(&pending, Duration::from_millis(50))
        .await
        .is_err());
    assert!(!task.is_finished());
    assert!(service.observation().await.0.applied < index);
    drop(readers);
    let applied = service.submit(&pending, DEADLINE).await?;
    assert_eq!(applied.index, index);
    assert_eq!(applied.result, MutationResult::Applied);
    assert_eq!(
        service
            .get_full_body(&key, DEADLINE)
            .await?
            .unwrap()
            .batch(),
        &input[0].slice(0, 1)
    );
    assert!(service.observation().await.0.applied >= index);
    // Pressure can publish the first cut while readers hold it. The resumed
    // suffix then has its own deliberately long timer; this test asserts apply
    // liveness and visibility, not expiry of that publication timer.
    stop.send(()).unwrap();
    task.await??;
    Ok(())
}
