use super::*;
use crate::http::annotations::StatusRequest;
use crate::materializer::Acquisition;
use lance_context_core::annotation_staging::StatusMatches;

async fn query(client: &reqwest::Client, server: &Server, bucket: u32) -> Result<StatusMatches> {
    Ok(post(client, server, "filter-status")
        .json(&StatusRequest {
            bucket,
            status: Some("pending".into()),
            limit: 16,
        })
        .send()
        .await?
        .error_for_status()?
        .json()
        .await?)
}

async fn indexed_owner(
    physical: &Physical,
    c: &network::Cluster,
    layout: &Arc<Layout>,
    wait: Duration,
) -> Result<(annotations::Service, annotations::runtime::Runtime)> {
    annotations::runtime::Runtime::connect_mixed_shard(
        c.client.clone(),
        layout.clone(),
        physical.binding.clone(),
        physical.narrow.clone(),
        physical.body.clone(),
        annotations::runtime::Configuration {
            limits: fixture::limits(),
            upload_budget: MergeMemoryBudget::new(8 << 20),
            apply_budget: MergeMemoryBudget::new(8 << 20),
            publication_budget: MergeMemoryBudget::new(512 << 20),
            publication: crate::materializer::Options {
                max_source_bytes: 1 << 20,
                min_source_bytes: 1 << 20,
                max_records: 64,
                max_batch_wait: wait,
                status_index: Some(crate::materializer::StatusIndex::default()),
                ..Default::default()
            },
        },
        Acquisition {
            owner: Uuid::new_v4(),
            request: Uuid::new_v4(),
            expected_epoch: physical.view(c).await?.authority.epoch,
        },
        physical.shard,
    )
    .await
}

async fn indexed(client: &reqwest::Client, server: &Server, bucket: u32) -> Result<StatusMatches> {
    tokio::time::timeout(Duration::from_secs(30), async {
        loop {
            let result = query(client, server, bucket).await?;
            if result.indexed_fragments > 0 && result.unindexed_fragments == 0 {
                return Ok::<_, anyhow::Error>(result);
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await?
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn sharded_status_filters_report_independent_cuts_and_catch_up_after_takeover() -> Result<()>
{
    tokio::time::timeout(Duration::from_secs(90), exercise()).await?
}

async fn exercise() -> Result<()> {
    let root = tempfile::tempdir()?;
    let mut definition = (*layout()).clone();
    definition.buckets = vec![10, 20];
    let layout = Arc::new(definition);
    let c = network::Cluster::open_signed(root.path(), layout.clone()).await?;
    let a = Physical::provision(&c, &layout, root.path(), 0).await?;
    let b = Physical::provision(&c, &layout, root.path(), 1).await?;
    let ta = a.turn(&layout);
    let tb = b.turn(&layout);
    let (sa, ra) = indexed_owner(&a, &c, &layout, Duration::from_secs(60)).await?;
    let (sb, rb) = indexed_owner(&b, &c, &layout, Duration::from_millis(20)).await?;
    let oa = Owner::new(&ra, keys()?, options())?;
    let ob = Owner::new(&rb, keys()?, options())?;
    let owner_a = Server::start(oa.router(), None).await?;
    let owner_b = Server::start(ob.router(), None).await?;
    let (_stop_a, task_a) = annotation_runtime::run_owner(ra);
    let (stop_b, task_b) = annotation_runtime::run_owner(rb);
    let ga = Forwarder::new(
        routes(&c, &layout, &[(&a, &owner_a), (&b, &owner_b)]).await?,
        keys()?,
        options(),
        TOKEN.into(),
    )?;
    let gb = Forwarder::new(
        routes(&c, &layout, &[(&b, &owner_b), (&a, &owner_a)]).await?,
        keys()?,
        options(),
        TOKEN.into(),
    )?;
    let ia = Server::start(ga.router(), None).await?;
    let ib = Server::start(gb.router(), None).await?;
    let client = reqwest::Client::builder()
        .retry(reqwest::retry::never())
        .timeout(Duration::from_secs(30))
        .build()?;
    let pa = open_bucket(&client, &ia, 0).await?;
    let pb = open_bucket(&client, &ib, 1).await?;
    let (ack_a, ack_b) = tokio::try_join!(
        mutate(
            &client,
            &ib,
            &pa,
            1,
            a.encode(fixture::create(&[&ta], &["s"], &[7])?)?
        ),
        mutate(
            &client,
            &ia,
            &pb,
            1,
            b.encode(fixture::create(&[&tb], &["s"], &[9])?)?
        ),
    )?;
    b.drained(&c, &layout, &sb, ack_b.completion.index).await?;
    let (lagging, caught_up) = tokio::try_join!(query(&client, &ib, 0), indexed(&client, &ia, 1))?;
    assert!(lagging.ids.is_empty());
    assert!(lagging.applied_through_at_capture >= ack_a.completion.index);
    assert!(lagging.materialized_through < ack_a.completion.index);
    assert_eq!(lagging.scope.shard, 0);
    assert_eq!(lagging.scope.source_group, 10);
    assert_eq!(caught_up.ids, vec![tb.clone()]);
    assert!(caught_up.materialized_through >= ack_b.completion.index);
    assert_eq!(caught_up.scope.shard, 1);
    assert_eq!(caught_up.scope.source_group, 20);
    assert_eq!(
        read(&client, &ia, &ta).await?.status.as_deref(),
        Some("pending")
    );
    for (bucket, limit) in [(2, 1), (0, 0), (0, 1025)] {
        let response = post(&client, &ib, "filter-status")
            .json(&StatusRequest {
                bucket,
                status: None,
                limit,
            })
            .send()
            .await?;
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    }

    // Reopen the same shard/WAL suffix with a new fenced owner, through the
    // unchanged ingress routes. Its pending create must reach the native index.
    task_a.abort();
    assert!(task_a.await.unwrap_err().is_cancelled());
    oa.shutdown().await;
    let address = owner_a.address;
    owner_a.shutdown().await?;
    drop(sa);
    let (sa, ra) = indexed_owner(&a, &c, &layout, Duration::from_millis(20)).await?;
    let oa = Owner::new(&ra, keys()?, options())?;
    let owner_a = Server::start(oa.router(), Some(address)).await?;
    let (stop_a, task_a) = annotation_runtime::run_owner(ra);
    a.drained(&c, &layout, &sa, ack_a.completion.index).await?;
    let (left, right) = tokio::try_join!(indexed(&client, &ia, 0), indexed(&client, &ib, 1))?;
    assert_eq!(left.ids, vec![ta]);
    assert_eq!(right.ids, vec![tb]);
    assert!(left.materialized_through >= ack_a.completion.index);
    assert_eq!(left.scope.table_instance, a.binding.incarnation.to_string());
    assert_eq!(
        right.scope.table_instance,
        b.binding.incarnation.to_string()
    );
    stop_a.send(()).unwrap();
    task_a.await??;
    stop_b.send(()).unwrap();
    task_b.await??;
    ga.shutdown().await;
    gb.shutdown().await;
    ia.shutdown().await?;
    ib.shutdown().await?;
    oa.shutdown().await;
    ob.shutdown().await;
    owner_a.shutdown().await?;
    owner_b.shutdown().await?;
    c.shutdown().await
}
