use super::*;

#[tokio::test]
async fn status_filter_refuses_full_ingress_budget_without_waiting_for_admission() -> Result<()> {
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
    let (_, runtime) = annotation_runtime::owner(&c, &storage, &limits).await?;
    let owner = Owner::new(&runtime, keys()?, options())?;
    let server = Server::start(owner.router(), None).await?;
    let budget = MergeMemoryBudget::new(8 << 20);
    let ingress = annotations::connect_ingress(
        c.client.clone(),
        annotation_publication::one_shard(),
        binding(),
        &limits,
        budget.clone(),
    )
    .await?;
    let gateway = Forwarder::new(
        vec![(ingress, server.uri())],
        keys()?,
        http::Options {
            admission_timeout: Duration::from_secs(30),
            ..options()
        },
        TOKEN.into(),
    )?;
    let frontend = Server::start(gateway.router(), None).await?;
    let occupied = budget.reserve(budget.limit()).await;
    let client = reqwest::Client::builder()
        .retry(reqwest::retry::never())
        .build()?;
    // Keep capacity occupied throughout the request. The ordinary admission
    // timeout is much longer; a queued query cannot pass this assertion.
    let response = tokio::time::timeout(
        Duration::from_secs(3),
        post(&client, &frontend, "filter-status")
            .json(&http::annotations::StatusRequest {
                bucket: 0,
                status: Some("pending".into()),
                limit: 16,
            })
            .send(),
    )
    .await;
    drop(occupied);
    gateway.shutdown().await;
    frontend.shutdown().await?;
    owner.shutdown().await;
    server.shutdown().await?;
    drop(runtime);
    c.shutdown().await?;
    let response = response.context("optional filter queued on shared ingress memory")??;
    assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
    let refusal: serde_json::Value = response.json().await?;
    assert_eq!(refusal["error"], "memory_capacity");
    assert_eq!(refusal["operation_outcome"], "not_submitted");
    assert_eq!(budget.reserved(), 0);
    Ok(())
}
