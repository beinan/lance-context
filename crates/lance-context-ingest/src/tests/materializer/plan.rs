use super::*;
use crate::tests::annotation_runtime::lost_prepare::proxy::Proxy;
use lance_context_wal::replication::publication as p;
use std::sync::atomic::Ordering;

#[derive(Clone, Copy)]
enum Case {
    Committed,
    Absent,
    DifferentContent,
    Fenced,
}

async fn exercise(case: Case) -> Result<()> {
    let root = tempfile::tempdir()?;
    let path = root.path().join("plans.lance");
    let mut cluster = network::Cluster::open_signed(root.path(), layout()).await?;
    bootstrap(&cluster, &path, &binding()).await?;
    let proxy = Proxy::open_plan(
        &cluster,
        root.path(),
        matches!(case, Case::Committed | Case::Fenced),
    )
    .await?;
    let direct = std::mem::replace(&mut cluster.client, proxy.client.clone());
    let ingress = connect(&cluster, binding()).await?;
    let storage = Storage::resolve(
        path.to_str().unwrap().into(),
        Default::default(),
        Arc::default(),
    )
    .await?;
    let budget = MergeMemoryBudget::new(32 << 20);
    let publisher = publisher(&ingress, &storage, &budget, 1).await?;
    let view = publisher.state().await?;
    let mut plan = p::Plan {
        transaction: Uuid::new_v4(),
        read_version: view.manifests.as_ref().unwrap().head,
        sources: BTreeMap::new(),
        next_group: 10,
    };
    if matches!(case, Case::DifferentContent) {
        let response = direct
            .request_routed(Request::Commit(Box::new(Command::Plan(p::PlanRequest {
                authority: publisher.authority().clone(),
                plan: plan.clone(),
            }))))
            .await?;
        assert!(matches!(
            response.reply,
            Reply::Commit(Outcome::Committed(Applied::Publisher(
                p::Reply::Planned { .. }
            )))
        ));
        // Same UUID, different version: UUID equality alone must not confirm it.
        plan.read_version += 1;
    }
    let attempted = plan.clone();
    let original_authority = publisher.authority().clone();
    let operation = tokio::spawn(async move { publisher.record_plan(attempted).await });
    if matches!(case, Case::Committed | Case::Fenced) {
        tokio::time::timeout(Duration::from_secs(10), proxy.committed.notified()).await?;
        if matches!(case, Case::Fenced) {
            cluster.client = direct.clone();
            let ingress = connect(&cluster, binding()).await?;
            let successor =
                super::publisher(&ingress, &storage, &budget, original_authority.epoch).await?;
            assert_ne!(successor.authority(), &original_authority);
        }
        proxy.release.notify_one();
    }
    let result = tokio::time::timeout(Duration::from_secs(10), operation).await??;
    assert_eq!(
        result.is_ok(),
        matches!(case, Case::Committed),
        "{result:?}"
    );
    assert_eq!(proxy.dropped.load(Ordering::SeqCst), 1);
    assert_eq!(
        proxy.plans.load(Ordering::SeqCst),
        1,
        "uncertain plan was resubmitted"
    );
    cluster.client = direct;
    // Query through the proxy-independent client, including when fenced.
    let response = cluster
        .client
        .request_routed(Request::Query {
            group: original_authority.group,
            query: query::Query::Publisher {
                table: original_authority.table,
            },
        })
        .await?;
    let Reply::Query(query::Outcome::Observed {
        value: query::Value::Publisher(Some(view)),
        ..
    }) = response.reply
    else {
        bail!("missing test state")
    };
    if matches!(case, Case::Absent) {
        assert!(view.progress.pending.is_none());
    } else {
        let pending = view.progress.pending.as_ref().unwrap();
        assert_eq!(pending.plan.transaction, plan.transaction);
        assert_eq!(
            pending.plan.read_version,
            if matches!(case, Case::DifferentContent) {
                plan.read_version - 1
            } else {
                plan.read_version
            }
        );
    }
    drop(proxy);
    cluster.shutdown().await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn committed_unknown_plan_is_observed_without_resubmission() -> Result<()> {
    exercise(Case::Committed).await
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn absent_unknown_plan_remains_unknown() -> Result<()> {
    exercise(Case::Absent).await
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn unknown_plan_requires_full_content_match() -> Result<()> {
    exercise(Case::DifferentContent).await
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn unknown_plan_cannot_bypass_owner_fencing() -> Result<()> {
    exercise(Case::Fenced).await
}
