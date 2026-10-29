//! Count real metadata GETs while an owner keeps polling an unchanged snapshot.
use super::*;
use crate::tests::annotation_runtime::automatic_reclamation::deletion_faults::fault_store::{
    self, Faults,
};
use std::sync::atomic::Ordering;

fn checks(service: &Service) -> u64 {
    service
        .phase_observation()
        .iter()
        .find(|phase| phase.phase == "compact")
        .unwrap()
        .completed
}

async fn polls(service: &Service, count: u64) -> Result<()> {
    let before = checks(service);
    tokio::time::timeout(Duration::from_secs(10), async {
        while checks(service) - before < count {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await?;
    Ok(())
}

async fn compacted(
    c: &network::Cluster,
    narrow: &Storage,
    body: &Storage,
    max_body_fragments: usize,
) -> Result<u64> {
    tokio::time::timeout(Duration::from_secs(30), async {
        loop {
            let selected = annotation_publication::read(c, narrow, None).await?;
            if selected.fragments().len() == 1
                && selected_body(c, narrow, body).await?.fragments().len() <= max_body_fragments
                && annotation_publication::state(c)
                    .await?
                    .progress
                    .pending
                    .is_none()
            {
                return Ok::<_, anyhow::Error>(selected.version_id());
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await?
}

async fn unchanged(service: &Service, faults: &Faults) -> Result<()> {
    // Let the final maintenance finish and check its newly selected version.
    // There are no writes or collector, so subsequent polls have the same head.
    polls(service, 3).await?;
    let before = faults.manifest_reads.load(Ordering::Relaxed);
    polls(service, 20).await?;
    let reads = faults.manifest_reads.load(Ordering::Relaxed) - before;
    eprintln!("unchanged snapshot: 20 compaction polls, {reads} manifest GETs");
    assert_eq!(reads, 0, "unchanged compaction polls reread manifests");
    Ok(())
}

#[tokio::test]
async fn mixed_compaction_checks_each_selected_version_once() -> Result<()> {
    let root = artifact_root()?;
    let c = network::Cluster::open_signed(root.path(), annotation_publication::one_shard()).await?;
    let faults = Arc::new(Faults::default());
    // Disable Lance's metadata cache so it cannot hide redundant opens.
    let narrow =
        fault_store::storage_with_cache(&root.path().join("annotations.lance"), faults.clone(), 0)
            .await?;
    let body =
        fault_store::storage_with_cache(&root.path().join("body.lance"), faults.clone(), 0).await?;
    fragmented(&c, &narrow, &body).await?;
    let (a, runtime) = compact_owner(&c, &narrow, &body).await?;
    let (stop, task) = annotation_runtime::run_owner(runtime);
    let before = compacted(&c, &narrow, &body, 1).await?;
    unchanged(&a, &faults).await?;

    // A real publication adds small fragments to both tables. The remembered
    // no-work result must not suppress maintenance on this new selected head.
    let p = producer(&a, 2).await?;
    let create = request(&a, &p, 1, encoded(create(&["D"], &["s"], &[4])?)?).await?;
    let result = a.submit(&create, Duration::from_secs(10)).await?;
    annotation_runtime::drained(&c, &a, result.index).await?;
    // The existing body fragment may now exceed the small-file threshold;
    // the two narrow fragments must still be compacted back to one.
    assert!(compacted(&c, &narrow, &body, 2).await? > before);
    unchanged(&a, &faults).await?;
    for (id, value) in [("A", 1), ("B", 2), ("C", 3), ("D", 4)] {
        check(&a, id, "s", value, "pending", None).await?;
    }
    assert_eq!(a.submit(&create, Duration::from_secs(10)).await?, result);
    stop.send(()).unwrap();
    task.await??;

    // A replacement owner reopens the durable view, then can again poll idle
    // without depending on the predecessor's process-local cached decision.
    let (a, runtime) = compact_owner(&c, &narrow, &body).await?;
    let (stop, task) = annotation_runtime::run_owner(runtime);
    unchanged(&a, &faults).await?;
    check(&a, "D", "s", 4, "pending", None).await?;
    assert_eq!(a.submit(&create, Duration::from_secs(10)).await?, result);
    stop.send(()).unwrap();
    task.await??;
    c.shutdown().await
}
