//! A live-only page needs no retained-manifest I/O; the next page still collects.
use super::*;
use lance_context_api::annotation::{AnnotationFieldUpdate::Set, TurnAnnotationPatch};
use lance_context_core::annotation_staging::{AnnotationStaging, Scope};
use std::sync::atomic::Ordering;

#[tokio::test]
async fn annotation_collector_skips_manifest_reads_after_a_page_is_fully_protected() -> Result<()> {
    let root = root()?;
    let path = root.path().join("turns.lance");
    let c = network::Cluster::open_signed(root.path(), annotation_publication::one_shard()).await?;
    let faults = Arc::new(Faults::default());
    let storage = fault_store::storage(&path, faults.clone()).await?;
    annotation_publication::seed_contract(&c, &storage, Service::contract(&binding(), &limits())?)
        .await?;
    let original = files(&path)?;
    let staging = AnnotationStaging::new(
        annotation_publication::read(&c, &storage, None).await?,
        Scope {
            table_instance: binding().incarnation.to_string(),
            source_group: 10,
            shard: 0,
        },
        0,
        limits(),
    )?;
    staging
        .apply(
            0,
            1,
            "T",
            &TurnAnnotationPatch {
                score: Set(0.9),
                ..Default::default()
            },
        )
        .await?;
    let frozen = staging.freeze(Uuid::new_v4()).await?.unwrap();
    let prepared = frozen
        .prepare(&MergeMemoryBudget::new(8 << 20), 8192)
        .await?;
    let abandoned: Vec<_> = files(&path)?.difference(&original).cloned().collect();
    assert_eq!(abandoned.len(), 1, "one real uncommitted Lance column file");
    drop((prepared, frozen, staging));

    let (service, runtime) = owned(&c, &storage, &limits()).await?;
    let (entered, reached) = oneshot::channel();
    let (resume, resumed) = oneshot::channel();
    faults.pause_listing(&path, Some(original.first().unwrap()), entered, resumed)?;
    faults.manifest_reads.store(0, Ordering::Relaxed);
    let (stop, task) = run_owner(whole_page(runtime)?);
    tokio::time::timeout(Duration::from_secs(30), reached).await??;
    // A second pass starts only after protection of the first page finished.
    // The remaining listing response is held, so it cannot add any candidates.
    tokio::time::timeout(Duration::from_secs(30), async {
        while service.reclamation_observation().passes < 2 {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await?;
    let protected_page_reads = faults.manifest_reads.load(Ordering::Relaxed);
    let protected_page_deletes = faults.calls.lock().unwrap().len();
    resume.send(()).unwrap();
    wait_files(&path, original.len()).await?;
    let following_page_reads = faults.manifest_reads.load(Ordering::Relaxed);
    stop.send(()).unwrap();
    task.await??;
    assert_eq!(files(&path)?, original);
    assert!(abandoned.iter().all(|p| !p.exists()));
    let dataset = annotation_publication::read(&c, &storage, None).await?;
    assert_eq!(dataset.count_rows(None).await?, 2);
    assert!(dataset.read_transaction().await?.is_some());
    c.shutdown().await?;
    eprintln!(
        "protected page manifest GETs={protected_page_reads}; following page total={following_page_reads}"
    );
    assert_eq!(protected_page_deletes, 0, "live page must delete nothing");
    assert!(
        following_page_reads > protected_page_reads,
        "orphan page still needs history protection"
    );
    assert_eq!(
        protected_page_reads, 0,
        "fully protected page reread retained manifests"
    );
    Ok(())
}
