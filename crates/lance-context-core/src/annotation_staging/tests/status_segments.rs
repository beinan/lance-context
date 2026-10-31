//! A new index delta must not restore stale matches from an older segment.
use super::*;
use lance_index::optimize::OptimizeOptions;
use lance_table::transaction::{Operation, Transaction};

const NAME: &str = "annotation_status_segments";

async fn flush(staging: &AnnotationStaging) -> Result<Arc<Dataset>> {
    let frozen = staging.freeze(uuid::Uuid::new_v4()).await?.unwrap();
    let prepared = frozen
        .prepare(&crate::MergeMemoryBudget::new(8 << 20), 8192)
        .await?;
    let selected = commit(&prepared).await?;
    staging
        .install_published(&prepared, selected.clone())
        .await?;
    Ok(selected)
}

async fn ids(base: &Dataset, predicate: &str, indexed: bool) -> Result<BTreeSet<String>> {
    let mut scan = base.scan();
    scan.project(&["id"])?;
    scan.filter(predicate)?;
    scan.use_scalar_index(indexed);
    let batches = scan
        .try_into_stream()
        .await?
        .try_collect::<Vec<_>>()
        .await?;
    let mut ids = BTreeSet::new();
    for batch in batches {
        let values = batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        for id in values.iter() {
            assert!(ids.insert(id.unwrap().to_owned()), "duplicate indexed turn");
        }
    }
    Ok(ids)
}

async fn check(base: &Dataset, status: Option<&str>) -> Result<()> {
    for (predicate, expected_t) in [
        ("status = 'pending'", status == Some("pending")),
        ("status = 'reviewed'", status == Some("reviewed")),
        ("status IS NULL", status.is_none()),
        ("status IS NOT NULL", status.is_some()),
        ("NOT (status = 'pending')", status == Some("reviewed")),
    ] {
        let scanned = ids(base, predicate, false).await?;
        assert_eq!(scanned.contains("T"), expected_t, "{predicate}");
        assert_eq!(ids(base, predicate, true).await?, scanned, "{predicate}");
    }
    Ok(())
}

#[tokio::test]
async fn mutable_index_segments_respect_coverage_after_catch_up_and_reopen() -> Result<()> {
    for stable in [false, true] {
        for kind in [IndexType::BTree, IndexType::Bitmap, IndexType::ZoneMap] {
            let root = tempfile::tempdir()?;
            let initial = fixture_with_row_ids(&root.path().join("turns.lance"), stable).await?;
            let staging = AnnotationStaging::new(initial, scope(), 0, Limits::default())?;
            staging
                .apply(
                    0,
                    1,
                    "T",
                    &TurnAnnotationPatch {
                        score: Set(0.1),
                        ..Default::default()
                    },
                )
                .await?;
            let mut base = flush(&staging).await?;
            let mut builder = base.as_ref().clone();
            let index = builder
                .create_index_builder(&["status"], kind, &ScalarIndexParams::default())
                .name(NAME.into())
                .execute_uncommitted()
                .await?;
            let original = index.uuid;
            let mut transaction = Transaction::new_from_version(
                base.version_id(),
                Operation::CreateIndex {
                    new_indices: vec![index],
                    removed_indices: vec![],
                },
            );
            transaction.transaction_properties = base
                .read_transaction()
                .await?
                .unwrap()
                .transaction_properties;
            base = compaction::select(base, &transaction).await?;
            staging.install_indexed(&transaction, base.clone()).await?;
            for (round, status) in [Some("reviewed"), None, Some("pending")]
                .into_iter()
                .enumerate()
            {
                staging
                    .apply(
                        round as u64 + 1,
                        round as u64 + 2,
                        "T",
                        &TurnAnnotationPatch {
                            status: status.map_or(Clear, |value| Set(value.to_owned())),
                            ..Default::default()
                        },
                    )
                    .await?;
                base = flush(&staging).await?;
                check(&base, status).await?;
                let mut next = base.as_ref().clone();
                let mut options = OptimizeOptions::append().index_names(vec![NAME.into()]);
                options.transaction_properties = base
                    .read_transaction()
                    .await?
                    .unwrap()
                    .transaction_properties;
                next.optimize_indices(&options).await?;
                let transaction = next.read_transaction().await?.unwrap();
                base = Arc::new(next);
                staging.install_indexed(&transaction, base.clone()).await?;
                let indices = base.load_indices().await?;
                assert!(
                    indices.iter().any(|i| i.uuid == original),
                    "catch-up must retain the old segment"
                );
                assert!(indices.iter().filter(|i| i.name == NAME).count() >= 2);
                check(&base, status).await?;
                let reopened = Dataset::open(base.uri()).await?;
                check(&reopened, status).await?;
            }
        }
    }
    Ok(())
}
