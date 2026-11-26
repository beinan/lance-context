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

#[tokio::test]
async fn mutable_btree_merge_filters_stale_and_empty_segments() -> Result<()> {
    let root = tempfile::tempdir()?;
    let initial = fixture_with_row_ids(&root.path().join("turns.lance"), true).await?;
    let original_body = body_paths(&initial)?;
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
    let index = base
        .as_ref()
        .clone()
        .create_index_builder(&["status"], IndexType::BTree, &ScalarIndexParams::default())
        .name(NAME.into())
        .fragments(vec![base.fragments()[0].id as u32])
        .execute_uncommitted()
        .await?;
    install_indices(&staging, base, vec![index], vec![]).await?;

    // The first update empties the sole indexed fragment. Lance keeps the last
    // empty definition, but retires empty segments with live siblings. Merge
    // that definition with an uncommitted catch-up segment before publication;
    // subsequent rounds exercise a partially masked source instead.
    // Consolidation must filter each source before their coverage is combined.
    // Otherwise an old pending/reviewed value can become visible again.
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
        let covered: BTreeSet<_> = base
            .load_indices()
            .await?
            .iter()
            .filter(|i| i.name == NAME)
            .flat_map(|i| i.fragment_bitmap.as_ref().unwrap().iter())
            .collect();
        let tail = base
            .fragments()
            .iter()
            .map(|f| f.id as u32)
            .filter(|id| !covered.contains(id))
            .collect::<Vec<_>>();
        assert!(!tail.is_empty());
        let index = base
            .as_ref()
            .clone()
            .create_index_builder(&["status"], IndexType::BTree, &ScalarIndexParams::default())
            .name(NAME.into())
            .replace(true)
            .fragments(tail)
            .execute_uncommitted()
            .await?;
        let removed = base
            .load_indices()
            .await?
            .iter()
            .filter(|i| i.name == NAME)
            .cloned()
            .collect::<Vec<_>>();
        if round == 0 {
            assert!(removed
                .iter()
                .any(|i| i.fragment_bitmap.as_ref().unwrap().is_empty()));
        } else {
            assert!(removed.iter().any(|i| {
                let coverage = i.fragment_bitmap.as_ref().unwrap();
                !coverage.is_empty() && coverage.len() < base.fragments().len() as u64
            }));
        }
        let sources = removed
            .iter()
            .cloned()
            .chain(std::iter::once(index))
            .collect::<Vec<_>>();
        assert!(sources.len() >= 2);
        for source in &sources {
            let files = source
                .files
                .as_ref()
                .expect("native merge needs file costs");
            assert!(files.iter().map(|file| file.size_bytes).sum::<u64>() > 0);
        }
        let merged = base.merge_existing_index_segments(sources.clone()).await?;
        base = install_indices(&staging, base, vec![merged], removed).await?;
        assert_eq!(base.load_indices().await?.len(), 1);
        assert_eq!(body_paths(&base)?, original_body);
        check(&base, status).await?;
        let reopened = Dataset::open(base.uri()).await?;
        check(&reopened, status).await?;
        let mut scan = reopened.scan();
        scan.project(&["id"])?;
        scan.filter(if status.is_none() {
            "status IS NULL"
        } else {
            "status = 'pending'"
        })?;
        let plan = scan.explain_plan(false).await?;
        assert!(plan.contains(NAME), "merged index was not used: {plan}");
        let visible = current(&staging, "T").await?;
        assert_eq!(visible.status.as_deref(), status);
        assert_eq!(visible.score, Some(0.1));
    }
    Ok(())
}

async fn install_indices(
    staging: &AnnotationStaging,
    base: Arc<Dataset>,
    new_indices: Vec<lance_table::format::IndexMetadata>,
    removed_indices: Vec<lance_table::format::IndexMetadata>,
) -> Result<Arc<Dataset>> {
    let mut transaction = Transaction::new_from_version(
        base.version_id(),
        Operation::CreateIndex {
            new_indices,
            removed_indices,
        },
    );
    transaction.transaction_properties = base
        .read_transaction()
        .await?
        .unwrap()
        .transaction_properties;
    let selected = compaction::select(base, &transaction).await?;
    staging
        .install_indexed(&transaction, selected.clone())
        .await?;
    Ok(selected)
}
