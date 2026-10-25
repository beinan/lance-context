//! Storage counterexamples for GC coordination. Only disposable local datasets
//! use native cleanup here; managed-table cleanup remains disabled.
use super::*;
use lance::dataset::cleanup::{cleanup_old_versions, CleanupPolicy};

async fn update(staging: &AnnotationStaging, position: u64) -> Result<Arc<Dataset>> {
    staging
        .apply(
            position - 1,
            position,
            "T",
            &TurnAnnotationPatch {
                score: Set(position as f32),
                ..Default::default()
            },
        )
        .await?;
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

fn data_paths(dataset: &Dataset) -> BTreeSet<String> {
    dataset
        .manifest()
        .fragments
        .iter()
        .flat_map(|f| f.referenced_lance_files())
        .map(|f| f.path.clone())
        .collect()
}

fn exists(root: &std::path::Path, path: &str) -> bool {
    root.join("data").join(path).is_file()
}

async fn verify_current(root: &std::path::Path, expected: f32) -> Result<()> {
    // Reopen without the former Session's caches, then read every body byte.
    let dataset = Arc::new(Dataset::open(root.to_str().unwrap()).await?);
    assert_eq!(dataset.count_rows(None).await?, 3);
    let rows = dataset
        .scan()
        .try_into_stream()
        .await?
        .try_collect::<Vec<_>>()
        .await?;
    for batch in rows {
        let bodies = batch
            .column_by_name("body")
            .unwrap()
            .as_any()
            .downcast_ref::<LargeBinaryArray>()
            .unwrap();
        for body in bodies.iter().flatten() {
            assert_eq!(body.len(), 1 << 20);
            assert!(body
                .iter()
                .enumerate()
                .all(|(i, b)| *b == ((i * 31 + i / 17) % 251) as u8));
        }
    }
    // This helper verifies stored values, not a recovered WAL coverage proof.
    let recovered = AnnotationStaging::new(dataset, scope(), 0, Limits::default())?;
    assert_eq!(current(&recovered, "T").await?.score, Some(expected));
    assert_eq!(current(&recovered, "U").await?.score, None);
    Ok(())
}

#[tokio::test]
async fn retained_reader_floor_defers_physical_annotation_reclamation() -> Result<()> {
    for stable in [false, true] {
        let root = tempfile::tempdir()?;
        let path = root.path().join("turns.lance");
        let original = fixture_with_row_ids(&path, stable).await?;
        let bodies = body_paths(&original)?;
        let staging = AnnotationStaging::new(original, scope(), 0, Limits::default())?;
        let first = update(&staging, 1).await?;
        let held = staging.capture("T").await?;
        let latest = update(&staging, 2).await?;
        let obsolete: Vec<_> = data_paths(&first)
            .difference(&data_paths(&latest))
            .cloned()
            .collect();
        assert_eq!(obsolete.len(), 1);
        assert!(exists(&path, &obsolete[0]));
        assert_eq!(body_paths(&latest)?, bodies);
        assert_eq!(data_paths(&latest).len(), 3);
        // Staging can be empty while a reader still needs the old data object.
        assert_eq!(staging.observation().await.retained_entries, 0);
        let removed = cleanup_old_versions(
            &latest,
            CleanupPolicy {
                before_version: Some(held.base_version),
                ..Default::default()
            },
        )
        .await?;
        assert_eq!(removed.data_files_removed, 0);
        assert!(exists(&path, &obsolete[0]));
        assert_eq!(held.resolve().await?.unwrap().score, Some(1.0));
        drop((held, first));
        let removed = cleanup_old_versions(
            &latest,
            CleanupPolicy {
                before_version: Some(latest.version_id()),
                ..Default::default()
            },
        )
        .await?;
        assert_eq!(removed.data_files_removed, 1);
        assert_eq!(removed.failed_deletes, 0);
        assert!(!exists(&path, &obsolete[0]));
        assert!(data_paths(&latest).iter().all(|p| exists(&path, p)));
        assert!(latest.read_transaction().await?.is_some());
        verify_current(&path, 2.0).await?;
        drop(latest);
        // Continue draining known old data, rather than qualifying only one
        // successful deletion. The two original files and one current narrow
        // file are the complete data working set for this fixture.
        for position in 3..=8 {
            let selected = update(&staging, position).await?;
            let removed = cleanup_old_versions(
                &selected,
                CleanupPolicy {
                    before_version: Some(selected.version_id()),
                    ..Default::default()
                },
            )
            .await?;
            assert_eq!(removed.data_files_removed, 1);
            assert_eq!(removed.failed_deletes, 0);
            assert_eq!(data_paths(&selected).len(), 3);
            assert_eq!(
                std::fs::read_dir(path.join("data"))?
                    .collect::<std::io::Result<Vec<_>>>()?
                    .len(),
                3
            );
            assert_eq!(staging.observation().await.retained_entries, 0);
            assert_eq!(current(&staging, "T").await?.score, Some(position as f32));
        }
        verify_current(&path, 8.0).await?;
    }
    Ok(())
}

#[tokio::test]
async fn native_cleanup_does_not_observe_a_held_annotation_reader() -> Result<()> {
    let root = tempfile::tempdir()?;
    let path = root.path().join("turns.lance");
    let staging = AnnotationStaging::new(fixture(&path).await?, scope(), 0, Limits::default())?;
    update(&staging, 1).await?;
    // A separate Session is essential: preparing the next update can cache the
    // prior column data in the writer's Session, hiding the deleted object.
    let reader = AnnotationStaging::new(
        Arc::new(Dataset::open(path.to_str().unwrap()).await?),
        scope(),
        1,
        Limits::default(),
    )?;
    let held = reader.capture("T").await?;
    let latest = update(&staging, 2).await?;
    let removed = cleanup_old_versions(
        &latest,
        CleanupPolicy {
            before_version: Some(latest.version_id()),
            ..Default::default()
        },
    )
    .await?;
    assert_eq!(removed.data_files_removed, 1);
    let error = held.resolve().await.unwrap_err().to_string();
    assert!(
        error.contains("Not found")
            || error.contains("not found")
            || error.contains("No such file"),
        "expected missing old annotation data, got: {error}"
    );
    verify_current(&path, 2.0).await?;
    Ok(())
}

#[tokio::test]
async fn unverified_cleanup_can_delete_a_prepared_annotation_file() -> Result<()> {
    let root = tempfile::tempdir()?;
    let path = root.path().join("turns.lance");
    let staging = AnnotationStaging::new(fixture(&path).await?, scope(), 0, Limits::default())?;
    let base = update(&staging, 1).await?;
    staging
        .apply(
            1,
            2,
            "T",
            &TurnAnnotationPatch {
                score: Set(2.0),
                ..Default::default()
            },
        )
        .await?;
    let frozen = staging.freeze(uuid::Uuid::new_v4()).await?.unwrap();
    let prepared = frozen
        .prepare(&crate::MergeMemoryBudget::new(8 << 20), 8192)
        .await?;
    let lance_table::transaction::Operation::Update {
        updated_fragments, ..
    } = &prepared.merge().transaction.operation
    else {
        panic!("expected a prepared column update");
    };
    let uploaded: BTreeSet<_> = updated_fragments
        .iter()
        .flat_map(|f| f.referenced_lance_files())
        .map(|f| f.path.clone())
        .filter(|p| !data_paths(&base).contains(p))
        .collect();
    assert_eq!(uploaded.len(), 1);
    assert!(uploaded.iter().all(|p| exists(&path, p)));
    let removed = cleanup_old_versions(
        &base,
        CleanupPolicy {
            before_version: Some(base.version_id()),
            delete_unverified: true,
            ..Default::default()
        },
    )
    .await?;
    assert_eq!(removed.data_files_removed, 1);
    assert!(uploaded.iter().all(|p| !exists(&path, p)));
    // The selected base still reads correctly; that alone cannot qualify GC.
    verify_current(&path, 1.0).await?;
    Ok(())
}
