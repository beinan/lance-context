//! Collect only native BTree page objects after protecting every raw index UUID.
use super::*;
use lance::cache::{CacheKey, LanceCache};
use lance_io::object_store::ObjectStore;
use lance_table::{
    format::{IndexMetadata, Manifest},
    io::commit::ManifestLocation,
};
use object_store::ObjectStoreExt;
use std::{borrow::Cow, collections::BTreeSet};

/// Native single-segment builds/merges write these two files. Leave unfamiliar
/// layouts and all non-index paths to their own collection policy.
pub(super) fn candidate(path: &Path, base: &Path) -> Option<Uuid> {
    if path.as_ref().len() > 4096 {
        return None;
    }
    let prefix = base.clone().join("_indices");
    let relative = path
        .as_ref()
        .strip_prefix(prefix.as_ref())?
        .strip_prefix('/')?;
    let (directory, file) = relative.split_once('/')?;
    if !matches!(file, "page_lookup.lance" | "page_data.lance") {
        return None;
    }
    let uuid = Uuid::parse_str(directory).ok()?;
    (!uuid.is_nil() && uuid.to_string() == directory).then_some(uuid)
}

struct Key<'a> {
    location: &'a ManifestLocation,
    bytes: u64,
}
impl CacheKey for Key<'_> {
    type ValueType = Vec<IndexMetadata>;
    fn key(&self) -> Cow<'_, str> {
        // The collector already has a private namespace for this fixed binding.
        // Include the immutable object identity, not only its Lance version.
        format!(
            "index-roots:{:?}:{:?}:{}:{}:{}",
            self.location.identity,
            self.location.e_tag,
            self.location.version,
            self.bytes,
            self.location.path
        )
        .into()
    }
    fn type_name() -> &'static str {
        "lance-context.retained-index-roots"
    }
}

#[allow(clippy::too_many_arguments)]
pub(super) async fn protect(
    candidates: &mut BTreeMap<Path, u64>,
    manifest: &Manifest,
    location: &ManifestLocation,
    base: &Path,
    store: &ObjectStore,
    cache: &LanceCache,
    budget: &Arc<MergeMemoryBudget>,
    already_reserved: usize,
) -> Result<()> {
    if manifest.index_section.is_none() || !candidates.keys().any(|p| candidate(p, base).is_some())
    {
        return Ok(());
    }
    ensure!(
        manifest.version == location.version,
        "index root manifest version differs"
    );
    let bytes = match location.size {
        Some(size) => size,
        None => store.inner.head(&location.path).await?.size,
    };
    let charge = usize::try_from(bytes)?
        .checked_mul(4)
        .context("index root workspace overflow")?;
    ensure!(
        already_reserved
            .checked_add(charge)
            .is_some_and(|n| n <= budget.limit()),
        "retained index metadata exceeds collection workspace"
    );
    let _reservation = budget.reserve(charge).await;
    let indices = cache
        .get_or_insert_with_key(Key { location, bytes }, || async {
            let mut bounded = location.clone();
            bounded.size = Some(bytes);
            // Query-visible metadata may omit hidden/unsupported native indices.
            // Native cleanup also uses this raw section to protect directory UUIDs.
            lance_table::io::manifest::read_manifest_indexes(store, &bounded, manifest).await
        })
        .await?;
    let live: BTreeSet<_> = indices.iter().map(|index| index.uuid).collect();
    candidates.retain(|path, _| candidate(path, base).is_none_or(|uuid| !live.contains(&uuid)));
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use lance::index::DatasetIndexExt;
    #[test]
    fn index_candidates_are_only_exact_native_page_paths() {
        let base = Path::from("owned/narrow");
        let uuid = Uuid::new_v4();
        for file in ["page_lookup.lance", "page_data.lance"] {
            assert_eq!(
                candidate(
                    &Path::from(format!("owned/narrow/_indices/{uuid}/{file}")),
                    &base
                ),
                Some(uuid)
            );
        }
        for path in [
            format!("owned/narrow-other/_indices/{uuid}/page_data.lance"),
            format!("owned/narrow/_indices/{uuid}/nested/page_data.lance"),
            format!("owned/narrow/_indices/{uuid}/unknown.lance"),
            format!("owned/narrow/_indices/{uuid}/part_0_page_data.lance"),
            format!("owned/narrow/data/{uuid}/page_data.lance"),
            "owned/narrow/_indices/not-a-uuid/page_data.lance".into(),
            format!("owned/narrow/_indices/{}/page_data.lance", Uuid::nil()),
        ] {
            assert_eq!(candidate(&Path::from(path), &base), None);
        }
    }

    #[tokio::test]
    async fn raw_index_roots_protect_a_version_hidden_from_query_planning() -> Result<()> {
        use arrow_array::{RecordBatch, RecordBatchIterator, StringArray};
        use lance::dataset::{CommitBuilder, WriteParams};
        use lance_table::transaction::{Operation, Transaction};
        let mut builder = tempfile::Builder::new();
        builder.prefix("annotation-index-roots-");
        let root = if let Ok(parent) = std::env::var("INGEST_TEST_ARTIFACTS") {
            builder.disable_cleanup(true).tempdir_in(parent)?
        } else {
            builder.tempdir()?
        };
        let storage = Storage::resolve(
            root.path().join("narrow.lance").to_str().unwrap().into(),
            Default::default(),
            Arc::default(),
        )
        .await?;
        let batch = RecordBatch::try_from_iter([(
            "id",
            Arc::new(StringArray::from(vec!["T"])) as arrow_array::ArrayRef,
        )])?;
        let mut dataset = Dataset::write(
            RecordBatchIterator::new(vec![Ok(batch.clone())], batch.schema()),
            &storage.uri,
            Some(WriteParams {
                enable_stable_row_ids: true,
                skip_auto_cleanup: true,
                ..Default::default()
            }),
        )
        .await?;
        let mut index = dataset
            .create_index_builder(
                &["id"],
                lance_index::IndexType::BTree,
                &lance_index::scalar::ScalarIndexParams::default(),
            )
            .name("future_index".into())
            .execute_uncommitted()
            .await?;
        let uuid = index.uuid;
        index.index_version = i32::MAX;
        let transaction = Transaction::new_from_version(
            dataset.version_id(),
            Operation::CreateIndex {
                new_indices: vec![index],
                removed_indices: vec![],
            },
        );
        let selected = CommitBuilder::new(Arc::new(dataset))
            .with_max_retries(0)
            .with_skip_auto_cleanup(true)
            .execute(transaction)
            .await?;
        assert!(selected.load_indices().await?.is_empty());
        let live = storage
            .base
            .clone()
            .join("_indices")
            .join(uuid.to_string())
            .join("page_data.lance");
        let abandoned = storage
            .base
            .clone()
            .join("_indices")
            .join(Uuid::new_v4().to_string())
            .join("page_data.lance");
        let mut page = BTreeMap::from([(live.clone(), 1), (abandoned.clone(), 1)]);
        let store = selected.object_store(None).await?;
        let cache = storage
            .session
            .file_metadata_cache()
            .with_key_prefix("raw-index-root-test");
        // Refusing a root read must leave the entire candidate page intact;
        // the caller cannot safely classify even abandoned pages yet.
        let before = page.clone();
        let budget = MergeMemoryBudget::new(1 << 20);
        let page_reservation = budget.reserve(budget.limit()).await;
        let error = protect(
            &mut page,
            selected.manifest(),
            selected.manifest_location(),
            &storage.base,
            &store,
            &cache,
            &budget,
            page_reservation.bytes(),
        )
        .await
        .unwrap_err();
        assert_eq!(
            error.to_string(),
            "retained index metadata exceeds collection workspace"
        );
        assert_eq!(page, before);
        drop(page_reservation);
        protect(
            &mut page,
            selected.manifest(),
            selected.manifest_location(),
            &storage.base,
            &store,
            &cache,
            &MergeMemoryBudget::new(8 << 20),
            0,
        )
        .await?;
        assert!(!page.contains_key(&live));
        assert_eq!(page, BTreeMap::from([(abandoned, 1)]));
        Ok(())
    }
}
