//! Follow only body references selected by protected narrow snapshots.
use super::*;
use lance_context_core::annotation_staging::BodySnapshot;
use lance_io::object_store::{ObjectStore, ObjectStoreParams, StorageOptionsAccessor};
use lance_table::io::commit::{CommitHandler, ConditionalPutCommitHandler};
use object_store::ObjectStoreExt;

pub(super) async fn store(storage: &Storage) -> Result<Arc<ObjectStore>> {
    let (store, base) = ObjectStore::from_uri_and_params(
        storage.session.store_registry(),
        &storage.uri,
        &ObjectStoreParams {
            storage_options_accessor: Some(Arc::new(StorageOptionsAccessor::with_static_options(
                storage.options.clone(),
            ))),
            ..Default::default()
        },
    )
    .await?;
    ensure!(
        base == storage.base,
        "body collection storage binding changed"
    );
    Ok(store)
}

#[allow(clippy::too_many_arguments)]
pub(super) async fn protect_body(
    candidates: &mut BTreeMap<Path, u64>,
    narrow: &Dataset,
    storage: &Storage,
    store: &ObjectStore,
    incarnation: Uuid,
    budget: &Arc<MergeMemoryBudget>,
    already_reserved: usize,
    retain_data: bool,
) -> Result<()> {
    let reference = BodySnapshot::read(narrow)
        .await?
        .context("protected narrow snapshot has no body reference")?;
    ensure!(
        reference.uri == storage.uri && reference.table_instance == incarnation.to_string(),
        "protected body reference differs from authorized destination"
    );
    // Bound a single body manifest before decoding it. Retained roots are opened
    // one at a time; they do not form another unbounded manifest/root cache.
    let location = ConditionalPutCommitHandler
        .resolve_version_location(&storage.base, reference.version, store.inner.as_ref())
        .await?;
    let bytes = match location.size {
        Some(size) => size,
        None => store.inner.head(&location.path).await?.size,
    };
    let charge = usize::try_from(bytes)?
        .checked_mul(4)
        .context("body manifest workspace overflow")?;
    ensure!(
        already_reserved
            .checked_add(charge)
            .is_some_and(|total| total <= budget.limit()),
        "body retained manifest exceeds collection workspace"
    );
    let _manifest = budget.reserve(charge).await;
    let body = mixed::load_body(storage, &reference, &incarnation.to_string()).await?;
    ensure!(
        body.manifest_location().path == location.path,
        "body manifest location changed"
    );
    protect(
        candidates,
        body.manifest(),
        &location.path,
        &storage.base,
        retain_data,
    )
}
