//! Bounded file collection for an exclusively owned annotation table.
//! Called only between publications in the same serial publisher loop. Native
//! preparation cannot overlap listing/sealing in this owner; a successor fences
//! the final catalog operation and opens its recovery base after acquisition.
use super::*;
use futures::{stream, stream::BoxStream, StreamExt, TryStreamExt};
use object_store::{path::Path, ObjectMeta, ObjectStoreExt};

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct Options {
    pub max_objects: usize,
    pub max_bytes: usize,
    pub delete_parallelism: usize,
    /// Minimum delay after a completed collection pass. Slow object storage I/O
    /// does not consume this cooldown.
    pub interval: Duration,
}
impl Default for Options {
    fn default() -> Self {
        Self {
            max_objects: 8192,
            max_bytes: 2 << 20,
            delete_parallelism: 16,
            interval: Duration::from_secs(1),
        }
    }
}

impl Options {
    /// Check deployment limits before acquiring ownership or opening storage.
    pub fn validate(&self, publication_workspace: usize) -> Result<()> {
        ensure!(
            (1..=65_536).contains(&self.max_objects)
                && (16 << 10..=16 << 20).contains(&self.max_bytes)
                && (1..=64).contains(&self.delete_parallelism)
                && (Duration::from_millis(1)..=Duration::from_secs(3600)).contains(&self.interval)
                && self.max_bytes * 3 + (64 << 10) <= publication_workspace,
            "invalid annotation collection limits or insufficient publication workspace"
        );
        Ok(())
    }
}

/// Process-local work counters. Delete counts include successful removal of an
/// already absent immutable path; they are not an object-store inventory.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct Observation {
    pub passes: u64,
    pub examined: u64,
    pub deleted: u64,
    pub deleted_bytes: u64,
}

pub(super) struct Collector {
    options: Options,
    next_run: tokio::time::Instant,
    listing: Option<BoxStream<'static, object_store::Result<ObjectMeta>>>,
    deferred: Option<(Path, u64)>,
}
impl Collector {
    pub(super) fn new(options: Options, budget: &Arc<MergeMemoryBudget>) -> Result<Self> {
        options.validate(budget.limit())?;
        Ok(Self {
            options,
            next_run: tokio::time::Instant::now(),
            listing: None,
            deferred: None,
        })
    }

    pub(super) async fn step(
        &mut self,
        service: &Service,
        catalog: &materializer::Publisher,
        storage: &Storage,
        budget: &Arc<MergeMemoryBudget>,
    ) -> Result<()> {
        if tokio::time::Instant::now() < self.next_run {
            return Ok(());
        }
        let result = self.collect_page(service, catalog, storage, budget).await;
        // LIST/protection/DELETE can take longer than the configured interval.
        // Starting the clock before that I/O makes every following publisher
        // iteration eligible for another pass, starving materialization.
        self.next_run = tokio::time::Instant::now() + self.options.interval;
        result
    }

    async fn collect_page(
        &mut self,
        service: &Service,
        catalog: &materializer::Publisher,
        storage: &Storage,
        budget: &Arc<MergeMemoryBudget>,
    ) -> Result<()> {
        let state = catalog.state().await?;
        // A retained plan must first finish exact recovery, including its old
        // base. Do not classify uncommitted files while such a plan exists.
        if state.progress.pending.is_some() {
            return Ok(());
        }
        catalog.retire_history(&state).await?;
        let state = catalog.state().await?;
        ensure!(
            state.progress.pending.is_none(),
            "publication overlapped collection"
        );
        let manifests = state
            .manifests
            .as_ref()
            .context("annotation manifests unavailable")?;
        let roots = service.shared.staging.protected_bases().await?;
        let base = roots
            .datasets()
            .next()
            .context("annotation local base missing")?;
        let object_store = base.object_store(None).await?;
        let store = object_store.inner.clone();
        // Inspect at most the first reference. Do not enumerate arbitrary tag
        // or branch histories into memory. External writers are prohibited by
        // the exclusive-ownership contract, including during this check.
        for prefix in [
            lance::dataset::refs::base_tags_path(&storage.base),
            lance::dataset::refs::base_branches_contents_path(&storage.base),
        ] {
            ensure!(
                store.list(Some(&prefix)).try_next().await?.is_none(),
                "annotation data collection requires a table without tags or branches"
            );
        }
        let page_charge = self.options.max_bytes * 3;
        let _workspace = budget.reserve(page_charge).await;
        if self.listing.is_none() {
            self.listing = Some(store.list(Some(&storage.base)));
        }
        let mut candidates = BTreeMap::new();
        let mut bytes = 0;
        let mut examined = 0;
        for _ in 0..self.options.max_objects {
            let (path, size) = if let Some(deferred) = self.deferred.take() {
                deferred
            } else {
                let Some(meta) = self.listing.as_mut().unwrap().try_next().await? else {
                    self.listing = None;
                    break;
                };
                examined += 1;
                (meta.location, meta.size)
            };
            if !candidate(&path, &storage.base) {
                continue;
            }
            // Charge string storage and map nodes conservatively. A deferred
            // single item prevents dropping the first object of the next page.
            let charge = 2 * path.as_ref().len() + 256;
            if bytes + charge > self.options.max_bytes {
                self.deferred = Some((path, size));
                break;
            }
            bytes += charge;
            candidates.insert(path, size);
        }
        {
            let mut observed = service.shared.reclamation.lock().unwrap();
            observed.passes += 1;
            observed.examined += examined;
        }
        // This finite page is now closed. No more listing or candidate additions
        // occur after protection/sealing, even if a DELETE outlives this owner.
        for base in roots.datasets() {
            protect(
                &mut candidates,
                base.manifest(),
                &base.manifest_location().path,
                &storage.base,
            )?;
        }
        for record in manifests.versions.values() {
            // Open one retained manifest at a time, including bootstrap and the
            // durable advancement receipt. Session metadata has its own bound.
            let charge = usize::try_from(record.object.bytes)?
                .checked_mul(4)
                .context("annotation manifest workspace overflow")?;
            ensure!(
                page_charge + charge <= budget.limit(),
                "annotation retained manifest exceeds collection workspace"
            );
            let _manifest = budget.reserve(charge).await;
            // The captured quorum state already names the selected immutable
            // object. Read its metadata directly; reopening a Dataset here would
            // issue redundant catalog queries for every retained version/page.
            record.object.validate(record.version)?;
            // join takes one path component: passing "_versions/name" would
            // encode the slash as %2F and address a different blob object.
            let name = record.object.path.strip_prefix("_versions/").unwrap();
            let path = storage.base.clone().join("_versions").join(name);
            let manifest = lance_table::io::manifest::read_manifest(
                &object_store,
                &path,
                Some(record.object.bytes),
            )
            .await?;
            ensure!(
                manifest.version == record.version,
                "retained manifest version mismatch"
            );
            protect(&mut candidates, &manifest, &path, &storage.base)?;
        }
        if candidates.is_empty() {
            return Ok(());
        }
        let boundary = manifests.history_boundary();
        let reply = materializer::commit(
            &service.ingress.client,
            Command::RetireHistory(p::RetireManifestHistory {
                authority: catalog.authority().clone(),
                incarnation: service.ingress.scope.incarnation,
                retain_from: boundary.retained_from,
            }),
        )
        .await?;
        ensure!(
            matches!(reply, Applied::Publisher(p::Reply::ManifestHistory(ref actual)) if *actual == boundary),
            "annotation collection fence rejected: {reply:?}"
        );
        // An unknown fence result returns above without deleting anything. A
        // subsequent owner enumerates again; it never guesses the old outcome.
        let outcomes = stream::iter(candidates)
            .map(|(path, bytes)| {
                let store = store.clone();
                async move {
                    match store.delete(&path).await {
                        Ok(()) | Err(object_store::Error::NotFound { .. }) => Ok(bytes),
                        Err(error) => Err(error),
                    }
                }
            })
            .buffer_unordered(self.options.delete_parallelism)
            .collect::<Vec<_>>()
            .await;
        let mut first_error = None;
        let mut observed = service.shared.reclamation.lock().unwrap();
        for outcome in outcomes {
            match outcome {
                Ok(bytes) => {
                    observed.deleted += 1;
                    observed.deleted_bytes += bytes;
                }
                Err(error) => {
                    first_error.get_or_insert(error);
                }
            }
        }
        if let Some(error) = first_error {
            return Err(error).context("deleting sealed annotation file page");
        }
        Ok(())
    }
}

fn candidate(path: &Path, base: &Path) -> bool {
    if path.as_ref().len() > 4096 {
        return false;
    }
    let Some(relative) = path
        .as_ref()
        .strip_prefix(base.as_ref())
        .and_then(|p| p.strip_prefix('/'))
    else {
        return false;
    };
    let Some((directory, name)) = relative.split_once('/') else {
        return false;
    };
    if name.contains('/') {
        return false;
    }
    match directory {
        "data" => name.ends_with(".lance"),
        "_transactions" => name.ends_with(".txn"),
        "_versions" => {
            name.ends_with(".manifest")
                || name
                    .split_once(".manifest-")
                    .is_some_and(|(_, suffix)| Uuid::parse_str(suffix).is_ok())
        }
        _ => false,
    }
}

fn protect(
    candidates: &mut BTreeMap<Path, u64>,
    manifest: &lance_table::format::Manifest,
    manifest_path: &Path,
    base: &Path,
) -> Result<()> {
    ensure!(
        manifest.base_paths.is_empty() && manifest.branch.is_none() && manifest.tag.is_none(),
        "annotation data collection does not support shared or branched bases"
    );
    // Local readers carry their exact immutable manifest location; a retired
    // catalog version can still be required by one of these admitted views.
    candidates.remove(manifest_path);
    if let Some(transaction) = &manifest.transaction_file {
        ensure!(
            !transaction.is_empty() && !transaction.contains('/'),
            "invalid annotation transaction path"
        );
        candidates.remove(
            &base
                .clone()
                .join("_transactions")
                .join(transaction.as_str()),
        );
    }
    let prefix = base.clone().join("data");
    for file in manifest
        .fragments
        .iter()
        .flat_map(|f| f.referenced_lance_files())
    {
        ensure!(
            file.base_id.is_none(),
            "annotation data file has an external base"
        );
        candidates.remove(&prefix.clone().join(file.path.as_str()));
    }
    Ok(())
}
