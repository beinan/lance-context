//! Reuse immutable protection metadata within the existing session cache budget.
use super::*;
use lance::cache::{CacheKey, LanceCache};
use lance_io::object_store::ObjectStore;
use lance_table::format::Manifest;
use std::borrow::Cow;

struct ManifestKey<'a>(&'a p::ManifestRecord);

impl CacheKey for ManifestKey<'_> {
    type ValueType = Manifest;

    fn key(&self) -> Cow<'_, str> {
        // The UUID is the catalog-selected immutable identity, not just a
        // version number. The validated relative path contains no colons.
        let record = self.0;
        format!(
            "{}:{}:{}:{}",
            record.identity, record.version, record.object.bytes, record.object.path
        )
        .into()
    }

    fn type_name() -> &'static str {
        "lance-context.retained-manifest"
    }
}

pub(super) async fn read(
    cache: &LanceCache,
    store: &ObjectStore,
    path: &Path,
    record: &p::ManifestRecord,
) -> Result<Arc<Manifest>> {
    // The collector's namespace shares the session's weighted limit. Disabled
    // caches and evictions simply read the selected immutable object again.
    Ok(cache
        .get_or_insert_with_key(ManifestKey(record), || async {
            let manifest =
                lance_table::io::manifest::read_manifest(store, path, Some(record.object.bytes))
                    .await?;
            if manifest.version != record.version {
                return Err(lance::Error::invalid_input(
                    "retained manifest version mismatch",
                ));
            }
            Ok(manifest)
        })
        .await?)
}
