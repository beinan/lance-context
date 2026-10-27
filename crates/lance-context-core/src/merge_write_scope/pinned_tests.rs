use super::*;
use crate::RolloutStore;
use lance::dataset::builder::DatasetBuilder;

/// Count attempts outside the guard, including those rejected before storage.
#[derive(Debug)]
struct CountedCommit {
    guard: GuardedCommit,
    attempts: AtomicU64,
}

#[async_trait::async_trait]
impl CommitHandler for CountedCommit {
    fn is_version_not_found_definitive(&self) -> bool {
        self.guard.is_version_not_found_definitive()
    }

    async fn commit(
        &self,
        manifest: &mut Manifest,
        indices: Option<Vec<IndexMetadata>>,
        base_path: &Path,
        object_store: &ObjectStore,
        manifest_writer: ManifestWriter,
        naming_scheme: ManifestNamingScheme,
        transaction: Option<Transaction>,
    ) -> std::result::Result<ManifestLocation, CommitError> {
        self.attempts.fetch_add(1, Ordering::SeqCst);
        self.guard
            .commit(
                manifest,
                indices,
                base_path,
                object_store,
                manifest_writer,
                naming_scheme,
                transaction,
            )
            .await
    }
}

#[tokio::test]
async fn pinned_publication_rejects_rebase_without_native_retries() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap();
    let _store = RolloutStore::open(uri).await.unwrap();
    let handler = Arc::new(CountedCommit {
        guard: GuardedCommit::new(
            lance_table::io::commit::commit_handler_from_url(uri, &None)
                .await
                .unwrap(),
        ),
        attempts: AtomicU64::new(0),
    });
    let mut stale = DatasetBuilder::from_uri(uri)
        .with_commit_handler(handler.clone())
        .load()
        .await
        .unwrap();
    let expected = stale.version().version + 1;
    let mut peer = lance::Dataset::open(uri).await.unwrap();
    peer.update_metadata([("peer", "committed")]).await.unwrap();

    // Keep Lance's default retry policy. The guard must end this operation,
    // without depending on the caller to disable retries or on a timing bound.
    let scope = MergeWriteScope::new();
    let error = scope
        .run(at_base_version(expected, async {
            stale.update_metadata([("stale", "must-not-publish")]).await
        }))
        .await
        .unwrap_err();
    scope.drain().await;
    assert_eq!(handler.attempts.load(Ordering::SeqCst), 1, "{error}");
    assert!(matches!(error, Error::CommitConflict { .. }), "{error}");
    assert!(!scope.has_uncertain_commit());
    assert_eq!(scope.completed_steps(), 0);

    stale.checkout_latest().await.unwrap();
    assert_eq!(stale.version().version, expected);
    assert_eq!(
        stale.metadata().get("peer").map(String::as_str),
        Some("committed")
    );
    assert!(!stale.metadata().contains_key("stale"));

    // The rejection is scoped to the old publication. Reconciliation can use
    // the same handle to publish at a newly selected version.
    at_base_version(expected + 1, async {
        stale.update_metadata([("fresh", "committed")]).await
    })
    .await
    .unwrap();
    assert_eq!(handler.attempts.load(Ordering::SeqCst), 2);
    assert_eq!(stale.version().version, expected + 1);
    assert_eq!(
        stale.metadata().get("fresh").map(String::as_str),
        Some("committed")
    );
    assert!(!stale.metadata().contains_key("stale"));
}
