//! Cancel merge preparation without abandoning a manifest commit already sent
//! to storage. The executor must drain this scope before acknowledging terminal.
use futures::stream::BoxStream;
use lance::{Error, Result};
use lance_io::object_store::ObjectStore;
use lance_table::{
    format::{IndexMetadata, Manifest, Transaction},
    io::commit::{
        CommitError, CommitHandler, ManifestLocation, ManifestNamingScheme, ManifestWriter,
    },
};
use object_store::path::Path;
use std::{
    future::Future,
    sync::{Arc, Mutex},
};
use tokio::sync::{oneshot, Notify};

tokio::task_local! { static CURRENT: Arc<MergeWriteScope>; }

#[derive(Debug, Default)]
struct Progress {
    closed: bool,
    active: usize,
    uncertain: bool,
}

#[derive(Debug, Default)]
pub struct MergeWriteScope {
    progress: Mutex<Progress>,
    changed: Notify,
}

impl MergeWriteScope {
    pub fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    pub async fn run<F: Future>(self: &Arc<Self>, future: F) -> F::Output {
        CURRENT.scope(self.clone(), future).await
    }

    pub fn has_uncertain_commit(&self) -> bool {
        self.progress.lock().unwrap().uncertain
    }

    fn mark_uncertain(&self) {
        self.progress.lock().unwrap().uncertain = true;
    }

    /// Call only after dropping/joining the merge future. Prevent new commits
    /// and join all manifest writes that started before cancellation.
    pub async fn drain(&self) {
        self.progress.lock().unwrap().closed = true;
        loop {
            let changed = self.changed.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            if self.progress.lock().unwrap().active == 0 {
                return;
            }
            changed.await;
        }
    }
}

struct Active {
    scope: Arc<MergeWriteScope>,
    completed: bool,
}
impl Drop for Active {
    fn drop(&mut self) {
        let mut progress = self.scope.progress.lock().unwrap();
        progress.active -= 1;
        progress.uncertain |= !self.completed;
        drop(progress);
        self.scope.changed.notify_waiters();
    }
}

pub(crate) async fn shield<F, T, E>(write: F) -> std::result::Result<T, E>
where
    F: Future<Output = std::result::Result<T, E>> + Send + 'static,
    T: Send + 'static,
    E: From<Error> + Send + 'static,
{
    let Ok(scope) = CURRENT.try_with(Arc::clone) else {
        return write.await;
    };
    {
        let mut progress = scope.progress.lock().unwrap();
        if progress.closed {
            return Err(Error::io("merge cancelled before manifest commit").into());
        }
        progress.active += 1;
    }
    let active = Active {
        scope,
        completed: false,
    };
    let (send, receive) = oneshot::channel();
    tokio::spawn(async move {
        let mut active = active;
        let result = write.await;
        if result.is_err() {
            active.scope.mark_uncertain();
        }
        active.completed = true;
        drop(active);
        let _ = send.send(result);
    });
    receive
        .await
        .map_err(|_| E::from(Error::io("manifest commit executor panicked")))?
}

#[derive(Debug)]
pub(crate) struct GuardedCommit(pub Arc<dyn CommitHandler>);

#[async_trait::async_trait]
#[allow(clippy::too_many_arguments)]
impl CommitHandler for GuardedCommit {
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
        let inner = self.0.clone();
        let mut owned_manifest = manifest.clone();
        let path = base_path.clone();
        let store = object_store.clone();
        let scope = CURRENT.try_with(Arc::clone).ok();
        let (location, committed) = shield(async move {
            let location = inner
                .commit(
                    &mut owned_manifest,
                    indices,
                    &path,
                    &store,
                    manifest_writer,
                    naming_scheme,
                    transaction,
                )
                .await;
            // Only a definite conflict proves this conditional commit did
            // not happen. A transport/storage error can arrive after the
            // remote service accepted it, even though the Rust future ended.
            if matches!(&location, Err(CommitError::OtherError(_))) {
                if let Some(scope) = scope {
                    scope.mark_uncertain();
                }
            }
            Ok::<_, CommitError>((location, owned_manifest))
        })
        .await?;
        *manifest = committed;
        location
    }

    async fn resolve_latest_location(
        &self,
        base_path: &Path,
        object_store: &ObjectStore,
    ) -> Result<ManifestLocation> {
        self.0
            .resolve_latest_location(base_path, object_store)
            .await
    }
    async fn resolve_version_location(
        &self,
        base_path: &Path,
        version: u64,
        object_store: &dyn object_store::ObjectStore,
    ) -> Result<ManifestLocation> {
        self.0
            .resolve_version_location(base_path, version, object_store)
            .await
    }
    async fn version_exists(
        &self,
        base_path: &Path,
        version: u64,
        object_store: &dyn object_store::ObjectStore,
        naming: ManifestNamingScheme,
    ) -> Result<bool> {
        self.0
            .version_exists(base_path, version, object_store, naming)
            .await
    }
    fn list_detached_manifest_locations<'a>(
        &self,
        base_path: &Path,
        object_store: &'a ObjectStore,
    ) -> BoxStream<'a, Result<ManifestLocation>> {
        self.0
            .list_detached_manifest_locations(base_path, object_store)
    }
    fn list_manifest_locations<'a>(
        &self,
        base_path: &Path,
        object_store: &'a ObjectStore,
        sorted: bool,
    ) -> BoxStream<'a, Result<ManifestLocation>> {
        self.0
            .list_manifest_locations(base_path, object_store, sorted)
    }
    fn list_manifest_locations_since<'a>(
        &self,
        base_path: &Path,
        object_store: &'a ObjectStore,
        since: u64,
    ) -> BoxStream<'a, Result<ManifestLocation>> {
        self.0
            .list_manifest_locations_since(base_path, object_store, since)
    }
    async fn delete(&self, base_path: &Path) -> Result<()> {
        self.0.delete(base_path).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicBool, Ordering};

    #[tokio::test]
    async fn failed_or_panicked_leaf_commit_needs_reconciliation() {
        let scope = MergeWriteScope::new();
        let result = scope
            .run(shield(async {
                Err::<(), _>(Error::io("lost storage response"))
            }))
            .await;
        assert!(result.is_err());
        scope.drain().await;
        assert!(
            scope.has_uncertain_commit(),
            "an error is not proof that remote storage did not commit"
        );
        let panicked = MergeWriteScope::new();
        let result = panicked
            .run(shield(async {
                panic!("commit panic");
                #[allow(unreachable_code)]
                Ok::<(), Error>(())
            }))
            .await;
        assert!(result.is_err());
        panicked.drain().await;
        assert!(panicked.has_uncertain_commit());
    }

    #[tokio::test]
    async fn dropping_merge_waits_for_the_surviving_manifest_write() {
        let scope = MergeWriteScope::new();
        let (entered, began) = oneshot::channel();
        let (release, blocked) = oneshot::channel();
        let committed = Arc::new(AtomicBool::new(false));
        let flag = committed.clone();
        let task_scope = scope.clone();
        let merge = tokio::spawn(async move {
            task_scope
                .run(shield(async move {
                    entered.send(()).unwrap();
                    blocked.await.unwrap();
                    flag.store(true, Ordering::SeqCst);
                    Ok::<_, Error>(())
                }))
                .await
        });
        began.await.unwrap();
        merge.abort();
        assert!(merge.await.unwrap_err().is_cancelled());
        let draining_scope = scope.clone();
        let drain = tokio::spawn(async move { draining_scope.drain().await });
        tokio::task::yield_now().await;
        assert!(
            !drain.is_finished(),
            "cannot hand off while storage write survives"
        );
        release.send(()).unwrap();
        drain.await.unwrap();
        assert!(committed.load(Ordering::SeqCst));
        assert!(!scope.has_uncertain_commit());
        assert!(
            scope
                .run(shield(async { Ok::<_, Error>(()) }))
                .await
                .is_err(),
            "cancelled scope cannot issue another commit"
        );
    }
}
