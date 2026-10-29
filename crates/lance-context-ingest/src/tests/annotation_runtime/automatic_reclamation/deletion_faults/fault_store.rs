//! Test-only interception of actual file-object-store deletions.
use super::*;
use futures::{stream::BoxStream, StreamExt, TryStreamExt};
use lance_io::object_store::{
    providers::{ObjectStoreProvider, ObjectStoreRegistry},
    ObjectStore as LanceStore, ObjectStoreParams,
};
use object_store::{
    path::Path, CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta,
    ObjectStore, ObjectStoreExt, PutMultipartOptions, PutOptions, PutPayload, PutResult,
};
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Mutex,
};
use tokio::sync::oneshot;

#[derive(Debug)]
pub(in crate::tests) enum Action {
    Pause {
        entered: oneshot::Sender<()>,
        resume: oneshot::Receiver<()>,
    },
    FailBefore,
    WrongReply,
    #[cfg(feature = "http")]
    FailWhile(Arc<std::sync::atomic::AtomicBool>),
    LoseReply,
    #[cfg(unix)]
    CrashAfter(std::path::PathBuf),
}

#[derive(Debug, Default)]
pub(in crate::tests) struct Faults {
    actions: Mutex<BTreeMap<Path, Action>>,
    pub calls: Mutex<Vec<Path>>,
    pub collection_starts: Mutex<Vec<tokio::time::Instant>>,
    listing: Mutex<Option<PausedListing>>,
    pub data_listings: Mutex<Vec<Path>>,
    pub manifest_reads: AtomicUsize,
    remote: Mutex<Vec<tokio::task::JoinHandle<()>>>,
}

#[derive(Debug)]
struct PausedListing {
    prefix: Path,
    first: Option<Path>,
    entered: oneshot::Sender<()>,
    resume: oneshot::Receiver<()>,
}

impl Faults {
    pub fn pause_listing(
        &self,
        prefix: &std::path::Path,
        first: Option<&std::path::Path>,
        entered: oneshot::Sender<()>,
        resume: oneshot::Receiver<()>,
    ) -> Result<()> {
        let pause = PausedListing {
            prefix: Path::from_absolute_path(prefix)?,
            first: first.map(Path::from_absolute_path).transpose()?,
            entered,
            resume,
        };
        assert!(self.listing.lock().unwrap().replace(pause).is_none());
        self.data_listings.lock().unwrap().clear();
        Ok(())
    }

    pub fn arm(&self, path: &std::path::Path, action: Action) -> Result<()> {
        let path = Path::from_absolute_path(path)?;
        assert!(self.actions.lock().unwrap().insert(path, action).is_none());
        Ok(())
    }

    pub async fn finish_remote(&self) -> Result<()> {
        let tasks = std::mem::take(&mut *self.remote.lock().unwrap());
        for task in tasks {
            task.await?;
        }
        Ok(())
    }
}
impl Drop for Faults {
    fn drop(&mut self) {
        for task in self.remote.get_mut().unwrap() {
            task.abort();
        }
    }
}

#[derive(Debug)]
struct Provider {
    inner: Arc<dyn ObjectStoreProvider>,
    faults: Arc<Faults>,
}
#[async_trait::async_trait]
impl ObjectStoreProvider for Provider {
    async fn new_store(
        &self,
        uri: url::Url,
        params: &ObjectStoreParams,
    ) -> lance::Result<LanceStore> {
        let mut store = self.inner.new_store(uri, params).await?;
        store.inner = Arc::new(FaultStore {
            inner: store.inner,
            faults: self.faults.clone(),
        });
        Ok(store)
    }
}

pub(in crate::tests) async fn storage(
    path: &std::path::Path,
    faults: Arc<Faults>,
) -> Result<Storage> {
    storage_with_cache(path, faults, 1 << 20).await
}

pub(super) async fn storage_with_cache(
    path: &std::path::Path,
    faults: Arc<Faults>,
    cache_bytes: usize,
) -> Result<Storage> {
    let registry = Arc::new(ObjectStoreRegistry::default());
    registry.insert(
        "file-object-store",
        Arc::new(Provider {
            inner: registry.get_provider("file-object-store").unwrap(),
            faults,
        }),
    );
    Storage::resolve(
        format!("file-object-store://{}", path.display()),
        Default::default(),
        Arc::new(lance::session::Session::new(1 << 20, cache_bytes, registry)),
    )
    .await
}

#[derive(Debug)]
struct FaultStore {
    inner: Arc<dyn ObjectStore>,
    faults: Arc<Faults>,
}
impl std::fmt::Display for FaultStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "annotation fault store: {}", self.inner)
    }
}

fn failure() -> object_store::Error {
    object_store::Error::Generic {
        store: "annotation-delete-test",
        source: std::io::Error::other("injected DELETE response failure").into(),
    }
}

#[async_trait::async_trait]
impl ObjectStore for FaultStore {
    async fn put_opts(
        &self,
        path: &Path,
        payload: PutPayload,
        opts: PutOptions,
    ) -> object_store::Result<PutResult> {
        self.inner.put_opts(path, payload, opts).await
    }
    async fn put_multipart_opts(
        &self,
        path: &Path,
        opts: PutMultipartOptions,
    ) -> object_store::Result<Box<dyn MultipartUpload>> {
        self.inner.put_multipart_opts(path, opts).await
    }
    async fn get_opts(&self, path: &Path, opts: GetOptions) -> object_store::Result<GetResult> {
        if !opts.head && path.as_ref().contains("/_versions/") {
            self.faults.manifest_reads.fetch_add(1, Ordering::Relaxed);
        }
        self.inner.get_opts(path, opts).await
    }
    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
        if prefix.is_some_and(|p| p.as_ref().ends_with("/_refs/tags")) {
            self.faults
                .collection_starts
                .lock()
                .unwrap()
                .push(tokio::time::Instant::now());
        }
        if let Some(prefix) = prefix.filter(|p| p.as_ref().ends_with(".lance")) {
            self.faults
                .data_listings
                .lock()
                .unwrap()
                .push(prefix.clone());
        }
        let pause = self
            .faults
            .listing
            .lock()
            .unwrap()
            .take_if(|pause| Some(&pause.prefix) == prefix);
        let listing = self.inner.list(prefix);
        let Some(pause) = pause else {
            return listing;
        };
        // Snapshot real object metadata before holding one page response. The
        // receiver lives inside the retained stream, just like SDK pagination.
        async_stream::try_stream! {
            let mut objects = listing.try_collect::<Vec<_>>().await?;
            if let Some(first) = pause.first {
                let index = objects.iter().position(|o| o.location == first)
                    .expect("paused listing candidate missing");
                yield objects.remove(index);
            }
            let _ = pause.entered.send(());
            pause.resume.await.map_err(|_| object_store::Error::Generic {
                store: "annotation-list-test",
                source: std::io::Error::other("paused listing receiver dropped").into(),
            })?;
            for object in objects {
                yield object;
            }
        }
        .boxed()
    }
    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> object_store::Result<ListResult> {
        self.inner.list_with_delimiter(prefix).await
    }
    async fn copy_opts(
        &self,
        from: &Path,
        to: &Path,
        opts: CopyOptions,
    ) -> object_store::Result<()> {
        self.inner.copy_opts(from, to, opts).await
    }
    fn delete_stream(
        &self,
        locations: BoxStream<'static, object_store::Result<Path>>,
    ) -> BoxStream<'static, object_store::Result<Path>> {
        let inner = self.inner.clone();
        let faults = self.faults.clone();
        locations
            .then(move |path| {
                let inner = inner.clone();
                let faults = faults.clone();
                async move {
                    let path = path?;
                    faults.calls.lock().unwrap().push(path.clone());
                    let action = faults.actions.lock().unwrap().remove(&path);
                    let (lose, crash): (bool, Option<std::path::PathBuf>) = match action {
                        Some(Action::Pause { entered, resume }) => {
                            // Once sent to remote storage, a DELETE can outlive the
                            // client's future. Keep the real operation in the test
                            // server's owned task set, including across owner exit.
                            let (reply, response) = oneshot::channel();
                            let deleting = path.clone();
                            let task = tokio::spawn(async move {
                                let _ = entered.send(());
                                let result = async {
                                    resume.await.map_err(|_| failure())?;
                                    inner.delete(&deleting).await
                                }
                                .await;
                                let _ = reply.send(result);
                            });
                            faults.remote.lock().unwrap().push(task);
                            response.await.map_err(|_| failure())??;
                            return Ok(path);
                        }
                        Some(Action::FailBefore) => return Err(failure()),
                        Some(Action::WrongReply) => {
                            inner.delete(&path).await?;
                            return Ok(path.join("unexpected"));
                        }
                        #[cfg(feature = "http")]
                        Some(Action::FailWhile(blocked)) => {
                            if blocked.load(std::sync::atomic::Ordering::SeqCst) {
                                faults
                                    .actions
                                    .lock()
                                    .unwrap()
                                    .insert(path.clone(), Action::FailWhile(blocked));
                                return Err(failure());
                            }
                            (false, None)
                        }
                        Some(Action::LoseReply) => (true, None),
                        #[cfg(unix)]
                        Some(Action::CrashAfter(marker)) => (false, Some(marker)),
                        None => (false, None),
                    };
                    inner.delete(&path).await?;
                    if let Some(marker) = crash {
                        std::fs::write(marker, path.as_ref()).map_err(|_| failure())?;
                        std::future::pending::<()>().await;
                    }
                    if lose {
                        return Err(failure());
                    }
                    Ok(path)
                }
            })
            .boxed()
    }
}
