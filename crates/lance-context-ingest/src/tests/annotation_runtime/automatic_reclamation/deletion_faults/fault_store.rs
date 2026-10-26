//! Test-only interception of actual file-object-store deletions.
use super::*;
use futures::{stream::BoxStream, StreamExt};
use lance_io::object_store::{
    providers::{ObjectStoreProvider, ObjectStoreRegistry},
    ObjectStore as LanceStore, ObjectStoreParams,
};
use object_store::{
    path::Path, CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta,
    ObjectStore, ObjectStoreExt, PutMultipartOptions, PutOptions, PutPayload, PutResult,
};
use std::sync::Mutex;
use tokio::sync::oneshot;

#[derive(Debug)]
pub(super) enum Action {
    Pause {
        entered: oneshot::Sender<()>,
        resume: oneshot::Receiver<()>,
    },
}

#[derive(Debug, Default)]
pub(super) struct Faults {
    actions: Mutex<BTreeMap<Path, Action>>,
    pub collection_starts: Mutex<Vec<tokio::time::Instant>>,
    remote: Mutex<Vec<tokio::task::JoinHandle<()>>>,
}
impl Faults {
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

pub(super) async fn storage(path: &std::path::Path, faults: Arc<Faults>) -> Result<Storage> {
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
        Arc::new(lance::session::Session::new(1 << 20, 1 << 20, registry)),
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
        self.inner.list(prefix)
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
                    let action = faults.actions.lock().unwrap().remove(&path);
                    if let Some(Action::Pause { entered, resume }) = action {
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
                    } else {
                        inner.delete(&path).await?;
                    }
                    Ok(path)
                }
            })
            .boxed()
    }
}
