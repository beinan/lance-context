use crate::store_base::StorageBase;
use arrow_array::{Int32Array, RecordBatch, RecordBatchIterator};
use arrow_schema::{DataType, Field, Schema};
use async_trait::async_trait;
use futures::{stream::BoxStream, StreamExt, TryStreamExt};
use lance::Result;
use lance_io::object_store::{
    providers::local::FileStoreProvider, ObjectStore, ObjectStoreParams, ObjectStoreProvider,
    ObjectStoreRegistry,
};
use object_store::{
    path::Path, signer::Url, CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload,
    ObjectMeta, PutMultipartOptions, PutOptions, PutPayload, PutResult, RenameOptions,
    Result as StoreResult,
};
use std::{
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    time::Duration,
};
use tokio::sync::Notify;

#[derive(Debug, Default)]
struct CreationRace {
    listed: AtomicBool,
    ready: Notify,
    release: Notify,
}

#[derive(Debug)]
struct DelayedProvider(Arc<CreationRace>);

#[async_trait]
impl ObjectStoreProvider for DelayedProvider {
    async fn new_store(&self, url: Url, params: &ObjectStoreParams) -> Result<ObjectStore> {
        let mut store = FileStoreProvider.new_store(url, params).await?;
        store.inner = Arc::new(DelayedListing {
            inner: store.inner,
            race: self.0.clone(),
        });
        Ok(store)
    }

    fn extract_path(&self, url: &Url) -> Result<Path> {
        FileStoreProvider.extract_path(url)
    }
}

#[derive(Debug)]
struct DelayedListing {
    inner: Arc<dyn object_store::ObjectStore>,
    race: Arc<CreationRace>,
}

impl std::fmt::Display for DelayedListing {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "DelayedListing({})", self.inner)
    }
}

#[async_trait]
impl object_store::ObjectStore for DelayedListing {
    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, StoreResult<ObjectMeta>> {
        let listing = self.inner.list(prefix);
        if prefix.and_then(Path::filename) != Some("_versions")
            || self.race.listed.swap(true, Ordering::SeqCst)
        {
            return listing;
        }
        let race = self.race.clone();
        futures::stream::once(async move {
            let entries = listing.try_collect::<Vec<_>>().await?;
            assert!(entries.is_empty(), "the initial dataset must be absent");
            race.ready.notify_one();
            race.release.notified().await;
            Ok::<_, object_store::Error>(futures::stream::iter(entries.into_iter().map(Ok)))
        })
        .try_flatten()
        .boxed()
    }

    async fn put_opts(&self, p: &Path, b: PutPayload, o: PutOptions) -> StoreResult<PutResult> {
        self.inner.put_opts(p, b, o).await
    }
    async fn put_multipart_opts(
        &self,
        p: &Path,
        o: PutMultipartOptions,
    ) -> StoreResult<Box<dyn MultipartUpload>> {
        self.inner.put_multipart_opts(p, o).await
    }
    async fn get_opts(&self, p: &Path, o: GetOptions) -> StoreResult<GetResult> {
        self.inner.get_opts(p, o).await
    }
    fn delete_stream(
        &self,
        p: BoxStream<'static, StoreResult<Path>>,
    ) -> BoxStream<'static, StoreResult<Path>> {
        self.inner.delete_stream(p)
    }
    async fn list_with_delimiter(&self, p: Option<&Path>) -> StoreResult<ListResult> {
        self.inner.list_with_delimiter(p).await
    }
    async fn copy_opts(&self, from: &Path, to: &Path, o: CopyOptions) -> StoreResult<()> {
        self.inner.copy_opts(from, to, o).await
    }
    async fn rename_opts(&self, from: &Path, to: &Path, o: RenameOptions) -> StoreResult<()> {
        self.inner.rename_opts(from, to, o).await
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn stale_create_cannot_erase_the_winners_append() -> Result<()> {
    let root = tempfile::tempdir().unwrap();
    let uri = root
        .path()
        .join("create.lance")
        .to_str()
        .unwrap()
        .to_owned();
    let schema = Arc::new(Schema::new(vec![Field::new(
        "value",
        DataType::Int32,
        false,
    )]));
    let race = Arc::new(CreationRace::default());
    let registry = ObjectStoreRegistry::default();
    registry.insert("file", Arc::new(DelayedProvider(race.clone())));
    let session = Arc::new(lance::session::Session::new(0, 0, Arc::new(registry)));
    let mut creators = tokio::task::JoinSet::new();
    let (delayed_uri, delayed_schema) = (uri.clone(), schema.clone());
    creators.spawn(async move {
        StorageBase::create_with_options(&delayed_uri, delayed_schema, None, Some(session)).await
    });
    // Hold the first creator's actual empty listing result. After this initial
    // existence check, Lance's commit builder reopens the URI. The other writer
    // must create and append before that second check; pausing at manifest
    // authorization is too late to exercise the overwrite race.
    tokio::time::timeout(Duration::from_secs(30), race.ready.notified())
        .await
        .expect("initial existence check did not arrive");
    let mut winner = StorageBase::create_with_options(&uri, schema.clone(), None, None).await?;
    assert_eq!(winner.version().version, 1);
    let batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(Int32Array::from(vec![7]))])?;
    winner
        .append(RecordBatchIterator::new(vec![Ok(batch)], schema), None)
        .await?;
    assert_eq!(winner.version().version, 2);
    race.release.notify_one();
    let loser = tokio::time::timeout(Duration::from_secs(30), creators.join_next())
        .await
        .expect("stale creator did not resolve")
        .unwrap()
        .unwrap();
    let reopened = lance::Dataset::open(&uri).await?;
    assert_eq!(
        reopened.count_rows(None).await?,
        1,
        "stale create erased committed data"
    );
    assert_eq!(reopened.version().version, 2);
    assert!(
        loser.is_err(),
        "a stale create must not overwrite committed data"
    );
    Ok(())
}
