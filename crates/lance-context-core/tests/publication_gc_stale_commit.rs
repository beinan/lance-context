//! Real Lance commit/cleanup interleaving on a local object store.
//! No cloud or distributed-GC claim is made by this experiment.
use std::{collections::BTreeMap, sync::Arc, time::Duration};

use arrow_array::{Int64Array, RecordBatch, RecordBatchIterator, StringArray};
use arrow_schema::{DataType, Field, Schema};
use futures::TryStreamExt;
use lance::{
    dataset::{
        cleanup::{cleanup_old_versions, CleanupPolicy},
        CommitBuilder, InsertBuilder, WriteMode, WriteParams,
    },
    Dataset,
};
use lance_table::{
    format::{IndexMetadata, Manifest, Transaction},
    io::commit::{
        CommitError, CommitHandler, ConditionalPutCommitHandler, ManifestLocation,
        ManifestNamingScheme, ManifestWriter, RenameCommitHandler,
    },
};
use tokio::sync::Notify;

type TestResult<T = ()> = Result<T, Box<dyn std::error::Error + Send + Sync>>;

fn payload(id: i64) -> String {
    format!("{id}:{}", "immutable-value".repeat(4096))
}

fn batch(id: i64) -> TestResult<RecordBatch> {
    Ok(RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("payload", DataType::Utf8, false),
        ])),
        vec![
            Arc::new(Int64Array::from(vec![id])),
            Arc::new(StringArray::from(vec![payload(id)])),
        ],
    )?)
}

async fn write(uri: &str, id: i64, handler: Arc<dyn CommitHandler>) -> TestResult<Dataset> {
    let data = batch(id)?;
    let reader = RecordBatchIterator::new(vec![Ok(data.clone())], data.schema());
    Ok(Dataset::write(
        reader,
        uri,
        Some(WriteParams {
            mode: if id == 1 {
                WriteMode::Create
            } else {
                WriteMode::Append
            },
            commit_handler: Some(handler),
            ..Default::default()
        }),
    )
    .await?)
}

async fn verify(dataset: &Dataset, ids: &[i64]) -> TestResult {
    let batches: Vec<_> = dataset
        .scan()
        .try_into_stream()
        .await?
        .try_collect()
        .await?;
    let mut rows = BTreeMap::new();
    for batch in batches {
        let ids = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let values = batch
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        for row in 0..batch.num_rows() {
            assert!(rows
                .insert(ids.value(row), values.value(row).to_owned())
                .is_none());
        }
    }
    assert_eq!(rows, ids.iter().map(|id| (*id, payload(*id))).collect());
    Ok(())
}

#[derive(Debug)]
struct PausedTarget {
    inner: Arc<dyn CommitHandler>,
    entered: Arc<Notify>,
    release: Arc<Notify>,
}

#[async_trait::async_trait]
#[allow(clippy::too_many_arguments)]
impl CommitHandler for PausedTarget {
    async fn commit(
        &self,
        manifest: &mut Manifest,
        indices: Option<Vec<IndexMetadata>>,
        path: &object_store::path::Path,
        store: &lance_io::object_store::ObjectStore,
        writer: ManifestWriter,
        naming: ManifestNamingScheme,
        transaction: Option<Transaction>,
    ) -> Result<ManifestLocation, CommitError> {
        // Enforce the fixed target, then suspend before the actual storage write.
        // An epoch check in this position has the same delayed-write window.
        if manifest.version != 2 {
            return Err(CommitError::CommitConflict);
        }
        self.entered.notify_one();
        self.release.notified().await;
        self.inner
            .commit(manifest, indices, path, store, writer, naming, transaction)
            .await
    }
}

async fn interleave(handler: Arc<dyn CommitHandler>, collect_target: bool) -> TestResult {
    let dir = tempfile::tempdir()?;
    let uri = dir.path().join("table.lance").to_str().unwrap().to_owned();
    let base = Arc::new(write(&uri, 1, handler.clone()).await?);
    let tx = InsertBuilder::new(base.clone())
        .with_params(&WriteParams {
            mode: WriteMode::Append,
            ..Default::default()
        })
        .execute_uncommitted(vec![batch(99)?])
        .await?;
    assert_eq!(tx.read_version, 1);
    let delayed_uuid = tx.uuid.clone();
    let entered = Arc::new(Notify::new());
    let release = Arc::new(Notify::new());
    let paused = Arc::new(PausedTarget {
        inner: handler.clone(),
        entered: entered.clone(),
        release: release.clone(),
    });
    let worker = tokio::spawn(async move {
        CommitBuilder::new(base)
            .with_commit_handler(paused)
            .with_max_retries(0)
            .with_skip_auto_cleanup(true)
            .execute(tx)
            .await
    });
    tokio::time::timeout(Duration::from_secs(30), entered.notified()).await?;
    let second = write(&uri, 2, handler.clone()).await?;
    let incumbent_uuid = second
        .read_transaction_by_version(2)
        .await?
        .unwrap()
        .uuid
        .clone();
    assert_ne!(incumbent_uuid, delayed_uuid);
    let latest = write(&uri, 3, handler).await?;
    if collect_target {
        let stats = cleanup_old_versions(
            &latest,
            CleanupPolicy {
                versions: Some(std::collections::HashSet::from([2])),
                ..Default::default()
            },
        )
        .await?;
        assert_eq!(stats.old_versions, 1);
        assert!(Dataset::open(&uri)
            .await?
            .read_transaction_by_version(2)
            .await
            .is_err());
    }
    release.notify_one();
    let result = tokio::time::timeout(Duration::from_secs(30), worker).await??;
    let reopened = Dataset::open(&uri).await?;
    assert_eq!(reopened.version().version, 3);
    verify(&reopened, &[1, 2, 3]).await?;
    let actual_uuid = reopened
        .read_transaction_by_version(2)
        .await?
        .unwrap()
        .uuid
        .clone();
    if collect_target {
        let committed = result?;
        assert_eq!(committed.version().version, 2);
        assert_eq!(actual_uuid, delayed_uuid);
        verify(&reopened.checkout_version(2).await?, &[1, 99]).await?;
        println!("verified: collected target recreated with delayed UUID; latest version excludes delayed payload, so target UUID alone is insufficient after unconstrained GC");
    } else {
        assert!(
            result.is_err(),
            "occupied fixed target accepted a foreign transaction"
        );
        assert_eq!(actual_uuid, incumbent_uuid);
        verify(&reopened.checkout_version(2).await?, &[1, 2]).await?;
        println!("verified control: retained target rejects delayed foreign commit");
    }
    Ok(())
}

#[tokio::test]
async fn conditional_put_reuses_a_collected_fixed_target() -> TestResult {
    interleave(Arc::new(ConditionalPutCommitHandler), false).await?;
    interleave(Arc::new(ConditionalPutCommitHandler), true).await
}

#[tokio::test]
async fn atomic_rename_reuses_a_collected_fixed_target() -> TestResult {
    interleave(Arc::new(RenameCommitHandler), false).await?;
    interleave(Arc::new(RenameCommitHandler), true).await
}
