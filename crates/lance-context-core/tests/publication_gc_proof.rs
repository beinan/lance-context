//! Real Lance manifest/transaction cleanup, with full data readback after reopen.
//! This qualifies storage semantics; it does not implement a distributed GC fence.
use std::{
    collections::{BTreeMap, HashSet},
    sync::Arc,
};

use arrow_array::{Int64Array, RecordBatch, RecordBatchIterator, StringArray};
use arrow_schema::{DataType, Field, Schema};
use futures::TryStreamExt;
use lance::{
    dataset::{
        cleanup::{cleanup_old_versions, CleanupPolicy},
        WriteMode, WriteParams,
    },
    Dataset,
};

type TestResult<T = ()> = Result<T, Box<dyn std::error::Error + Send + Sync>>;

fn payload(id: i64) -> String {
    format!("{id}:{}", "opaque-value".repeat(4096))
}

async fn write(uri: &str, id: i64) -> TestResult<Dataset> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("payload", DataType::Utf8, false),
    ]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int64Array::from(vec![id])),
            Arc::new(StringArray::from(vec![payload(id)])),
        ],
    )?;
    let reader = RecordBatchIterator::new(vec![Ok(batch)], schema);
    Ok(Dataset::write(
        reader,
        uri,
        Some(WriteParams {
            mode: if id == 1 {
                WriteMode::Create
            } else {
                WriteMode::Append
            },
            ..Default::default()
        }),
    )
    .await?)
}

async fn verify(uri: &str, count: i64) -> TestResult<Dataset> {
    let dataset = Dataset::open(uri).await?;
    assert_eq!(dataset.version().version, count as u64);
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
        let bodies = batch
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        for row in 0..batch.num_rows() {
            assert!(rows
                .insert(ids.value(row), bodies.value(row).to_owned())
                .is_none());
        }
    }
    assert_eq!(rows, (1..=count).map(|id| (id, payload(id))).collect());
    Ok(dataset)
}

#[tokio::test]
async fn cleanup_can_remove_exact_transaction_proof_while_latest_data_survives() -> TestResult {
    let dir = tempfile::tempdir()?;
    let uri = dir.path().join("table.lance").to_str().unwrap().to_owned();
    write(&uri, 1).await?;
    let second = write(&uri, 2).await?;
    let committed = second
        .read_transaction_by_version(2)
        .await?
        .expect("append transaction");
    assert!(!committed.uuid.is_empty());
    drop(second);
    let latest = write(&uri, 3).await?;
    let removed = cleanup_old_versions(
        &latest,
        CleanupPolicy {
            versions: Some(HashSet::from([2])),
            ..Default::default()
        },
    )
    .await?;
    assert_eq!(removed.old_versions, 1);
    assert!(removed.transaction_files_removed >= 1);
    drop(latest);
    let reopened = verify(&uri, 3).await?;
    let error = reopened
        .read_transaction_by_version(2)
        .await
        .expect_err("collected target still resolvable");
    assert!(
        matches!(
            &error,
            lance::Error::NotFound { .. }
                | lance::Error::DatasetNotFound { .. }
                | lance::Error::VersionNotFound { .. }
        ),
        "unexpected lookup failure: {error}"
    );
    println!("verified: exact target proof can disappear while all latest rows remain; missing target is not proof of an uncommitted append");
    Ok(())
}

#[tokio::test]
async fn numeric_cleanup_floor_preserves_pending_base_target_and_transactions() -> TestResult {
    let dir = tempfile::tempdir()?;
    let uri = dir.path().join("table.lance").to_str().unwrap().to_owned();
    write(&uri, 1).await?;
    let second = write(&uri, 2).await?;
    let base = second
        .read_transaction_by_version(2)
        .await?
        .expect("base transaction")
        .uuid;
    drop(second);
    let third = write(&uri, 3).await?;
    let target = third
        .read_transaction_by_version(3)
        .await?
        .expect("target transaction")
        .uuid;
    drop(third);
    let latest = write(&uri, 4).await?;
    let removed = cleanup_old_versions(
        &latest,
        CleanupPolicy {
            before_version: Some(2),
            ..Default::default()
        },
    )
    .await?;
    assert_eq!(
        removed.old_versions, 1,
        "floor did not collect the older version"
    );
    drop(latest);
    let reopened = verify(&uri, 4).await?;
    assert!(reopened.read_transaction_by_version(1).await.is_err());
    assert_eq!(
        reopened
            .read_transaction_by_version(2)
            .await?
            .expect("base evidence lost")
            .uuid,
        base
    );
    assert_eq!(
        reopened
            .read_transaction_by_version(3)
            .await?
            .expect("target evidence lost")
            .uuid,
        target
    );
    println!("verified: numeric cleanup floor removes older history while retaining base/target transactions and full latest data; distributed floor coordination still required");
    Ok(())
}
