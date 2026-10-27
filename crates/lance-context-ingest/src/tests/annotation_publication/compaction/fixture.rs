//! Small indexed fixture; independent of oversized annotation preparations.
use super::*;

pub(super) fn fixture_binding() -> TableBinding {
    TableBinding {
        max_rows: 2048,
        ..binding()
    }
}

pub(super) async fn owner(
    c: &network::Cluster,
    storage: &Storage,
    limits: &Limits,
) -> Result<(Service, Runtime)> {
    owner_with_workspace(c, storage, limits, 8 << 20).await
}

async fn owner_with_workspace(
    c: &network::Cluster,
    storage: &Storage,
    limits: &Limits,
    publication_bytes: usize,
) -> Result<(Service, Runtime)> {
    Runtime::connect(
        c.client.clone(),
        one_shard(),
        fixture_binding(),
        storage.clone(),
        Configuration {
            limits: limits.clone(),
            upload_budget: MergeMemoryBudget::new(32 << 20),
            apply_budget: MergeMemoryBudget::new(16 << 20),
            publication_budget: MergeMemoryBudget::new(publication_bytes),
            publication: Options {
                max_source_bytes: 1 << 20,
                min_source_bytes: 1 << 20,
                max_records: 2,
                max_batch_wait: Duration::from_millis(20),
                ..Default::default()
            },
        },
        Acquisition {
            owner: Uuid::new_v4(),
            request: Uuid::new_v4(),
            expected_epoch: state(c).await?.authority.epoch,
        },
    )
    .await
}

pub(super) async fn seed_fragments(
    c: &network::Cluster,
    storage: &Storage,
    limits: &Limits,
    fragments: usize,
) -> Result<()> {
    let rows = fragments * 2;
    let ids: Vec<_> = (0..rows).map(|n| format!("t{n:08}")).collect();
    let mut labels = ListBuilder::new(StringBuilder::new());
    for _ in 0..rows {
        labels.append(true);
    }
    let labels = labels.finish();
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Utf8, false),
        Field::new("body", DataType::LargeBinary, false),
        Field::new("labels", labels.data_type().clone(), false),
        Field::new("status", DataType::Utf8, true),
        Field::new("score", DataType::Float32, true),
    ]));
    let body = vec![0x5a; 1024];
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(StringArray::from(ids)),
            Arc::new(LargeBinaryArray::from_iter_values(
                (0..rows).map(|_| body.as_slice()),
            )),
            Arc::new(labels),
            Arc::new(StringArray::from(vec!["pending"; rows])),
            Arc::new(Float32Array::from(vec![None; rows])),
        ],
    )?;
    let mut base = Dataset::write(
        RecordBatchIterator::new(vec![Ok(batch)], schema),
        &storage.uri,
        Some(WriteParams {
            commit_handler: Some(Arc::new(ConditionalPutCommitHandler)),
            enable_stable_row_ids: true,
            max_rows_per_file: 2,
            max_rows_per_group: 2,
            ..Default::default()
        }),
    )
    .await?;
    assert_eq!(base.manifest().fragments.len(), fragments);
    base.create_index_builder(
        &["id"],
        lance_index::IndexType::BTree,
        &lance_index::scalar::ScalarIndexParams::default(),
    )
    .name("annotation_point_id".into())
    .await?;
    seed_base(c, Service::contract(&fixture_binding(), limits)?, &base).await
}

pub(super) fn collector(runtime: Runtime) -> Result<Runtime> {
    runtime.with_exclusive_reclamation(crate::annotations::runtime::reclamation::Options {
        max_bytes: 256 << 10,
        interval: Duration::from_millis(10),
        ..Default::default()
    })
}
