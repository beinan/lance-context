//! One ordered narrow scan supplies validation and the native ZoneMap trainer.
use super::*;
use arrow_array::RecordBatch;
use arrow_schema::{DataType, Field, Schema};
use datafusion_common::DataFusionError;
use datafusion_physical_plan::stream::RecordBatchStreamAdapter;
use lance_datafusion::exec::{execute_plan, LanceExecutionOptions};
use lance_index::{
    progress::NoopIndexBuildProgress,
    scalar::{
        index_files_to_table,
        lance_format::LanceIndexStore,
        registry::BasicTrainer,
        zonemap::{ZoneMapIndexBuilderParams, ZoneMapIndexPlugin},
    },
};
use lance_table::format::IndexMetadata;

pub(in crate::materializer) async fn build(
    dataset: &Dataset,
    ids: Vec<u32>,
) -> Result<IndexMetadata> {
    ensure!(
        !ids.is_empty() && ids.len() <= 32,
        "ZoneMap build fragment bound exceeded"
    );
    let coverage: BTreeSet<_> = ids.iter().copied().collect();
    ensure!(
        coverage.len() == ids.len(),
        "duplicate ZoneMap source fragment"
    );
    let fragments = ids
        .iter()
        .map(|&id| {
            dataset
                .get_fragment(id as usize)
                .map(|f| f.metadata().clone())
                .context("ZoneMap source fragment missing")
        })
        .collect::<Result<Vec<_>>>()?;
    let rows = fragments
        .iter()
        .try_fold(0usize, |n, f| n.checked_add(f.physical_rows?))
        .context("ZoneMap source row count unknown or overflowing")?;
    ensure!(rows <= MAX_BUILD_ROWS, "ZoneMap build row bound exceeded");
    let mut scan = dataset.scan();
    scan.project(&["session_id", "id", session_turn_key::COLUMN])?
        .with_row_address()
        .with_fragments(fragments)
        .scan_in_order(true)
        .batch_size(2048)
        .batch_size_bytes(1 << 20)
        .io_buffer_size(8 << 20)
        .batch_readahead(1)
        .fragment_readahead(1);
    let stream = execute_plan(
        scan.create_plan().await?,
        LanceExecutionOptions {
            mem_pool_size: Some(32 << 20),
            ..Default::default()
        },
    )?;
    let schema = Arc::new(Schema::new(vec![
        Field::new("value", DataType::Utf8, false),
        Field::new("_rowaddr", DataType::UInt64, false),
    ]));
    let batch_schema = schema.clone();
    let stream = stream.and_then(move |batch| {
        let result = (|| {
            session_turn_key::validate_batch(&batch)?;
            // Share the two Arrow arrays. Neither the derived key nor body is
            // decoded a second time or copied into a new value buffer.
            Ok(RecordBatch::try_new(
                batch_schema.clone(),
                vec![
                    batch
                        .column_by_name(session_turn_key::COLUMN)
                        .context("ZoneMap key missing")?
                        .clone(),
                    batch
                        .column_by_name("_rowaddr")
                        .context("ZoneMap address missing")?
                        .clone(),
                ],
            )?)
        })()
        .map_err(|error: anyhow::Error| DataFusionError::External(error.into()));
        std::future::ready(result)
    });
    let uuid = Uuid::new_v4();
    let store = LanceIndexStore::new(
        dataset.object_store(None).await?,
        dataset.indices_dir().join(uuid.to_string()),
        Arc::new(dataset.session().file_metadata_cache().clone()),
    );
    let trainer = ZoneMapIndexPlugin;
    let request = trainer.new_training_request(
        &serde_json::to_string(&ZoneMapIndexBuilderParams::new(1024))?,
        schema.field(0),
    )?;
    let created = trainer
        .train_index(
            Box::pin(RecordBatchStreamAdapter::new(schema, stream)),
            &store,
            request,
            Some(ids),
            Arc::new(NoopIndexBuildProgress),
        )
        .await?;
    Ok(IndexMetadata {
        uuid,
        fields: vec![
            dataset
                .schema()
                .field(session_turn_key::COLUMN)
                .context("ZoneMap field missing")?
                .id,
        ],
        covering_fields: Vec::new(),
        name: Key::SessionTurn.name().into(),
        dataset_version: dataset.version_id(),
        fragment_bitmap: Some(coverage.into_iter().collect()),
        index_details: Some(Arc::new(created.index_details)),
        index_version: i32::try_from(created.index_version)?,
        created_at: None,
        base_id: None,
        files: Some(index_files_to_table(created.files)),
    })
}
