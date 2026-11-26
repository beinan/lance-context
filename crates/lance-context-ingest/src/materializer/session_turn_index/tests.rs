use super::*;
use crate::turn_ingestion::TurnIngestionSpec;
use arrow_array::{ArrayRef, RecordBatch, RecordBatchIterator, StringArray};
use arrow_schema::{DataType, Field, Schema};
use lance::dataset::{CommitBuilder, WriteMode, WriteParams};
use lance_table::transaction::Transaction;
use std::collections::HashMap;

fn batch(rows: &[(&str, &str)]) -> Result<RecordBatch> {
    let schema = Arc::new(Schema::new_with_metadata(
        ["session_id", "id", session_turn_key::COLUMN]
            .map(|name| Arc::new(Field::new(name, DataType::Utf8, false))),
        HashMap::from([("trace.session_turn_key".into(), "hex-utf8-v1".into())]),
    ));
    Ok(RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from_iter_values(rows.iter().map(|r| r.0))) as ArrayRef,
            Arc::new(StringArray::from_iter_values(rows.iter().map(|r| r.1))),
            Arc::new(StringArray::from_iter_values(
                rows.iter().map(|r| session_turn_key::encode(r.0, r.1)),
            )),
        ],
    )?)
}

async fn write(uri: &str, rows: &[(&str, &str)], mode: WriteMode) -> Result<Dataset> {
    let batch = batch(rows)?;
    Ok(Dataset::write(
        RecordBatchIterator::new(vec![Ok(batch.clone())], batch.schema()),
        uri,
        Some(WriteParams {
            mode,
            enable_stable_row_ids: true,
            skip_auto_cleanup: true,
            ..Default::default()
        }),
    )
    .await?)
}

async fn prepare(source: &Dataset, options: &SessionTurnIndex) -> Result<p::Descriptors> {
    let budget = MergeMemoryBudget::new(session_index::WORKSPACE);
    session_index::spawn_action(
        source.clone(),
        options.action(source).await?.context("no index work")?,
        Key::SessionTurn,
        budget.reserve(budget.limit()).await,
        Arc::default(),
    )
    .await?
}

async fn install(source: &Dataset, descriptors: &p::Descriptors) -> Result<Dataset> {
    // Recovery decodes the retained descriptor without consulting today's options.
    let operation = session_index::annotation_operation(source, descriptors).await?;
    Ok(CommitBuilder::new(Arc::new(source.clone()))
        .with_max_retries(0)
        .with_skip_auto_cleanup(true)
        .execute(Transaction::new_from_version(
            source.version_id(),
            operation,
        ))
        .await?)
}

async fn lookup(
    dataset: &Dataset,
    keys: &[(&str, &str)],
    optimized: bool,
) -> Result<crate::turn_ingestion::TurnLookup> {
    let spec = TurnIngestionSpec::new(
        Arc::new(Schema::from(dataset.schema())),
        vec!["session_id".into(), "id".into()],
        "session_id",
    )?;
    let keys = keys
        .iter()
        .map(|r| spec.business_key(&[r.0, r.1], 4096))
        .collect::<Result<Vec<_>>>()?;
    let budget = MergeMemoryBudget::new(8 << 20);
    let prepared = spec
        .prepare_lookup(
            &keys.iter().map(String::as_str).collect::<Vec<_>>(),
            &budget,
        )?
        .unwrap();
    spec.lookup_prepared_with_key(Arc::new(dataset.clone()), prepared, optimized)
        .await
}

#[tokio::test]
async fn composite_zonemap_prunes_cross_product_and_reads_uncovered_append() -> Result<()> {
    let root = tempfile::tempdir()?;
    let uri = root.path().join("body.lance");
    let source = write(
        uri.to_str().unwrap(),
        &[("a", "z"), ("z", "a")],
        WriteMode::Create,
    )
    .await?;
    let raw = lookup(&source, &[("z", "z")], false).await?;
    assert_eq!(raw.get("[\"z\",\"z\"]"), Some(false));
    assert_eq!(raw.observation().rows_scanned, Some(2));
    let options = SessionTurnIndex::hex_utf8_v1();
    let indexed = install(&source, &prepare(&source, &options).await?).await?;
    let result = lookup(&indexed, &[("z", "z")], true).await?;
    assert_eq!(result.get("[\"z\",\"z\"]"), Some(false));
    assert_eq!(result.observation().rows_scanned, Some(0));
    let appended = write(indexed.uri(), &[("z", "z")], WriteMode::Append).await?;
    let result = lookup(&appended, &[("z", "z"), ("zz", "z")], true).await?;
    assert_eq!(result.get("[\"z\",\"z\"]"), Some(true));
    assert_eq!(result.get("[\"zz\",\"z\"]"), Some(false));
    assert_eq!(result.observation().rows_scanned, Some(1));
    let caught_up = install(&appended, &prepare(&appended, &options).await?).await?;
    assert!(options.action(&caught_up).await?.is_none());
    assert_eq!(
        lookup(&caught_up, &[("z", "a"), ("z", "z")], true)
            .await?
            .get("[\"z\",\"z\"]"),
        Some(true)
    );
    Ok(())
}

#[tokio::test]
async fn composite_zonemap_rebases_bounded_builds_and_merges_on_main() -> Result<()> {
    let root = tempfile::tempdir()?;
    let uri = root.path().join("body.lance");
    let source = write(uri.to_str().unwrap(), &[("s", "0")], WriteMode::Create).await?;
    let options = SessionTurnIndex { max_fragments: 1 };
    let prepared = prepare(&source, &options).await?;
    let mut current = source.clone();
    for id in ["1", "2", "3"] {
        current = write(current.uri(), &[("s", id)], WriteMode::Append).await?;
    }
    assert!(session_index::annotation_operation(&current, &prepared)
        .await
        .is_err());
    let rebased = session_index::rebase_unchanged(&source, &current, &prepared, Key::SessionTurn)
        .await?
        .unwrap();
    current = install(&current, &rebased).await?;
    for expected in [2, 3, 4] {
        current = install(&current, &prepare(&current, &options).await?).await?;
        assert_eq!(current.load_indices().await?.len(), expected);
    }
    assert!(matches!(
        options.action(&current).await?,
        Some(Action::Merge(_))
    ));
    current = install(&current, &prepare(&current, &options).await?).await?;
    assert_eq!(current.load_indices().await?.len(), 1);
    assert!(options.action(&current).await?.is_none());
    let result = lookup(&current, &[("s", "0"), ("s", "3"), ("s", "4")], true).await?;
    assert_eq!(result.get("[\"s\",\"0\"]"), Some(true));
    assert_eq!(result.get("[\"s\",\"3\"]"), Some(true));
    assert_eq!(result.get("[\"s\",\"4\"]"), Some(false));
    Ok(())
}

#[test]
fn composite_codec_checks_original_identity_and_does_not_change_fingerprint() -> Result<()> {
    let input = batch(&[("会/话", "'\\x"), ("ab", "c"), ("a", "bc")])?;
    session_turn_key::validate_batch(&input)?;
    assert_eq!(session_turn_key::encode("ab", "c"), "v1:6162/63");
    let spec = TurnIngestionSpec::new(
        input.schema(),
        vec!["session_id".into(), "id".into()],
        "session_id",
    )?;
    let before = spec.fingerprint()?;
    session_turn_key::validate(&spec)?;
    let roundtrip: TurnIngestionSpec = serde_json::from_slice(&serde_json::to_vec(&spec)?)?;
    assert_eq!(before, roundtrip.fingerprint()?);
    let mut columns = input.columns().to_vec();
    columns[2] = Arc::new(StringArray::from(vec!["v1:bad/wrong"; 3]));
    assert!(
        session_turn_key::validate_batch(&RecordBatch::try_new(input.schema(), columns)?).is_err()
    );
    assert!(session_turn_key::validate(&TurnIngestionSpec::new(
        input.schema(),
        vec!["id".into()],
        "id"
    )?)
    .is_err());
    Ok(())
}

#[tokio::test]
async fn composite_zonemap_invalid_old_key_cannot_hide_original_identity() -> Result<()> {
    let root = tempfile::tempdir()?;
    let uri = root.path().join("body.lance");
    let original = batch(&[("s", "id")])?;
    let mut columns = original.columns().to_vec();
    columns[2] = Arc::new(StringArray::from(vec!["v1:bad/wrong"]));
    let malformed = RecordBatch::try_new(original.schema(), columns)?;
    let source = Dataset::write(
        RecordBatchIterator::new(vec![Ok(malformed.clone())], malformed.schema()),
        uri.to_str().unwrap(),
        Some(WriteParams {
            enable_stable_row_ids: true,
            ..Default::default()
        }),
    )
    .await?;
    // No validated main coverage: the query must use original columns.
    assert_eq!(
        lookup(&source, &[("s", "id")], true)
            .await?
            .get("[\"s\",\"id\"]"),
        Some(true)
    );
    let error = prepare(&source, &SessionTurnIndex::hex_utf8_v1())
        .await
        .unwrap_err();
    assert!(format!("{error:#}").contains("differs from its original business key"));
    assert!(source.load_indices().await?.is_empty());
    Ok(())
}
