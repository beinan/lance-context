use super::*;
use crate::materializer::{SessionTurnIndex, SmallFileCompaction};
use crate::turn_ingestion::session_turn_key;
use lance::index::DatasetIndexExt;
use std::collections::{BTreeSet, HashMap};

fn inputs() -> Result<Vec<RecordBatch>> {
    bodies()?
        .into_iter()
        .map(|body| {
            let session = body
                .column_by_name("session_id")
                .unwrap()
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap();
            let id = body
                .column_by_name("id")
                .unwrap()
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap();
            let keys = Arc::new(StringArray::from_iter_values(
                (0..body.num_rows())
                    .map(|row| session_turn_key::encode(session.value(row), id.value(row))),
            ));
            let mut fields = body.schema().fields().to_vec();
            fields.push(Arc::new(Field::new(
                session_turn_key::COLUMN,
                DataType::Utf8,
                false,
            )));
            let mut metadata: HashMap<_, _> = body.schema().metadata().clone();
            metadata.insert("trace.session_turn_key".into(), "hex-utf8-v1".into());
            let mut columns = body.columns().to_vec();
            columns.push(keys);
            Ok(RecordBatch::try_new(
                Arc::new(Schema::new_with_metadata(fields, metadata)),
                columns,
            )?)
        })
        .collect()
}

async fn indexed_owner(
    c: &network::Cluster,
    storage: &Storage,
    spec: &TurnIngestionSpec,
    index: bool,
    compact: bool,
) -> Result<(Service, Runtime)> {
    let view = annotation_publication::state(c).await?;
    Runtime::connect_body_shard(
        c.client.clone(),
        annotation_publication::one_shard(),
        compaction::compact_binding(),
        storage.clone(),
        Configuration {
            limits: fixture::limits(),
            upload_budget: MergeMemoryBudget::new(8 << 20),
            apply_budget: MergeMemoryBudget::new(8 << 20),
            publication_budget: MergeMemoryBudget::new(768 << 20),
            publication: Options {
                max_source_bytes: 1 << 20,
                min_source_bytes: 1 << 20,
                max_records: 2,
                max_batch_wait: Duration::from_millis(20),
                session_turn_index: index.then(SessionTurnIndex::hex_utf8_v1),
                compaction: compact.then_some(SmallFileCompaction {
                    target_file_bytes: 256 << 10,
                    min_fragments: 2,
                }),
                ..Default::default()
            },
        },
        Acquisition {
            owner: Uuid::new_v4(),
            request: Uuid::new_v4(),
            expected_epoch: view.authority.epoch,
        },
        Shard { bucket: 0 },
        spec,
    )
    .await
}

async fn provision(
    c: &network::Cluster,
    storage: &Storage,
    spec: &TurnIngestionSpec,
) -> Result<()> {
    crate::bootstrap::provision_body_shard(
        c.client.clone(),
        annotation_publication::one_shard(),
        compaction::compact_binding(),
        storage.clone(),
        Identity {
            owner: Uuid::new_v4(),
            acquisition: Uuid::new_v4(),
            manifest: Uuid::new_v4(),
        },
        &fixture::limits(),
        Shard { bucket: 0 },
        spec,
    )
    .await?;
    Ok(())
}

fn encoded(spec: &TurnIngestionSpec, batch: &RecordBatch) -> Result<Bytes> {
    fixture::encoded(single_body::batch(
        spec,
        &fixture::limits(),
        Shard { bucket: 0 },
        batch,
    )?)
}

async fn covered(c: &network::Cluster, storage: &Storage, compact: bool) -> Result<Arc<Dataset>> {
    tokio::time::timeout(DEADLINE, async {
        loop {
            let current = annotation_publication::read(c, storage, None).await?;
            let coverage: BTreeSet<_> = current
                .load_indices()
                .await?
                .iter()
                .filter(|i| i.name == session_turn_key::INDEX)
                .flat_map(|i| i.fragment_bitmap.as_ref().unwrap().iter())
                .collect();
            if !coverage.is_empty()
                && current
                    .fragments()
                    .iter()
                    .all(|f| coverage.contains(&(f.id as u32)))
                && (!compact || current.fragments().len() == 1)
                && annotation_publication::state(c)
                    .await?
                    .progress
                    .pending
                    .is_none()
            {
                return Ok(current);
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await?
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn composite_zonemap_rf3_main_catchup_first_create_compaction_restart() -> Result<()> {
    let input = inputs()?;
    let spec = TurnIngestionSpec::new(
        input[0].schema(),
        vec!["session_id".into(), "id".into()],
        "session_id",
    )?;
    let fingerprint = spec.fingerprint()?;
    let root = tempfile::tempdir()?;
    let c = network::Cluster::open_signed(root.path(), annotation_publication::one_shard()).await?;
    let storage = fixture::storage(root.path(), "body.lance").await?;
    provision(&c, &storage, &spec).await?;
    let (service, runtime) = indexed_owner(&c, &storage, &spec, false, false).await?;
    let (stop, task) = run_owner(runtime);
    let lane = producer(&service, 1).await?;
    let mut last = None;
    for (sequence, i) in [0, 2, 3].into_iter().enumerate() {
        let append = request(
            &service,
            &lane,
            sequence as u64 + 1,
            encoded(&spec, &input[i])?,
        )
        .await?;
        let result = service.submit(&append, DEADLINE).await?;
        drained(&c, &service, result.index).await?;
        last = Some((append, result));
    }
    let old = annotation_publication::read(&c, &storage, None).await?;
    assert_eq!(old.fragments().len(), 3);
    let row_ids = compaction::row_ids(&old).await?;
    stop.send(()).unwrap();
    task.await??;
    let (service, runtime) = indexed_owner(&c, &storage, &spec, true, false).await?;
    let (stop, task) = run_owner(runtime);
    let indexed = covered(&c, &storage, false).await?;
    assert_eq!(indexed.fragments(), old.fragments());
    let duplicate = producer(&service, 2).await?;
    let append = request(&service, &duplicate, 1, encoded(&spec, &input[1])?).await?;
    let result = service.submit(&append, DEADLINE).await?;
    drained(&c, &service, result.index).await?;
    let key = spec.row_key(&input[0], 0, 4096)?;
    assert_eq!(
        service
            .get_full_body(&key, DEADLINE)
            .await?
            .unwrap()
            .batch(),
        &input[0].slice(0, 1)
    );
    stop.send(()).unwrap();
    task.await??;
    // Rewrite while new indexing is disabled: native ZoneMap coverage must
    // withdraw, and current point reads must remain correct before rebuilding.
    let (service, runtime) = indexed_owner(&c, &storage, &spec, false, true).await?;
    let (stop, task) = run_owner(runtime);
    let withdrawn = tokio::time::timeout(DEADLINE, async {
        loop {
            let current = annotation_publication::read(&c, &storage, None).await?;
            if current.fragments().len() == 1 {
                return Ok::<_, anyhow::Error>(current);
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await??;
    assert!(withdrawn.load_indices().await?.iter().all(|i| i
        .fragment_bitmap
        .as_ref()
        .unwrap()
        .is_empty()));
    assert_eq!(
        service
            .get_full_body(&key, DEADLINE)
            .await?
            .unwrap()
            .batch(),
        &input[0].slice(0, 1)
    );
    stop.send(()).unwrap();
    task.await??;
    let (service, runtime) = indexed_owner(&c, &storage, &spec, true, true).await?;
    let (stop, task) = run_owner(runtime);
    let compacted = covered(&c, &storage, true).await?;
    assert_eq!(compacted.count_rows(None).await?, 6);
    assert_eq!(compaction::row_ids(&compacted).await?, row_ids);
    assert_eq!(compaction::row_ids(&old).await?, row_ids);
    let (last, completed) = last.unwrap();
    assert_eq!(service.submit(&last, DEADLINE).await?, completed);
    assert_eq!(service.submit(&append, DEADLINE).await?, result);
    assert_eq!(
        service
            .get_full_body(&key, DEADLINE)
            .await?
            .unwrap()
            .batch(),
        &input[0].slice(0, 1)
    );
    assert_eq!(spec.fingerprint()?, fingerprint);
    stop.send(()).unwrap();
    task.await??;
    c.shutdown().await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn composite_zonemap_rf3_unknown_prepare_recovers_with_option_disabled() -> Result<()> {
    use crate::tests::annotation_runtime::lost_prepare::proxy::Proxy;
    use lance_table::transaction::Operation;
    let input = inputs()?;
    let spec = TurnIngestionSpec::new(
        input[0].schema(),
        vec!["session_id".into(), "id".into()],
        "session_id",
    )?;
    let root = tempfile::tempdir()?;
    let mut c =
        network::Cluster::open_signed(root.path(), annotation_publication::one_shard()).await?;
    let storage = fixture::storage(root.path(), "body.lance").await?;
    provision(&c, &storage, &spec).await?;
    let (service, runtime) = indexed_owner(&c, &storage, &spec, false, false).await?;
    let (stop, task) = run_owner(runtime);
    let lane = producer(&service, 1).await?;
    let previous = request(&service, &lane, 1, encoded(&spec, &input[0])?).await?;
    let previous_result = service.submit(&previous, DEADLINE).await?;
    drained(&c, &service, previous_result.index).await?;
    stop.send(()).unwrap();
    task.await??;
    let base = annotation_publication::read(&c, &storage, None).await?;
    let proxy = Proxy::open(&c, root.path()).await?;
    let direct = std::mem::replace(&mut c.client, proxy.client.clone());
    let (service, runtime) = indexed_owner(&c, &storage, &spec, true, false).await?;
    let (_stop, task) = run_owner(runtime);
    tokio::time::timeout(DEADLINE, proxy.committed.notified()).await?;
    let tail = producer(&service, 2).await?;
    let append = request(&service, &tail, 1, encoded(&spec, &input[3])?).await?;
    let result = service.submit(&append, DEADLINE).await?;
    let key = spec.row_key(&input[3], 0, 4096)?;
    assert_eq!(
        service
            .get_full_body(&key, DEADLINE)
            .await?
            .unwrap()
            .batch(),
        &input[3].slice(0, 1)
    );
    let state = annotation_publication::state(&c).await?;
    let pending = state.progress.pending.as_ref().unwrap();
    let prepared = state.progress.prepared.clone().unwrap();
    let expected = crate::materializer::stage::annotation_compaction_transaction(
        &base,
        pending,
        &prepared.descriptors,
    )
    .await?;
    assert!(
        matches!(&expected.operation, Operation::CreateIndex { new_indices, .. } if new_indices[0].name == session_turn_key::INDEX)
    );
    let target = pending.plan.target_version()?;
    proxy.release.notify_one();
    let error = tokio::time::timeout(Duration::from_secs(45), task)
        .await??
        .unwrap_err();
    proxy.restore_observations();
    assert!(format!("{error:#}").contains("outcome unknown"));
    drop(service);
    c.client = direct;
    drop(proxy);
    c.shutdown().await?;
    let c = network::Cluster::open_signed(root.path(), annotation_publication::one_shard()).await?;
    assert_eq!(
        annotation_publication::state(&c).await?.progress.prepared,
        Some(prepared)
    );
    let (service, runtime) = indexed_owner(&c, &storage, &spec, false, false).await?;
    let (stop, task) = run_owner(runtime);
    drained(&c, &service, result.index).await?;
    let original = annotation_publication::read(&c, &storage, Some(target)).await?;
    assert_eq!(original.read_transaction().await?, Some(expected));
    assert_eq!(service.submit(&previous, DEADLINE).await?, previous_result);
    assert_eq!(service.submit(&append, DEADLINE).await?, result);
    assert_eq!(
        service
            .get_full_body(&key, DEADLINE)
            .await?
            .unwrap()
            .batch(),
        &input[3].slice(0, 1)
    );
    stop.send(()).unwrap();
    task.await??;
    c.shutdown().await
}
