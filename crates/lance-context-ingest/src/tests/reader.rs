use super::*;
use crate::{materializer::Storage, reader};
use futures::TryStreamExt;

#[cfg(feature = "service")]
mod perf;

async fn storage(path: &std::path::Path) -> Result<Storage> {
    Storage::resolve(
        path.to_str().unwrap().into(),
        Default::default(),
        Arc::default(),
    )
    .await
}

async fn append(
    ingress: &Ingress,
    producer: &Producer,
    sequence: u64,
    batch: RecordBatch,
) -> Result<AcceptedAppend> {
    let body = ipc_file(&[batch])?;
    let prepared = ingress
        .prepare(
            ingress.reserve_upload(body.len()).await?,
            body,
            producer,
            sequence,
        )
        .await?;
    ingress.append(&prepared).await
}

async fn producer(ingress: &Ingress, bucket: u32) -> Result<Producer> {
    ingress
        .open_producer(ProducerOpen {
            bucket,
            slot: bucket,
            expected_generation: 0,
            operation: Uuid::new_v4(),
        })
        .await
}

async fn verify(read: reader::SessionRead, expected: &[RecordBatch]) -> Result<()> {
    verify_batches(read.batches.try_collect().await?, expected)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn partial_index_reads_full_rows_from_index_tail_and_wal_during_publication() -> Result<()> {
    use crate::materializer::{Acquisition, Options, Publisher, SessionIndex};
    use lance::index::DatasetIndexExt;

    let root = tempfile::tempdir()?;
    let path = root.path().join("partial-index.lance");
    let cluster = network::Cluster::open_signed(root.path(), layout()).await?;
    bootstrap(&cluster, &path, &binding()).await?;
    let storage = storage(&path).await?;
    let ingress = connect(&cluster, binding())
        .await?
        .with_session_scan_batch_rows(4)?
        .with_session_scan_fragment_readahead(4)?;
    let selected = (0..1000)
        .map(|i| format!("quoted'session-{i}"))
        .find(|s| ingress.bucket_for_session(s).unwrap() == 0)
        .unwrap();
    let foreign = session(0);
    let producer = producer(&ingress, 0).await?;
    let budget = MergeMemoryBudget::new(768 << 20);
    let mut publisher = Publisher::acquire(
        ingress.clone(),
        storage.clone(),
        Options {
            session_index: Some(SessionIndex {
                min_fragments: 2,
                max_fragments: 2,
                max_segments: 2,
            }),
            ..Options::default()
        },
        budget.clone(),
        Acquisition {
            owner: Uuid::new_v4(),
            request: Uuid::new_v4(),
            expected_epoch: 1,
        },
    )
    .await?;
    let mut expected = Vec::new();
    for sequence in 1..=4 {
        let ids = [
            format!("selected-{sequence}"),
            format!("foreign-{sequence}"),
        ];
        let mixed = batch(&[&ids[0], &ids[1]], &[&selected, &foreign])?;
        let mut columns = mixed.columns().to_vec();
        // Distinct binary values catch payload substitution as well as ID errors.
        columns[mixed.schema().index_of("binary_payload")?] =
            Arc::new(LargeBinaryArray::from_iter_values([
                vec![sequence as u8; 64 << 10],
                vec![sequence as u8 + 10; 64 << 10],
            ]));
        let mixed = RecordBatch::try_new(mixed.schema(), columns)?;
        expected.push(mixed.slice(0, 1));
        append(&ingress, &producer, sequence, mixed).await?;
        if sequence <= 3 {
            assert!(!publisher.run_once().await?.unwrap().sources.is_empty());
        }
        if sequence == 2 {
            // First check the unindexed path, then complete index coverage.
            verify(
                reader::read_session(&ingress, &storage, &selected).await?,
                &expected,
            )
            .await?;
            assert!(publisher.run_once().await?.unwrap().sources.is_empty());
            let dataset = publisher.read_dataset(None).await?;
            assert_eq!(dataset.load_indices().await?.len(), 1);
            assert!(crate::materializer::session_scan_fragments(&dataset)
                .await?
                .is_none());
        }
        verify(
            reader::read_session(&ingress, &storage, &selected).await?,
            &expected,
        )
        .await?;
    }
    let dataset = publisher.read_dataset(None).await?;
    let [covered, tail] = crate::materializer::session_scan_fragments(&dataset)
        .await?
        .context("expected partial index coverage")?;
    assert_eq!(covered.len(), 2);
    assert_eq!(tail.len(), 1);
    // The fourth batch is still WAL-only. Commit it while preserving the source,
    // then retire it: each full row must appear exactly once in all three states.
    assert!(publisher.prepare_next().await?.is_some());
    assert!(publisher.commit_pending().await?.is_some());
    verify(
        reader::read_session(&ingress, &storage, &selected).await?,
        &expected,
    )
    .await?;
    assert!(publisher.finish_pending().await?.is_some());
    verify(
        reader::read_session(&ingress, &storage, &selected).await?,
        &expected,
    )
    .await?;
    verify(
        reader::read_session(&ingress, &storage, "absent'session").await?,
        &[],
    )
    .await?;
    assert_eq!(ingress.budget.reserved(), 0);
    assert_eq!(budget.reserved(), 0);
    drop((publisher, ingress));
    cluster.shutdown().await
}

fn verify_batches(batches: Vec<RecordBatch>, expected: &[RecordBatch]) -> Result<()> {
    fn rows(batches: &[RecordBatch]) -> BTreeMap<String, RecordBatch> {
        let mut rows = BTreeMap::new();
        for b in batches {
            let ids = b
                .column_by_name("id")
                .unwrap()
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap();
            for i in 0..b.num_rows() {
                assert!(
                    rows.insert(ids.value(i).to_owned(), b.slice(i, 1))
                        .is_none(),
                    "duplicate result"
                );
            }
        }
        rows
    }
    let actual = rows(&batches);
    let expected = rows(expected);
    assert_eq!(
        actual.keys().collect::<Vec<_>>(),
        expected.keys().collect::<Vec<_>>()
    );
    for (id, row) in actual {
        assert_eq!(row.schema(), expected[&id].schema());
        for (left, right) in row.columns().iter().zip(expected[&id].columns()) {
            assert_eq!(left, right, "column mismatch for {id}");
        }
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn ack_visible_before_publication_after_restart_and_during_handoff() -> Result<()> {
    let root = tempfile::tempdir()?;
    let path = root.path().join("rollouts.lance");
    let c = network::Cluster::open_signed(root.path(), layout()).await?;
    bootstrap(&c, &path, &binding()).await?;
    let storage = storage(&path).await?;
    let ingress = connect(&c, binding())
        .await?
        .with_session_read_concurrency(4)?;
    let s = (0..1000)
        .map(|i| format!("quoted'session-{i}"))
        .find(|s| ingress.bucket_for_session(s).unwrap() == 0)
        .unwrap();
    verify(reader::read_session(&ingress, &storage, &s).await?, &[]).await?;
    let producer = producer(&ingress, 0).await?;
    let other = session(0);
    let mixed = batch(&["one", "other", "two"], &[&s, &other, &s])?;
    let expected = vec![mixed.slice(0, 1), mixed.slice(2, 1)];
    append(&ingress, &producer, 1, mixed).await?;
    let other_bucket = self::producer(&ingress, 2).await?;
    append(
        &ingress,
        &other_bucket,
        1,
        batch(&["bucket-two"], &[&session(2)])?,
    )
    .await?;
    // No publisher exists: the ACK alone must make the complete row queryable.
    let read = reader::read_session(&ingress, &storage, &s).await?;
    verify(read, &expected).await?;
    assert_eq!(ingress.budget.reserved(), 0);
    drop(ingress);
    c.shutdown().await?;

    let c = network::Cluster::open_signed(root.path(), layout()).await?;
    let ingress = connect(&c, binding())
        .await?
        .with_session_read_concurrency(4)?;
    verify(
        reader::read_session(&ingress, &storage, &s).await?,
        &expected,
    )
    .await?;
    let mut publisher =
        materializer::publisher(&ingress, &storage, &MergeMemoryBudget::new(32 << 20), 1).await?;
    assert!(publisher.prepare_next().await?.is_some());
    verify(
        reader::read_session(&ingress, &storage, &s).await?,
        &expected,
    )
    .await?;
    assert!(publisher.commit_pending().await?.is_some());
    // The same rows now exist in both Lance and WAL; return each ID once.
    let read = reader::read_session(&ingress, &storage, &s).await?;
    verify(read, &expected).await?;
    assert!(publisher.finish_pending().await?.is_some());
    verify(
        reader::read_session(&ingress, &storage, &s).await?,
        &expected,
    )
    .await?;
    let mut expected = expected;
    for sequence in 2..=4 {
        let added = batch(&[&format!("published-{sequence}")], &[&s])?;
        append(&ingress, &producer, sequence, added.clone()).await?;
        assert!(publisher.prepare_next().await?.is_some());
        assert!(publisher.commit_pending().await?.is_some());
        assert!(publisher.finish_pending().await?.is_some());
        expected.push(added);
    }
    assert!(publisher.read_dataset(None).await?.get_fragments().len() >= 4);
    let added = batch(&["new-tail"], &[&s])?;
    append(&ingress, &producer, 5, added.clone()).await?;
    expected.push(added);
    // Parallel reads of several real fragments preserve the union with the tail.
    for rows in [1, 4] {
        for fragments in [1, 4] {
            let batched = ingress
                .clone()
                .with_session_scan_batch_rows(rows)?
                .with_session_scan_fragment_readahead(fragments)?;
            verify(
                reader::read_session(&batched, &storage, &s).await?,
                &expected,
            )
            .await?;
        }
    }
    assert_eq!(ingress.budget.reserved(), 0);
    drop((publisher, ingress));
    c.shutdown().await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn publication_between_read_phases_neither_loses_nor_duplicates_rows() -> Result<()> {
    let root = tempfile::tempdir()?;
    let path = root.path().join("rollouts.lance");
    let c = network::Cluster::open_signed(root.path(), layout()).await?;
    bootstrap(&c, &path, &binding()).await?;
    let storage = storage(&path).await?;
    let ingress = connect(&c, binding())
        .await?
        .with_session_read_concurrency(4)?;
    let s = session(0);
    let producer = producer(&ingress, 0).await?;
    let mut publisher =
        materializer::publisher(&ingress, &storage, &MergeMemoryBudget::new(32 << 20), 1).await?;
    let first = batch(&["retired-before-fetch"], &[&s])?;
    let accepted = append(&ingress, &producer, 1, first.clone()).await?;
    let positions = reader::positions(&ingress, &s).await?;
    publisher.run_once().await?.context("publication absent")?;
    // Ensure every payload endpoint has retired it before continuing the read.
    for node in 1..=3 {
        tokio::time::timeout(Duration::from_secs(10), async {
            loop {
                let r = c
                    .client
                    .request(
                        node,
                        Request::Fetch {
                            group: accepted.position().group,
                            index: accepted.position().index,
                        },
                    )
                    .await?;
                if matches!(r.reply, Reply::Record(None)) {
                    return Ok::<_, anyhow::Error>(());
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await??;
    }
    verify(
        reader::from_positions(&ingress, &storage, &s, positions),
        std::slice::from_ref(&first),
    )
    .await?;
    let second = batch(&["published-after-capture"], &[&s])?;
    append(&ingress, &producer, 2, second.clone()).await?;
    let mut read = reader::read_session(&ingress, &storage, &s).await?;
    let first_result = read
        .batches
        .try_next()
        .await?
        .context("missing WAL result")?;
    publisher.run_once().await?.context("publication absent")?;
    let mut actual = vec![first_result];
    actual.extend(read.batches.try_collect::<Vec<_>>().await?);
    verify_batches(actual, &[first, second])?;
    assert_eq!(ingress.budget.reserved(), 0);
    drop((publisher, ingress));
    c.shutdown().await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn published_scan_waits_for_shared_budget_and_releases_on_cancel() -> Result<()> {
    let root = tempfile::tempdir()?;
    let path = root.path().join("rollouts.lance");
    let c = network::Cluster::open_signed(root.path(), layout()).await?;
    bootstrap(&c, &path, &binding()).await?;
    let storage = storage(&path).await?;
    let ingress = connect(&c, binding()).await?;
    let s = session(0);
    let expected = batch(&["published"], &[&s])?;
    append(&ingress, &producer(&ingress, 0).await?, 1, expected.clone()).await?;
    let mut publisher =
        materializer::publisher(&ingress, &storage, &MergeMemoryBudget::new(32 << 20), 1).await?;
    publisher.run_once().await?.context("publication absent")?;

    let mut limited = ingress.clone();
    limited.budget = MergeMemoryBudget::new(workspace(
        &limited.scope.contract,
        limited.scope.contract.max_record_bytes as usize,
    )?);
    // Publication closed the WAL source. The published scan must keep its
    // reservation even while the consumer is suspended after a yielded batch.
    let mut first = reader::read_session(&limited, &storage, &s).await?;
    let row = first.batches.try_next().await?.context("missing row")?;
    verify_batches(vec![row], std::slice::from_ref(&expected))?;
    assert!(limited.budget.reserved() > 0);
    let mut second = reader::read_session(&limited, &storage, &s).await?;
    assert!(
        tokio::time::timeout(Duration::from_millis(50), second.batches.try_next())
            .await
            .is_err()
    );
    drop(first);
    tokio::time::timeout(
        Duration::from_secs(10),
        verify(second, std::slice::from_ref(&expected)),
    )
    .await??;
    assert_eq!(limited.budget.reserved(), 0);
    c.shutdown().await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn read_streams_a_tail_larger_than_its_payload_budget() -> Result<()> {
    let root = tempfile::tempdir()?;
    let path = root.path().join("rollouts.lance");
    let c = network::Cluster::open_signed(root.path(), layout()).await?;
    bootstrap(&c, &path, &binding()).await?;
    let storage = storage(&path).await?;
    let ingress = Ingress::connect(
        c.client.clone(),
        layout(),
        binding(),
        MergeMemoryBudget::new(16 << 20),
    )
    .await?;
    let s = session(0);
    let producer = producer(&ingress, 0).await?;
    let mut expected = Vec::new();
    for seq in 1..=129 {
        let row = batch(&[&format!("row-{seq}")], &[&s])?;
        append(&ingress, &producer, seq, row.clone()).await?;
        expected.push(row);
    }
    verify(
        reader::read_session(&ingress, &storage, &s).await?,
        &expected,
    )
    .await?;
    let mut limited = ingress.clone();
    limited.budget = MergeMemoryBudget::new(256 << 10);
    limited = limited.with_session_read_concurrency(4)?;
    let mut cancelled = reader::read_session(&limited, &storage, &s).await?;
    cancelled
        .batches
        .try_next()
        .await?
        .context("missing first row")?;
    assert!(limited.budget.reserved() > 0);
    drop(cancelled);
    tokio::time::timeout(Duration::from_secs(5), async {
        while limited.budget.reserved() != 0 {
            tokio::task::yield_now().await;
        }
    })
    .await?;
    // Two readers share a budget that admits only one record at a time.
    // Unordered completion must allow each holder to drain before waiting.
    tokio::time::timeout(Duration::from_secs(10), async {
        tokio::try_join!(
            async {
                verify(
                    reader::read_session(&limited, &storage, &s).await?,
                    &expected,
                )
                .await
            },
            async {
                verify(
                    reader::read_session(&limited, &storage, &s).await?,
                    &expected,
                )
                .await
            },
        )
    })
    .await??;
    let mut read = reader::read_session(&limited, &storage, &s).await?;
    let mut actual = Vec::new();
    while let Some(batch) = read.batches.try_next().await? {
        assert!(limited.budget.reserved() <= limited.budget.limit());
        actual.push(batch);
    }
    drop(read);
    verify_batches(actual, &expected)?;
    assert_eq!(limited.budget.reserved(), 0);
    c.shutdown().await
}

#[cfg(feature = "service")]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn stalled_replica_does_not_block_other_prefetched_records() -> Result<()> {
    let root = tempfile::tempdir()?;
    let path = root.path().join("rollouts.lance");
    let c = network::Cluster::open_signed(root.path(), layout()).await?;
    bootstrap(&c, &path, &binding()).await?;
    let storage = storage(&path).await?;
    let ingress = connect(&c, binding()).await?;
    let s = session(0);
    let producer = producer(&ingress, 0).await?;
    let expected = vec![
        batch(&["first"], &[&s])?,
        batch(&["second"], &[&s])?,
        batch(&["third"], &[&s])?,
    ];
    for (i, batch) in expected.iter().enumerate() {
        append(&ingress, &producer, i as u64 + 1, batch.clone()).await?;
    }
    // The first replica accepts TCP but cannot finish TLS. Other replicas stay
    // healthy. Release it only after another record has reached the consumer.
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let address = listener.local_addr()?.to_string();
    let stalled = Arc::new(tokio::sync::Notify::new());
    let release = Arc::new(tokio::sync::Notify::new());
    let (observed, resume) = (stalled.clone(), release.clone());
    let trap = tokio::spawn(async move {
        let (socket, _) = listener.accept().await?;
        observed.notify_one();
        resume.notified().await;
        drop(socket);
        Ok::<_, anyhow::Error>(())
    });
    let mut config = c.application.clone();
    config.tables.push(crate::application::config::Table {
        binding: binding(),
        uri: path.to_str().unwrap().into(),
        storage_options: Default::default(),
        bootstrap: bootstrap::identity(),
    });
    config.endpoints.get_mut(&1).unwrap().data_address = address;
    config.rpc.timeout = Duration::from_secs(60);
    let concurrent = Ingress::connect(
        config.client()?,
        layout(),
        binding(),
        MergeMemoryBudget::new(8 << 20),
    )
    .await?
    .with_session_read_concurrency(3)?;
    let mut read = reader::read_session(&concurrent, &storage, &s).await?;
    let first = tokio::time::timeout(Duration::from_secs(10), async {
        let (first, ()) = tokio::try_join!(
            async {
                read.batches
                    .try_next()
                    .await?
                    .context("missing concurrent result")
            },
            async {
                stalled.notified().await;
                Ok::<_, anyhow::Error>(())
            },
        )?;
        Ok::<_, anyhow::Error>(first)
    })
    .await??;
    let ids = first
        .column_by_name("id")
        .unwrap()
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert_ne!(ids.value(0), "first");
    release.notify_one();
    trap.await??;
    let mut actual = vec![first];
    actual.extend(read.batches.try_collect::<Vec<_>>().await?);
    verify_batches(actual, &expected)?;
    assert_eq!(concurrent.budget.reserved(), 0);
    c.shutdown().await
}

#[cfg(feature = "service")]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn unavailable_unpublished_payload_is_an_error() -> Result<()> {
    let root = tempfile::tempdir()?;
    let path = root.path().join("rollouts.lance");
    let c = network::Cluster::open_signed(root.path(), layout()).await?;
    bootstrap(&c, &path, &binding()).await?;
    let storage = storage(&path).await?;
    let ingress = connect(&c, binding())
        .await?
        .with_session_read_concurrency(4)?;
    let s = session(0);
    append(
        &ingress,
        &producer(&ingress, 0).await?,
        1,
        batch(&["unpublished"], &[&s])?,
    )
    .await?;
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let address = listener.local_addr()?.to_string();
    let reset = tokio::spawn(async move {
        while let Ok((socket, _)) = listener.accept().await {
            drop(socket);
        }
    });
    let mut config = c.application.clone();
    config.session_read_concurrency = 4;
    config.tables.push(crate::application::config::Table {
        binding: binding(),
        uri: path.to_str().unwrap().into(),
        storage_options: Default::default(),
        bootstrap: bootstrap::identity(),
    });
    for endpoint in config.endpoints.values_mut() {
        endpoint.data_address = address.clone();
    }
    let unavailable = Ingress::connect(
        config.client()?,
        layout(),
        binding(),
        MergeMemoryBudget::new(8 << 20),
    )
    .await?;
    let error = reader::read_session(&unavailable, &storage, &s)
        .await?
        .batches
        .try_collect::<Vec<_>>()
        .await
        .err()
        .context("read silently lost WAL rows")?;
    assert!(
        error
            .to_string()
            .contains("unpublished WAL payload unavailable"),
        "{error:#}"
    );
    assert_eq!(unavailable.budget.reserved(), 0);
    reset.abort();
    let _ = reset.await;
    c.shutdown().await
}

#[cfg(feature = "service")]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn http_ack_is_queryable_without_a_worker_then_merges_base_and_tail() -> Result<()> {
    use crate::{
        application::{self, config, Registry},
        http,
    };
    let root = tempfile::tempdir()?;
    let path = root.path().join("rollouts.lance");
    let c = network::Cluster::open_signed(root.path(), layout()).await?;
    bootstrap(&c, &path, &binding()).await?;
    // Older deployment JSON retains serial scanning; explicit settings reach HTTP reads.
    let mut legacy = serde_json::to_value(&c.application)?;
    legacy
        .as_object_mut()
        .unwrap()
        .remove("session_scan_fragment_readahead");
    let mut config: application::Config = serde_json::from_value(legacy)?;
    assert_eq!(config.session_scan_fragment_readahead, 1);
    config.tables.push(config::Table {
        binding: binding(),
        uri: path.to_str().unwrap().into(),
        storage_options: Default::default(),
        bootstrap: bootstrap::identity(),
    });
    config.validate()?;
    config.session_scan_fragment_readahead = 0;
    assert!(config
        .validate()
        .unwrap_err()
        .to_string()
        .contains("session scan fragment readahead"));
    config.session_scan_fragment_readahead = 4;
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let address = listener.local_addr()?;
    let token = "session-read-test-workload-token-0001";
    let bearer = root.path().join("token");
    std::fs::write(&bearer, token)?;
    let receipts = root.path().join("receipts.json");
    std::fs::write(
        &receipts,
        serde_json::to_vec(&serde_json::json!({"active":1,"keys":{"1":vec![0x42u8;32]}}))?,
    )?;
    let http = config::Http {
        listen: address,
        bearer_token_file: bearer,
        receipt_key_file: receipts,
        options: Default::default(),
    };
    let registry = Registry::connect(&config).await?;
    let (stop, stopped) = tokio::sync::oneshot::channel();
    let server = tokio::spawn(async move {
        application::serve_http(&registry, &http, listener, async {
            let _ = stopped.await;
        })
        .await
    });
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(15))
        .build()?;
    let base = format!("http://{address}/api/v1/ingest/rollouts");
    let s = session(0);
    let url = format!("{base}/sessions/{s}");
    assert_eq!(client.get(&url).send().await?.status(), 401);
    let open = ProducerOpen {
        bucket: 0,
        slot: 0,
        expected_generation: 0,
        operation: Uuid::new_v4(),
    };
    let producer: http::Opened = client
        .post(format!("{base}/producer"))
        .bearer_auth(token)
        .json(&open)
        .send()
        .await?
        .error_for_status()?
        .json()
        .await?;
    let first = batch(&["ack-visible"], &[&s])?;
    let body = ipc_file(std::slice::from_ref(&first))?;
    let response = client
        .post(format!("{base}/append"))
        .bearer_auth(token)
        .header(reqwest::header::CONTENT_TYPE, http::ARROW_FILE)
        .header(http::PRODUCER, &producer.producer)
        .header(http::SEQUENCE, "1")
        .body(body)
        .send()
        .await?;
    assert_eq!(response.status(), 202);
    let accepted: http::Accepted = response.json().await?;
    let response = client
        .get(&url)
        .bearer_auth(token)
        .send()
        .await?
        .error_for_status()?;
    assert_eq!(
        response.headers()["content-type"],
        "application/vnd.apache.arrow.stream"
    );
    let batches = arrow_ipc::reader::StreamReader::try_new(
        std::io::Cursor::new(response.bytes().await?),
        None,
    )?
    .collect::<std::result::Result<Vec<_>, _>>()?;
    verify_batches(batches, std::slice::from_ref(&first))?;
    let storage = storage(&path).await?;
    let ingress = connect(&c, binding())
        .await?
        .with_session_read_concurrency(4)?;
    let mut publisher =
        materializer::publisher(&ingress, &storage, &MergeMemoryBudget::new(32 << 20), 1).await?;
    publisher.run_once().await?.context("publication missing")?;
    let second = batch(&["still-in-wal"], &[&s])?;
    let body = ipc_file(std::slice::from_ref(&second))?;
    client
        .post(format!("{base}/append"))
        .bearer_auth(token)
        .header(reqwest::header::CONTENT_TYPE, http::ARROW_FILE)
        .header(http::PRODUCER, &producer.producer)
        .header(http::SEQUENCE, "2")
        .body(body)
        .send()
        .await?
        .error_for_status()?;
    let response = client
        .get(&url)
        .bearer_auth(token)
        .send()
        .await?
        .error_for_status()?;
    let batches = arrow_ipc::reader::StreamReader::try_new(
        std::io::Cursor::new(response.bytes().await?),
        None,
    )?
    .collect::<std::result::Result<Vec<_>, _>>()?;
    verify_batches(batches, &[first, second])?;
    let visibility: http::Visibility = client
        .post(format!("{base}/publication"))
        .bearer_auth(token)
        .json(&http::ReceiptRequest {
            receipt: accepted.receipt,
        })
        .send()
        .await?
        .error_for_status()?
        .json()
        .await?;
    assert_eq!(visibility.minimum_read_version, Some(2));
    let _ = stop.send(());
    tokio::time::timeout(Duration::from_secs(15), server).await???;
    drop((publisher, ingress));
    c.shutdown().await
}
