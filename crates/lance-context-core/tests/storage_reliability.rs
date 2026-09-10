use lance_context_api::AddRecordRequest;
use lance_context_core::{
    record_from_add_request, ContextStore, RolloutRecord, RolloutStore, RolloutStoreOptions,
    ROLE_ASSISTANT,
};

fn rec(id: &str) -> RolloutRecord {
    RolloutRecord {
        id: id.to_string(),
        rollout_id: "r".to_string(),
        problem_id: "p".to_string(),
        dataset: Some("d".to_string()),
        sequence_order: 0,
        role: ROLE_ASSISTANT.to_string(),
        created_at: chrono::Utc::now(),
        content: Some("x".to_string()),
        content_type: "text/plain".to_string(),
        model_input_string: None,
        model_output_string: None,
        rationale: None,
        problem_text: None,
        user_metadata: None,
        input_tokens: None,
        output_tokens: None,
        num_input_tokens: None,
        num_output_tokens: None,
        output_logprobs: None,
        input_logprobs: None,
        ref_logprobs: None,
        loss_mask: None,
        advantage: None,
        reward: None,
        raw_reward: None,
        grader_id: None,
        score: None,
        include_in_training: None,
        exclude_reason: None,
        policy_version: None,
        relationships: vec![],
        binary_payload: None,
        payload_size: None,
        payload_checksum: None,
        artifact_type: None,
        metadata: None,
    }
}

#[tokio::test]
async fn review_pinned_add_preserves_external_id_uniqueness() {
    let dir = tempfile::tempdir().unwrap();
    let mut store = ContextStore::open(dir.path().to_str().unwrap())
        .await
        .unwrap();
    store.checkout(store.version()).await.unwrap();
    let request: AddRecordRequest = serde_json::from_value(serde_json::json!({
        "text_payload": "hello", "external_id": "same-key"
    }))
    .unwrap();
    let left = record_from_add_request(&request, "left".into(), "r".into());
    let right = record_from_add_request(&request, "right".into(), "r".into());
    for record in [&left, &right] {
        let err = store.add(std::slice::from_ref(record)).await.unwrap_err();
        assert!(err.to_string().contains("read-only"));
    }
    assert!(store.is_version_pinned());
    store.refresh_latest().await.unwrap();
    assert!(store.list(None, None).await.unwrap().is_empty());
    store.add(&[left]).await.unwrap();
    assert!(store.add(&[right]).await.is_err());
    assert_eq!(store.list(None, None).await.unwrap().len(), 1);
}

#[tokio::test]
async fn pinned_context_rejects_mutations_without_unpinning() {
    let dir = tempfile::tempdir().unwrap();
    let mut store = ContextStore::open(dir.path().to_str().unwrap())
        .await
        .unwrap();
    let request: AddRecordRequest = serde_json::from_value(serde_json::json!({
        "text_payload": "hello", "external_id": "same-key"
    }))
    .unwrap();
    let row = record_from_add_request(&request, "original".into(), "r".into());
    store.add(&[row]).await.unwrap();
    store.cleanup_wal().await.unwrap();
    let version = store.version();
    store.checkout(version).await.unwrap();
    let replacement = record_from_add_request(&request, "replacement".into(), "r".into());
    let patch = lance_context_core::RecordPatch {
        source: Some("updated".into()),
        ..Default::default()
    };
    assert!(store
        .upsert_by_external_id(replacement.clone())
        .await
        .is_err());
    assert!(store
        .upsert_many_by_external_id(vec![replacement])
        .await
        .is_err());
    assert!(store.update_by_id("original", patch).await.is_err());
    let patch = lance_context_core::RecordPatch {
        source: Some("updated".into()),
        ..Default::default()
    };
    assert!(store
        .update_by_external_id("same-key", patch)
        .await
        .is_err());
    assert!(store.delete_by_id("original").await.is_err());
    assert!(store.delete_by_external_id("same-key").await.is_err());
    assert!(store.migrate_relationships_column().await.is_err());
    assert!(store.compact(None).await.is_err());
    assert!(store.create_id_index().await.is_err());
    let payload = dir.path().join("payload.bin");
    assert!(store
        .put_payload(payload.to_str().unwrap(), b"payload")
        .await
        .is_err());
    assert!(!payload.exists());
    assert!(store.is_version_pinned());
    assert_eq!(store.version(), version);
    store.refresh_latest().await.unwrap();
    assert_eq!(store.version(), version);
    assert_eq!(store.list(None, None).await.unwrap()[0].id, "original");
    assert!(store.delete_by_id("original").await.unwrap());
}

#[tokio::test]
async fn pinned_rollout_rejects_writes_until_refresh() {
    let dir = tempfile::tempdir().unwrap();
    let mut store = RolloutStore::open(dir.path().to_str().unwrap())
        .await
        .unwrap();
    let version = store.version();
    store.checkout(version).await.unwrap();
    assert!(store
        .add(&[rec("blocked")])
        .await
        .unwrap_err()
        .to_string()
        .contains("read-only"));
    assert!(store.compact(None).await.is_err());
    assert!(store.create_id_zonemap_index().await.is_err());
    assert!(store.is_version_pinned());
    store.refresh_latest().await.unwrap();
    assert_eq!(store.version(), version);
    store.add(&[rec("allowed")]).await.unwrap();
    store.flush().await.unwrap();
    assert_eq!(store.list(None, None).await.unwrap()[0].id, "allowed");
    store.close().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn list_keeps_rows_after_peer_merge() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap();
    let mut writer = RolloutStore::open_with_options(
        uri,
        RolloutStoreOptions {
            shard_id: Some("writer".into()),
            ..Default::default()
        },
    )
    .await
    .unwrap();
    let reader = RolloutStore::open_with_options(
        uri,
        RolloutStoreOptions {
            shard_id: Some("reader".into()),
            ..Default::default()
        },
    )
    .await
    .unwrap();
    writer.add(&[rec("persisted")]).await.unwrap();
    writer.flush().await.unwrap();
    assert_eq!(reader.list(None, None).await.unwrap().len(), 1);
    writer.cleanup_own_shard().await.unwrap();
    let page = reader
        .list_filtered(&Default::default(), 10, 0)
        .await
        .unwrap();
    assert_eq!(page.records.len(), 1);
    assert!(!page.has_more);
    assert_eq!(
        reader.list(None, None).await.unwrap().len(),
        1,
        "a successful peer merge must not hide a previously visible row"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn checkout_excludes_future_wal() {
    let dir = tempfile::tempdir().unwrap();
    let mut store = RolloutStore::open(dir.path().to_str().unwrap())
        .await
        .unwrap();
    store.add(&[rec("old")]).await.unwrap();
    store.cleanup_own_shard().await.unwrap();
    let version = store.version();
    let mut future = rec("future");
    future.binary_payload = Some(vec![1, 2, 3]);
    store.add(&[future]).await.unwrap();
    store.flush().await.unwrap();
    store.checkout(version).await.unwrap();
    assert!(store.get_by_id("future").await.unwrap().is_none());
    assert!(store.get_blob("future").await.unwrap().is_none());
    assert_eq!(store.observe().await.unwrap().row_count, 1);
    assert_eq!(store.cleanup_own_shard().await.unwrap(), 0);
    assert!(store.is_version_pinned());
    let ids: Vec<_> = store
        .list(None, None)
        .await
        .unwrap()
        .into_iter()
        .map(|r| r.id)
        .collect();
    assert_eq!(
        ids,
        vec!["old"],
        "checkout must not include later WAL writes"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn concurrent_add_preserves_external_id_uniqueness() {
    let dir = tempfile::tempdir().unwrap();
    let store = ContextStore::open(dir.path().to_str().unwrap())
        .await
        .unwrap();
    let request: AddRecordRequest = serde_json::from_value(serde_json::json!({
        "text_payload":"hello", "external_id":"same-external-id"
    }))
    .unwrap();
    let left = [record_from_add_request(&request, "left".into(), "r".into())];
    let right = [record_from_add_request(
        &request,
        "right".into(),
        "r".into(),
    )];
    let (a, b) = tokio::join!(store.add(&left), store.add(&right));
    assert_eq!(store.list(None, None).await.unwrap().len(), 1);
    assert_eq!(
        usize::from(a.is_ok()) + usize::from(b.is_ok()),
        1,
        "only one concurrent insertion of the same external_id may succeed"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn context_checkout_excludes_future_wal() {
    let dir = tempfile::tempdir().unwrap();
    let mut store = ContextStore::open(dir.path().to_str().unwrap())
        .await
        .unwrap();
    let request: AddRecordRequest =
        serde_json::from_value(serde_json::json!({"text_payload":"hello"})).unwrap();
    let old = record_from_add_request(&request, "old".into(), "r".into());
    store.add(&[old]).await.unwrap();
    store.cleanup_wal().await.unwrap();
    let version = store.version();
    let future = record_from_add_request(&request, "future".into(), "r".into());
    store.add(&[future]).await.unwrap();
    store.checkout(version).await.unwrap();
    let ids: Vec<_> = store
        .list(None, None)
        .await
        .unwrap()
        .into_iter()
        .map(|r| r.id)
        .collect();
    assert_eq!(
        ids,
        vec!["old"],
        "a pinned context snapshot must exclude subsequent writes"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn write_to_legacy_rollout_preserves_new_fields() {
    use arrow_array::{RecordBatch, RecordBatchIterator};
    use arrow_schema::{ArrowError, Schema};
    use lance::Dataset;
    use std::sync::Arc;
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap();
    let current = lance_context_core::rollout_schema();
    let legacy = Arc::new(Schema::new(
        current
            .fields()
            .iter()
            .filter(|f| {
                ![
                    "model_input_string",
                    "model_output_string",
                    "rationale",
                    "problem_text",
                    "user_metadata",
                ]
                .contains(&f.name().as_str())
            })
            .cloned()
            .collect::<Vec<_>>(),
    ));
    let empty = RecordBatch::new_empty(legacy.clone());
    let reader = RecordBatchIterator::new(vec![Ok::<_, ArrowError>(empty)].into_iter(), legacy);
    Dataset::write(reader, uri, None).await.unwrap();
    let (left, right) = tokio::join!(RolloutStore::open(uri), RolloutStore::open(uri));
    let mut store = left.unwrap();
    let _peer = right.unwrap();
    let mut record = rec("new-format");
    record.model_input_string = Some("must survive".into());
    record.model_output_string = Some("output".into());
    record.rationale = Some("reason".into());
    record.problem_text = Some("question".into());
    record.user_metadata = Some("metadata".into());
    store
        .add(&[record])
        .await
        .expect("new writer should support an older additive schema");
    store.flush().await.unwrap();
    store.cleanup_own_shard().await.unwrap();
    let got = store.get_by_id("new-format").await.unwrap().unwrap();
    assert_eq!(got.model_input_string.as_deref(), Some("must survive"));
    assert_eq!(got.model_output_string.as_deref(), Some("output"));
    assert_eq!(got.rationale.as_deref(), Some("reason"));
    assert_eq!(got.problem_text.as_deref(), Some("question"));
    assert_eq!(got.user_metadata.as_deref(), Some("metadata"));
    store.close().await.unwrap();
    let reopened = RolloutStore::open(uri).await.unwrap();
    assert_eq!(
        reopened
            .get_by_id("new-format")
            .await
            .unwrap()
            .unwrap()
            .model_input_string,
        got.model_input_string
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn concurrent_add_preserves_internal_id_uniqueness() {
    let dir = tempfile::tempdir().unwrap();
    let store = ContextStore::open(dir.path().to_str().unwrap())
        .await
        .unwrap();
    let request: AddRecordRequest =
        serde_json::from_value(serde_json::json!({"text_payload":"hello"})).unwrap();
    let records = [record_from_add_request(
        &request,
        "same-id".into(),
        "r".into(),
    )];
    let (left, right) = tokio::join!(store.add(&records), store.add(&records));
    assert_eq!(usize::from(left.is_ok()) + usize::from(right.is_ok()), 1);
    assert_eq!(store.list(None, None).await.unwrap().len(), 1);
}

#[tokio::test]
async fn checkout_cancels_an_already_prepared_merge() {
    let dir = tempfile::tempdir().unwrap();
    let mut store = RolloutStore::open(dir.path().to_str().unwrap())
        .await
        .unwrap();
    store.add(&[rec("old")]).await.unwrap();
    store.cleanup_own_shard().await.unwrap();
    let version = store.version();
    store.add(&[rec("future")]).await.unwrap();
    let (manifest_store, manifest, prepared) =
        store.prepare_cleanup_merge().await.unwrap().unwrap();
    store.checkout(version).await.unwrap();
    assert_eq!(
        store
            .commit_prepared_merge(&manifest_store, &manifest, prepared)
            .await
            .unwrap(),
        0
    );
    assert_eq!(store.version(), version);
    assert!(store.is_version_pinned());
    assert!(store.get_by_id("future").await.unwrap().is_none());
    store.refresh_latest().await.unwrap();
    assert_eq!(store.pending_wal_generations().await.unwrap(), 1);
}
