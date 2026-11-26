#![recursion_limit = "256"]
#![cfg(feature = "lance-io-metrics")]

use lance_context_core::{trace_schema, GenericStore, GenericStoreOptions};
use metrics_util::debugging::{DebugValue, DebuggingRecorder};
use serde_json::json;

/// This integration-test executable owns its recorder, so Lance's background
/// tasks use the same recorder without depending on thread-local propagation.
#[tokio::test]
async fn real_lance_write_and_reopen_emit_storage_metrics() {
    // One test in a separate process; set the label policy before opening stores.
    std::env::set_var("LANCE_OBJECT_STORE_METRICS_LABEL", "scheme");
    let recorder = DebuggingRecorder::new();
    let snapshotter = recorder.snapshotter();
    metrics::set_global_recorder(recorder).unwrap();
    lance_context_core::metrics::describe_lance_io_metrics();

    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().join("records.lance");
    let uri = uri.to_str().unwrap();
    let mut store = GenericStore::open(
        uri,
        trace_schema(),
        GenericStoreOptions {
            seal_on_add: true,
            ..Default::default()
        },
    )
    .await
    .unwrap();
    let row = json!({"id":"record", "session_id":"session", "turn_id":0,
        "role":"assistant", "content_type":"text/plain", "content":"retained payload"})
    .as_object()
    .unwrap()
    .clone();
    store.add(std::slice::from_ref(&row)).await.unwrap();
    store.cleanup_wal().await.unwrap();
    store.close().await.unwrap();
    drop(store);
    let store = GenericStore::open_existing(uri, Default::default())
        .await
        .unwrap();
    let rows = store.list(None, None).await.unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0]["content"], row["content"]);

    let mut read_bytes = 0;
    let mut written_bytes = 0;
    let mut durations = 0;
    let mut requests = 0;
    for (key, _, _, value) in snapshotter.snapshot().into_vec() {
        if !key.key().name().starts_with("lance_object_store_") {
            continue;
        }
        let labels: Vec<_> = key.key().labels().collect();
        assert_eq!(
            labels.len(),
            2,
            "local I/O exposes only operation and scheme"
        );
        for label in &labels {
            match label.key() {
                "base" => assert_eq!(label.value(), "file"),
                "operation" => assert!(matches!(
                    label.value(),
                    "get"
                        | "head"
                        | "put"
                        | "put_part"
                        | "complete_multipart"
                        | "abort_multipart"
                        | "list"
                        | "copy"
                        | "rename"
                        | "delete"
                )),
                key => panic!("unexpected local I/O label {key}"),
            }
        }
        let operation = labels
            .iter()
            .find(|label| label.key() == "operation")
            .map(|label| label.value());
        match (key.key().name(), value) {
            ("lance_object_store_request_bytes_total", DebugValue::Counter(bytes)) => {
                match operation {
                    Some("get") => read_bytes += bytes,
                    Some("put" | "put_part") => written_bytes += bytes,
                    _ => {}
                }
            }
            ("lance_object_store_requests_total", DebugValue::Counter(count)) => {
                requests += count;
            }
            ("lance_object_store_request_duration_seconds", DebugValue::Histogram(samples)) => {
                assert!(samples
                    .iter()
                    .all(|s| s.into_inner().is_finite() && s.into_inner() >= 0.0));
                durations += samples.len();
            }
            _ => {}
        }
    }
    assert!(read_bytes > 0, "reopen/read must record storage bytes");
    assert!(
        written_bytes > 0,
        "actual Lance writes must record storage bytes"
    );
    assert!(requests > 0 && durations > 0);
}
