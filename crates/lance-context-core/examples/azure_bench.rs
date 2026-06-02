//! Azure Blob Storage benchmark: Write → Compact → Read full cycle
//! Run on a pod with Azure workload identity configured.

use std::sync::Arc;
use std::time::Instant;

use arrow_array::RecordBatchIterator;
use arrow_schema::ArrowError;
use chrono::Utc;
use lance::dataset::{Dataset, WriteMode, WriteParams};
use lance_context_core::{
    CompactionConfig, ContextRecord, ContextStore, ContextStoreOptions, StateMetadata,
};

fn make_records(count: usize, bot_id: &str, session_id: &str) -> Vec<ContextRecord> {
    let base = "Benchmark record with realistic content. This simulates a large context message \
                containing code snippets, documentation, conversation history, and other artifacts \
                that agents typically store in their context window. ";
    let big_text: String = base.repeat(800_000 / base.len() + 1);
    let big_text = &big_text[..800_000];

    (0..count)
        .map(|i| ContextRecord {
            id: format!("rec-{bot_id}-{session_id}-{i}"),
            run_id: "azure-bench".to_string(),
            bot_id: Some(bot_id.to_string()),
            session_id: Some(session_id.to_string()),
            created_at: Utc::now(),
            role: "user".to_string(),
            state_metadata: Some(StateMetadata {
                step: Some(i as i32),
                active_plan_id: None,
                tokens_used: None,
                custom: None,
            }),
            content_type: "text/plain".to_string(),
            text_payload: Some(big_text.to_string()),
            binary_payload: None,
            embedding: Some(vec![0.0f32; 1536]),
        })
        .collect()
}

fn storage_options() -> std::collections::HashMap<String, String> {
    let mut opts = std::collections::HashMap::new();
    // Azure storage account
    opts.insert(
        "azure_storage_account_name".to_string(),
        "falconphxdataprocessing".to_string(),
    );
    // Use workload identity from environment
    if let Ok(client_id) = std::env::var("AZURE_CLIENT_ID") {
        opts.insert("azure_client_id".to_string(), client_id);
    }
    if let Ok(tenant_id) = std::env::var("AZURE_TENANT_ID") {
        opts.insert("azure_tenant_id".to_string(), tenant_id);
    }
    if let Ok(token_file) = std::env::var("AZURE_FEDERATED_TOKEN_FILE") {
        opts.insert("azure_federated_token_file".to_string(), token_file);
    }
    if let Ok(authority) = std::env::var("AZURE_AUTHORITY_HOST") {
        opts.insert("azure_authority_host".to_string(), authority);
    }
    opts.insert("azure_use_fabric_endpoint".to_string(), "false".to_string());
    opts
}

async fn bench_phase(label: &str, f: impl std::future::Future<Output = ()>) {
    let start = Instant::now();
    f.await;
    let elapsed = start.elapsed();
    println!("  {label}: {elapsed:?}");
}

#[tokio::main]
async fn main() {
    let base_uri = std::env::args().nth(1).unwrap_or_else(|| {
        "az://falconphxdataprocessing@falconphxdataprocessing.dfs.core.windows.net/users/beinanwang/lance-bench".to_string()
    });

    let opts = storage_options();
    let records_per_session = 10usize;

    println!("=== Azure Blob Storage Benchmark ===");
    println!("Base URI: {base_uri}");
    println!("Record size: ~806 KB (800KB text + 6KB embedding)");
    println!();

    // -----------------------------------------------------------------------
    // Test 1: WAL write scalability on Azure
    // -----------------------------------------------------------------------
    println!("--- WAL Write (ContextStore::add) ---");
    for num_sessions in [1, 10, 50] {
        let uri = format!("{base_uri}/wal-{num_sessions}sess");
        let total = num_sessions * records_per_session;
        let data_mb = total as f64 * 0.806;

        let start = Instant::now();
        let mut store = ContextStore::open_with_options(
            &uri,
            ContextStoreOptions {
                storage_options: Some(opts.clone()),
                ..Default::default()
            },
        )
        .await
        .unwrap();

        for s in 0..num_sessions {
            let records = make_records(
                records_per_session,
                &format!("bot-{s}"),
                &format!("session-{s}"),
            );
            store.add(&records).await.unwrap();
        }
        let elapsed = start.elapsed();
        let throughput = data_mb / elapsed.as_secs_f64();
        println!(
            "  {num_sessions} sessions ({total} records, {data_mb:.0} MB): {elapsed:?} ({throughput:.1} MB/s)"
        );
    }
    println!();

    // -----------------------------------------------------------------------
    // Test 2: Full cycle (direct append) — Write → Compact → List → Search
    // -----------------------------------------------------------------------
    println!("--- Full Cycle (Direct Append → Compact → Read) ---");
    for num_sessions in [1, 10, 50] {
        let uri = format!("{base_uri}/full-{num_sessions}sess");
        let total = num_sessions * records_per_session;
        let data_mb = total as f64 * 0.806;
        let schema = Arc::new(ContextStore::schema());

        println!("  [{num_sessions} sessions, {total} records, {data_mb:.0} MB]");

        // Write
        let write_start = Instant::now();
        for s in 0..num_sessions {
            let records = make_records(
                records_per_session,
                &format!("bot-{s}"),
                &format!("session-{s}"),
            );
            let batch = ContextStore::records_to_batch(&records).unwrap();
            let reader = RecordBatchIterator::new(
                vec![Ok::<_, ArrowError>(batch)],
                schema.clone(),
            );
            let mode = if s == 0 {
                WriteMode::Create
            } else {
                WriteMode::Append
            };

            let mut params = WriteParams {
                mode,
                ..Default::default()
            };
            let store_params = lance::io::ObjectStoreParams {
                storage_options_accessor: Some(Arc::new(
                    lance::io::StorageOptionsAccessor::with_static_options(opts.clone()),
                )),
                ..Default::default()
            };
            params.store_params = Some(store_params);

            Dataset::write(reader, &uri, Some(params)).await.unwrap();
        }
        let write_elapsed = write_start.elapsed();
        let write_tp = data_mb / write_elapsed.as_secs_f64();
        println!("    Write:   {write_elapsed:?} ({write_tp:.1} MB/s)");

        // Compact
        bench_phase("Compact", async {
            let mut store = ContextStore::open_with_options(
                &uri,
                ContextStoreOptions {
                    storage_options: Some(opts.clone()),
                    ..Default::default()
                },
            )
            .await
            .unwrap();
            store
                .compact(Some(CompactionConfig {
                    min_fragments: 0,
                    ..Default::default()
                }))
                .await
                .unwrap();
        })
        .await;

        // List (full scan)
        bench_phase("List", async {
            let store = ContextStore::open_with_options(
                &uri,
                ContextStoreOptions {
                    storage_options: Some(opts.clone()),
                    ..Default::default()
                },
            )
            .await
            .unwrap();
            let results = store.list(None, None).await.unwrap();
            println!("    (read back {} records)", results.len());
        })
        .await;

        // Search (KNN)
        bench_phase("Search", async {
            let store = ContextStore::open_with_options(
                &uri,
                ContextStoreOptions {
                    storage_options: Some(opts.clone()),
                    ..Default::default()
                },
            )
            .await
            .unwrap();
            let query = vec![0.0f32; 1536];
            let results = store.search(&query, Some(10)).await.unwrap();
            println!("    (found {} results)", results.len());
        })
        .await;

        println!();
    }

    println!("=== Done ===");
    println!("Cleanup: delete {base_uri}/* from Azure Blob Storage when finished.");
}
