//! Read only base + shard manifests. Never opens a generation dataset or payload.
use lance_context_core::{RolloutStore, RolloutStoreOptions};
use std::{
    io::{self, BufRead},
    time::Duration,
};
fn main() {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .unwrap();
    let session = RolloutStore::build_session(96 * 1024 * 1024, 32 * 1024 * 1024);
    for line in io::stdin().lock().lines() {
        let uri = line.unwrap();
        if uri.is_empty() {
            continue;
        }
        let start = std::time::Instant::now();
        let options = RolloutStoreOptions {
            session: Some(session.clone()),
            pending_generations_max: Some(0),
            pending_generations_warn: Some(0),
            ..Default::default()
        };
        let result = rt.block_on(async {
            tokio::time::timeout(Duration::from_secs(40), async {
                let store = RolloutStore::open_existing_with_options(&uri, options).await?;
                let pending = store.pending_wal_generations().await?;
                Ok::<_, lance::Error>((
                    pending,
                    store.version(),
                    store.compaction_stats().total_fragments,
                ))
            })
            .await
        });
        let row = match result {
            Ok(Ok((pending, version, fragments))) => {
                serde_json::json!({"uri":uri,"pending":pending,"version":version,"fragments":fragments,"seconds":start.elapsed().as_secs_f64()})
            }
            Ok(Err(e)) => serde_json::json!({"uri":uri,"error":e.to_string()}),
            Err(e) => serde_json::json!({"uri":uri,"error":e.to_string()}),
        };
        println!("{row}");
    }
}
