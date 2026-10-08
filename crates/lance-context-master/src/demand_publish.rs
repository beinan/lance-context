//! Detached, bounded publication of demand watermarks from master-side
//! sources (`docs/design/scheduler-p1-data-model.md` §1.3).
//!
//! Same discipline as the writer-side publish: never awaited by the caller,
//! one hard timeout, a process-wide concurrency cap, drop-and-count when the
//! cap is full. Every publish is a per-shard monotonic join, so a dropped or
//! late publish cannot misstate demand; the next scan reconciles it.

use std::sync::Arc;
use std::time::Duration;

use lance_context_merge::demand::ShardMerged;
use lance_context_merge::Coordinator;
use tokio::sync::Semaphore;

/// Base-table index read for the merged watermarks plus one etcd put per shard.
const MERGED_PUBLISH_TIMEOUT: Duration = Duration::from_secs(15);

static MERGED_PUBLISH_SLOTS: std::sync::LazyLock<Arc<Semaphore>> =
    std::sync::LazyLock::new(|| Arc::new(Semaphore::new(4)));

/// After a successful WAL catch-up has released ownership, publish every
/// shard's merged watermark. Detached and bounded; never on the release
/// path. Each shard is a separate monotonic put, so a table with hundreds of
/// shards never approaches the etcd transaction op limit and a concurrent
/// writer flush on one shard cannot fail the others.
pub(crate) fn spawn_merged_demand(coordinator: Coordinator, target: String, uri: String) {
    let Ok(permit) = MERGED_PUBLISH_SLOTS.clone().try_acquire_owned() else {
        metrics::counter!("master_demand_merged_watermarks_total", "result" => "dropped")
            .increment(1);
        return;
    };
    tokio::spawn(async move {
        let _permit = permit;
        let result = tokio::time::timeout(MERGED_PUBLISH_TIMEOUT, async {
            let marks = lance_context_core::rollout_append::merged_watermarks(&uri)
                .await
                .map_err(|e| e.to_string())?;
            let merged: Vec<ShardMerged> = marks
                .into_iter()
                .map(|(shard, generation)| ShardMerged {
                    shard: shard.to_string(),
                    merged_through: generation,
                })
                .collect();
            let mut written = 0usize;
            for shard in &merged {
                if coordinator.publish_merged_watermark(&target, shard).await? {
                    written += 1;
                }
            }
            Ok::<(usize, usize), String>((written, merged.len()))
        })
        .await;
        let (label, count) = match result {
            Ok(Ok((written, total))) => (if written > 0 { "written" } else { "unchanged" }, total),
            Ok(Err(error)) => {
                tracing::warn!(target = %target, %error, "merged watermark publish failed");
                ("failed", 0)
            }
            Err(_) => {
                tracing::warn!(target = %target, "merged watermark publish timed out");
                ("timeout", 0)
            }
        };
        metrics::counter!("master_demand_merged_watermarks_total", "result" => label)
            .increment(count.max(1) as u64);
    });
}
