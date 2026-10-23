//! Exercise the service scheduler against real Raft workers and physical logs.
use super::*;
use crate::{raft_log::DiskOptions, service::maintenance};
use uuid::Uuid;

fn disk() -> DiskOptions {
    DiskOptions {
        max_bytes: 1 << 30,
        data_bytes: 512 << 20,
        headroom_bytes: 256 << 20,
    }
}

// A long normal interval makes an erroneous full-interval postponement visible.
// Failed work should become eligible at the next existing scheduler tick.
fn options() -> maintenance::Options {
    maintenance::Options {
        interval_ms: 25,
        checkpoint_entries: u64::MAX,
        checkpoint_interval_ms: 4000,
        log_interval_ms: 60_000,
        scan_entries: 8,
    }
}

async fn failure_has_occurred(progress: &Arc<Mutex<maintenance::Progress>>) -> Result<()> {
    timeout(Duration::from_secs(8), async {
        loop {
            if progress.lock().unwrap().attempts_failed > 0 {
                break;
            }
            sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .context("expected maintenance admission failure did not occur")
}

fn verify_reopened(root: &Path, index: u64) -> Result<()> {
    for id in 1..=3 {
        let node = Node::open(&root.join(id.to_string()), layout(), id, Limits::default())?;
        assert_eq!(node.read_accepted(10, index)?, Some(batch(1)));
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn busy_checkpoint_does_not_postpone_the_next_eligible_attempt() -> Result<()> {
    let root = tempfile::tempdir()?;
    let c = Cluster::open_with_disk(root.path(), Options::default(), Some(disk()))?;
    c.elect(1, 10).await?;
    let index = commit(&c.handles[&1], 10, batch(1)).await?;
    wait_for(|| c.handles[&3].observation(10).unwrap().status.applied >= index).await?;
    // Hold the same physical-node permit that a snapshot receiver/build uses.
    let busy = c.handles[&3]
        .prepare_checkpoint(10, Uuid::new_v4())?
        .await?;
    let progress = Arc::new(Mutex::new(maintenance::Progress::default()));
    let (stop, stopped) = tokio::sync::watch::channel(false);
    let task = tokio::spawn(maintenance::run(
        c.handles[&3].clone(),
        vec![10],
        options(),
        progress.clone(),
        stopped,
    ));
    // Collection also uses this permit. Keep it until the first checkpoint age
    // expires, rather than treating an earlier collection failure as evidence.
    sleep(Duration::from_millis(4500)).await;
    failure_has_occurred(&progress).await?;
    assert_eq!(progress.lock().unwrap().checkpoints, 0);
    drop(busy);
    let result = timeout(Duration::from_secs(2), async {
        loop {
            if c.handles[&3].observation(10)?.checkpoint_index >= index {
                return Ok::<_, anyhow::Error>(());
            }
            sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .context("released checkpoint permit was postponed for a full checkpoint interval");
    stop.send(true)?;
    task.await??;
    c.shutdown().await?;
    result??;
    verify_reopened(root.path(), index)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn released_rewrite_headroom_does_not_wait_another_log_interval() -> Result<()> {
    let root = tempfile::tempdir()?;
    let c = Cluster::open_with_disk(root.path(), Options::default(), Some(disk()))?;
    c.elect(1, 10).await?;
    let index = commit(&c.handles[&1], 10, batch(1)).await?;
    wait_for(|| c.handles[&3].observation(10).unwrap().status.applied >= index).await?;
    let budget = c.handles[&3].disk.as_ref().unwrap();
    let measured = c.handles[&3].prepare_disk_measurement()?.await?.run()?;
    // Reserve headroom without creating extra data or changing disk ceilings.
    // A native rewrite must reject until this concurrent reservation is freed.
    let held = budget.reserve(
        usize::try_from(measured.max_log_bytes - measured.accounted_disk_bytes - (1 << 20))?,
        false,
    )?;
    let progress = Arc::new(Mutex::new(maintenance::Progress::default()));
    let (stop, stopped) = tokio::sync::watch::channel(false);
    let task = tokio::spawn(maintenance::run(
        c.handles[&3].clone(),
        vec![10],
        maintenance::Options {
            checkpoint_interval_ms: 60_000,
            log_interval_ms: 4000,
            ..options()
        },
        progress.clone(),
        stopped,
    ));
    failure_has_occurred(&progress).await?;
    assert_eq!(progress.lock().unwrap().log_passes, 0);
    drop(held);
    let result = timeout(Duration::from_secs(2), async {
        loop {
            if progress.lock().unwrap().log_passes > 0 {
                break;
            }
            sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .context("released rewrite headroom was postponed for a full log interval");
    stop.send(true)?;
    task.await??;
    c.shutdown().await?;
    result?;
    verify_reopened(root.path(), index)
}
