use super::*;

fn hold_worker(handle: &Handle) -> Result<(mpsc::Receiver<()>, mpsc::Sender<()>)> {
    let (entered, arrived) = mpsc::channel();
    let (resume, resumed) = mpsc::channel::<()>();
    handle.enqueue(
        handle.route(10)?,
        Work::Local(Box::new(move |_| {
            entered.send(()).unwrap();
            let _ = resumed.recv();
            Box::new(|| {})
        })),
        false,
    )?;
    Ok((arrived, resume))
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn slow_batch_polls_before_the_next_control_request_and_preserves_durable_ack() -> Result<()>
{
    let directory = tempfile::tempdir()?;
    let c = Cluster::open(
        directory.path(),
        Options {
            batch_requests: 32,
            batch_time: Duration::from_millis(1),
            tick_interval: Duration::from_secs(1),
            ..Options::default()
        },
    )?;
    c.elect(1, 10).await?;
    let h = &c.handles[&1];
    assert_eq!(c.nonempty_entry_packets.load(Ordering::Acquire), 0);

    // The first control operation occupies the current drain round. Queue an
    // append in its data lane, followed by another control operation. Holding
    // the first operation gives a deterministic slow round without assuming
    // CPU speed or adding a production delay.
    let (first_entered, first_resume) = hold_worker(h)?;
    first_entered.recv_timeout(Duration::from_secs(5))?;
    let pending = h.commit(Command::Append {
        group: 10,
        batch: batch(1),
    })?;
    let (second_entered, second_resume) = hold_worker(h)?;
    sleep(Duration::from_millis(20)).await;
    drop(first_resume);
    second_entered.recv_timeout(Duration::from_secs(5))?;

    // The first append must have crossed local persistence and entered actual
    // replica transport before the next slow request can occupy the worker.
    // Keep this result until after cleanup so the old behavior fails without
    // leaving a blocked thread or an unresolved mutation behind.
    let emitted = wait_for(|| c.nonempty_entry_packets.load(Ordering::Acquire) > 0).await;
    let unacknowledged = pending.0.is_empty();
    drop(second_resume);
    let committed = index(timeout(Duration::from_secs(10), pending).await??);
    wait_for(|| {
        c.handles
            .values()
            .all(|h| h.observation(10).unwrap().status.applied >= committed)
    })
    .await?;
    c.shutdown().await?;
    for id in 1..=3 {
        let node = Node::open(
            &directory.path().join(id.to_string()),
            layout(),
            id,
            Limits::default(),
        )?;
        assert_eq!(node.read_accepted(10, committed)?, Some(batch(1)));
    }
    assert!(unacknowledged, "append bypassed quorum/apply completion");
    assert!(
        emitted.is_ok(),
        "slow drain round delayed replication behind the next control request"
    );
    Ok(())
}
