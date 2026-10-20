//! Real quorum closure after a real Lance publication, with a lost reply and
//! full process restart before attempting to replay the original pin request.
use super::*;
use lance_context_wal::replication::source::Rejection as SourceRejection;

pub(super) fn close_and_restart(
    cluster: &Arc<Mutex<Cluster>>,
    table: Uuid,
    published: &ManifestRecord,
    input: &Batch,
) -> Result<()> {
    let mut c = cluster.lock().unwrap();
    let catalog = c.elect(1, 10, &[1, 2, 3])?;
    let source = c.elect(4, 20, &[4, 5, 6])?;
    c.command(
        catalog,
        Request::Query(
            10,
            600_000,
            Query::Pin {
                table,
                transaction: published.transaction,
                plan_index: published.plan_index,
                source_group: 20,
            },
        ),
    )?;
    c.command(
        catalog,
        Request::Query(
            10,
            600_001,
            Query::ClosePermit {
                table,
                transaction: published.transaction,
                plan_index: published.plan_index,
                source_group: 20,
            },
        ),
    )?;
    c.pump(&[1, 2, 3])?;
    let Some(ReadOutcome::Observed {
        value: Value::Pin(Some(stale)),
        ..
    }) = c.read_outcome(600_000)
    else {
        anyhow::bail!("original pin retry permit missing");
    };
    let Some(ReadOutcome::Observed {
        value: Value::ClosePermit(Some(permit)),
        ..
    }) = c.read_outcome(600_001)
    else {
        anyhow::bail!("closure authorization missing");
    };
    c.command(source, Request::CloseSource(600_002, *permit.clone()))?;
    c.pump(&[4, 5, 6])?;
    let before = c.completions.len();
    c.completions.retain(|c| c.request != 600_002);
    assert_eq!(before - c.completions.len(), 1);
    c.peers
        .get_mut(&source)
        .unwrap()
        .call(Request::PrepareCheckpoint(20, Uuid::new_v4()))?;
    c.command(source, Request::InstallCheckpoint)?;
    c.pump(&[4, 5, 6])?;
    c.queue.clear();
    c.peers.clear();
    for id in 1..=6 {
        c.start(id)?;
    }
    let source = c.elect(4, 20, &[4, 5, 6])?;
    c.command(
        source,
        Request::Query(20, 600_003, Query::SourceClosure { table }),
    )?;
    c.command(
        source,
        Request::Query(20, 600_004, Query::SourcePin { table }),
    )?;
    c.pump(&[4, 5, 6])?;
    let Some(ReadOutcome::Observed {
        value: Value::SourceClosure(Some(closed)),
        ..
    }) = c.read_outcome(600_003)
    else {
        anyhow::bail!("closed-source fence lost on restart");
    };
    assert_eq!(closed.permit.closure(), permit.closure());
    assert!(matches!(
        c.read_outcome(600_004),
        Some(ReadOutcome::Observed {
            value: Value::SourcePin(None),
            ..
        })
    ));
    c.command(source, Request::Pin(600_005, *stale))?;
    c.command(source, Request::CloseSource(600_006, *permit))?;
    c.pump(&[4, 5, 6])?;
    assert_eq!(
        c.outcome(600_005),
        Some(Outcome::Committed(Applied::Source(SourceReply::Rejected(
            SourceRejection::Closed
        ))))
    );
    assert_eq!(
        c.outcome(600_006),
        Some(Outcome::Committed(Applied::Source(SourceReply::Closed {
            index: closed.index
        })))
    );
    for id in 4..=6 {
        verify_records(
            c.peers.get_mut(&id).unwrap(),
            20,
            std::slice::from_ref(input),
        )?;
    }
    Ok(())
}
