//! Exercise lifecycle boundaries through actual Raft processes and the existing
//! Lance publication/closure fixture. No synthetic retirement proof is inserted.
use super::*;
use lance_context_wal::replication::producer::{self, Action, Command, Last, Reply};

fn command(c: &Cluster, table: Uuid, action: Action) -> Result<Command> {
    let layout: Layout =
        serde_json::from_slice(&std::fs::read(c.root.path().join("layout.json"))?)?;
    Ok(Command {
        cluster: layout.cluster,
        layout: layout.fingerprint()?,
        group: 20,
        table,
        slot: 0,
        action,
    })
}
fn allocate(c: &Cluster, table: Uuid, expected_generation: u64) -> Result<Command> {
    command(
        c,
        table,
        Action::Open {
            expected_generation,
            operation: Uuid::from_u128(800_000 + expected_generation as u128),
            bucket: 1,
        },
    )
}
fn control(c: &mut Cluster, leader: u64, token: u64, command: Command) -> Result<Reply> {
    c.command(leader, Request::Producer(token, command))?;
    c.pump(&[4, 5, 6])?;
    let Some(Outcome::Committed(Applied::Producer(reply))) = c.outcome(token) else {
        anyhow::bail!("producer command did not commit")
    };
    Ok(reply)
}
fn view(c: &mut Cluster, leader: u64, token: u64) -> Result<producer::View> {
    c.command(
        leader,
        Request::Query(20, token, Query::ProducerSlot { slot: 0 }),
    )?;
    c.pump(&[4, 5, 6])?;
    let Some(ReadOutcome::Observed {
        value: Value::ProducerSlot(Some(view)),
        ..
    }) = c.read_outcome(token)
    else {
        anyhow::bail!("slot quorum view missing")
    };
    Ok(view)
}

pub(in crate::lance_publication) fn open(
    c: &mut Cluster,
    leader: u64,
    table: Uuid,
) -> Result<Uuid> {
    let request = allocate(c, table, 0)?;
    // Drop a committed allocation completion, then retry the saved CAS exactly.
    c.command(leader, Request::Producer(700_000, request.clone()))?;
    c.pump(&[4, 5, 6])?;
    assert!(c.outcome(700_000).is_some());
    let reply = control(c, leader, 700_001, request)?;
    assert_eq!(
        reply,
        Reply::Opened {
            producer: producer::producer_id(0, 1),
            generation: 1,
            closed: false
        }
    );
    assert_eq!(view(c, leader, 700_002)?.generation, 1);
    Ok(producer::producer_id(0, 1))
}

pub(in crate::lance_publication) fn close_before_publication(
    c: &mut Cluster,
    leader: u64,
    input: &Batch,
    index: u64,
) -> Result<()> {
    let table = input.meta.stream;
    let last = Last { sequence: 1, index };
    let stale = command(
        c,
        table,
        Action::Close {
            generation: 1,
            expected_last: None,
        },
    )?;
    assert_eq!(
        control(c, leader, 700_010, stale)?,
        Reply::LastChanged { actual: Some(last) }
    );
    let close = command(
        c,
        table,
        Action::Close {
            generation: 1,
            expected_last: Some(last),
        },
    )?;
    assert_eq!(
        control(c, leader, 700_011, close)?,
        Reply::Closed { last: Some(last) }
    );
    let next = allocate(c, table, 1)?;
    assert_eq!(control(c, leader, 700_012, next)?, Reply::Busy);
    let mut delayed = input.clone();
    delayed.meta.sequence = 2;
    c.command(leader, Request::Propose(20, 700_013, delayed))?;
    c.pump(&[4, 5, 6])?;
    assert_eq!(
        c.outcome(700_013),
        Some(Outcome::Committed(Applied::ProducerFenced))
    );
    let current = view(c, leader, 700_014)?;
    assert!(current.lease.unwrap().closed && !current.reusable);
    Ok(())
}

pub(super) fn reuse_after_retirement(cluster: &Arc<Mutex<Cluster>>, input: &Batch) -> Result<()> {
    let mut c = cluster.lock().unwrap();
    let table = input.meta.stream;
    let leader = c.elect(4, 20, &[4, 5, 6])?;
    let before = view(&mut c, leader, 700_020)?;
    assert_eq!(before.generation, 1);
    assert!(before.lease.unwrap().closed && before.reusable);
    let request = allocate(&c, table, 1)?;
    assert_eq!(
        control(&mut c, leader, 700_021, request.clone())?,
        Reply::Opened {
            producer: producer::producer_id(0, 2),
            generation: 2,
            closed: false
        }
    );
    c.command(leader, Request::Propose(20, 700_022, input.clone()))?;
    c.pump(&[4, 5, 6])?;
    assert_eq!(
        c.outcome(700_022),
        Some(Outcome::Committed(Applied::ProducerFenced))
    );
    let old = allocate(&c, table, 0)?;
    assert_eq!(control(&mut c, leader, 700_023, old)?, Reply::Fenced);
    let revoke = command(&c, table, Action::Revoke { generation: 1 })?;
    assert_eq!(control(&mut c, leader, 700_024, revoke)?, Reply::Fenced);
    let mut next = input.clone();
    next.meta.producer = producer::producer_id(0, 2);
    c.command(leader, Request::Propose(20, 700_025, next.clone()))?;
    c.pump(&[4, 5, 6])?;
    let Some(Outcome::Committed(Applied::Accepted { index })) = c.outcome(700_025) else {
        anyhow::bail!("new generation append missing")
    };
    // Kill the active leader. Resolve the exact new write on the surviving quorum.
    c.peers.remove(&leader);
    c.queue.clear();
    let survivors: Vec<_> = (4..=6).filter(|id| *id != leader).collect();
    let replacement = c.elect(survivors[0], 20, &survivors)?;
    c.command(replacement, Request::Propose(20, 700_026, next.clone()))?;
    c.pump(&survivors)?;
    assert_eq!(
        c.outcome(700_026),
        Some(Outcome::Committed(Applied::Accepted { index }))
    );
    c.start(leader)?;
    c.elect(replacement, 20, &[4, 5, 6])?;
    for id in 4..=6 {
        let Response::Image(_, _, bytes) = c
            .peers
            .get_mut(&id)
            .unwrap()
            .call(Request::PrepareCheckpoint(20, Uuid::new_v4()))?
        else {
            anyhow::bail!("slot image missing")
        };
        assert_eq!(bytes, next.body.len() as u64);
        c.command(id, Request::InstallCheckpoint)?;
        c.pump(&[4, 5, 6])?;
    }
    c.peers.clear();
    c.queue.clear();
    for id in 1..=6 {
        c.start(id)?;
    }
    let leader = c.elect(4, 20, &[4, 5, 6])?;
    assert_eq!(
        control(&mut c, leader, 700_027, request)?,
        Reply::Opened {
            producer: producer::producer_id(0, 2),
            generation: 2,
            closed: false
        }
    );
    c.command(leader, Request::Propose(20, 700_028, input.clone()))?;
    c.command(leader, Request::Propose(20, 700_029, next.clone()))?;
    c.pump(&[4, 5, 6])?;
    assert_eq!(
        c.outcome(700_028),
        Some(Outcome::Committed(Applied::ProducerFenced))
    );
    assert_eq!(
        c.outcome(700_029),
        Some(Outcome::Committed(Applied::Accepted { index }))
    );
    let current = view(&mut c, leader, 700_030)?;
    assert_eq!(current.capacity, 1);
    assert_eq!(current.last, Some(Last { sequence: 1, index }));
    c.command(
        leader,
        Request::Query(
            20,
            700_031,
            Query::Producer {
                table,
                bucket: 1,
                producer: input.meta.producer,
            },
        ),
    )?;
    c.pump(&[4, 5, 6])?;
    assert!(matches!(
        c.read_outcome(700_031),
        Some(ReadOutcome::Observed {
            value: Value::Producer(None),
            ..
        })
    ));
    for id in 4..=6 {
        verify_records(
            c.peers.get_mut(&id).unwrap(),
            20,
            std::slice::from_ref(&next),
        )?;
    }
    Ok(())
}
