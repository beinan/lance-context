use super::*;
use lance_context_wal::replication::producer::{self, Action, Command as ProducerCommand, Reply};

#[test]
fn producer_generations_survive_interrupted_snapshot_and_all_process_restart() -> Result<()> {
    let layout = Layout {
        cluster: Uuid::from_u128(808),
        nodes: (1..=3)
            .map(|id| (id, format!("wal-{id}.example.invalid:7800")))
            .collect(),
        groups: BTreeMap::from([(10, vec![1, 2, 3])]),
        buckets: vec![10],
    };
    let command = |action| ProducerCommand {
        cluster: layout.cluster,
        layout: layout.fingerprint().unwrap(),
        group: 10,
        table: Uuid::from_u128(809),
        slot: 0,
        action,
    };
    let open = |generation| {
        command(Action::Open {
            expected_generation: generation,
            operation: Uuid::from_u128(810 + generation as u128),
            bucket: 0,
        })
    };
    let mut c = Cluster::with_producer_slots(layout.clone(), None, 1)?;
    let leader = c.elect(1, 10, &[1, 2])?;
    for generation in 0..8 {
        let token = 1000 + generation * 2;
        c.command(leader, Request::Producer(token, open(generation)))?;
        c.pump(&[1, 2])?;
        assert_eq!(
            c.outcome(token),
            Some(Outcome::Committed(Applied::Producer(Reply::Opened {
                producer: producer::producer_id(0, generation + 1),
                generation: generation + 1,
                closed: false
            })))
        );
        c.command(
            leader,
            Request::Producer(
                token + 1,
                command(Action::Revoke {
                    generation: generation + 1,
                }),
            ),
        )?;
        c.pump(&[1, 2])?;
        assert_eq!(
            c.outcome(token + 1),
            Some(Outcome::Committed(Applied::Producer(Reply::Closed {
                last: None
            })))
        );
    }
    let Response::Status(cut) = c
        .peers
        .get_mut(&leader)
        .unwrap()
        .call(Request::Status(10))?
    else {
        anyhow::bail!("missing cut")
    };
    assert!(matches!(
        c.peers
            .get_mut(&leader)
            .unwrap()
            .call(Request::PrepareCheckpoint(10, Uuid::new_v4()))?,
        Response::Image(_, _, 0)
    ));
    c.command(leader, Request::InstallCheckpoint)?;
    c.pump(&[1, 2])?;
    c.interrupt_next_snapshot = true;
    for _ in 0..30 {
        c.command(leader, Request::Tick)?;
        c.pump(&[1, 2, 3])?;
        if !c.peers.contains_key(&3) {
            break;
        }
    }
    assert!(
        !c.peers.contains_key(&3),
        "snapshot interruption was not reached"
    );
    c.start(3)?;
    for _ in 0..30 {
        c.command(leader, Request::Tick)?;
        c.pump(&[1, 2, 3])?;
        let Response::Status(status) = c.peers.get_mut(&3).unwrap().call(Request::Status(10))?
        else {
            anyhow::bail!("missing follower status")
        };
        if status.applied >= cut.applied {
            break;
        }
    }
    assert!(c.transfers >= 2, "snapshot retry not exercised");
    c.peers.clear();
    c.queue.clear();
    for id in 1..=3 {
        c.start(id)?;
    }
    let leader = c.elect(3, 10, &[1, 2, 3])?;
    c.command(
        leader,
        Request::Query(10, 2000, Query::ProducerSlot { slot: 0 }),
    )?;
    c.pump(&[1, 2, 3])?;
    let Some(ReadOutcome::Observed {
        value: Value::ProducerSlot(Some(view)),
        ..
    }) = c.read_outcome(2000)
    else {
        anyhow::bail!("slot lost on restart")
    };
    assert_eq!(view.generation, 8);
    assert!(view.reusable);
    c.command(leader, Request::Producer(2001, open(0)))?;
    c.pump(&[1, 2, 3])?;
    assert_eq!(
        c.outcome(2001),
        Some(Outcome::Committed(Applied::Producer(Reply::Fenced)))
    );
    c.command(leader, Request::Producer(2002, open(7)))?;
    c.pump(&[leader])?;
    assert!(
        c.outcome(2002).is_none(),
        "isolated leader returned an allocation retry from cache"
    );
    for _ in 0..10 {
        for id in 1..=3 {
            c.command(id, Request::Tick)?;
        }
        c.pump(&[1, 2, 3])?;
        if c.completions
            .iter()
            .any(|completion| completion.request == 2002)
        {
            break;
        }
    }
    assert_eq!(
        c.outcome(2002),
        Some(Outcome::Committed(Applied::Producer(Reply::Opened {
            producer: producer::producer_id(0, 8),
            generation: 8,
            closed: true
        })))
    );
    c.command(leader, Request::Producer(2003, open(8)))?;
    c.pump(&[1, 2, 3])?;
    assert!(matches!(
        c.outcome(2003),
        Some(Outcome::Committed(Applied::Producer(Reply::Opened {
            generation: 9,
            ..
        })))
    ));
    Ok(())
}
