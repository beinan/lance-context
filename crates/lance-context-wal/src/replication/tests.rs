use super::*;
use bytes::Bytes;

fn layout() -> Arc<Layout> {
    Arc::new(Layout {
        cluster: Uuid::from_u128(1),
        nodes: (1..=3)
            .map(|id| (id, format!("wal-{id}.example.invalid:7000")))
            .collect(),
        groups: BTreeMap::from([(10, vec![1, 2, 3]), (20, vec![1, 2, 3])]),
        buckets: vec![10, 20],
    })
}

fn batch(sequence: u64) -> Batch {
    Batch::new(
        Uuid::from_u128(2),
        0,
        Uuid::from_u128(3),
        sequence,
        Bytes::from(vec![sequence as u8; 4096]),
    )
}

fn pump(nodes: &mut [Node]) -> Result<Vec<Completion>> {
    let mut completions = Vec::new();
    for _ in 0..100 {
        let mut messages = Vec::new();
        for node in &mut *nodes {
            let events = node.poll()?;
            completions.extend(events.completions);
            messages.extend(events.messages);
        }
        if messages.is_empty() {
            return Ok(completions);
        }
        for packet in messages {
            nodes[packet.message.to as usize - 1].step(packet)?;
        }
    }
    anyhow::bail!("message pump did not settle")
}

#[test]
fn no_quorum_ack_before_peer_persistence_and_proposal_keeps_body_allocation() -> Result<()> {
    let directory = tempfile::tempdir()?;
    let mut nodes = (1..=3)
        .map(|id| {
            Node::open(
                &directory.path().join(id.to_string()),
                layout(),
                id,
                Limits::default(),
            )
        })
        .collect::<Result<Vec<_>>>()?;
    nodes[0].campaign(10)?;
    pump(&mut nodes)?;
    let body = batch(1);
    let address = body.body.as_ptr();
    nodes[0].propose(10, 7, body)?;
    let unstable = &nodes[0].groups[&10].raw.raft.raft_log.unstable.entries;
    assert_eq!(unstable.last().unwrap().data.as_ptr(), address);
    let events = nodes[0].poll()?;
    assert!(events.completions.is_empty());
    assert_eq!(nodes[0].status(10)?.accepted_records, 0);
    for packet in events.messages.into_iter().filter(|p| p.message.to == 2) {
        nodes[1].step(packet)?;
    }
    // Receiving the append in memory is not a durable follower response.
    assert!(nodes[0].poll()?.completions.is_empty());
    nodes[1].groups[&10].raw.store().fail_next_sync_for_test();
    assert!(nodes[1].poll().is_err());
    assert!(
        nodes[1].status(20).is_err(),
        "one engine failure fences every group"
    );
    assert!(nodes[1].poll().is_err());
    assert!(nodes[0].poll()?.completions.is_empty());
    Ok(())
}

#[test]
fn committed_retries_preserve_original_index_and_scope_with_bounded_state() -> Result<()> {
    let directory = tempfile::tempdir()?;
    let limits = Limits {
        max_producers: 1,
        max_records: 2,
        ..Default::default()
    };
    let mut nodes = (1..=3)
        .map(|id| {
            Node::open(
                &directory.path().join(id.to_string()),
                layout(),
                id,
                limits.clone(),
            )
        })
        .collect::<Result<Vec<_>>>()?;
    nodes[0].campaign(10)?;
    pump(&mut nodes)?;
    nodes[0].propose(10, 1, batch(1))?;
    let first = pump(&mut nodes)?.remove(0).outcome;
    assert!(matches!(
        first,
        Outcome::Committed(Applied::Accepted { .. })
    ));
    nodes[0].propose(10, 2, batch(1))?;
    assert_eq!(pump(&mut nodes)?.remove(0).outcome, first);
    let mut changed = batch(1);
    changed.body = Bytes::from(vec![4; 4096]);
    changed.meta.digest = Sha256::digest(&changed.body).into();
    nodes[0].propose(10, 3, changed)?;
    assert_eq!(
        pump(&mut nodes)?.remove(0).outcome,
        Outcome::Committed(Applied::Rejected {
            expected_sequence: 2
        })
    );
    nodes[0].propose(10, 4, batch(3))?;
    assert_eq!(
        pump(&mut nodes)?.remove(0).outcome,
        Outcome::Committed(Applied::Rejected {
            expected_sequence: 2
        })
    );
    let mut other = batch(1);
    other.meta.stream = Uuid::from_u128(9);
    nodes[0].propose(10, 5, other)?;
    assert_eq!(
        pump(&mut nodes)?.remove(0).outcome,
        Outcome::Committed(Applied::Capacity)
    );
    nodes[0].propose(10, 6, batch(2))?;
    assert!(matches!(
        pump(&mut nodes)?.remove(0).outcome,
        Outcome::Committed(Applied::Accepted { .. })
    ));
    nodes[0].propose(10, 7, batch(3))?;
    assert_eq!(
        pump(&mut nodes)?.remove(0).outcome,
        Outcome::Committed(Applied::Capacity)
    );
    // Latest retry still works at capacity, and old sequences do not.
    nodes[0].propose(10, 8, batch(2))?;
    assert!(matches!(
        pump(&mut nodes)?.remove(0).outcome,
        Outcome::Committed(Applied::Accepted { .. })
    ));
    for node in &nodes {
        assert_eq!(node.status(10)?.accepted_records, 2);
    }
    Ok(())
}

#[test]
fn foreign_policy_invalid_input_and_unidentified_logs_are_rejected() -> Result<()> {
    let directory = tempfile::tempdir()?;
    let path = directory.path().join("log");
    let mut node = Node::open(&path, layout(), 1, Limits::default())?;
    let mut invalid = batch(1);
    invalid.meta.bucket = 1;
    assert!(node.propose(10, 1, invalid).is_err());
    node.campaign(10)?;
    let mut packet = node.poll()?.messages.remove(0);
    packet.message.to = 1;
    packet.message.from = 2;
    packet.policy[0] ^= 1;
    assert!(node.step(packet).is_err());
    assert_eq!(node.status(10)?.accepted_records, 0);
    drop(node);
    let limits = Limits {
        max_records: 1,
        ..Default::default()
    };
    assert!(Node::open(&path, layout(), 1, limits).is_err());
    Node::open(&path, layout(), 1, Limits::default())?;
    let path = directory.path().join("unidentified");
    let log = PhysicalLog::open(&path, &layout(), 1)?;
    log.group(10)?.persist(
        &[Entry {
            index: 1,
            term: 1,
            data: vec![1].into(),
            ..Default::default()
        }],
        Some(&raft_proto::eraftpb::HardState {
            term: 1,
            vote: 1,
            commit: 1,
            ..Default::default()
        }),
    )?;
    drop(log);
    assert!(Node::open(&path, layout(), 1, Limits::default()).is_err());
    Ok(())
}

#[test]
fn accepted_pages_capture_progress_without_skipping_later_appends() -> Result<()> {
    let directory = tempfile::tempdir()?;
    let mut nodes = (1..=3)
        .map(|id| {
            Node::open(
                &directory.path().join(id.to_string()),
                layout(),
                id,
                Limits::default(),
            )
        })
        .collect::<Result<Vec<_>>>()?;
    nodes[0].campaign(10)?;
    pump(&mut nodes)?;
    // The election's blank entry advances even an empty page's cursor.
    let blank = nodes[0].accepted(10, 0, 1)?;
    assert!(blank.entries.is_empty());
    assert!(blank.through > 0);
    for sequence in 1..=2 {
        nodes[0].propose(10, sequence, batch(sequence))?;
        assert!(matches!(
            pump(&mut nodes)?.remove(0).outcome,
            Outcome::Committed(Applied::Accepted { .. })
        ));
    }
    let first = nodes[0].accepted(10, blank.through, 1)?;
    assert_eq!(first.entries.len(), 1);
    assert_eq!(first.entries[0].1, batch(1).meta);
    assert_eq!(first.through, first.entries[0].0);
    let second = nodes[0].accepted(10, first.through, 1)?;
    assert_eq!(second.entries.len(), 1);
    assert_eq!(second.entries[0].1, batch(2).meta);
    let captured = second.through;
    // A concurrent append after page capture must be read on the next call.
    nodes[0].propose(10, 3, batch(3))?;
    assert!(matches!(
        pump(&mut nodes)?.remove(0).outcome,
        Outcome::Committed(Applied::Accepted { .. })
    ));
    assert!(nodes[0].status(10)?.applied > captured);
    let third = nodes[0].accepted(10, captured, 1)?;
    assert_eq!(third.entries.len(), 1);
    assert_eq!(third.entries[0].1, batch(3).meta);
    // Rejected and duplicate entries make progress without new accepted rows.
    nodes[0].propose(10, 4, batch(5))?;
    assert_eq!(
        pump(&mut nodes)?.remove(0).outcome,
        Outcome::Committed(Applied::Rejected {
            expected_sequence: 4
        })
    );
    nodes[0].propose(10, 5, batch(3))?;
    assert_eq!(
        pump(&mut nodes)?.remove(0).outcome,
        Outcome::Committed(Applied::Accepted {
            index: third.entries[0].0
        })
    );
    let tail = nodes[0].accepted(10, third.through, 1)?;
    assert!(tail.entries.is_empty());
    assert_eq!(tail.through, nodes[0].status(10)?.applied);
    assert!(tail.through > third.through);
    assert_eq!(nodes[0].accepted(10, tail.through, 1)?, tail);
    // A full page with no more accepted rows can still cover the non-accepted tail.
    let last = nodes[0].accepted(10, captured, 1)?;
    assert_eq!(last.entries, third.entries);
    assert_eq!(last.through, tail.through);
    assert!(nodes[0].accepted(10, tail.through + 1, 1).is_err());
    assert!(nodes[0].accepted(10, 0, 0).is_err());
    assert!(nodes[0].accepted(10, 0, 129).is_err());
    Ok(())
}
