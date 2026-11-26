use super::*;

fn layout() -> Arc<Layout> {
    crate::replication::tests::layout()
}
fn limits() -> Limits {
    Limits {
        producer_slots: 1,
        ..Default::default()
    }
}
fn command(action: Action) -> Command {
    let layout = layout();
    Command {
        cluster: layout.cluster,
        layout: layout.fingerprint().unwrap(),
        group: 10,
        table: Uuid::from_u128(42),
        slot: 0,
        action,
    }
}
fn open(expected_generation: u64) -> Command {
    command(Action::Open {
        expected_generation,
        operation: Uuid::from_u128(100 + expected_generation as u128),
        bucket: 0,
    })
}
fn apply(state: &mut State, batch: Batch) -> Result<Applied> {
    let entry = Entry {
        index: state.applied + 1,
        term: 1,
        data: batch.body,
        context: state::encode_meta(&batch.meta)?.into(),
        ..Default::default()
    };
    state::validate_entry(&layout(), 10, &limits(), &entry, true)?;
    Ok(state.apply(&entry, &limits())?.unwrap())
}
fn control(state: &mut State, command: Command) -> Result<Reply> {
    let Applied::Producer(reply) = apply(state, command.batch(&layout(), &limits())?)? else {
        anyhow::bail!("missing producer outcome")
    };
    Ok(reply)
}
fn data(generation: u64, sequence: u64) -> Batch {
    Batch::new(
        Uuid::from_u128(42),
        0,
        producer_id(0, generation),
        sequence,
        Bytes::from(vec![sequence as u8; 128]),
    )
}

#[test]
fn close_requires_exact_tail_and_reuse_requires_retirement() -> Result<()> {
    let mut state = State::with_producer_slots(1);
    assert!(matches!(
        control(&mut state, open(0))?,
        Reply::Opened { generation: 1, .. }
    ));
    let first = data(1, 1);
    let Applied::Accepted { index } = apply(&mut state, first.clone())? else {
        panic!("not accepted")
    };
    let last = Last { sequence: 1, index };
    assert_eq!(
        control(
            &mut state,
            command(Action::Close {
                generation: 1,
                expected_last: None
            })
        )?,
        Reply::LastChanged { actual: Some(last) }
    );
    assert_eq!(control(&mut state, open(1))?, Reply::Busy);
    assert_eq!(
        control(
            &mut state,
            command(Action::Close {
                generation: 1,
                expected_last: Some(last)
            })
        )?,
        Reply::Closed { last: Some(last) }
    );
    assert_eq!(
        apply(&mut state, first.clone())?,
        Applied::Accepted { index }
    );
    assert_eq!(apply(&mut state, data(1, 2))?, Applied::ProducerFenced);
    assert_eq!(control(&mut state, open(1))?, Reply::Busy);
    // State-machine boundary test; the real Lance/process fixture separately
    // exercises the signed publication/closure transition that creates this tail.
    let key = (first.meta.stream, first.meta.bucket, first.meta.producer);
    state.accepted.remove(&index);
    state
        .retired
        .insert(key, state::Retired { sequence: 1, index });
    assert!(state.producer_view(0).unwrap().reusable);
    assert!(matches!(
        control(&mut state, open(1))?,
        Reply::Opened { generation: 2, .. }
    ));
    assert!(state.producers.is_empty() && state.retired.is_empty());
    assert_eq!(apply(&mut state, first)?, Applied::ProducerFenced);
    assert_eq!(control(&mut state, open(0))?, Reply::Fenced);
    assert_eq!(
        control(&mut state, command(Action::Revoke { generation: 1 }))?,
        Reply::Fenced
    );
    assert!(matches!(
        apply(&mut state, data(2, 1))?,
        Applied::Accepted { .. }
    ));
    Ok(())
}

#[test]
fn allocation_retries_revocation_scope_and_exhaustion_are_explicit() -> Result<()> {
    let mut state = State::with_producer_slots(1);
    let opened = control(&mut state, open(0))?;
    assert_eq!(control(&mut state, open(0))?, opened);
    let mut competing = open(0);
    if let Action::Open { operation, .. } = &mut competing.action {
        *operation = Uuid::new_v4();
    }
    assert_eq!(control(&mut state, competing)?, Reply::Fenced);
    let mut wrong = command(Action::Revoke { generation: 1 });
    wrong.table = Uuid::new_v4();
    assert_eq!(control(&mut state, wrong)?, Reply::Fenced);
    assert_eq!(
        control(&mut state, command(Action::Revoke { generation: 1 }))?,
        Reply::Closed { last: None }
    );
    assert!(matches!(
        control(&mut state, open(0))?,
        Reply::Opened { closed: true, .. }
    ));
    assert!(matches!(
        control(&mut state, open(1))?,
        Reply::Opened { generation: 2, .. }
    ));
    let mut wrong = data(2, 1);
    wrong.meta.stream = Uuid::new_v4();
    assert_eq!(apply(&mut state, wrong)?, Applied::ProducerFenced);
    state.producer_slots[0].generation = u64::MAX;
    state.producer_slots[0].lease.as_mut().unwrap().closed = true;
    assert_eq!(control(&mut state, open(u64::MAX))?, Reply::Exhausted);
    assert_eq!(state.producer_slots[0].generation, u64::MAX);
    assert!(!state.producer_view(0).unwrap().reusable);
    Ok(())
}

#[test]
fn checkpoint_preserves_slot_generation_without_receipt_history() -> Result<()> {
    let directory = tempfile::tempdir()?;
    let limits = limits();
    let layout = layout();
    let mut state = State::with_producer_slots(1);
    for generation in 0..40 {
        assert!(matches!(
            control(&mut state, open(generation))?,
            Reply::Opened { .. }
        ));
        control(
            &mut state,
            command(Action::Revoke {
                generation: generation + 1,
            }),
        )?;
    }
    state.validate_checkpoint(&layout, 10, &limits, state.applied)?;
    let built = checkpoint::build(
        directory.path(),
        &layout,
        &limits,
        checkpoint::Cut {
            group: 10,
            index: state.applied,
            term: 1,
            id: Uuid::new_v4(),
        },
        state.clone(),
        |_, _| anyhow::bail!("no retained payload"),
    )?;
    let snapshot = built.image.snapshot()?;
    let restored = checkpoint::load(directory.path(), &snapshot, &layout, &limits, 10)?;
    assert_eq!(restored.state, state);
    assert_eq!(restored.image.bytes(checkpoint::Part::Payload), 0);
    assert!(restored.image.bytes(checkpoint::Part::Metadata) < 1024);
    assert!(
        checkpoint::load(directory.path(), &snapshot, &layout, &Limits::default(), 10).is_err()
    );
    let mut restored = restored.state;
    assert_eq!(control(&mut restored, open(0))?, Reply::Fenced);
    assert!(matches!(
        control(&mut restored, open(40))?,
        Reply::Opened { generation: 41, .. }
    ));
    Ok(())
}

#[test]
fn command_and_checkpoint_cell_decoders_reject_unbounded_or_foreign_input() -> Result<()> {
    let batch = open(0).batch(&layout(), &limits())?;
    for length in 0..batch.body.len() {
        assert!(decode(&batch.body[..length]).is_err());
    }
    let mut trailing = batch.body.to_vec();
    trailing.push(0);
    assert!(decode(&trailing).is_err());
    assert!(decode(&vec![0; MAX_COMMAND_BYTES + 1]).is_err());
    for mutate in [0, 1, 2, 3] {
        let mut request = open(0);
        match mutate {
            0 => request.cluster = Uuid::new_v4(),
            1 => request.layout[0] ^= 1,
            2 => request.group = 20,
            _ => request.slot = 1,
        }
        assert!(request.batch(&layout(), &limits()).is_err());
    }
    assert!(open(0).batch(&layout(), &Limits::default()).is_err());
    let cell = Cell {
        generation: 1,
        lease: Some(Lease {
            table: Uuid::new_v4(),
            bucket: 0,
            operation: Uuid::new_v4(),
            closed: true,
        }),
    };
    let encoded = encode_cell(&cell)?;
    assert!(encoded.len() <= MAX_CELL_BYTES);
    for length in 0..encoded.len() {
        assert!(decode_cell(&encoded[..length]).is_err());
    }
    assert_eq!(decode_cell(&encoded)?, cell);
    let mut bad = State::with_producer_slots(1);
    bad.producer_slots[0] = cell;
    bad.producer_slots[0].generation = 0;
    assert!(bad.validate_producers(&layout(), 10, &limits()).is_err());
    assert!(Limits {
        producer_slots: MAX_SLOTS + 1,
        ..Default::default()
    }
    .identity()
    .is_err());
    assert!(Limits {
        producer_slots: 2,
        max_producers: 1,
        ..Default::default()
    }
    .identity()
    .is_err());
    Ok(())
}

#[test]
fn zero_slot_policy_keeps_v10_bytes_and_enabled_policy_cannot_reopen_legacy() -> Result<()> {
    // Independent pre-slot struct: an accidental new field in the old bincode
    // identity would invalidate existing journals and signed closure permits.
    #[derive(Serialize)]
    struct OldLimits {
        catalog_key: Option<[u8; 32]>,
        source_keys: BTreeMap<u64, [u8; 32]>,
        max_tables: u64,
        max_active_plans: u64,
        max_plan_source_bytes: u64,
        max_descriptor_bytes: u64,
        max_manifest_records: u64,
        max_record_bytes: u64,
        max_records: u64,
        max_producers: u64,
        max_pending: u64,
        max_uncommitted_bytes: u64,
    }
    let limits = Limits {
        catalog_key: Some([1; 32]),
        source_keys: BTreeMap::from([(10, [2; 32])]),
        ..Default::default()
    };
    let old = OldLimits {
        catalog_key: limits.catalog_key,
        source_keys: limits.source_keys.clone(),
        max_tables: limits.max_tables,
        max_active_plans: limits.max_active_plans,
        max_plan_source_bytes: limits.max_plan_source_bytes,
        max_descriptor_bytes: limits.max_descriptor_bytes,
        max_manifest_records: limits.max_manifest_records,
        max_record_bytes: limits.max_record_bytes,
        max_records: limits.max_records,
        max_producers: limits.max_producers,
        max_pending: limits.max_pending,
        max_uncommitted_bytes: limits.max_uncommitted_bytes,
    };
    assert_eq!(
        limits.identity()?,
        bincode::serialize(&("lance-context/replicated-append/v10", old))?
    );
    let directory = tempfile::tempdir()?;
    drop(Node::open(
        directory.path(),
        layout(),
        1,
        Limits::default(),
    )?);
    assert!(Node::open(directory.path(), layout(), 1, super::tests::limits()).is_err());
    drop(Node::open(
        directory.path(),
        layout(),
        1,
        Limits::default(),
    )?);
    Ok(())
}
