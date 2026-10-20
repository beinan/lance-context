use super::*;
use std::fs;

fn layout() -> Arc<Layout> {
    super::super::tests::layout()
}

fn fixture() -> Result<(State, BTreeMap<u64, Bytes>)> {
    let mut state = State::default();
    let mut bodies = BTreeMap::new();
    // Blank, exact retry and rejected entries leave gaps in accepted indexes.
    for (index, sequence) in [(1, 0), (2, 1), (3, 1), (4, 3), (5, 2), (6, 0)] {
        let mut entry = Entry {
            index,
            term: 2,
            ..Default::default()
        };
        if sequence > 0 {
            let batch = super::super::tests::batch(sequence);
            entry.context = state::encode_meta(&batch.meta)?.into();
            entry.data = batch.body.clone();
            bodies.insert(index, batch.body);
        }
        state.apply(&entry, &Limits::default())?;
    }
    Ok((state, bodies))
}

fn create(directory: &Path, id: u128) -> Result<Loaded> {
    let (state, bodies) = fixture()?;
    build(
        directory,
        &layout(),
        &Limits::default(),
        Cut {
            group: 10,
            index: state.applied,
            term: 2,
            id: Uuid::from_u128(id),
        },
        state,
        |index, _| Ok(bodies[&index].clone()),
    )
}

fn copy(source: &Image, receiver: &mut Receiver) -> Result<()> {
    for part in [Part::Metadata, Part::Payload] {
        let mut offset = 0;
        while offset < source.bytes(part) {
            let bytes = source.chunk(
                part,
                offset,
                (source.bytes(part) - offset).min(997) as usize,
            )?;
            receiver.write(part, offset, &bytes)?;
            offset += bytes.len() as u64;
        }
    }
    Ok(())
}

#[test]
fn exact_state_payloads_and_streamed_transfer_without_changing_source_image() -> Result<()> {
    let source = tempfile::tempdir()?;
    let target = tempfile::tempdir()?;
    let built = create(source.path(), 101)?;
    let snapshot = built.image.snapshot()?;
    let restored = load(source.path(), &snapshot, &layout(), &Limits::default(), 10)?;
    assert_eq!(built.state, restored.state);
    assert_eq!(
        restored.state.accepted.keys().copied().collect::<Vec<_>>(),
        [2, 5]
    );
    assert_eq!(restored.state.producers.values().next().unwrap().1, 5);
    let mut receiver = Receiver::new(
        target.path(),
        &snapshot,
        layout(),
        Limits::default(),
        10,
        Uuid::from_u128(102),
    )?;
    copy(&built.image, &mut receiver)?;
    let received = receiver.finish()?;
    assert_eq!(received.state, restored.state);
    assert_ne!(received.image.descriptor.id, built.image.descriptor.id);
    assert_eq!(built.image.snapshot()?, snapshot);
    for index in [2, 5] {
        assert_eq!(
            restored.image.read_for_validation(index)?,
            received.image.read_for_validation(index)?
        );
        assert_eq!(
            received.image.read_for_validation(index)?.unwrap(),
            fixture()?.1[&index]
        );
    }
    assert!(received.image.read_for_validation(3)?.is_none());
    assert!(built.image.chunk(Part::Payload, 0, 0).is_err());
    assert!(built
        .image
        .chunk(Part::Payload, 0, CHUNK_BYTES + 1)
        .is_err());
    assert!(built.image.chunk(Part::Payload, u64::MAX, 1).is_err());
    assert!(
        create(source.path(), 101).is_err(),
        "existing immutable files overwritten"
    );
    assert_eq!(
        load(source.path(), &snapshot, &layout(), &Limits::default(), 10)?.state,
        restored.state
    );
    Ok(())
}

#[test]
fn received_images_reject_partial_corrupt_and_out_of_order_bytes() -> Result<()> {
    let source = tempfile::tempdir()?;
    let target = tempfile::tempdir()?;
    let built = create(source.path(), 111)?;
    let snapshot = built.image.snapshot()?;
    let receive = |id| {
        Receiver::new(
            target.path(),
            &snapshot,
            layout(),
            Limits::default(),
            10,
            Uuid::from_u128(id),
        )
    };
    let mut partial = receive(112)?;
    let first = built.image.chunk(Part::Metadata, 0, 8)?;
    assert!(partial.write(Part::Metadata, 1, &first).is_err());
    partial.write(Part::Metadata, 0, &first)?;
    assert!(
        partial.finish().is_err(),
        "partial state became installable"
    );
    let mut bad = receive(113)?;
    for part in [Part::Metadata, Part::Payload] {
        let mut bytes = built
            .image
            .chunk(part, 0, built.image.bytes(part) as usize)?
            .to_vec();
        if part == Part::Payload {
            bytes[0] ^= 1;
        }
        bad.write(part, 0, &bytes)?;
    }
    assert!(
        bad.finish().is_err(),
        "wrong payload hash became installable"
    );
    let mut good = receive(114)?;
    copy(&built.image, &mut good)?;
    assert_eq!(good.finish()?.state, built.state);
    assert!(
        receive(114).is_err(),
        "interrupted or existing files overwritten"
    );
    Ok(())
}

#[test]
fn installed_files_fail_closed_on_corruption_missing_parts_and_foreign_binding() -> Result<()> {
    let root = tempfile::tempdir()?;
    let built = create(root.path(), 121)?;
    let snapshot = built.image.snapshot()?;
    let policy = Limits {
        max_records: 999,
        ..Default::default()
    };
    assert!(load(root.path(), &snapshot, &layout(), &policy, 10).is_err());
    assert!(load(root.path(), &snapshot, &layout(), &Limits::default(), 20).is_err());
    let mut cut = snapshot.clone();
    cut.mut_metadata().index += 1;
    assert!(load(root.path(), &cut, &layout(), &Limits::default(), 10).is_err());
    let paths = built.image.descriptor.paths(root.path());
    let original = fs::read(&paths[1])?;
    let mut corrupt = original.clone();
    corrupt[0] ^= 1;
    fs::write(&paths[1], &corrupt)?;
    assert!(built.state.accepted[&2]
        .validate_payload(
            &built.image.read_for_validation(2)?.unwrap(),
            8 * 1024 * 1024
        )
        .is_err());
    assert!(load(root.path(), &snapshot, &layout(), &Limits::default(), 10).is_err());
    // Even a matching outer digest cannot hide a record/metadata mismatch.
    let mut descriptor = built.image.descriptor.clone();
    descriptor.payload_digest = Sha256::digest(&corrupt).into();
    assert!(load(
        root.path(),
        &descriptor.snapshot()?,
        &layout(),
        &Limits::default(),
        10
    )
    .is_err());
    fs::write(&paths[1], original)?;
    fs::remove_file(&paths[0])?;
    assert!(load(root.path(), &snapshot, &layout(), &Limits::default(), 10).is_err());
    Ok(())
}

#[test]
fn metadata_counts_frames_trailing_bytes_and_exact_producer_tails_are_validated() -> Result<()> {
    let root = tempfile::tempdir()?;
    let built = create(root.path(), 131)?;
    let path = built.image.descriptor.paths(root.path())[0].clone();
    let original = fs::read(&path)?;
    let try_bytes = |bytes: &[u8]| -> Result<()> {
        fs::write(&path, bytes)?;
        let mut descriptor = built.image.descriptor.clone();
        descriptor.metadata_bytes = bytes.len() as u64;
        descriptor.metadata_digest = Sha256::digest(bytes).into();
        assert!(load(
            root.path(),
            &descriptor.snapshot()?,
            &layout(),
            &Limits::default(),
            10
        )
        .is_err());
        Ok(())
    };
    let mut trailing = original.clone();
    trailing.push(0);
    try_bytes(&trailing)?;
    let mut frame = original.clone();
    frame[8..12].copy_from_slice(&u32::MAX.to_le_bytes());
    try_bytes(&frame)?;
    let binding_bytes = u32::from_le_bytes(original[8..12].try_into()?) as usize;
    let mut count = original.clone();
    count[12 + binding_bytes..20 + binding_bytes].copy_from_slice(&u64::MAX.to_le_bytes());
    try_bytes(&count)?;
    fs::write(&path, original)?;
    let (state, _) = fixture()?;
    for mutation in 0..4 {
        let mut invalid = state.clone();
        match mutation {
            0 => invalid.producers.clear(),
            1 => invalid.producers.values_mut().next().unwrap().1 = 2,
            2 => invalid.accepted.get_mut(&5).unwrap().sequence = 3,
            _ => {
                invalid.accepted.insert(7, invalid.accepted[&5].clone());
            }
        }
        assert!(invalid
            .validate_checkpoint(&layout(), 10, &Limits::default(), 6)
            .is_err());
    }
    Ok(())
}

#[test]
fn empty_application_cut_has_a_real_empty_pack_and_no_payload_fetch() -> Result<()> {
    let root = tempfile::tempdir()?;
    let state = State {
        applied: 1,
        ..Default::default()
    };
    let built = build(
        root.path(),
        &layout(),
        &Limits::default(),
        Cut {
            group: 10,
            index: 1,
            term: 1,
            id: Uuid::from_u128(141),
        },
        state.clone(),
        |_, _| anyhow::bail!("unexpected payload fetch"),
    )?;
    assert_eq!(built.image.bytes(Part::Payload), 0);
    assert_eq!(
        load(
            root.path(),
            &built.image.snapshot()?,
            &layout(),
            &Limits::default(),
            10
        )?
        .state,
        state
    );
    Ok(())
}
