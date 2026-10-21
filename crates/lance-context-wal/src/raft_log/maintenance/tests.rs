use super::*;
use std::{fs, path::PathBuf};

fn layout() -> Layout {
    Layout {
        cluster: uuid::Uuid::from_u128(1200),
        nodes: (1..=3)
            .map(|id| (id, format!("wal-{id}.example.invalid:7800")))
            .collect(),
        groups: BTreeMap::from([(10, vec![1, 2, 3]), (20, vec![1, 2, 3])]),
        buckets: vec![10, 20],
    }
}

fn entry(group: u64, index: u64) -> Entry {
    Entry {
        index,
        term: 1,
        data: vec![(group ^ index) as u8; 16 * 1024].into(),
        ..Default::default()
    }
}

fn append(group: &GroupLog, first: u64, last: u64) -> Result<()> {
    group.persist(
        &(first..=last)
            .map(|index| entry(group.group, index))
            .collect::<Vec<_>>(),
        Some(&HardState {
            term: 1,
            vote: 1,
            commit: last,
            ..Default::default()
        }),
    )
}

fn checkpoint(group: &GroupLog, index: u64) -> Result<Snapshot> {
    let mut snapshot = Snapshot::default();
    snapshot.mut_metadata().index = index;
    snapshot.mut_metadata().term = 1;
    snapshot
        .mut_metadata()
        .set_conf_state(group.initial_state()?.conf_state);
    // Storage-level tests use an opaque descriptor. End-to-end tests must first
    // materialize and sync the actual application files referenced by a cut.
    snapshot.data = format!("maintenance-test/{index}").into_bytes().into();
    group.checkpoint(&snapshot)?;
    Ok(snapshot)
}

fn files(directory: &Path) -> Result<BTreeMap<PathBuf, u64>> {
    let mut found = BTreeMap::new();
    for entry in fs::read_dir(directory)? {
        let entry = entry?;
        if entry
            .path()
            .extension()
            .is_some_and(|extension| extension == "raftlog" || extension == "rewrite")
        {
            found.insert(entry.path(), entry.metadata()?.len());
        }
    }
    Ok(found)
}

fn verify(group: &GroupLog, first: u64, last: u64) -> Result<()> {
    assert_eq!(group.first_index()?, first);
    assert_eq!(group.last_index()?, last);
    assert_eq!(group.initial_state()?.hard_state.commit, last);
    assert_eq!(
        group.entries(first, last + 1, None, GetEntriesContext::empty(false))?,
        (first..=last)
            .map(|index| entry(group.group, index))
            .collect::<Vec<_>>()
    );
    Ok(())
}

#[test]
fn repeated_physical_collection_preserves_suffix_sibling_and_recovery() -> Result<()> {
    let root = tempfile::tempdir()?;
    let directory = root.path().join("log");
    let layout = layout();
    let log = PhysicalLog::open_with_file_size(&directory, &layout, 1, 4096)?;
    let group = log.group(10)?;
    let sibling = log.group(20)?;
    group.bind_application(b"maintenance-test/v1")?;
    append(&sibling, 1, 4)?;
    let maintenance = group.maintenance();
    let unrelated = directory.join("context-application-unrelated.pack");
    fs::write(&unrelated, b"not a native engine segment")?;
    let mut removed = BTreeSet::new();
    let mut snapshot = Snapshot::default();
    let mut maximum_retained_bytes = 0;
    for round in 0..16 {
        let last = (round + 1) * 32;
        append(&group, round * 32 + 1, last)?;
        snapshot = checkpoint(&group, last - 4)?;
        let before = files(&directory)?;
        for _ in 0..4 {
            assert!(maintenance.run()?.iter().all(|id| [10, 20].contains(id)));
        }
        let after = files(&directory)?;
        removed.extend(
            before
                .keys()
                .filter(|file| !after.contains_key(*file))
                .cloned(),
        );
        maximum_retained_bytes = maximum_retained_bytes.max(after.values().sum::<u64>());
        verify(&group, last - 3, last)?;
        verify(&sibling, 1, 4)?;
        assert_eq!(fs::read(&unrelated)?, b"not a native engine segment");
    }
    assert!(!removed.is_empty(), "no physical file was reclaimed");
    // 8 MiB was appended across 16 rounds. Check actual retained file lengths,
    // rather than equating a logical truncation count with physical collection.
    assert!(
        maximum_retained_bytes < 2 * 1024 * 1024,
        "physical retention grew beyond the fixture bound: {maximum_retained_bytes}"
    );
    drop((group, sibling, log));
    assert!(PhysicalLog::open(&directory, &layout, 1).is_err());
    drop(maintenance);
    let reopened = PhysicalLog::open(&directory, &layout, 1)?;
    let group = reopened.group(10)?;
    group.bind_application(b"maintenance-test/v1")?;
    assert_eq!(group.snapshot(508, 2)?, snapshot);
    verify(&group, 509, 512)?;
    verify(&reopened.group(20)?, 1, 4)?;
    Ok(())
}

#[test]
fn rewrite_sync_failure_fences_all_groups_and_recovers_retained_records() -> Result<()> {
    let root = tempfile::tempdir()?;
    let directory = root.path().join("log");
    let layout = layout();
    let log = PhysicalLog::open_with_file_size(&directory, &layout, 1, 4096)?;
    let group = log.group(10)?;
    let sibling = log.group(20)?;
    group.bind_application(b"maintenance-test/v1")?;
    append(&sibling, 1, 4)?;
    append(&group, 1, 32)?;
    let snapshot = checkpoint(&group, 28)?;
    let maintenance = group.maintenance();
    // Fail the rewrite queue's sync, after the append-queue marker barrier.
    let fault = group.node.fault.fail_rewrite_sync_for_test();
    assert!(maintenance.run().is_err());
    assert!(group.initial_state().is_err());
    assert!(sibling.initial_state().is_err());
    assert!(sibling.persist(&[], None).is_err());
    assert!(sibling
        .entries(1, 5, None, GetEntriesContext::empty(false))
        .is_err());
    assert!(maintenance.run().is_err());
    drop((fault, maintenance, group, sibling, log));
    let reopened = PhysicalLog::open(&directory, &layout, 1)?;
    let group = reopened.group(10)?;
    group.bind_application(b"maintenance-test/v1")?;
    assert_eq!(group.snapshot(28, 2)?, snapshot);
    verify(&group, 29, 32)?;
    verify(&reopened.group(20)?, 1, 4)?;
    append(&reopened.group(20)?, 5, 5)?;
    Ok(())
}
