#![cfg(feature = "raft-log")]

//! Independent physical logs must never reuse another engine's cached payloads.
use std::collections::BTreeMap;

use anyhow::{ensure, Result};
use lance_context_wal::placement::Layout;
use lance_context_wal::raft_log::{GroupLog, PhysicalLog};
use raft::{GetEntriesContext, Storage};
use raft_proto::eraftpb::{Entry, HardState};
use uuid::Uuid;

fn layout() -> Layout {
    Layout {
        cluster: Uuid::from_u128(1),
        nodes: (1..=3)
            .map(|id| (id, format!("wal-{id}.example.invalid:7800")))
            .collect(),
        groups: BTreeMap::from([(10, vec![1, 2, 3])]),
        buckets: vec![10],
    }
}

fn append(group: &GroupLog, byte: u8) -> Result<Vec<Entry>> {
    // Identical serialized sizes and write ordering make block handles collide
    // across independent engines. The full payload must identify its owner.
    let entries: Vec<_> = (1..=3)
        .map(|index| Entry {
            index,
            term: 1,
            data: vec![byte; 4096].into(),
            ..Default::default()
        })
        .collect();
    group.persist(
        &entries,
        Some(&HardState {
            term: 1,
            vote: 1,
            commit: 3,
            ..Default::default()
        }),
    )?;
    Ok(entries)
}

fn read(group: &GroupLog) -> Result<Vec<Entry>> {
    Ok(group.entries(1, 4, None, GetEntriesContext::empty(false))?)
}

fn verify(group: &GroupLog, expected: &[Entry]) -> Result<()> {
    let actual = read(group)?;
    ensure!(actual.len() == expected.len(), "unexpected entry count");
    for (actual, expected) in actual.iter().zip(expected) {
        ensure!(
            actual.index == expected.index && actual.term == expected.term,
            "unexpected entry index or term"
        );
        ensure!(
            actual.data == expected.data,
            "wrong physical log payload at index {}: expected byte {:?}, actual byte {:?}",
            actual.index,
            expected.data.first(),
            actual.data.first()
        );
        ensure!(actual == expected, "unexpected entry metadata");
    }
    Ok(())
}

#[test]
fn independent_physical_logs_return_their_own_payloads_on_one_thread() -> Result<()> {
    let directory = tempfile::tempdir()?;
    let a = PhysicalLog::open(&directory.path().join("a"), &layout(), 1)?;
    let b = PhysicalLog::open(&directory.path().join("b"), &layout(), 2)?;
    let group_a = a.group(10)?;
    let group_b = b.group(10)?;
    let expected_a = append(&group_a, 41)?;
    let expected_b = append(&group_b, 42)?;

    // Fresh-thread controls establish that each actual file contains its own
    // data before exercising alternating cache ownership on the caller thread.
    for (group, expected) in [
        (group_a.clone(), expected_a.clone()),
        (group_b.clone(), expected_b.clone()),
    ] {
        std::thread::spawn(move || -> Result<()> { verify(&group, &expected) })
            .join()
            .expect("control reader panicked")?;
    }
    for _ in 0..3 {
        verify(&group_a, &expected_a)?;
        verify(&group_b, &expected_b)?;
    }
    Ok(())
}

#[test]
fn dropping_every_handle_does_not_alias_a_later_physical_log() -> Result<()> {
    let directory = tempfile::tempdir()?;
    {
        let log = PhysicalLog::open(&directory.path().join("a"), &layout(), 1)?;
        let group = log.group(10)?;
        let expected = append(&group, 51)?;
        verify(&group, &expected)?;
    }
    let log = PhysicalLog::open(&directory.path().join("b"), &layout(), 1)?;
    let group = log.group(10)?;
    let expected = append(&group, 52)?;
    verify(&group, &expected)?;
    Ok(())
}

#[test]
fn a_recreated_directory_does_not_reuse_the_prior_cache_identity() -> Result<()> {
    let directory = tempfile::tempdir()?;
    let path = directory.path().join("log");
    {
        let log = PhysicalLog::open(&path, &layout(), 1)?;
        let group = log.group(10)?;
        let expected = append(&group, 61)?;
        verify(&group, &expected)?;
    }
    // Only this test's TempDir is removed. Reuse of the exact pathname rules out
    // a path-only cache key as a sufficient dependency correction.
    std::fs::remove_dir_all(&path)?;
    let log = PhysicalLog::open(&path, &layout(), 1)?;
    let group = log.group(10)?;
    let expected = append(&group, 62)?;
    verify(&group, &expected)?;
    Ok(())
}
