//! Test-only filesystem adapter: real files with a controlled sync error. No
//! fault adapter or injection flag is included in ordinary library/binary builds.
use std::io;
use std::path::Path;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{mpsc, Arc, Mutex};
use std::time::Duration;

use raft_engine::env::{DefaultFileSystem, FileSystem, Handle, Permission};

use super::*;

type FileHandle = <DefaultFileSystem as FileSystem>::Handle;

#[derive(Default)]
pub(super) struct FaultControl {
    fail_sync: AtomicBool,
    sync_calls: AtomicU64,
    entered: Mutex<Option<mpsc::SyncSender<()>>>,
    release: Mutex<Option<mpsc::Receiver<()>>>,
}

pub(super) struct FaultHandle {
    inner: Arc<FileHandle>,
    control: Arc<FaultControl>,
}

impl Handle for FaultHandle {
    fn truncate(&self, offset: usize) -> io::Result<()> {
        self.inner.truncate(offset)
    }

    fn file_size(&self) -> io::Result<usize> {
        self.inner.file_size()
    }

    fn sync(&self) -> io::Result<()> {
        self.control.sync_calls.fetch_add(1, Ordering::Relaxed);
        if self.control.fail_sync.load(Ordering::Acquire) {
            if let Some(entered) = self.control.entered.lock().unwrap().take() {
                entered.send(()).unwrap();
            }
            if let Some(release) = self.control.release.lock().unwrap().take() {
                release.recv_timeout(Duration::from_secs(10)).unwrap();
            }
            return Err(io::Error::other("injected file sync failure"));
        }
        self.inner.sync()
    }
}

#[test]
fn concurrent_appends_share_one_real_sync() -> Result<()> {
    let layout = Layout {
        cluster: uuid::Uuid::from_u128(2),
        nodes: (1..=3)
            .map(|id| (id, format!("wal-{id}.example.invalid:7800")))
            .collect(),
        groups: BTreeMap::from([(10, vec![1, 2, 3]), (20, vec![1, 2, 3])]),
        buckets: vec![10, 20],
    };
    let directory = tempfile::tempdir()?;
    let log = PhysicalLog::open(&directory.path().join("log"), &layout, 1)?;
    let node = log.group(10)?.node.clone();
    let before = node.fault.sync_calls.load(Ordering::Relaxed);
    let gate = node.sync_gate.lock().unwrap();
    let writers: Vec<_> = [10, 20]
        .into_iter()
        .map(|id| {
            let group = log.group(id).unwrap();
            std::thread::spawn(move || {
                group.persist(
                    &[Entry {
                        index: 1,
                        term: 1,
                        data: vec![id as u8; 4096].into(),
                        ..Default::default()
                    }],
                    Some(&HardState {
                        term: 1,
                        vote: 1,
                        commit: 1,
                        ..Default::default()
                    }),
                )
            })
        })
        .collect();
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    while node.engine.last_index(10) != Some(1) || node.engine.last_index(20) != Some(1) {
        assert!(
            std::time::Instant::now() < deadline,
            "both appends must reach the real engine before sync"
        );
        std::thread::sleep(Duration::from_millis(1));
    }
    assert_eq!(node.fault.sync_calls.load(Ordering::Relaxed), before);
    drop(gate);
    for writer in writers {
        writer.join().unwrap()?;
    }
    assert_eq!(node.fault.sync_calls.load(Ordering::Relaxed) - before, 1);
    for id in [10, 20] {
        let group = log.group(id)?;
        assert_eq!(group.initial_state()?.hard_state.commit, 1);
        assert_eq!(
            group.entries(1, 2, None, GetEntriesContext::empty(false))?[0]
                .data
                .as_ref(),
            vec![id as u8; 4096]
        );
    }
    // An append submitted after that barrier needs a new physical sync.
    log.group(10)?.persist(
        &[],
        Some(&HardState {
            term: 2,
            vote: 2,
            commit: 1,
            ..Default::default()
        }),
    )?;
    assert_eq!(node.fault.sync_calls.load(Ordering::Relaxed) - before, 2);
    Ok(())
}

#[derive(Default)]
pub(super) struct FaultFileSystem {
    pub(super) control: Arc<FaultControl>,
}

impl FaultFileSystem {
    fn wrap(&self, inner: FileHandle) -> FaultHandle {
        FaultHandle {
            inner: Arc::new(inner),
            control: self.control.clone(),
        }
    }
}

impl FileSystem for FaultFileSystem {
    type Handle = FaultHandle;
    type Reader = <DefaultFileSystem as FileSystem>::Reader;
    type Writer = <DefaultFileSystem as FileSystem>::Writer;

    fn create<P: AsRef<Path>>(&self, path: P) -> io::Result<Self::Handle> {
        Ok(self.wrap(DefaultFileSystem.create(path)?))
    }

    fn open<P: AsRef<Path>>(&self, path: P, permission: Permission) -> io::Result<Self::Handle> {
        Ok(self.wrap(DefaultFileSystem.open(path, permission)?))
    }

    fn delete<P: AsRef<Path>>(&self, path: P) -> io::Result<()> {
        DefaultFileSystem.delete(path)
    }

    fn rename<P: AsRef<Path>>(&self, source: P, destination: P) -> io::Result<()> {
        DefaultFileSystem.rename(source, destination)
    }

    fn new_reader(&self, handle: Arc<Self::Handle>) -> io::Result<Self::Reader> {
        DefaultFileSystem.new_reader(handle.inner.clone())
    }

    fn new_writer(&self, handle: Arc<Self::Handle>) -> io::Result<Self::Writer> {
        DefaultFileSystem.new_writer(handle.inner.clone())
    }
}

#[test]
fn sync_panic_cannot_ack_another_group() -> Result<()> {
    sync_failure_cannot_ack_another_group(64 * 1024 * 1024)
}

#[test]
fn rotation_sync_panic_cannot_ack_another_group() -> Result<()> {
    // Rotation also calls Handle::sync, even for an engine write(sync=false).
    // Force rotation at every append to exercise that different panic boundary.
    sync_failure_cannot_ack_another_group(1)
}

fn sync_failure_cannot_ack_another_group(file_size: u64) -> Result<()> {
    let layout = Layout {
        cluster: uuid::Uuid::from_u128(1),
        nodes: (1..=3)
            .map(|id| (id, format!("wal-{id}.example.invalid:7800")))
            .collect(),
        groups: BTreeMap::from([(10, vec![1, 2, 3]), (20, vec![1, 2, 3])]),
        buckets: vec![10, 20],
    };
    let directory = tempfile::tempdir()?;
    let path = directory.path().join("log");
    let log = PhysicalLog::open_with_file_size(&path, &layout, 1, file_size)?;
    let first = log.group(10)?;
    let second = log.group(20)?;
    let prefix = Entry {
        index: 1,
        term: 1,
        data: vec![7; 4096].into(),
        ..Default::default()
    };
    first.persist(
        std::slice::from_ref(&prefix),
        Some(&HardState {
            term: 1,
            vote: 1,
            commit: 1,
            ..Default::default()
        }),
    )?;

    let control = first.node.fault.clone();
    let (entered_tx, entered_rx) = mpsc::sync_channel(1);
    let (release_tx, release_rx) = mpsc::sync_channel(1);
    *control.entered.lock().unwrap() = Some(entered_tx);
    *control.release.lock().unwrap() = Some(release_rx);
    control.fail_sync.store(true, Ordering::Release);
    let writer = first.clone();
    let a = std::thread::spawn(move || {
        writer.persist(
            &[Entry {
                index: 2,
                term: 1,
                data: vec![8; 4096].into(),
                ..Default::default()
            }],
            Some(&HardState {
                term: 1,
                vote: 1,
                commit: 2,
                ..Default::default()
            }),
        )
    });
    entered_rx.recv_timeout(Duration::from_secs(10))?;
    let (finished_tx, finished_rx) = mpsc::sync_channel(1);
    let (started_tx, started_rx) = mpsc::sync_channel(1);
    let writer = second.clone();
    let b = std::thread::spawn(move || {
        started_tx.send(()).unwrap();
        let result = writer.persist(
            &[],
            Some(&HardState {
                term: 1,
                vote: 1,
                commit: 0,
                ..Default::default()
            }),
        );
        finished_tx.send(result).unwrap();
    });
    started_rx.recv_timeout(Duration::from_secs(10))?;
    assert!(finished_rx.recv_timeout(Duration::from_millis(50)).is_err());
    release_tx.send(())?;
    assert!(
        a.join().unwrap().is_err(),
        "sync panic must not become a successful persist"
    );
    assert!(finished_rx.recv_timeout(Duration::from_secs(10))?.is_err());
    b.join().unwrap();
    assert!(first.initial_state().is_err());
    assert!(second.initial_state().is_err());
    assert!(first.persist(&[], None).is_err());
    control.fail_sync.store(false, Ordering::Release);
    drop((first, second, log));
    let reopened = PhysicalLog::open(&path, &layout, 1)?;
    let group = reopened.group(10)?;
    assert!(group.initial_state()?.hard_state.commit >= 1);
    assert_eq!(
        group.entries(1, 2, None, GetEntriesContext::empty(false))?,
        vec![prefix]
    );
    assert_eq!(reopened.group(20)?.initial_state()?.hard_state.term, 0);
    Ok(())
}
