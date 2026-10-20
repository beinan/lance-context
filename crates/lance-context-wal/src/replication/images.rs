use super::*;
use checkpoint::{Cut, Image, Loaded, Part};
use raft_proto::eraftpb::Snapshot;
use std::{
    path::PathBuf,
    sync::atomic::{AtomicBool, Ordering},
};

pub(super) struct Owner {
    directory: PathBuf,
    busy: AtomicBool,
}
impl Owner {
    pub fn new(directory: &Path) -> Result<Arc<Self>> {
        Ok(Arc::new(Self {
            directory: directory.canonicalize()?,
            busy: AtomicBool::new(false),
        }))
    }
    fn acquire(self: &Arc<Self>) -> Result<Permit> {
        self.busy
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .map_err(|_| anyhow::anyhow!("node image builder/receiver busy"))?;
        Ok(Permit(self.clone()))
    }
}
struct Permit(Arc<Owner>);
impl Drop for Permit {
    fn drop(&mut self) {
        self.0.busy.store(false, Ordering::Release);
    }
}

/// Captures a committed application cut without copying payload bodies. Run it
/// on a bounded blocking worker; the node may keep polling and appending. One
/// build/receive permit spans every group sharing this physical node. It also
/// prevents another checkpoint from compacting this task's captured log input.
pub struct CheckpointTask {
    permit: Permit,
    store: GroupLog,
    previous: Option<Arc<Image>>,
    layout: Arc<Layout>,
    limits: Limits,
    cut: Cut,
    state: State,
}
impl CheckpointTask {
    pub fn run(self) -> Result<PreparedCheckpoint> {
        let Self {
            permit,
            store,
            previous,
            layout,
            limits,
            cut,
            state,
        } = self;
        let group = cut.group;
        let loaded = checkpoint::build(
            &permit.0.directory,
            &layout,
            &limits,
            cut,
            state,
            |index, meta| read_body(&store, previous.as_deref(), index, meta),
        )?;
        Ok(PreparedCheckpoint {
            permit,
            group,
            loaded,
            received: None,
        })
    }
}

/// Complete, validated and synced files. This value cannot be constructed from
/// unverified descriptors. Holding it reserves the node's image worker slot;
/// dropping it abandons the candidate and retains its files as orphans.
pub struct PreparedCheckpoint {
    permit: Permit,
    group: u64,
    loaded: Loaded,
    received: Option<Packet>,
}
impl PreparedCheckpoint {
    pub fn image(&self) -> Arc<Image> {
        self.loaded.image.clone()
    }
}

/// Bounded transfer state. Socket I/O stays outside the Raft owner; move this
/// object to the admitted worker and feed exact, sequential file chunks.
pub struct SnapshotReceiver {
    permit: Permit,
    packet: Packet,
    receiver: checkpoint::Receiver,
}
impl SnapshotReceiver {
    pub fn write(&mut self, part: Part, offset: u64, bytes: &[u8]) -> Result<()> {
        self.receiver.write(part, offset, bytes)
    }
    pub fn finish(self) -> Result<PreparedCheckpoint> {
        Ok(PreparedCheckpoint {
            permit: self.permit,
            group: self.packet.group,
            loaded: self.receiver.finish()?,
            received: Some(self.packet),
        })
    }
}

pub(super) fn read_body(
    store: &GroupLog,
    image: Option<&Image>,
    index: u64,
    meta: &BatchMeta,
) -> Result<bytes::Bytes> {
    if let Some(image) = image {
        if let Some(body) = image.read_for_validation(index)? {
            return Ok(body);
        }
    }
    let mut entries = store.entries(
        index,
        index + 1,
        None,
        raft::GetEntriesContext::empty(false),
    )?;
    let entry = entries.pop().context("missing application image input")?;
    ensure!(entry.index == index, "image input index mismatch");
    ensure!(
        &state::decode_meta(&entry.context)? == meta,
        "accepted metadata mismatch"
    );
    Ok(entry.data)
}

impl Node {
    pub fn prepare_checkpoint(&self, group: u64, id: Uuid) -> Result<CheckpointTask> {
        self.healthy()?;
        ensure!(!id.is_nil(), "nil application image id");
        let g = self.groups.get(&group).context("unhosted group")?;
        let cut = Cut {
            group,
            id,
            index: g.state.applied,
            term: g.raw.store().term(g.state.applied)?,
        };
        ensure!(
            cut.index >= g.raw.store().first_index()?,
            "no new committed state to checkpoint"
        );
        Ok(CheckpointTask {
            permit: self.images.acquire()?,
            store: g.raw.store().clone(),
            previous: g.image.clone(),
            layout: self.layout.clone(),
            limits: self.limits.clone(),
            cut,
            state: g.state.clone(),
        })
    }

    /// Install a completed local build. Inputs after the captured cut remain in
    /// the log. The full image is now available for a lagging follower's transfer;
    /// callers must service MsgSnapshot before enabling periodic compaction.
    /// This logically compacts only; it does not bound retained disk space.
    pub fn install_checkpoint(&mut self, prepared: PreparedCheckpoint) -> Result<()> {
        self.healthy()?;
        ensure!(
            Arc::ptr_eq(&prepared.permit.0, &self.images),
            "foreign node image worker"
        );
        ensure!(
            prepared.received.is_none(),
            "received image needs step_snapshot"
        );
        let g = self
            .groups
            .get_mut(&prepared.group)
            .context("unhosted group")?;
        let snapshot = prepared.loaded.image.snapshot()?;
        ensure!(
            prepared.loaded.state.applied <= g.state.applied,
            "checkpoint exceeds applied state"
        );
        if let Err(error) = g.raw.store().checkpoint(&snapshot) {
            self.failed = true;
            return Err(error);
        }
        g.image = Some(prepared.loaded.image);
        Ok(())
    }

    /// Capture immediately after poll, before installing another checkpoint.
    /// Keep the returned Arc until the outgoing transfer finishes. A transport
    /// can discard an obsolete message and report failure so Raft retries.
    pub fn snapshot_image(&self, group: u64, snapshot: &Snapshot) -> Result<Arc<Image>> {
        self.healthy()?;
        let image = self
            .groups
            .get(&group)
            .context("unhosted group")?
            .image
            .as_ref()
            .context("no installed application image")?;
        ensure!(
            image.snapshot()? == *snapshot,
            "outgoing snapshot is obsolete"
        );
        Ok(image.clone())
    }

    pub fn receive_snapshot(&self, packet: Packet, local_id: Uuid) -> Result<SnapshotReceiver> {
        self.healthy()?;
        self.validate_packet(&packet)?;
        ensure!(
            packet.message.get_msg_type() == MessageType::MsgSnapshot
                && packet.message.entries.is_empty()
                && packet.message.has_snapshot(),
            "expected snapshot-only message"
        );
        let permit = self.images.acquire()?;
        let receiver = checkpoint::Receiver::new(
            &self.images.directory,
            packet.message.get_snapshot(),
            self.layout.clone(),
            self.limits.clone(),
            packet.group,
            local_id,
        )?;
        Ok(SnapshotReceiver {
            permit,
            packet,
            receiver,
        })
    }

    /// Only a complete image can reach RawNode. It may now be stale and ignored;
    /// otherwise poll persists the received snapshot, HardState and new suffix
    /// atomically before releasing the follower response. Keep polling during
    /// download; no Ready is retained across a network or file wait.
    pub fn step_snapshot(&mut self, mut prepared: PreparedCheckpoint) -> Result<()> {
        self.healthy()?;
        ensure!(
            Arc::ptr_eq(&prepared.permit.0, &self.images),
            "foreign node image worker"
        );
        let mut packet = prepared
            .received
            .take()
            .context("local image needs install_checkpoint")?;
        self.validate_packet(&packet)?;
        let snapshot = prepared.loaded.image.snapshot()?;
        packet.message.set_snapshot(snapshot.clone());
        let g = self
            .groups
            .get_mut(&prepared.group)
            .context("unhosted group")?;
        ensure!(
            g.incoming.is_none(),
            "poll the previous received snapshot first"
        );
        g.raw.step(packet.message)?;
        if g.raw.raft.raft_log.unstable.snapshot.as_ref() == Some(&snapshot) {
            g.incoming = Some(prepared);
        }
        Ok(())
    }

    pub fn report_snapshot(&mut self, group: u64, to: u64, delivered: bool) -> Result<()> {
        self.healthy()?;
        ensure!(
            to != self.id
                && self
                    .layout
                    .groups
                    .get(&group)
                    .context("unknown group")?
                    .contains(&to),
            "invalid snapshot destination"
        );
        self.groups
            .get_mut(&group)
            .context("unhosted group")?
            .raw
            .report_snapshot(
                to,
                if delivered {
                    raft::SnapshotStatus::Finish
                } else {
                    raft::SnapshotStatus::Failure
                },
            );
        Ok(())
    }
}

pub(super) fn install_received(
    g: &mut Group,
    ready: &raft::Ready,
    id: u64,
    events: &mut Events,
) -> Result<()> {
    let prepared = g
        .incoming
        .take()
        .context("received Raft snapshot has no prepared application image")?;
    ensure!(
        prepared.loaded.image.snapshot()? == *ready.snapshot(),
        "prepared image differs from Ready"
    );
    g.raw.store().install_snapshot(
        ready.snapshot(),
        ready.entries(),
        ready.hs().context("received snapshot has no HardState")?,
    )?;
    g.state = prepared.loaded.state;
    g.image = Some(prepared.loaded.image);
    for (_, (request, _)) in std::mem::take(&mut g.pending) {
        events.completions.push(Completion {
            group: id,
            request,
            outcome: Outcome::Unknown,
        });
    }
    Ok(())
}
