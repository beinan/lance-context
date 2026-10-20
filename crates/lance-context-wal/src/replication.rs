//! Synchronous, transport-independent Raft ingestion for static RF3 groups.
//!
//! `propose` queues input; only `poll` can return a committed result. All outgoing
//! messages and completions follow durable Ready/LightReady persistence. Exact
//! retries enter consensus again and return the original accepted index after
//! commit. An isolated former leader cannot answer retries from local state.
//!
//! The owner schedules ticks, bounded network I/O and calls on one thread. This
//! component supports explicit application checkpoints and prepared snapshot
//! transfer. It retains bounded metadata and all image files: it is not the
//! production service, a physical retention policy, or a bounded disk allocator.
//! Message scope checks are not authentication. Do not expose it to untrusted
//! transport or enable ingestion without the remaining admission/lifecycle work.

pub mod checkpoint;
mod images;
mod state;
#[cfg(test)]
mod tests;

use std::{collections::BTreeMap, path::Path, sync::Arc};

use anyhow::{ensure, Context, Result};
use protobuf::Message as _;
use raft::{RawNode, StateRole, Storage};
use raft_proto::eraftpb::{Entry, Message, MessageType};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use uuid::Uuid;

use crate::{
    placement::Layout,
    raft_log::{GroupLog, PhysicalLog},
    record::{Batch, BatchMeta},
};
pub use images::{CheckpointTask, PreparedCheckpoint, SnapshotReceiver};
use state::State;

/// Immutable, replicated-state bounds. All peers and reopens must use the same
/// policy. These bounds do not account for queues in the caller's transport.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Limits {
    pub max_record_bytes: u64,
    pub max_records: u64,
    pub max_producers: u64,
    pub max_pending: u64,
    pub max_uncommitted_bytes: u64,
}
impl Default for Limits {
    fn default() -> Self {
        Self {
            max_record_bytes: 8 * 1024 * 1024,
            max_records: 100_000,
            max_producers: 16_384,
            max_pending: 128,
            max_uncommitted_bytes: 64 * 1024 * 1024,
        }
    }
}
impl Limits {
    fn identity(&self) -> Result<Vec<u8>> {
        ensure!(
            (1..=8 * 1024 * 1024).contains(&self.max_record_bytes)
                && (1..=500_000).contains(&self.max_records)
                && (1..=100_000).contains(&self.max_producers)
                && (1..=4096).contains(&self.max_pending)
                && self.max_uncommitted_bytes >= self.max_record_bytes.max(1024 * 1024)
                && self.max_uncommitted_bytes <= 256 * 1024 * 1024,
            "invalid replication limits"
        );
        Ok(bincode::serialize(&(
            "lance-context/replicated-append/v1",
            self,
        ))?)
    }
}

/// Transport supplies a bounded protobuf decoder. Scope checks prevent mixing
/// groups/layouts/policies; authenticated peer identity is still the caller's job.
#[derive(Clone, Debug)]
pub struct Packet {
    pub cluster: Uuid,
    pub layout: [u8; 32],
    pub policy: [u8; 32],
    pub group: u64,
    pub message: Message,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum Applied {
    /// A new append or exact latest retry. Index identifies the original append.
    Accepted {
        index: u64,
    },
    /// Gap, stale sequence or changed content at an already-used sequence.
    Rejected {
        expected_sequence: u64,
    },
    Capacity,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum Outcome {
    Committed(Applied),
    /// Leadership changed before this owner observed the result. The append may
    /// still commit; retry its exact identity and body, not a new sequence.
    Unknown,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Completion {
    pub group: u64,
    /// Caller correlation within this process; never persisted as a producer ID.
    pub request: u64,
    pub outcome: Outcome,
}

#[derive(Default)]
pub struct Events {
    pub messages: Vec<Packet>,
    pub completions: Vec<Completion>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Status {
    pub leader: u64,
    pub term: u64,
    pub committed: u64,
    pub applied: u64,
    pub accepted_records: usize,
    pub producers: usize,
    pub pending: usize,
}

/// A bounded local view captured together with its next cursor. Preserve the
/// group/layout scope; this is neither a quorum read nor proof of Lance visibility.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Page {
    pub entries: Vec<(u64, BatchMeta)>,
    /// All accepted records after the requested cursor through this position
    /// are included. May advance over rejected, duplicate or blank entries.
    pub through: u64,
}

struct Group {
    raw: RawNode<GroupLog>,
    state: State,
    // Index AND term prevents suffix replacement from completing another input.
    pending: BTreeMap<(u64, u64), (u64, BatchMeta)>,
    image: Option<Arc<checkpoint::Image>>,
    incoming: Option<PreparedCheckpoint>,
}

/// Owns all Raft groups in one shared physical engine. Do not call concurrently
/// or retain storage handles outside it. An internal/disk error fences this node;
/// drop it, reconcile outstanding requests and reopen the original directories.
pub struct Node {
    layout: Arc<Layout>,
    layout_hash: [u8; 32],
    policy: [u8; 32],
    limits: Limits,
    id: u64,
    groups: BTreeMap<u64, Group>,
    images: Arc<images::Owner>,
    failed: bool,
}

impl Node {
    /// Give each group to an independent worker while retaining the shared
    /// physical engine and its sync/failure fence. Consumes the aggregate owner;
    /// it never clones a RawNode or permits two owners of one group.
    pub fn into_groups(self) -> Vec<Self> {
        self.groups
            .into_iter()
            .map(|(id, group)| Self {
                layout: self.layout.clone(),
                layout_hash: self.layout_hash,
                policy: self.policy,
                limits: self.limits.clone(),
                id: self.id,
                groups: BTreeMap::from([(id, group)]),
                images: self.images.clone(),
                failed: self.failed,
            })
            .collect()
    }

    pub fn open(directory: &Path, layout: Arc<Layout>, id: u64, limits: Limits) -> Result<Self> {
        let layout_hash = layout.fingerprint()?;
        let identity = limits.identity()?;
        let policy = Sha256::digest(&identity).into();
        let log = PhysicalLog::open(directory, &layout, id)?;
        let images = images::Owner::new(directory)?;
        let mut groups = BTreeMap::new();
        let logger = slog::Logger::root(slog::Discard, slog::o!());
        for (group, voters) in &layout.groups {
            if !voters.contains(&id) {
                continue;
            }
            let store = log.group(*group)?;
            store.bind_application(&identity)?;
            let committed = store.initial_state()?.hard_state.commit;
            let (mut state, image) = if store.first_index()? > 1 {
                let snapshot = store.snapshot(store.first_index()? - 1, id)?;
                let loaded = checkpoint::load(directory, &snapshot, &layout, &limits, *group)?;
                (loaded.state, Some(loaded.image))
            } else {
                (State::default(), None)
            };
            while state.applied < committed {
                let entries = store.entries(
                    state.applied + 1,
                    (state.applied + 257).min(committed + 1),
                    Some(1024 * 1024),
                    raft::GetEntriesContext::empty(false),
                )?;
                ensure!(!entries.is_empty(), "missing committed recovery entries");
                for entry in entries {
                    state::validate_entry(&layout, *group, &limits, &entry, true)?;
                    state.apply(&entry, &limits)?;
                }
            }
            let config = raft::Config {
                id,
                applied: committed,
                election_tick: 10,
                heartbeat_tick: 2,
                check_quorum: true,
                pre_vote: true,
                max_size_per_msg: 1024 * 1024,
                max_inflight_msgs: 16,
                max_uncommitted_size: limits.max_uncommitted_bytes,
                max_committed_size_per_ready: 1024 * 1024,
                ..Default::default()
            };
            let raw = RawNode::new(&config, store, &logger)?;
            groups.insert(
                *group,
                Group {
                    raw,
                    state,
                    pending: BTreeMap::new(),
                    image,
                    incoming: None,
                },
            );
        }
        Ok(Self {
            layout,
            layout_hash,
            policy,
            limits,
            id,
            groups,
            images,
            failed: false,
        })
    }

    fn healthy(&self) -> Result<()> {
        ensure!(!self.failed, "replication node fenced; drop and reopen it");
        // Another group shares this engine's uncertain-write fence.
        for group in self.groups.values() {
            group.raw.store().initial_state()?;
        }
        Ok(())
    }

    pub fn tick(&mut self) -> Result<()> {
        self.healthy()?;
        for group in self.groups.values_mut() {
            group.raw.tick();
        }
        Ok(())
    }

    pub fn campaign(&mut self, group: u64) -> Result<()> {
        self.healthy()?;
        self.groups
            .get_mut(&group)
            .context("unhosted group")?
            .raw
            .campaign()?;
        Ok(())
    }

    pub fn status(&self, group: u64) -> Result<Status> {
        self.healthy()?;
        let g = self.groups.get(&group).context("unhosted group")?;
        Ok(Status {
            leader: g.raw.raft.leader_id,
            term: g.raw.raft.term,
            committed: g.raw.store().initial_state()?.hard_state.commit,
            applied: g.state.applied,
            accepted_records: g.state.accepted.len(),
            producers: g.state.producers.len(),
            pending: g.pending.len(),
        })
    }

    /// Ok means queued locally, NOT durable or committed. A token may have only
    /// one unresolved request on this node. No retry result bypasses consensus.
    pub fn propose(&mut self, group: u64, request: u64, batch: Batch) -> Result<()> {
        self.healthy()?;
        ensure!(
            !self
                .groups
                .values()
                .any(|g| g.pending.values().any(|p| p.0 == request)),
            "request token already pending"
        );
        state::validate_meta(
            &self.layout,
            group,
            &self.limits,
            &batch.meta,
            &batch.body,
            true,
        )?;
        let g = self.groups.get_mut(&group).context("unhosted group")?;
        ensure!(g.raw.raft.state == StateRole::Leader, "not leader");
        ensure!(
            g.pending.len() < self.limits.max_pending as usize,
            "pending proposal capacity"
        );
        let index = g.raw.raft.raft_log.last_index() + 1;
        let term = g.raw.raft.term;
        // RawNode::propose accepts Vec and would copy the opaque Bytes. This is
        // the equivalent raft-rs proposal path with shared entry.data ownership.
        let entry = Entry {
            context: state::encode_meta(&batch.meta)?.into(),
            data: batch.body,
            ..Default::default()
        };
        let mut message = Message::default();
        message.set_msg_type(MessageType::MsgPropose);
        message.from = self.id;
        message.set_entries(vec![entry].into());
        g.raw.raft.step(message)?;
        ensure!(
            g.raw.raft.raft_log.last_index() == index,
            "proposal was not queued"
        );
        g.pending.insert((index, term), (request, batch.meta));
        Ok(())
    }

    fn validate_packet(&self, packet: &Packet) -> Result<()> {
        ensure!(
            packet.cluster == self.layout.cluster
                && packet.layout == self.layout_hash
                && packet.policy == self.policy,
            "foreign replication scope or policy"
        );
        let voters = self
            .layout
            .groups
            .get(&packet.group)
            .context("unknown group")?;
        let message = &packet.message;
        ensure!(
            message.to == self.id && message.from != self.id && voters.contains(&message.from),
            "invalid Raft peer route"
        );
        ensure!(self.groups.contains_key(&packet.group), "unhosted group");
        ensure!(
            message.compute_size() <= 16 * 1024 * 1024,
            "oversized message"
        );
        Ok(())
    }

    pub fn step(&mut self, packet: Packet) -> Result<()> {
        self.healthy()?;
        self.validate_packet(&packet)?;
        let message = packet.message;
        ensure!(
            matches!(
                message.get_msg_type(),
                MessageType::MsgAppend
                    | MessageType::MsgAppendResponse
                    | MessageType::MsgHeartbeat
                    | MessageType::MsgHeartbeatResponse
                    | MessageType::MsgRequestVote
                    | MessageType::MsgRequestVoteResponse
                    | MessageType::MsgRequestPreVote
                    | MessageType::MsgRequestPreVoteResponse
            ),
            "unsupported peer message"
        );
        ensure!(
            message.snapshot.is_none(),
            "snapshot requires receive_snapshot and step_snapshot"
        );
        for entry in &message.entries {
            state::validate_entry(&self.layout, packet.group, &self.limits, entry, true)?;
        }
        self.groups
            .get_mut(&packet.group)
            .context("unhosted group")?
            .raw
            .step(message)?;
        Ok(())
    }

    /// Performs blocking storage I/O. The transport may drop output messages;
    /// Raft will retransmit. On error NONE of this call's events are released.
    pub fn poll(&mut self) -> Result<Events> {
        self.healthy()?;
        let result = self.drive();
        if result.is_err() {
            self.failed = true;
        }
        result
    }

    fn drive(&mut self) -> Result<Events> {
        let mut events = Events::default();
        for (id, group) in &mut self.groups {
            let mut outbound = Vec::new();
            while group.raw.has_ready() {
                let mut ready = group.raw.ready();
                if ready.snapshot().is_empty() {
                    group.raw.store().persist(ready.entries(), ready.hs())?;
                } else {
                    images::install_received(group, &ready, *id, &mut events)?;
                }
                outbound.extend(ready.take_messages());
                outbound.extend(ready.take_persisted_messages());
                apply(
                    group,
                    *id,
                    ready.take_committed_entries(),
                    &self.limits,
                    &mut events,
                )?;
                let mut light = group.raw.advance(ready);
                if light.commit_index().is_some() {
                    group
                        .raw
                        .store()
                        .persist(&[], Some(&group.raw.raft.hard_state()))?;
                }
                outbound.extend(light.take_messages());
                apply(
                    group,
                    *id,
                    light.take_committed_entries(),
                    &self.limits,
                    &mut events,
                )?;
                group.raw.advance_apply();
            }
            // Old-term pending entries may still commit after leader loss. They
            // must not be reported as a definite rejection or leak forever.
            let uncertain: Vec<_> = group
                .pending
                .keys()
                .copied()
                .filter(|(_, term)| {
                    group.raw.raft.state != StateRole::Leader || *term != group.raw.raft.term
                })
                .collect();
            for key in uncertain {
                let (request, _) = group.pending.remove(&key).unwrap();
                events.completions.push(Completion {
                    group: *id,
                    request,
                    outcome: Outcome::Unknown,
                });
            }
            events
                .messages
                .extend(outbound.into_iter().map(|message| Packet {
                    cluster: self.layout.cluster,
                    layout: self.layout_hash,
                    policy: self.policy,
                    group: *id,
                    message,
                }));
        }
        self.healthy()?;
        Ok(events)
    }

    /// Local accepted rows at this durable applied cut. This is not a quorum
    /// read or a producer ACK and cannot be used to shortcut retry completion.
    pub fn accepted(&self, group: u64, after: u64, limit: usize) -> Result<Page> {
        self.healthy()?;
        ensure!((1..=128).contains(&limit), "invalid accepted page limit");
        let state = &self.groups.get(&group).context("unhosted group")?.state;
        ensure!(
            after <= state.applied,
            "cursor exceeds local applied position"
        );
        let mut remaining = state
            .accepted
            .range((std::ops::Bound::Excluded(after), std::ops::Bound::Unbounded));
        let entries: Vec<_> = remaining
            .by_ref()
            .take(limit)
            .map(|(index, meta)| (*index, meta.clone()))
            .collect();
        let through = if remaining.next().is_some() {
            entries.last().expect("positive page limit").0
        } else {
            state.applied
        };
        Ok(Page { entries, through })
    }

    pub fn read_accepted(&self, group: u64, index: u64) -> Result<Option<Batch>> {
        self.healthy()?;
        let group = self.groups.get(&group).context("unhosted group")?;
        let Some(meta) = group.state.accepted.get(&index) else {
            return Ok(None);
        };
        let body = images::read_body(group.raw.store(), group.image.as_deref(), index, meta)?;
        meta.validate_payload(&body, self.limits.max_record_bytes as usize)?;
        Ok(Some(Batch {
            meta: meta.clone(),
            body,
        }))
    }
}

fn apply(
    group: &mut Group,
    id: u64,
    entries: Vec<Entry>,
    limits: &Limits,
    events: &mut Events,
) -> Result<()> {
    let durable = group.raw.store().initial_state()?.hard_state.commit;
    for entry in entries {
        ensure!(
            entry.index <= durable,
            "application precedes durable commit"
        );
        let result = group.state.apply(&entry, limits)?;
        if let Some((request, meta)) = group.pending.remove(&(entry.index, entry.term)) {
            ensure!(
                state::decode_meta(&entry.context)? == meta,
                "pending proposal identity changed"
            );
            events.completions.push(Completion {
                group: id,
                request,
                outcome: Outcome::Committed(result.context("pending proposal applied as blank")?),
            });
        }
    }
    Ok(())
}
