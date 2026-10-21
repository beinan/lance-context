//! Bounded producer identities with durable generation fences.
//!
//! Configure slots when creating a group. Reuse requires a committed close and
//! retirement of its latest source record. Neither a timeout nor a new UUID can
//! discard an uncertain write. Control commands require a trusted caller; scope
//! validation is not authorization. In particular, revocation is administrative.

use super::*;
use crate::record::RecordKind;
use bincode::Options;
use bytes::Bytes;

pub const MAX_SLOTS: u32 = 16_384;
pub(super) const MAX_CELL_BYTES: usize = 96;
pub(super) const MAX_COMMAND_BYTES: usize = 256;
const DOMAIN: u32 = 0x57414c50;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Command {
    pub cluster: Uuid,
    pub layout: [u8; 32],
    pub group: u64,
    pub table: Uuid,
    pub slot: u32,
    pub action: Action,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum Action {
    /// Persist this exact tuple before sending. An ambiguous reply never
    /// authorizes selecting another slot, generation or operation UUID.
    Open {
        expected_generation: u64,
        operation: Uuid,
        bucket: u32,
    },
    Close {
        generation: u64,
        expected_last: Option<Last>,
    },
    /// Fences an abandoned writer while retaining its latest receipt and data.
    Revoke { generation: u64 },
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Last {
    pub sequence: u64,
    pub index: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum Reply {
    Opened {
        producer: Uuid,
        generation: u64,
        closed: bool,
    },
    Closed {
        last: Option<Last>,
    },
    LastChanged {
        actual: Option<Last>,
    },
    Fenced,
    Busy,
    Exhausted,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Lease {
    pub table: Uuid,
    pub bucket: u32,
    pub operation: Uuid,
    pub closed: bool,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct View {
    pub capacity: u32,
    pub slot: u32,
    pub generation: u64,
    pub lease: Option<Lease>,
    pub last: Option<Last>,
    /// Local state at the query's quorum cut; an open still needs its CAS.
    pub reusable: bool,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub(super) struct Cell {
    pub generation: u64,
    pub lease: Option<Lease>,
}

fn codec(limit: usize) -> impl Options {
    bincode::DefaultOptions::new()
        .with_fixint_encoding()
        .with_little_endian()
        .with_limit(limit as u64)
        .reject_trailing_bytes()
}

/// This UUID-shaped value is meaningful only with its cluster/layout/group.
pub fn producer_id(slot: u32, generation: u64) -> Uuid {
    Uuid::from_u128(((DOMAIN as u128) << 96) | ((slot as u128) << 64) | generation as u128)
}

fn handle(id: Uuid) -> Option<(usize, u64)> {
    let bits = id.as_u128();
    ((bits >> 96) as u32 == DOMAIN).then_some(((bits >> 64) as u32 as usize, bits as u64))
}

impl Command {
    fn validate(&self, layout: &Layout, group: u64, limits: &Limits) -> Result<()> {
        ensure!(
            self.cluster == layout.cluster
                && self.layout == layout.fingerprint()?
                && self.group == group
                && layout.groups.contains_key(&group)
                && !self.table.is_nil()
                && self.slot < limits.producer_slots,
            "invalid producer command scope or slot"
        );
        match &self.action {
            Action::Open {
                operation, bucket, ..
            } => ensure!(
                !operation.is_nil() && layout.group_for_bucket(*bucket)? == group,
                "invalid producer allocation identity or route"
            ),
            Action::Close {
                generation,
                expected_last,
            } => ensure!(
                *generation > 0
                    && expected_last.is_none_or(|last| last.sequence > 0
                        && last.sequence < u64::MAX
                        && last.index > 0
                        && last.index < u64::MAX),
                "invalid producer close"
            ),
            Action::Revoke { generation } => ensure!(*generation > 0, "invalid producer revoke"),
        }
        Ok(())
    }

    pub(super) fn batch(self, layout: &Layout, limits: &Limits) -> Result<Batch> {
        self.validate(layout, self.group, limits)?;
        let body = codec(MAX_COMMAND_BYTES).serialize(&self)?;
        ensure!(
            body.len() <= MAX_COMMAND_BYTES,
            "oversized producer command"
        );
        let mut batch = Batch::new(
            self.table,
            self.slot,
            producer_id(self.slot, 0),
            1,
            Bytes::from(body),
        );
        batch.meta.kind = RecordKind::Producer;
        Ok(batch)
    }
}

pub(super) fn decode(bytes: &[u8]) -> Result<Command> {
    ensure!(
        bytes.len() <= MAX_COMMAND_BYTES,
        "oversized producer command"
    );
    let command = codec(MAX_COMMAND_BYTES).deserialize(bytes)?;
    ensure!(
        codec(MAX_COMMAND_BYTES).serialize(&command)? == bytes,
        "noncanonical producer command"
    );
    Ok(command)
}

pub(super) fn validate_record(
    layout: &Layout,
    group: u64,
    limits: &Limits,
    meta: &BatchMeta,
    body: &[u8],
) -> Result<()> {
    let command = decode(body)?;
    command.validate(layout, group, limits)?;
    ensure!(
        meta.kind == RecordKind::Producer
            && meta.stream == command.table
            && meta.bucket == command.slot
            && meta.producer == producer_id(command.slot, 0)
            && meta.sequence == 1,
        "producer command envelope mismatch"
    );
    Ok(())
}

pub(super) fn encode_cell(cell: &Cell) -> Result<Vec<u8>> {
    Ok(codec(MAX_CELL_BYTES).serialize(cell)?)
}
pub(super) fn decode_cell(bytes: &[u8]) -> Result<Cell> {
    ensure!(bytes.len() <= MAX_CELL_BYTES, "oversized producer cell");
    let cell = codec(MAX_CELL_BYTES).deserialize(bytes)?;
    ensure!(encode_cell(&cell)? == bytes, "noncanonical producer cell");
    Ok(cell)
}

impl State {
    pub(super) fn with_producer_slots(capacity: u32) -> Self {
        Self {
            producer_slots: vec![Cell::default(); capacity as usize],
            ..Self::default()
        }
    }

    fn producer_key(slot: u32, cell: &Cell) -> Option<state::ProducerKey> {
        cell.lease.as_ref().map(|lease| {
            (
                lease.table,
                lease.bucket,
                producer_id(slot, cell.generation),
            )
        })
    }

    fn producer_last(&self, slot: u32, cell: &Cell) -> Option<Last> {
        self.producers
            .get(&Self::producer_key(slot, cell)?)
            .map(|(meta, index)| Last {
                sequence: meta.sequence,
                index: *index,
            })
    }

    fn producer_reusable(&self, slot: u32, cell: &Cell) -> bool {
        if cell.generation == u64::MAX {
            return false;
        }
        let Some(lease) = &cell.lease else {
            return cell.generation == 0;
        };
        if !lease.closed {
            return false;
        }
        let Some(last) = self.producer_last(slot, cell) else {
            return true;
        };
        let key = Self::producer_key(slot, cell).expect("lease present");
        // Closure alone is not sufficient: require the exact latest lane tail
        // to have been retired by the existing signed source-closure transition.
        self.retired
            .get(&key)
            .is_some_and(|retired| retired.sequence == last.sequence && retired.index == last.index)
    }

    pub(super) fn producer_view(&self, slot: u32) -> Option<View> {
        let cell = self.producer_slots.get(slot as usize)?;
        Some(View {
            capacity: self.producer_slots.len() as u32,
            slot,
            generation: cell.generation,
            lease: cell.lease.clone(),
            last: self.producer_last(slot, cell),
            reusable: self.producer_reusable(slot, cell),
        })
    }

    pub(super) fn producer_admission(&self, meta: &BatchMeta, allow_closed: bool) -> bool {
        if self.producer_slots.is_empty() {
            return true;
        }
        let Some((slot, generation)) = handle(meta.producer) else {
            return false;
        };
        self.producer_slots.get(slot).is_some_and(|cell| {
            cell.generation == generation
                && generation > 0
                && cell.lease.as_ref().is_some_and(|lease| {
                    lease.table == meta.stream
                        && lease.bucket == meta.bucket
                        && (allow_closed || !lease.closed)
                })
        })
    }

    pub(super) fn validate_producers(
        &self,
        layout: &Layout,
        group: u64,
        limits: &Limits,
    ) -> Result<()> {
        ensure!(
            self.producer_slots.len() == limits.producer_slots as usize,
            "producer slot capacity mismatch"
        );
        if self.producer_slots.is_empty() {
            return Ok(());
        }
        ensure!(
            self.producers.len() <= self.producer_slots.len(),
            "producer receipts exceed slot capacity"
        );
        for cell in &self.producer_slots {
            match &cell.lease {
                None => ensure!(cell.generation == 0, "missing producer generation lease"),
                Some(lease) => ensure!(
                    cell.generation > 0
                        && !lease.table.is_nil()
                        && !lease.operation.is_nil()
                        && layout.group_for_bucket(lease.bucket)? == group,
                    "invalid producer lease"
                ),
            }
        }
        ensure!(
            self.producers
                .values()
                .all(|(meta, _)| self.producer_admission(meta, true)),
            "producer receipt has stale slot scope"
        );
        Ok(())
    }

    pub(super) fn apply_producer(&mut self, command: Command) -> Reply {
        let Some(cell) = self.producer_slots.get(command.slot as usize).cloned() else {
            return Reply::Fenced;
        };
        let slot = command.slot;
        let last = self.producer_last(slot, &cell);
        match command.action {
            Action::Open {
                expected_generation,
                operation,
                bucket,
            } => {
                let Some(generation) = expected_generation.checked_add(1) else {
                    return Reply::Exhausted;
                };
                if cell.generation == generation {
                    return match cell.lease {
                        Some(lease)
                            if lease.table == command.table
                                && lease.bucket == bucket
                                && lease.operation == operation =>
                        {
                            Reply::Opened {
                                producer: producer_id(slot, generation),
                                generation,
                                closed: lease.closed,
                            }
                        }
                        _ => Reply::Fenced,
                    };
                }
                if cell.generation != expected_generation {
                    return Reply::Fenced;
                }
                if !self.producer_reusable(slot, &cell) {
                    return Reply::Busy;
                }
                if let Some(key) = Self::producer_key(slot, &cell) {
                    self.producers.remove(&key);
                    self.retired.remove(&key);
                }
                self.producer_slots[slot as usize] = Cell {
                    generation,
                    lease: Some(Lease {
                        table: command.table,
                        bucket,
                        operation,
                        closed: false,
                    }),
                };
                Reply::Opened {
                    producer: producer_id(slot, generation),
                    generation,
                    closed: false,
                }
            }
            Action::Close {
                generation,
                expected_last,
            } => {
                if cell.generation != generation
                    || cell.lease.as_ref().is_none_or(|l| l.table != command.table)
                {
                    return Reply::Fenced;
                }
                if last != expected_last {
                    return Reply::LastChanged { actual: last };
                }
                self.producer_slots[slot as usize]
                    .lease
                    .as_mut()
                    .unwrap()
                    .closed = true;
                Reply::Closed { last }
            }
            Action::Revoke { generation } => {
                if cell.generation != generation
                    || cell.lease.as_ref().is_none_or(|l| l.table != command.table)
                {
                    return Reply::Fenced;
                }
                self.producer_slots[slot as usize]
                    .lease
                    .as_mut()
                    .unwrap()
                    .closed = true;
                Reply::Closed { last }
            }
        }
    }
}

#[cfg(test)]
mod tests;
