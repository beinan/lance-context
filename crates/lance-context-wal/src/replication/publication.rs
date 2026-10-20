//! Replicated table bindings and publisher epochs. These commands run through
//! the same durable Raft path as appends; a local view is not an active lease.
//! Publication plans, manifest outcomes and source retirement build on these
//! epochs in subsequent changes. No Lance manifest is changed by this module.
use super::*;
use crate::{ingest_contract::IngestContract, record::RecordKind};
use bincode::Options;
use bytes::Bytes;
#[cfg(test)]
mod tests;

pub(super) const MAX_BYTES: usize = 8192;
fn codec() -> impl Options {
    bincode::DefaultOptions::new()
        .with_fixint_encoding()
        .with_little_endian()
        .with_limit(MAX_BYTES as u64)
        .reject_trailing_bytes()
}

/// The authorized catalog supplies one stable storage namespace and a canonical
/// dataset key within it. Raw aliases or client-provided URIs are not identities.
/// This representation does not discover aliases or authenticate the caller.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct Destination {
    pub namespace: Uuid,
    pub dataset: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Acquire {
    pub table: Uuid,
    pub destination: Destination,
    pub ingest: IngestContract,
    pub owner: Uuid,
    /// Stable across an ambiguous retry; use a fresh UUID for a new takeover.
    pub request: Uuid,
    pub expected_epoch: u64,
}
impl Acquire {
    fn validate(&self) -> Result<()> {
        ensure!(
            !self.table.is_nil() && !self.owner.is_nil() && !self.request.is_nil(),
            "nil publisher identity"
        );
        validate_binding(&self.destination, &self.ingest)?;
        ensure!(self.expected_epoch < u64::MAX, "publisher epoch exhausted");
        Ok(())
    }
    pub(super) fn batch(self, layout: &Layout) -> Result<Batch> {
        self.validate()?;
        let envelope = Envelope {
            version: 1,
            cluster: layout.cluster,
            layout: layout.fingerprint()?,
            group: control_group(layout),
            acquire: self,
        };
        let bytes = codec().serialize(&envelope)?;
        ensure!(bytes.len() <= MAX_BYTES, "publisher command too large");
        let mut batch = Batch::new(
            envelope.acquire.table,
            0,
            envelope.acquire.owner,
            envelope.acquire.expected_epoch + 1,
            Bytes::from(bytes),
        );
        batch.meta.kind = RecordKind::MultiPublication;
        Ok(batch)
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Authority {
    pub cluster: Uuid,
    pub layout: [u8; 32],
    pub group: u64,
    pub table: Uuid,
    pub owner: Uuid,
    pub epoch: u64,
    pub grant_index: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Table {
    pub destination: Destination,
    pub ingest: IngestContract,
    pub authority: Authority,
    pub request: Uuid,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum Rejection {
    BindingConflict,
    DestinationBound,
    Epoch { current: u64 },
    RequestReused,
    Capacity,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum Reply {
    Acquired(Authority),
    Rejected(Rejection),
}

#[derive(Serialize, Deserialize)]
pub(super) struct Envelope {
    version: u32,
    cluster: Uuid,
    layout: [u8; 32],
    group: u64,
    acquire: Acquire,
}
pub(super) fn control_group(layout: &Layout) -> u64 {
    *layout
        .groups
        .first_key_value()
        .expect("validated nonempty layout")
        .0
}
fn validate_binding(destination: &Destination, ingest: &IngestContract) -> Result<()> {
    ensure!(
        !destination.namespace.is_nil()
            && !destination.dataset.is_empty()
            && destination.dataset.len() <= 4096,
        "invalid canonical destination"
    );
    ingest.validate()?;
    Ok(())
}
pub(super) fn decode(bytes: &[u8]) -> Result<Envelope> {
    ensure!(bytes.len() <= MAX_BYTES, "publisher command too large");
    let envelope: Envelope = codec().deserialize(bytes)?;
    ensure!(envelope.version == 1, "unsupported publisher command");
    envelope.acquire.validate()?;
    Ok(envelope)
}
pub(super) fn validate_record(
    layout: &Layout,
    group: u64,
    limits: &Limits,
    meta: &BatchMeta,
    body: &[u8],
) -> Result<()> {
    let e = decode(body)?;
    ensure!(
        group == control_group(layout)
            && e.group == group
            && e.cluster == layout.cluster
            && e.layout == layout.fingerprint()?
            && meta.kind == RecordKind::MultiPublication
            && meta.stream == e.acquire.table
            && meta.bucket == 0
            && meta.producer == e.acquire.owner
            && meta.sequence == e.acquire.expected_epoch + 1
            && u64::from(e.acquire.ingest.max_record_bytes) <= limits.max_record_bytes,
        "publisher scope/envelope or ingestion limit mismatch"
    );
    Ok(())
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(super) struct Catalog {
    pub tables: BTreeMap<Uuid, Table>,
}
impl Catalog {
    pub fn apply(&mut self, e: Envelope, index: u64, max_tables: u64) -> Reply {
        let a = e.acquire;
        let old = self.tables.get(&a.table);
        if let Some(old) = old {
            if old.destination != a.destination || old.ingest != a.ingest {
                return Reply::Rejected(Rejection::BindingConflict);
            }
            if old.request == a.request {
                return if old.authority.owner == a.owner
                    && old.authority.epoch == a.expected_epoch + 1
                {
                    Reply::Acquired(old.authority.clone())
                } else {
                    Reply::Rejected(Rejection::RequestReused)
                };
            }
        }
        let current = old.map_or(0, |old| old.authority.epoch);
        if a.expected_epoch != current {
            return Reply::Rejected(Rejection::Epoch { current });
        }
        if old.is_none() {
            if self.tables.values().any(|t| t.destination == a.destination) {
                return Reply::Rejected(Rejection::DestinationBound);
            }
            if self.tables.len() as u64 >= max_tables {
                return Reply::Rejected(Rejection::Capacity);
            }
        }
        let authority = Authority {
            cluster: e.cluster,
            layout: e.layout,
            group: e.group,
            table: a.table,
            owner: a.owner,
            epoch: current + 1,
            grant_index: index,
        };
        self.tables.insert(
            a.table,
            Table {
                destination: a.destination,
                ingest: a.ingest,
                authority: authority.clone(),
                request: a.request,
            },
        );
        Reply::Acquired(authority)
    }
    pub fn validate(&self, layout: &Layout, group: u64, cut: u64, limits: &Limits) -> Result<()> {
        ensure!(
            self.tables.len() as u64 <= limits.max_tables,
            "publisher catalog exceeds limit"
        );
        ensure!(
            group == control_group(layout) || self.tables.is_empty(),
            "catalog in source group"
        );
        let fingerprint = layout.fingerprint()?;
        let mut destinations = std::collections::BTreeSet::new();
        for (id, table) in &self.tables {
            validate_binding(&table.destination, &table.ingest)?;
            let a = &table.authority;
            ensure!(
                !id.is_nil()
                    && *id == a.table
                    && !a.owner.is_nil()
                    && !table.request.is_nil()
                    && a.cluster == layout.cluster
                    && a.layout == fingerprint
                    && a.group == group
                    && a.epoch > 0
                    && a.epoch <= a.grant_index
                    && a.grant_index <= cut
                    && u64::from(table.ingest.max_record_bytes) <= limits.max_record_bytes
                    && destinations.insert(&table.destination),
                "invalid publisher checkpoint state"
            );
        }
        Ok(())
    }
}
pub(super) fn encode_table(table: &Table) -> Result<Vec<u8>> {
    Ok(codec().serialize(table)?)
}
pub(super) fn decode_table(bytes: &[u8]) -> Result<Table> {
    Ok(codec().deserialize(bytes)?)
}
