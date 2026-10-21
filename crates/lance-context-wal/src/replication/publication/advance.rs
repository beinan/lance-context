//! Atomic source-cursor advancement after all selected source quorums close.
use super::*;
use crate::replication::source::{ClosedProof, Selection};

#[cfg(test)]
mod tests;

pub(super) const MAX_ADVANCE_BYTES: usize = 64 * 4096 + 8192;
pub(super) const MAX_ADVANCED_BYTES: usize = 8192;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct AdvancePublication {
    pub authority: Authority,
    pub published_index: u64,
    pub proofs: BTreeMap<u64, ClosedProof>,
}

impl AdvancePublication {
    pub(in crate::replication) fn batch(self, layout: &Layout, limits: &Limits) -> Result<Batch> {
        self.validate(layout, limits)?;
        Command::AdvancePublication(Box::new(self)).batch(layout)
    }

    pub(super) fn validate(&self, layout: &Layout, limits: &Limits) -> Result<()> {
        self.authority.validate(layout)?;
        ensure!(
            self.published_index > 0
                && self.published_index < u64::MAX
                && !self.proofs.is_empty()
                && self.proofs.len() <= layout.groups.len()
                && self.proofs.len() <= 64,
            "invalid source closure set"
        );
        for (group, proof) in &self.proofs {
            proof.verify(layout, limits)?;
            let closure = proof.closed().permit.closure();
            ensure!(
                *group == proof.source_cut().group
                    && closure.table == self.authority.table
                    && closure.published_index == self.published_index,
                "source closure request scope mismatch"
            );
        }
        Ok(())
    }

    fn digest(&self) -> Result<[u8; 32]> {
        // Bind the entire sorted set, not refreshed attestation read cuts.
        let closures: Vec<_> = self
            .proofs
            .iter()
            .map(|(group, proof)| (*group, proof.closed().permit.closure()))
            .collect();
        Ok(Sha256::digest(
            codec(MAX_ADVANCE_BYTES)
                .serialize(&("lance-context/catalog-advanced-closures/v1", closures))?,
        )
        .into())
    }
}

/// One bounded receipt per table. It shares the already selected manifest and
/// retains no source payloads, input metadata or prepared fragment descriptors.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Advanced {
    pub index: u64,
    pub publication: Arc<Published>,
    pub sources: BTreeMap<u64, u64>,
    pub closure_digest: [u8; 32],
}

impl Advanced {
    pub(super) fn validate(&self, table: &Table, cut: u64) -> Result<()> {
        let manifests = table
            .manifests
            .as_ref()
            .context("advancement without manifest index")?;
        let published = &self.publication;
        let selected = &published.manifest;
        ensure!(
            published.incarnation == manifests.incarnation
                && manifests.versions.get(&selected.version) == Some(selected)
                && selected.version > manifests.initial_version
                && selected.version == table.progress.minimum_read_version
                && published.index > selected.index
                && self.index > published.index
                && self.index <= cut
                && self.index != table.authority.grant_index
                && self.closure_digest != [0; 32]
                && !self.sources.is_empty()
                && self.sources.len() <= 64
                && self.sources.iter().all(|(group, through)| {
                    *through > 0
                        && *through < u64::MAX
                        && table.progress.checkpoint.get(group) == Some(through)
                })
                && table
                    .progress
                    .pending
                    .as_ref()
                    .is_none_or(|p| p.index > self.index),
            "invalid catalog advancement receipt"
        );
        Ok(())
    }
}

impl Catalog {
    pub(super) fn advance_publication(&mut self, request: AdvancePublication, index: u64) -> Reply {
        let Some(table) = self.tables.get(&request.authority.table) else {
            return Reply::Rejected(Rejection::Authority);
        };
        let Ok(digest) = request.digest() else {
            return Reply::Rejected(Rejection::SourceClosure);
        };
        if let Some(old) = &table.progress.advanced {
            if old.publication.index == request.published_index && old.closure_digest == digest {
                // A stale owner may resolve only the already completed exact
                // operation. It cannot mutate a newer pending publication.
                return Reply::Advanced(old.clone());
            }
        }
        if table.authority != request.authority {
            return Reply::Rejected(Rejection::Authority);
        }
        let (Some(pending), Some(published)) = (&table.progress.pending, &table.progress.published)
        else {
            return Reply::Rejected(Rejection::PublicationIdentity);
        };
        if published.index != request.published_index {
            return Reply::Rejected(Rejection::PublicationIdentity);
        }
        if !request.proofs.keys().eq(pending.plan.sources.keys()) {
            return Reply::Rejected(Rejection::SourceClosure);
        }
        for (group, range) in &pending.plan.sources {
            let selection = Selection {
                table: table.authority.table,
                transaction: pending.plan.transaction,
                plan_index: pending.index,
                read_version: pending.plan.read_version,
                group: *group,
                ingest: table.ingest.clone(),
                range: range.clone(),
            };
            let closure = request.proofs[group].closed().permit.closure();
            if !closure
                .matches_publication(&selection, published)
                .unwrap_or(false)
            {
                return Reply::Rejected(Rejection::SourceClosure);
            }
        }
        let advanced = Arc::new(Advanced {
            index,
            publication: published.clone(),
            sources: pending
                .plan
                .sources
                .iter()
                .map(|(group, range)| (*group, range.through))
                .collect(),
            closure_digest: digest,
        });
        let progress = &mut self
            .tables
            .get_mut(&request.authority.table)
            .expect("known table")
            .progress;
        progress.checkpoint.extend(
            advanced
                .sources
                .iter()
                .map(|(group, through)| (*group, *through)),
        );
        progress.minimum_read_version = advanced.publication.manifest.version;
        progress.pending = None;
        progress.prepared = None;
        progress.published = None;
        progress.advanced = Some(advanced.clone());
        Reply::Advanced(advanced)
    }
}
