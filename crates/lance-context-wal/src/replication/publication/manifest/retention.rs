//! Explicit catalog-history retirement. Immutable objects are not deleted here.
use super::*;
use std::ops::Bound::{Excluded, Unbounded};

/// The bootstrap record remains available for exact adoption retries. Other
/// retained versions form one contiguous suffix starting at `retained_from`.
/// A retired position does not prove that any supplied object was once selected.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct HistoryBoundary {
    pub incarnation: Uuid,
    pub bootstrap_version: u64,
    pub retained_from: u64,
}

impl HistoryBoundary {
    pub fn is_retired(&self, version: u64) -> bool {
        version > self.bootstrap_version && version < self.retained_from
    }
}

impl ManifestIndex {
    pub fn history_boundary(&self) -> HistoryBoundary {
        HistoryBoundary {
            incarnation: self.incarnation,
            bootstrap_version: self.initial_version,
            retained_from: self
                .versions
                .range((Excluded(self.initial_version), Unbounded))
                .next()
                .map_or(self.initial_version, |(version, _)| *version),
        }
    }
}

/// Retire directory records strictly below `retain_from`, except the bootstrap
/// anchor. The current head, active publication dependencies and latest durable
/// advancement receipt must remain resolvable. This grants no object GC permit.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct RetireManifestHistory {
    pub authority: Authority,
    pub incarnation: Uuid,
    pub retain_from: u64,
}

impl RetireManifestHistory {
    pub(in crate::replication) fn batch(self, layout: &Layout) -> Result<Batch> {
        self.validate(layout)?;
        Command::RetireManifestHistory(Box::new(self)).batch(layout)
    }

    pub(in crate::replication::publication) fn validate(&self, layout: &Layout) -> Result<()> {
        self.authority.validate(layout)?;
        ensure!(
            !self.incarnation.is_nil() && self.retain_from > 0 && self.retain_from < (1u64 << 63),
            "invalid manifest history retirement scope"
        );
        Ok(())
    }
}

impl Catalog {
    pub(in crate::replication::publication) fn retire_manifest_history(
        &mut self,
        request: RetireManifestHistory,
    ) -> Reply {
        let Some(table) = self.tables.get(&request.authority.table) else {
            return Reply::Rejected(Rejection::Authority);
        };
        if table.authority != request.authority {
            return Reply::Rejected(Rejection::Authority);
        }
        let Some(manifests) = &table.manifests else {
            return Reply::Rejected(Rejection::ManifestIdentity);
        };
        if manifests.incarnation != request.incarnation {
            return Reply::Rejected(Rejection::ManifestIdentity);
        }
        if request.retain_from < manifests.initial_version || request.retain_from > manifests.head {
            return Reply::Rejected(Rejection::ManifestHistoryBoundary);
        }
        let protected = table
            .progress
            .pending
            .iter()
            .map(|p| p.plan.read_version)
            .chain(table.progress.published.iter().map(|p| p.manifest.version))
            .chain(
                table
                    .progress
                    .advanced
                    .iter()
                    .map(|a| a.publication.manifest.version),
            );
        if protected.into_iter().any(|v| request.retain_from > v) {
            return Reply::Rejected(Rejection::PendingPlan);
        }
        // At most MAX_MANIFEST_VERSIONS records are visited. Captured immutable
        // views keep their Arc records; the directory has no external I/O here.
        let table = self
            .tables
            .get_mut(&request.authority.table)
            .expect("known table");
        let manifests = Arc::make_mut(table.manifests.as_mut().expect("managed table"));
        let bootstrap = manifests.initial_version;
        manifests
            .versions
            .retain(|v, _| *v == bootstrap || *v >= request.retain_from);
        Reply::ManifestHistory(manifests.history_boundary())
    }
}
