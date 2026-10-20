//! Durable publication resolution from the catalog's selected manifest. Source
//! selection and descriptors stay retained until a separate closure protocol.
use super::*;

#[cfg(test)]
mod tests;

pub(super) const MAX_OUTCOME_BYTES: usize = manifest::MAX_MANIFEST_BYTES + 128;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ConfirmPublication {
    pub authority: Authority,
    pub incarnation: Uuid,
    pub transaction: Uuid,
    pub plan_index: u64,
    pub prepared_index: u64,
    pub descriptor_digest: [u8; 32],
    pub target: u64,
    pub manifest_identity: Uuid,
}
impl ConfirmPublication {
    pub(in crate::replication) fn batch(self, layout: &Layout) -> Result<Batch> {
        self.validate(layout)?;
        Command::ConfirmPublication(Box::new(self)).batch(layout)
    }

    pub(super) fn validate(&self, layout: &Layout) -> Result<()> {
        self.authority.validate(layout)?;
        ensure!(
            !self.incarnation.is_nil()
                && !self.transaction.is_nil()
                && !self.manifest_identity.is_nil()
                && self.plan_index > 0
                && self.prepared_index > self.plan_index
                && self.prepared_index < u64::MAX
                && self.descriptor_digest != [0; 32]
                && self.target > 1
                && self.target < (1u64 << 63),
            "invalid publication confirmation identity"
        );
        Ok(())
    }

    fn matches(&self, incarnation: Uuid, manifest: &ManifestRecord) -> bool {
        self.incarnation == incarnation
            && self.transaction == manifest.transaction
            && self.plan_index == manifest.plan_index
            && self.prepared_index == manifest.prepared_index
            && self.descriptor_digest == manifest.descriptor_digest
            && self.target == manifest.version
            && self.manifest_identity == manifest.identity
    }
}

/// One outcome per unresolved plan. This shares the immutable selected record;
/// cloning a table view does not copy descriptors or any source payloads.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Published {
    pub index: u64,
    pub incarnation: Uuid,
    pub manifest: Arc<ManifestRecord>,
}
impl Published {
    pub(in crate::replication) fn validate(&self, table: &Table, cut: u64) -> Result<()> {
        let manifests = table
            .manifests
            .as_ref()
            .context("outcome without manifest index")?;
        let pending = table
            .progress
            .pending
            .as_ref()
            .context("outcome without pending selection")?;
        let prepared = table
            .progress
            .prepared
            .as_ref()
            .context("outcome without prepared descriptors")?;
        let selected = manifests
            .versions
            .get(&self.manifest.version)
            .context("outcome manifest missing")?;
        ensure!(
            self.incarnation == manifests.incarnation
                && self.manifest == *selected
                && self.manifest.version == manifests.head
                && self.manifest.version == pending.plan.target_version()?
                && self.manifest.transaction == pending.plan.transaction
                && self.manifest.plan_index == pending.index
                && self.manifest.prepared_index == prepared.index
                && self.manifest.descriptor_digest == prepared.descriptors.digest
                && self.index > self.manifest.index
                && self.index <= cut
                && self.index != table.authority.grant_index,
            "publication outcome disagrees with selected manifest or pending plan"
        );
        Ok(())
    }
}

impl Catalog {
    pub(super) fn confirm_publication(&mut self, request: ConfirmPublication, index: u64) -> Reply {
        let Some(table) = self.tables.get(&request.authority.table) else {
            return Reply::Rejected(Rejection::Authority);
        };
        // Exact resolution remains available after publisher takeover. It
        // reports an existing outcome and grants no authority to change state.
        if let Some(published) = &table.progress.published {
            return if request.matches(published.incarnation, &published.manifest) {
                Reply::Published(published.clone())
            } else {
                Reply::Rejected(Rejection::PublicationIdentity)
            };
        }
        if request.authority != table.authority {
            return Reply::Rejected(Rejection::Authority);
        }
        let Some(manifests) = &table.manifests else {
            return Reply::Rejected(Rejection::ManifestIdentity);
        };
        let Some(selected) = manifests.versions.get(&request.target) else {
            return Reply::Rejected(Rejection::ManifestIdentity);
        };
        let (Some(pending), Some(prepared)) = (&table.progress.pending, &table.progress.prepared)
        else {
            return Reply::Rejected(Rejection::PlanIdentity);
        };
        if !request.matches(manifests.incarnation, selected)
            || selected.version != manifests.head
            || pending.plan.target_version().ok() != Some(selected.version)
            || selected.transaction != pending.plan.transaction
            || selected.plan_index != pending.index
            || selected.prepared_index != prepared.index
            || selected.descriptor_digest != prepared.descriptors.digest
        {
            return Reply::Rejected(Rejection::PublicationIdentity);
        }
        let published = Arc::new(Published {
            index,
            incarnation: manifests.incarnation,
            manifest: selected.clone(),
        });
        // Do not erase the selection, advance source cursors, or release pins.
        // Closure must preserve independent source indices and durable fences.
        self.tables
            .get_mut(&request.authority.table)
            .expect("known table")
            .progress
            .published = Some(published.clone());
        Reply::Published(published)
    }
}
