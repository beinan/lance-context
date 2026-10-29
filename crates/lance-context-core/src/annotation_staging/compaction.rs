//! Install a confirmed physical rewrite without retiring annotation updates.
use super::*;
use lance_table::transaction::{Operation, Transaction};

impl AnnotationStaging {
    /// Switch the base after a confirmed, row-preserving native compaction.
    /// The caller must validate the rewrite against its pinned source snapshot
    /// and resolve the exact fenced publication before calling this method.
    /// Matching a native transaction is not itself a quorum or reclamation proof.
    ///
    /// Compaction must preserve the base's annotation cut and request results.
    /// It cannot overlap a frozen publication. Active updates and their applied
    /// position remain intact; captured cold readers continue pinning the old
    /// base through the same registry used by ordinary annotation publication.
    pub async fn install_compacted(
        &self,
        transaction: &Transaction,
        selected: Arc<Dataset>,
    ) -> Result<()> {
        self.install_maintenance(transaction, selected, false).await
    }

    /// Install a confirmed body-only rewrite selected by a native metadata
    /// transaction. The caller verifies the exact row-preserving body rewrite
    /// and fenced publication. Narrow files, cut and results remain unchanged;
    /// active updates and already captured body readers keep their own views.
    pub async fn install_body_compacted(
        &self,
        transaction: &Transaction,
        selected: Arc<Dataset>,
    ) -> Result<()> {
        self.install_maintenance(transaction, selected, true).await
    }

    async fn install_maintenance(
        &self,
        transaction: &Transaction,
        selected: Arc<Dataset>,
        body_only: bool,
    ) -> Result<()> {
        let (base, materialized) = {
            let state = self.state.read().await;
            if state.frozen.is_some() {
                return Err(invalid(
                    "compaction overlaps a frozen annotation generation",
                ));
            }
            (state.base.clone(), state.materialized)
        };
        let operation = if body_only {
            matches!(&transaction.operation, Operation::UpdateConfig {
                config_updates: None,
                table_metadata_updates: None,
                schema_metadata_updates: None,
                field_metadata_updates,
            } if field_metadata_updates.is_empty())
                && selected.manifest().fragments == base.manifest().fragments
        } else {
            matches!(&transaction.operation, Operation::Rewrite { groups, .. } if !groups.is_empty())
        };
        if !operation
            || transaction.read_version != base.version_id()
            || selected.uri() != base.uri()
            || selected.version_id() != base.version_id() + 1
            || selected.schema() != base.schema()
            || selected.read_transaction().await?.as_ref() != Some(transaction)
        {
            return Err(invalid("selected snapshot is not this native compaction"));
        }
        let original = base
            .read_transaction()
            .await?
            .ok_or_else(|| invalid("compaction base has no annotation transaction"))?;
        let properties = original.transaction_properties.as_ref();
        let cut = properties
            .and_then(|p| p.get("lance-context.annotation-cut"))
            .ok_or_else(|| invalid("compaction base has no annotation cut"))?;
        let (scope, through): (Scope, u64) =
            serde_json::from_str(cut).map_err(|e| invalid(e.to_string()))?;
        if scope != self.scope || through != materialized {
            return Err(invalid("compaction base annotation cut differs"));
        }
        for key in [
            "lance-context.annotation-cut",
            RESULTS_PROPERTY,
            BODY_SNAPSHOT_PROPERTY,
        ] {
            if body_only && key == BODY_SNAPSHOT_PROPERTY {
                continue;
            }
            if properties.and_then(|p| p.get(key))
                != transaction
                    .transaction_properties
                    .as_ref()
                    .and_then(|p| p.get(key))
            {
                return Err(invalid(
                    "compaction changed annotation cut, request results or body reference",
                ));
            }
        }
        if body_only {
            let prior = BodySnapshot::read(&base)
                .await?
                .ok_or_else(|| invalid("body maintenance base has no body reference"))?;
            let next = BodySnapshot::read(&selected)
                .await?
                .ok_or_else(|| invalid("body maintenance dropped its body reference"))?;
            if next.uri != prior.uri
                || next.table_instance != prior.table_instance
                || next.version == prior.version
                || !lance_table::format::is_detached_version(next.version)
                || next.transaction != transaction.uuid
            {
                return Err(invalid(
                    "body maintenance changed its binding or native identity",
                ));
            }
        }
        let _mutation = self.mutation.lock().await;
        let mut state = self.state.write().await;
        if state.frozen.is_some() || !Arc::ptr_eq(&state.base, &base) {
            return Err(invalid(
                "annotation base changed during compaction installation",
            ));
        }
        state.base = state.views.install(selected);
        Ok(())
    }
}
