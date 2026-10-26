//! Bounded physical maintenance through the existing empty-source WAL plan.
use super::*;

impl Publisher {
    pub(super) async fn compact_next(&mut self, view: &p::Table) -> Result<bool> {
        if let Some(pending) = &view.progress.pending {
            if !pending.plan.is_maintenance() {
                return Ok(false);
            }
        } else {
            // The first annotation publication installs the durable cut/result
            // checkpoint. Afterwards allow one compaction between publications,
            // or continue draining small files while no source is waiting.
            if self.options.compaction.is_none()
                || view
                    .progress
                    .checkpoint
                    .get(&self.service.ingress.group(0)?)
                    .copied()
                    .unwrap_or(0)
                    == 0
                || (self.service.source_observation().records != 0
                    && !view
                        .progress
                        .advanced
                        .as_ref()
                        .is_some_and(|a| !a.sources.is_empty()))
            {
                return Ok(false);
            }
            let Some(plan) = materializer::compaction::plan(&self.catalog, view).await? else {
                return Ok(false);
            };
            self.catalog.retire_history(view).await?;
            let reply = materializer::commit(
                &self.service.ingress.client,
                Command::Plan(p::PlanRequest {
                    authority: self.catalog.authority().clone(),
                    plan,
                }),
            )
            .await?;
            ensure!(
                matches!(reply, Applied::Publisher(p::Reply::Planned { .. })),
                "annotation compaction plan rejected: {reply:?}"
            );
        }
        // A maintenance plan already exists, so the ordinary publisher only
        // prepares that exact plan. It cannot select an ingestion append here.
        self.catalog
            .prepare_next()
            .await?
            .context("annotation compaction preparation missing")?;
        let ready = self.catalog.state().await?;
        let selected = self.resolve_prepared(&ready).await?;
        let transaction = selected
            .read_transaction()
            .await?
            .context("selected compaction has no transaction")?;
        {
            let _ordered = self.service.shared.ordered.lock().await;
            self.service
                .shared
                .staging
                .install_compacted(&transaction, selected)
                .await?;
        }
        // An empty-source maintenance completion changes neither the source
        // checkpoint nor the in-memory result snapshot or active source window.
        self.catalog.finish_pending().await?;
        Ok(true)
    }
}
