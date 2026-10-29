//! Resolve an uncertain plan by observation of its exact durable identity.
use super::*;

impl Publisher {
    pub(crate) async fn record_plan(&self, plan: p::Plan) -> Result<()> {
        let submitted = commit(
            &self.ingress.client,
            Command::Plan(p::PlanRequest {
                authority: self.authority.clone(),
                plan: plan.clone(),
            }),
        )
        .await;
        match submitted {
            Ok(Applied::Publisher(p::Reply::Planned { .. })) => Ok(()),
            Ok(reply) => bail!("publication selection rejected: {reply:?}"),
            Err(error) => {
                // An unknown reply does not prove the Plan failed. One bounded
                // quorum observation can prove it committed. state() validates
                // authority, binding and table incarnation; compare the entire
                // plan, including its sources, not just its transaction UUID.
                // No mutation is resubmitted, and absence remains unknown.
                match self.state().await {
                    Ok(view)
                        if view
                            .progress
                            .pending
                            .as_ref()
                            .is_some_and(|pending| pending.plan == plan) =>
                    {
                        Ok(())
                    }
                    Ok(_) => Err(error).context("original publication plan not confirmed"),
                    Err(observation) => Err(error).context(format!(
                        "original publication plan observation failed: {observation:#}"
                    )),
                }
            }
        }
    }
}
