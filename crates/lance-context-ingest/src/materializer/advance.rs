//! Observe the original advancement after an uncertain submission outcome.
use super::*;

impl Publisher {
    pub(crate) async fn record_advance(
        &self,
        request: p::AdvancePublication,
    ) -> Result<Arc<p::Advanced>> {
        match commit(&self.ingress.client, Command::Advance(request.clone())).await {
            Ok(Applied::Publisher(p::Reply::Advanced(advanced))) => Ok(advanced),
            Ok(reply) => bail!("source cursor advancement rejected: {reply:?}"),
            Err(error) => {
                // One bounded quorum observation, never a second submission.
                // state() checks current authority, binding and incarnation;
                // the receipt must cover this publication and all closures.
                match self.state().await {
                    Ok(view) => {
                        if let Some(advanced) = view.progress.advanced {
                            if request.matches_receipt(&advanced)? {
                                return Ok(advanced);
                            }
                        }
                        Err(error).context("original publication advancement not confirmed")
                    }
                    Err(observation) => Err(error).context(format!(
                        "original publication advancement observation failed: {observation:#}"
                    )),
                }
            }
        }
    }
}
