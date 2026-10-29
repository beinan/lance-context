//! Resolve an uncertain append by observing its exact committed identity.
use super::*;

impl Service {
    pub(super) async fn append_observed(
        &self,
        prepared: &PreparedAppend,
    ) -> Result<AcceptedAppend> {
        match self.ingress.append(prepared).await {
            Ok(accepted) => Ok(accepted),
            Err(error)
                if matches!(
                    error.downcast_ref::<AppendFailure>(),
                    Some(AppendFailure::Unknown)
                ) =>
            {
                // Do not submit the mutation again. A read-index observation can
                // prove an existing acceptance, but absence cannot prove failure.
                match self.observe_accepted(prepared).await {
                    Ok(Some(accepted)) => Ok(accepted),
                    Ok(None) => Err(error),
                    Err(observation) => {
                        Err(error.context(format!("append observation failed: {observation:#}")))
                    }
                }
            }
            Err(error) => Err(error),
        }
    }

    async fn observe_accepted(&self, prepared: &PreparedAppend) -> Result<Option<AcceptedAppend>> {
        let expected = &prepared.batch.meta;
        let group = self.ingress.group(expected.bucket)?;
        let (cut, value) = reader::observe(
            &self.ingress,
            group,
            query::Query::Producer {
                table: expected.stream,
                bucket: expected.bucket,
                producer: expected.producer,
            },
        )
        .await?;
        let query::Value::Producer(Some((meta, index))) = value else {
            return Ok(None);
        };
        if meta != *expected || index == 0 || index > cut.applied || index == u64::MAX {
            return Ok(None);
        }
        Ok(Some(AcceptedAppend {
            scope: self.ingress.scope.clone(),
            position: Receipt {
                cluster: cut.cluster,
                layout: cut.layout,
                group,
                index,
            },
            meta,
            rows: prepared.rows,
        }))
    }
}
