//! Bounded ZoneMap coverage for the full-body owner's selected main snapshot.
use super::*;
use crate::turn_ingestion::session_turn_key;
use lance::index::DatasetIndexExt;
use session_index::{Action, Key};
use std::collections::BTreeSet;

#[cfg(test)]
mod tests;

/// Runtime-only opt-in for an existing `session_turn_key` column whose schema
/// declares `trace.session_turn_key=hex-utf8-v1`. The column must equal
/// `v1:<lowercase UTF8 hex(session_id)>/<lowercase UTF8 hex(id)>` in existing
/// rows as well as future writes. Business identity remains `(session_id,id)`.
///
/// Builds native ZoneMaps on the authoritative owner main, in bounded fragment
/// windows, through its existing fenced publication/recovery protocol. Uncovered
/// rows still participate in every existence query. Range overlap can limit
/// pruning; this option is not a throughput guarantee.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct SessionTurnIndex {
    /// Maximum physical fragments per build (also capped at 1048576 rows).
    pub max_fragments: usize,
}

impl Default for SessionTurnIndex {
    fn default() -> Self {
        Self { max_fragments: 128 }
    }
}

impl SessionTurnIndex {
    /// Enable the existing hex-utf8-v1 contract; adds no column or metadata.
    pub fn hex_utf8_v1() -> Self {
        Self::default()
    }

    pub(super) fn validate(&self, budget: usize) -> Result<()> {
        ensure!(
            (1..=128).contains(&self.max_fragments) && budget >= session_index::WORKSPACE,
            "invalid session/turn ZoneMap fragment bound or insufficient workspace"
        );
        Ok(())
    }

    pub(super) async fn action(&self, dataset: &Dataset) -> Result<Option<Action>> {
        session_turn_key::validate_schema(&arrow_schema::Schema::from(dataset.schema()))?;
        TurnIdIndex::validate_base(dataset)?;
        let fragments: BTreeMap<_, _> = dataset
            .fragments()
            .iter()
            .map(|f| Ok((u32::try_from(f.id)?, f.physical_rows.unwrap())))
            .collect::<Result<_>>()?;
        let mut covered = BTreeSet::new();
        let mut segments = Vec::new();
        let mut empty_sources = Vec::new();
        for index in dataset
            .load_indices()
            .await?
            .iter()
            .filter(|i| i.name == Key::SessionTurn.name())
        {
            session_index::validate_metadata(dataset, index, Key::SessionTurn)?;
            let coverage = index.fragment_bitmap.as_ref().unwrap();
            let mut rows = 0usize;
            for id in coverage {
                ensure!(covered.insert(id), "session/turn ZoneMap segments overlap");
                rows = rows
                    .checked_add(
                        *fragments
                            .get(&id)
                            .context("ZoneMap covers foreign fragment")?,
                    )
                    .context("ZoneMap row count overflow")?;
            }
            if coverage.is_empty() {
                if empty_sources.len() < 64 {
                    empty_sources.push(index.uuid);
                }
            } else {
                segments.push((index.clone(), rows));
            }
        }
        // Same bounded four-way row tiers as ID maintenance, with native
        // ZoneMap merging (no BTree writer or body reads).
        if let Some(action) = TurnIdIndex::merge(segments) {
            return Ok(Some(action));
        }
        let mut selected = Vec::new();
        let mut rows = 0;
        for (id, count) in fragments {
            if covered.contains(&id) || count == 0 || count > turn_id_index::MAX_BUILD_ROWS - rows {
                continue;
            }
            selected.push(id);
            rows += count;
            if selected.len() == self.max_fragments {
                break;
            }
        }
        Ok((rows != 0).then_some(if empty_sources.is_empty() {
            Action::Build(selected)
        } else {
            Action::BuildReplacingEmpty {
                fragments: selected,
                empty_sources,
            }
        }))
    }
}

pub(crate) async fn has_work(publisher: &Publisher, version: u64) -> Result<bool> {
    let Some(options) = &publisher.options.session_turn_index else {
        return Ok(false);
    };
    let dataset = compaction::read_compaction_base(publisher, version).await?;
    Ok(options.action(&dataset).await?.is_some())
}

pub(super) async fn plan(publisher: &Publisher, view: &p::Table) -> Result<Option<p::Plan>> {
    let version = view.progress.minimum_read_version;
    let next_group = publisher.maintenance_group()?;
    Ok(has_work(publisher, version).await?.then(|| p::Plan {
        transaction: Uuid::new_v4(),
        read_version: version,
        sources: BTreeMap::new(),
        next_group,
    }))
}

pub(super) async fn prepare(
    publisher: &Publisher,
    pending: &p::PendingPlan,
) -> Result<Option<p::Descriptors>> {
    let Some(options) = &publisher.options.session_turn_index else {
        return Ok(None);
    };
    let dataset = compaction::read_compaction_base(publisher, pending.plan.read_version).await?;
    let Some(action) = options.action(&dataset).await? else {
        return Ok(None);
    };
    Ok(Some(
        session_index::prepare_action(publisher, dataset, action, Key::SessionTurn).await?,
    ))
}

/// Audit only the three narrow identity columns of this bounded source window.
/// Do not publish an index for malformed pre-existing derived values.
pub(super) async fn validate_inputs(dataset: &Dataset, ids: &[u32]) -> Result<()> {
    let fragments = ids
        .iter()
        .map(|&id| {
            dataset
                .get_fragment(id as usize)
                .map(|f| f.metadata().clone())
                .context("ZoneMap source fragment missing")
        })
        .collect::<Result<Vec<_>>>()?;
    let mut scan = dataset.scan();
    scan.project(&["session_id", "id", session_turn_key::COLUMN])?
        .with_fragments(fragments)
        .batch_size(2048)
        .batch_readahead(1)
        .fragment_readahead(1);
    let mut stream = scan.try_into_stream().await?;
    while let Some(batch) = stream.try_next().await? {
        session_turn_key::validate_batch(&batch)?;
    }
    Ok(())
}

/// Only validated, selected-main coverage may prune on the derived column.
/// Before its first build, and on an appended tail, probe the original keys.
pub(crate) type LookupFragments = (bool, Option<Vec<lance_table::format::Fragment>>);

pub(crate) async fn lookup_fragments(
    dataset: &Dataset,
    workspace: &mut MergeReservation,
) -> Result<Vec<LookupFragments>> {
    let indices = dataset.load_indices().await?;
    for index in indices.iter().filter(|i| i.name == Key::SessionTurn.name()) {
        session_index::validate_metadata(dataset, index, Key::SessionTurn)?;
    }
    let covered = |id: u64| {
        indices.iter().any(|i| {
            i.name == Key::SessionTurn.name()
                && u32::try_from(id)
                    .is_ok_and(|id| i.fragment_bitmap.as_ref().unwrap().contains(id))
        })
    };
    if !dataset.fragments().iter().any(|f| covered(f.id)) {
        return Ok(vec![(false, None)]);
    }
    if dataset.fragments().iter().all(|f| covered(f.id)) {
        return Ok(vec![(true, None)]);
    }
    // Admit the cloned fragment metadata before partitioning. If it does not
    // fit, keep the original one-scan raw-key path. Never wait for capacity or
    // interpret an unadmitted split as absent business keys.
    let charge = dataset
        .manifest_location()
        .size
        .and_then(|n| usize::try_from(n).ok())
        .and_then(|n| n.checked_mul(4))
        .and_then(|n| workspace.bytes().checked_add(n));
    if !charge.is_some_and(|bytes| workspace.try_grow_to(bytes)) {
        return Ok(vec![(false, None)]);
    }
    let (indexed, tail): (Vec<_>, Vec<_>) = dataset
        .fragments()
        .iter()
        .cloned()
        .partition(|f| covered(f.id));
    Ok(vec![(true, Some(indexed)), (false, Some(tail))])
}
