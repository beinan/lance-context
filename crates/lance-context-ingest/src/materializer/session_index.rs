//! Bounded native BTree segments, published by the existing fenced transaction.
use super::*;
use lance::{dataset::transaction::Operation, index::DatasetIndexExt};
use lance_index::{scalar::ScalarIndexParams, IndexType};
use lance_table::format::{pb, IndexMetadata};
use prost::Message;
use std::collections::BTreeSet;

const NAME: &str = "lance_context_rollout_id";
const MAX_ROWS: usize = 1 << 20;
const WORKSPACE: usize = 512 << 20;

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct SessionIndex {
    pub min_fragments: usize,
    pub max_fragments: usize,
    /// Consolidate native index files before adding another segment at this bound.
    pub max_segments: usize,
}
impl Default for SessionIndex {
    fn default() -> Self {
        Self {
            min_fragments: 16,
            max_fragments: 128,
            max_segments: 16,
        }
    }
}
impl SessionIndex {
    pub(super) fn validate(&self, budget: usize) -> Result<()> {
        ensure!(
            self.min_fragments > 0
                && self.min_fragments <= self.max_fragments
                && self.max_fragments <= 512
                && (2..=64).contains(&self.max_segments),
            "invalid session index fragment bounds"
        );
        // Admission is a workspace estimate, not a cap on native allocations.
        ensure!(
            budget >= WORKSPACE,
            "session index workspace exceeds shared budget"
        );
        Ok(())
    }

    async fn action(&self, dataset: &Dataset) -> Result<Option<Action>> {
        ensure!(
            dataset.manifest().uses_stable_row_ids(),
            "session index maintenance requires stable row IDs"
        );
        let indices = dataset.load_indices().await?;
        let mut covered = BTreeSet::new();
        for index in indices.iter().filter(|i| i.name == NAME) {
            validate_metadata(dataset, index)?;
            covered.extend(index.fragment_bitmap.as_ref().unwrap().iter());
        }
        let segments: Vec<_> = indices.iter().filter(|i| i.name == NAME).cloned().collect();
        if segments.len() >= self.max_segments {
            return Ok(Some(Action::Merge(segments)));
        }
        let mut selected = Vec::new();
        let mut rows = 0usize;
        for fragment in dataset.manifest().fragments.iter() {
            let id = u32::try_from(fragment.id)?;
            if covered.contains(&id) || fragment.deletion_file.is_some() {
                continue;
            }
            let Some(count) = fragment.physical_rows.filter(|n| *n > 0) else {
                continue;
            };
            if count > MAX_ROWS - rows {
                continue;
            }
            selected.push(id);
            rows += count;
            if selected.len() == self.max_fragments {
                break;
            }
        }
        Ok((selected.len() >= self.min_fragments).then_some(Action::Build(selected)))
    }
}

enum Action {
    Build(Vec<u32>),
    Merge(Vec<IndexMetadata>),
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Change {
    read_version: u64,
    index: Vec<u8>,
    removed: Vec<Uuid>,
}

fn validate_metadata(dataset: &Dataset, index: &IndexMetadata) -> Result<()> {
    let field = dataset
        .schema()
        .field("rollout_id")
        .context("missing session column")?;
    ensure!(
        index.name == NAME
            && index.fields == vec![field.id]
            && index.covering_fields.is_empty()
            && !index.uuid.is_nil()
            && index.base_id.is_none()
            && index.dataset_version <= dataset.version().version
            && index
                .index_details
                .as_ref()
                .is_some_and(|d| d.type_url.ends_with("BTreeIndexDetails"))
            && index
                .fragment_bitmap
                .as_ref()
                .is_some_and(|b| !b.is_empty()),
        "incompatible managed session index metadata"
    );
    Ok(())
}

pub(super) async fn plan(publisher: &Publisher, view: &p::Table) -> Result<Option<p::Plan>> {
    let Some(options) = &publisher.options.session_index else {
        return Ok(None);
    };
    let version = view.progress.minimum_read_version;
    let dataset = publisher.read_dataset(Some(version)).await?;
    if options.action(&dataset).await?.is_none() {
        return Ok(None);
    }
    Ok(Some(p::Plan {
        transaction: Uuid::new_v4(),
        read_version: version,
        sources: BTreeMap::new(),
        next_group: *publisher.ingress.layout.groups.keys().next().unwrap(),
    }))
}

pub(super) async fn prepare(
    publisher: &Publisher,
    pending: &p::PendingPlan,
) -> Result<Option<p::Descriptors>> {
    let Some(options) = &publisher.options.session_index else {
        return Ok(None);
    };
    let mut dataset = publisher
        .read_dataset(Some(pending.plan.read_version))
        .await?;
    let Some(action) = options.action(&dataset).await? else {
        return Ok(None);
    };
    let reservation = publisher.budget.reserve(WORKSPACE).await;
    let phases = publisher.phases.clone();
    // Cancellation cannot release admission while a native index build still runs.
    let descriptors = tokio::spawn(async move {
        let _reservation = reservation;
        let (coverage, removed): (BTreeSet<_>, Vec<_>) = match &action {
            Action::Build(ids) => (ids.iter().copied().collect(), Vec::new()),
            Action::Merge(segments) => (
                segments
                    .iter()
                    .flat_map(|i| i.fragment_bitmap.as_ref().unwrap().iter())
                    .collect(),
                segments.iter().map(|i| i.uuid).collect(),
            ),
        };
        let index = phases
            .measure(Phase::BlobWrite, async {
                match action {
                    Action::Build(ids) => {
                        dataset
                            .create_index_builder(
                                &["rollout_id"],
                                IndexType::BTree,
                                &ScalarIndexParams::default(),
                            )
                            .name(NAME.to_owned())
                            .replace(true)
                            .fragments(ids)
                            .execute_uncommitted()
                            .await
                    }
                    Action::Merge(segments) => {
                        dataset.merge_existing_index_segments(segments).await
                    }
                }
            })
            .await?;
        validate_metadata(&dataset, &index)?;
        ensure!(
            !removed.is_empty() || index.dataset_version == dataset.version().version,
            "index snapshot changed"
        );
        ensure!(
            index
                .fragment_bitmap
                .as_ref()
                .unwrap()
                .iter()
                .collect::<BTreeSet<_>>()
                == coverage,
            "index coverage differs from selected fragments"
        );
        let change = Change {
            read_version: dataset.version().version,
            index: pb::IndexMetadata::from(&index).encode_to_vec(),
            removed,
        };
        p::Descriptors::lance_index(Bytes::from(serde_json::to_vec(&change)?))
    })
    .await??;
    Ok(Some(descriptors))
}

pub(super) async fn operation(
    dataset: &Dataset,
    descriptors: &p::Descriptors,
) -> Result<Operation> {
    ensure!(
        descriptors.format == 3 && dataset.manifest().uses_stable_row_ids(),
        "unsupported index publication"
    );
    let change: Change = serde_json::from_slice(&descriptors.bytes)?;
    ensure!(
        change.read_version == dataset.version().version,
        "index snapshot changed"
    );
    let index = IndexMetadata::try_from(pb::IndexMetadata::decode(change.index.as_slice())?)?;
    validate_metadata(dataset, &index)?;
    ensure!(
        !change.removed.is_empty() || index.dataset_version == dataset.version().version,
        "index snapshot changed"
    );
    let covered = index.fragment_bitmap.as_ref().unwrap();
    let live: BTreeSet<_> = dataset.manifest().fragments.iter().map(|f| f.id).collect();
    ensure!(
        covered.iter().all(|id| live.contains(&u64::from(id))),
        "index covers foreign fragments"
    );
    let remove: BTreeSet<_> = change.removed.iter().copied().collect();
    ensure!(
        remove.len() == change.removed.len() && remove.len() <= 64,
        "invalid index consolidation sources"
    );
    let mut removed_indices = Vec::new();
    let mut removed_coverage = BTreeSet::new();
    for old in dataset
        .load_indices()
        .await?
        .iter()
        .filter(|i| i.name == NAME)
    {
        validate_metadata(dataset, old)?;
        ensure!(
            old.uuid != index.uuid,
            "index reused an existing segment identity"
        );
        if remove.contains(&old.uuid) {
            removed_coverage.extend(old.fragment_bitmap.as_ref().unwrap().iter());
            removed_indices.push(old.clone());
        } else {
            ensure!(
                old.fragment_bitmap
                    .as_ref()
                    .unwrap()
                    .iter()
                    .all(|id| !covered.contains(id)),
                "index segment overlaps existing coverage"
            );
        }
    }
    ensure!(
        removed_indices.len() == remove.len(),
        "index consolidation source changed"
    );
    if !remove.is_empty() {
        ensure!(
            remove.len() >= 2 && covered.iter().collect::<BTreeSet<_>>() == removed_coverage,
            "index consolidation changed coverage"
        );
    }
    Ok(Operation::CreateIndex {
        new_indices: vec![index],
        removed_indices,
    })
}
