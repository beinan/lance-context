//! Rewrite bounded small-file groups through the existing managed publication.
//! Old files remain available to captured snapshots; this does not run object GC.
use super::*;
use lance::dataset::{
    optimize::{CompactionMode, CompactionOptions, CompactionTask, RewriteResult, TaskData},
    transaction::{Operation, RewriteGroup},
};
use lance::index::DatasetIndexExt;
use lance_table::format::Fragment;
use std::collections::BTreeSet;

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct SmallFileCompaction {
    pub target_file_bytes: usize,
    pub min_fragments: usize,
}
impl Default for SmallFileCompaction {
    fn default() -> Self {
        Self {
            target_file_bytes: 128 << 20,
            min_fragments: 4,
        }
    }
}
impl SmallFileCompaction {
    fn workspace(&self, record_bytes: usize) -> Result<usize> {
        // One task admits at most twice the target on disk. Also allow for
        // four late-materialization batches and decode/encoding workspace.
        // These are targets, not an allocator/RSS cap on Lance internals.
        self.target_file_bytes
            .checked_mul(6)
            .and_then(|bytes| {
                record_bytes
                    .checked_mul(12)
                    .and_then(|rows| bytes.checked_add(rows))
            })
            .context("compaction workspace overflow")
    }
    pub(super) fn validate(&self, record_bytes: usize, budget: usize) -> Result<()> {
        ensure!(
            (64 << 10..=1 << 30).contains(&self.target_file_bytes)
                && (2..=64).contains(&self.min_fragments),
            "invalid small-file compaction bounds"
        );
        ensure!(
            self.workspace(record_bytes)? <= budget,
            "compaction workspace exceeds shared budget"
        );
        Ok(())
    }
    fn fragments(&self, dataset: &Dataset) -> Option<Vec<Fragment>> {
        let mut selected = Vec::new();
        let mut bytes = 0usize;
        for fragment in dataset.manifest().fragments.iter() {
            // Managed ingest creates append-only, sized fragments. Unknown
            // sizes and deletion rewrites need separate planning policies.
            if fragment.deletion_file.is_some() || fragment.physical_rows.is_none_or(|n| n == 0) {
                continue;
            }
            let size = fragment.files.iter().try_fold(0usize, |n, file| {
                n.checked_add(usize::try_from(file.file_size_bytes.get()?.get()).ok()?)
            });
            let Some(size) = size.filter(|n| *n > 0 && *n < self.target_file_bytes / 2) else {
                continue;
            };
            if size > self.target_file_bytes * 2 - bytes {
                continue;
            }
            selected.push(fragment.clone());
            bytes += size;
            if selected.len() == 64 {
                break;
            }
        }
        (selected.len() >= self.min_fragments).then_some(selected)
    }
}

pub(super) async fn plan(publisher: &Publisher, view: &p::Table) -> Result<Option<p::Plan>> {
    let Some(options) = &publisher.options.compaction else {
        return Ok(None);
    };
    let version = view.progress.minimum_read_version;
    let dataset = publisher.read_dataset(Some(version)).await?;
    if options.fragments(&dataset).is_none() {
        return Ok(None);
    }
    ensure!(
        dataset.load_indices().await?.is_empty(),
        "managed small-file compaction requires an unindexed table"
    );
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
) -> Result<p::Descriptors> {
    let options = publisher
        .options
        .compaction
        .as_ref()
        .context("pending compaction needs configured limits")?;
    let dataset = publisher
        .read_dataset(Some(pending.plan.read_version))
        .await?;
    ensure!(
        dataset.load_indices().await?.is_empty(),
        "managed small-file compaction requires an unindexed table"
    );
    let fragments = options
        .fragments(&dataset)
        .context("pending compaction has no eligible fragments under configured limits")?;
    let rows = fragments
        .iter()
        .try_fold(0usize, |n, f| n.checked_add(f.physical_rows?))
        .context("compaction row count overflow")?;
    let record_bytes = publisher.ingress.scope.contract.max_record_bytes as usize;
    let task = CompactionTask {
        task: TaskData { fragments },
        read_version: pending.plan.read_version,
        options: CompactionOptions {
            target_rows_per_fragment: rows,
            max_bytes_per_file: Some(options.target_file_bytes),
            num_threads: Some(1),
            batch_size: Some(1),
            io_buffer_size: Some(record_bytes as u64),
            compaction_mode: Some(CompactionMode::TryBinaryCopy),
            binary_copy_read_batch_bytes: Some(record_bytes),
            ..Default::default()
        },
    };
    let reservation = publisher
        .phases
        .measure(Phase::Admission, async {
            Ok::<_, anyhow::Error>(
                publisher
                    .budget
                    .reserve(options.workspace(record_bytes)?)
                    .await,
            )
        })
        .await?;
    let phases = publisher.phases.clone();
    // The detached task retains admission if the scheduling caller is dropped.
    // No prepared output is deleted on an error or an ambiguous later commit.
    tokio::spawn(async move {
        let _reservation = reservation;
        let result = phases
            .measure(Phase::BlobWrite, task.execute(&dataset))
            .await?;
        validate_result(&dataset, &result).await?;
        p::Descriptors::lance_compaction(Bytes::from(serde_json::to_vec(&result)?))
    })
    .await?
}

async fn validate_result(dataset: &Dataset, result: &RewriteResult) -> Result<()> {
    ensure!(
        result.read_version == dataset.version().version
            && result.original_fragments.len() >= 2
            && !result.new_fragments.is_empty()
            && result.new_fragments.len() < result.original_fragments.len()
            && result.row_addrs.is_none(),
        "invalid or ineffective compaction result"
    );
    ensure!(
        dataset.load_indices().await?.is_empty(),
        "indexed compaction requires index remapping"
    );
    let mut ids = BTreeSet::new();
    for fragment in &result.original_fragments {
        ensure!(
            ids.insert(fragment.id)
                && fragment.deletion_file.is_none()
                && dataset.manifest().fragments.iter().any(|f| f == fragment),
            "compaction source differs from fixed snapshot"
        );
    }
    let rows = |fragments: &[Fragment]| {
        fragments
            .iter()
            .try_fold(0usize, |n, f| n.checked_add(f.physical_rows?))
    };
    ensure!(
        rows(&result.original_fragments).is_some()
            && rows(&result.original_fragments) == rows(&result.new_fragments)
            && result
                .new_fragments
                .iter()
                .all(|f| f.deletion_file.is_none() && !f.files.is_empty()),
        "compaction changed row count"
    );
    Ok(())
}

pub(super) async fn operation(
    dataset: &Dataset,
    descriptors: &p::Descriptors,
) -> Result<Operation> {
    ensure!(
        descriptors.format == 2,
        "unexpected compaction descriptor format"
    );
    let result: RewriteResult = serde_json::from_slice(&descriptors.bytes)?;
    validate_result(dataset, &result).await?;
    Ok(Operation::Rewrite {
        groups: vec![RewriteGroup {
            old_fragments: result.original_fragments,
            new_fragments: result.new_fragments,
        }],
        rewritten_indices: Vec::new(),
        frag_reuse_index: None,
    })
}
