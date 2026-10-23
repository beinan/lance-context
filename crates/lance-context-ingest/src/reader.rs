//! Stream committed WAL rows first, then open Lance and exclude their row IDs.
//! Source retirement follows publication: a record retired before its fetch is
//! in the later Lance snapshot. A record fetched before retirement is emitted
//! from WAL and excluded from Lance. No reader lease or new consensus command.
use super::*;
use crate::materializer::{self, Storage};
use futures::{stream, stream::BoxStream, StreamExt, TryStreamExt};

pub type BatchStream = BoxStream<'static, Result<RecordBatch>>;

/// A complete successful stream includes every session append acknowledged
/// before the read began. Order is unspecified. Consume through EOF and propagate
/// errors: an incomplete stream must not be treated as a complete query result.
/// Concurrent payload workspace uses the shared ingress budget; metadata and the IDs
/// used for overlap removal grow with the captured tail.
pub struct SessionRead {
    pub batches: BatchStream,
}

impl Ingress {
    /// Bound concurrent WAL record fetches/decodes per session read. All reads
    /// and uploads still share the ingress byte budget. The default is one;
    /// tune against concurrent ingestion, not isolated read throughput alone.
    pub fn with_session_read_concurrency(mut self, concurrency: usize) -> Result<Self> {
        validate_concurrency(concurrency)?;
        self.session_read_concurrency = concurrency;
        Ok(self)
    }
}

pub(crate) fn validate_concurrency(concurrency: usize) -> Result<()> {
    ensure!(
        (1..=32).contains(&concurrency),
        "session read concurrency must be 1..=32"
    );
    Ok(())
}

pub(crate) fn default_concurrency() -> usize {
    1
}

pub(super) struct Positions {
    group: u64,
    records: Vec<(u64, BatchMeta)>,
}

pub async fn read_session(
    ingress: &Ingress,
    storage: &Storage,
    session: &str,
) -> Result<SessionRead> {
    ensure!(!session.is_empty(), "empty session key");
    Ok(from_positions(
        ingress,
        storage,
        session,
        positions(ingress, session).await?,
    ))
}

async fn observe(
    ingress: &Ingress,
    group: u64,
    query: query::Query,
) -> Result<(query::Cut, query::Value)> {
    let response = ingress
        .client
        .request_routed(Request::Query { group, query })
        .await?;
    let Reply::Query(query::Outcome::Observed { cut, value }) = response.reply else {
        bail!("quorum observation unavailable");
    };
    ensure!(
        cut.cluster == ingress.scope.cluster
            && cut.layout == ingress.scope.layout
            && cut.group == group
            && cut.term > 0
            && cut.quorum_index > 0
            && cut.applied >= cut.quorum_index,
        "foreign quorum observation"
    );
    Ok((cut, value))
}

pub(super) async fn positions(ingress: &Ingress, session: &str) -> Result<Positions> {
    let bucket = ingress.bucket_for_session(session)?;
    let group = ingress.group(bucket)?;
    let mut result = Positions {
        group,
        records: vec![],
    };
    let mut after = 0;
    let mut upper = None;
    loop {
        let (cut, value) = observe(
            ingress,
            group,
            query::Query::AcceptedBucket {
                table: ingress.scope.table,
                bucket,
                after,
                limit: 128,
            },
        )
        .await?;
        let upper = *upper.get_or_insert(cut.applied);
        let query::Value::Accepted(page) = value else {
            bail!("source page unavailable");
        };
        ensure!(
            page.through >= after && page.through <= cut.applied,
            "invalid source cursor"
        );
        for (index, meta) in page.entries {
            ensure!(
                index > after && index <= page.through,
                "record outside source page"
            );
            if index > upper
                || meta.stream != ingress.scope.table
                || meta.bucket != bucket
                || meta.kind != RecordKind::Data
            {
                continue;
            }
            let bytes = usize::try_from(meta.body_bytes)?;
            ensure!(
                bytes <= ingress.scope.contract.max_record_bytes as usize,
                "oversized source record"
            );
            ensure!(
                workspace(&ingress.scope.contract, bytes)? <= ingress.budget.limit(),
                "WAL record exceeds read memory budget"
            );
            result.records.push((index, meta));
        }
        if page.through >= upper {
            return Ok(result);
        }
        ensure!(page.through > after, "source cursor did not advance");
        after = page.through;
    }
}

pub(super) fn from_positions(
    ingress: &Ingress,
    storage: &Storage,
    session: &str,
    positions: Positions,
) -> SessionRead {
    let ingress = ingress.clone();
    let storage = storage.clone();
    let session = session.to_owned();
    let batches = async_stream::try_stream! {
        let group = positions.group;
        let mut ids = HashSet::new();
        let mut missing_through = 0;
        // Unordered completion lets a ready record release its reservation even
        // if an earlier record is waiting for the same shared byte budget.
        let mut records = stream::iter(positions.records.into_iter().enumerate())
            .map(|(ordinal, (index, meta))| fetch(&ingress, group, index, meta, ordinal))
            .buffer_unordered(ingress.session_read_concurrency);
        while let Some((index, fetched, reservation)) = records.try_next().await? {
            if let Some(batches) = fetched {
                for batch in batches {
                    let sessions = strings(&batch, "rollout_id")?;
                    let keys = strings(&batch, "id")?;
                    for selected in slices(&batch, |row| sessions.value(row) == session && ids.insert(keys.value(row).to_owned())) {
                        yield selected;
                    }
                }
            } else { missing_through = missing_through.max(index); }
            drop(reservation);
        }
        let (_, value) = observe(&ingress, group, query::Query::SourceClosure { table: ingress.scope.table }).await?;
        let closed = match value {
            query::Value::SourceClosure(closed) => closed,
            _ => Err(anyhow::anyhow!("source closure unavailable"))?,
        };
        let minimum_version = if let Some(closed) = closed {
            let closure = closed.permit.closure();
            if closure.table != ingress.scope.table || closure.group != group
                || closure.incarnation != ingress.scope.incarnation { Err(anyhow::anyhow!("foreign source closure"))?; }
            if missing_through > closure.through { Err(anyhow::anyhow!("unpublished WAL payload unavailable"))?; }
            closure.version
        } else {
            if missing_through != 0 { Err(anyhow::anyhow!("unpublished WAL payload unavailable"))?; }
            1
        };
        let dataset = materializer::read_dataset(&ingress, &storage, None).await?;
        if dataset.version().version < minimum_version { Err(anyhow::anyhow!("dataset precedes captured WAL closure"))?; }
        let batch_bytes = ingress.scope.contract.max_record_bytes as usize;
        // Output batching alone does not bound Lance's storage prefetch. Share
        // admission with uploads/WAL reads and retain it across streamed batches.
        // These are workspace targets, not an allocator limit: a Lance page or
        // an oversized row can exceed a target, and callers may retain outputs.
        let _scan_reservation = ingress.budget.reserve(workspace(&ingress.scope.contract, batch_bytes)?).await;
        let mut scanner = dataset.scan();
        // Large binary rows need a byte target, not just a row-count limit.
        scanner.batch_size_bytes(batch_bytes as u64);
        scanner.batch_readahead(1);
        scanner.io_buffer_size(batch_bytes as u64);
        scanner.fragment_readahead(1);
        scanner.materialization_readahead_bytes(batch_bytes as u64);
        scanner.filter(&format!("rollout_id = '{}'", session.replace('\'', "''")))?;
        let mut base = scanner.try_into_stream().await?;
        while let Some(batch) = base.try_next().await? {
            let keys = strings(&batch, "id")?;
            for selected in slices(&batch, |row| !ids.contains(keys.value(row))) {
                yield selected;
            }
        }
    }.boxed();
    SessionRead { batches }
}

async fn fetch(
    ingress: &Ingress,
    group: u64,
    index: u64,
    meta: BatchMeta,
    ordinal: usize,
) -> Result<(u64, Option<Vec<RecordBatch>>, Arc<MergeReservation>)> {
    let reservation = Arc::new(
        ingress
            .budget
            .reserve(workspace(
                &ingress.scope.contract,
                usize::try_from(meta.body_bytes)?,
            )?)
            .await,
    );
    let nodes = &ingress.layout.groups[&group];
    // Spread distinct records across replicas; never fetch multiple copies of
    // one record at once. Metadata was already captured with a quorum read.
    for offset in 0..nodes.len() {
        let node = nodes[(ordinal + offset) % nodes.len()];
        let Ok(response) = ingress
            .client
            .request(node, Request::Fetch { group, index })
            .await
        else {
            continue;
        };
        if let Reply::Record(Some(observed)) = response.reply {
            ensure!(
                observed == meta,
                "fetched record differs from committed source"
            );
            // Fetch already validates stored bytes. Avoid another digest scan.
            meta.check_body_length(
                response.body.len(),
                ingress.scope.contract.max_record_bytes as usize,
            )?;
            let schema = ingress.schema.clone();
            let keep = reservation.clone();
            let batches = tokio::task::spawn_blocking(move || {
                // Blocking decode may outlive a cancelled query; retain its bytes.
                let _keep = keep;
                ipc::decode(response.body, schema)
            })
            .await??;
            return Ok((index, Some(batches), reservation));
        }
    }
    Ok((index, None, reservation))
}

fn strings<'a>(batch: &'a RecordBatch, name: &str) -> Result<&'a StringArray> {
    batch
        .column_by_name(name)
        .and_then(|a| a.as_any().downcast_ref())
        .context("missing session read column")
}

fn slices(batch: &RecordBatch, mut keep: impl FnMut(usize) -> bool) -> Vec<RecordBatch> {
    let mut output = Vec::new();
    let mut start = None;
    for row in 0..batch.num_rows() {
        if keep(row) {
            start.get_or_insert(row);
        } else if let Some(first) = start.take() {
            output.push(batch.slice(first, row - first));
        }
    }
    if let Some(first) = start {
        output.push(batch.slice(first, batch.num_rows() - first));
    }
    output
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::LargeBinaryArray;

    #[test]
    fn session_filter_slices_share_binary_values() -> Result<()> {
        let payloads = Arc::new(LargeBinaryArray::from_iter_values([
            b"first".as_slice(),
            b"excluded",
            b"third",
            b"fourth",
        ]));
        let original = payloads.values().as_ptr();
        let batch = RecordBatch::try_from_iter(vec![(
            "binary_payload",
            payloads as arrow_array::ArrayRef,
        )])?;
        let selected = slices(&batch, |row| row != 1);
        assert_eq!(
            selected
                .iter()
                .map(RecordBatch::num_rows)
                .collect::<Vec<_>>(),
            vec![1, 2]
        );
        for b in selected {
            let values = b
                .column(0)
                .as_any()
                .downcast_ref::<LargeBinaryArray>()
                .unwrap();
            assert_eq!(values.values().as_ptr(), original);
        }
        Ok(())
    }
}
