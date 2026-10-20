use super::*;
use crate::record::RecordKind;
use bincode::Options;
use raft_proto::eraftpb::EntryType;

fn codec() -> impl Options {
    bincode::DefaultOptions::new()
        .with_fixint_encoding()
        .with_little_endian()
        .with_limit(256)
        .reject_trailing_bytes()
}
pub(super) fn encode_meta(meta: &BatchMeta) -> Result<Vec<u8>> {
    Ok(codec().serialize(meta)?)
}
pub(super) fn decode_meta(bytes: &[u8]) -> Result<BatchMeta> {
    ensure!(bytes.len() <= 256, "oversized record metadata");
    Ok(codec().deserialize(bytes)?)
}
pub(super) fn validate_meta(
    layout: &Layout,
    group: u64,
    limits: &Limits,
    meta: &BatchMeta,
    body: &[u8],
    digest: bool,
) -> Result<()> {
    if meta.kind == RecordKind::MultiPublication {
        meta.validate_payload(body, publication::MAX_COMMAND_BYTES)?;
        return publication::validate_record(layout, group, limits, meta, body);
    }
    validate_identity(layout, group, meta)?;
    if digest {
        meta.validate_payload(body, limits.max_record_bytes as usize)?;
    } else {
        meta.check_body_length(body.len(), limits.max_record_bytes as usize)?;
    }
    Ok(())
}

fn validate_identity(layout: &Layout, group: u64, meta: &BatchMeta) -> Result<()> {
    ensure!(
        meta.kind == RecordKind::Data
            && !meta.stream.is_nil()
            && !meta.producer.is_nil()
            && meta.sequence < u64::MAX
            && layout.group_for_bucket(meta.bucket)? == group,
        "invalid data identity, record kind or group placement"
    );
    Ok(())
}
pub(super) fn validate_entry(
    layout: &Layout,
    group: u64,
    limits: &Limits,
    entry: &Entry,
    digest: bool,
) -> Result<()> {
    ensure!(
        entry.get_entry_type() == EntryType::EntryNormal,
        "membership changes not integrated"
    );
    if entry.data.is_empty() {
        ensure!(
            entry.context.is_empty(),
            "empty data with nonempty metadata"
        );
        return Ok(());
    }
    validate_meta(
        layout,
        group,
        limits,
        &decode_meta(&entry.context)?,
        &entry.data,
        digest,
    )
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(super) struct State {
    pub catalog: publication::Catalog,
    pub applied: u64,
    pub accepted: BTreeMap<u64, BatchMeta>,
    pub producers: BTreeMap<(Uuid, u32, Uuid), (BatchMeta, u64)>,
}
impl State {
    pub fn page(&self, after: u64, limit: usize) -> Result<Page> {
        ensure!((1..=128).contains(&limit), "invalid accepted page limit");
        ensure!(
            after <= self.applied,
            "cursor exceeds local applied position"
        );
        let mut remaining = self
            .accepted
            .range((std::ops::Bound::Excluded(after), std::ops::Bound::Unbounded));
        let entries: Vec<_> = remaining
            .by_ref()
            .take(limit)
            .map(|(index, meta)| (*index, meta.clone()))
            .collect();
        let through = if remaining.next().is_some() {
            entries.last().expect("positive page limit").0
        } else {
            self.applied
        };
        Ok(Page { entries, through })
    }

    /// The image retains every accepted record. Producer tails must exactly
    /// match those records, including sequence continuity within each lane.
    /// Retirement will need a new format with independently retained tails.
    pub fn validate_checkpoint(
        &self,
        layout: &Layout,
        group: u64,
        limits: &Limits,
        cut: u64,
    ) -> Result<()> {
        ensure!(
            cut > 0 && cut < u64::MAX && self.applied == cut,
            "invalid application checkpoint cut"
        );
        ensure!(
            self.accepted.len() as u64 <= limits.max_records
                && self.producers.len() as u64 <= limits.max_producers,
            "application checkpoint exceeds state limits"
        );
        let mut tails = BTreeMap::new();
        for (index, meta) in &self.accepted {
            ensure!(
                *index > 0 && *index <= cut,
                "accepted index outside checkpoint"
            );
            validate_identity(layout, group, meta)?;
            meta.check_body_length(
                usize::try_from(meta.body_bytes)?,
                limits.max_record_bytes as usize,
            )?;
            let key = (meta.stream, meta.bucket, meta.producer);
            let expected = tails
                .get(&key)
                .map_or(1, |(old, _): &(BatchMeta, u64)| old.sequence + 1);
            ensure!(
                meta.sequence == expected,
                "checkpoint producer sequence gap"
            );
            tails.insert(key, (meta.clone(), *index));
        }
        ensure!(
            tails == self.producers,
            "checkpoint producer tails disagree"
        );
        self.catalog.validate(layout, group, cut, limits)?;
        Ok(())
    }

    pub fn apply(&mut self, entry: &Entry, limits: &Limits) -> Result<Option<Applied>> {
        ensure!(
            entry.index == self.applied + 1,
            "noncontiguous committed apply"
        );
        self.applied = entry.index;
        if entry.data.is_empty() {
            return Ok(None);
        }
        let meta = decode_meta(&entry.context)?;
        if meta.kind == RecordKind::MultiPublication {
            return Ok(Some(Applied::Publisher(self.catalog.apply(
                publication::decode(&entry.data)?,
                entry.index,
                limits,
            ))));
        }
        let key = (meta.stream, meta.bucket, meta.producer);
        let previous = self.producers.get(&key);
        if let Some((old, index)) = previous {
            if *old == meta {
                return Ok(Some(Applied::Accepted { index: *index }));
            }
        }
        let expected_sequence = previous.map_or(1, |(old, _)| old.sequence + 1);
        if meta.sequence != expected_sequence {
            return Ok(Some(Applied::Rejected { expected_sequence }));
        }
        if self.accepted.len() >= limits.max_records as usize
            || (previous.is_none() && self.producers.len() >= limits.max_producers as usize)
        {
            return Ok(Some(Applied::Capacity));
        }
        self.producers.insert(key, (meta.clone(), entry.index));
        self.accepted.insert(entry.index, meta);
        Ok(Some(Applied::Accepted { index: entry.index }))
    }
}
