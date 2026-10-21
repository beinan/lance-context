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
    if meta.kind == RecordKind::Producer {
        meta.validate_payload(body, producer::MAX_COMMAND_BYTES)?;
        return producer::validate_record(layout, group, limits, meta, body);
    }
    if meta.kind == RecordKind::Source {
        meta.validate_payload(body, source::MAX_BYTES)?;
        return source::validate_record(layout, group, limits, meta, body);
    }
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

pub(super) type ProducerKey = (Uuid, u32, Uuid);
pub(super) const MAX_RETIRED_BYTES: usize = 64;

/// The last sequence removed from a lane's payload prefix. Latest receipts
/// remain in `producers`, independently of whether their payload is retained.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub(super) struct Retired {
    pub sequence: u64,
    pub index: u64,
}
pub(super) fn encode_retired(key: ProducerKey, retired: Retired) -> Result<Vec<u8>> {
    // UUID's serde representation adds a length prefix. Store the two fixed
    // arrays explicitly: 16 + 4 + 16 + 8 + 8 = 52 bytes per producer boundary.
    let bytes = codec().serialize(&(key.0.as_bytes(), key.1, key.2.as_bytes(), retired))?;
    ensure!(
        bytes.len() <= MAX_RETIRED_BYTES,
        "retired boundary byte limit"
    );
    Ok(bytes)
}
pub(super) fn decode_retired(bytes: &[u8]) -> Result<(ProducerKey, Retired)> {
    ensure!(
        bytes.len() <= MAX_RETIRED_BYTES,
        "retired boundary byte limit"
    );
    let (table, bucket, producer, retired): ([u8; 16], u32, [u8; 16], Retired) =
        codec().deserialize(bytes)?;
    let key = (Uuid::from_bytes(table), bucket, Uuid::from_bytes(producer));
    ensure!(
        encode_retired(key, retired)? == bytes,
        "noncanonical retired boundary"
    );
    Ok((key, retired))
}

#[cfg(test)]
mod retired_encoding_tests {
    use super::*;

    #[test]
    fn retired_boundary_has_a_fixed_52_byte_little_endian_frame() -> Result<()> {
        let key = (
            Uuid::from_bytes([0x11; 16]),
            0x01020304,
            Uuid::from_bytes([0x22; 16]),
        );
        let retired = Retired {
            sequence: 0x0102030405060708,
            index: 9,
        };
        let mut expected = vec![0x11; 16];
        expected.extend_from_slice(&[4, 3, 2, 1]);
        expected.extend_from_slice(&[0x22; 16]);
        expected.extend_from_slice(&[8, 7, 6, 5, 4, 3, 2, 1, 9, 0, 0, 0, 0, 0, 0, 0]);
        assert_eq!(expected.len(), 52);
        assert_eq!(encode_retired(key, retired)?, expected);
        assert_eq!(decode_retired(&expected)?, (key, retired));
        assert!(decode_retired(&expected[..51]).is_err());
        expected.push(0);
        assert!(decode_retired(&expected).is_err());
        Ok(())
    }
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
    pub producer_slots: Vec<producer::Cell>,
    pub retired: BTreeMap<ProducerKey, Retired>,
    pub closed: BTreeMap<Uuid, Arc<source::Closed>>,
    pub pins: BTreeMap<Uuid, Arc<source::Pin>>,
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

    /// Retained lane sequences continue from a durable retirement boundary.
    /// Latest receipts survive even when their complete prefix has been retired.
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
                && self.producers.len() as u64 <= limits.max_producers
                && self.retired.len() <= self.producers.len(),
            "application checkpoint exceeds state limits"
        );
        self.validate_producers(layout, group, limits)?;
        self.catalog.validate(layout, group, cut, limits)?;
        source::validate_state(self, layout, group, cut, limits)?;
        for (key, (meta, index)) in &self.producers {
            validate_identity(layout, group, meta)?;
            meta.check_body_length(
                usize::try_from(meta.body_bytes)?,
                limits.max_record_bytes as usize,
            )?;
            ensure!(
                *key == (meta.stream, meta.bucket, meta.producer)
                    && meta.sequence > 0
                    && *index > 0
                    && *index <= cut,
                "invalid producer receipt identity or position"
            );
        }
        for (key, retired) in &self.retired {
            let (latest, index) = self
                .producers
                .get(key)
                .context("retired lane lacks latest receipt")?;
            let closed = self
                .closed
                .get(&key.0)
                .context("retired lane lacks source closure")?;
            ensure!(
                retired.sequence > 0
                    && retired.sequence <= latest.sequence
                    && retired.index > 0
                    && retired.index <= closed.permit.closure().through
                    && ((retired.sequence == latest.sequence && retired.index == *index)
                        || (retired.sequence < latest.sequence && retired.index < *index)),
                "retired boundary disagrees with source closure or latest receipt"
            );
        }
        let mut tails = BTreeMap::new();
        for (index, meta) in &self.accepted {
            ensure!(
                *index > 0
                    && *index <= cut
                    && self
                        .closed
                        .get(&meta.stream)
                        .is_none_or(|c| *index > c.permit.closure().through),
                "accepted index outside checkpoint"
            );
            validate_identity(layout, group, meta)?;
            meta.check_body_length(
                usize::try_from(meta.body_bytes)?,
                limits.max_record_bytes as usize,
            )?;
            let key = (meta.stream, meta.bucket, meta.producer);
            ensure!(
                self.producers.contains_key(&key),
                "accepted lane lacks latest receipt"
            );
            let expected = tails.get(&key).map_or_else(
                || self.retired.get(&key).map_or(1, |r| r.sequence + 1),
                |(old, _): &(BatchMeta, u64)| old.sequence + 1,
            );
            ensure!(
                meta.sequence == expected,
                "checkpoint producer sequence gap"
            );
            tails.insert(key, (meta.clone(), *index));
        }
        for (key, latest) in &self.producers {
            ensure!(
                match tails.get(key) {
                    Some(retained) => retained == latest,
                    None => self
                        .retired
                        .get(key)
                        .is_some_and(|r| r.sequence == latest.0.sequence && r.index == latest.1),
                },
                "checkpoint producer tail has no retained or retired evidence"
            );
        }
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
        if meta.kind == RecordKind::Source {
            return Ok(Some(Applied::Source(source::apply(
                self,
                source::decode(&entry.data)?,
                entry.index,
                limits,
            )?)));
        }
        if meta.kind == RecordKind::MultiPublication {
            return Ok(Some(Applied::Publisher(self.catalog.apply(
                publication::decode(&entry.data)?,
                entry.index,
                limits,
            ))));
        }
        if meta.kind == RecordKind::Producer {
            return Ok(Some(Applied::Producer(
                self.apply_producer(producer::decode(&entry.data)?),
            )));
        }
        if !self.producer_admission(&meta, true) {
            return Ok(Some(Applied::ProducerFenced));
        }
        let key = (meta.stream, meta.bucket, meta.producer);
        let previous = self.producers.get(&key);
        if let Some((old, index)) = previous {
            if *old == meta {
                return Ok(Some(Applied::Accepted { index: *index }));
            }
        }
        if !self.producer_admission(&meta, false) {
            return Ok(Some(Applied::ProducerFenced));
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
