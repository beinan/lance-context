use super::*;
use std::{cmp::Reverse, collections::BTreeSet};

/// How one immutable publication divides its records among preparers.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Ord, PartialOrd, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Partitioning {
    /// Round-robin records, preserving the original task identity and behavior.
    #[default]
    RecordStripe,
    /// Keep each bucket whole, assigning larger buckets to the least-loaded part.
    /// This version is part of the request identity; changing the assignment
    /// algorithm requires a new version. Enable only after preparers support it.
    BucketBytesV1,
}

impl Partitioning {
    pub(crate) fn is_record_stripe(&self) -> bool {
        *self == Self::RecordStripe
    }

    pub(super) fn parts(self, pending: &p::PendingPlan, endpoints: usize) -> usize {
        let records = pending
            .plan
            .sources
            .values()
            .flat_map(|s| s.records.iter().map(|(_, meta)| meta));
        endpoints.min(match self {
            Self::RecordStripe => records.count(),
            Self::BucketBytesV1 => records.map(|m| m.bucket).collect::<BTreeSet<_>>().len(),
        })
    }

    pub(super) fn select(
        self,
        mut inputs: Vec<(u64, u64, BatchMeta)>,
        part: usize,
        parts: usize,
    ) -> Result<Vec<(u64, u64, BatchMeta)>> {
        ensure!(
            (1..=8).contains(&parts) && part < parts && parts <= inputs.len(),
            "invalid preparation partition"
        );
        if self == Self::RecordStripe {
            return Ok(inputs
                .into_iter()
                .enumerate()
                .filter_map(|(i, record)| (i % parts == part).then_some(record))
                .collect());
        }
        let mut bytes = BTreeMap::<u32, u64>::new();
        for (_, _, meta) in &inputs {
            ensure!(meta.body_bytes > 0, "empty bucket record");
            let size = bytes.entry(meta.bucket).or_default();
            *size = size
                .checked_add(meta.body_bytes)
                .context("bucket size overflow")?;
        }
        ensure!(parts <= bytes.len(), "empty bucket partition");
        let mut buckets = bytes.into_iter().collect::<Vec<_>>();
        buckets.sort_unstable_by_key(|&(bucket, size)| (Reverse(size), bucket));
        let mut loads = vec![0u64; parts];
        let mut owners = BTreeMap::new();
        for (bucket, size) in buckets {
            let owner = (0..parts).min_by_key(|&i| (loads[i], i)).unwrap();
            loads[owner] = loads[owner]
                .checked_add(size)
                .context("partition size overflow")?;
            owners.insert(bucket, owner);
        }
        inputs.retain(|(_, _, meta)| owners[&meta.bucket] == part);
        // Sort source metadata only. The service fetches and decodes each record
        // once; payloads are never copied to compute the assignment.
        inputs.sort_unstable_by_key(|(group, index, meta)| (meta.bucket, *group, *index));
        Ok(inputs)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn record(bucket: u32, index: u64, body_bytes: u64) -> (u64, u64, BatchMeta) {
        (
            10,
            index,
            BatchMeta {
                kind: Default::default(),
                stream: Uuid::from_u128(1),
                bucket,
                producer: Uuid::from_u128(2),
                sequence: index,
                digest: [0; 32],
                body_bytes,
            },
        )
    }

    #[test]
    fn whole_buckets_are_complete_deterministic_and_byte_balanced() -> Result<()> {
        // An indivisible hot bucket may dominate a worker. Other buckets must
        // fill the other worker, rather than splitting the hot bucket's rows.
        let inputs = vec![
            record(0, 1, 70),
            record(1, 2, 10),
            record(2, 3, 9),
            record(0, 4, 30),
            record(3, 5, 8),
        ];
        let mut ids = BTreeSet::new();
        let mut owners = BTreeMap::new();
        let mut loads = Vec::new();
        for part in 0..2 {
            let selected = Partitioning::BucketBytesV1.select(inputs.clone(), part, 2)?;
            let mut reversed = inputs.clone();
            reversed.reverse();
            assert_eq!(
                selected,
                Partitioning::BucketBytesV1.select(reversed, part, 2)?
            );
            loads.push(selected.iter().map(|r| r.2.body_bytes).sum::<u64>());
            for (group, index, meta) in selected {
                assert!(ids.insert((group, index)));
                assert_eq!(*owners.entry(meta.bucket).or_insert(part), part);
            }
        }
        assert_eq!(ids.len(), inputs.len());
        assert_eq!(loads, [100, 27]);
        assert!(Partitioning::BucketBytesV1.select(inputs, 0, 5).is_err());
        Ok(())
    }

    #[test]
    fn bucket_assignment_has_a_distinct_identity_and_legacy_tasks_roundtrip() -> Result<()> {
        let old = serde_json::json!({"cluster":Uuid::from_u128(1),"layout":vec![0;32],
            "table":Uuid::from_u128(2),"incarnation":Uuid::from_u128(3),"plan_index":7,
            "transaction":Uuid::from_u128(4),"read_version":1,"part":0,"parts":2});
        let legacy: Task = serde_json::from_value(old.clone())?;
        assert_eq!(legacy.partitioning, Partitioning::RecordStripe);
        assert_eq!(serde_json::to_value(&legacy)?, old);
        let bucket = Task {
            partitioning: Partitioning::BucketBytesV1,
            ..legacy.clone()
        };
        assert_ne!(legacy.identity()?, bucket.identity()?);
        assert_eq!(
            serde_json::from_slice::<Task>(&serde_json::to_vec(&bucket)?)?,
            bucket
        );
        let mut unknown = serde_json::to_value(bucket)?;
        unknown["partitioning"] = "bucket_bytes_v2".into();
        assert!(serde_json::from_value::<Task>(unknown).is_err());
        Ok(())
    }
}
