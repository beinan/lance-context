//! Immutable placement and receipt scopes for an independent WAL.
//!
//! Validate configuration before routing. Persist the full layout and its
//! fingerprint with storage: changing addresses, voters or bucket ownership is a
//! migration, not a restart option. This module provides no consensus or I/O.
use std::collections::{BTreeMap, BTreeSet};

use anyhow::{ensure, Context, Result};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use uuid::Uuid;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Layout {
    pub cluster: Uuid,
    /// Stable physical identities and addresses, such as StatefulSet service DNS.
    pub nodes: BTreeMap<u64, String>,
    /// Three sorted, distinct voters per physical group; at most eight per node.
    pub groups: BTreeMap<u64, Vec<u64>>,
    /// Virtual bucket to physical group, independent of client worker count.
    pub buckets: Vec<u64>,
}

impl Layout {
    pub fn validate(&self) -> Result<()> {
        ensure!(!self.cluster.is_nil(), "nil cluster identity");
        ensure!(
            !self.nodes.is_empty() && self.nodes.len() <= 192,
            "node count out of bounds"
        );
        ensure!(
            !self.groups.is_empty() && self.groups.len() <= 64,
            "group count out of bounds"
        );
        ensure!(
            self.buckets.len().is_power_of_two() && self.buckets.len() <= 65536,
            "bucket count must be a power of two <= 65536"
        );
        let mut addresses = BTreeSet::new();
        for (id, address) in &self.nodes {
            ensure!(
                *id > 0
                    && *id <= u32::MAX as u64
                    && !address.is_empty()
                    && address.len() <= 1024
                    && addresses.insert(address),
                "invalid/duplicate node identity or address"
            );
        }
        let mut hosted = BTreeMap::<u64, usize>::new();
        for (group, voters) in &self.groups {
            ensure!(
                *group > 0 && voters.len() == 3 && voters.windows(2).all(|v| v[0] < v[1]),
                "groups require three sorted, distinct voters and a positive id"
            );
            for voter in voters {
                ensure!(self.nodes.contains_key(voter), "unknown voter");
                *hosted.entry(*voter).or_default() += 1;
            }
        }
        ensure!(
            hosted.len() == self.nodes.len() && hosted.values().all(|n| *n <= 8),
            "every node must host between one and eight physical groups"
        );
        let used: BTreeSet<_> = self.buckets.iter().copied().collect();
        ensure!(
            used == self.groups.keys().copied().collect(),
            "bucket map must use exactly the configured groups"
        );
        Ok(())
    }

    /// Validates and hashes the canonical v1 representation. Cache this at
    /// admission; do not serialize the layout for every payload on the hot path.
    pub fn fingerprint(&self) -> Result<[u8; 32]> {
        self.validate()?;
        let mut digest = Sha256::new();
        digest.update(b"lance-context/static-layout/v1");
        digest.update(serde_json::to_vec(self)?);
        Ok(digest.finalize().into())
    }

    pub fn peers(&self, group: u64) -> Result<BTreeMap<u64, String>> {
        self.groups
            .get(&group)
            .context("unknown group")?
            .iter()
            .map(|id| Ok((*id, self.nodes.get(id).context("unknown node")?.clone())))
            .collect()
    }

    pub fn group_for_bucket(&self, bucket: u32) -> Result<u64> {
        self.buckets
            .get(bucket as usize)
            .copied()
            .context("unknown virtual bucket")
    }

    pub fn session_bucket(
        &self,
        tenant: &str,
        qualified_table: &str,
        session: &str,
    ) -> Result<u32> {
        bucket_for(tenant, qualified_table, session, self.buckets.len() as u32)
    }
}

/// Stable v1 session routing using length-prefixed UTF-8 fields and SHA-256.
/// Persist the bucket count; changing it requires migration. This deliberately
/// avoids Rust's unspecified DefaultHasher algorithm and process-random seeds.
pub fn bucket_for(tenant: &str, table: &str, session: &str, bucket_count: u32) -> Result<u32> {
    ensure!(
        bucket_count.is_power_of_two(),
        "bucket count must be a power of two"
    );
    let mut hash = Sha256::new();
    hash.update(b"lance-context/session-route/v1");
    for part in [tenant, table, session] {
        hash.update((part.len() as u64).to_le_bytes());
        hash.update(part.as_bytes());
    }
    let hash = hash.finalize();
    Ok(u32::from_le_bytes(hash[..4].try_into().unwrap()) & (bucket_count - 1))
}

/// A log index is meaningful only within its cluster, layout and group.
/// This identifies a position; it is not authentication or proof of durability.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Receipt {
    pub cluster: Uuid,
    pub layout: [u8; 32],
    pub group: u64,
    pub index: u64,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn measured_layout() -> Layout {
        Layout {
            cluster: Uuid::parse_str("b2da8f23-3a69-4538-9f86-2c10a61540cc").unwrap(),
            nodes: (1..=3).map(|id| (id, format!(
                "context-wal-lab-20261018-{}.context-wal-lab-20261018.souschef.svc.cluster.local:7800", id-1))).collect(),
            groups: (1..=3).map(|id| (id, vec![1,2,3])).collect(),
            buckets: (0..1024).map(|bucket| 1 + bucket % 3).collect(),
        }
    }

    #[test]
    fn layout_fingerprint_matches_recorded_multinode_experiment() -> Result<()> {
        // Fixed compatibility vector from the real PHX protocol10 experiment.
        // Changing serialization or the hash domain must not reroute old data.
        let layout = measured_layout();
        let expected = [
            32, 110, 106, 153, 114, 83, 142, 215, 88, 81, 79, 126, 93, 225, 129, 65, 94, 127, 167,
            140, 244, 218, 38, 47, 110, 21, 222, 171, 71, 7, 133, 7,
        ];
        assert_eq!(layout.fingerprint()?, expected);
        let decoded: Layout = serde_json::from_slice(&serde_json::to_vec(&layout)?)?;
        assert_eq!(decoded.fingerprint()?, expected);
        let mut changed = layout.clone();
        changed.buckets.swap(0, 1);
        assert_ne!(changed.fingerprint()?, expected);
        let mut changed = layout;
        changed.nodes.get_mut(&1).unwrap().push_str("-changed");
        assert_ne!(changed.fingerprint()?, expected);
        Ok(())
    }

    #[test]
    fn placement_rejects_ambiguous_or_incomplete_ownership() {
        let mut layout = measured_layout();
        layout.groups.insert(1, vec![1, 1, 3]);
        assert!(layout.validate().is_err());
        let mut layout = measured_layout();
        layout.nodes.insert(2, layout.nodes[&1].clone());
        assert!(layout.validate().is_err());
        let mut layout = measured_layout();
        layout.buckets[0] = 99;
        assert!(layout.validate().is_err());
        let mut layout = measured_layout();
        layout.nodes.insert(4, "unused:7800".into());
        assert!(layout.validate().is_err());
        let mut layout = measured_layout();
        layout.groups = (1..=9).map(|id| (id, vec![1, 2, 3])).collect();
        layout.buckets = (0..16).map(|id| 1 + id % 9).collect();
        assert!(layout.validate().is_err());
        assert!(bucket_for("tenant", "table", "session", 0).is_err());
        assert!(bucket_for("tenant", "table", "session", 3).is_err());
    }

    #[test]
    fn session_routing_survives_configuration_roundtrip() -> Result<()> {
        // SHA-256 of the literal v1 prefix and three LE-u64 length=1 fields
        // a/b/c, independently checked with OpenSSL: c5f0901a5b7f96b9... .
        assert_eq!(bucket_for("a", "b", "c", 65536)?, 0xf0c5);
        let layout = measured_layout();
        let restored: Layout = serde_json::from_slice(&serde_json::to_vec(&layout)?)?;
        for session in ["session-1", "session-2", "训练会话", ""] {
            let bucket = layout.session_bucket("tenant", "namespace/table", session)?;
            assert_eq!(
                restored.session_bucket("tenant", "namespace/table", session)?,
                bucket
            );
            assert_eq!(
                restored.group_for_bucket(bucket)?,
                layout.group_for_bucket(bucket)?
            );
        }
        assert!(restored.group_for_bucket(1024).is_err());
        assert!(restored.peers(99).is_err());
        Ok(())
    }
}
