//! A source quorum's durable closure, distinct from a catalog permit to close.
use super::*;

const DOMAIN: &str = "lance-context/source-closed-proof/v1";
const MAX_PROOF_BYTES: usize = 4096;

/// Private signing key shared only by the replicas of one source group.
/// This has no public signing operation and does not contain the catalog key.
pub struct SourceSigner(Ed25519KeyPair);
impl SourceSigner {
    pub fn from_pkcs8(bytes: &[u8]) -> Result<Self> {
        Ok(Self(Ed25519KeyPair::from_pkcs8(bytes).map_err(|_| {
            anyhow::anyhow!("invalid source signing key")
        })?))
    }

    pub fn public_key(&self) -> [u8; 32] {
        self.0
            .public_key()
            .as_ref()
            .try_into()
            .expect("Ed25519 key")
    }
}

/// Issued only from a successful source ReadIndex completion. It attests the
/// exact persisted closure and its original source index. It is not a lease,
/// publisher authority or permission to delete Lance objects or log files.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ClosedProof {
    policy: [u8; 32],
    cut: query::Cut,
    closed: Arc<Closed>,
    signature: ([u8; 32], [u8; 32]),
}
impl ClosedProof {
    pub fn source_cut(&self) -> &query::Cut {
        &self.cut
    }

    pub fn closed(&self) -> &Closed {
        &self.closed
    }

    fn signed_bytes(&self) -> Result<Vec<u8>> {
        Ok(codec(MAX_PROOF_BYTES).serialize(&(DOMAIN, self.policy, &self.cut, &self.closed))?)
    }

    pub(in crate::replication) fn issue(
        signer: &SourceSigner,
        cut: query::Cut,
        layout: &Layout,
        limits: &Limits,
        closed: Arc<Closed>,
    ) -> Result<Self> {
        ensure!(
            limits.source_keys.get(&cut.group) == Some(&signer.public_key()),
            "source signing policy mismatch"
        );
        let mut proof = Self {
            policy: Sha256::digest(limits.identity()?).into(),
            cut,
            closed,
            signature: ([0; 32], [0; 32]),
        };
        let signature = signer.0.sign(&proof.signed_bytes()?);
        proof.signature = (
            signature.as_ref()[..32].try_into()?,
            signature.as_ref()[32..].try_into()?,
        );
        proof.verify(layout, limits)?;
        Ok(proof)
    }

    /// Authenticate the scoped statement. A consumer must additionally match
    /// its exact expected table, plan and publication before advancing state.
    pub fn verify(&self, layout: &Layout, limits: &Limits) -> Result<()> {
        let key = limits
            .source_keys
            .get(&self.cut.group)
            .context("source verification key unavailable")?;
        ensure!(
            self.policy == <[u8; 32]>::from(Sha256::digest(limits.identity()?)),
            "foreign source proof policy"
        );
        let mut signature = [0; 64];
        signature[..32].copy_from_slice(&self.signature.0);
        signature[32..].copy_from_slice(&self.signature.1);
        UnparsedPublicKey::new(&ED25519, key)
            .verify(&self.signed_bytes()?, &signature)
            .map_err(|_| anyhow::anyhow!("invalid source closure proof"))?;
        self.closed.permit.verify(layout, limits, self.cut.group)?;
        ensure!(
            self.cut.cluster == layout.cluster
                && self.cut.layout == layout.fingerprint()?
                && self.cut.term > 0
                && self.cut.quorum_index > 0
                && self.cut.quorum_index <= self.cut.applied
                && self.cut.applied < u64::MAX
                && self.closed.index <= self.cut.applied
                && self.closed.index > self.closed.pin_index
                && self.closed.pin_index > self.closed.permit.closure().through,
            "invalid source closure proof scope or cut"
        );
        Ok(())
    }
}
