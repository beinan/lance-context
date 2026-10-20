//! Catalog-authorized closure of an exact source pin. Closure persists a
//! monotonic fence before releasing the pin and retiring its exact payload range.
use super::*;

const DOMAIN: &str = "lance-context/catalog-source-closure/v1";
pub(in crate::replication) const MAX_CLOSED_BYTES: usize = 2048;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Closure {
    pub table: Uuid,
    pub transaction: Uuid,
    pub plan_index: u64,
    pub group: u64,
    pub after: u64,
    pub through: u64,
    pub selection_digest: [u8; 32],
    pub incarnation: Uuid,
    pub manifest_identity: Uuid,
    pub version: u64,
    pub manifest_index: u64,
    pub published_index: u64,
}

fn selection_digest(selection: &Selection) -> Result<[u8; 32]> {
    Ok(Sha256::digest(
        codec(MAX_BYTES).serialize(&("lance-context/source-selection/v1", selection))?,
    )
    .into())
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ClosurePermit {
    policy: [u8; 32],
    cut: query::Cut,
    closure: Closure,
    signature: ([u8; 32], [u8; 32]),
}
impl ClosurePermit {
    pub fn closure(&self) -> &Closure {
        &self.closure
    }
    pub fn catalog_cut(&self) -> &query::Cut {
        &self.cut
    }
    fn signed_bytes(&self) -> Result<Vec<u8>> {
        Ok(codec(MAX_CLOSED_BYTES).serialize(&(DOMAIN, self.policy, &self.cut, &self.closure))?)
    }
    pub(in crate::replication) fn issue(
        signer: &CatalogSigner,
        cut: query::Cut,
        layout: &Layout,
        limits: &Limits,
        table: &publication::Table,
        group: u64,
    ) -> Result<Self> {
        ensure!(
            limits.catalog_key == Some(signer.public_key()),
            "catalog signing policy mismatch"
        );
        let published = table
            .progress
            .published
            .as_ref()
            .context("publication unresolved")?;
        published.validate(table, cut.applied)?;
        let pending = table
            .progress
            .pending
            .as_ref()
            .context("source selection missing")?;
        let range = pending
            .plan
            .sources
            .get(&group)
            .context("source not selected")?;
        let selection = Selection {
            table: table.authority.table,
            transaction: pending.plan.transaction,
            plan_index: pending.index,
            read_version: pending.plan.read_version,
            group,
            ingest: table.ingest.clone(),
            range: range.clone(),
        };
        let mut permit = Self {
            policy: Sha256::digest(limits.identity()?).into(),
            cut,
            closure: Closure {
                table: selection.table,
                transaction: selection.transaction,
                plan_index: selection.plan_index,
                group,
                after: range.after,
                through: range.through,
                selection_digest: selection_digest(&selection)?,
                incarnation: published.incarnation,
                manifest_identity: published.manifest.identity,
                version: published.manifest.version,
                manifest_index: published.manifest.index,
                published_index: published.index,
            },
            signature: ([0; 32], [0; 32]),
        };
        let signature = signer.0.sign(&permit.signed_bytes()?);
        permit.signature = (
            signature.as_ref()[..32].try_into()?,
            signature.as_ref()[32..].try_into()?,
        );
        permit.verify(layout, limits, group)?;
        Ok(permit)
    }
    pub(super) fn verify(&self, layout: &Layout, limits: &Limits, group: u64) -> Result<()> {
        let key = limits
            .catalog_key
            .context("closure needs catalog verification key")?;
        ensure!(
            self.policy == <[u8; 32]>::from(Sha256::digest(limits.identity()?)),
            "foreign closure policy"
        );
        let mut signature = [0; 64];
        signature[..32].copy_from_slice(&self.signature.0);
        signature[32..].copy_from_slice(&self.signature.1);
        UnparsedPublicKey::new(&ED25519, key)
            .verify(&self.signed_bytes()?, &signature)
            .map_err(|_| anyhow::anyhow!("invalid catalog closure attestation"))?;
        let c = &self.closure;
        ensure!(
            self.cut.cluster == layout.cluster
                && self.cut.layout == layout.fingerprint()?
                && self.cut.group == publication::control_group(layout)
                && self.cut.term > 0
                && self.cut.quorum_index > 0
                && self.cut.quorum_index <= self.cut.applied
                && self.cut.applied < u64::MAX
                && c.plan_index > 0
                && c.plan_index < c.manifest_index
                && c.manifest_index < c.published_index
                && c.published_index <= self.cut.applied
                && !c.table.is_nil()
                && !c.transaction.is_nil()
                && !c.incarnation.is_nil()
                && !c.manifest_identity.is_nil()
                && c.selection_digest != [0; 32]
                && c.group == group
                && layout.groups.contains_key(&group)
                && c.after < c.through
                && c.through < u64::MAX
                && c.version > 1
                && c.version < (1u64 << 63)
                && (group != self.cut.group || c.through < c.plan_index),
            "invalid source closure scope or cut"
        );
        Ok(())
    }
    pub(in crate::replication) fn batch(self, layout: &Layout, limits: &Limits) -> Result<Batch> {
        self.verify(layout, limits, self.closure.group)?;
        let c = &self.closure;
        let mut batch = Batch::new(
            c.table,
            0,
            c.transaction,
            c.plan_index,
            Bytes::from(codec(MAX_BYTES).serialize(&Command::Close(self))?),
        );
        batch.meta.kind = RecordKind::Source;
        Ok(batch)
    }
}

/// Bounded by registered tables, not active pin slots. The signed statement is
/// compact and never retains the selected source metadata or payload buffers.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Closed {
    pub index: u64,
    pub pin_index: u64,
    pub permit: ClosurePermit,
}

pub(super) fn apply(
    state: &mut State,
    permit: ClosurePermit,
    index: u64,
    limits: &Limits,
) -> Result<Reply> {
    let c = &permit.closure;
    if let Some(old) = state.closed.get(&c.table) {
        if old.permit.closure == *c {
            return Ok(Reply::Closed { index: old.index });
        }
        if c.plan_index <= old.permit.closure.plan_index {
            return Ok(Reply::Rejected(Rejection::Closed));
        }
        if c.after != old.permit.closure.through
            || c.version <= old.permit.closure.version
            || c.incarnation != old.permit.closure.incarnation
        {
            return Ok(Reply::Rejected(Rejection::Prefix));
        }
    } else if c.after != 0 {
        return Ok(Reply::Rejected(Rejection::Prefix));
    }
    let Some(pin) = state.pins.get(&c.table) else {
        return Ok(Reply::Rejected(Rejection::Pending));
    };
    let selection = pin.permit.selection();
    if c.selection_digest != selection_digest(selection)?
        || c.table != selection.table
        || c.transaction != selection.transaction
        || c.plan_index != selection.plan_index
        || c.group != selection.group
        || c.after != selection.range.after
        || c.through != selection.range.through
        || c.version != selection.read_version + 1
    {
        return Ok(Reply::Rejected(Rejection::Prefix));
    }
    if !state.closed.contains_key(&c.table) && state.closed.len() as u64 >= limits.max_tables {
        return Ok(Reply::Rejected(Rejection::Capacity));
    }
    for (i, meta) in &selection.range.records {
        ensure!(
            state.accepted.get(i) == Some(meta),
            "pinned source payload metadata missing"
        );
    }
    let table = c.table;
    let pin_index = pin.index;
    state.closed.insert(
        table,
        Arc::new(Closed {
            index,
            pin_index,
            permit,
        }),
    );
    let pin = state.pins.remove(&table).expect("verified source pin");
    for (i, meta) in &pin.permit.selection().range.records {
        state.retired.insert(
            (meta.stream, meta.bucket, meta.producer),
            state::Retired {
                sequence: meta.sequence,
                index: *i,
            },
        );
        state.accepted.remove(i);
    }
    // Latest producer receipts survive even if their payload has been retired.
    Ok(Reply::Closed { index })
}

pub(super) fn validate_state(
    state: &State,
    layout: &Layout,
    group: u64,
    cut: u64,
    limits: &Limits,
) -> Result<()> {
    ensure!(
        state.closed.len() as u64 <= limits.max_tables,
        "closed-source table capacity"
    );
    for (table, closed) in &state.closed {
        closed.permit.verify(layout, limits, group)?;
        ensure!(
            *table == closed.permit.closure.table
                && closed.index <= cut
                && closed.index > closed.pin_index
                && closed.pin_index > closed.permit.closure.through,
            "invalid closed-source checkpoint position"
        );
    }
    Ok(())
}
pub(in crate::replication) fn encode_closed(closed: &Closed) -> Result<Vec<u8>> {
    Ok(codec(MAX_CLOSED_BYTES).serialize(closed)?)
}
pub(in crate::replication) fn decode_closed(bytes: &[u8]) -> Result<Closed> {
    ensure!(
        bytes.len() <= MAX_CLOSED_BYTES,
        "oversized closed-source frame"
    );
    let closed = codec(MAX_CLOSED_BYTES).deserialize(bytes)?;
    ensure!(
        encode_closed(&closed)? == bytes,
        "noncanonical closed-source frame"
    );
    Ok(closed)
}
