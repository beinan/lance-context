use super::*;
use crate::replication::{
    query::Cut,
    source::{CatalogSigner, Closed, ClosurePermit, SourceSigner},
};
use ring::{rand::SystemRandom, signature::Ed25519KeyPair};

mod compaction;
mod manifest_retention;

struct Fixture {
    layout: Arc<Layout>,
    limits: Limits,
    catalog: Catalog,
    signer: CatalogSigner,
    sources: BTreeMap<u64, SourceSigner>,
    confirmation: ConfirmPublication,
}

fn fixture() -> Result<Fixture> {
    let random = SystemRandom::new();
    let signer =
        CatalogSigner::from_pkcs8(Ed25519KeyPair::generate_pkcs8(&random).unwrap().as_ref())?;
    let layout = crate::replication::tests::layout();
    let mut sources = BTreeMap::new();
    for group in layout.groups.keys() {
        sources.insert(
            *group,
            SourceSigner::from_pkcs8(Ed25519KeyPair::generate_pkcs8(&random).unwrap().as_ref())?,
        );
    }
    let limits = Limits {
        catalog_key: Some(signer.public_key()),
        source_keys: sources
            .iter()
            .map(|(group, signer)| (*group, signer.public_key()))
            .collect(),
        ..Default::default()
    };
    let (mut catalog, _, reserve) = super::super::manifest::tests::fixture(&limits)?;
    let table = catalog.tables.get_mut(&reserve.authority.table).unwrap();
    Arc::make_mut(table.progress.pending.as_mut().unwrap())
        .plan
        .sources
        .insert(
            10,
            SourceRange {
                after: 0,
                through: 2,
                records: vec![(
                    2,
                    Batch::new(
                        table.authority.table,
                        0,
                        Uuid::from_u128(900),
                        1,
                        Bytes::from_static(b"source ten"),
                    )
                    .meta,
                )],
            },
        );
    let Reply::ManifestReserved(selected) = catalog.reserve_manifest(reserve.clone(), 8, &limits)
    else {
        anyhow::bail!("manifest not selected")
    };
    let confirmation = ConfirmPublication {
        authority: reserve.authority,
        incarnation: reserve.incarnation,
        transaction: reserve.transaction,
        plan_index: reserve.plan_index,
        prepared_index: reserve.prepared_index,
        descriptor_digest: reserve.descriptor_digest,
        target: reserve.target,
        manifest_identity: selected.identity,
    };
    assert!(matches!(
        catalog.confirm_publication(confirmation.clone(), 9),
        Reply::Published(_)
    ));
    catalog.validate(&layout, 10, 9, &limits)?;
    Ok(Fixture {
        layout,
        limits,
        catalog,
        signer,
        sources,
        confirmation,
    })
}

impl Fixture {
    fn table(&self) -> &Table {
        &self.catalog.tables[&self.confirmation.authority.table]
    }

    fn cut(&self, group: u64, index: u64) -> Result<Cut> {
        Ok(Cut {
            cluster: self.layout.cluster,
            layout: self.layout.fingerprint()?,
            group,
            term: 1,
            quorum_index: index,
            applied: index,
        })
    }

    fn request_for(&self, table: &Table, source_cut: u64) -> Result<AdvancePublication> {
        let published = table.progress.published.as_ref().unwrap();
        let mut proofs = BTreeMap::new();
        for (group, range) in &table.progress.pending.as_ref().unwrap().plan.sources {
            let permit = ClosurePermit::issue(
                &self.signer,
                self.cut(10, published.index)?,
                &self.layout,
                &self.limits,
                table,
                *group,
            )?;
            let pin_index = range.through.max(published.index) + 1;
            let closed = Arc::new(Closed {
                index: pin_index + 1,
                pin_index,
                permit,
            });
            proofs.insert(
                *group,
                ClosedProof::issue(
                    &self.sources[group],
                    self.cut(*group, source_cut.max(closed.index))?,
                    &self.layout,
                    &self.limits,
                    closed,
                )?,
            );
        }
        Ok(AdvancePublication {
            authority: table.authority.clone(),
            published_index: published.index,
            proofs,
        })
    }

    fn request(&self) -> Result<AdvancePublication> {
        self.request_for(self.table(), 23)
    }

    fn apply(&mut self, request: AdvancePublication, index: u64) -> Result<Reply> {
        let batch = request.batch(&self.layout, &self.limits)?;
        validate_record(&self.layout, 10, &self.limits, &batch.meta, &batch.body)?;
        Ok(self
            .catalog
            .apply(decode(&batch.body)?, index, &self.limits))
    }
}

#[test]
fn all_source_closures_advance_independent_cursors_and_free_next_plan() -> Result<()> {
    let mut f = fixture()?;
    let captured = f.table().clone();
    let request = f.request()?;
    let Reply::Advanced(advanced) = f.apply(request.clone(), 30)? else {
        anyhow::bail!("not advanced")
    };
    assert_eq!(advanced.index, 30);
    assert_eq!(advanced.sources, BTreeMap::from([(10, 2), (20, 20)]));
    assert_eq!(f.table().progress.checkpoint, advanced.sources);
    assert_eq!(f.table().progress.minimum_read_version, 8);
    assert!(f.table().progress.pending.is_none());
    assert!(f.table().progress.prepared.is_none());
    assert!(f.table().progress.published.is_none());
    assert!(Arc::ptr_eq(
        &advanced.publication,
        captured.progress.published.as_ref().unwrap()
    ));
    assert!(captured.progress.checkpoint.is_empty());
    assert_eq!(
        f.apply(request.clone(), 31)?,
        Reply::Advanced(advanced.clone())
    );
    let refreshed = f.request_for(&captured, 100)?;
    assert_ne!(
        request.proofs[&20].source_cut(),
        refreshed.proofs[&20].source_cut()
    );
    assert_eq!(f.apply(refreshed, 32)?, Reply::Advanced(advanced.clone()));

    let mut takeover = super::super::tests::input();
    takeover.owner = Uuid::new_v4();
    takeover.request = Uuid::new_v4();
    takeover.expected_epoch = 1;
    let Reply::Acquired(authority) =
        f.catalog
            .apply(decode(&takeover.batch(&f.layout)?.body)?, 33, &f.limits)
    else {
        anyhow::bail!("takeover")
    };
    let mut next = captured.progress.pending.as_ref().unwrap().plan.clone();
    next.transaction = Uuid::new_v4();
    next.read_version = 8;
    for (group, range) in &mut next.sources {
        range.after = advanced.sources[group];
        range.through = if *group == 10 { 32 } else { 40 };
        range.records[0].0 = range.through;
        range.records[0].1.sequence += 1;
    }
    let batch = PlanRequest {
        authority,
        plan: next,
    }
    .batch(&f.layout, &f.limits)?;
    assert_eq!(
        f.catalog.apply(decode(&batch.body)?, 34, &f.limits),
        Reply::Planned { index: 34 }
    );
    let pending = f.table().progress.pending.clone();
    assert_eq!(
        f.apply(request, 35)?,
        Reply::Advanced(advanced.clone()),
        "old-owner exact completion survives takeover"
    );
    assert_eq!(f.table().progress.pending, pending);
    assert_eq!(
        f.catalog.confirm_publication(f.confirmation.clone(), 36),
        Reply::Published(advanced.publication.clone())
    );
    f.catalog.validate(&f.layout, 10, 36, &f.limits)?;
    assert_eq!(decode_table(&encode_table(f.table())?)?, *f.table());
    Ok(())
}

#[test]
fn missing_or_wrong_valid_closure_cannot_partially_advance() -> Result<()> {
    let mut f = fixture()?;
    let before = f.catalog.clone();
    let request = f.request()?;
    let mut missing = request.clone();
    missing.proofs.remove(&20);
    assert_eq!(
        f.apply(missing, 30)?,
        Reply::Rejected(Rejection::SourceClosure)
    );
    assert_eq!(f.catalog, before);
    let mut wrong = f.table().clone();
    Arc::make_mut(wrong.progress.pending.as_mut().unwrap())
        .plan
        .sources
        .get_mut(&20)
        .unwrap()
        .through += 1;
    let altered = f.request_for(&wrong, 30)?;
    for proof in altered.proofs.values() {
        proof.verify(&f.layout, &f.limits)?;
    }
    assert_eq!(
        f.apply(altered, 31)?,
        Reply::Rejected(Rejection::SourceClosure)
    );
    assert_eq!(f.catalog, before);
    let mut stale = request.clone();
    stale.authority.owner = Uuid::new_v4();
    assert_eq!(f.apply(stale, 32)?, Reply::Rejected(Rejection::Authority));
    assert_eq!(f.catalog, before);
    let mut forged = request.clone();
    let mut wire = bincode::serialize(&forged.proofs[&20])?;
    let tail = wire.len() - 1;
    wire[tail] ^= 1;
    forged.proofs.insert(20, bincode::deserialize(&wire)?);
    assert!(f.apply(forged, 33).is_err());
    assert_eq!(f.catalog, before);
    let mut mixed = request.clone();
    mixed.published_index += 1;
    assert!(f.apply(mixed, 34).is_err());
    assert_eq!(f.catalog, before);
    let mut misplaced = request.clone();
    misplaced.proofs.insert(10, request.proofs[&20].clone());
    assert!(f.apply(misplaced, 35).is_err());
    assert_eq!(f.catalog, before);
    assert!(matches!(f.apply(request, 36)?, Reply::Advanced(_)));
    f.catalog.validate(&f.layout, 10, 36, &f.limits)?;
    Ok(())
}

#[test]
fn legacy_table_bytes_are_preserved_and_advancement_suffix_is_strict() -> Result<()> {
    let mut f = fixture()?;
    let table = f.table();
    // Independent encoding of the fields in the old on-disk table schema.
    let old = codec(MAX_TABLE_BYTES).serialize(&(
        &table.manifests,
        &table.destination,
        &table.ingest,
        &table.authority,
        table.request,
        (
            &table.progress.checkpoint,
            table.progress.minimum_read_version,
            &table.progress.pending,
            &table.progress.prepared,
            &table.progress.published,
        ),
    ))?;
    assert_eq!(encode_table(table)?, old);
    assert_eq!(decode_table(&old)?, *table);
    let request = f.request()?;
    f.apply(request, 30)?;
    let encoded = encode_table(f.table())?;
    assert_eq!(decode_table(&encoded)?, *f.table());
    for removed in 1..=8 {
        assert!(decode_table(&encoded[..encoded.len() - removed]).is_err());
    }
    let mut trailing = encoded.clone();
    trailing.push(0);
    assert!(decode_table(&trailing).is_err());
    let marker = encoded.windows(8).position(|w| w == b"LCWALAD1").unwrap();
    let mut unknown = encoded;
    unknown[marker] ^= 1;
    assert!(decode_table(&unknown).is_err());
    Ok(())
}

#[test]
fn checkpoint_rejects_corrupt_completed_receipts() -> Result<()> {
    let mut f = fixture()?;
    let request = f.request()?;
    f.apply(request, 30)?;
    for mutation in 0..8 {
        let mut broken = f.catalog.clone();
        let table = broken
            .tables
            .get_mut(&f.confirmation.authority.table)
            .unwrap();
        let advanced = Arc::make_mut(table.progress.advanced.as_mut().unwrap());
        match mutation {
            0 => advanced.index = advanced.publication.index,
            1 => advanced.index = 31,
            2 => advanced.closure_digest = [0; 32],
            3 => advanced.sources.clear(),
            4 => {
                advanced.sources.insert(20, 21);
            }
            5 => Arc::make_mut(&mut advanced.publication).incarnation = Uuid::new_v4(),
            6 => table.progress.minimum_read_version += 1,
            7 => {
                table.progress.checkpoint.remove(&10);
            }
            _ => unreachable!(),
        }
        assert!(
            broken.validate(&f.layout, 10, 30, &f.limits).is_err(),
            "mutation {mutation}"
        );
    }
    Ok(())
}
