use super::*;

fn fixture() -> Result<(Catalog, ConfirmPublication)> {
    let limits = Limits::default();
    let (mut catalog, _, reserve) = super::super::manifest::tests::fixture(&limits)?;
    let Reply::ManifestReserved(selected) = catalog.reserve_manifest(reserve.clone(), 8, &limits)
    else {
        anyhow::bail!("manifest not selected");
    };
    Ok((
        catalog,
        ConfirmPublication {
            authority: reserve.authority,
            incarnation: reserve.incarnation,
            transaction: reserve.transaction,
            plan_index: reserve.plan_index,
            prepared_index: reserve.prepared_index,
            descriptor_digest: reserve.descriptor_digest,
            target: reserve.target,
            manifest_identity: selected.identity,
        },
    ))
}

fn confirm(catalog: &mut Catalog, request: ConfirmPublication, index: u64) -> Result<Reply> {
    let layout = crate::replication::tests::layout();
    let limits = Limits::default();
    let batch = request.batch(&layout)?;
    validate_record(&layout, 10, &limits, &batch.meta, &batch.body)?;
    Ok(catalog.apply(decode(&batch.body)?, index, &limits))
}

fn takeover(catalog: &mut Catalog, index: u64) -> Result<Authority> {
    let layout = crate::replication::tests::layout();
    let mut request = super::super::tests::input();
    request.owner = Uuid::from_u128(500);
    request.request = Uuid::from_u128(501);
    request.expected_epoch = 1;
    let Reply::Acquired(authority) = catalog.apply(
        decode(&request.batch(&layout)?.body)?,
        index,
        &Limits::default(),
    ) else {
        anyhow::bail!("takeover failed")
    };
    Ok(authority)
}

#[test]
fn published_resolution_is_immutable_shared_and_preserves_source_dependencies() -> Result<()> {
    let (mut catalog, request) = fixture()?;
    let captured = catalog.tables[&request.authority.table].clone();
    let Reply::Published(published) = confirm(&mut catalog, request.clone(), 9)? else {
        anyhow::bail!("publication confirmation missing");
    };
    assert_eq!(published.index, 9);
    let table = &catalog.tables[&request.authority.table];
    assert!(Arc::ptr_eq(
        &published.manifest,
        &table.manifests.as_ref().unwrap().versions[&8]
    ));
    assert!(Arc::ptr_eq(
        table.progress.pending.as_ref().unwrap(),
        captured.progress.pending.as_ref().unwrap()
    ));
    assert!(Arc::ptr_eq(
        table.progress.prepared.as_ref().unwrap(),
        captured.progress.prepared.as_ref().unwrap()
    ));
    assert!(captured.progress.published.is_none());
    assert!(table.progress.checkpoint.is_empty());
    assert_eq!(table.progress.minimum_read_version, 1);
    // A source cursor is independent of this catalog's confirmation index.
    assert_eq!(
        table.progress.pending.as_ref().unwrap().plan.sources[&20].through,
        20
    );
    let new_authority = takeover(&mut catalog, 10)?;
    assert_eq!(
        confirm(&mut catalog, request.clone(), 11)?,
        Reply::Published(published.clone())
    );
    let mut retry = request.clone();
    retry.authority = new_authority.clone();
    assert_eq!(
        confirm(&mut catalog, retry, 12)?,
        Reply::Published(published.clone())
    );
    assert!(Arc::ptr_eq(
        catalog.tables[&request.authority.table]
            .progress
            .published
            .as_ref()
            .unwrap(),
        &published
    ));
    let mut next = captured.progress.pending.as_ref().unwrap().plan.clone();
    next.transaction = Uuid::new_v4();
    next.read_version = 8;
    let layout = crate::replication::tests::layout();
    let limits = Limits::default();
    let batch = PlanRequest {
        authority: new_authority,
        plan: next,
    }
    .batch(&layout, &limits)?;
    assert_eq!(
        catalog.apply(decode(&batch.body)?, 13, &limits),
        Reply::Rejected(Rejection::PendingPlan)
    );
    catalog.validate(&layout, 10, 13, &limits)?;
    Ok(())
}

#[test]
fn confirmation_requires_current_owner_and_exact_selected_manifest() -> Result<()> {
    let (catalog, request) = fixture()?;
    for mutation in 0..9 {
        let mut altered = request.clone();
        let expected = match mutation {
            0 => {
                altered.authority.owner = Uuid::new_v4();
                Rejection::Authority
            }
            1 => {
                altered.incarnation = Uuid::new_v4();
                Rejection::PublicationIdentity
            }
            2 => {
                altered.manifest_identity = Uuid::new_v4();
                Rejection::PublicationIdentity
            }
            3 => {
                altered.transaction = Uuid::new_v4();
                Rejection::PublicationIdentity
            }
            4 => {
                altered.plan_index -= 1;
                Rejection::PublicationIdentity
            }
            5 => {
                altered.prepared_index += 1;
                Rejection::PublicationIdentity
            }
            6 => {
                altered.descriptor_digest[0] ^= 1;
                Rejection::PublicationIdentity
            }
            7 => {
                altered.target += 1;
                Rejection::ManifestIdentity
            }
            8 => {
                altered.authority.table = Uuid::new_v4();
                Rejection::Authority
            }
            _ => unreachable!(),
        };
        let mut changed = catalog.clone();
        assert_eq!(
            confirm(&mut changed, altered.clone(), 9)?,
            Reply::Rejected(expected)
        );
        assert_eq!(changed, catalog);
        confirm(&mut changed, request.clone(), 10)?;
        // Old-owner exact outcome lookup is allowed; another identity never is.
        if mutation != 0 {
            let before = changed.clone();
            assert!(matches!(
                confirm(&mut changed, altered, 11)?,
                Reply::Rejected(_)
            ));
            assert_eq!(before, changed);
        }
    }
    let mut changed = catalog.clone();
    let authority = takeover(&mut changed, 9)?;
    assert_eq!(
        confirm(&mut changed, request.clone(), 10)?,
        Reply::Rejected(Rejection::Authority)
    );
    let mut new = request.clone();
    new.authority = authority;
    assert!(matches!(
        confirm(&mut changed, new, 11)?,
        Reply::Published(_)
    ));
    let limits = Limits::default();
    let (mut unselected, _, _) = super::super::manifest::tests::fixture(&limits)?;
    let before = unselected.clone();
    assert_eq!(
        confirm(&mut unselected, request, 9)?,
        Reply::Rejected(Rejection::ManifestIdentity)
    );
    assert_eq!(unselected, before);
    Ok(())
}

#[test]
fn checkpoint_rejects_damaged_outcomes_or_missing_retained_dependencies() -> Result<()> {
    let (mut catalog, request) = fixture()?;
    confirm(&mut catalog, request.clone(), 9)?;
    let layout = crate::replication::tests::layout();
    let limits = Limits::default();
    catalog.validate(&layout, 10, 9, &limits)?;
    let table = &catalog.tables[&request.authority.table];
    assert_eq!(decode_table(&encode_table(table)?)?, *table);
    for mutation in 0..9 {
        let mut changed = catalog.clone();
        let table = changed.tables.get_mut(&request.authority.table).unwrap();
        let outcome = Arc::make_mut(table.progress.published.as_mut().unwrap());
        match mutation {
            0 => outcome.index = outcome.manifest.index,
            1 => outcome.index = 10,
            2 => outcome.index = table.authority.grant_index,
            3 => outcome.incarnation = Uuid::new_v4(),
            4 => Arc::make_mut(&mut outcome.manifest).identity = Uuid::new_v4(),
            5 => table.progress.pending = None,
            6 => table.progress.prepared = None,
            7 => table.manifests = None,
            8 => {
                table.progress.checkpoint.insert(20, 20);
            }
            _ => unreachable!(),
        }
        assert!(
            changed.validate(&layout, 10, 9, &limits).is_err(),
            "mutation {mutation}"
        );
    }
    Ok(())
}
