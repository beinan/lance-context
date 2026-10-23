use super::*;

fn command(f: &mut Fixture, command: Command, index: u64) -> Result<Reply> {
    let batch = command.batch(&f.layout)?;
    validate_record(&f.layout, 10, &f.limits, &batch.meta, &batch.body)?;
    Ok(f.catalog.apply(decode(&batch.body)?, index, &f.limits))
}

fn reopen(f: &mut Fixture, index: u64) -> Result<()> {
    let table = decode_table(&encode_table(f.table())?)?;
    f.catalog.tables.insert(table.authority.table, table);
    f.catalog.validate(&f.layout, 10, index, &f.limits)
}

#[test]
fn compaction_recovers_fixed_publication_without_advancing_wal_cursors() -> Result<()> {
    maintenance_recovers(Descriptors::lance_compaction(Bytes::from_static(
        b"rewrite-files",
    ))?)
}

#[test]
fn index_recovers_fixed_publication_without_advancing_wal_cursors() -> Result<()> {
    maintenance_recovers(Descriptors::lance_index(Bytes::from_static(
        b"index-files",
    ))?)
}

fn maintenance_recovers(descriptors: Descriptors) -> Result<()> {
    let mut f = fixture()?;
    let old_plan = f.table().progress.pending.as_ref().unwrap().plan.clone();
    let append = f.request()?;
    let mut missing = append.clone();
    missing.proofs.clear();
    assert_eq!(
        f.apply(missing, 30)?,
        Reply::Rejected(Rejection::SourceClosure),
        "append still requires every source closure"
    );
    assert!(matches!(f.apply(append, 31)?, Reply::Advanced(_)));
    let cursors = f.table().progress.checkpoint.clone();
    let plan = PlanRequest {
        authority: f.table().authority.clone(),
        plan: Plan {
            transaction: Uuid::new_v4(),
            read_version: 8,
            sources: BTreeMap::new(),
            next_group: 20,
        },
    };
    assert!(plan.plan.is_compaction());
    assert_eq!(plan.plan.source_bytes()?, 0);
    assert_eq!(
        command(&mut f, Command::Plan(Box::new(plan.clone())), 32)?,
        Reply::Planned { index: 32 }
    );
    let mut prepare = PrepareRequest {
        authority: plan.authority.clone(),
        transaction: plan.plan.transaction,
        plan_index: 32,
        descriptors: Descriptors::lance_fragments(Bytes::from_static(b"append-files"))?,
    };
    assert_eq!(
        command(&mut f, Command::Prepare(Box::new(prepare.clone())), 33)?,
        Reply::Rejected(Rejection::PreparedConflict),
        "empty-source plan cannot publish append descriptors"
    );
    prepare.descriptors = descriptors;
    assert_eq!(
        command(&mut f, Command::Prepare(Box::new(prepare.clone())), 34)?,
        Reply::Prepared { index: 34 }
    );
    reopen(&mut f, 34)?;

    let mut takeover = super::super::super::tests::input();
    takeover.owner = Uuid::new_v4();
    takeover.request = Uuid::new_v4();
    takeover.expected_epoch = 1;
    let Reply::Acquired(authority) = command(&mut f, Command::Acquire(Box::new(takeover)), 35)?
    else {
        anyhow::bail!("compaction takeover failed");
    };
    assert_eq!(
        command(&mut f, Command::Prepare(Box::new(prepare.clone())), 36)?,
        Reply::Rejected(Rejection::Authority)
    );
    prepare.authority = authority.clone();
    assert_eq!(
        command(&mut f, Command::Prepare(Box::new(prepare.clone())), 37)?,
        Reply::Prepared { index: 34 },
        "takeover must retain the original prepared files"
    );
    let manifests = f.table().manifests.as_ref().unwrap();
    let reserve = ReserveManifest {
        authority: authority.clone(),
        incarnation: manifests.incarnation,
        transaction: plan.plan.transaction,
        plan_index: 32,
        prepared_index: 34,
        descriptor_digest: prepare.descriptors.digest,
        predecessor: manifests.versions[&8].identity,
        target: 9,
        object: ManifestObject {
            path: format!("_versions/9.manifest-{}", Uuid::new_v4()),
            bytes: 8192,
            v2_names: false,
        },
    };
    let Reply::ManifestReserved(selected) = command(
        &mut f,
        Command::ReserveManifest(Box::new(reserve.clone())),
        38,
    )?
    else {
        anyhow::bail!("compaction manifest not selected");
    };
    reopen(&mut f, 38)?;
    let confirm = ConfirmPublication {
        authority,
        incarnation: reserve.incarnation,
        transaction: reserve.transaction,
        plan_index: reserve.plan_index,
        prepared_index: reserve.prepared_index,
        descriptor_digest: reserve.descriptor_digest,
        target: reserve.target,
        manifest_identity: selected.identity,
    };
    assert!(matches!(
        command(&mut f, Command::ConfirmPublication(Box::new(confirm)), 39)?,
        Reply::Published(_)
    ));
    assert!(
        ClosurePermit::issue(
            &f.signer,
            f.cut(10, 39)?,
            &f.layout,
            &f.limits,
            f.table(),
            10,
        )
        .is_err(),
        "compaction cannot authorize WAL source retirement"
    );
    let finish = f.request()?;
    assert!(finish.proofs.is_empty());
    let Reply::Advanced(completed) = f.apply(finish.clone(), 40)? else {
        anyhow::bail!("compaction completion rejected");
    };
    assert!(completed.sources.is_empty());
    assert_eq!(f.table().progress.checkpoint, cursors);
    assert_eq!(f.table().progress.minimum_read_version, 9);
    reopen(&mut f, 40)?;
    let saved = f.table().clone();
    let mut corrupt = saved.clone();
    Arc::make_mut(corrupt.progress.advanced.as_mut().unwrap())
        .sources
        .insert(10, cursors[&10]);
    f.catalog.tables.insert(corrupt.authority.table, corrupt);
    assert!(f.catalog.validate(&f.layout, 10, 40, &f.limits).is_err());
    f.catalog.tables.insert(saved.authority.table, saved);
    assert_eq!(
        f.apply(finish.clone(), 41)?,
        Reply::Advanced(completed.clone())
    );

    let mut next = old_plan;
    next.transaction = Uuid::new_v4();
    next.read_version = 9;
    for (group, range) in &mut next.sources {
        range.after = cursors[group];
        range.through = 42;
        range.records[0].0 = 42;
        range.records[0].1.sequence += 1;
    }
    let next = PlanRequest {
        authority: f.table().authority.clone(),
        plan: next,
    };
    assert_eq!(
        command(&mut f, Command::Plan(Box::new(next)), 43)?,
        Reply::Planned { index: 43 }
    );
    let pending = f.table().progress.pending.clone();
    assert_eq!(f.apply(finish, 44)?, Reply::Advanced(completed));
    assert_eq!(f.table().progress.pending, pending);
    assert_eq!(f.table().progress.checkpoint, cursors);
    reopen(&mut f, 44)?;
    Ok(())
}

#[test]
fn append_cannot_prepare_compaction_descriptors_or_lose_its_sources() -> Result<()> {
    append_rejects_maintenance(Descriptors::lance_compaction(Bytes::from_static(
        b"rewrite-files",
    ))?)
}

#[test]
fn append_cannot_prepare_index_descriptors_or_lose_its_sources() -> Result<()> {
    append_rejects_maintenance(Descriptors::lance_index(Bytes::from_static(
        b"index-files",
    ))?)
}

fn append_rejects_maintenance(descriptors: Descriptors) -> Result<()> {
    let mut f = fixture()?;
    let prepared = f.table().progress.prepared.as_ref().unwrap();
    let request = PrepareRequest {
        authority: f.table().authority.clone(),
        transaction: prepared.transaction,
        plan_index: prepared.plan_index,
        descriptors,
    };
    assert_eq!(
        command(&mut f, Command::Prepare(Box::new(request.clone())), 30)?,
        Reply::Rejected(Rejection::PreparedConflict)
    );
    let mut corrupt = f.table().clone();
    Arc::make_mut(corrupt.progress.prepared.as_mut().unwrap()).descriptors = request.descriptors;
    f.catalog.tables.insert(corrupt.authority.table, corrupt);
    assert!(f.catalog.validate(&f.layout, 10, 30, &f.limits).is_err());
    Ok(())
}
