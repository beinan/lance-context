use super::*;

fn object(version: u64, staging: bool) -> ManifestObject {
    ManifestObject {
        path: format!(
            "_versions/{version}.manifest{}",
            if staging {
                format!("-{}", Uuid::new_v4())
            } else {
                String::new()
            }
        ),
        bytes: 8192,
        v2_names: false,
    }
}

fn fixture(limits: &Limits) -> Result<(Catalog, BootstrapManifest, ReserveManifest)> {
    let layout = crate::replication::tests::layout();
    let mut catalog = Catalog::default();
    let acquire = super::super::tests::input();
    let Reply::Acquired(authority) =
        catalog.apply(decode(&acquire.clone().batch(&layout)?.body)?, 4, limits)
    else {
        anyhow::bail!("missing authority");
    };
    let bootstrap = BootstrapManifest {
        authority: authority.clone(),
        incarnation: Uuid::from_u128(400),
        request: Uuid::from_u128(401),
        version: 7,
        object: object(7, false),
        transaction: Uuid::from_u128(402),
    };
    let Reply::ManifestReserved(first) =
        catalog.apply(decode(&bootstrap.clone().batch(&layout)?.body)?, 5, limits)
    else {
        anyhow::bail!("missing bootstrap");
    };
    let plan = PlanRequest {
        authority: authority.clone(),
        plan: Plan {
            transaction: Uuid::from_u128(403),
            read_version: 7,
            next_group: 10,
            sources: BTreeMap::from([(
                20,
                SourceRange {
                    after: 0,
                    through: 20,
                    records: vec![(
                        20,
                        Batch::new(
                            acquire.table,
                            1,
                            Uuid::from_u128(404),
                            1,
                            Bytes::from_static(b"opaque"),
                        )
                        .meta,
                    )],
                },
            )]),
        },
    };
    assert_eq!(
        catalog.apply(
            decode(&plan.clone().batch(&layout, limits)?.body)?,
            6,
            limits
        ),
        Reply::Planned { index: 6 }
    );
    let descriptors = Descriptors::lance_fragments(Bytes::from_static(b"fragments"))?;
    let prepare = PrepareRequest {
        authority: authority.clone(),
        transaction: plan.plan.transaction,
        plan_index: 6,
        descriptors: descriptors.clone(),
    };
    assert_eq!(
        catalog.apply(decode(&prepare.batch(&layout, limits)?.body)?, 7, limits),
        Reply::Prepared { index: 7 }
    );
    let reserve = ReserveManifest {
        authority,
        incarnation: bootstrap.incarnation,
        transaction: plan.plan.transaction,
        plan_index: 6,
        prepared_index: 7,
        descriptor_digest: descriptors.digest,
        predecessor: first.identity,
        target: 8,
        object: object(8, true),
    };
    Ok((catalog, bootstrap, reserve))
}

fn reserve(
    catalog: &mut Catalog,
    request: ReserveManifest,
    index: u64,
    limits: &Limits,
) -> Result<Reply> {
    let layout = crate::replication::tests::layout();
    let batch = request.batch(&layout)?;
    validate_record(&layout, 10, limits, &batch.meta, &batch.body)?;
    Ok(catalog.apply(decode(&batch.body)?, index, limits))
}

#[test]
fn immutable_manifest_identity_survives_takeover_and_resolves_lost_success() -> Result<()> {
    let limits = Limits::default();
    let layout = crate::replication::tests::layout();
    let (mut catalog, bootstrap, request) = fixture(&limits)?;
    let captured = catalog.tables[&request.authority.table].clone();
    let Reply::ManifestReserved(first) = reserve(&mut catalog, request.clone(), 8, &limits)? else {
        anyhow::bail!("reservation missing");
    };
    assert_eq!(first.index, 8);
    assert_eq!(captured.manifests.as_ref().unwrap().head, 7);
    assert_eq!(
        catalog.tables[&request.authority.table]
            .manifests
            .as_ref()
            .unwrap()
            .head,
        8
    );
    assert!(Arc::ptr_eq(
        &captured.manifests.as_ref().unwrap().versions[&7],
        &catalog.tables[&request.authority.table]
            .manifests
            .as_ref()
            .unwrap()
            .versions[&7]
    ));
    let mut acquire = super::super::tests::input();
    acquire.owner = Uuid::from_u128(405);
    acquire.request = Uuid::from_u128(406);
    acquire.expected_epoch = 1;
    let Reply::Acquired(authority) =
        catalog.apply(decode(&acquire.batch(&layout)?.body)?, 9, &limits)
    else {
        anyhow::bail!("takeover missing");
    };
    // Resolve the old successful call, even with its old authority: reporting
    // a refusal here could delete the already selected staging object.
    assert_eq!(
        reserve(&mut catalog, request.clone(), 10, &limits)?,
        Reply::ManifestReserved(first.clone())
    );
    let mut retry = request.clone();
    retry.authority = authority;
    retry.object = object(8, true);
    assert_eq!(
        reserve(&mut catalog, retry, 11, &limits)?,
        Reply::ManifestTaken(first.clone())
    );
    assert_eq!(
        catalog.tables[&request.authority.table]
            .manifests
            .as_ref()
            .unwrap()
            .versions
            .len(),
        2
    );
    let mut wrong = request.clone();
    wrong.incarnation = Uuid::new_v4();
    assert_eq!(
        reserve(&mut catalog, wrong, 12, &limits)?,
        Reply::Rejected(Rejection::ManifestIdentity)
    );
    catalog.validate(&layout, 10, 12, &limits)?;
    let encoded = encode_table(&catalog.tables[&request.authority.table])?;
    assert_eq!(
        decode_table(&encoded)?,
        catalog.tables[&request.authority.table]
    );
    assert!(catalog.tables[&request.authority.table]
        .progress
        .checkpoint
        .is_empty());
    for mutation in 0..6 {
        let mut invalid = catalog.clone();
        let table = invalid.tables.get_mut(&request.authority.table).unwrap();
        let manifests = Arc::make_mut(table.manifests.as_mut().unwrap());
        match mutation {
            0 => manifests.head += 1,
            1 => {
                manifests.versions.remove(&7);
            }
            2 => Arc::make_mut(manifests.versions.get_mut(&8).unwrap()).identity = Uuid::new_v4(),
            3 => Arc::make_mut(manifests.versions.get_mut(&8).unwrap()).descriptor_digest[0] ^= 1,
            4 => Arc::make_mut(manifests.versions.get_mut(&8).unwrap()).index = 7,
            5 => manifests.incarnation = Uuid::new_v4(),
            _ => unreachable!(),
        }
        assert!(invalid.validate(&layout, 10, 12, &limits).is_err());
    }
    assert_eq!(
        bootstrap.version,
        captured.manifests.unwrap().initial_version
    );
    Ok(())
}

#[test]
fn reservation_requires_exact_prepared_plan_current_owner_and_head() -> Result<()> {
    let limits = Limits::default();
    let (catalog, _, request) = fixture(&limits)?;
    for mutation in 0..8 {
        let mut changed = request.clone();
        let expected = match mutation {
            0 => {
                changed.authority.owner = Uuid::new_v4();
                Rejection::Authority
            }
            1 => {
                changed.predecessor = Uuid::new_v4();
                Rejection::ManifestPredecessor
            }
            2 => {
                changed.transaction = Uuid::new_v4();
                Rejection::PlanIdentity
            }
            3 => {
                changed.plan_index -= 1;
                Rejection::PlanIdentity
            }
            4 => {
                changed.prepared_index += 1;
                Rejection::PlanIdentity
            }
            5 => {
                changed.descriptor_digest[0] ^= 1;
                Rejection::PlanIdentity
            }
            6 => {
                changed.target = 9;
                changed.object = object(9, true);
                Rejection::PlanIdentity
            }
            7 => {
                changed.incarnation = Uuid::new_v4();
                Rejection::ManifestIdentity
            }
            _ => unreachable!(),
        };
        let mut current = catalog.clone();
        assert_eq!(
            reserve(&mut current, changed, 8, &limits)?,
            Reply::Rejected(expected)
        );
        assert_eq!(current, catalog);
    }
    Ok(())
}

#[test]
fn bounded_capacity_preserves_exact_retries_and_paths_cannot_cross_versions() -> Result<()> {
    let limits = Limits {
        max_manifest_records: 1,
        ..Default::default()
    };
    let (mut catalog, bootstrap, request) = fixture(&limits)?;
    assert_eq!(
        reserve(&mut catalog, request, 8, &limits)?,
        Reply::Rejected(Rejection::Capacity)
    );
    let layout = crate::replication::tests::layout();
    let Reply::ManifestReserved(record) =
        catalog.apply(decode(&bootstrap.clone().batch(&layout)?.body)?, 9, &limits)
    else {
        anyhow::bail!("retry blocked at capacity");
    };
    assert_eq!(record.index, 5);
    for path in [
        "_versions/8.manifest",
        "_versions/../7.manifest",
        "other/7.manifest",
        "_versions/7.manifest-not-uuid",
        "_versions/7.manifest?secret=x",
    ] {
        let mut bad = bootstrap.clone();
        bad.object.path = path.into();
        assert!(bad.batch(&layout).is_err());
    }
    let mut v2 = bootstrap;
    v2.object.v2_names = true;
    v2.object.path = format!("_versions/{:020}.manifest", u64::MAX - 7);
    v2.batch(&layout)?;
    Ok(())
}
