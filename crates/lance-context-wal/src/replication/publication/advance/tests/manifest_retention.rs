use super::*;
use crate::replication::query::{Query, Value};

fn apply_batch(f: &mut Fixture, batch: Batch, index: &mut u64) -> Result<Reply> {
    *index += 1;
    validate_record(&f.layout, 10, &f.limits, &batch.meta, &batch.body)?;
    Ok(f.catalog.apply(decode(&batch.body)?, *index, &f.limits))
}

fn retirement(f: &Fixture, retain_from: u64) -> RetireManifestHistory {
    RetireManifestHistory {
        authority: f.table().authority.clone(),
        incarnation: f.table().manifests.as_ref().unwrap().incarnation,
        retain_from,
    }
}

fn next_manifest(f: &mut Fixture, template: &Plan, index: &mut u64) -> Result<ReserveManifest> {
    let authority = f.table().authority.clone();
    let manifests = f.table().manifests.as_ref().unwrap();
    let predecessor = manifests.versions[&manifests.head].identity;
    let incarnation = manifests.incarnation;
    let mut plan = template.clone();
    plan.transaction = Uuid::new_v4();
    plan.read_version = manifests.head;
    for (group, range) in &mut plan.sources {
        range.after = f.table().progress.checkpoint[group];
        range.through = range.after + 1;
        range.records[0].0 = range.through;
        range.records[0].1.sequence = plan.read_version;
    }
    let batch = PlanRequest {
        authority: authority.clone(),
        plan: plan.clone(),
    }
    .batch(&f.layout, &f.limits)?;
    assert_eq!(
        apply_batch(f, batch, index)?,
        Reply::Planned { index: *index }
    );
    let plan_index = *index;
    let descriptors = Descriptors::lance_fragments(Bytes::from_static(b"bounded-fixture"))?;
    let batch = PrepareRequest {
        authority: authority.clone(),
        transaction: plan.transaction,
        plan_index,
        descriptors: descriptors.clone(),
    }
    .batch(&f.layout, &f.limits)?;
    assert_eq!(
        apply_batch(f, batch, index)?,
        Reply::Prepared { index: *index }
    );
    let target = plan.target_version()?;
    Ok(ReserveManifest {
        authority,
        incarnation,
        transaction: plan.transaction,
        plan_index,
        prepared_index: *index,
        descriptor_digest: descriptors.digest,
        predecessor,
        target,
        object: ManifestObject {
            path: format!("_versions/{target}.manifest-{}", Uuid::new_v4()),
            bytes: 4096,
            v2_names: false,
        },
    })
}

fn confirm_and_advance(
    f: &mut Fixture,
    record: Arc<ManifestRecord>,
    index: &mut u64,
) -> Result<()> {
    let request = ConfirmPublication {
        authority: f.table().authority.clone(),
        incarnation: f.table().manifests.as_ref().unwrap().incarnation,
        transaction: record.transaction,
        plan_index: record.plan_index,
        prepared_index: record.prepared_index,
        descriptor_digest: record.descriptor_digest,
        target: record.version,
        manifest_identity: record.identity,
    };
    let batch = request.batch(&f.layout)?;
    assert!(matches!(apply_batch(f, batch, index)?, Reply::Published(_)));
    let request = f.request()?;
    *index += 1;
    assert!(matches!(f.apply(request, *index)?, Reply::Advanced(_)));
    Ok(())
}

#[test]
fn manifest_history_stays_bounded_across_three_hundred_publications_and_restore() -> Result<()> {
    let mut f = fixture()?;
    let template = f.table().progress.pending.as_ref().unwrap().plan.clone();
    let mut index = 30;
    let request = f.request()?;
    assert!(matches!(f.apply(request, index)?, Reply::Advanced(_)));
    let bootstrap = f.table().manifests.as_ref().unwrap().versions[&7].clone();
    let mut old = None;
    let mut captured = None;
    for round in 0..300u64 {
        let request = next_manifest(&mut f, &template, &mut index)?;
        let batch = request.clone().batch(&f.layout)?;
        let Reply::ManifestReserved(record) = apply_batch(&mut f, batch, &mut index)? else {
            anyhow::bail!("manifest reservation failed")
        };
        confirm_and_advance(&mut f, record, &mut index)?;
        if round == 0 {
            old = Some(request);
            captured = Some(f.table().clone());
        }
        let head = f.table().manifests.as_ref().unwrap().head;
        if head >= 24 && round.is_multiple_of(8) {
            let batch = retirement(&f, head - 7).batch(&f.layout)?;
            assert!(matches!(
                apply_batch(&mut f, batch, &mut index)?,
                Reply::ManifestHistory(_)
            ));
            let bytes = encode_table(f.table())?;
            let restored = decode_table(&bytes)?;
            assert_eq!(encode_table(&restored)?, bytes);
            f.catalog.tables.insert(restored.authority.table, restored);
        }
        let manifests = f.table().manifests.as_ref().unwrap();
        assert!(manifests.versions.len() <= 25);
        assert_eq!(manifests.versions[&7], bootstrap);
        f.catalog.validate(&f.layout, 10, index, &f.limits)?;
    }
    let old = old.unwrap();
    let captured = captured.unwrap();
    assert!(captured
        .manifests
        .as_ref()
        .unwrap()
        .versions
        .contains_key(&old.target));
    let boundary = f.table().manifests.as_ref().unwrap().history_boundary();
    assert!(boundary.is_retired(old.target));
    assert!(!boundary.is_retired(7));
    let mut corrupt = f.catalog.clone();
    let manifests = Arc::make_mut(
        corrupt
            .tables
            .get_mut(&f.table().authority.table)
            .unwrap()
            .manifests
            .as_mut()
            .unwrap(),
    );
    manifests.versions.remove(&(boundary.retained_from + 1));
    assert!(
        corrupt.validate(&f.layout, 10, index, &f.limits).is_err(),
        "a hole inside the retained suffix must not become another retirement boundary"
    );
    let before = f.catalog.clone();
    let batch = old.batch(&f.layout)?;
    assert_eq!(
        apply_batch(&mut f, batch, &mut index)?,
        Reply::ManifestRetired(boundary.clone())
    );
    assert_eq!(
        f.catalog, before,
        "old reservation must not create another manifest"
    );
    let state = State {
        applied: index,
        catalog: f.catalog.clone(),
        ..Default::default()
    };
    for (version, expected) in [
        (Some(9), Value::ManifestRetired(boundary)),
        (Some(7), Value::Manifest(Some(bootstrap.clone()))),
        (Some(6), Value::Manifest(None)),
    ] {
        assert_eq!(
            Query::Manifest {
                table: f.table().authority.table,
                version
            }
            .evaluate(
                &state,
                &f.cut(10, index)?,
                &f.layout,
                &f.limits,
                source::QuerySigners {
                    catalog: None,
                    source: None
                },
            )?,
            expected
        );
    }
    let bootstrap_retry = BootstrapManifest {
        authority: f.table().authority.clone(),
        incarnation: f.table().manifests.as_ref().unwrap().incarnation,
        request: f.table().manifests.as_ref().unwrap().bootstrap_request,
        version: bootstrap.version,
        object: bootstrap.object.clone(),
        transaction: bootstrap.transaction,
    }
    .batch(&f.layout)?;
    assert_eq!(
        apply_batch(&mut f, bootstrap_retry, &mut index)?,
        Reply::ManifestReserved(bootstrap)
    );
    Ok(())
}

#[test]
fn manifest_retirement_preserves_pending_predecessor_and_rejects_stale_authority() -> Result<()> {
    let mut f = fixture()?;
    let mut index = 20;
    let before = f.catalog.clone();
    for (retain_from, expected) in [
        (6, Rejection::ManifestHistoryBoundary),
        (9, Rejection::ManifestHistoryBoundary),
        (8, Rejection::PendingPlan),
    ] {
        let batch = retirement(&f, retain_from).batch(&f.layout)?;
        assert_eq!(
            apply_batch(&mut f, batch, &mut index)?,
            Reply::Rejected(expected)
        );
        assert_eq!(f.catalog, before);
    }
    let request = f.request()?;
    index += 1;
    f.apply(request, index)?;
    let mut stale = retirement(&f, 8);
    stale.authority.owner = Uuid::new_v4();
    let batch = stale.batch(&f.layout)?;
    assert_eq!(
        apply_batch(&mut f, batch, &mut index)?,
        Reply::Rejected(Rejection::Authority)
    );
    let mut foreign = retirement(&f, 8);
    foreign.incarnation = Uuid::new_v4();
    let batch = foreign.batch(&f.layout)?;
    assert_eq!(
        apply_batch(&mut f, batch, &mut index)?,
        Reply::Rejected(Rejection::ManifestIdentity)
    );
    let batch = retirement(&f, 8).batch(&f.layout)?;
    assert!(matches!(
        apply_batch(&mut f, batch, &mut index)?,
        Reply::ManifestHistory(_)
    ));
    f.catalog.validate(&f.layout, 10, index, &f.limits)?;
    Ok(())
}

#[test]
fn manifest_capacity_is_released_without_losing_the_pending_plan_or_retry_boundary() -> Result<()> {
    let mut f = fixture()?;
    f.limits.max_manifest_records = 4;
    let template = f.table().progress.pending.as_ref().unwrap().plan.clone();
    let mut index = 30;
    let request = f.request()?;
    f.apply(request, index)?;
    for _ in 0..2 {
        let request = next_manifest(&mut f, &template, &mut index)?;
        let batch = request.batch(&f.layout)?;
        let Reply::ManifestReserved(record) = apply_batch(&mut f, batch, &mut index)? else {
            anyhow::bail!("reservation missing")
        };
        confirm_and_advance(&mut f, record, &mut index)?;
    }
    let request = next_manifest(&mut f, &template, &mut index)?;
    let batch = request.clone().batch(&f.layout)?;
    assert_eq!(
        apply_batch(&mut f, batch, &mut index)?,
        Reply::Rejected(Rejection::Capacity)
    );
    let pending = f.table().progress.pending.clone();
    let batch = retirement(&f, 10).batch(&f.layout)?;
    assert!(matches!(
        apply_batch(&mut f, batch, &mut index)?,
        Reply::ManifestHistory(_)
    ));
    assert_eq!(f.table().progress.pending, pending);
    let boundary = f.table().manifests.as_ref().unwrap().history_boundary();
    assert!(boundary.is_retired(8));
    let batch = retirement(&f, 8).batch(&f.layout)?;
    assert_eq!(
        apply_batch(&mut f, batch, &mut index)?,
        Reply::ManifestHistory(boundary)
    );
    let batch = request.batch(&f.layout)?;
    let Reply::ManifestReserved(record) = apply_batch(&mut f, batch, &mut index)? else {
        anyhow::bail!("pending plan cannot resume after retirement")
    };
    confirm_and_advance(&mut f, record, &mut index)?;
    f.catalog.validate(&f.layout, 10, index, &f.limits)?;
    Ok(())
}
