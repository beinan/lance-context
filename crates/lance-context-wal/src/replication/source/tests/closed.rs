use super::*;
use publication::{
    BootstrapManifest, ConfirmPublication, Descriptors, ManifestObject, PrepareRequest,
    ReserveManifest,
};

fn publisher(events: Events) -> Result<publication::Reply> {
    events
        .completions
        .into_iter()
        .find_map(|c| match c.outcome {
            Outcome::Committed(Applied::Publisher(reply)) => Some(reply),
            _ => None,
        })
        .context("publisher completion missing")
}

fn fixture() -> Result<(Cluster, Permit, Uuid, Uuid)> {
    let mut c = Cluster::new()?;
    let authority = c.acquire(0)?;
    let incarnation = Uuid::from_u128(800);
    c.nodes[0].bootstrap_manifest(
        800,
        BootstrapManifest {
            authority,
            incarnation,
            request: Uuid::from_u128(801),
            version: 8,
            object: ManifestObject {
                path: "_versions/8.manifest".into(),
                bytes: 8192,
                v2_names: false,
            },
            transaction: Uuid::from_u128(802),
        },
    )?;
    let publication::Reply::ManifestReserved(base) = publisher(c.pump()?)? else {
        anyhow::bail!("bootstrap missing")
    };
    let plan = c.plan(|_| {})?;
    let permit = c.permit(plan)?;
    assert!(matches!(c.pin(permit.clone())?, Reply::Pinned { .. }));
    Ok((c, permit, incarnation, base.identity))
}

fn closure_permit(c: &mut Cluster, plan: u64) -> Result<Option<ClosurePermit>> {
    c.nodes[0].query(
        10,
        803,
        Query::ClosePermit {
            table: c.table,
            transaction: c.transaction,
            plan_index: plan,
            source_group: 20,
        },
    )?;
    c.pump()?
        .reads
        .into_iter()
        .find_map(|read| match read.outcome {
            ReadOutcome::Observed {
                value: Value::ClosePermit(permit),
                ..
            } => Some(permit.map(|p| *p)),
            _ => None,
        })
        .context("closure query missing")
}

fn publish(c: &mut Cluster, incarnation: Uuid, predecessor: Uuid) -> Result<ClosurePermit> {
    let table = c.nodes[0].publisher(c.table)?.unwrap().clone();
    let pending = table.progress.pending.unwrap();
    let descriptors = Descriptors::lance_fragments(Bytes::from_static(b"unit-test-descriptors"))?;
    c.nodes[0].prepare_publication(
        804,
        PrepareRequest {
            authority: table.authority.clone(),
            transaction: c.transaction,
            plan_index: pending.index,
            descriptors: descriptors.clone(),
        },
    )?;
    let publication::Reply::Prepared { index } = publisher(c.pump()?)? else {
        anyhow::bail!("prepare missing")
    };
    c.nodes[0].reserve_manifest(
        805,
        ReserveManifest {
            authority: table.authority.clone(),
            incarnation,
            transaction: c.transaction,
            plan_index: pending.index,
            prepared_index: index,
            descriptor_digest: descriptors.digest,
            predecessor,
            target: 9,
            object: ManifestObject {
                path: format!("_versions/9.manifest-{}", Uuid::new_v4()),
                bytes: 8192,
                v2_names: false,
            },
        },
    )?;
    let publication::Reply::ManifestReserved(selected) = publisher(c.pump()?)? else {
        anyhow::bail!("reservation missing")
    };
    assert!(
        closure_permit(c, pending.index)?.is_none(),
        "selection alone is not a closure outcome"
    );
    c.nodes[0].confirm_publication(
        806,
        ConfirmPublication {
            authority: table.authority,
            incarnation,
            transaction: c.transaction,
            plan_index: pending.index,
            prepared_index: index,
            descriptor_digest: descriptors.digest,
            target: 9,
            manifest_identity: selected.identity,
        },
    )?;
    assert!(matches!(
        publisher(c.pump()?)?,
        publication::Reply::Published(_)
    ));
    closure_permit(c, pending.index)?.context("published closure permit missing")
}

fn close(c: &mut Cluster, permit: ClosurePermit) -> Result<Reply> {
    c.nodes[3].close_source(807, permit)?;
    c.pump()?
        .completions
        .into_iter()
        .find_map(|completion| match completion.outcome {
            Outcome::Committed(Applied::Source(reply)) => Some(reply),
            _ => None,
        })
        .context("source closure missing")
}

#[test]
fn durable_closure_releases_pin_rejects_delayed_permits_and_preserves_data() -> Result<()> {
    let (mut c, pin, incarnation, predecessor) = fixture()?;
    assert!(closure_permit(&mut c, pin.selection().plan_index)?.is_none());
    let permit = publish(&mut c, incarnation, predecessor)?;
    c.append(c.table, 3)?;
    let before = c.nodes[3].groups[&20].state.clone();
    let result = close(&mut c, permit.clone())?;
    assert!(matches!(result, Reply::Closed { .. }));
    assert_eq!(c.pin(pin.clone())?, Reply::Rejected(Rejection::Closed));
    assert_eq!(close(&mut c, permit.clone())?, result);
    let state = &c.nodes[3].groups[&20].state;
    assert!(state.pins.is_empty());
    assert_eq!(
        state.closed.len(),
        1,
        "raw streams must not allocate closure history"
    );
    assert_eq!(state.accepted, before.accepted);
    assert_eq!(state.producers, before.producers);
    assert_eq!(
        state.closed[&c.table].pin_index,
        before.pins[&c.table].index
    );
    assert_eq!(
        state.closed[&c.table].permit.closure().through,
        pin.selection().range.through
    );
    for n in 0..6 {
        let group = if n < 3 { 10 } else { 20 };
        let image = c.nodes[n]
            .prepare_checkpoint(group, Uuid::new_v4())?
            .run()?;
        c.nodes[n].install_checkpoint(image)?;
    }
    c.reopen()?;
    c.acquire(1)?;
    let refreshed = closure_permit(&mut c, pin.selection().plan_index)?.unwrap();
    assert_ne!(permit.catalog_cut(), refreshed.catalog_cut());
    assert_eq!(close(&mut c, refreshed)?, result);
    assert_eq!(c.pin(pin.clone())?, Reply::Rejected(Rejection::Closed));
    for node in &c.nodes[3..] {
        let state = &node.groups[&20].state;
        assert!(state.pins.is_empty());
        state.validate_checkpoint(&c.layout, 20, &c.limits, state.applied)?;
        for (index, meta) in &before.accepted {
            let record = node
                .read_accepted(20, *index)?
                .context("retained source disappeared")?;
            assert_eq!(&record.meta, meta);
            assert_eq!(record.body, Bytes::from(vec![meta.sequence as u8; 4096]));
        }
    }
    Ok(())
}

#[test]
fn closure_signature_scope_and_snapshot_fences_are_checked() -> Result<()> {
    let (mut c, pin, incarnation, predecessor) = fixture()?;
    let permit = publish(&mut c, incarnation, predecessor)?;
    let before = c.nodes[3].groups[&20].raw.raft.raft_log.last_index();
    for field in ["through", "plan_index", "group"] {
        let mut value = serde_json::to_value(&permit)?;
        let slot = &mut value["closure"][field];
        *slot = serde_json::json!(slot.as_u64().unwrap() + 1);
        assert!(c.nodes[3]
            .close_source(808, serde_json::from_value(value)?)
            .is_err());
    }
    let mut bytes = bincode::serialize(&permit)?;
    *bytes.last_mut().unwrap() ^= 1;
    assert!(c.nodes[3]
        .close_source(808, bincode::deserialize(&bytes)?)
        .is_err());
    assert_eq!(
        c.nodes[3].groups[&20].raw.raft.raft_log.last_index(),
        before
    );
    assert!(matches!(close(&mut c, permit)?, Reply::Closed { .. }));
    let state = c.nodes[3].groups[&20].state.clone();
    for mutation in 0..4 {
        let mut invalid = state.clone();
        let closed = Arc::make_mut(invalid.closed.get_mut(&c.table).unwrap());
        match mutation {
            0 => closed.index = state.applied + 1,
            1 => closed.pin_index = closed.index,
            2 => closed.pin_index = closed.permit.closure().through,
            3 => {
                invalid.pins.insert(
                    c.table,
                    Arc::new(Pin {
                        index: before,
                        permit: pin.clone(),
                    }),
                );
            }
            _ => unreachable!(),
        }
        assert!(invalid
            .validate_checkpoint(&c.layout, 20, &c.limits, invalid.applied)
            .is_err());
    }
    Ok(())
}
