use super::*;

fn proofs_fixture() -> Result<(Cluster, Permit, Uuid, Uuid)> {
    fixture_from(Cluster::with_source_policy(128, 100_000, true)?, 12)
}

fn observe(c: &mut Cluster, token: u64) -> Result<Option<Box<ClosedProof>>> {
    c.nodes[3].query(20, token, Query::ClosedProof { table: c.table })?;
    c.pump()?
        .reads
        .into_iter()
        .find_map(|r| match r.outcome {
            ReadOutcome::Observed {
                value: Value::ClosedProof(proof),
                ..
            } => Some(proof),
            _ => None,
        })
        .context("closure proof read missing")
}

#[test]
fn source_attests_only_committed_closure_and_preserves_it_after_checkpoint_restart() -> Result<()> {
    let (mut c, _, incarnation, predecessor) = proofs_fixture()?;
    assert!(observe(&mut c, 900)?.is_none());
    let permit = publish(&mut c, incarnation, predecessor)?;
    assert!(
        observe(&mut c, 901)?.is_none(),
        "catalog authorization is not source closure"
    );
    let Reply::Closed { index } = close(&mut c, permit.clone())? else {
        anyhow::bail!("closure missing")
    };
    let first = observe(&mut c, 902)?.context("closed proof missing")?;
    first.verify(&c.layout, &c.limits)?;
    assert_eq!(first.closed().index, index);
    assert_eq!(first.closed().permit, permit);
    assert!(c.nodes[3..].iter().all(|node| node.signer.is_none()));
    let wire = bincode::serialize(&first)?;
    let decoded: ClosedProof = bincode::deserialize(&wire)?;
    decoded.verify(&c.layout, &c.limits)?;
    for offset in [0, wire.len() / 2, wire.len() - 1] {
        let mut damaged = wire.clone();
        damaged[offset] ^= 1;
        if let Ok(proof) = bincode::deserialize::<ClosedProof>(&damaged) {
            assert!(proof.verify(&c.layout, &c.limits).is_err());
        }
    }
    let mut foreign = (*c.layout).clone();
    foreign.cluster = Uuid::new_v4();
    assert!(first.verify(&foreign, &c.limits).is_err());
    let mut policy = c.limits.clone();
    policy.max_records += 1;
    assert!(first.verify(&c.layout, &policy).is_err());
    policy = c.limits.clone();
    policy
        .source_keys
        .insert(20, c.source_signers[&10].public_key());
    assert!(first.verify(&c.layout, &policy).is_err());
    for node in &mut c.nodes {
        let group = if node.id <= 3 { 10 } else { 20 };
        let image = node.prepare_checkpoint(group, Uuid::new_v4())?.run()?;
        node.install_checkpoint(image)?;
    }
    c.reopen()?;
    let refreshed = observe(&mut c, 903)?.context("proof missing after restart")?;
    refreshed.verify(&c.layout, &c.limits)?;
    assert_eq!(refreshed.closed(), first.closed());
    assert_ne!(refreshed.source_cut(), first.source_cut());
    first.verify(&c.layout, &c.limits)?;
    Ok(())
}

#[test]
fn isolated_source_leader_cannot_refresh_a_proof_from_its_local_closure() -> Result<()> {
    let (mut c, _, incarnation, predecessor) = proofs_fixture()?;
    let permit = publish(&mut c, incarnation, predecessor)?;
    close(&mut c, permit)?;
    assert!(observe(&mut c, 904)?.is_some());
    c.nodes[3].query(20, 905, Query::ClosedProof { table: c.table })?;
    let mut unknown = false;
    for _ in 0..40 {
        c.nodes[3].tick()?;
        for read in c.nodes[3].poll()?.reads {
            assert!(matches!(read.outcome, ReadOutcome::Unknown));
            unknown = true;
        }
    }
    assert!(unknown);
    Ok(())
}

#[test]
fn missing_signers_reject_before_read_admission_and_keep_the_node_healthy() -> Result<()> {
    let (mut c, _, _, _) = proofs_fixture()?;
    c.nodes[3].source_signers.clear();
    assert!(c.nodes[3]
        .query(20, 906, Query::ClosedProof { table: c.table })
        .is_err());
    c.nodes[3].query(20, 906, Query::SourceClosure { table: c.table })?;
    assert!(c.pump()?.reads.iter().any(|r| r.request == 906));
    c.nodes[0].signer = None;
    assert!(c.nodes[0]
        .query(
            10,
            907,
            Query::ClosePermit {
                table: c.table,
                transaction: c.transaction,
                plan_index: 1,
                source_group: 20,
            }
        )
        .is_err());
    c.nodes[0].query(10, 907, Query::Publisher { table: c.table })?;
    assert!(c.pump()?.reads.iter().any(|r| r.request == 907));
    Ok(())
}

#[test]
fn source_signing_policy_rejects_missing_reused_and_unhosted_keys() -> Result<()> {
    let c = Cluster::with_source_policy(128, 100_000, true)?;
    for keys in [
        BTreeMap::from([(10, c.source_signers[&10].public_key())]),
        BTreeMap::from([
            (10, c.source_signers[&10].public_key()),
            (30, c.source_signers[&20].public_key()),
        ]),
        BTreeMap::from([
            (10, c.source_signers[&10].public_key()),
            (20, c.source_signers[&10].public_key()),
        ]),
        BTreeMap::from([
            (10, c.signer.public_key()),
            (20, c.source_signers[&20].public_key()),
        ]),
        BTreeMap::from([(10, [0; 32]), (20, c.source_signers[&20].public_key())]),
    ] {
        let root = tempfile::tempdir()?;
        let mut policy = c.limits.clone();
        policy.source_keys = keys;
        assert!(Node::open(root.path(), c.layout.clone(), 1, policy).is_err());
        assert_eq!(
            std::fs::read_dir(root.path())?.count(),
            0,
            "bad policy opened storage"
        );
    }
    let root = tempfile::tempdir()?;
    let mut node = Node::open(root.path(), c.layout.clone(), 4, c.limits.clone())?;
    assert!(node
        .set_source_signer(10, c.source_signers[&10].clone())
        .is_err());
    assert!(node
        .set_source_signer(20, c.source_signers[&10].clone())
        .is_err());
    node.set_source_signer(20, c.source_signers[&20].clone())?;
    drop(node);
    let mut policy = c.limits.clone();
    policy.source_keys.clear();
    let error = Node::open(root.path(), c.layout.clone(), 4, policy)
        .err()
        .context("policy change accepted")?;
    assert!(format!("{error:#}").contains("policy"));
    Ok(())
}

#[test]
fn splitting_hosted_groups_keeps_only_their_own_source_key() -> Result<()> {
    let c = Cluster::with_source_policy(128, 100_000, true)?;
    let root = tempfile::tempdir()?;
    let mut layout = (*c.layout).clone();
    layout.nodes.retain(|id, _| *id <= 3);
    layout.groups.insert(20, vec![1, 2, 3]);
    let mut node = Node::open(root.path(), Arc::new(layout), 1, c.limits.clone())?;
    node.set_catalog_signer(c.signer.clone())?;
    for (group, signer) in &c.source_signers {
        node.set_source_signer(*group, signer.clone())?;
    }
    let groups = node.into_groups();
    assert_eq!(groups.len(), 2);
    for node in groups {
        assert_eq!(node.groups.len(), 1);
        let group = *node.groups.keys().next().unwrap();
        assert_eq!(node.source_signers.len(), 1);
        assert_eq!(
            node.source_signers[&group].public_key(),
            c.limits.source_keys[&group]
        );
        assert_eq!(node.signer.is_some(), group == 10);
    }
    Ok(())
}

#[test]
fn empty_source_keys_preserve_independently_encoded_v9_policy_bytes() -> Result<()> {
    #[derive(Serialize)]
    struct LegacyLimits {
        catalog_key: Option<[u8; 32]>,
        max_tables: u64,
        max_active_plans: u64,
        max_plan_source_bytes: u64,
        max_descriptor_bytes: u64,
        max_manifest_records: u64,
        max_record_bytes: u64,
        max_records: u64,
        max_producers: u64,
        max_pending: u64,
        max_uncommitted_bytes: u64,
    }
    for limits in [
        Limits::default(),
        Limits {
            catalog_key: Some([17; 32]),
            max_tables: 7,
            max_active_plans: 3,
            max_plan_source_bytes: 12345,
            max_descriptor_bytes: 4321,
            max_manifest_records: 9,
            max_record_bytes: 16384,
            max_records: 123,
            max_producers: 31,
            max_pending: 43,
            max_uncommitted_bytes: 2 * 1024 * 1024,
            ..Default::default()
        },
    ] {
        let old = LegacyLimits {
            catalog_key: limits.catalog_key,
            max_tables: limits.max_tables,
            max_active_plans: limits.max_active_plans,
            max_plan_source_bytes: limits.max_plan_source_bytes,
            max_descriptor_bytes: limits.max_descriptor_bytes,
            max_manifest_records: limits.max_manifest_records,
            max_record_bytes: limits.max_record_bytes,
            max_records: limits.max_records,
            max_producers: limits.max_producers,
            max_pending: limits.max_pending,
            max_uncommitted_bytes: limits.max_uncommitted_bytes,
        };
        assert_eq!(
            limits.identity()?,
            bincode::serialize(&("lance-context/replicated-append/v9", old))?
        );
    }
    Ok(())
}
