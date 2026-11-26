use super::*;
use crate::ingest_contract::WritePolicy;

pub(super) fn input() -> Acquire {
    Acquire {
        table: Uuid::from_u128(10),
        destination: Destination {
            namespace: Uuid::from_u128(11),
            dataset: "canonical/table".into(),
        },
        ingest: IngestContract {
            tenant: "tenant".into(),
            qualified_table: "table".into(),
            schema: [3; 32],
            policy: WritePolicy::ImmutableUniqueIds,
            max_record_bytes: 1024 * 1024,
            max_rows: 1024,
        },
        owner: Uuid::from_u128(12),
        request: Uuid::from_u128(13),
        expected_epoch: 0,
    }
}
fn apply(catalog: &mut Catalog, acquire: Acquire, index: u64, capacity: u64) -> Result<Reply> {
    let batch = acquire.batch(&super::super::tests::layout())?;
    Ok(catalog.apply(
        decode(&batch.body)?,
        index,
        &Limits {
            max_tables: capacity,
            ..Default::default()
        },
    ))
}

#[test]
fn competing_acquisitions_bind_once_and_retry_only_the_exact_current_grant() -> Result<()> {
    let mut catalog = Catalog::default();
    let mut acquire = input();
    let first = apply(&mut catalog, acquire.clone(), 5, 1)?;
    assert_eq!(apply(&mut catalog, acquire.clone(), 6, 1)?, first);
    let mut changed = acquire.clone();
    changed.ingest.schema[0] ^= 1;
    assert_eq!(
        apply(&mut catalog, changed, 7, 1)?,
        Reply::Rejected(Rejection::BindingConflict)
    );
    changed = acquire.clone();
    changed.expected_epoch = 1;
    assert_eq!(
        apply(&mut catalog, changed, 8, 1)?,
        Reply::Rejected(Rejection::RequestReused)
    );
    changed = acquire.clone();
    changed.owner = Uuid::from_u128(20);
    changed.request = Uuid::from_u128(21);
    assert_eq!(
        apply(&mut catalog, changed.clone(), 9, 1)?,
        Reply::Rejected(Rejection::Epoch { current: 1 })
    );
    changed.expected_epoch = 1;
    let second = apply(&mut catalog, changed.clone(), 10, 1)?;
    assert!(matches!(&second, Reply::Acquired(a) if a.epoch == 2 && a.grant_index == 10));
    assert_eq!(apply(&mut catalog, changed, 11, 1)?, second);
    assert_eq!(
        apply(&mut catalog, acquire.clone(), 12, 1)?,
        Reply::Rejected(Rejection::Epoch { current: 2 })
    );
    acquire.table = Uuid::from_u128(22);
    assert_eq!(
        apply(&mut catalog, acquire.clone(), 13, 8)?,
        Reply::Rejected(Rejection::DestinationBound)
    );
    acquire.destination.namespace = Uuid::from_u128(23);
    assert_eq!(
        apply(&mut catalog, acquire.clone(), 14, 1)?,
        Reply::Rejected(Rejection::Capacity)
    );
    assert!(matches!(
        apply(&mut catalog, acquire, 15, 8)?,
        Reply::Acquired(_)
    ));
    assert_eq!(catalog.tables.len(), 2);
    Ok(())
}

#[test]
fn bounded_decode_rejects_wrong_scopes_domains_and_unserviceable_ingest_limits() -> Result<()> {
    let layout = super::super::tests::layout();
    let limits = Limits::default();
    let mut batch = input().batch(&layout)?;
    validate_record(&layout, 10, &limits, &batch.meta, &batch.body)?;
    assert!(validate_record(&layout, 20, &limits, &batch.meta, &batch.body).is_err());
    let mut foreign = (*layout).clone();
    foreign.cluster = Uuid::from_u128(100);
    assert!(validate_record(&foreign, 10, &limits, &batch.meta, &batch.body).is_err());
    batch.meta.stream = Uuid::from_u128(101);
    assert!(validate_record(&layout, 10, &limits, &batch.meta, &batch.body).is_err());
    batch = input().batch(&layout)?;
    let mut trailing = batch.body.to_vec();
    trailing.push(0);
    assert!(decode(&trailing).is_err());
    assert!(decode(&vec![0; MAX_COMMAND_BYTES + 1]).is_err());
    let mut oversized = input();
    oversized.destination.dataset = "x".repeat(4097);
    assert!(oversized.batch(&layout).is_err());
    let mut exhausted = input();
    exhausted.expected_epoch = u64::MAX;
    assert!(exhausted.batch(&layout).is_err());
    let mut contract = input();
    contract.ingest.max_record_bytes = 16 * 1024 * 1024;
    batch = contract.batch(&layout)?;
    assert!(validate_record(&layout, 10, &limits, &batch.meta, &batch.body).is_err());
    Ok(())
}

#[test]
fn checkpoint_catalog_requires_exact_scope_ordering_identity_and_unique_destinations() -> Result<()>
{
    let layout = super::super::tests::layout();
    let limits = Limits::default();
    let mut catalog = Catalog::default();
    let acquire = input();
    apply(&mut catalog, acquire.clone(), 4, 8)?;
    catalog.validate(&layout, 10, 4, &limits)?;
    assert!(catalog.validate(&layout, 20, 4, &limits).is_err());
    assert!(catalog.validate(&layout, 10, 3, &limits).is_err());
    let original = catalog.tables[&acquire.table].clone();
    assert_eq!(decode_table(&encode_table(&original)?)?, original);
    for mutate in [
        (|t: &mut Table| t.authority.epoch = 5) as fn(&mut Table),
        |t| t.authority.grant_index = 0,
        |t| t.authority.owner = Uuid::nil(),
        |t| t.authority.layout[0] ^= 1,
        |t| t.request = Uuid::nil(),
        |t| t.ingest.schema = [0; 32],
    ] {
        let mut table = original.clone();
        mutate(&mut table);
        let changed = Catalog {
            tables: BTreeMap::from([(acquire.table, table)]),
        };
        assert!(changed.validate(&layout, 10, 4, &limits).is_err());
    }
    let other = Uuid::from_u128(400);
    let mut duplicate = original;
    duplicate.authority.table = other;
    catalog.tables.insert(other, duplicate);
    assert!(catalog.validate(&layout, 10, 4, &limits).is_err());
    Ok(())
}

#[test]
fn registered_records_must_fit_the_publication_source_budget() -> Result<()> {
    let layout = super::super::tests::layout();
    let limits = Limits {
        max_plan_source_bytes: 512 * 1024,
        ..Default::default()
    };
    limits.identity()?; // A small publication budget is valid for smaller contracts.
    let mut acquire = input();
    let batch = acquire.clone().batch(&layout)?;
    assert!(
        validate_record(&layout, 10, &limits, &batch.meta, &batch.body).is_err(),
        "registered contract can admit a record no publication plan can select"
    );
    let mut catalog = Catalog::default();
    apply(&mut catalog, acquire.clone(), 4, 1)?;
    assert!(
        catalog.validate(&layout, 10, 4, &limits).is_err(),
        "checkpoint restored an unserviceable ingestion contract"
    );

    acquire.ingest.max_record_bytes = limits.max_plan_source_bytes as u32;
    let batch = acquire.clone().batch(&layout)?;
    validate_record(&layout, 10, &limits, &batch.meta, &batch.body)?;
    let mut catalog = Catalog::default();
    apply(&mut catalog, acquire, 4, 1)?;
    catalog.validate(&layout, 10, 4, &limits)?;
    Ok(())
}

#[test]
fn failed_follower_sync_cannot_release_a_publisher_grant() -> Result<()> {
    let root = tempfile::tempdir()?;
    let layout = super::super::tests::layout();
    let mut nodes = (1..=3)
        .map(|id| {
            Node::open(
                &root.path().join(id.to_string()),
                layout.clone(),
                id,
                Limits::default(),
            )
        })
        .collect::<Result<Vec<_>>>()?;
    nodes[0].campaign(10)?;
    // Elect with real persistent logs, then cut delivery to the third replica.
    for _ in 0..100 {
        let mut packets = Vec::new();
        for node in &mut nodes {
            packets.extend(node.poll()?.messages);
        }
        if packets.is_empty() {
            break;
        }
        for packet in packets {
            nodes[packet.message.to as usize - 1].step(packet)?;
        }
    }
    nodes[0].acquire_publisher(1, input())?;
    let events = nodes[0].poll()?;
    assert!(events.completions.is_empty());
    assert!(nodes[0].publisher(input().table)?.is_none());
    let mut delivered = 0;
    for packet in events.messages.into_iter().filter(|p| p.message.to == 2) {
        delivered += 1;
        nodes[1].step(packet)?;
    }
    assert!(delivered > 0, "publisher proposal never reached follower");
    nodes[1].groups[&10].raw.store().fail_next_sync_for_test();
    assert!(nodes[1].poll().is_err());
    assert!(nodes[1].publisher(input().table).is_err());
    assert!(nodes[1].status(20).is_err(), "shared engine not fenced");
    assert!(nodes[0].poll()?.completions.is_empty());
    assert!(nodes[0].publisher(input().table)?.is_none());
    Ok(())
}
