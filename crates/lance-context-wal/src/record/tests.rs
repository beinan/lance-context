use super::*;

#[test]
fn payload_hash_backend_preserves_existing_digests_at_block_boundaries() {
    use sha2::{Digest, Sha256};

    let data: Vec<_> = (0..512 * 1024 + 3)
        .map(|index| ((index * 29 + index / 251) % 256) as u8)
        .collect();
    for offset in [0, 1, 3] {
        for size in [0, 1, 55, 56, 63, 64, 65, 127, 128, 129, 4096, 512 * 1024] {
            let body = &data[offset..offset + size];
            let old_digest: [u8; 32] = Sha256::digest(body).into();
            assert_eq!(payload_digest(body), old_digest);
            if !body.is_empty() {
                // A record written by the previous backend remains readable.
                let mut batch = record(Bytes::copy_from_slice(body));
                assert_eq!(batch.meta.digest, old_digest);
                batch.meta.digest = old_digest;
                batch.validate(size).unwrap();
                batch.meta.digest[31] ^= 1;
                assert_eq!(batch.validate(size), Err(ValidationError::DigestMismatch));
            }
        }
    }
}

fn record(body: Bytes) -> Batch {
    Batch::new(
        Uuid::from_bytes([0x11; 16]),
        17,
        Uuid::from_bytes([0x22; 16]),
        7,
        body,
    )
}

#[test]
fn payload_owners_share_slices_and_outlive_the_original() {
    let body = Bytes::from(vec![37; 1024 * 1024]);
    let batch = record(body.slice(3..body.len() - 5));
    let copy = batch.clone();
    assert_eq!(batch.body.as_ptr(), body[3..].as_ptr());
    assert_eq!(copy.body.as_ptr(), batch.body.as_ptr());
    drop(batch);
    drop(body);
    copy.validate(1024 * 1024).unwrap();
    assert!(copy.body.iter().all(|byte| *byte == 37));
}

#[test]
fn length_limits_are_checked_before_payload_validation() {
    let mut meta = record(Bytes::from_static(b"abc")).meta;
    assert!(meta.check_body_length(3, 3).is_ok());
    assert_eq!(
        meta.check_body_length(3, 2),
        Err(ValidationError::BodyTooLarge { bytes: 3, limit: 2 })
    );
    assert_eq!(
        meta.check_body_length(0, 3),
        Err(ValidationError::EmptyBody)
    );
    meta.body_bytes = u64::MAX;
    assert_eq!(
        meta.check_body_length(3, 3),
        Err(ValidationError::LengthMismatch {
            declared: u64::MAX,
            actual: 3
        })
    );
    meta.body_bytes = 3;
    meta.sequence = 0;
    assert_eq!(
        meta.check_body_length(3, 3),
        Err(ValidationError::ZeroSequence)
    );
}

#[test]
fn changed_payloads_and_truncation_are_rejected_without_decoding() {
    let expected = record(Bytes::from_static(b"opaque payload"));
    let mut changed = expected.clone();
    changed.body = Bytes::from_static(b"opaque payloaD");
    assert_eq!(changed.validate(64), Err(ValidationError::DigestMismatch));
    changed.body = expected.body.slice(..expected.body.len() - 1);
    assert!(matches!(
        changed.validate(64),
        Err(ValidationError::LengthMismatch { .. })
    ));
    changed.body = expected.body;
    changed.meta.digest[0] ^= 1;
    assert_eq!(changed.validate(64), Err(ValidationError::DigestMismatch));
}

#[test]
fn record_domains_keep_the_metadata_wire_layout() {
    // Fixed-int bincode 1: domain u32, length-prefixed UUID byte strings, bucket
    // u32, sequence u64, digest bytes and body length u64, all integers LE.
    // SHA-256("abc") is the standard published test vector.
    let suffix = concat!(
        "100000000000000011111111111111111111111111111111",
        "11000000",
        "100000000000000022222222222222222222222222222222",
        "0700000000000000",
        "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad",
        "0300000000000000",
    );
    for (tag, kind) in [
        RecordKind::Data,
        RecordKind::Publication,
        RecordKind::MultiPublication,
        RecordKind::Source,
    ]
    .into_iter()
    .enumerate()
    {
        let mut meta = record(Bytes::from_static(b"abc")).meta;
        meta.kind = kind;
        let expected = format!("{tag:02x}000000{suffix}");
        let encoded = bincode::serialize(&meta).unwrap();
        let hex: String = encoded.iter().map(|byte| format!("{byte:02x}")).collect();
        assert_eq!(hex, expected);
        assert_eq!(bincode::deserialize::<BatchMeta>(&encoded).unwrap(), meta);
    }
}

#[test]
fn control_envelopes_allow_their_reserved_producer_and_hide_payload_in_debug() {
    let mut batch = record(Bytes::from_static(b"private command body"));
    batch.meta.kind = RecordKind::Source;
    batch.meta.producer = Uuid::nil();
    batch.validate(64).unwrap();
    assert!(!format!("{batch:?}").contains("private command body"));
    let copy: Batch = bincode::deserialize(&bincode::serialize(&batch).unwrap()).unwrap();
    assert_eq!(copy, batch);
}
