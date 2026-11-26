use super::*;
use arrow_schema::{DataType, Field};
use std::{collections::HashMap, sync::Arc};

fn metadata(reverse: bool) -> HashMap<String, String> {
    let pairs = if reverse {
        [("zeta", "last"), ("alpha", "first")]
    } else {
        [("alpha", "first"), ("zeta", "last")]
    };
    pairs
        .into_iter()
        .map(|(k, v)| (k.into(), v.into()))
        .collect()
}

#[test]
fn canonical_bytes_match_fixed_arrow_58_document() {
    let schema = Schema::new_with_metadata(
        vec![Field::new("event", DataType::Utf8, false).with_metadata(metadata(true))],
        [("owner".into(), "public".into())].into(),
    );
    let expected = concat!(
        "{\"fields\":[{\"data_type\":\"Utf8\",\"dict_id\":0,\"dict_is_ordered\":false,",
        "\"metadata\":{\"alpha\":\"first\",\"zeta\":\"last\"},\"name\":\"event\",\"nullable\":false}],",
        "\"metadata\":{\"owner\":\"public\"}}"
    );
    assert_eq!(canonical_document(&schema).unwrap(), expected.as_bytes());
    // SHA-256 over the domain, little-endian u64 byte length and the fixed
    // document above, independently calculated with OpenSSL.
    assert_eq!(
        arrow_schema_fingerprint(&schema).unwrap(),
        [
            0x92, 0xd1, 0xbf, 0x40, 0x10, 0xe1, 0x9a, 0xf8, 0x01, 0x7a, 0xe8, 0x0f, 0x7e, 0xa9,
            0x4b, 0x46, 0x67, 0x3b, 0xa3, 0xc1, 0xee, 0x41, 0x7e, 0x4e, 0xf9, 0x62, 0x49, 0xae,
            0x2e, 0x49, 0x2a, 0xab,
        ]
    );
}

fn nested(reverse: bool) -> Schema {
    let child =
        Arc::new(Field::new("child", DataType::Utf8, false).with_metadata(metadata(reverse)));
    Schema::new_with_metadata(
        vec![
            Field::new("nested", DataType::List(child), false).with_metadata(metadata(reverse)),
            Field::new("other", DataType::UInt64, false),
        ],
        metadata(reverse),
    )
}

#[test]
fn metadata_order_and_json_round_trip_preserve_identity() {
    let expected = arrow_schema_fingerprint(&nested(false)).unwrap();
    // Each fresh HashMap has an independent iteration seed. Nested metadata and
    // schema metadata must stay stable across service restarts and JSON reloads.
    for attempt in 0..32 {
        let schema = nested(attempt % 2 == 0);
        assert_eq!(arrow_schema_fingerprint(&schema).unwrap(), expected);
        let restored: Schema =
            serde_json::from_slice(&serde_json::to_vec(&schema).unwrap()).unwrap();
        assert_eq!(arrow_schema_fingerprint(&restored).unwrap(), expected);
    }
}

#[test]
fn identity_preserves_field_order_type_nullability_and_metadata_values() {
    let original = nested(false);
    let expected = arrow_schema_fingerprint(&original).unwrap();
    let mut variants = vec![
        Schema::new_with_metadata(
            original.fields().iter().rev().cloned().collect::<Vec<_>>(),
            original.metadata().clone(),
        ),
        original.clone().with_metadata(
            metadata(true)
                .into_iter()
                .map(|(k, v)| (k, v + "!"))
                .collect(),
        ),
    ];
    for changed in [
        Field::new("other", DataType::Int64, false),
        Field::new("other", DataType::UInt64, true),
        Field::new("renamed", DataType::UInt64, false),
    ] {
        variants.push(Schema::new_with_metadata(
            vec![original.field(0).clone(), changed],
            original.metadata().clone(),
        ));
    }
    let changed_child = Arc::new(
        Field::new("child", DataType::Utf8, false)
            .with_metadata([("alpha".into(), "changed".into())].into()),
    );
    variants.push(Schema::new_with_metadata(
        vec![
            Field::new("nested", DataType::List(changed_child), false)
                .with_metadata(metadata(false)),
            original.field(1).clone(),
        ],
        original.metadata().clone(),
    ));
    for schema in variants {
        assert_ne!(arrow_schema_fingerprint(&schema).unwrap(), expected);
    }
}

#[test]
fn dictionary_attributes_remain_part_of_the_serialized_contract() {
    let field = Field::new(
        "label",
        DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8)),
        false,
    );
    let plain = Schema::new(vec![field.clone()]);
    let ordered = Schema::new(vec![field.with_dict_is_ordered(true)]);
    assert_ne!(
        arrow_schema_fingerprint(&plain).unwrap(),
        arrow_schema_fingerprint(&ordered).unwrap()
    );
}
