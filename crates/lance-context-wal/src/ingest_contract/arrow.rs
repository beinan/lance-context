use arrow_schema::Schema;
use serde_json::Value;

/// Fingerprint an Arrow 58 schema for a durable ingestion contract.
///
/// Sort every JSON object's keys, including nested field metadata, while
/// preserving field/array order and all serialized values. Uses the same domain
/// as [`super::schema_fingerprint`]. This identifies the serialized schema,
/// including dictionary attributes, rather than Arrow's looser schema equality.
/// An Arrow serialization change requires compatibility review before upgrading
/// writers of existing contracts. This does not validate an uploaded IPC body.
///
/// Compute once when binding a table generation; this allocates schema metadata
/// and is not intended for the per-record replication path.
pub fn arrow_schema_fingerprint(schema: &Schema) -> Result<[u8; 32], serde_json::Error> {
    Ok(super::schema_fingerprint(&canonical_document(schema)?))
}

fn canonical_document(schema: &Schema) -> Result<Vec<u8>, serde_json::Error> {
    fn canonical(value: Value) -> Value {
        match value {
            Value::Object(fields) => Value::Object(
                fields
                    .into_iter()
                    .map(|(key, value)| (key, canonical(value)))
                    .collect::<std::collections::BTreeMap<_, _>>()
                    .into_iter()
                    .collect(),
            ),
            Value::Array(values) => Value::Array(values.into_iter().map(canonical).collect()),
            value => value,
        }
    }
    serde_json::to_vec(&canonical(serde_json::to_value(schema)?))
}

#[cfg(test)]
mod tests;
