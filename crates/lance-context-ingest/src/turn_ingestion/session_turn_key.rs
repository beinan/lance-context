//! Explicit runtime contract for an existing derived session/turn column.
use super::*;

pub(crate) const COLUMN: &str = "session_turn_key";
pub(crate) const INDEX: &str = "session_turn_zonemap_v1";
const METADATA: &str = "trace.session_turn_key";

pub(crate) fn validate(spec: &TurnIngestionSpec) -> Result<()> {
    ensure!(
        spec.primary_key == ["session_id", "id"] && spec.routing_column() == "session_id",
        "hex-utf8-v1 requires the original (session_id, id) identity and session routing"
    );
    validate_schema(spec.body_schema())
}

pub(crate) fn validate_schema(schema: &Schema) -> Result<()> {
    let field = schema.field_with_name(COLUMN)?;
    ensure!(
        field.data_type() == &DataType::Utf8
            && !field.is_nullable()
            && schema.metadata().get(METADATA).map(String::as_str) == Some("hex-utf8-v1"),
        "session_turn_key requires non-null UTF8 and the hex-utf8-v1 schema contract"
    );
    Ok(())
}

pub(crate) fn encode(session: &str, turn: &str) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut result = String::with_capacity(4 + 2 * (session.len() + turn.len()));
    result.push_str("v1:");
    for (i, value) in [session, turn].iter().enumerate() {
        if i != 0 {
            result.push('/');
        }
        for &byte in value.as_bytes() {
            result.push(HEX[(byte >> 4) as usize] as char);
            result.push(HEX[(byte & 15) as usize] as char);
        }
    }
    result
}

/// Verify borrowed Arrow values, without encoding or copying the body buffers.
/// Apply/replay and admission both call this when the runtime option is enabled.
pub(crate) fn validate_batch(batch: &RecordBatch) -> Result<()> {
    let columns = ["session_id", "id", COLUMN].map(|name| {
        batch
            .column_by_name(name)
            .and_then(|a| a.as_any().downcast_ref::<StringArray>())
            .context("missing session/turn derived key column")
    });
    let [session, turn, key] = columns;
    let (session, turn, key) = (session?, turn?, key?);
    for row in 0..batch.num_rows() {
        ensure!(
            !session.is_null(row)
                && !turn.is_null(row)
                && !key.is_null(row)
                && matches(key.value(row), session.value(row), turn.value(row)),
            "session_turn_key differs from its original business key"
        );
    }
    Ok(())
}

fn matches(key: &str, session: &str, turn: &str) -> bool {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let expected = b"v1:"
        .iter()
        .copied()
        .chain(
            session
                .bytes()
                .flat_map(|b| [HEX[(b >> 4) as usize], HEX[(b & 15) as usize]]),
        )
        .chain(std::iter::once(b'/'))
        .chain(
            turn.bytes()
                .flat_map(|b| [HEX[(b >> 4) as usize], HEX[(b & 15) as usize]]),
        );
    key.bytes().eq(expected)
}
