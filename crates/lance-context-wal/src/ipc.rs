//! IPC file decoding for materializers over reference-counted input buffers.
//! Replication does not need this optional module or decode Arrow rows.
//! Compressed IPC is rejected until expansion is separately budgeted.
use anyhow::{ensure, Context, Result};
use arrow_array::RecordBatch;
use arrow_buffer::Buffer;
use arrow_ipc::{
    reader::{read_footer_length, FileDecoder},
    Block,
};
use arrow_schema::SchemaRef;
use bytes::Bytes;

const MAX_METADATA: usize = 1024 * 1024;
const MAX_ROWS: i64 = 1_000_000;

#[cfg(test)]
mod tests;

/// Decode an uncompressed IPC file against the expected table schema.
///
/// Binary values share the input's ownership; alignment repair and Arrow array
/// metadata can still allocate. Callers must bound/reserve input and decoding
/// workspace before calling. The format checks and row/count limits here do not
/// replace process-wide memory admission or a transport checksum.
/// Dictionary deltas and compressed batches are intentionally unsupported.
pub fn decode(body: Bytes, schema: SchemaRef) -> Result<Vec<RecordBatch>> {
    let buffer = Buffer::from(body);
    ensure!(
        buffer.len() >= 16 && &buffer[..6] == b"ARROW1",
        "not an IPC file"
    );
    let end = buffer.len() - 10;
    let footer_len = read_footer_length(buffer[end..].try_into()?)?;
    ensure!(footer_len <= MAX_METADATA, "IPC footer too large");
    let start = end
        .checked_sub(footer_len)
        .context("invalid IPC footer range")?;
    let footer = arrow_ipc::root_as_footer(&buffer[start..end])
        .map_err(|e| anyhow::anyhow!("invalid IPC footer: {e}"))?;
    let source_schema =
        arrow_ipc::convert::fb_to_schema(footer.schema().context("missing IPC schema")?);
    ensure!(
        source_schema == *schema,
        "IPC schema differs from expected schema"
    );
    let records = footer.recordBatches().context("missing record blocks")?;
    ensure!(
        !records.is_empty() && records.len() <= 128,
        "invalid batch count"
    );
    let dictionaries = footer.dictionaries();
    ensure!(
        dictionaries.is_none_or(|d| d.len() <= 128),
        "too many dictionary blocks"
    );
    let mut blocks = Vec::new();
    if let Some(dictionaries) = dictionaries {
        blocks.extend(dictionaries.iter().map(|b| (*b, true)));
    }
    blocks.extend(records.iter().map(|b| (*b, false)));
    blocks.sort_by_key(|(b, _)| b.offset());
    let mut previous_end = 8;
    let mut rows = 0;
    // Validate the complete block layout and compression before decoding any
    // arrays. Disallow overlapping blocks and bound logical rows, including
    // null/dictionary values that could otherwise expand from tiny messages.
    for (block, dictionary) in &blocks {
        let offset = usize::try_from(block.offset())?;
        let length = usize::try_from(block.metaDataLength())?
            .checked_add(usize::try_from(block.bodyLength())?)
            .context("IPC block overflow")?;
        let block_end = offset.checked_add(length).context("IPC offset overflow")?;
        ensure!(
            offset >= previous_end && block_end <= start,
            "overlapping/out-of-range IPC blocks"
        );
        previous_end = block_end;
        rows += validate_message(block, &buffer[offset..block_end], *dictionary)?;
        ensure!(rows <= MAX_ROWS, "IPC row limit exceeded");
    }
    let mut decoder = FileDecoder::new(schema, footer.version());
    // Match FileReader: load dictionary blocks before records, preserving
    // dictionary replacement/delta validation in Arrow's decoder.
    for (block, _) in blocks.iter().filter(|(_, dictionary)| *dictionary) {
        decoder.read_dictionary(block, &slice(&buffer, block))?;
    }
    let mut output = Vec::new();
    for (block, _) in blocks.iter().filter(|(_, dictionary)| !dictionary) {
        output.push(
            decoder
                .read_record_batch(block, &slice(&buffer, block))?
                .context("missing IPC batch")?,
        );
    }
    Ok(output)
}

fn slice(buffer: &Buffer, block: &Block) -> Buffer {
    buffer.slice_with_length(
        block.offset() as usize,
        block.metaDataLength() as usize + block.bodyLength() as usize,
    )
}

fn validate_message(block: &Block, bytes: &[u8], dictionary: bool) -> Result<i64> {
    let metadata_len = usize::try_from(block.metaDataLength())?;
    ensure!(
        (8..=MAX_METADATA).contains(&metadata_len),
        "invalid IPC metadata size"
    );
    let prefix = if bytes.get(..4) == Some(&[255; 4]) {
        8
    } else {
        4
    };
    let metadata = bytes
        .get(prefix..metadata_len)
        .context("truncated IPC metadata")?;
    let message = arrow_ipc::root_as_message(metadata)
        .map_err(|e| anyhow::anyhow!("invalid IPC message: {e}"))?;
    ensure!(
        message.bodyLength() == block.bodyLength(),
        "IPC body length mismatch"
    );
    let batch = if dictionary {
        let dict = message
            .header_as_dictionary_batch()
            .context("invalid dictionary block")?;
        ensure!(
            !dict.isDelta(),
            "dictionary deltas require separate expansion accounting"
        );
        dict.data().context("missing dictionary data")?
    } else {
        message
            .header_as_record_batch()
            .context("invalid record block")?
    };
    ensure!(
        batch.compression().is_none(),
        "compressed IPC requires expansion accounting"
    );
    ensure!(
        (0..=MAX_ROWS).contains(&batch.length()),
        "invalid IPC row count"
    );
    if let Some(nodes) = batch.nodes() {
        ensure!(
            nodes
                .iter()
                .all(|node| (0..=MAX_ROWS).contains(&node.length())),
            "nested IPC row limit exceeded"
        );
    }
    Ok(batch.length())
}
