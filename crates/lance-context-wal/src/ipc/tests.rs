use super::*;
use arrow_array::{
    types::Int8Type, ArrayRef, DictionaryArray, Int8Array, LargeBinaryArray, NullArray, StringArray,
};
use arrow_ipc::writer::{FileWriter, IpcWriteOptions};
use arrow_schema::Schema;
use std::sync::Arc;

fn fixture() -> RecordBatch {
    let payload = vec![73u8; 256 * 1024];
    RecordBatch::try_from_iter([
        (
            "payload",
            Arc::new(LargeBinaryArray::from(vec![
                Some(payload.as_slice()),
                None,
                Some(&[]),
            ])) as ArrayRef,
        ),
        (
            "role",
            Arc::new(
                DictionaryArray::<Int8Type>::try_new(
                    Int8Array::from(vec![Some(0), None, Some(1)]),
                    Arc::new(StringArray::from(vec!["request", "response"])),
                )
                .unwrap(),
            ) as ArrayRef,
        ),
    ])
    .unwrap()
}

fn encode(batches: &[RecordBatch], compressed: bool) -> Result<Bytes> {
    let mut bytes = Vec::new();
    let options = IpcWriteOptions::default()
        .try_with_compression(compressed.then_some(arrow_ipc::CompressionType::ZSTD))?;
    let mut writer = FileWriter::try_new_with_options(&mut bytes, &batches[0].schema(), options)?;
    for batch in batches {
        writer.write(batch)?;
    }
    writer.finish()?;
    drop(writer);
    Ok(bytes.into())
}

#[test]
fn dictionaries_nulls_and_unaligned_binary_keep_input_ownership() -> Result<()> {
    let batch = fixture();
    let encoded = encode(&[batch.clone(), batch.clone()], false)?;
    for prefix in [0, 1] {
        let mut bytes = vec![0; prefix];
        bytes.extend_from_slice(&encoded);
        let bytes = Bytes::from(bytes).slice(prefix..);
        let start = bytes.as_ptr() as usize;
        let end = start + bytes.len();
        let decoded = decode(bytes, batch.schema())?;
        // The caller's Bytes has been consumed. Arrow owns the backing storage.
        assert_eq!(decoded, vec![batch.clone(), batch.clone()]);
        for batch in &decoded {
            let binary = batch
                .column(0)
                .as_any()
                .downcast_ref::<LargeBinaryArray>()
                .unwrap();
            let pointer = binary.value_data().as_ptr() as usize;
            assert!(pointer >= start && pointer + binary.value_data().len() <= end);
        }
    }
    Ok(())
}

#[test]
fn rejects_truncation_wrong_schema_and_unsupported_expansion() -> Result<()> {
    let batch = fixture();
    let bytes = encode(std::slice::from_ref(&batch), false)?;
    for len in [0, 5, 15, bytes.len() / 2, bytes.len() - 8, bytes.len() - 1] {
        assert!(decode(bytes.slice(..len), batch.schema()).is_err());
    }
    assert!(decode(bytes, Arc::new(Schema::empty())).is_err());
    let compressed = encode(std::slice::from_ref(&batch), true)?;
    assert!(decode(compressed, batch.schema())
        .unwrap_err()
        .to_string()
        .contains("compressed IPC"));
    // Null rows encode almost no payload; row limits must apply before decoding.
    let huge = RecordBatch::try_from_iter([(
        "nulls",
        Arc::new(NullArray::new(MAX_ROWS as usize + 1)) as ArrayRef,
    )])?;
    assert!(decode(encode(std::slice::from_ref(&huge), false)?, huge.schema()).is_err());
    assert!(decode(encode(&vec![batch.clone(); 129], false)?, batch.schema()).is_err());
    Ok(())
}

#[test]
fn rejects_invalid_footer_and_block_ranges_before_slicing() -> Result<()> {
    let batch = fixture();
    let bytes = encode(std::slice::from_ref(&batch), false)?;
    let end = bytes.len() - 10;
    let footer_len = read_footer_length(bytes[end..].try_into()?)?;
    let block_offset = {
        let footer = arrow_ipc::root_as_footer(&bytes[end - footer_len..end]).unwrap();
        let block = footer.recordBatches().unwrap().get(0);
        block as *const Block as usize - bytes.as_ptr() as usize
    };
    for offset in [1i64, -1, i64::MAX] {
        let mut corrupt = bytes.to_vec();
        corrupt[block_offset..block_offset + 8].copy_from_slice(&offset.to_le_bytes());
        assert!(decode(corrupt.into(), batch.schema()).is_err());
    }
    let mut corrupt = bytes.to_vec();
    corrupt[end..end + 4].copy_from_slice(&((MAX_METADATA + 1) as i32).to_le_bytes());
    assert!(decode(corrupt.into(), batch.schema()).is_err());
    Ok(())
}
