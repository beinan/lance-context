//! Preserve exact legacy table bytes until a table has its first advancement.
//! A bounded tagged suffix carries the new receipt in application checkpoints.
use super::*;
use std::io::Cursor;

const EXTENSION: [u8; 8] = *b"LCWALAD1";

#[derive(Serialize, Deserialize)]
struct LegacyProgress {
    checkpoint: BTreeMap<u64, u64>,
    minimum_read_version: u64,
    pending: Option<Arc<PendingPlan>>,
    prepared: Option<Arc<Prepared>>,
    published: Option<Arc<Published>>,
}

#[derive(Serialize, Deserialize)]
struct LegacyTable {
    manifests: Option<Arc<ManifestIndex>>,
    destination: Destination,
    ingest: IngestContract,
    authority: Authority,
    request: Uuid,
    progress: LegacyProgress,
}

pub(super) fn encode(table: &Table) -> Result<Vec<u8>> {
    let progress = &table.progress;
    let legacy = LegacyTable {
        manifests: table.manifests.clone(),
        destination: table.destination.clone(),
        ingest: table.ingest.clone(),
        authority: table.authority.clone(),
        request: table.request,
        progress: LegacyProgress {
            checkpoint: progress.checkpoint.clone(),
            minimum_read_version: progress.minimum_read_version,
            pending: progress.pending.clone(),
            prepared: progress.prepared.clone(),
            published: progress.published.clone(),
        },
    };
    let mut bytes = codec(MAX_TABLE_BYTES).serialize(&legacy)?;
    if let Some(advanced) = &progress.advanced {
        bytes.extend(codec(advance::MAX_ADVANCED_BYTES).serialize(&(EXTENSION, advanced))?);
    }
    ensure!(bytes.len() <= MAX_TABLE_BYTES, "publisher table byte limit");
    Ok(bytes)
}

pub(super) fn decode(bytes: &[u8]) -> Result<Table> {
    ensure!(bytes.len() <= MAX_TABLE_BYTES, "publisher table byte limit");
    let mut reader = Cursor::new(bytes);
    let legacy: LegacyTable = codec(MAX_TABLE_BYTES)
        .allow_trailing_bytes()
        .deserialize_from(&mut reader)?;
    let advanced = if reader.position() as usize == bytes.len() {
        None
    } else {
        let (tag, advanced): ([u8; 8], Arc<Advanced>) =
            codec(advance::MAX_ADVANCED_BYTES).deserialize(&bytes[reader.position() as usize..])?;
        ensure!(tag == EXTENSION, "unknown table extension");
        Some(advanced)
    };
    let table = Table {
        manifests: legacy.manifests,
        destination: legacy.destination,
        ingest: legacy.ingest,
        authority: legacy.authority,
        request: legacy.request,
        progress: Progress {
            checkpoint: legacy.progress.checkpoint,
            minimum_read_version: legacy.progress.minimum_read_version,
            pending: legacy.progress.pending,
            prepared: legacy.progress.prepared,
            published: legacy.progress.published,
            advanced,
        },
    };
    ensure!(encode(&table)? == bytes, "noncanonical publisher table");
    Ok(table)
}
