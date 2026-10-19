//! Component measurements only: these numbers are NOT distributed-WAL throughput.
use std::alloc::{GlobalAlloc, Layout, System};
use std::hint::black_box;
use std::io::Cursor;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Barrier};
use std::time::Instant;

use anyhow::{ensure, Context, Result};
use arrow_array::{Array, LargeBinaryArray, RecordBatch, StringArray, UInt64Array};
use arrow_buffer::Buffer;
use arrow_ipc::reader::{read_footer_length, FileDecoder, FileReader};
use arrow_ipc::writer::FileWriter;
use bytes::Bytes;
use clap::{Parser, Subcommand};
use hdrhistogram::Histogram;
use raft_engine::{Config, Engine, LogBatch, MessageExt, ReadableSize};
use raft_proto::eraftpb::Entry;
use serde_json::json;

struct CountingAllocator;
static ALLOCS: AtomicU64 = AtomicU64::new(0);
static ALLOC_BYTES: AtomicU64 = AtomicU64::new(0);
unsafe impl GlobalAlloc for CountingAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        ALLOCS.fetch_add(1, Ordering::Relaxed);
        ALLOC_BYTES.fetch_add(layout.size() as u64, Ordering::Relaxed);
        System.alloc(layout)
    }
    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        System.dealloc(ptr, layout)
    }
    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, size: usize) -> *mut u8 {
        ALLOCS.fetch_add(1, Ordering::Relaxed);
        ALLOC_BYTES.fetch_add(size as u64, Ordering::Relaxed);
        System.realloc(ptr, layout, size)
    }
}
#[global_allocator]
static ALLOCATOR: CountingAllocator = CountingAllocator;

#[derive(Parser)]
struct Args {
    #[command(subcommand)]
    mode: Mode,
}
#[derive(Subcommand)]
enum Mode {
    /// With strace, demonstrate the pinned engine's empty-sync behavior.
    SyncProbe {
        #[arg(long)]
        directory: PathBuf,
    },
    /// Same Arrow IPC bytes through reader vs shared-buffer decoder vs row copies.
    Codec {
        #[arg(long, default_value_t = 262144)]
        payload_bytes: usize,
        #[arg(long, default_value_t = 8)]
        rows: usize,
        #[arg(long, default_value_t = 30)]
        iterations: usize,
    },
    /// Multi-group fsync baseline, followed by reopen and full payload verification.
    Disk {
        /// Must not exist; no existing data is deleted or overwritten.
        #[arg(long)]
        directory: PathBuf,
        #[arg(long, default_value_t = 262144)]
        payload_bytes: usize,
        #[arg(long, default_value_t = 4)]
        batch: usize,
        #[arg(long, default_value_t = 4)]
        writers: usize,
        #[arg(long, default_value_t = 4)]
        groups_per_writer: usize,
        #[arg(long, default_value_t = 64)]
        rounds: usize,
        #[arg(long, default_value_t = 0)]
        compression_threshold: u64,
    },
}

fn payload(size: usize, seed: u64) -> Bytes {
    // Stable, non-zero, incompressible-ish bytes; generation is outside timing.
    let mut x = seed + 1;
    let mut out = Vec::with_capacity(size);
    for _ in 0..size {
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        out.push(x as u8);
    }
    out.into()
}

fn make_batch(rows: usize, size: usize) -> Result<RecordBatch> {
    let bodies: Vec<_> = (0..rows).map(|i| payload(size, i as u64)).collect();
    Ok(RecordBatch::try_from_iter([
        (
            "id",
            Arc::new(StringArray::from_iter_values(
                (0..rows).map(|i| format!("event-{i}")),
            )) as Arc<dyn Array>,
        ),
        (
            "rollout_id",
            Arc::new(StringArray::from_iter_values(
                (0..rows).map(|i| format!("session-{}", i % 4)),
            )) as _,
        ),
        (
            "sequence_order",
            Arc::new(UInt64Array::from_iter_values(0..rows as u64)) as _,
        ),
        (
            "binary_payload",
            Arc::new(LargeBinaryArray::from_iter_values(
                bodies.iter().map(|b| b.as_ref()),
            )) as _,
        ),
    ])?)
}

fn encode(batch: &RecordBatch) -> Result<Vec<u8>> {
    let mut data = Vec::new();
    let mut writer = FileWriter::try_new(&mut data, batch.schema().as_ref())?;
    writer.write(batch)?;
    writer.finish()?;
    drop(writer);
    Ok(data)
}

fn decode(buffer: &Buffer, schema: arrow_schema::SchemaRef) -> Result<RecordBatch> {
    let end = buffer.len().checked_sub(10).context("short IPC file")?;
    let len = read_footer_length(buffer[end..].try_into()?)?;
    let start = end.checked_sub(len).context("bad footer")?;
    let footer = arrow_ipc::root_as_footer(&buffer[start..end])
        .map_err(|e| anyhow::anyhow!("invalid IPC footer: {e}"))?;
    let decoder = FileDecoder::new(schema, footer.version());
    let blocks = footer.recordBatches().context("no batches")?;
    ensure!(blocks.len() == 1, "one batch required");
    let block = blocks.get(0);
    let length = block.bodyLength() as usize + block.metaDataLength() as usize;
    let bytes = buffer.slice_with_length(block.offset() as usize, length);
    decoder
        .read_record_batch(block, &bytes)?
        .context("missing batch")
}

fn measured<T>(
    name: &str,
    iterations: usize,
    bytes: usize,
    mut work: impl FnMut() -> Result<T>,
) -> Result<()> {
    black_box(work()?); // Warm caches and lazy initialization equally for every path.
    let count = ALLOCS.load(Ordering::Relaxed);
    let allocated = ALLOC_BYTES.load(Ordering::Relaxed);
    let start = Instant::now();
    for _ in 0..iterations {
        black_box(work()?);
    }
    let elapsed = start.elapsed();
    let allocations = ALLOCS.load(Ordering::Relaxed) - count;
    let allocated_bytes = ALLOC_BYTES.load(Ordering::Relaxed) - allocated;
    println!(
        "{}",
        json!({"kind":"codec", "path":name, "iterations":iterations,
        "ipc_bytes":bytes, "ns_per_iteration":elapsed.as_nanos() / iterations as u128,
        "logical_mib_per_second": bytes as f64 * iterations as f64 / elapsed.as_secs_f64() / 1048576.0,
        "allocations_per_iteration":allocations as f64 / iterations as f64,
        "allocated_bytes_per_iteration":allocated_bytes as f64 / iterations as f64})
    );
    Ok(())
}

fn codec(size: usize, rows: usize, iterations: usize) -> Result<()> {
    ensure!(size > 0 && rows > 0 && iterations > 0);
    let batch = make_batch(rows, size)?;
    let buffer = Buffer::from_vec(encode(&batch)?);
    verify_shared_decode(&buffer, &batch)?;
    let reader_batch = FileReader::try_new(Cursor::new(buffer.as_slice()), None)?
        .next()
        .context("missing reader batch")??;
    ensure!(reader_batch == batch, "reader round trip mismatch");
    measured("encode_ipc", iterations, buffer.len(), || encode(&batch))?;
    measured("file_reader", iterations, buffer.len(), || {
        Ok(FileReader::try_new(Cursor::new(buffer.as_slice()), None)?
            .next()
            .context("missing batch")??)
    })?;
    measured("shared_buffer_decoder", iterations, buffer.len(), || {
        decode(&buffer, batch.schema())
    })?;
    // Numeric offsets may need alignment repair; LargeBinary body need not copy.
    let mut unaligned = Vec::with_capacity(buffer.len() + 1);
    unaligned.push(0);
    unaligned.extend_from_slice(&buffer);
    let unaligned = Buffer::from_vec(unaligned).slice(1);
    verify_shared_decode(&unaligned, &batch)?;
    measured(
        "unaligned_shared_buffer_decoder",
        iterations,
        buffer.len(),
        || decode(&unaligned, batch.schema()),
    )?;
    measured(
        "shared_decoder_then_row_vec",
        iterations,
        buffer.len(),
        || {
            let decoded = decode(&buffer, batch.schema())?;
            let bodies = decoded
                .column(3)
                .as_any()
                .downcast_ref::<LargeBinaryArray>()
                .unwrap();
            Ok((0..rows)
                .map(|row| bodies.value(row).to_vec())
                .collect::<Vec<_>>())
        },
    )?;
    Ok(())
}

/// Check equality and actual payload ownership outside the measured interval.
/// Numeric offset buffers can still allocate when fixing alignment.
fn verify_shared_decode(buffer: &Buffer, expected: &RecordBatch) -> Result<()> {
    let decoded = decode(buffer, expected.schema())?;
    ensure!(&decoded == expected, "full Arrow round trip mismatch");
    let bodies = decoded
        .column(3)
        .as_any()
        .downcast_ref::<LargeBinaryArray>()
        .context("missing binary payload column")?;
    let input_start = buffer.as_ptr() as usize;
    let output_start = bodies.value_data().as_ptr() as usize;
    let offset = output_start
        .checked_sub(input_start)
        .context("decoded payload does not share IPC storage")?;
    ensure!(
        offset
            .checked_add(bodies.value_data().len())
            .is_some_and(|end| end <= buffer.len()),
        "decoded payload does not share IPC storage"
    );
    Ok(())
}

struct EntryIndex;
impl MessageExt for EntryIndex {
    type Entry = Entry;
    fn index(entry: &Entry) -> u64 {
        entry.index
    }
}

#[allow(clippy::too_many_arguments)]
fn disk(
    directory: PathBuf,
    size: usize,
    batch: usize,
    writers: usize,
    groups: usize,
    rounds: usize,
    compression: u64,
) -> Result<()> {
    ensure!(size > 0 && batch > 0 && writers > 0 && groups > 0 && rounds > 0);
    ensure!(
        size.checked_mul(batch).context("batch overflow")? < 512 * 1024 * 1024,
        "cap per-writer batch at 512 MiB"
    );
    std::fs::create_dir(&directory).context("directory must be new")?;
    std::fs::File::open(directory.parent().context("missing parent")?)?.sync_all()?;
    let config = Config {
        dir: directory.to_string_lossy().into_owned(),
        batch_compression_threshold: ReadableSize(compression),
        ..Config::default()
    };
    let engine = Arc::new(Engine::open(config.clone())?);
    let gate = Arc::new(Barrier::new(writers + 1));
    let mut handles = Vec::new();
    for writer in 0..writers {
        let engine = engine.clone();
        let gate = gate.clone();
        handles.push(std::thread::spawn(move || -> Result<Histogram<u64>> {
            let bodies: Vec<_> = (0..batch)
                .map(|i| payload(size, (writer * batch + i) as u64))
                .collect();
            let mut latency = Histogram::<u64>::new(3)?;
            gate.wait();
            gate.wait();
            for round in 0..rounds {
                let before = Instant::now();
                let region = (writer * groups + round % groups + 1) as u64;
                let first = (round / groups * batch + 1) as u64;
                let entries: Vec<_> = bodies
                    .iter()
                    .enumerate()
                    .map(|(i, body)| Entry {
                        index: first + i as u64,
                        data: body.clone(),
                        ..Default::default()
                    })
                    .collect();
                let mut log = LogBatch::default();
                log.add_entries::<EntryIndex>(region, &entries)?;
                engine.write(&mut log, true)?; // ACK boundary: includes fdatasync.
                latency.record(before.elapsed().as_micros().max(1) as u64)?;
            }
            Ok(latency)
        }));
    }
    let start = Instant::now();
    gate.wait();
    // Timestamp after generation/barrier; includes all write and join completion.
    let writes_started = Instant::now();
    gate.wait();
    let mut latency = Histogram::<u64>::new(3)?;
    for handle in handles {
        latency.add(
            handle
                .join()
                .map_err(|_| anyhow::anyhow!("writer panicked"))??,
        )?;
    }
    let elapsed = writes_started.elapsed();
    let disk_bytes = engine.get_used_size();
    drop(engine);
    let recovery_started = Instant::now();
    let engine = Engine::open(config)?;
    let reopen = recovery_started.elapsed();
    let verify_started = Instant::now();
    for writer in 0..writers {
        let bodies: Vec<_> = (0..batch)
            .map(|i| payload(size, (writer * batch + i) as u64))
            .collect();
        for round in 0..rounds {
            let region = (writer * groups + round % groups + 1) as u64;
            let first = (round / groups * batch + 1) as u64;
            for (i, body) in bodies.iter().enumerate() {
                let entry = engine
                    .get_entry::<EntryIndex>(region, first + i as u64)?
                    .context("lost entry")?;
                ensure!(entry.data == *body, "recovered payload mismatch");
            }
        }
    }
    println!(
        "{}",
        json!({"kind":"disk", "engine":"raft-engine-0.4.2", "sync":true,
        "payload_bytes":size, "batch":batch, "writers":writers, "groups_per_writer":groups,
        "rounds":rounds, "compression_threshold":compression, "payload_mib": size as f64 * batch as f64 * rounds as f64 * writers as f64 / 1048576.0,
        "elapsed_seconds":elapsed.as_secs_f64(), "payload_mib_per_second":size as f64 * batch as f64 * rounds as f64 * writers as f64 / 1048576.0 / elapsed.as_secs_f64(),
        "batch_latency_us_p50":latency.value_at_quantile(0.5),"batch_latency_us_p99":latency.value_at_quantile(0.99),
        "disk_bytes":disk_bytes,"reopen_seconds":reopen.as_secs_f64(),"verify_seconds":verify_started.elapsed().as_secs_f64(),
        "verified_entries":writers * rounds * batch,"preparation_seconds": writes_started.duration_since(start).as_secs_f64(),
        "directory":directory})
    );
    Ok(())
}

fn main() -> Result<()> {
    match Args::parse().mode {
        Mode::SyncProbe { directory } => {
            std::fs::create_dir(&directory)?;
            let engine = Engine::open(Config {
                dir: directory.to_string_lossy().into_owned(),
                ..Config::default()
            })?;
            let mut batch = LogBatch::default();
            batch.put(1, b"probe".to_vec(), vec![3; 8192])?;
            engine.write(&mut batch, false)?;
            eprintln!("BEFORE_EMPTY_SYNC");
            engine.sync()?;
            eprintln!("AFTER_EMPTY_SYNC");
            let mut barrier = LogBatch::default();
            barrier.put(1, b"barrier".to_vec(), vec![1])?;
            eprintln!("BEFORE_NONEMPTY_SYNC");
            engine.write(&mut barrier, true)?;
            eprintln!("AFTER_NONEMPTY_SYNC");
            Ok(())
        }
        Mode::Codec {
            payload_bytes,
            rows,
            iterations,
        } => codec(payload_bytes, rows, iterations),
        Mode::Disk {
            directory,
            payload_bytes,
            batch,
            writers,
            groups_per_writer,
            rounds,
            compression_threshold,
        } => disk(
            directory,
            payload_bytes,
            batch,
            writers,
            groups_per_writer,
            rounds,
            compression_threshold,
        ),
    }
}
