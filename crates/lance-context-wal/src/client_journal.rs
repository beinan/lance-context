//! Local, single-owner recoverable client state with a shared durable barrier.
//! This is not a replicated WAL or an ownership allocator. Payload parts stay
//! shared until the disk worker encodes one owned engine value. Engine encoding,
//! checksums and recovery still copy; this does not promise zero-copy storage.
use anyhow::{ensure, Context, Result};
use bytes::Bytes;
use raft_engine::{env::FileSystem, Engine, LogBatch, ReadableSize};
use serde::{Deserialize, Serialize};
use std::path::Path;
use std::sync::{
    atomic::{AtomicBool, AtomicU64, Ordering},
    Arc,
};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};
use tokio::sync::{mpsc, oneshot, OwnedSemaphorePermit, Semaphore};

const GROUP: u64 = 1;
const OVERHEAD: usize = 128;
const BINDING: &[u8] = b"client-journal-v1";

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Settings {
    pub linger_ms: u64,
    pub batch_bytes: u32,
    pub queue_bytes: u32,
    pub state_bytes: u32,
    pub file_bytes: u32,
    pub log_bytes: u64,
}
impl Default for Settings {
    fn default() -> Self {
        Self {
            linger_ms: 1,
            batch_bytes: 16 << 20,
            queue_bytes: 32 << 20,
            state_bytes: 128 << 20,
            file_bytes: 8 << 20,
            log_bytes: 512 << 20,
        }
    }
}
impl Settings {
    pub fn validate(&self) -> Result<()> {
        ensure!(self.linger_ms <= 25, "journal linger exceeds 25 ms");
        ensure!(
            self.batch_bytes > OVERHEAD as u32
                && self.batch_bytes <= self.queue_bytes
                && self.queue_bytes <= 256 << 20
                && self.state_bytes > 0
                && self.state_bytes <= 256 << 20
                && self.file_bytes >= 64 << 10
                && self.file_bytes <= 64 << 20,
            "invalid journal memory/segment bounds"
        );
        ensure!(
            self.log_bytes
                >= self.reserve() + self.state_bytes as u64 + 2 * self.batch_bytes as u64,
            "journal disk budget lacks rewrite and append headroom"
        );
        Ok(())
    }
    fn reserve(&self) -> u64 {
        // Rewrite live values plus two segment tails. This is conservative
        // admission headroom, not a filesystem quota or power-loss guarantee.
        2 * self.state_bytes as u64 + 4 * self.file_bytes as u64
    }
    fn engine(&self, path: &Path) -> raft_engine::Config {
        raft_engine::Config {
            dir: path.to_string_lossy().into_owned(),
            target_file_size: ReadableSize(self.file_bytes as u64),
            purge_threshold: ReadableSize(2 * self.file_bytes as u64),
            purge_rewrite_threshold: Some(ReadableSize(self.file_bytes as u64)),
            batch_compression_threshold: ReadableSize(0),
            enable_log_recycle: false,
            ..Default::default()
        }
    }
}

#[derive(Default)]
struct Counters {
    groups: AtomicU64,
    requests: AtomicU64,
    bytes: AtomicU64,
    max_group_requests: AtomicU64,
    write_ns: AtomicU64,
    maintenance_calls: AtomicU64,
    maintenance_ns: AtomicU64,
    queue_ns: AtomicU64,
    fenced: AtomicBool,
}
#[derive(Debug, Serialize)]
pub struct Statistics {
    pub groups: u64,
    pub requests: u64,
    pub bytes: u64,
    pub max_group_requests: u64,
    pub write_seconds: f64,
    pub maintenance_calls: u64,
    pub maintenance_seconds: f64,
    pub queue_seconds: f64,
    pub fenced: bool,
}
fn nanos(start: Instant) -> u64 {
    start.elapsed().as_nanos().min(u64::MAX as u128) as u64
}
struct Request {
    key: u32,
    parts: [Bytes; 2],
    _permit: OwnedSemaphorePermit,
    reply: oneshot::Sender<std::result::Result<(), String>>,
}
impl Request {
    fn bytes(&self) -> usize {
        self.parts.iter().map(Bytes::len).sum::<usize>() + OVERHEAD
    }
}
struct Inner {
    sender: Option<mpsc::Sender<Request>>,
    thread: Option<JoinHandle<()>>,
    queue: Arc<Semaphore>,
    counters: Arc<Counters>,
    batch_bytes: usize,
    keys: u32,
    // Keep a failed engine alive until every caller has observed the fence and
    // the owner drops its handles. Never destroy it during mutation unwinding.
    engine_guard: Option<Box<dyn Send + Sync>>,
}
impl Drop for Inner {
    fn drop(&mut self) {
        self.sender.take();
        // Join before releasing caller ownership. A cancelled put can already
        // be durable; its original intent remains the only safe retry identity.
        if let Some(thread) = self.thread.take() {
            let _ = thread.join();
        }
        self.engine_guard.take();
    }
}
#[derive(Clone)]
pub struct Journal(Arc<Inner>);

impl Journal {
    /// Open under the caller's single-owner lock. The engine also locks its own
    /// directory. Recovery returns latest values only; callers must validate
    /// their complete protocol state before issuing any network requests.
    pub fn open(
        path: &Path,
        scope: &[u8],
        keys: u32,
        settings: Settings,
    ) -> Result<(Self, Vec<Option<Bytes>>)> {
        settings.validate()?;
        // Require the parent to exist: syncing only the immediate parent cannot
        // make a recursively created ancestor chain durable. A bare relative
        // directory name has an empty parent, meaning the current directory.
        let parent = path.parent().context("journal parent missing")?;
        let parent = if parent.as_os_str().is_empty() {
            Path::new(".")
        } else {
            parent
        };
        let parent = std::fs::File::open(parent).context("journal parent must already exist")?;
        let engine = Engine::open(settings.engine(path))?;
        // Persist creation of the engine directory in its existing parent too.
        std::fs::File::open(path)?.sync_all()?;
        parent.sync_all()?;
        Self::with_engine(engine, scope, keys, settings)
    }

    fn with_engine<F: FileSystem + 'static>(
        engine: Engine<F>,
        scope: &[u8],
        keys: u32,
        settings: Settings,
    ) -> Result<(Self, Vec<Option<Bytes>>)> {
        settings.validate()?;
        ensure!(
            (1..=16384).contains(&keys) && scope.len() <= 8192,
            "invalid journal binding"
        );
        let binding = serde_json::to_vec(&(1u32, scope, keys, &settings))?;
        if let Some(saved) = engine.get(0, BINDING) {
            ensure!(saved == binding, "client journal scope/settings mismatch");
        } else {
            ensure!(engine.is_empty(), "unbound nonempty client journal");
            let mut batch = LogBatch::default();
            batch.put(0, BINDING.to_vec(), binding)?;
            engine.write(&mut batch, true)?;
        }
        let recovered: Vec<_> = (0..keys)
            .map(|key| engine.get(GROUP, &key.to_be_bytes()).map(Bytes::from))
            .collect();
        let lengths: Vec<_> = recovered
            .iter()
            .map(|v| v.as_ref().map_or(0, Bytes::len))
            .collect();
        ensure!(
            lengths.iter().sum::<usize>() <= settings.state_bytes as usize,
            "recovered client state exceeds bound"
        );
        let (sender, receiver) = mpsc::channel(1024);
        let counters = Arc::new(Counters::default());
        let queue = Arc::new(Semaphore::new(settings.queue_bytes as usize));
        let worker_counters = counters.clone();
        let worker_queue = queue.clone();
        let batch_bytes = settings.batch_bytes as usize;
        let engine = Arc::new(engine);
        let engine_guard = engine.clone();
        let thread = std::thread::Builder::new()
            .name("client-journal".into())
            .spawn(move || {
                // Includes unwinding panics from filesystem hooks. No pending sender
                // can mistake an abandoned reply for durable completion.
                let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                    worker(&engine, receiver, lengths, settings, &worker_counters)
                }));
                if !matches!(result, Ok(Ok(()))) {
                    worker_counters.fenced.store(true, Ordering::Release);
                }
                worker_queue.close();
            })?;
        Ok((
            Self(Arc::new(Inner {
                sender: Some(sender),
                thread: Some(thread),
                queue,
                counters,
                batch_bytes,
                keys,
                engine_guard: Some(Box::new(engine_guard)),
            })),
            recovered,
        ))
    }

    pub async fn put(&self, key: u32, prefix: Bytes, body: Bytes) -> Result<()> {
        let size = prefix
            .len()
            .checked_add(body.len())
            .and_then(|n| n.checked_add(OVERHEAD))
            .context("journal value overflow")?;
        ensure!(
            key < self.0.keys && size <= self.0.batch_bytes,
            "journal key/value exceeds bounds"
        );
        ensure!(
            !self.0.counters.fenced.load(Ordering::Acquire),
            "client journal fenced"
        );
        let start = Instant::now();
        let permit = self
            .0
            .queue
            .clone()
            .acquire_many_owned(size as u32)
            .await
            .context("client journal unavailable")?;
        let (reply, receive) = oneshot::channel();
        self.0
            .sender
            .as_ref()
            .unwrap()
            .send(Request {
                key,
                parts: [prefix, body],
                _permit: permit,
                reply,
            })
            .await
            .context("client journal worker stopped")?;
        self.0
            .counters
            .queue_ns
            .fetch_add(nanos(start), Ordering::Relaxed);
        receive
            .await
            .context("client journal outcome unknown; reopen before recovery")?
            .map_err(anyhow::Error::msg)
    }

    pub fn statistics(&self) -> Statistics {
        let c = &self.0.counters;
        let get = |a: &AtomicU64| a.load(Ordering::Relaxed);
        Statistics {
            groups: get(&c.groups),
            requests: get(&c.requests),
            bytes: get(&c.bytes),
            max_group_requests: get(&c.max_group_requests),
            write_seconds: get(&c.write_ns) as f64 / 1e9,
            maintenance_calls: get(&c.maintenance_calls),
            maintenance_seconds: get(&c.maintenance_ns) as f64 / 1e9,
            queue_seconds: get(&c.queue_ns) as f64 / 1e9,
            fenced: c.fenced.load(Ordering::Acquire),
        }
    }
}

fn footprint(path: &Path) -> Result<u64> {
    let mut bytes = 0u64;
    for file in std::fs::read_dir(path)? {
        let metadata = file?.metadata()?;
        ensure!(metadata.is_file(), "unexpected journal directory entry");
        #[cfg(unix)]
        let allocated = {
            use std::os::unix::fs::MetadataExt;
            metadata.blocks().saturating_mul(512)
        };
        #[cfg(not(unix))]
        let allocated = metadata.len();
        bytes = bytes
            .checked_add(metadata.len().max(allocated))
            .context("disk accounting overflow")?;
    }
    Ok(bytes)
}

fn worker<F: FileSystem>(
    engine: &Engine<F>,
    mut receive: mpsc::Receiver<Request>,
    mut lengths: Vec<usize>,
    settings: Settings,
    counters: &Counters,
) -> Result<()> {
    let mut deferred = None;
    while let Some(first) = deferred.take().or_else(|| receive.blocking_recv()) {
        let mut bytes = first.bytes();
        let mut requests = vec![first];
        // Bound intentional batching delay; queue pressure can add further wait.
        if settings.linger_ms > 0 {
            std::thread::sleep(Duration::from_millis(settings.linger_ms));
        }
        while requests.len() < 1024 {
            let Ok(next) = receive.try_recv() else {
                break;
            };
            if bytes + next.bytes() > settings.batch_bytes as usize {
                deferred = Some(next);
                break;
            }
            bytes += next.bytes();
            requests.push(next);
        }
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| -> Result<()> {
            let physical = footprint(Path::new(engine.path()))?;
            if physical >= 2 * settings.file_bytes as u64 {
                let start = Instant::now();
                engine.purge_expired_files()?;
                counters
                    .maintenance_ns
                    .fetch_add(nanos(start), Ordering::Relaxed);
                counters.maintenance_calls.fetch_add(1, Ordering::Relaxed);
            }
            ensure!(
                footprint(Path::new(engine.path()))? + bytes as u64
                    <= settings.log_bytes - settings.reserve(),
                "client journal disk admission closed"
            );
            let mut total: usize = lengths.iter().sum();
            let mut batch = LogBatch::default();
            for request in &requests {
                let length = request.bytes() - OVERHEAD;
                let old = &mut lengths[request.key as usize];
                total = total - *old + length;
                ensure!(
                    total <= settings.state_bytes as usize,
                    "client journal state budget exceeded"
                );
                *old = length;
                let mut value = Vec::with_capacity(length);
                for part in &request.parts {
                    value.extend_from_slice(part);
                }
                batch.put(GROUP, request.key.to_be_bytes().to_vec(), value)?;
            }
            let start = Instant::now();
            // Never use Engine::sync(): pinned 0.4.2's empty sync is a no-op.
            engine.write(&mut batch, true)?;
            counters.write_ns.fetch_add(nanos(start), Ordering::Relaxed);
            Ok(())
        }))
        .unwrap_or_else(|_| {
            Err(anyhow::anyhow!(
                "client journal mutation panicked; reopen required"
            ))
        });
        if result.is_ok() {
            counters.groups.fetch_add(1, Ordering::Relaxed);
            counters
                .requests
                .fetch_add(requests.len() as u64, Ordering::Relaxed);
            counters.bytes.fetch_add(
                (bytes - OVERHEAD * requests.len()) as u64,
                Ordering::Relaxed,
            );
            counters
                .max_group_requests
                .fetch_max(requests.len() as u64, Ordering::Relaxed);
        } else {
            counters.fenced.store(true, Ordering::Release);
        }
        for request in requests {
            let _ = request.reply.send(
                result
                    .as_ref()
                    .map(|_| ())
                    .map_err(|e| format!("client journal fenced: {e:#}")),
            );
        }
        result?;
    }
    Ok(())
}

#[cfg(test)]
mod tests;
