//! Quiescent directory observation for the disk experiment, not a write counter.
use std::path::Path;

use anyhow::{Context, Result};

pub(super) struct DiskUsage {
    pub apparent_bytes: u64,
    /// Filesystem block allocation is available on Unix only.
    pub allocated_bytes: Option<u64>,
    /// Sum each file's larger apparent/allocated size, matching WAL accounting.
    pub accounted_bytes: u64,
}

pub(super) fn measure(directory: &Path) -> Result<DiskUsage> {
    let mut usage = DiskUsage {
        apparent_bytes: 0,
        allocated_bytes: cfg!(unix).then_some(0),
        accounted_bytes: 0,
    };
    for entry in std::fs::read_dir(directory)? {
        let metadata = entry?.metadata()?;
        if !metadata.is_file() {
            continue;
        }
        let apparent = metadata.len();
        let accounted = apparent;
        #[cfg(unix)]
        let accounted = {
            use std::os::unix::fs::MetadataExt;
            let allocated = metadata
                .blocks()
                .checked_mul(512)
                .context("allocated file size overflow")?;
            usage.allocated_bytes = Some(
                usage
                    .allocated_bytes
                    .unwrap()
                    .checked_add(allocated)
                    .context("allocated directory size overflow")?,
            );
            accounted.max(allocated)
        };
        usage.apparent_bytes = usage
            .apparent_bytes
            .checked_add(apparent)
            .context("apparent directory size overflow")?;
        usage.accounted_bytes = usage
            .accounted_bytes
            .checked_add(accounted)
            .context("accounted directory size overflow")?;
    }
    Ok(usage)
}

#[cfg(test)]
mod tests {
    use raft_engine::{Config, Engine, LogBatch, ReadableSize};

    #[test]
    fn oversized_segments_are_measured_after_synced_writes() -> anyhow::Result<()> {
        let directory = tempfile::tempdir()?;
        let engine = Engine::open(Config {
            dir: directory.path().to_string_lossy().into_owned(),
            target_file_size: ReadableSize(64 * 1024),
            batch_compression_threshold: ReadableSize(0),
            ..Default::default()
        })?;
        let payload = vec![7; 120 * 1024];
        for key in 0u64..8 {
            let mut batch = LogBatch::default();
            batch.put(1, key.to_le_bytes().to_vec(), payload.clone())?;
            engine.write(&mut batch, true)?;
        }
        let engine_estimate = engine.get_used_size() as u64;
        drop(engine);

        let usage = super::measure(directory.path())?;
        assert!(usage.apparent_bytes >= 8 * payload.len() as u64);
        assert!(usage.accounted_bytes >= usage.apparent_bytes);
        assert!(
            engine_estimate < usage.apparent_bytes,
            "the pinned engine estimates overshot segments at their target size"
        );
        assert_eq!(usage.allocated_bytes.is_some(), cfg!(unix));
        if let Some(allocated) = usage.allocated_bytes {
            assert!(usage.accounted_bytes >= allocated);
        }
        Ok(())
    }
}
