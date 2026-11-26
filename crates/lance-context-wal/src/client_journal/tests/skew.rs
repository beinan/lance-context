//! Byte-heavy replacements must not be pinned by many tiny cold values.
use super::*;

#[tokio::test]
async fn skewed_replacements_stay_within_budget_without_reopening() -> Result<()> {
    let mut dir = if let Some(base) = std::env::var_os("CONTEXT_CLIENT_JOURNAL_TEST_ARTIFACTS") {
        std::fs::create_dir_all(&base)?;
        tempfile::Builder::new().prefix("skew-").tempdir_in(base)?
    } else {
        tempfile::tempdir()?
    };
    if std::env::var_os("CONTEXT_CLIENT_JOURNAL_TEST_ARTIFACTS").is_some() {
        dir.disable_cleanup(true);
    }
    let config = Settings {
        linger_ms: 0,
        ..Settings::default()
    };
    let (journal, _) = Journal::open(dir.path(), b"skewed-replacements", 128, config.clone())?;
    let mut latest = vec![None; 128];
    for key in 0..128u32 {
        let value = Bytes::from(vec![key as u8; 128]);
        journal.put(key, Bytes::new(), value.clone()).await?;
        latest[key as usize] = Some(value);
    }
    let mut failure = None;
    let mut peak = 0;
    'rounds: for round in 0..40u8 {
        for size in [4 << 20, 128] {
            for key in 0..8u32 {
                let mut value = vec![round; size];
                value[0] = key as u8;
                let value = Bytes::from(value);
                if let Err(error) = journal.put(key, Bytes::new(), value.clone()).await {
                    failure = Some(format!("round={round} key={key} size={size}: {error:#}"));
                    break 'rounds;
                }
                latest[key as usize] = Some(value);
                peak = peak.max(footprint(dir.path())?);
            }
        }
    }
    let before_reopen = footprint(dir.path())?;
    let stats = journal.statistics();
    drop(journal);
    let engine = Engine::open(config.engine(dir.path()))?;
    for (key, expected) in latest.iter().enumerate() {
        assert_eq!(
            engine
                .get(GROUP, &(key as u32).to_be_bytes())
                .map(Bytes::from),
            *expected
        );
    }
    engine.purge_expired_files()?;
    let after_reopen_purge = footprint(dir.path())?;
    println!(
        "{}",
        serde_json::json!({
            "kind":"live_skewed_replacements", "failure":failure, "peak":peak,
            "before_reopen":before_reopen, "after_reopen_purge":after_reopen_purge,
            "live_bytes":latest.iter().flatten().map(Bytes::len).sum::<usize>(),
            "statistics":stats, "path":dir.path(), "all_acknowledged_values_verified":true
        })
    );
    ensure!(failure.is_none(), "skewed live journal fenced: {failure:?}");
    ensure!(
        stats.pressure_rewrites > 0,
        "byte-pressure fallback was not exercised"
    );
    ensure!(
        peak <= config.log_bytes - config.reserve(),
        "admission bound exceeded"
    );
    Ok(())
}
