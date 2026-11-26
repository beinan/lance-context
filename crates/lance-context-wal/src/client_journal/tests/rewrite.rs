//! Kill a real journal process at the actual Rewrite queue's sync boundary.
use super::*;
use std::io::Write;
use std::process::{Child, Command, Stdio};

fn config() -> Settings {
    Settings {
        file_bytes: 64 << 10,
        ..settings()
    }
}
fn hot(round: u64) -> Bytes {
    let mut bytes = round.to_le_bytes().to_vec();
    bytes.extend(vec![(round % 251) as u8; 32768]);
    Bytes::from(bytes)
}
fn record_ack(root: &Path, round: u64) -> Result<()> {
    let temporary = root.join("ack.new");
    let mut file = std::fs::File::create(&temporary)?;
    file.write_all(&round.to_le_bytes())?;
    file.sync_all()?;
    std::fs::rename(temporary, root.join("ack"))?;
    std::fs::File::open(root)?.sync_all()?;
    Ok(())
}

#[test]
#[ignore = "subprocess fixture invoked by the real SIGKILL test"]
fn rewrite_child() -> Result<()> {
    let root = std::path::PathBuf::from(std::env::var("CONTEXT_CLIENT_JOURNAL_REWRITE_ROOT")?);
    let after = std::env::var("CONTEXT_CLIENT_JOURNAL_REWRITE_AFTER")? == "1";
    let runtime = tokio::runtime::Runtime::new()?;
    runtime.block_on(async {
        let filesystem = Arc::new(FaultFs::default());
        let control = filesystem.control.clone();
        let engine = Engine::open_with(config().engine(&root.join("journal")), filesystem, vec![])?;
        let (journal, _) = Journal::with_engine(engine, b"rewrite-process", 2, config())?;
        journal
            .put(
                0,
                Bytes::from_static(b"cold"),
                Bytes::from(vec![197; 32768]),
            )
            .await?;
        journal.put(1, Bytes::new(), hot(0)).await?;
        record_ack(&root, 0)?;
        *control.rewrite_pause.lock().unwrap() = Some((root.join("paused"), after));
        for round in 1..1024 {
            journal.put(1, Bytes::new(), hot(round)).await?;
            record_ack(&root, round)?;
        }
        anyhow::bail!("rewrite sync was not exercised")
    })
}
struct Stop(Child);
impl Drop for Stop {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

#[test]
fn sigkill_before_and_after_rewrite_sync_preserves_every_acknowledged_value() -> Result<()> {
    for after in [false, true] {
        let root = if let Some(base) = std::env::var_os("CONTEXT_CLIENT_JOURNAL_TEST_ARTIFACTS") {
            std::fs::create_dir_all(&base)?;
            let mut dir = tempfile::Builder::new()
                .prefix("journal-rewrite-")
                .tempdir_in(base)?;
            dir.disable_cleanup(true);
            dir
        } else {
            tempfile::tempdir()?
        };
        let mut child = Stop(
            Command::new(std::env::current_exe()?)
                .args([
                    "--ignored",
                    "--exact",
                    "client_journal::tests::rewrite::rewrite_child",
                    "--nocapture",
                ])
                .env("CONTEXT_CLIENT_JOURNAL_REWRITE_ROOT", root.path())
                .env(
                    "CONTEXT_CLIENT_JOURNAL_REWRITE_AFTER",
                    if after { "1" } else { "0" },
                )
                .stdin(Stdio::null())
                .stdout(std::fs::File::create(root.path().join("stdout"))?)
                .stderr(std::fs::File::create(root.path().join("stderr"))?)
                .spawn()?,
        );
        let until = Instant::now() + Duration::from_secs(20);
        while !root.path().join("paused").exists() {
            ensure!(
                child.0.try_wait()?.is_none(),
                "rewrite child exited before its sync boundary: {}",
                root.path().display()
            );
            ensure!(
                Instant::now() < until,
                "rewrite child did not reach sync boundary"
            );
            std::thread::sleep(Duration::from_millis(10));
        }
        child.0.kill()?;
        let status = child.0.wait()?;
        #[cfg(unix)]
        {
            use std::os::unix::process::ExitStatusExt;
            assert_eq!(status.signal(), Some(9));
        }
        let bytes = std::fs::read(root.path().join("ack"))?;
        let round = u64::from_le_bytes(bytes.try_into().unwrap());
        let (_journal, values) = Journal::open(
            &root.path().join("journal"),
            b"rewrite-process",
            2,
            config(),
        )?;
        let cold = values[0].as_ref().context("lost acknowledged cold value")?;
        assert_eq!(&cold[..4], b"cold");
        assert_eq!(&cold[4..], vec![197; 32768]);
        assert_eq!(values[1].as_ref(), Some(&hot(round)));
        std::fs::write(
            root.path().join("verified.json"),
            serde_json::to_vec(
                &serde_json::json!({"after_sync":after,"acknowledged_round":round,"signal":9,"full_values_verified":true}),
            )?,
        )?;
    }
    Ok(())
}
