//! Kill the actual process with all three WAL replicas at each compaction boundary.
use super::*;
use std::os::unix::process::ExitStatusExt;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "subprocess role, launched by annotation_compaction_*_sigkill tests"]
async fn annotation_compaction_process_role() -> Result<()> {
    let root = std::path::PathBuf::from(std::env::var("ANNOTATION_COMPACTION_PROCESS_ROOT")?);
    let stage = std::env::var("ANNOTATION_COMPACTION_PROCESS_STAGE")?.parse()?;
    recover_at(stage, &root, true).await?;
    bail!("compaction child exited without reaching its SIGKILL boundary")
}

struct Child(std::process::Child);
impl Drop for Child {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

async fn killed_recovery(stage: u8) -> Result<()> {
    tokio::time::timeout(Duration::from_secs(90), killed_recovery_inner(stage)).await?
}

async fn killed_recovery_inner(stage: u8) -> Result<()> {
    let mut builder = tempfile::Builder::new();
    builder.prefix("annotation-compaction-process-");
    let root = if let Ok(parent) = std::env::var("INGEST_TEST_ARTIFACTS") {
        builder.disable_cleanup(true).tempdir_in(parent)?
    } else {
        builder.tempdir()?
    };
    eprintln!(
        "compaction SIGKILL fixture: {} stage={stage}",
        root.path().display()
    );
    let log = std::fs::File::create(root.path().join("child.log"))?;
    let mut child = Child(std::process::Command::new(std::env::current_exe()?)
        .args(["--exact", "tests::annotation_publication::compaction::process::annotation_compaction_process_role", "--ignored", "--nocapture"])
        .env("ANNOTATION_COMPACTION_PROCESS_ROOT", root.path())
        .env("ANNOTATION_COMPACTION_PROCESS_STAGE", stage.to_string())
        .stdout(log.try_clone()?).stderr(log).spawn()?);
    while !root.path().join("ready").exists() {
        if let Some(status) = child.0.try_wait()? {
            bail!(
                "compaction child exited {status}: {}",
                std::fs::read_to_string(root.path().join("child.log"))?
            );
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    child.0.kill()?;
    assert_eq!(child.0.wait()?.signal(), Some(9));
    let expected: Expected =
        serde_json::from_slice(&std::fs::read(root.path().join("expected.json"))?)?;
    verify_recovery(root.path(), expected).await
}

#[tokio::test]
async fn annotation_compaction_unprepared_plan_survives_sigkill() -> Result<()> {
    killed_recovery(0).await
}
#[tokio::test]
async fn annotation_compaction_prepared_files_survive_sigkill() -> Result<()> {
    killed_recovery(1).await
}
#[tokio::test]
async fn annotation_compaction_unknown_selection_survives_sigkill() -> Result<()> {
    killed_recovery(2).await
}
#[tokio::test]
async fn annotation_compaction_confirmed_cut_survives_sigkill() -> Result<()> {
    killed_recovery(3).await
}
