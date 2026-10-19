#![cfg(feature = "runner")]

use serde_json::Value;
use sha2::{Digest, Sha256};
use std::{
    fs::{self, OpenOptions},
    io::Write,
    path::{Path, PathBuf},
    process::{Command, Output},
    thread,
    time::{Duration, Instant},
};

const RUNNER: &str = env!("CARGO_BIN_EXE_context-wal-runner");

struct Run {
    root: tempfile::TempDir,
    operation: PathBuf,
    executable: PathBuf,
    sha: String,
}

impl Run {
    fn new() -> Self {
        let root = tempfile::tempdir().unwrap();
        let executable = std::env::current_exe().unwrap();
        let sha = format!("{:x}", Sha256::digest(fs::read(&executable).unwrap()));
        Self {
            operation: root.path().join("operation"),
            root,
            executable,
            sha,
        }
    }

    fn launch(&self, mode: &str) -> Command {
        let mut command = Command::new(RUNNER);
        command
            .arg("start")
            .arg(&self.operation)
            .arg("--executable")
            .arg(&self.executable)
            .arg("--sha256")
            .arg(&self.sha)
            .arg("--")
            .args(["--exact", "runner_fixture", "--ignored", "--nocapture"])
            .env("CONTEXT_RUNNER_FIXTURE", self.root.path())
            .env("CONTEXT_RUNNER_FIXTURE_MODE", mode);
        command
    }

    fn observe(&self) -> Value {
        json(
            Command::new(RUNNER)
                .arg("observe")
                .arg(&self.operation)
                .output()
                .unwrap(),
        )
    }

    fn wait(&self, predicate: impl Fn(&Value) -> bool) -> Value {
        let deadline = Instant::now() + Duration::from_secs(15);
        loop {
            let status = self.observe();
            if predicate(&status) {
                return status;
            }
            assert!(
                Instant::now() < deadline,
                "operation did not reach expected state: {status}"
            );
            thread::sleep(Duration::from_millis(10));
        }
    }
}

impl Drop for Run {
    fn drop(&mut self) {
        let _ = Command::new(RUNNER)
            .arg("stop")
            .arg(&self.operation)
            .output();
        // Give the detached supervisor time to reap its child before TempDir.
        for _ in 0..100 {
            if !self.operation.exists() {
                break;
            }
            let status = self.observe();
            if status["phase"] != "running" && status["phase"] != "orphaned_running" {
                break;
            }
            thread::sleep(Duration::from_millis(10));
        }
    }
}

fn json(output: Output) -> Value {
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    serde_json::from_slice(&output.stdout).unwrap()
}

fn assert_artifact(path: &Path, saved: &Value) {
    let bytes = fs::read(path).unwrap();
    assert_eq!(saved["bytes"].as_u64(), Some(bytes.len() as u64));
    assert_eq!(saved["sha256"], format!("{:x}", Sha256::digest(&bytes)));
}

#[test]
fn concurrent_launch_and_lost_observation_never_repeat_the_rust_command() {
    let run = Run::new();
    thread::scope(|scope| {
        let workers: Vec<_> = (0..8)
            .map(|_| scope.spawn(|| json(run.launch("finish").output().unwrap())))
            .collect();
        for worker in workers {
            worker.join().unwrap();
        }
    });
    let completed = run.wait(|value| value["phase"] == "completed");
    assert_eq!(completed["completed"]["code"], 0);
    assert_artifact(
        &run.operation.join("stdout"),
        &completed["completed"]["stdout"],
    );
    assert_artifact(
        &run.operation.join("stderr"),
        &completed["completed"]["stderr"],
    );
    for _ in 0..3 {
        assert_eq!(json(run.launch("finish").output().unwrap()), completed);
    }
    assert_eq!(fs::read(run.root.path().join("launches")).unwrap(), b"x");
    let changed = run.launch("finish").arg("--show-output").output().unwrap();
    assert!(!changed.status.success());
    assert!(String::from_utf8_lossy(&changed.stderr).contains("different request"));
    let replay = Command::new(RUNNER)
        .arg("supervise")
        .arg(&run.operation)
        .output()
        .unwrap();
    assert!(!replay.status.success());
    assert_eq!(run.observe(), completed);
}

#[test]
fn supervisor_loss_preserves_unknown_outcome_and_stop_targets_the_orphan() {
    let run = Run::new();
    json(run.launch("wait").output().unwrap());
    let active =
        run.wait(|value| value["child"].is_object() && run.root.path().join("launches").exists());
    let supervisor = active["supervisor"]["pid"].as_i64().unwrap();
    // SAFETY: the target is this test's live supervisor, not a borrowed PID.
    assert_eq!(unsafe { libc::kill(supervisor as i32, libc::SIGKILL) }, 0);
    run.wait(|value| value["phase"] == "orphaned_running");
    assert_eq!(
        json(run.launch("wait").output().unwrap())["phase"],
        "orphaned_running"
    );
    assert_eq!(fs::read(run.root.path().join("launches")).unwrap(), b"x");

    // A mismatched process incarnation must not be signaled. Keep the original
    // child live, then restore its record and stop that exact orphan via pidfd.
    let original = fs::read(run.operation.join("child.json")).unwrap();
    let mut foreign: Value = serde_json::from_slice(&original).unwrap();
    foreign["start_ticks"] = Value::from(foreign["start_ticks"].as_u64().unwrap() + 1);
    fs::write(
        run.operation.join("child.json"),
        serde_json::to_vec(&foreign).unwrap(),
    )
    .unwrap();
    json(
        Command::new(RUNNER)
            .arg("stop")
            .arg(&run.operation)
            .output()
            .unwrap(),
    );
    fs::write(run.operation.join("child.json"), &original).unwrap();
    assert_eq!(run.observe()["phase"], "orphaned_running");
    json(
        Command::new(RUNNER)
            .arg("stop")
            .arg(&run.operation)
            .output()
            .unwrap(),
    );
    let unknown = run.wait(|value| value["phase"] == "unknown");
    assert!(unknown["completed"].is_null());
    assert_eq!(
        json(run.launch("wait").output().unwrap())["phase"],
        "unknown"
    );
    assert_eq!(fs::read(run.root.path().join("launches")).unwrap(), b"x");
}

#[test]
fn claimed_directory_and_checksum_failure_are_never_relaunched() {
    let run = Run::new();
    fs::create_dir(&run.operation).unwrap();
    assert_eq!(
        json(run.launch("finish").output().unwrap())["phase"],
        "unknown"
    );
    assert!(!run.root.path().join("launches").exists());
    let mut corrupt = Run::new();
    corrupt.sha = "0".repeat(64);
    let result = corrupt.launch("finish").output().unwrap();
    assert!(!result.status.success());
    assert!(String::from_utf8_lossy(&result.stderr).contains("checksum mismatch"));
    assert_eq!(
        json(corrupt.launch("finish").output().unwrap())["phase"],
        "unknown"
    );
    assert!(!corrupt.root.path().join("launches").exists());
}

#[test]
fn hangup_does_not_stop_the_workload_and_explicit_stop_retains_its_exit() {
    let run = Run::new();
    json(run.launch("wait").output().unwrap());
    let active =
        run.wait(|value| value["child"].is_object() && run.root.path().join("launches").exists());
    let supervisor = active["supervisor"]["pid"].as_i64().unwrap();
    // SAFETY: signal only this test's live supervisor.
    assert_eq!(unsafe { libc::kill(supervisor as i32, libc::SIGHUP) }, 0);
    thread::sleep(Duration::from_millis(50));
    assert_eq!(run.observe()["phase"], "running");
    json(
        Command::new(RUNNER)
            .arg("stop")
            .arg(&run.operation)
            .output()
            .unwrap(),
    );
    let completed = run.wait(|value| value["phase"] == "completed");
    assert_eq!(completed["completed"]["signal"], libc::SIGKILL);
    assert!(completed["completed"]["code"].is_null());
    assert_eq!(json(run.launch("wait").output().unwrap()), completed);
    assert_eq!(fs::read(run.root.path().join("launches")).unwrap(), b"x");
}

#[test]
#[ignore = "separate Rust child process launched only by runner integration tests"]
fn runner_fixture() {
    let root = PathBuf::from(std::env::var_os("CONTEXT_RUNNER_FIXTURE").unwrap());
    let mut marker = OpenOptions::new()
        .create(true)
        .append(true)
        .open(root.join("launches"))
        .unwrap();
    marker.write_all(b"x").unwrap();
    marker.sync_all().unwrap();
    println!("complete stdout payload");
    eprintln!("complete stderr payload");
    if std::env::var("CONTEXT_RUNNER_FIXTURE_MODE").unwrap() == "wait" {
        loop {
            thread::sleep(Duration::from_secs(1));
        }
    }
}
