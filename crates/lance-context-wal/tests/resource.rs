#![cfg(all(target_os = "linux", feature = "runner"))]
use anyhow::{ensure, Result};
use std::{
    fs,
    io::{BufRead, BufReader},
    process::{Child, Command, Stdio},
};
struct OwnedChild(Child);
impl Drop for OwnedChild {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}
#[test]
fn samples_the_original_process_and_finishes_when_it_exits() -> Result<()> {
    let directory = tempfile::tempdir()?;
    let mut target = OwnedChild(Command::new("sleep").arg("10").spawn()?);
    let stat = fs::read_to_string(format!("/proc/{}/stat", target.0.id()))?;
    let fields: Vec<_> = stat
        .rsplit_once(") ")
        .unwrap()
        .1
        .split_whitespace()
        .collect();
    fs::write(
        directory.path().join("child.json"),
        serde_json::to_vec(&serde_json::json!({
            "pid":target.0.id(),"start_ticks":fields[19].parse::<u64>()?,
            "boot":fs::read_to_string("/proc/sys/kernel/random/boot_id")?.trim(),
        }))?,
    )?;
    let mut sampler = OwnedChild(
        Command::new(env!("CARGO_BIN_EXE_context-wal-resource"))
            .arg("--operation")
            .arg(directory.path())
            .args(["--interval-ms", "100"])
            .stdout(Stdio::piped())
            .spawn()?,
    );
    let mut out = BufReader::new(sampler.0.stdout.take().unwrap());
    for _ in 0..2 {
        let mut line = String::new();
        ensure!(out.read_line(&mut line)? > 0, "missing sample");
        let sample: serde_json::Value = serde_json::from_str(&line)?;
        assert_eq!(sample["kind"], "resource_sample");
        assert_eq!(sample["pid"], target.0.id());
        assert!(sample["cgroup_memory_bytes"].as_u64().unwrap() > 0);
    }
    target.0.kill()?;
    target.0.wait()?;
    let lines: Vec<_> = out.lines().collect::<std::io::Result<_>>()?;
    let last: serde_json::Value = serde_json::from_str(lines.last().unwrap())?;
    assert_eq!(last["kind"], "resource_sampling_complete");
    assert!(last["samples"].as_u64().unwrap() >= 2);
    ensure!(sampler.0.wait()?.success(), "sampler failed");
    Ok(())
}
