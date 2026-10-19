//! Linux experiment supervisor. Each operation directory authorizes at most one
//! launch. Observation failures never authorize another launch or cleanup.
//! This is not a service supervisor: unknown outcomes require reconciliation.
use anyhow::{bail, ensure, Context, Result};
use clap::{Parser, Subcommand};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::{
    fs::{self, File, OpenOptions},
    io::{Read, Write},
    os::{
        fd::{AsRawFd, FromRawFd, OwnedFd},
        unix::process::ExitStatusExt,
    },
    path::{Path, PathBuf},
    process::{Command, Stdio},
    thread,
    time::Duration,
};

#[derive(Parser)]
struct Args {
    #[command(subcommand)]
    command: Action,
}

#[derive(Subcommand)]
enum Action {
    /// Use a new directory for each intentional launch. An existing directory
    /// is observed only; it is never reset, even if its owner has disappeared.
    Start {
        directory: PathBuf,
        #[arg(long)]
        executable: PathBuf,
        #[arg(long)]
        sha256: String,
        #[arg(last = true)]
        arguments: Vec<String>,
    },
    Observe {
        directory: PathBuf,
    },
    /// Explicit fault injection/cleanup of this recorded process incarnation.
    Stop {
        directory: PathBuf,
    },
    #[command(hide = true)]
    Supervise {
        directory: PathBuf,
    },
}

#[derive(Debug, PartialEq, Eq, Serialize, Deserialize)]
struct Request {
    executable: PathBuf,
    sha256: String,
    arguments: Vec<String>,
}

#[derive(Debug, Serialize, Deserialize)]
struct Identity {
    pid: u32,
    start_ticks: u64,
    boot: String,
}

#[derive(Debug, Serialize, Deserialize)]
struct Artifact {
    bytes: u64,
    sha256: String,
}

#[derive(Debug, Serialize, Deserialize)]
struct Completed {
    code: Option<i32>,
    signal: Option<i32>,
    spawn_error: Option<String>,
    stdout: Artifact,
    stderr: Artifact,
}

#[derive(Serialize)]
struct Observation {
    phase: &'static str,
    supervisor: Option<Identity>,
    child: Option<Identity>,
    completed: Option<Completed>,
}

fn read<T: for<'a> Deserialize<'a>>(path: &Path) -> Result<Option<T>> {
    match fs::read(path) {
        Ok(body) => Ok(Some(serde_json::from_slice(&body)?)),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(error) => Err(error.into()),
    }
}

fn persist<T: Serialize>(directory: &Path, name: &str, value: &T) -> Result<()> {
    let temp = directory.join(format!(".{name}.{}", std::process::id()));
    let mut file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&temp)?;
    serde_json::to_writer(&mut file, value)?;
    file.write_all(b"\n")?;
    file.sync_all()?;
    fs::rename(temp, directory.join(name))?;
    File::open(directory)?.sync_all()?;
    Ok(())
}

fn artifact(path: &Path) -> Result<Artifact> {
    let mut file = File::open(path)?;
    let mut digest = Sha256::new();
    let mut bytes = 0;
    let mut buffer = [0u8; 64 * 1024];
    loop {
        let count = file.read(&mut buffer)?;
        if count == 0 {
            break;
        }
        digest.update(&buffer[..count]);
        bytes += count as u64;
    }
    Ok(Artifact {
        bytes,
        sha256: format!("{:x}", digest.finalize()),
    })
}

fn process(pid: u32) -> Result<Option<(Identity, char)>> {
    let stat = match fs::read_to_string(format!("/proc/{pid}/stat")) {
        Ok(stat) => stat,
        Err(error)
            if error.kind() == std::io::ErrorKind::NotFound
                || error.raw_os_error() == Some(libc::ESRCH) =>
        {
            return Ok(None);
        }
        Err(error) => return Err(error.into()),
    };
    // comm can contain spaces and ')'; field 3 starts after the final ') '.
    let (_, rest) = stat.rsplit_once(") ").context("invalid process stat")?;
    let fields: Vec<_> = rest.split_whitespace().collect();
    let start_ticks = fields
        .get(19)
        .context("missing process start time")?
        .parse()?;
    let state = fields
        .first()
        .and_then(|v| v.chars().next())
        .context("missing process state")?;
    Ok(Some((
        Identity {
            pid,
            start_ticks,
            boot: fs::read_to_string("/proc/sys/kernel/random/boot_id")?
                .trim()
                .into(),
        },
        state,
    )))
}

fn live(identity: &Identity) -> Result<bool> {
    Ok(process(identity.pid)?.is_some_and(|(current, state)| {
        current.start_ticks == identity.start_ticks
            && current.boot == identity.boot
            && state != 'Z'
            && state != 'X'
    }))
}

fn observe(directory: &Path) -> Result<Observation> {
    ensure!(directory.is_dir(), "operation directory does not exist");
    let supervisor: Option<Identity> = read(&directory.join("supervisor.json"))?;
    let child: Option<Identity> = read(&directory.join("child.json"))?;
    let completed: Option<Completed> = read(&directory.join("completed.json"))?;
    let phase = if completed.is_some() {
        // The synced journal is authoritative after completion. Re-probing
        // /proc can race reaping (ESRCH), or inspect a reused PID needlessly.
        "completed"
    } else {
        let supervisor_live = supervisor.as_ref().map(live).transpose()?.unwrap_or(false);
        let child_live = child.as_ref().map(live).transpose()?.unwrap_or(false);
        if child_live && !supervisor_live {
            "orphaned_running"
        } else if supervisor_live {
            "running"
        } else {
            "unknown"
        }
    };
    Ok(Observation {
        phase,
        supervisor,
        child,
        completed,
    })
}

fn start(directory: &Path, request: Request) -> Result<()> {
    match fs::create_dir(directory) {
        Ok(()) => {
            File::open(directory.parent().context("operation needs a parent")?)?.sync_all()?;
            persist(directory, "request.json", &request)?;
            ensure!(
                artifact(&request.executable)?.sha256 == request.sha256,
                "executable checksum mismatch; operation will not be retried"
            );
            // Explicitly detached stdio: losing the launcher/exec connection
            // cannot close the workload's output streams.
            let _supervisor = Command::new(std::env::current_exe()?)
                .arg("supervise")
                .arg(directory)
                .stdin(Stdio::null())
                .stdout(Stdio::null())
                .stderr(
                    OpenOptions::new()
                        .write(true)
                        .create_new(true)
                        .open(directory.join("supervisor.stderr"))?,
                )
                .spawn()?;
        }
        Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
            if let Some(saved) = read::<Request>(&directory.join("request.json"))? {
                ensure!(
                    saved == request,
                    "operation ID already belongs to a different request"
                );
            }
            // Missing metadata can mean the first launcher died before writing.
            // That ambiguity is never permission to launch another command.
        }
        Err(error) => return Err(error.into()),
    }
    print_observation(directory)
}

fn supervise(directory: &Path) -> Result<()> {
    // SAFETY: process-local session/signal setup, before creating any threads.
    unsafe {
        ensure!(libc::setsid() != -1, "could not detach supervisor session");
        ensure!(
            libc::signal(libc::SIGHUP, libc::SIG_IGN) != libc::SIG_ERR,
            "could not ignore hangup"
        );
    }
    let lock = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .open(directory.join("lock"))?;
    lock.try_lock()
        .context("operation already has a supervisor")?;
    ensure!(
        !directory.join("supervisor.json").exists(),
        "operation was already started; refusing replay"
    );
    let identity = process(std::process::id())?
        .context("supervisor identity unavailable")?
        .0;
    persist(directory, "supervisor.json", &identity)?;
    let request: Request = read(&directory.join("request.json"))?.context("missing request")?;
    ensure!(
        artifact(&request.executable)?.sha256 == request.sha256,
        "executable changed before launch"
    );
    let stdout = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(directory.join("stdout"))?;
    let stderr = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(directory.join("stderr"))?;
    let result = if directory.join("stop.json").exists() {
        Err(std::io::Error::other(
            "operation stopped before child launch",
        ))
    } else {
        Command::new(&request.executable)
            .args(&request.arguments)
            .stdin(Stdio::null())
            .stdout(stdout.try_clone()?)
            .stderr(stderr.try_clone()?)
            .spawn()
    };
    let (code, signal, spawn_error) = match result {
        Err(error) => (None, None, Some(error.to_string())),
        Ok(mut child) => {
            persist(directory, "pid", &child.id())?;
            if let Some((identity, _)) = process(child.id())? {
                persist(directory, "child.json", &identity)?;
            }
            let status = loop {
                if let Some(status) = child.try_wait()? {
                    break status;
                }
                if directory.join("stop.json").exists() {
                    child.kill()?;
                    break child.wait()?;
                }
                thread::sleep(Duration::from_millis(25));
            };
            (status.code(), status.signal(), None)
        }
    };
    stdout.sync_all()?;
    stderr.sync_all()?;
    persist(
        directory,
        "completed.json",
        &Completed {
            code,
            signal,
            spawn_error,
            stdout: artifact(&directory.join("stdout"))?,
            stderr: artifact(&directory.join("stderr"))?,
        },
    )
}

fn stop(directory: &Path) -> Result<()> {
    // The marker fences a supervisor that has not spawned its child yet.
    persist(directory, "stop.json", &true)?;
    if let Some(identity) = read::<Identity>(&directory.join("child.json"))? {
        if live(&identity)? {
            // Bind the signal to a kernel process handle, then check the saved
            // incarnation again. PID reuse cannot signal an unrelated process.
            // SAFETY: pidfd_open has scalar arguments and returns an owned fd.
            let raw = unsafe { libc::syscall(libc::SYS_pidfd_open, identity.pid, 0) };
            if raw < 0 {
                let error = std::io::Error::last_os_error();
                if error.raw_os_error() != Some(libc::ESRCH) {
                    return Err(error.into());
                }
            } else {
                // SAFETY: successful pidfd_open returned a new owned descriptor.
                let fd = unsafe { OwnedFd::from_raw_fd(raw as i32) };
                if live(&identity)? {
                    // SAFETY: pidfd references only this process; no siginfo pointer.
                    let result = unsafe {
                        libc::syscall(
                            libc::SYS_pidfd_send_signal,
                            fd.as_raw_fd(),
                            libc::SIGKILL,
                            std::ptr::null::<libc::siginfo_t>(),
                            0,
                        )
                    };
                    if result < 0 {
                        let error = std::io::Error::last_os_error();
                        if error.raw_os_error() != Some(libc::ESRCH) {
                            return Err(error.into());
                        }
                    }
                }
            }
        }
    }
    print_observation(directory)
}

fn print_observation(directory: &Path) -> Result<()> {
    println!("{}", serde_json::to_string(&observe(directory)?)?);
    Ok(())
}

fn main() -> Result<()> {
    match Args::parse().command {
        Action::Start {
            directory,
            executable,
            sha256,
            arguments,
        } => {
            ensure!(
                sha256.len() == 64 && sha256.bytes().all(|v| v.is_ascii_hexdigit()),
                "invalid SHA-256"
            );
            let parent = directory
                .parent()
                .context("operation needs parent")?
                .canonicalize()?;
            let name = directory.file_name().context("operation needs name")?;
            start(
                &parent.join(name),
                Request {
                    executable: executable.canonicalize()?,
                    sha256: sha256.to_ascii_lowercase(),
                    arguments,
                },
            )
        }
        Action::Observe { directory } => print_observation(&directory),
        Action::Stop { directory } => stop(&directory),
        Action::Supervise { directory } => supervise(&directory),
    }
    .or_else(|error| bail!("{error:#}"))
}
