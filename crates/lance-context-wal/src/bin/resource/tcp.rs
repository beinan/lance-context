//! Procfs socket snapshots are sampled, not atomic peaks. Namespace TCP tables
//! include other processes; intersect with the target's open socket inodes.
use super::*;

#[derive(Default, Debug, Serialize)]
pub(super) struct Observation {
    port: u16,
    socket_descriptors: u64,
    owned_tcp_sockets: u64,
    /// Linux TCP states as two-digit hexadecimal values (01 = established).
    states: BTreeMap<String, u64>,
    local_port_states: BTreeMap<String, u64>,
    remote_port_states: BTreeMap<String, u64>,
}

fn port(endpoint: &str) -> Result<u16> {
    let (address, port) = endpoint.rsplit_once(':').context("missing TCP port")?;
    ensure!(
        (address.len() == 8 || address.len() == 32)
            && address.bytes().all(|c| c.is_ascii_hexdigit())
            && port.len() == 4,
        "invalid TCP endpoint"
    );
    Ok(u16::from_str_radix(port, 16)?)
}

fn table(text: &str, owned: &BTreeSet<u64>, result: &mut Observation) -> Result<()> {
    for line in text.lines().skip(1) {
        let fields: Vec<_> = line.split_whitespace().collect();
        ensure!(fields.len() >= 10, "truncated TCP observation");
        let inode: u64 = fields[9].parse()?;
        if !owned.contains(&inode) {
            continue;
        }
        ensure!(fields[3].len() == 2, "invalid TCP state");
        let state = format!("{:02X}", u8::from_str_radix(fields[3], 16)?);
        let local = port(fields[1])?;
        let remote = port(fields[2])?;
        result.owned_tcp_sockets += 1;
        *result.states.entry(state.clone()).or_default() += 1;
        if local == result.port {
            *result.local_port_states.entry(state.clone()).or_default() += 1;
        }
        if remote == result.port {
            *result.remote_port_states.entry(state).or_default() += 1;
        }
    }
    Ok(())
}

pub(super) fn observe(pid: u32, port: u16) -> Result<Observation> {
    let mut owned = BTreeSet::new();
    let mut result = Observation {
        port,
        ..Observation::default()
    };
    for entry in fs::read_dir(format!("/proc/{pid}/fd"))? {
        let path = entry?.path();
        let link = match fs::read_link(path) {
            Ok(link) => link,
            // A connection can close between directory enumeration and readlink.
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => continue,
            Err(error) => return Err(error.into()),
        };
        if let Some(inode) = link
            .to_string_lossy()
            .strip_prefix("socket:[")
            .and_then(|s| s.strip_suffix(']'))
        {
            result.socket_descriptors += 1;
            owned.insert(inode.parse()?);
        }
    }
    for family in ["tcp", "tcp6"] {
        table(
            &fs::read_to_string(format!("/proc/{pid}/net/{family}"))?,
            &owned,
            &mut result,
        )?;
    }
    Ok(result)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{
        io::Read,
        net::{Shutdown, TcpListener, TcpStream},
        process::{Command, Stdio},
    };

    #[test]
    #[ignore = "subprocess socket ownership fixture"]
    fn socket_fixture() -> Result<()> {
        let directory = PathBuf::from(std::env::var("CONTEXT_SOCKET_FIXTURE")?);
        let address = fs::read_to_string(directory.join("address"))?;
        let connections = (0..3)
            .map(|_| TcpStream::connect(address.trim()))
            .collect::<std::io::Result<Vec<_>>>()?;
        let _duplicate = connections[0].try_clone()?;
        let listener = TcpListener::bind("127.0.0.1:0")?;
        fs::write(
            directory.join("ready"),
            listener.local_addr()?.port().to_string(),
        )?;
        std::io::stdin().read_exact(&mut [0u8])?;
        Ok(())
    }

    #[test]
    fn live_socket_counts_exclude_namespace_neighbors_and_deduplicate_fds() -> Result<()> {
        let directory = tempfile::tempdir()?;
        let listener = TcpListener::bind("127.0.0.1:0")?;
        let port = listener.local_addr()?.port();
        fs::write(
            directory.path().join("address"),
            listener.local_addr()?.to_string(),
        )?;
        // These sockets use the same port and namespace but belong to the parent.
        let mut unrelated = Vec::new();
        for _ in 0..5 {
            unrelated.push(TcpStream::connect(listener.local_addr()?)?);
            unrelated.push(listener.accept()?.0);
        }
        let mut child = Command::new(std::env::current_exe()?)
            .args(["--exact", "tcp::tests::socket_fixture", "--ignored"])
            .env("CONTEXT_SOCKET_FIXTURE", directory.path())
            .stdin(Stdio::piped())
            .stdout(Stdio::null())
            .spawn()?;
        let result = (|| -> Result<()> {
            let deadline = Instant::now() + Duration::from_secs(5);
            while !directory.path().join("ready").exists() {
                ensure!(child.try_wait()?.is_none(), "socket fixture exited");
                ensure!(
                    Instant::now() < deadline,
                    "socket fixture did not become ready"
                );
                std::thread::sleep(Duration::from_millis(2));
            }
            let peers = (0..3)
                .map(|_| listener.accept().map(|v| v.0))
                .collect::<std::io::Result<Vec<_>>>()?;
            let sampled = observe(child.id(), port)?;
            assert_eq!(sampled.socket_descriptors, 5); // three clients, one dup, one listener
            assert_eq!(sampled.owned_tcp_sockets, 4);
            assert_eq!(
                sampled.states,
                BTreeMap::from([("01".into(), 3), ("0A".into(), 1)])
            );
            assert_eq!(
                sampled.remote_port_states,
                BTreeMap::from([("01".into(), 3)])
            );
            assert!(sampled.local_port_states.is_empty());
            let child_port = fs::read_to_string(directory.path().join("ready"))?.parse()?;
            let own_listener = observe(child.id(), child_port)?;
            assert_eq!(
                own_listener.local_port_states,
                BTreeMap::from([("0A".into(), 1)])
            );
            // A peer's FIN leaves the target's descriptor open. Counting only
            // ESTABLISHED would miss this socket while it still occupies a slot.
            peers[0].shutdown(Shutdown::Write)?;
            let deadline = Instant::now() + Duration::from_secs(5);
            loop {
                let closing = observe(child.id(), port)?;
                if closing.remote_port_states.get("08") == Some(&1) {
                    assert_eq!(closing.owned_tcp_sockets, 4);
                    assert_eq!(
                        closing.remote_port_states,
                        BTreeMap::from([("01".into(), 2), ("08".into(), 1)])
                    );
                    break;
                }
                ensure!(Instant::now() < deadline, "target never entered CLOSE_WAIT");
                std::thread::sleep(Duration::from_millis(2));
            }
            drop(peers);
            Ok(())
        })();
        let _ = child.kill();
        child.wait()?;
        result
    }

    #[test]
    fn ipv6_and_unknown_owners_are_accounted_without_double_counting() -> Result<()> {
        let mut sample = Observation {
            port: 7800,
            ..Observation::default()
        };
        table("header\n0: 00000000000000000000000001000000:1E78 00000000000000000000000001000000:A000 01 0 0 0 0 0 99\n1: 0100007F:1E78 0100007F:A000 01 0 0 0 0 0 100\n", &BTreeSet::from([99]), &mut sample)?;
        assert_eq!(sample.owned_tcp_sockets, 1);
        assert_eq!(sample.local_port_states["01"], 1);
        assert!(sample.remote_port_states.is_empty());
        assert!(table("header\ntruncated\n", &BTreeSet::new(), &mut sample).is_err());
        Ok(())
    }
}
