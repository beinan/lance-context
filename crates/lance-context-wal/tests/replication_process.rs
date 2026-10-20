#![cfg(feature = "replication")]
//! Real child processes and files; the parent relays Raft packets over TCP and
//! deliberately drops selected links. No simulated consensus or storage engine.
use anyhow::{ensure, Context, Result};
use bytes::Bytes;
use lance_context_wal::{
    placement::Layout,
    record::Batch,
    replication::{Applied, Completion, Events, Limits, Node, Outcome, Packet, Page, Status},
};
use protobuf::Message as _;
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use std::{
    collections::{BTreeMap, VecDeque},
    fs::File,
    io::{Read, Write},
    net::{TcpListener, TcpStream},
    path::{Path, PathBuf},
    process::{Child, Command, Stdio},
    sync::Arc,
    time::{Duration, Instant},
};
use uuid::Uuid;

#[derive(Serialize, Deserialize)]
struct WirePacket {
    cluster: Uuid,
    layout: [u8; 32],
    policy: [u8; 32],
    group: u64,
    message: Vec<u8>,
}
impl WirePacket {
    fn encode(packet: Packet) -> Result<Self> {
        Ok(Self {
            cluster: packet.cluster,
            layout: packet.layout,
            policy: packet.policy,
            group: packet.group,
            message: packet.message.write_to_bytes()?,
        })
    }
    fn decode(self) -> Result<Packet> {
        Ok(Packet {
            cluster: self.cluster,
            layout: self.layout,
            policy: self.policy,
            group: self.group,
            message: raft_proto::eraftpb::Message::parse_from_bytes(&self.message)?,
        })
    }
}
#[derive(Serialize, Deserialize)]
enum Request {
    Campaign(u64),
    Propose(u64, u64, Batch),
    Step(WirePacket),
    Tick,
    Status(u64),
    Read(u64, u64),
    Page(u64),
}
#[derive(Serialize, Deserialize)]
enum Response {
    Events(Vec<WirePacket>, Vec<Completion>),
    Status(Status),
    Record(Option<Batch>),
    Page(Page),
    Error(String),
}
fn send<T: Serialize>(stream: &mut TcpStream, value: &T) -> Result<()> {
    let bytes = bincode::serialize(value)?;
    ensure!(bytes.len() <= 32 * 1024 * 1024, "oversized test frame");
    stream.write_all(&(bytes.len() as u32).to_le_bytes())?;
    stream.write_all(&bytes)?;
    Ok(())
}
fn receive<T: DeserializeOwned>(stream: &mut TcpStream) -> Result<T> {
    let mut len = [0u8; 4];
    stream.read_exact(&mut len)?;
    let len = u32::from_le_bytes(len) as usize;
    ensure!(len <= 32 * 1024 * 1024, "oversized test frame");
    let mut bytes = vec![0; len];
    stream.read_exact(&mut bytes)?;
    Ok(bincode::deserialize(&bytes)?)
}
fn run(node: &mut Node, request: Request) -> Result<Response> {
    match request {
        Request::Campaign(group) => node.campaign(group)?,
        Request::Propose(group, token, batch) => node.propose(group, token, batch)?,
        Request::Step(packet) => node.step(packet.decode()?)?,
        Request::Tick => node.tick()?,
        Request::Status(group) => return Ok(Response::Status(node.status(group)?)),
        Request::Read(group, index) => {
            return Ok(Response::Record(node.read_accepted(group, index)?))
        }
        Request::Page(group) => return Ok(Response::Page(node.accepted(group, 0, 128)?)),
    }
    let Events {
        messages,
        completions,
    } = node.poll()?;
    Ok(Response::Events(
        messages
            .into_iter()
            .map(WirePacket::encode)
            .collect::<Result<_>>()?,
        completions,
    ))
}

#[test]
#[ignore = "child fixture, invoked by the process qualification"]
fn replication_peer_fixture() -> Result<()> {
    let Ok(root) = std::env::var("CONTEXT_REPLICATION_FIXTURE") else {
        return Ok(());
    };
    let root = PathBuf::from(root);
    let id = std::env::var("CONTEXT_REPLICATION_NODE")?.parse()?;
    let listener = TcpListener::bind("127.0.0.1:0")?;
    let mut node = Node::open(
        &root.join(format!("data-{id}")),
        Arc::new(serde_json::from_slice(&std::fs::read(
            root.join("layout.json"),
        )?)?),
        id,
        Limits::default(),
    )?;
    let address = PathBuf::from(std::env::var("CONTEXT_REPLICATION_ADDRESS_FILE")?);
    let temporary = address.with_extension("tmp");
    std::fs::write(&temporary, listener.local_addr()?.to_string())?;
    std::fs::rename(temporary, address)?;
    let (mut stream, _) = listener.accept()?;
    stream.set_nodelay(true)?;
    stream.set_read_timeout(Some(Duration::from_secs(30)))?;
    stream.set_write_timeout(Some(Duration::from_secs(10)))?;
    loop {
        let request = receive(&mut stream)?;
        let response =
            run(&mut node, request).unwrap_or_else(|error| Response::Error(format!("{error:#}")));
        send(&mut stream, &response)?;
    }
}

struct Peer {
    child: Child,
    stream: TcpStream,
}
impl Peer {
    fn spawn(root: &Path, id: u64, incarnation: u64) -> Result<Self> {
        let address = root.join(format!("address-{id}-{incarnation}"));
        let mut child = Command::new(std::env::current_exe()?)
            .args([
                "--exact",
                "replication_peer_fixture",
                "--ignored",
                "--nocapture",
            ])
            .env("CONTEXT_REPLICATION_FIXTURE", root)
            .env("CONTEXT_REPLICATION_NODE", id.to_string())
            .env("CONTEXT_REPLICATION_ADDRESS_FILE", &address)
            .stdout(Stdio::from(File::create(
                root.join(format!("stdout-{id}-{incarnation}")),
            )?))
            .stderr(Stdio::from(File::create(
                root.join(format!("stderr-{id}-{incarnation}")),
            )?))
            .spawn()?;
        let result = (|| -> Result<TcpStream> {
            let deadline = Instant::now() + Duration::from_secs(10);
            while !address.exists() {
                ensure!(child.try_wait()?.is_none(), "peer exited during startup");
                ensure!(Instant::now() < deadline, "peer startup outcome unknown");
                std::thread::sleep(Duration::from_millis(5));
            }
            let stream = TcpStream::connect(std::fs::read_to_string(address)?)?;
            stream.set_nodelay(true)?;
            stream.set_read_timeout(Some(Duration::from_secs(10)))?;
            stream.set_write_timeout(Some(Duration::from_secs(10)))?;
            Ok(stream)
        })();
        match result {
            Ok(stream) => Ok(Self { child, stream }),
            Err(error) => {
                let _ = child.kill();
                let _ = child.wait();
                Err(error)
            }
        }
    }
    fn call(&mut self, request: Request) -> Result<Response> {
        send(&mut self.stream, &request)?;
        match receive(&mut self.stream)? {
            Response::Error(error) => anyhow::bail!("peer rejected request: {error}"),
            response => Ok(response),
        }
    }
}
impl Drop for Peer {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

struct Cluster {
    // Peers must die before TempDir removes their exclusively owned files.
    peers: BTreeMap<u64, Peer>,
    root: tempfile::TempDir,
    incarnations: BTreeMap<u64, u64>,
    queue: VecDeque<Packet>,
    completions: Vec<Completion>,
}
impl Cluster {
    fn new() -> Result<Self> {
        let root = tempfile::tempdir()?;
        let layout = Layout {
            cluster: Uuid::from_u128(50),
            nodes: (1..=3)
                .map(|id| (id, format!("wal-{id}.example.invalid:7800")))
                .collect(),
            groups: BTreeMap::from([(10, vec![1, 2, 3]), (20, vec![1, 2, 3])]),
            buckets: vec![10, 20],
        };
        std::fs::write(
            root.path().join("layout.json"),
            serde_json::to_vec(&layout)?,
        )?;
        let mut this = Self {
            peers: BTreeMap::new(),
            root,
            incarnations: BTreeMap::new(),
            queue: VecDeque::new(),
            completions: Vec::new(),
        };
        for id in 1..=3 {
            this.start(id)?;
        }
        Ok(this)
    }
    fn start(&mut self, id: u64) -> Result<()> {
        ensure!(
            !self.peers.contains_key(&id),
            "cannot launch an existing peer"
        );
        let incarnation = self.incarnations.entry(id).or_default();
        *incarnation += 1;
        self.peers
            .insert(id, Peer::spawn(self.root.path(), id, *incarnation)?);
        Ok(())
    }
    fn command(&mut self, id: u64, request: Request) -> Result<()> {
        let Response::Events(packets, completions) = self
            .peers
            .get_mut(&id)
            .context("missing peer")?
            .call(request)?
        else {
            anyhow::bail!("wrong reply");
        };
        for packet in packets {
            self.queue.push_back(packet.decode()?);
        }
        self.completions.extend(completions);
        Ok(())
    }
    fn pump(&mut self, allowed: &[u64]) -> Result<()> {
        for _ in 0..5000 {
            let Some(packet) = self.queue.pop_front() else {
                return Ok(());
            };
            if allowed.contains(&packet.message.from)
                && allowed.contains(&packet.message.to)
                && self.peers.contains_key(&packet.message.to)
            {
                self.command(
                    packet.message.to,
                    Request::Step(WirePacket::encode(packet)?),
                )?;
            }
        }
        anyhow::bail!("test message queue did not settle")
    }
    fn outcome(&mut self, token: u64) -> Option<Outcome> {
        self.completions
            .iter()
            .position(|c| c.request == token)
            .map(|position| self.completions.remove(position).outcome)
    }
    fn elect(&mut self, id: u64, group: u64, allowed: &[u64]) -> Result<u64> {
        self.command(id, Request::Campaign(group))?;
        self.pump(allowed)?;
        for _ in 0..100 {
            let mut views = Vec::new();
            for node in allowed {
                let Response::Status(status) = self
                    .peers
                    .get_mut(node)
                    .unwrap()
                    .call(Request::Status(group))?
                else {
                    anyhow::bail!("wrong status");
                };
                views.push(status);
            }
            let leader = views[0].leader;
            if allowed.contains(&leader)
                && views
                    .iter()
                    .all(|s| s.leader == leader && s.term == views[0].term)
            {
                return Ok(leader);
            }
            // Expire the old election lease; do not assume which randomized
            // timer wins when both eligible replicas have identical logs.
            for node in allowed {
                self.command(*node, Request::Tick)?;
            }
            self.pump(allowed)?;
        }
        anyhow::bail!("majority did not elect a leader")
    }
}

fn input(bucket: u32, sequence: u64, fill: u8) -> Batch {
    Batch::new(
        Uuid::from_u128(80),
        bucket,
        Uuid::from_u128(90),
        sequence,
        Bytes::from(vec![fill; 64 * 1024]),
    )
}

#[test]
fn durable_quorum_retry_leader_loss_suffix_replacement_and_full_restart() -> Result<()> {
    let mut c = Cluster::new()?;
    c.command(1, Request::Campaign(10))?;
    c.pump(&[1, 2, 3])?;
    let first = input(0, 1, 7);
    c.command(1, Request::Propose(10, 1, first.clone()))?;
    assert!(
        c.outcome(1).is_none(),
        "local append was mistaken for quorum ACK"
    );
    c.pump(&[1, 2])?;
    let first_ack = c.outcome(1).context("no majority ACK")?;
    assert!(matches!(
        first_ack,
        Outcome::Committed(Applied::Accepted { .. })
    ));
    // Only one follower held the acknowledged input when its leader was killed.
    drop(c.peers.remove(&1));
    assert_eq!(
        c.elect(2, 10, &[2, 3])?,
        2,
        "only replica 2 has the committed suffix"
    );
    c.command(2, Request::Propose(10, 2, first.clone()))?;
    c.pump(&[2, 3])?;
    assert_eq!(
        c.outcome(2),
        Some(first_ack.clone()),
        "ambiguous client retry lost original receipt"
    );
    c.start(1)?;
    for _ in 0..4 {
        c.command(2, Request::Tick)?;
        c.pump(&[1, 2, 3])?;
    }
    // A still-self-declared but isolated leader cannot acknowledge a local retry.
    c.command(2, Request::Propose(10, 3, first.clone()))?;
    c.pump(&[2])?;
    assert!(c.outcome(3).is_none());
    let abandoned = input(0, 2, 99);
    c.command(2, Request::Propose(10, 4, abandoned))?;
    c.pump(&[2])?;
    assert!(c.outcome(4).is_none());
    let replacement = c.elect(1, 10, &[1, 3])?;
    let second = input(0, 2, 8);
    c.command(replacement, Request::Propose(10, 5, second.clone()))?;
    c.pump(&[1, 3])?;
    assert!(matches!(
        c.outcome(5),
        Some(Outcome::Committed(Applied::Accepted { .. }))
    ));
    for _ in 0..4 {
        c.command(replacement, Request::Tick)?;
        c.pump(&[1, 2, 3])?;
    }
    assert_eq!(c.outcome(3), Some(Outcome::Unknown));
    assert_eq!(
        c.outcome(4),
        Some(Outcome::Unknown),
        "replaced suffix completed the wrong input"
    );
    let other_leader = c.elect(replacement, 20, &[1, 2, 3])?;
    let separate = input(1, 1, 9);
    c.command(other_leader, Request::Propose(20, 6, separate.clone()))?;
    c.pump(&[1, 2, 3])?;
    let other_outcome = c.outcome(6);
    assert!(
        matches!(
            other_outcome,
            Some(Outcome::Committed(Applied::Accepted { .. }))
        ),
        "other group outcome: {other_outcome:?}"
    );
    // Kill every process, retain its original files, then reconstruct metadata
    // and exact payloads solely from its durable committed prefix.
    c.queue.clear();
    c.peers.clear();
    for id in 1..=3 {
        c.start(id)?;
    }
    for peer in c.peers.values_mut() {
        for (group, expected) in [(10, vec![&first, &second]), (20, vec![&separate])] {
            let Response::Page(page) = peer.call(Request::Page(group))? else {
                anyhow::bail!("wrong page");
            };
            assert_eq!(page.entries.len(), expected.len());
            for ((index, meta), batch) in page.entries.into_iter().zip(expected) {
                assert_eq!(&meta, &batch.meta);
                let Response::Record(Some(actual)) = peer.call(Request::Read(group, index))? else {
                    anyhow::bail!("missing record");
                };
                assert_eq!(&actual, batch);
            }
        }
    }
    c.command(1, Request::Campaign(10))?;
    c.pump(&[1, 2, 3])?;
    c.command(1, Request::Propose(10, 7, second))?;
    c.pump(&[1, 2, 3])?;
    assert!(matches!(
        c.outcome(7),
        Some(Outcome::Committed(Applied::Accepted { .. }))
    ));
    println!("verified real RF3 processes: durable majority ACK, latest retry, isolated old leader, replaced uncommitted suffix, two groups and full process restart with exact payloads");
    Ok(())
}
