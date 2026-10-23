//! Kill a real publisher process at durable boundaries. The parent hosts three
//! TLS WAL runtimes, then reopens their stores between publisher incarnations.
//! This tests process failure, not loss of the filesystem or storage power.
use super::*;
use crate::application::{self, config};
use std::{
    fs::File,
    io::Write,
    os::unix::process::ExitStatusExt,
    path::{Path, PathBuf},
    process::{Child, Command},
};

#[derive(Serialize, Deserialize)]
struct Case {
    config: PathBuf,
    ready: PathBuf,
    phase: String,
    expected_epoch: u64,
}

#[derive(Serialize, Deserialize)]
struct Ready {
    pending: p::PendingPlan,
    prepared: p::Prepared,
    checkpoint: BTreeMap<u64, u64>,
    selected: bool,
    confirmed: bool,
}

struct Process(Child);
impl Drop for Process {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "publisher subprocess fixture; invoked by compaction_survives_sigkill"]
async fn compaction_process_child() -> Result<()> {
    let case: Case = serde_json::from_slice(&std::fs::read(std::env::var(
        "CONTEXT_COMPACTION_PROCESS_CASE",
    )?)?)?;
    let config = application::Config::read(&case.config)?;
    let table = &config.tables[0];
    let ingress = Ingress::connect(
        config.client()?,
        Arc::new(config.layout.clone()),
        table.binding.clone(),
        MergeMemoryBudget::new(config.ingress_memory_bytes),
    )
    .await?;
    let storage = Storage::resolve(
        table.uri.clone(),
        table.storage_options.clone(),
        Arc::default(),
    )
    .await?;
    let mut publisher = acquire(
        &ingress,
        &storage,
        &MergeMemoryBudget::new(32 << 20),
        case.expected_epoch,
        options(case.phase == "prepared"),
    )
    .await?;
    let prepared = publisher
        .prepare_next()
        .await?
        .context("missing compaction")?;
    let view = publisher.state().await?;
    let pending = view.progress.pending.as_ref().context("missing plan")?;
    ensure!(pending.plan.is_compaction(), "child selected an append");
    match case.phase.as_str() {
        "prepared" => {}
        "selected" => {
            crate::materializer::stage::publish(
                &publisher,
                pending,
                &prepared,
                &view.manifests.as_ref().unwrap().versions[&pending.plan.read_version],
                publisher
                    .read_dataset(Some(pending.plan.read_version))
                    .await?,
            )
            .await?;
        }
        "confirmed" => {
            publisher.commit_pending().await?;
        }
        _ => anyhow::bail!("unknown process test phase"),
    }
    let view = publisher.state().await?;
    let ready = Ready {
        pending: (**pending).clone(),
        prepared: (*prepared).clone(),
        checkpoint: view.progress.checkpoint.clone(),
        selected: view
            .manifests
            .as_ref()
            .unwrap()
            .versions
            .contains_key(&pending.plan.target_version()?),
        confirmed: view.progress.published.is_some(),
    };
    let temporary = case.ready.with_extension("part");
    let mut file = File::create(&temporary)?;
    file.write_all(&serde_json::to_vec(&ready)?)?;
    file.sync_all()?;
    std::fs::rename(temporary, &case.ready)?;
    // The parent checks this exact checkpoint, then sends SIGKILL. No graceful
    // shutdown or unwinding may complete publication on the child's behalf.
    std::future::pending::<()>().await;
    unreachable!()
}

fn data_files(path: &Path) -> Result<std::collections::BTreeSet<PathBuf>> {
    std::fs::read_dir(path.join("data"))?
        .map(|entry| Ok(entry?.path()))
        .collect()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn compaction_survives_sigkill() -> Result<()> {
    let root = tempfile::tempdir()?;
    let path = root.path().join("crash.lance");
    let mut cluster = network::Cluster::open_signed(root.path(), layout()).await?;
    bootstrap(&cluster, &path, &binding()).await?;
    let mut ingress = connect(&cluster, binding())
        .await?
        .with_session_read_concurrency(2)?;
    let storage = Storage::resolve(
        path.to_str().unwrap().into(),
        Default::default(),
        Arc::default(),
    )
    .await?;
    let budget = MergeMemoryBudget::new(32 << 20);
    let opens = (0..2)
        .map(|bucket| ProducerOpen {
            bucket,
            slot: bucket,
            expected_generation: 0,
            operation: Uuid::new_v4(),
        })
        .collect::<Vec<_>>();
    let mut expected = BTreeMap::new();
    let mut publisher = acquire(&ingress, &storage, &budget, 1, options(false)).await?;
    for i in 0..4 {
        let (body, _) = append(
            &ingress,
            &opens[i % 2],
            (i / 2 + 1) as u64,
            &[&format!("row-{i}")],
        )
        .await?;
        remember(&mut expected, body)?;
        publisher
            .run_once()
            .await?
            .context("append was not published")?;
    }
    verify(&publisher, &expected, 5).await?;
    assert_eq!(
        publisher
            .read_dataset(None)
            .await?
            .manifest()
            .fragments
            .len(),
        4
    );
    let checkpoint = publisher.state().await?.progress.checkpoint.clone();
    drop(publisher);
    let mut all = expected.clone();
    let mut original: Option<Ready> = None;
    let mut original_files = None;
    for (ordinal, phase) in ["prepared", "selected", "confirmed"]
        .into_iter()
        .enumerate()
    {
        let mut config = cluster.application.clone();
        config.tables.push(config::Table {
            binding: binding(),
            uri: path.to_str().unwrap().into(),
            storage_options: Default::default(),
            bootstrap: bootstrap::identity(),
        });
        let config_file = root.path().join(format!("{phase}-config.json"));
        std::fs::write(&config_file, serde_json::to_vec(&config)?)?;
        let case = Case {
            config: config_file,
            ready: root.path().join(format!("{phase}-ready.json")),
            phase: phase.into(),
            expected_epoch: 2 + ordinal as u64,
        };
        let case_file = root.path().join(format!("{phase}-case.json"));
        std::fs::write(&case_file, serde_json::to_vec(&case)?)?;
        let log = root.path().join(format!("{phase}.log"));
        let output = File::create(&log)?;
        let mut child = Process(
            Command::new(std::env::current_exe()?)
                .args([
                    "--exact",
                    "tests::materializer::compaction::process::compaction_process_child",
                    "--ignored",
                    "--nocapture",
                ])
                .env("CONTEXT_COMPACTION_PROCESS_CASE", &case_file)
                .stdout(output.try_clone()?)
                .stderr(output)
                .spawn()?,
        );
        tokio::time::timeout(Duration::from_secs(30), async {
            while !case.ready.exists() {
                ensure!(
                    child.0.try_wait()?.is_none(),
                    "publisher exited before {phase}: {}",
                    std::fs::read_to_string(&log)?
                );
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
            Ok::<_, anyhow::Error>(())
        })
        .await
        .with_context(|| {
            format!(
                "publisher never reached {phase}: {}",
                std::fs::read_to_string(&log).unwrap_or_default()
            )
        })??;
        let ready: Ready = serde_json::from_slice(&std::fs::read(&case.ready)?)?;
        assert_eq!(ready.checkpoint, checkpoint);
        assert_eq!(ready.selected, ordinal >= 1);
        assert_eq!(ready.confirmed, ordinal == 2);
        if let Some(original) = &original {
            assert_eq!(ready.pending, original.pending);
            assert_eq!(ready.prepared, original.prepared);
            assert_eq!(
                data_files(&path)?,
                *original_files.as_ref().unwrap(),
                "recovery rewrote prepared files"
            );
        } else {
            original_files = Some(data_files(&path)?);
            original = Some(ready);
            let (body, _) = append(&ingress, &opens[0], 3, &["queued-after-prepare"]).await?;
            remember(&mut all, body)?;
        }
        query_all(&ingress, &storage, &all).await?;
        child.0.kill()?;
        assert_eq!(child.0.wait()?.signal(), Some(9));
        query_all(&ingress, &storage, &all).await?;
        drop(ingress);
        cluster.shutdown().await?;
        cluster = network::Cluster::open_signed(root.path(), layout()).await?;
        ingress = connect(&cluster, binding())
            .await?
            .with_session_read_concurrency(2)?;
        query_all(&ingress, &storage, &all).await?;
    }
    let original = original.unwrap();
    let mut publisher = acquire(&ingress, &storage, &budget, 5, options(false)).await?;
    assert_eq!(*publisher.prepare_next().await?.unwrap(), original.prepared);
    publisher.commit_pending().await?;
    assert_eq!(data_files(&path)?, original_files.unwrap());
    publisher.finish_pending().await?;
    assert_eq!(publisher.state().await?.progress.checkpoint, checkpoint);
    verify(&publisher, &expected, 6).await?;
    assert!(
        publisher
            .read_dataset(None)
            .await?
            .manifest()
            .fragments
            .len()
            < 4
    );
    publisher
        .run_once()
        .await?
        .context("queued append was not published")?;
    verify(&publisher, &all, 7).await?;
    query_all(&ingress, &storage, &all).await?;
    assert!(publisher.run_once().await?.is_none());
    assert_eq!(budget.reserved(), 0);
    drop((publisher, ingress));
    cluster.shutdown().await
}
