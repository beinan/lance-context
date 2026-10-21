//! Actual Lance writer/reader adapter over the six-process catalog fixture.
//! This qualifies the durable index, not production transport or coordinated GC.
use super::*;
#[path = "manifest_retention.rs"]
mod manifest_retention;
#[path = "producer_lifecycle.rs"]
pub(super) mod producer_lifecycle;
#[path = "source_closure.rs"]
mod source_closure;
use lance_context_wal::replication::publication::{
    Authority, BootstrapManifest, ConfirmPublication, ManifestObject, ManifestRecord, Prepared,
    ReserveManifest,
};
use object_store::path::Path as ObjectPath;
use std::sync::{
    atomic::{AtomicBool, AtomicU64, Ordering},
    Mutex,
};

pub(super) async fn bootstrap(
    c: &mut Cluster,
    leader: u64,
    dataset: &Dataset,
    authority: &Authority,
) -> Result<(Uuid, Arc<ManifestRecord>)> {
    let location = dataset.manifest_location();
    let filename = location.path.filename().context("manifest filename")?;
    let incarnation = Uuid::from_u128(930);
    let genesis = dataset
        .read_transaction_by_version(1)
        .await?
        .context("actual genesis transaction")?;
    c.command(
        leader,
        Request::BootstrapManifest(
            2009,
            BootstrapManifest {
                authority: authority.clone(),
                incarnation,
                request: Uuid::from_u128(931),
                version: 1,
                object: ManifestObject {
                    path: format!("_versions/{filename}"),
                    bytes: location.size.context("known written size")?,
                    v2_names: location.naming_scheme == ManifestNamingScheme::V2,
                },
                transaction: Uuid::parse_str(&genesis.uuid)?,
            },
        ),
    )?;
    c.pump(&[1, 2, 3])?;
    let Some(Outcome::Committed(Applied::Publisher(PublisherReply::ManifestReserved(record)))) =
        c.outcome(2009)
    else {
        anyhow::bail!("manifest bootstrap missing");
    };
    Ok((incarnation, record))
}

pub(super) struct Attempt {
    pub uri: String,
    pub base: Dataset,
    pub tx: Transaction,
    pub prepared: Arc<Prepared>,
    pub authority: Authority,
    pub bootstrap: (Uuid, Arc<ManifestRecord>),
    pub input: Batch,
}

#[derive(Clone, Copy, PartialEq, Eq)]
pub(super) enum Mode {
    Normal,
    LostReply,
    Takeover,
    Retention,
    ProducerSlots,
}

#[derive(Default)]
struct Pause {
    uploaded: tokio::sync::Notify,
    resume: tokio::sync::Notify,
    path: Mutex<Option<ObjectPath>>,
}

struct Managed {
    cluster: Arc<Mutex<Cluster>>,
    base: ObjectPath,
    scope: ReserveManifest,
    tokens: AtomicU64,
    lose_reply: AtomicBool,
    pause: Option<Arc<Pause>>,
}
impl std::fmt::Debug for Managed {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("ManagedCatalogFixture")
    }
}
fn error(error: impl std::fmt::Display) -> lance::Error {
    lance::Error::io(error.to_string())
}
impl Managed {
    fn check_base(&self, base: &ObjectPath) -> lance::Result<()> {
        if *base != self.base {
            return Err(lance::Error::invalid_input("foreign managed dataset"));
        }
        Ok(())
    }
    fn read(&self, version: Option<u64>) -> lance::Result<Option<Arc<ManifestRecord>>> {
        let mut c = self.cluster.lock().unwrap();
        let leader = c.elect(1, 10, &[1, 2, 3]).map_err(error)?;
        let token = self.tokens.fetch_add(1, Ordering::Relaxed);
        c.command(
            leader,
            Request::Query(
                10,
                token,
                Query::Manifest {
                    table: self.scope.authority.table,
                    version,
                },
            ),
        )
        .map_err(error)?;
        c.pump(&[1, 2, 3]).map_err(error)?;
        match c.read_outcome(token) {
            Some(ReadOutcome::Observed {
                value: Value::Manifest(record),
                ..
            }) => Ok(record),
            other => Err(error(format!(
                "manifest quorum read unavailable: {other:?}"
            ))),
        }
    }
    fn location(&self, record: &ManifestRecord) -> ManifestLocation {
        ManifestLocation {
            version: record.version,
            path: ObjectPath::parse(format!("{}/{}", self.base, record.object.path))
                .expect("validated relative manifest path"),
            size: Some(record.object.bytes),
            naming_scheme: if record.object.v2_names {
                ManifestNamingScheme::V2
            } else {
                ManifestNamingScheme::V1
            },
            e_tag: None,
            identity: Some(record.identity.to_string()),
        }
    }
}

#[async_trait::async_trait]
#[allow(clippy::too_many_arguments)]
impl CommitHandler for Managed {
    async fn resolve_latest_location(
        &self,
        base: &ObjectPath,
        _: &lance_io::object_store::ObjectStore,
    ) -> lance::Result<ManifestLocation> {
        self.check_base(base)?;
        self.read(None)?
            .map(|r| self.location(&r))
            .ok_or_else(|| lance::Error::not_found("managed head"))
    }
    async fn resolve_version_location(
        &self,
        base: &ObjectPath,
        version: u64,
        _: &dyn object_store::ObjectStore,
    ) -> lance::Result<ManifestLocation> {
        self.check_base(base)?;
        self.read(Some(version))?
            .map(|r| self.location(&r))
            .ok_or_else(|| lance::Error::not_found(format!("managed version {version}")))
    }
    async fn version_exists(
        &self,
        base: &ObjectPath,
        version: u64,
        _: &dyn object_store::ObjectStore,
        _: ManifestNamingScheme,
    ) -> lance::Result<bool> {
        self.check_base(base)?;
        Ok(self.read(Some(version))?.is_some())
    }
    async fn commit(
        &self,
        manifest: &mut Manifest,
        indices: Option<Vec<IndexMetadata>>,
        base: &ObjectPath,
        store: &lance_io::object_store::ObjectStore,
        writer: ManifestWriter,
        naming: ManifestNamingScheme,
        transaction: Option<ManifestTransaction>,
    ) -> std::result::Result<ManifestLocation, CommitError> {
        self.check_base(base)?;
        if manifest.version != self.scope.target
            || transaction
                .as_ref()
                .is_none_or(|tx| tx.as_pb().uuid != self.scope.transaction.to_string())
        {
            return Err(CommitError::CommitConflict);
        }
        let path = ObjectPath::parse(format!(
            "{}-{}",
            naming.manifest_path(base, manifest.version),
            Uuid::new_v4()
        ))
        .map_err(error)?;
        let written = writer(store, manifest, indices, &path, transaction).await?;
        if let Some(pause) = &self.pause {
            *pause.path.lock().unwrap() = Some(path.clone());
            pause.uploaded.notify_one();
            pause.resume.notified().await;
        }
        let mut request = self.scope.clone();
        request.object = ManifestObject {
            path: format!("_versions/{}", path.filename().expect("staging filename")),
            bytes: written.size as u64,
            v2_names: naming == ManifestNamingScheme::V2,
        };
        let outcome = {
            let mut c = self.cluster.lock().unwrap();
            let leader = c.elect(1, 10, &[1, 2, 3]).map_err(error)?;
            let token = self.tokens.fetch_add(1, Ordering::Relaxed);
            c.command(leader, Request::ReserveManifest(token, request))
                .map_err(error)?;
            c.pump(&[1, 2, 3]).map_err(error)?;
            c.outcome(token)
        };
        let Some(Outcome::Committed(Applied::Publisher(PublisherReply::ManifestReserved(record)))) =
            outcome
        else {
            // Keep staging on every unresolved/error outcome in this test.
            return Err(CommitError::OtherError(error(format!(
                "reservation unavailable: {outcome:?}"
            ))));
        };
        if self.lose_reply.swap(false, Ordering::SeqCst) {
            return Err(CommitError::OtherError(error(
                "injected lost reply after durable catalog commit",
            )));
        }
        Ok(self.location(&record))
    }
    async fn forget_version(&self, _: &ObjectPath, _: u64, _: &str) -> lance::Result<()> {
        Err(lance::Error::not_supported(
            "coordinated collection is not enabled",
        ))
    }
}

pub(super) async fn publish_and_recover(c: Cluster, attempt: Attempt, mode: Mode) -> Result<()> {
    let base_path = attempt
        .base
        .manifest_location()
        .path
        .as_ref()
        .rsplit_once("/_versions/")
        .context("dataset base path")?
        .0;
    let base_path = ObjectPath::parse(base_path)?;
    let cluster = Arc::new(Mutex::new(c));
    let pause = (mode == Mode::Takeover).then(|| Arc::new(Pause::default()));
    let mut handler = Arc::new(Managed {
        cluster: cluster.clone(),
        base: base_path,
        scope: ReserveManifest {
            authority: attempt.authority.clone(),
            incarnation: attempt.bootstrap.0,
            transaction: attempt.prepared.transaction,
            plan_index: attempt.prepared.plan_index,
            prepared_index: attempt.prepared.index,
            descriptor_digest: attempt.prepared.descriptors.digest,
            predecessor: attempt.bootstrap.1.identity,
            target: 2,
            object: attempt.bootstrap.1.object.clone(),
        },
        tokens: AtomicU64::new(6000),
        lose_reply: AtomicBool::new(mode == Mode::LostReply),
        pause: pause.clone(),
    });
    if let Some(pause) = pause {
        let old = tokio::spawn(commit(
            Arc::new(attempt.base),
            attempt.tx.clone(),
            handler.clone(),
        ));
        tokio::time::timeout(Duration::from_secs(20), pause.uploaded.notified()).await?;
        let old_path = pause
            .path
            .lock()
            .unwrap()
            .clone()
            .context("paused staging path")?;
        assert!(Path::new(&attempt.uri)
            .join("_versions")
            .join(old_path.filename().unwrap())
            .is_file());
        let authority = takeover(&handler)?;
        pause.resume.notify_one();
        let old_result = tokio::time::timeout(Duration::from_secs(20), old).await??;
        assert!(
            old_result.is_err(),
            "old owner selected a path after takeover"
        );
        assert!(handler.read(Some(2))?.is_none());
        assert_eq!(handler.read(None)?.unwrap().version, 1);
        let mut scope = handler.scope.clone();
        scope.authority = authority;
        handler = Arc::new(Managed {
            cluster: cluster.clone(),
            base: handler.base.clone(),
            scope,
            tokens: AtomicU64::new(10000),
            lose_reply: AtomicBool::new(false),
            pause: None,
        });
        let base = lance::dataset::builder::DatasetBuilder::from_uri(&attempt.uri)
            .with_commit_handler(handler.clone())
            .load()
            .await?;
        commit(Arc::new(base), attempt.tx, handler.clone()).await?;
        assert_ne!(
            handler.location(&handler.read(Some(2))?.unwrap()).path,
            old_path
        );
    } else {
        let result = commit(Arc::new(attempt.base), attempt.tx, handler.clone()).await;
        if mode == Mode::LostReply {
            assert!(
                result.is_err(),
                "lost response should remain ambiguous to the caller"
            );
        } else {
            result?;
        }
    }
    let selected = handler.read(Some(2))?.context("missing committed target")?;
    assert!(selected.object.path.contains(".manifest-"));
    let confirmation = ConfirmPublication {
        authority: handler.scope.authority.clone(),
        incarnation: handler.scope.incarnation,
        transaction: selected.transaction,
        plan_index: selected.plan_index,
        prepared_index: selected.prepared_index,
        descriptor_digest: selected.descriptor_digest,
        target: selected.version,
        manifest_identity: selected.identity,
    };
    {
        let mut c = cluster.lock().unwrap();
        let leader = c.elect(1, 10, &[1, 2, 3])?;
        c.command(
            leader,
            Request::ConfirmPublication(500_000, confirmation.clone()),
        )?;
        c.pump(&[1, 2, 3])?;
        // Lose the confirmation reply before any caller consumes it. Recovery
        // must discover the outcome from durable state, including its index.
        let before = c.completions.len();
        c.completions
            .retain(|completion| completion.request != 500_000);
        assert_eq!(before - c.completions.len(), 1);
        c.peers
            .get_mut(&leader)
            .unwrap()
            .call(Request::PrepareCheckpoint(10, Uuid::new_v4()))?;
        c.command(leader, Request::InstallCheckpoint)?;
        c.pump(&[1, 2, 3])?;
        c.queue.clear();
        c.peers.clear();
        for id in 1..=6 {
            c.start(id)?;
        }
        for id in 4..=6 {
            verify_records(
                c.peers.get_mut(&id).unwrap(),
                20,
                std::slice::from_ref(&attempt.input),
            )?;
        }
    }
    assert_eq!(handler.read(Some(2))?.as_deref(), Some(selected.as_ref()));
    {
        let mut c = cluster.lock().unwrap();
        let leader = c.elect(1, 10, &[1, 2, 3])?;
        c.command(
            leader,
            Request::Query(
                10,
                500_001,
                Query::Publisher {
                    table: confirmation.authority.table,
                },
            ),
        )?;
        c.pump(&[1, 2, 3])?;
        let Some(ReadOutcome::Observed {
            value: Value::Publisher(Some(view)),
            ..
        }) = c.read_outcome(500_001)
        else {
            anyhow::bail!("publication outcome query missing");
        };
        let published = view
            .progress
            .published
            .context("durable outcome missing after restart")?;
        assert_eq!(published.manifest, selected);
        assert!(published.index > selected.index);
        assert_eq!(
            view.progress
                .pending
                .context("source selection lost")?
                .index,
            selected.plan_index
        );
        assert_eq!(
            view.progress.prepared.context("descriptors lost")?,
            attempt.prepared
        );
        assert!(view.progress.checkpoint.is_empty());
        c.command(leader, Request::ConfirmPublication(500_002, confirmation))?;
        c.pump(&[1, 2, 3])?;
        assert_eq!(
            c.outcome(500_002),
            Some(Outcome::Committed(Applied::Publisher(
                PublisherReply::Published(published)
            )))
        );
    }
    source_closure::close_and_restart(
        &cluster,
        handler.scope.authority.table,
        &selected,
        &attempt.input,
    )?;
    if mode == Mode::ProducerSlots {
        producer_lifecycle::reuse_after_retirement(&cluster, &attempt.input)?;
    }
    // Both version lookup and latest lookup are strict catalog reads. A raw
    // URI reader cannot discover this unique path and is not a managed reader.
    let latest = lance::dataset::builder::DatasetBuilder::from_uri(&attempt.uri)
        .with_commit_handler(handler.clone())
        .load()
        .await?;
    assert_eq!(latest.version().version, 2);
    assert_eq!(
        latest.manifest_location().identity,
        Some(selected.identity.to_string())
    );
    let transaction = latest
        .read_transaction_by_version(2)
        .await?
        .context("missing actual transaction")?;
    assert_eq!(transaction.uuid, attempt.prepared.transaction.to_string());
    verify_dataset(latest, &[0, 1, 2, 3, 4]).await?;
    assert!(handler.read(Some(3))?.is_none());
    assert_eq!(handler.read(None)?.unwrap().version, 2);
    if mode == Mode::Retention {
        manifest_retention::qualify(&attempt.uri, handler).await?;
    }
    Ok(())
}

async fn commit(
    base: Arc<Dataset>,
    tx: Transaction,
    handler: Arc<Managed>,
) -> lance::Result<Dataset> {
    CommitBuilder::new(base)
        .with_commit_handler(handler)
        .with_max_retries(0)
        .with_skip_auto_cleanup(true)
        .execute(tx)
        .await
}

fn takeover(handler: &Managed) -> Result<Authority> {
    let mut c = handler.cluster.lock().unwrap();
    let leader = c.elect(1, 10, &[1, 2, 3])?;
    c.command(
        leader,
        Request::Query(
            10,
            100_000,
            Query::Publisher {
                table: handler.scope.authority.table,
            },
        ),
    )?;
    c.pump(&[1, 2, 3])?;
    let Some(ReadOutcome::Observed {
        value: Value::Publisher(Some(view)),
        ..
    }) = c.read_outcome(100_000)
    else {
        anyhow::bail!("takeover view missing");
    };
    c.command(
        leader,
        Request::Acquire(
            100_001,
            Acquire {
                table: view.authority.table,
                destination: view.destination,
                ingest: view.ingest,
                owner: Uuid::from_u128(940),
                request: Uuid::from_u128(941),
                expected_epoch: view.authority.epoch,
            },
        ),
    )?;
    c.pump(&[1, 2, 3])?;
    let Some(Outcome::Committed(Applied::Publisher(PublisherReply::Acquired(authority)))) =
        c.outcome(100_001)
    else {
        anyhow::bail!("takeover did not commit");
    };
    Ok(authority)
}
