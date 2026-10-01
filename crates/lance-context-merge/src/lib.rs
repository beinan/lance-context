//! Execution fences outlive scheduler leases and HTTP connections.
//!
//! A claim authorizes admission, not cancellation of an existing storage write.
//! Only the executor can publish its terminal outcome. Recovery first cancels
//! and reconciles the old execution; it never clears a running fence on timeout.

pub mod failure;

use etcd_client::{Client, Compare, CompareOp, Txn, TxnOp};
use serde::{Deserialize, Serialize};

pub type Result<T> = std::result::Result<T, String>;

#[derive(Clone, Debug)]
pub struct ClaimProof {
    pub key: String,
    pub token: String,
    pub lease_id: i64,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum Phase {
    Reserved,
    Running,
    Finished,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct Execution {
    pub id: String,
    pub target: String,
    pub endpoint: String,
    pub instance: String,
    pub timeout_secs: u64,
    pub phase: Phase,
    pub reclaimed: usize,
    pub error: Option<String>,
}

impl Execution {
    pub fn new(target: &str, endpoint: &str, instance: &str, timeout_secs: u64) -> Self {
        Self {
            id: uuid::Uuid::new_v4().to_string(),
            target: target.into(),
            endpoint: endpoint.into(),
            instance: instance.into(),
            timeout_secs,
            phase: Phase::Reserved,
            reclaimed: 0,
            error: None,
        }
    }

    pub fn finished(&self, outcome: Result<usize>) -> Self {
        let mut next = self.clone();
        next.phase = Phase::Finished;
        match outcome {
            Ok(n) => next.reclaimed = n,
            Err(e) => next.error = Some(e),
        }
        next
    }
}

/// Deliberately no lease on these keys. An expired task claim must not let a
/// second endpoint commit while the first endpoint's HTTP handler is still alive.
#[derive(Clone)]
pub struct Coordinator {
    client: Client,
    prefix: String,
}

pub fn execution_key(prefix: &str, target: &str) -> String {
    let encoded: String = target
        .as_bytes()
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect();
    format!(
        "{}/merge-executions/{encoded}",
        prefix.trim_end_matches('/')
    )
}

pub fn target_lock_key(prefix: &str, target: &str) -> String {
    execution_key(prefix, target).replace("/merge-executions/", "/target-locks/")
}

pub fn execution_owner(execution: &Execution) -> String {
    format!("merge-execution:{}", execution.id)
}

fn encode(execution: &Execution) -> Vec<u8> {
    serde_json::to_vec(execution).expect("execution is JSON serializable")
}

impl Coordinator {
    pub fn new(client: Client, prefix: impl Into<String>) -> Self {
        Self {
            client,
            prefix: prefix.into(),
        }
    }

    pub async fn get(&self, target: &str) -> Result<Option<Execution>> {
        let response = self
            .client
            .clone()
            .get(execution_key(&self.prefix, target), None)
            .await
            .map_err(|e| e.to_string())?;
        response
            .kvs()
            .first()
            .map(|kv| serde_json::from_slice(kv.value()).map_err(|e| e.to_string()))
            .transpose()
    }

    /// Atomic claim check and admission close the delayed-request/lease-loss race.
    pub async fn reserve(&self, claim: &ClaimProof, execution: &Execution) -> Result<bool> {
        if execution.phase != Phase::Reserved || execution.timeout_secs == 0 {
            return Err("invalid merge execution reservation".into());
        }
        let key = execution_key(&self.prefix, &execution.target);
        self.transact(
            vec![
                Compare::value(claim.key.as_str(), CompareOp::Equal, claim.token.as_bytes()),
                Compare::version(key.as_str(), CompareOp::Equal, 0),
                Compare::value(
                    target_lock_key(&self.prefix, &execution.target),
                    CompareOp::Equal,
                    claim.token.as_bytes(),
                ),
            ],
            vec![
                TxnOp::put(key, encode(execution), None),
                TxnOp::put(
                    target_lock_key(&self.prefix, &execution.target),
                    execution_owner(execution),
                    None,
                ),
            ],
        )
        .await
    }

    pub async fn start(&self, execution: &Execution) -> Result<Option<Execution>> {
        if execution.phase != Phase::Reserved {
            return Err("execution is not reserved".into());
        }
        let mut running = execution.clone();
        running.phase = Phase::Running;
        Ok(self.replace(execution, &running).await?.then_some(running))
    }

    pub async fn finish(&self, running: &Execution, outcome: Result<usize>) -> Result<bool> {
        if running.phase != Phase::Running {
            return Err("execution is not running".into());
        }
        self.replace(running, &running.finished(outcome)).await
    }

    /// Cancelling an unstarted request is a CAS. A late POST cannot start it.
    pub async fn cancel_reserved(&self, execution: &Execution) -> Result<bool> {
        if execution.phase != Phase::Reserved {
            return Ok(false);
        }
        self.replace(
            execution,
            &execution.finished(Err("cancelled before admission".into())),
        )
        .await
    }

    /// Never delete on elapsed time, an HTTP error, or claim expiry. Both the
    /// terminal result and the current task claim must still match atomically.
    pub async fn release(&self, claim: &ClaimProof, execution: &Execution) -> Result<bool> {
        if execution.phase != Phase::Finished {
            return Err("cannot release a live execution".into());
        }
        let key = execution_key(&self.prefix, &execution.target);
        let (mut compares, mut operations) = self.completion_changes(execution).await?;
        compares.extend([
            Compare::value(claim.key.as_str(), CompareOp::Equal, claim.token.as_bytes()),
            Compare::value(key.as_str(), CompareOp::Equal, encode(execution)),
            Compare::value(
                target_lock_key(&self.prefix, &execution.target),
                CompareOp::Equal,
                execution_owner(execution),
            ),
        ]);
        operations.extend([
            TxnOp::delete(key, None),
            TxnOp::put(
                target_lock_key(&self.prefix, &execution.target),
                claim.token.clone(),
                Some(etcd_client::PutOptions::new().with_lease(claim.lease_id)),
            ),
        ]);
        self.transact(compares, operations).await
    }

    async fn replace(&self, old: &Execution, new: &Execution) -> Result<bool> {
        let key = execution_key(&self.prefix, &old.target);
        self.transact(
            vec![Compare::value(key.as_str(), CompareOp::Equal, encode(old))],
            vec![TxnOp::put(key, encode(new), None)],
        )
        .await
    }

    async fn transact(&self, compares: Vec<Compare>, operations: Vec<TxnOp>) -> Result<bool> {
        self.client
            .clone()
            .txn(Txn::new().when(compares).and_then(operations))
            .await
            .map(|r| r.succeeded())
            .map_err(|e| e.to_string())
    }
}

/// The executor owns this future independently of the request handler. On
/// cancellation, drop its scoped storage future *before* publishing Finished.
/// Callers must not pass a detached JoinHandle: dropping one does not stop it.
pub async fn execute_scoped<F>(
    work: F,
    timeout: std::time::Duration,
    mut cancel: tokio::sync::watch::Receiver<bool>,
) -> Result<usize>
where
    F: std::future::Future<Output = Result<usize>>,
{
    // Keep the work in this inner scope: select! only drops branch borrows when
    // the future was pinned outside it, which would publish completion too soon.
    tokio::select! {
        biased;
        _ = async {
            loop {
                if *cancel.borrow_and_update() { return; }
                if cancel.changed().await.is_err() { std::future::pending::<()>().await; }
            }
        } => Err("merge execution cancelled".into()),
        result = tokio::time::timeout(timeout, work) =>
            result.unwrap_or_else(|_| Err("merge execution deadline exceeded".into())),
    }
}

#[derive(Clone, Debug, clap::Args)]
pub struct EtcdConfig {
    /// Comma-separated etcd v3 endpoints. Scheduler state (task queue,
    /// lease-based claims, per-experiment write locks) lives in etcd so several
    /// stateless master replicas can share one queue. Required.
    #[arg(long, env = "ETCD_ENDPOINTS", value_delimiter = ',')]
    pub etcd_endpoints: Vec<String>,

    /// Namespace for all lance-context master keys in etcd.
    #[arg(long, env = "ETCD_PREFIX", default_value = "/lance-context/master")]
    pub etcd_prefix: String,

    /// Optional etcd username. `ETCD_PASSWORD` must also be set.
    #[arg(long, env = "ETCD_USERNAME")]
    pub etcd_username: Option<String>,

    /// Optional etcd password. `ETCD_USERNAME` must also be set.
    #[arg(long, env = "ETCD_PASSWORD")]
    pub etcd_password: Option<String>,

    /// Optional PEM CA certificate path for etcd TLS.
    #[arg(long, env = "ETCD_CA_CERT")]
    pub etcd_ca_cert: Option<String>,

    /// Optional PEM client certificate path for etcd mutual TLS.
    #[arg(long, env = "ETCD_CLIENT_CERT")]
    pub etcd_client_cert: Option<String>,

    /// Optional PEM client private-key path for etcd mutual TLS.
    #[arg(long, env = "ETCD_CLIENT_KEY")]
    pub etcd_client_key: Option<String>,
}

impl EtcdConfig {
    pub async fn connect(&self) -> Result<Coordinator> {
        use etcd_client::{Certificate, ConnectOptions, Identity, TlsOptions};
        use std::time::Duration;
        let config = self;
        let mut options = ConnectOptions::new()
            .with_connect_timeout(Duration::from_secs(5))
            .with_timeout(Duration::from_secs(10))
            .with_keep_alive(Duration::from_secs(10), Duration::from_secs(3))
            .with_require_leader(true);
        match (&config.etcd_username, &config.etcd_password) {
            (Some(username), Some(password)) => {
                options = options.with_user(username, password);
            }
            (None, None) => {}
            _ => {
                return Err(String::from(
                    "ETCD_USERNAME and ETCD_PASSWORD must be configured together",
                ))
            }
        }
        if let Some(path) = &config.etcd_ca_cert {
            let pem = std::fs::read(path)
                .map_err(|err| format!("failed to read ETCD_CA_CERT '{path}': {err}"))?;
            let mut tls = TlsOptions::new().ca_certificate(Certificate::from_pem(pem));
            match (&config.etcd_client_cert, &config.etcd_client_key) {
                (Some(cert), Some(key)) => {
                    let cert_pem = std::fs::read(cert).map_err(|err| {
                        format!("failed to read ETCD_CLIENT_CERT '{cert}': {err}")
                    })?;
                    let key_pem = std::fs::read(key)
                        .map_err(|err| format!("failed to read ETCD_CLIENT_KEY '{key}': {err}"))?;
                    tls = tls.identity(Identity::from_pem(cert_pem, key_pem));
                }
                (None, None) => {}
                _ => {
                    return Err(String::from(
                        "ETCD_CLIENT_CERT and ETCD_CLIENT_KEY must be configured together",
                    ))
                }
            }
            options = options.with_tls(tls);
        } else if config.etcd_client_cert.is_some() || config.etcd_client_key.is_some() {
            return Err(String::from(
                "ETCD_CA_CERT is required when configuring an etcd client certificate",
            ));
        }
        let client = Client::connect(config.etcd_endpoints.clone(), Some(options))
            .await
            .map_err(|e| e.to_string())?;

        Ok(Coordinator::new(client, self.etcd_prefix.clone()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{
        sync::{
            atomic::{AtomicBool, Ordering},
            Arc,
        },
        time::Duration,
    };
    use tokio::sync::{oneshot, watch};

    struct InFlight(Arc<AtomicBool>);
    impl Drop for InFlight {
        fn drop(&mut self) {
            self.0.store(false, Ordering::SeqCst);
        }
    }

    #[tokio::test(start_paused = true)]
    async fn deadline_drops_the_writer_before_terminal_acknowledgement() {
        let writing = Arc::new(AtomicBool::new(false));
        let flag = writing.clone();
        let (_cancel, rx) = watch::channel(false);
        let result = execute_scoped(
            async move {
                flag.store(true, Ordering::SeqCst);
                let _guard = InFlight(flag);
                std::future::pending::<Result<usize>>().await
            },
            Duration::from_secs(600),
            rx,
        )
        .await;
        assert!(result.unwrap_err().contains("deadline"));
        assert!(!writing.load(Ordering::SeqCst));
    }

    #[tokio::test]
    async fn disconnected_caller_does_not_detach_uncontrolled_work() {
        let writing = Arc::new(AtomicBool::new(false));
        let flag = writing.clone();
        let (cancel, rx) = watch::channel(false);
        let (entered, began) = oneshot::channel();
        // The HTTP handler owns only admission, never this executor's lifetime.
        let executor = tokio::spawn(execute_scoped(
            async move {
                flag.store(true, Ordering::SeqCst);
                let _guard = InFlight(flag);
                entered.send(()).unwrap();
                std::future::pending::<Result<usize>>().await
            },
            Duration::from_secs(600),
            rx,
        ));
        began.await.unwrap();
        assert!(writing.load(Ordering::SeqCst));
        cancel.send(true).unwrap();
        assert!(executor.await.unwrap().unwrap_err().contains("cancelled"));
        assert!(!writing.load(Ordering::SeqCst));
    }

    #[tokio::test]
    async fn pre_cancelled_request_never_polls_storage() {
        let (cancel, rx) = watch::channel(false);
        cancel.send(true).unwrap();
        let result = execute_scoped(
            async { panic!("must not execute storage") },
            Duration::from_secs(1),
            rx,
        )
        .await;
        assert!(result.is_err());
    }

    // Isolated prefix on a LOCAL test etcd. These tests never use production.
    async fn fixture() -> (Coordinator, Client, ClaimProof, i64) {
        let endpoint = std::env::var("ETCD_TEST_ENDPOINTS")
            .expect("ETCD_TEST_ENDPOINTS is required for ignored etcd tests");
        let mut client = Client::connect([endpoint], None).await.unwrap();
        let prefix = format!("/merge-execution-tests/{}", uuid::Uuid::new_v4());
        let lease = client.lease_grant(30, None).await.unwrap().id();
        let proof = ClaimProof {
            key: format!("{prefix}/claims/test"),
            token: "original".into(),
            lease_id: lease,
        };
        client
            .put(
                target_lock_key(&prefix, "table"),
                proof.token.clone(),
                Some(etcd_client::PutOptions::new().with_lease(lease)),
            )
            .await
            .unwrap();
        client
            .put(
                proof.key.clone(),
                proof.token.clone(),
                Some(etcd_client::PutOptions::new().with_lease(lease)),
            )
            .await
            .unwrap();
        (
            Coordinator::new(client.clone(), prefix),
            client,
            proof,
            lease,
        )
    }

    #[tokio::test]
    #[ignore = "requires isolated local ETCD_TEST_ENDPOINTS"]
    async fn failure_budget_survives_reconnect_and_lost_claim_cannot_clear_it() {
        let (coordinator, mut client, proof, lease) = fixture().await;
        for count in 1..=3 {
            let failure = coordinator
                .record_failure(&proof, "table", "worker", "temporary failure")
                .await
                .unwrap();
            assert_eq!(failure.consecutive_attempts, count);
        }
        let reconnected = Coordinator::new(client.clone(), coordinator.prefix.clone());
        let failure = reconnected
            .failure("table", "worker")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(failure.consecutive_attempts, 3);
        assert!(failure.next_retry_ms > failure.last_failure_ms + 20_000);
        reconnected
            .record_failure(&proof, "table2", "worker", "schema mismatch")
            .await
            .unwrap();
        let (page, next) = reconnected.failure_page(None, 1).await.unwrap();
        assert_eq!(page.len(), 1);
        let (second, end) = reconnected.failure_page(next.as_deref(), 1).await.unwrap();
        assert_eq!(second.len(), 1);
        assert_ne!(page[0].target, second[0].target);
        assert!(end.is_none());
        client.lease_revoke(lease).await.unwrap();
        assert!(reconnected
            .clear_failure(&proof, "table", "worker")
            .await
            .is_err());
        assert!(reconnected
            .record_failure(&proof, "table", "worker", "stale writer")
            .await
            .is_err());
        assert_eq!(
            reconnected
                .failure("table", "worker")
                .await
                .unwrap()
                .unwrap()
                .consecutive_attempts,
            3
        );
        client
            .put(proof.key.clone(), proof.token.clone(), None)
            .await
            .unwrap();
        reconnected
            .clear_failure(&proof, "table", "worker")
            .await
            .unwrap();
        assert!(reconnected
            .failure("table", "worker")
            .await
            .unwrap()
            .is_none());
        client
            .delete(
                coordinator.prefix.clone(),
                Some(etcd_client::DeleteOptions::new().with_prefix()),
            )
            .await
            .unwrap();
    }

    #[tokio::test]
    #[ignore = "requires isolated local ETCD_TEST_ENDPOINTS"]
    async fn revoked_claim_cannot_release_surviving_server_work() {
        let (coordinator, mut client, proof, lease) = fixture().await;
        let operation = Execution::new("table", "worker", "boot", 600);
        assert!(coordinator.reserve(&proof, &operation).await.unwrap());
        let running = coordinator.start(&operation).await.unwrap().unwrap();
        client.lease_revoke(lease).await.unwrap();
        assert_eq!(
            coordinator.get("table").await.unwrap(),
            Some(running.clone())
        );
        let next = ClaimProof {
            key: proof.key.clone(),
            token: "replacement".into(),
            lease_id: 0,
        };
        client
            .put(next.key.clone(), next.token.clone(), None)
            .await
            .unwrap();
        assert!(!coordinator
            .reserve(&next, &Execution::new("table", "worker2", "boot2", 600))
            .await
            .unwrap());
        assert!(coordinator.release(&next, &running).await.is_err());
        assert!(coordinator
            .finish(&running, Err("cancelled".into()))
            .await
            .unwrap());
        let done = coordinator.get("table").await.unwrap().unwrap();
        assert!(!coordinator.release(&proof, &done).await.unwrap());
        assert!(coordinator
            .failure("table", "worker")
            .await
            .unwrap()
            .is_none());
        assert!(coordinator.release(&next, &done).await.unwrap());
        assert_eq!(
            coordinator
                .failure("table", "worker")
                .await
                .unwrap()
                .unwrap()
                .consecutive_attempts,
            1,
            "releasing a failed execution must atomically persist the retry budget"
        );
        assert!(coordinator
            .reserve(&next, &Execution::new("table", "worker2", "boot2", 600))
            .await
            .unwrap());
        client
            .delete(
                coordinator.prefix.clone(),
                Some(etcd_client::DeleteOptions::new().with_prefix()),
            )
            .await
            .unwrap();
    }

    #[tokio::test]
    #[ignore = "requires isolated local ETCD_TEST_ENDPOINTS"]
    async fn delayed_http_start_cannot_resurrect_cancelled_operation() {
        let (coordinator, mut client, proof, lease) = fixture().await;
        let old = Execution::new("table", "worker", "boot", 600);
        assert!(coordinator.reserve(&proof, &old).await.unwrap());
        assert!(coordinator.cancel_reserved(&old).await.unwrap());
        let done = coordinator.get("table").await.unwrap().unwrap();
        assert!(coordinator.release(&proof, &done).await.unwrap());
        let new = Execution::new("table", "worker2", "boot2", 600);
        assert!(coordinator.reserve(&proof, &new).await.unwrap());
        assert!(coordinator.start(&old).await.unwrap().is_none());
        assert_eq!(coordinator.get("table").await.unwrap(), Some(new));
        client.lease_revoke(lease).await.unwrap();
        client
            .delete(
                coordinator.prefix.clone(),
                Some(etcd_client::DeleteOptions::new().with_prefix()),
            )
            .await
            .unwrap();
    }
}
