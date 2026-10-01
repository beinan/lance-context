//! Serial worker merges with durable execution ownership and targeted retries.
use crate::{state::MasterState, task_store::TaskClaim};
use lance_context_merge::{ClaimProof, Coordinator, Execution, Phase};
use std::{sync::Arc, time::Duration};

const RPC_TIMEOUT: Duration = Duration::from_secs(10);
const POLL_DELAY: Duration = Duration::from_millis(250);
const RETRY_DELAY: Duration = Duration::from_secs(2);
const ATTEMPTS: usize = 3;

#[derive(serde::Deserialize)]
struct Capabilities {
    protocol: u32,
    instance: String,
    timeout_secs: u64,
}

pub(crate) async fn run_merge_wal(
    state: &Arc<MasterState>,
    claim: &TaskClaim,
) -> Result<String, String> {
    if state.config.worker_endpoints.is_empty() {
        return Err("no worker endpoints configured".into());
    }
    let coordinator = state.task_store.merge_coordinator();
    let proof = state.task_store.merge_claim(claim);
    // Reconcile first, before any index/base-table mutation or new fan-out.
    if let Some(old) = coordinator.get(&claim.task.target).await? {
        let endpoint = old.endpoint.clone();
        if old.phase == Phase::Running {
            if let Some(failure) = coordinator.failure(&claim.task.target, &endpoint).await? {
                if failure.class == lance_context_merge::failure::FailureClass::OwnershipUnresolved
                    && failure.next_retry_ms > lance_context_merge::failure::now_ms()
                {
                    return Err(format!(
                        "merge ownership unresolved; recovery probe at {}; {}",
                        failure.next_retry_ms, failure.last_error
                    ));
                }
            }
        }
        match reconcile(&state.http, &coordinator, &proof, old, true).await {
            Ok(reclaimed) => {
                metrics::counter!("master_merge_wal_generations_reclaimed_total")
                    .increment(reclaimed as u64);
            }
            Err(error) => {
                if coordinator.get(&claim.task.target).await?.is_some() {
                    return Err(error);
                }
                tracing::info!(target = %claim.task.target, %error, "previous merge terminated; resuming shards");
            }
        }
    }
    run_workers(
        &state.http,
        &coordinator,
        &proof,
        &claim.task.target,
        &state.config.worker_endpoints,
    )
    .await
}

async fn run_workers(
    http: &reqwest::Client,
    coordinator: &Coordinator,
    proof: &ClaimProof,
    target: &str,
    endpoints: &[String],
) -> Result<String, String> {
    let mut pending = Vec::new();
    let mut deferred = Vec::new();
    for endpoint in endpoints {
        match coordinator.failure(target, endpoint).await? {
            Some(failure) if failure.next_retry_ms > lance_context_merge::failure::now_ms() => {
                deferred.push(format!(
                    "{endpoint}: retry at {}; {:?}: {}",
                    failure.next_retry_ms, failure.class, failure.last_error
                ))
            }
            _ => pending.push(endpoint.clone()),
        }
    }

    let mut reclaimed = 0;
    let mut errors = Vec::new();
    for attempt in 0..ATTEMPTS {
        if attempt > 0 {
            tokio::time::sleep(RETRY_DELAY).await;
        }
        let mut retry = Vec::new();
        errors.clear();
        for endpoint in pending {
            let failures_before = coordinator
                .failure(target, &endpoint)
                .await?
                .map_or(0, |failure| failure.consecutive_attempts);
            let started = std::time::Instant::now();
            let outcome = one(http, coordinator, proof, target, &endpoint).await;
            metrics::histogram!("master_merge_wal_worker_duration_seconds")
                .record(started.elapsed().as_secs_f64());
            metrics::counter!("master_merge_wal_workers_total", "result" => if outcome.is_ok() { "ok" } else { "failed" }).increment(1);
            match outcome {
                Ok(n) => {
                    reclaimed += n;
                }
                Err(error) => {
                    // An execution still in storage is not a failed endpoint we
                    // may skip. Ownership must first be reconciled to terminal.
                    if coordinator.get(target).await?.is_some() {
                        return Err(error);
                    }
                    // Executed failures are recorded atomically with release.
                    // Capability/admission failures have no execution result.
                    let failure = match coordinator.failure(target, &endpoint).await? {
                        Some(failure) if failure.consecutive_attempts > failures_before => failure,
                        _ => {
                            coordinator
                                .record_failure(proof, target, &endpoint, &error)
                                .await?
                        }
                    };
                    let message = format!(
                        "{endpoint}: {error} (attempt {}; retry at {}; attention={})",
                        failure.consecutive_attempts,
                        failure.next_retry_ms,
                        failure.needs_attention
                    );
                    if failure.consecutive_attempts < ATTEMPTS as u32 && !failure.needs_attention {
                        errors.push(message);
                        retry.push(endpoint);
                    } else {
                        deferred.push(message);
                    }
                }
            }
        }
        pending = retry;
        if pending.is_empty() {
            break;
        }
    }
    metrics::counter!("master_merge_wal_generations_reclaimed_total").increment(reclaimed as u64);
    errors.extend(deferred);
    if !errors.is_empty() {
        return Err(format!("merged {reclaimed} generations; {} worker(s) failed or cooling down (up to {ATTEMPTS} targeted attempts): {}", errors.len(), errors.join("; ")));
    }
    Ok(format!(
        "merged {reclaimed} generations across {}/{} workers",
        endpoints.len(),
        endpoints.len()
    ))
}

async fn one(
    http: &reqwest::Client,
    coordinator: &Coordinator,
    proof: &ClaimProof,
    target: &str,
    endpoint: &str,
) -> Result<usize, String> {
    let base = format!(
        "{}/api/v1/internal/merge-executor",
        endpoint.trim_end_matches('/')
    );
    let capabilities: Capabilities = http
        .get(&base)
        .timeout(RPC_TIMEOUT)
        .send()
        .await
        .map_err(|e| e.to_string())?
        .error_for_status()
        .map_err(|e| e.to_string())?
        .json()
        .await
        .map_err(|e| e.to_string())?;
    if capabilities.protocol != 1 || capabilities.timeout_secs == 0 {
        return Err("worker lacks bounded owned-merge protocol".into());
    }
    let execution = Execution::new(
        target,
        endpoint,
        &capabilities.instance,
        capabilities.timeout_secs.min(600),
    );
    // Even a lost reserve response is ambiguous. Recover the fence before
    // issuing any other storage operation; never infer absence from an error.
    let admission = coordinator.reserve(proof, &execution).await;
    match admission {
        Ok(true) => {}
        _ => {
            if let Some(current) = coordinator.get(target).await? {
                if current.id == execution.id {
                    return reconcile(http, coordinator, proof, current, true).await;
                }
            }
            return Err("merge admission failed or task claim lost".into());
        }
    }
    let started = http
        .post(format!("{base}/start"))
        .timeout(RPC_TIMEOUT)
        .json(&execution)
        .send()
        .await;
    let cancel = !matches!(started, Ok(ref response) if response.status().is_success());
    // A transport error can mean the worker is already writing. It is NOT an
    // outcome, and cannot let this task advance to another endpoint.
    reconcile(http, coordinator, proof, execution, cancel).await
}

async fn reconcile(
    http: &reqwest::Client,
    coordinator: &Coordinator,
    proof: &ClaimProof,
    initial: Execution,
    cancel_immediately: bool,
) -> Result<usize, String> {
    reconcile_with_grace(
        http,
        coordinator,
        proof,
        initial,
        cancel_immediately,
        Duration::from_secs(60),
    )
    .await
}

async fn reconcile_with_grace(
    http: &reqwest::Client,
    coordinator: &Coordinator,
    proof: &ClaimProof,
    initial: Execution,
    cancel_immediately: bool,
    grace: Duration,
) -> Result<usize, String> {
    let deadline =
        tokio::time::Instant::now() + Duration::from_secs(initial.timeout_secs) + RPC_TIMEOUT;
    let handoff_deadline = if cancel_immediately {
        tokio::time::Instant::now()
    } else {
        deadline
    } + grace;
    let mut next_cancel = tokio::time::Instant::now();
    loop {
        if tokio::time::Instant::now() >= handoff_deadline {
            let error = "merge ownership unresolved: executor did not acknowledge termination; fence retained, recovery requires old writer termination evidence";
            coordinator
                .record_failure(proof, &initial.target, &initial.endpoint, error)
                .await?;
            return Err(error.into());
        }
        let current = match coordinator.get(&initial.target).await {
            Ok(Some(current)) if current.id == initial.id => current,
            Ok(_) => return Err("merge execution ownership changed during reconciliation".into()),
            Err(error) => {
                tracing::warn!(target = %initial.target, %error, "cannot confirm merge completion; retaining execution fence");
                tokio::time::sleep(RETRY_DELAY).await;
                continue;
            }
        };
        if current.phase == Phase::Finished {
            if !coordinator.release(proof, &current).await? {
                return Err("task claim lost while releasing terminal merge execution".into());
            }
            return current.error.map_or(Ok(current.reclaimed), Err);
        }
        if (cancel_immediately || tokio::time::Instant::now() >= deadline)
            && tokio::time::Instant::now() >= next_cancel
        {
            next_cancel = tokio::time::Instant::now() + RETRY_DELAY;
            // CAS reserved work to terminal, or ask its owner to cancel the
            // actual storage future. HTTP success alone is not acknowledgement.
            if current.phase == Phase::Reserved {
                let _ = coordinator.cancel_reserved(&current).await;
            }
            let _ = http
                .post(format!(
                    "{}/api/v1/internal/merge-executor/cancel",
                    current.endpoint.trim_end_matches('/')
                ))
                .timeout(RPC_TIMEOUT)
                .json(&current)
                .send()
                .await;
        }
        tokio::time::sleep(POLL_DELAY).await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::{
        extract::State,
        routing::{get, post},
        Json, Router,
    };
    use std::sync::{
        atomic::{AtomicUsize, Ordering},
        Mutex,
    };
    use tokio::sync::watch;

    #[derive(Clone)]
    struct Worker {
        coordinator: Coordinator,
        calls: Arc<AtomicUsize>,
        fail: bool,
        stall_first: bool,
        name: &'static str,
        events: Arc<Mutex<Vec<String>>>,
    }

    async fn worker(worker: Worker) -> (String, tokio::task::JoinHandle<()>) {
        let app = Router::new()
            .route(
                "/api/v1/internal/merge-executor",
                get(|| async {
                    Json(serde_json::json!({"protocol":1,"instance":"test","timeout_secs":1}))
                }),
            )
            .route(
                "/api/v1/internal/merge-executor/start",
                post(
                    |State(w): State<Worker>, Json(e): Json<Execution>| async move {
                        let running = w.coordinator.start(&e).await.unwrap().unwrap();
                        tokio::spawn(async move {
                            let call = w.calls.fetch_add(1, Ordering::SeqCst);
                            let (_cancel, rx) = watch::channel(false);
                            let result = lance_context_merge::execute_scoped(
                                async {
                                    if w.stall_first && call == 0 {
                                        return std::future::pending().await;
                                    }
                                    if w.fail {
                                        Err("injected shard failure".into())
                                    } else {
                                        Ok(7)
                                    }
                                },
                                Duration::from_secs(1),
                                rx,
                            )
                            .await;
                            w.events.lock().unwrap().push(format!(
                                "{}:{}",
                                w.name,
                                if result.is_ok() { "ok" } else { "failed" }
                            ));
                            assert!(w.coordinator.finish(&running, result).await.unwrap());
                        });
                        axum::http::StatusCode::ACCEPTED
                    },
                ),
            )
            .with_state(worker);
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = format!("http://{}", listener.local_addr().unwrap());
        (
            address,
            tokio::spawn(async move {
                axum::serve(listener, app).await.unwrap();
            }),
        )
    }

    async fn fixture() -> (Coordinator, ClaimProof) {
        let endpoint = std::env::var("ETCD_TEST_ENDPOINTS").expect("isolated local etcd required");
        let mut client = etcd_client::Client::connect([endpoint], None)
            .await
            .unwrap();
        let prefix = format!("/merge-fanout-tests/{}", Execution::new("", "", "", 1).id);
        let lease = client.lease_grant(120, None).await.unwrap().id();
        let proof = ClaimProof {
            key: format!("{prefix}/claim"),
            token: "owner".into(),
            lease_id: lease,
        };
        client
            .put(
                lance_context_merge::target_lock_key(&prefix, "table"),
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
        (Coordinator::new(client, prefix), proof)
    }

    #[tokio::test]
    #[ignore = "requires isolated local ETCD_TEST_ENDPOINTS"]
    async fn missing_executor_returns_attention_without_unlocking_surviving_write() {
        let (coordinator, proof) = fixture().await;
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = format!("http://{}", listener.local_addr().unwrap());
        let server =
            tokio::spawn(async move { axum::serve(listener, Router::new()).await.unwrap() });
        let execution = Execution::new("table", &endpoint, "lost-process", 1);
        assert!(coordinator.reserve(&proof, &execution).await.unwrap());
        let running = coordinator.start(&execution).await.unwrap().unwrap();
        let error = tokio::time::timeout(
            Duration::from_secs(3),
            reconcile_with_grace(
                &reqwest::Client::new(),
                &coordinator,
                &proof,
                running.clone(),
                true,
                Duration::from_millis(50),
            ),
        )
        .await
        .unwrap()
        .unwrap_err();
        assert!(error.contains("ownership unresolved"));
        assert_eq!(
            coordinator.get("table").await.unwrap(),
            Some(running.clone())
        );
        assert!(!coordinator
            .reserve(&proof, &Execution::new("table", "replacement", "new", 1))
            .await
            .unwrap());
        assert!(
            coordinator
                .failure("table", &endpoint)
                .await
                .unwrap()
                .unwrap()
                .needs_attention
        );
        // A late positive acknowledgement still permits normal recovery.
        assert!(coordinator
            .finish(&running, Err("cancelled".into()))
            .await
            .unwrap());
        let terminal = coordinator.get("table").await.unwrap().unwrap();
        assert!(coordinator.release(&proof, &terminal).await.unwrap());
        server.abort();
    }

    #[tokio::test]
    #[ignore = "requires isolated local ETCD_TEST_ENDPOINTS"]
    async fn stalled_shard_is_terminated_healthy_shards_advance_then_only_failure_retries() {
        let (coordinator, proof) = fixture().await;
        let events = Arc::new(Mutex::new(Vec::new()));
        let bad = Worker {
            coordinator: coordinator.clone(),
            calls: Arc::new(AtomicUsize::new(0)),
            fail: false,
            stall_first: true,
            name: "stalled",
            events: events.clone(),
        };
        let good = Worker {
            stall_first: false,
            name: "healthy",
            calls: Arc::new(AtomicUsize::new(0)),
            ..bad.clone()
        };
        let (first, first_server) = worker(bad.clone()).await;
        let (second, second_server) = worker(good.clone()).await;
        let result = tokio::time::timeout(
            Duration::from_secs(20),
            run_workers(
                &reqwest::Client::new(),
                &coordinator,
                &proof,
                "table",
                &[first, second],
            ),
        )
        .await
        .unwrap()
        .unwrap();
        assert!(result.contains("merged 14"));
        assert_eq!(bad.calls.load(Ordering::SeqCst), 2);
        assert_eq!(good.calls.load(Ordering::SeqCst), 1);
        assert_eq!(
            *events.lock().unwrap(),
            ["stalled:failed", "healthy:ok", "stalled:ok"]
        );
        assert!(coordinator.get("table").await.unwrap().is_none());
        first_server.abort();
        second_server.abort();
    }

    #[tokio::test]
    #[ignore = "requires isolated local ETCD_TEST_ENDPOINTS"]
    async fn partial_success_does_not_hide_exhausted_shard_failure() {
        let (coordinator, proof) = fixture().await;
        let bad = Worker {
            coordinator: coordinator.clone(),
            calls: Arc::new(AtomicUsize::new(0)),
            fail: true,
            stall_first: false,
            name: "bad",
            events: Arc::new(Mutex::new(Vec::new())),
        };
        let good = Worker {
            fail: false,
            name: "good",
            calls: Arc::new(AtomicUsize::new(0)),
            ..bad.clone()
        };
        let (first, first_server) = worker(bad.clone()).await;
        let (second, second_server) = worker(good.clone()).await;
        let result = tokio::time::timeout(
            Duration::from_secs(25),
            run_workers(
                &reqwest::Client::new(),
                &coordinator,
                &proof,
                "table",
                &[first.clone(), second.clone()],
            ),
        )
        .await
        .unwrap()
        .unwrap_err();
        assert!(result.contains("merged 7 generations"));
        assert!(result.contains("1 worker(s) failed or cooling down"));
        assert_eq!(bad.calls.load(Ordering::SeqCst), 3);
        assert_eq!(good.calls.load(Ordering::SeqCst), 1);
        // A fresh task must retain the failed shard's backoff, while still
        // admitting the healthy shard for newly arrived generations.
        let next = run_workers(
            &reqwest::Client::new(),
            &coordinator,
            &proof,
            "table",
            &[first.clone(), second],
        )
        .await
        .unwrap_err();
        assert!(next.contains("retry at"));
        assert_eq!(bad.calls.load(Ordering::SeqCst), 3);
        assert_eq!(good.calls.load(Ordering::SeqCst), 2);
        assert_eq!(
            coordinator
                .failure("table", &first)
                .await
                .unwrap()
                .unwrap()
                .consecutive_attempts,
            3
        );
        assert!(coordinator.get("table").await.unwrap().is_none());
        first_server.abort();
        second_server.abort();
    }
}
