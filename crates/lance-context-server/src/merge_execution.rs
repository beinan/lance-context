//! HTTP handlers only admit/cancel work. The owned executor, not the connection,
//! holds the durable execution fence until the scoped storage future is gone.
use crate::{
    error::AppError,
    routes::{generic, rollouts},
    state::AppState,
};
use axum::{extract::State, http::StatusCode, Json};
use futures::FutureExt;
use lance_context_merge::{execute_scoped, Coordinator, Execution, Phase};
use std::{collections::HashMap, sync::Arc, time::Duration};
use tokio::sync::{watch, Mutex};

pub struct Executions {
    coordinator: Option<Coordinator>,
    instance: String,
    timeout_secs: u64,
    running: Mutex<HashMap<String, watch::Sender<bool>>>,
}

impl Executions {
    pub fn new(coordinator: Option<Coordinator>, timeout_secs: u64) -> Self {
        Self {
            coordinator,
            instance: uuid::Uuid::new_v4().to_string(),
            timeout_secs,
            running: Mutex::new(HashMap::new()),
        }
    }
    pub(crate) fn enabled(&self) -> bool {
        self.coordinator.is_some()
    }

    /// Invoked after HTTP admission has stopped. Detached executors are not
    /// drained by axum's connection shutdown.
    pub(crate) async fn shutdown(&self) {
        for cancel in self.running.lock().await.values() {
            let _ = cancel.send(true);
        }
        while !self.running.lock().await.is_empty() {
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }

    fn coordinator(&self) -> Result<Coordinator, AppError> {
        self.coordinator.clone().ok_or_else(|| {
            AppError::Overloaded("owned merge execution requires ETCD_ENDPOINTS".into())
        })
    }
}

pub async fn capabilities(
    State(state): State<Arc<AppState>>,
) -> Result<Json<serde_json::Value>, AppError> {
    state.merge_executions.coordinator()?;
    Ok(Json(
        serde_json::json!({"protocol": 1, "instance": state.merge_executions.instance,
        "timeout_secs": state.merge_executions.timeout_secs}),
    ))
}

pub async fn start(
    State(state): State<Arc<AppState>>,
    Json(execution): Json<Execution>,
) -> Result<StatusCode, AppError> {
    let coordinator = state.merge_executions.coordinator()?;
    if execution.instance != state.merge_executions.instance
        || execution.phase != Phase::Reserved
        || execution.timeout_secs == 0
        || execution.timeout_secs > state.merge_executions.timeout_secs
    {
        return Err(AppError::InvalidRequest(
            "merge executor incarnation or deadline mismatch".into(),
        ));
    }
    let name = execution
        .target
        .strip_prefix("generic:")
        .unwrap_or(&execution.target);
    lance_context_core::validate_store_name(name).map_err(AppError::InvalidRequest)?;
    let mut running = state.merge_executions.running.lock().await;
    if running.contains_key(&execution.id) {
        return Ok(StatusCode::ACCEPTED);
    }
    let (cancel, cancelled) = watch::channel(false);
    running.insert(execution.id.clone(), cancel);
    // Spawn before any fallible admission I/O. Losing the HTTP connection must
    // never leave a Running fence without an executor (or cancel a live write).
    let owned = state.clone();
    tokio::spawn(async move {
        let result = run(owned.clone(), coordinator, execution.clone(), cancelled).await;
        if let Err(error) = result {
            tracing::error!(id = %execution.id, target = %execution.target, %error, "merge execution ownership unresolved");
        }
        owned
            .merge_executions
            .running
            .lock()
            .await
            .remove(&execution.id);
    });
    Ok(StatusCode::ACCEPTED)
}

async fn run(
    state: Arc<AppState>,
    coordinator: Coordinator,
    execution: Execution,
    cancelled: watch::Receiver<bool>,
) -> Result<(), String> {
    // CAS errors are ambiguous: never retry storage work. Reconciliation sees
    // the durable fence, so an uncertain admission cannot release ownership.
    let running = loop {
        match coordinator.start(&execution).await {
            Ok(Some(running)) => break running,
            Ok(None) => match coordinator.get(&execution.target).await {
                Ok(Some(current))
                    if current.id == execution.id && current.phase == Phase::Running =>
                {
                    break current
                }
                Ok(_) => return Ok(()),
                Err(_) => {}
            },
            Err(error) => {
                tracing::warn!(id = %execution.id, %error, "reconciling uncertain execution admission")
            }
        }
        tokio::time::sleep(Duration::from_secs(1)).await;
    };
    let target = execution.target.clone();
    let mut slot = None;
    let work = async {
        slot = state.acquire_merge_slot().await;
        let result = if let Some(name) = target.strip_prefix("generic:") {
            generic::merge_generic_wal_owned(
                State(state.clone()),
                axum::extract::Path(name.to_string()),
            )
            .await
            .map(|Json(reply)| reply["reclaimed"].as_u64().unwrap_or(0) as usize)
        } else {
            rollouts::merge_wal_owned(State(state.clone()), axum::extract::Path(target.clone()))
                .await
                .map(|Json(reply)| reply.reclaimed)
        };
        match result {
            Ok(n) => Ok(n),
            Err(AppError::NotFound(ref message))
                if message == &format!("Rollout store '{}' does not exist", target)
                    || message
                        == &format!(
                            "Generic store '{}' does not exist",
                            target.strip_prefix("generic:").unwrap_or(&target)
                        ) =>
            {
                Ok(0)
            }
            Err(e) => Err(format!("{e:?}")),
        }
    };
    let write_scope = lance_context_core::merge_write_scope::MergeWriteScope::new();
    let outcome = std::panic::AssertUnwindSafe(execute_scoped(
        write_scope.run(work),
        Duration::from_secs(execution.timeout_secs),
        cancelled,
    ))
    .catch_unwind()
    .await
    .unwrap_or_else(|_| Err("merge executor panicked".into()));
    // Top-level cancellation cannot acknowledge a write still in flight at
    // object storage. Join those leaf commits before publishing Finished.
    write_scope.drain().await;
    // Do not admit another memory-heavy merge into this slot while a
    // cancelled execution is still joining storage commits.
    drop(slot);
    // Storage work has ended. An etcd outage may delay acknowledgement, but
    // cannot erase the fence or cause the write to be executed twice.
    loop {
        match coordinator.finish(&running, outcome.clone()).await {
            Ok(true) => break,
            Ok(false) => {
                if coordinator.get(&execution.target).await?.as_ref()
                    == Some(&running.finished(outcome.clone()))
                {
                    break;
                }
                return Err("merge execution fence changed unexpectedly".into());
            }
            Err(error) => {
                tracing::warn!(id = %execution.id, %error, "retrying merge terminal acknowledgement")
            }
        }
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
    Ok(())
}

pub async fn cancel(
    State(state): State<Arc<AppState>>,
    Json(execution): Json<Execution>,
) -> Result<StatusCode, AppError> {
    let coordinator = state.merge_executions.coordinator()?;
    if coordinator
        .cancel_reserved(&execution)
        .await
        .map_err(AppError::Internal)?
    {
        return Ok(StatusCode::OK);
    }
    if let Some(cancel) = state
        .merge_executions
        .running
        .lock()
        .await
        .get(&execution.id)
    {
        let _ = cancel.send(true);
        return Ok(StatusCode::ACCEPTED);
    }
    // Absence from the local map is NOT proof that the old process stopped.
    // The caller must reconcile the durable result, not interpret HTTP 404 as
    // permission for a competing write.
    Err(AppError::NotFound(
        "execution is not owned by this worker incarnation".into(),
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use lance_context_merge::{ClaimProof, EtcdConfig};
    use tokio::io::AsyncWriteExt;

    #[tokio::test]
    #[ignore = "requires isolated local ETCD_TEST_ENDPOINTS"]
    async fn disconnected_http_call_is_cancelled_before_ownership_handoff() {
        let endpoints = std::env::var("ETCD_TEST_ENDPOINTS").unwrap();
        let prefix = format!("/server-merge-test/{}", uuid::Uuid::new_v4());
        let coordinator = EtcdConfig {
            etcd_endpoints: endpoints.split(',').map(str::to_string).collect(),
            etcd_prefix: prefix.clone(),
            etcd_username: None,
            etcd_password: None,
            etcd_ca_cert: None,
            etcd_client_cert: None,
            etcd_client_key: None,
        }
        .connect()
        .await
        .unwrap();
        // Test-only claim creation through the isolated etcd connection.
        let mut client =
            etcd_client::Client::connect(endpoints.split(',').collect::<Vec<_>>(), None)
                .await
                .unwrap();
        let proof = ClaimProof {
            key: format!("{prefix}/claim"),
            token: "owner".into(),
            lease_id: 0,
        };
        client
            .put(proof.key.clone(), proof.token.clone(), None)
            .await
            .unwrap();
        client
            .put(
                lance_context_merge::target_lock_key(&prefix, "blocked"),
                proof.token.clone(),
                None,
            )
            .await
            .unwrap();
        let dir = tempfile::tempdir().unwrap();
        let mut state = AppState::new_for_test(dir.path().to_path_buf()).await;
        state.merge_executions = Executions::new(Some(coordinator.clone()), 600);
        let state = Arc::new(state);
        let name = "blocked";
        // Create via the real API so registry and resident handle agree.
        let _ = rollouts::create_rollout_store(
            State(state.clone()),
            Json(lance_context_api::CreateRolloutStoreRequest {
                name: name.into(),
                storage_options: None,
            }),
        )
        .await
        .unwrap();
        let store = state.get_or_open_rollout_store(name).await.unwrap();
        let held = store.write().await;
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let app = crate::routes::router().with_state(state.clone());
        let server = tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        let execution = Execution::new(
            name,
            &format!("http://{address}"),
            &state.merge_executions.instance,
            600,
        );
        assert!(coordinator.reserve(&proof, &execution).await.unwrap());
        let body = serde_json::to_string(&execution).unwrap();
        let mut connection = tokio::net::TcpStream::connect(address).await.unwrap();
        connection.write_all(format!("POST /api/v1/internal/merge-executor/start HTTP/1.1\r\nHost: {address}\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\r\n{body}", body.len()).as_bytes()).await.unwrap();
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                if coordinator.get(name).await.unwrap().unwrap().phase == Phase::Running {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();
        drop(connection);
        assert_eq!(
            coordinator.get(name).await.unwrap().unwrap().phase,
            Phase::Running
        );
        assert!(!coordinator
            .reserve(&proof, &Execution::new(name, "other", "other", 1))
            .await
            .unwrap());
        cancel(State(state.clone()), Json(execution.clone()))
            .await
            .unwrap();
        let finished = tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                let current = coordinator.get(name).await.unwrap().unwrap();
                if current.phase == Phase::Finished {
                    break current;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();
        assert!(finished.error.as_ref().unwrap().contains("cancelled"));
        assert!(coordinator.release(&proof, &finished).await.unwrap());
        drop(held);
        let next = Execution::new(name, &execution.endpoint, &execution.instance, 30);
        assert!(coordinator.reserve(&proof, &next).await.unwrap());
        start(State(state.clone()), Json(next)).await.unwrap();
        let done = tokio::time::timeout(Duration::from_secs(30), async {
            loop {
                let current = coordinator.get(name).await.unwrap().unwrap();
                if current.phase == Phase::Finished {
                    break current;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();
        assert!(done.error.is_none(), "retry must run: {done:?}");
        assert!(coordinator.release(&proof, &done).await.unwrap());
        server.abort();
        client.delete(proof.key, None).await.unwrap();
    }
}
