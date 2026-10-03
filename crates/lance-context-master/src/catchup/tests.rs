use super::*;
use clap::Parser;
use lance_context_api::TaskKind;
use serde_json::json;

fn config() -> MasterConfig {
    let mut config = MasterConfig::parse_from(["master"]);
    config.catchup.enabled = true;
    config.catchup.shards = vec!["worker-0".into(), "worker-1".into()];
    config.merge_rollout.owned_targets = vec!["hot".into(), "other".into()];
    config
}
fn row(now: i64) -> StatRow {
    StatRow {
        name: "hot".into(),
        uri: "/hot".into(),
        row_count: 0,
        fragment_count: 0,
        last_updated: now,
        pending_wal_generations: 1000,
        last_compaction: -1,
        total_compactions: 0,
        scanned_at: now,
        version: 1,
    }
}
fn template() -> serde_json::Value {
    json!({"containers":[{"name":"catchup","image":format!("example/native@sha256:{}", "a".repeat(64)),"command":["/usr/local/bin/lance-context-master"],"resources":{"requests":{"cpu":"2","memory":"8Gi"},"limits":{"cpu":"2","memory":"8Gi"}}}]})
}
#[test]
fn admission_requires_fresh_owned_pressure_even_for_manual_requests() {
    let mut c = config();
    let now = 10_000_000;
    let mut r = row(now);
    assert!(eligibility(&c, Some(&r), "hot", now).is_none());
    assert_eq!(
        eligibility(&c, Some(&r), "legacy", now),
        Some("requires_owned_target")
    );
    r.scanned_at = now - 901_000;
    assert_eq!(eligibility(&c, Some(&r), "hot", now), Some("stale_stats"));
    r.scanned_at = now + 1;
    assert_eq!(eligibility(&c, Some(&r), "hot", now), Some("stale_stats"));
    r.scanned_at = now;
    r.pending_wal_generations = 0;
    assert_eq!(
        eligibility(&c, Some(&r), "hot", now),
        Some("below_threshold")
    );
    c.catchup.enabled = false;
    assert_eq!(eligibility(&c, Some(&r), "hot", now), Some("disabled"));
}
#[test]
fn job_is_native_and_bounded_and_overrides_unsafe_inherited_mode() {
    let c = config();
    let r = Record {
        target: "hot".into(),
        job: "lc-catchup-test".into(),
        slot: 0,
        active: true,
        reason: "test".into(),
        pending_at_admission: 1000,
        created_at_ms: 0,
        finished_at_ms: None,
        consecutive_failures: 0,
        needs_attention: false,
        next_retry_ms: 0,
        outcome: None,
    };
    let mut pod = template();
    pod["containers"][0]["env"] = json!([{"name":"CATCHUP_ENABLED","value":"true"}]);
    let job = kubernetes::render_job(&c, &r, pod);
    assert_eq!(job["spec"]["parallelism"], 1);
    assert_eq!(job["spec"]["backoffLimit"], 0);
    assert_eq!(job["spec"]["template"]["spec"]["restartPolicy"], "Never");
    assert_eq!(
        job["spec"]["template"]["spec"]["automountServiceAccountToken"],
        false
    );
    let env = job["spec"]["template"]["spec"]["containers"][0]["env"]
        .as_array()
        .unwrap();
    assert_eq!(
        env.iter()
            .filter(|e| e["name"] == "CATCHUP_ENABLED")
            .count(),
        1
    );
    assert_eq!(
        env.iter().find(|e| e["name"] == "CATCHUP_ENABLED").unwrap()["value"],
        "false"
    );
}
async fn fixture() -> Option<(tempfile::TempDir, Arc<MasterState>)> {
    let endpoints = std::env::var("ETCD_TEST_ENDPOINTS").expect("ETCD_TEST_ENDPOINTS is required");
    let dir = tempfile::tempdir().unwrap();
    let mut c = config();
    c.data_dir = dir.path().to_string_lossy().into();
    c.etcd.etcd_endpoints = endpoints.split(',').map(str::to_string).collect();
    c.etcd.etcd_prefix = format!("/catchup-test/{}", lance_context_core::generate_id());
    c.catchup.max_jobs = 1;
    let file = dir.path().join("pod.json");
    std::fs::write(&file, template().to_string()).unwrap();
    c.catchup.pod_template = Some(file.to_string_lossy().into());
    Some((dir, MasterState::new(c).await.unwrap()))
}
#[tokio::test]
#[ignore = "requires ETCD_TEST_ENDPOINTS"]
async fn replicas_share_one_slot_and_dedupe_duplicate_requests() {
    let Some((_dir, state)) = fixture().await else {
        return;
    };
    let a = Inventory::new(&state);
    let b = Inventory::new(&state);
    let (one, two) = tokio::join!(
        a.reserve("hot", "test", 1000, 100),
        b.reserve("hot", "retry", 1000, 100)
    );
    assert_eq!(
        [one.unwrap(), two.unwrap()]
            .iter()
            .filter(|d| d.decision == "reserved")
            .count(),
        1
    );
    assert_eq!(
        a.reserve("other", "test", 1000, 100)
            .await
            .unwrap()
            .decision,
        "capacity_exhausted"
    );
    assert_eq!(a.active().await.unwrap().len(), 1);
    let record = a.get("hot").await.unwrap().unwrap();
    b.complete(&record, false, 1000).await.unwrap();
    let failed = a.get("hot").await.unwrap().unwrap();
    assert_eq!(failed.consecutive_failures, 1);
    assert!(!failed.active);
    assert_ne!(
        a.reserve("hot", "retry", 1000, 1001)
            .await
            .unwrap()
            .decision,
        "reserved"
    );
    assert_eq!(
        a.reserve("other", "test", 1000, 1001)
            .await
            .unwrap()
            .decision,
        "reserved"
    );
    // Replaying an old completion cannot release the new table's slot.
    b.complete(&record, true, 2000).await.unwrap();
    assert_eq!(a.active().await.unwrap()[0].target, "other");
}
#[tokio::test]
#[ignore = "requires ETCD_TEST_ENDPOINTS"]
async fn dedicated_claim_excludes_normal_pollers_but_not_other_tables() {
    let Some((_dir, state)) = fixture().await else {
        return;
    };
    let inventory = Inventory::new(&state);
    let reserved = inventory.reserve("hot", "test", 1000, 100).await.unwrap();
    state
        .task_store
        .enqueue(TaskKind::MergeWal, "hot", vec![])
        .await
        .unwrap();
    state
        .task_store
        .enqueue(TaskKind::MergeWal, "other", vec![])
        .await
        .unwrap();
    let ordinary = state.task_store.claim_next().await.unwrap().unwrap();
    assert_eq!(ordinary.task.target, "other");
    assert!(state
        .task_store
        .claim_merge_target("hot", "wrong-job")
        .await
        .unwrap()
        .is_none());
    let dedicated = state
        .task_store
        .claim_merge_target("hot", reserved.job.as_ref().unwrap())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(dedicated.task.target, "hot");
    assert!(state.task_store.claim_next().await.unwrap().is_none());
    state
        .task_store
        .finish(dedicated, Ok("test".into()))
        .await
        .unwrap();
    state
        .task_store
        .finish(ordinary, Ok("test".into()))
        .await
        .unwrap();
}
#[tokio::test]
#[ignore = "requires ETCD_TEST_ENDPOINTS"]
async fn running_native_task_prevents_provisioning_empty_waiters() {
    let Some((_dir, state)) = fixture().await else {
        return;
    };
    state
        .task_store
        .enqueue(TaskKind::MergeWal, "hot", vec![])
        .await
        .unwrap();
    let claim = state.task_store.claim_next().await.unwrap().unwrap();
    let result = Inventory::new(&state)
        .reserve("hot", "test", 1000, 100)
        .await
        .unwrap();
    assert_ne!(result.decision, "reserved");
    state
        .task_store
        .finish(claim, Ok("test".into()))
        .await
        .unwrap();
}
#[tokio::test]
#[ignore = "requires ETCD_TEST_ENDPOINTS"]
async fn policy_disagreement_cannot_expand_the_cluster_budget() {
    let Some((_dir, state)) = fixture().await else {
        return;
    };
    let inventory = Inventory::new(&state);
    inventory
        .ensure_policy(&state.config.catchup)
        .await
        .unwrap();
    let mut other = state.config.catchup.clone();
    other.max_jobs += 1;
    assert!(inventory.ensure_policy(&other).await.is_err());
}

#[tokio::test]
#[ignore = "requires ETCD_TEST_ENDPOINTS"]
async fn native_executor_preserves_live_ingestion_and_drains_sealed_shards() {
    use lance_context_core::{
        ColumnSpec, ColumnType, GenericStore, GenericStoreOptions, SchemaSpec,
    };
    let (_dir, state) = fixture().await.unwrap();
    let target = "generic:hot";
    let uri = state.generic_uri("hot");
    let spec = SchemaSpec::new(vec![
        (
            "id".into(),
            ColumnSpec::required(ColumnType::String { large: false }),
        ),
        (
            "text".into(),
            ColumnSpec::new(ColumnType::String { large: true }),
        ),
    ]);
    let mut writers = Vec::new();
    for shard in ["worker-0", "worker-1"] {
        let writer = GenericStore::open(
            &uri,
            spec.clone(),
            GenericStoreOptions {
                shard_id: Some(shard.into()),
                merge_after_generations: Some(0),
                ..Default::default()
            },
        )
        .await
        .unwrap();
        for generation in 0..4 {
            let row = json!({"id":format!("{shard}-{generation}"),"text":"x".repeat(512 * 1024)})
                .as_object()
                .unwrap()
                .clone();
            writer.add(&[row]).await.unwrap();
            writer.flush().await.unwrap();
        }
        writers.push(writer);
    }
    assert_eq!(writers[0].pending_wal_generations().await.unwrap(), 8);
    let mut cfg = state.config.clone();
    cfg.catchup.enabled = false;
    cfg.merge_rollout.owned_targets.push(target.into());
    let decision = Inventory::new(&state)
        .reserve(target, "integration", 8, 100)
        .await
        .unwrap();
    cfg.catchup.target = Some(target.into());
    cfg.catchup.job_name = decision.job;
    let (executed, ()) = tokio::join!(execute(cfg, target), async {
        for n in 0..8 {
            for (i, writer) in writers.iter().enumerate() {
                writer
                    .add(
                        &[json!({"id":format!("during-{i}-{n}"),"text":"continuous"})
                            .as_object()
                            .unwrap()
                            .clone()],
                    )
                    .await
                    .unwrap();
                writer.flush().await.unwrap();
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    });
    executed.unwrap();
    // The old ingestion handles remain usable; the executor must not claim
    // a writer epoch or seal somebody else's active memtable.
    for (i, writer) in writers.iter().enumerate() {
        writer
            .add(&[json!({"id":format!("after-{i}"),"text":"live"})
                .as_object()
                .unwrap()
                .clone()])
            .await
            .unwrap();
        writer.flush().await.unwrap();
    }
    let reader = GenericStore::open_existing(&uri, GenericStoreOptions::default())
        .await
        .unwrap();
    assert!(reader.pending_wal_generations().await.unwrap() <= 18);
    let rows = reader.list(None, None).await.unwrap();
    assert_eq!(rows.len(), 26);
    let ids: std::collections::HashSet<_> =
        rows.iter().map(|r| r["id"].as_str().unwrap()).collect();
    assert_eq!(ids.len(), 26);
    assert!(state
        .task_store
        .merge_coordinator()
        .get(target)
        .await
        .unwrap()
        .is_none());
}

#[tokio::test]
#[ignore = "requires ETCD_TEST_ENDPOINTS"]
async fn repeated_failed_jobs_keep_backoff_across_controller_restarts() {
    let (_dir, state) = fixture().await.unwrap();
    let mut now = 1000;
    for attempt in 1..=4 {
        let inventory = Inventory::new(&state);
        assert_eq!(
            inventory
                .reserve("hot", "test", 1000, now)
                .await
                .unwrap()
                .decision,
            "reserved"
        );
        let record = inventory.get("hot").await.unwrap().unwrap();
        inventory.complete(&record, false, now + 1).await.unwrap();
        let persisted = Inventory::new(&state).get("hot").await.unwrap().unwrap();
        assert_eq!(persisted.consecutive_failures, attempt);
        assert_eq!(persisted.needs_attention, attempt >= 3);
        assert!(persisted.next_retry_ms > now + 60_000);
        now = persisted.next_retry_ms;
    }
}
