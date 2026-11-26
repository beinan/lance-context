use super::*;
use std::path::{Path, PathBuf};

fn files(root: &Path) -> Result<BTreeSet<PathBuf>> {
    let mut result = BTreeSet::new();
    for entry in std::fs::read_dir(root.join("_indices"))? {
        for name in ["page_lookup.lance", "page_data.lance"] {
            let file = entry.as_ref().unwrap().path().join(name);
            if file.exists() {
                result.insert(file);
            }
        }
    }
    Ok(result)
}

async fn referenced(base: &Dataset, root: &Path) -> Result<BTreeSet<PathBuf>> {
    Ok(base
        .load_indices()
        .await?
        .iter()
        .flat_map(|i| {
            ["page_lookup.lance", "page_data.lance"]
                .map(|name| root.join("_indices").join(i.uuid.to_string()).join(name))
        })
        .collect())
}

async fn wait_files(root: &Path, expected: &BTreeSet<PathBuf>) -> Result<()> {
    tokio::time::timeout(Duration::from_secs(30), async {
        while &files(root)? != expected {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        Ok::<_, anyhow::Error>(())
    })
    .await?
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn mixed_status_index_gc_keeps_old_reader_then_reclaims_withdrawn_files() -> Result<()> {
    tokio::time::timeout(Duration::from_secs(90), retained_reader()).await?
}

async fn retained_reader() -> Result<()> {
    let root = artifact_root()?;
    let path = root.path().join("annotations.lance");
    let c = network::Cluster::open_signed(root.path(), annotation_publication::one_shard()).await?;
    let narrow = storage(root.path(), "annotations.lance").await?;
    let body = storage(root.path(), "body.lance").await?;
    seed_with_binding(&c, &narrow, &body, compaction::compact_binding()).await?;
    let (a, runtime) = indexed_owner(&c, &narrow, &body, 4).await?;
    let runtime = runtime.with_exclusive_reclamation(service::runtime::reclamation::Options {
        max_objects: 2,
        max_bytes: 16 << 10,
        delete_parallelism: 2,
        interval: Duration::from_millis(10),
    })?;
    let (stop, task) = annotation_runtime::run_owner(runtime);
    let creator = producer(&a, 1).await?;
    let patcher = producer(&a, 2).await?;
    let input = request(
        &a,
        &creator,
        1,
        encoded(create(&["turn-0"], &["s"], &[0])?)?,
    )
    .await?;
    let result = a.submit(&input, Duration::from_secs(10)).await?;
    annotation_runtime::drained(&c, &a, result.index).await?;
    let base = caught_up(&c, &narrow, 1).await?;
    let old = referenced(&base, &path).await?;
    let mut previous_status = base
        .load_indices()
        .await?
        .iter()
        .find(|i| i.name == NAME)
        .unwrap()
        .uuid;
    let held = a.capture_read_for_test("turn-0").await?;
    let held_filter = a.capture_status_for_test(Some("pending".into())).await?;
    assert_eq!(held.base_version, base.version_id());
    drop(base);
    for sequence in 1..=8 {
        let status = if sequence % 2 == 1 {
            "reviewed"
        } else {
            "pending"
        };
        let input = request(
            &a,
            &patcher,
            sequence,
            encoded(mixed::wire::patches(
                &status_patch(Some(status))?,
                &limits(),
            )?)?,
        )
        .await?;
        let result = a.submit(&input, Duration::from_secs(10)).await?;
        assert_eq!(result.result, service::MutationResult::Applied);
        assert_eq!(
            a.get("turn-0", Duration::from_secs(10))
                .await?
                .unwrap()
                .status
                .as_deref(),
            Some(status)
        );
        annotation_runtime::drained(&c, &a, result.index).await?;
        let current = caught_up(&c, &narrow, 1).await?;
        matches_scan(&current, Some(status)).await?;
        let current_status = current
            .load_indices()
            .await?
            .iter()
            .find(|i| i.name == NAME)
            .unwrap()
            .uuid;
        assert_ne!(
            current_status, previous_status,
            "changed status did not replace its sole native segment"
        );
        previous_status = current_status;
    }
    let base = caught_up(&c, &narrow, 1).await?;
    let live = referenced(&base, &path).await?;
    assert!(
        !old.is_subset(&live),
        "old status files were never replaced"
    );
    // Recent selected versions still protect their status files. Wait for the
    // configured history bound, then include those exact roots in the expected
    // set rather than incorrectly requiring immediate deletion at publication.
    let manifests = tokio::time::timeout(Duration::from_secs(30), async {
        loop {
            let state = annotation_publication::state(&c).await?;
            let manifests = state.manifests.unwrap();
            if manifests.versions.len() <= 4 && state.progress.pending.is_none() {
                return Ok::<_, anyhow::Error>(manifests);
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await??;
    assert!(!manifests.versions.contains_key(&held.base_version));
    let mut retained = BTreeSet::new();
    for version in manifests
        .versions
        .keys()
        .filter(|v| **v != manifests.initial_version)
    {
        let selected = annotation_publication::read(&c, &narrow, Some(*version)).await?;
        retained.extend(referenced(&selected, &path).await?);
    }
    assert!(live.is_subset(&retained));
    assert!(
        old.difference(&retained).next().is_some(),
        "old status is still rooted"
    );
    let expected = retained.union(&old).cloned().collect();
    wait_files(&path, &expected)
        .await
        .context("collect all unrooted status files while reader is held")?;
    assert_eq!(
        held.resolve().await?.unwrap().status.as_deref(),
        Some("pending")
    );
    assert!(old.iter().all(|file| file.exists()));
    drop(held);
    // A materialized filter is also a real reader. Releasing the point read
    // must not withdraw the filter's old native data/index roots.
    let filtered = held_filter.resolve().await?;
    assert_eq!(filtered.ids, vec!["turn-0"]);
    assert!(old.iter().all(|file| file.exists()));
    drop(held_filter);
    wait_files(&path, &retained)
        .await
        .context("collect retired status files after reader release")?;
    assert!(old.difference(&retained).all(|file| !file.exists()));
    check(&a, "turn-0", "s", 0, "pending", None).await?;
    stop.send(()).unwrap();
    task.await??;
    c.shutdown().await
}
