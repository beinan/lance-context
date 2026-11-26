//! Slow cold apply must not prevent reading already-installed annotations.
use super::*;
use crate::preparation_io::test_support::{Throttle, ThrottleWrapper};
use lance::{dataset::builder::DatasetBuilder, dataset::ReadParams};
use lance_io::object_store::ObjectStoreParams;
use std::{sync::atomic::Ordering, time::Duration};

#[tokio::test]
async fn cold_apply_keeps_active_and_frozen_reads_available() -> Result<()> {
    paused_apply(false).await
}

#[tokio::test]
async fn cancelled_cold_apply_keeps_values_and_releases_mutation_gate() -> Result<()> {
    paused_apply(true).await
}

async fn paused_apply(cancel: bool) -> Result<()> {
    let root = tempfile::tempdir()?;
    let path = root.path().join("turns.lance");
    fixture(&path).await?;
    let throttle = Throttle::new(Duration::ZERO);
    let base = Arc::new(
        DatasetBuilder::from_uri(format!("file-object-store://{}", path.display()))
            .with_read_params(ReadParams {
                store_options: Some(ObjectStoreParams {
                    object_store_wrapper: Some(Arc::new(ThrottleWrapper(throttle.clone()))),
                    ..Default::default()
                }),
                ..Default::default()
            })
            .load()
            .await?,
    );
    let staging = Arc::new(AnnotationStaging::new(base, scope(), 0, Limits::default())?);
    staging
        .apply(
            0,
            1,
            "T",
            &TurnAnnotationPatch {
                status: Set("reviewed".into()),
                ..Default::default()
            },
        )
        .await?;
    let frozen = staging.freeze(uuid::Uuid::new_v4()).await?.unwrap();
    staging
        .apply(
            1,
            2,
            "U",
            &TurnAnnotationPatch {
                score: Set(0.7),
                ..Default::default()
            },
        )
        .await?;
    let workspace = crate::MergeMemoryBudget::new(8 << 20);
    let prepared = frozen.prepare(&workspace, 8192).await?;
    let selected = commit(&prepared).await?;
    let expected_t = current(&staging, "T").await?;
    let expected_u = current(&staging, "U").await?;

    let operations = throttle.ops.load(Ordering::Relaxed);
    throttle.frozen.store(true, Ordering::Relaxed);
    let apply = tokio::spawn({
        let staging = staging.clone();
        async move {
            let score = TurnAnnotationPatch {
                score: Set(0.9),
                ..Default::default()
            };
            staging
                .apply_batch(
                    2,
                    3,
                    &[
                        Mutation {
                            turn: "U",
                            patch: &score,
                        },
                        Mutation {
                            turn: "o'hara",
                            patch: &score,
                        },
                    ],
                )
                .await
        }
    });
    tokio::time::timeout(Duration::from_secs(5), async {
        while throttle.ops.load(Ordering::Relaxed) == operations {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .map_err(|_| invalid("cold apply did not enter actual data IO"))?;
    assert!(!apply.is_finished());

    // A queued state writer must not block subsequent readers behind cold IO.
    // Poll it directly so the check cannot pass merely because it wasn't scheduled.
    let advance = staging.advance_processed(3, 4);
    tokio::pin!(advance);
    assert!(futures::poll!(&mut advance).is_pending());
    let reads = tokio::time::timeout(Duration::from_secs(1), async {
        let (t, u) = tokio::try_join!(staging.capture("T"), staging.capture("U"))?;
        assert_eq!(t.applied, 2);
        assert_eq!(u.applied, 2);
        assert_eq!(t.resolve().await?.unwrap(), expected_t);
        assert_eq!(u.resolve().await?.unwrap(), expected_u);
        assert_eq!(staging.observation().await.applied, 2);
        let protected = staging.protected_bases().await?;
        assert_eq!(protected.datasets().count(), 1);
        // The root snapshot operation also must not queue an exclusive reader gate.
        assert_eq!(current(&staging, "T").await?, expected_t);
        Ok::<_, Error>((t, u))
    })
    .await;

    if cancel {
        apply.abort();
    }
    throttle.frozen.store(false, Ordering::Relaxed);
    let applied = apply.await;
    // Always release the injected stall and join apply before asserting failure.
    let held = reads.map_err(|_| invalid("installed point reads blocked behind cold apply"))??;
    if cancel {
        assert!(applied.unwrap_err().is_cancelled());
        assert!(
            advance.await.is_err(),
            "cancelled apply must not advance its cut"
        );
        assert_eq!(staging.observation().await.applied, 2);
        assert_eq!(current(&staging, "U").await?, expected_u);
        assert_eq!(current(&staging, "o'hara").await?.score, None);
        staging.advance_processed(2, 3).await?;
    } else {
        applied.map_err(|e| invalid(e.to_string()))??;
        advance.await?;
        assert_eq!(current(&staging, "U").await?.score, Some(0.9));
        assert_eq!(current(&staging, "o'hara").await?.score, Some(0.9));
        // Handles captured during the blocked apply retain the earlier values.
        assert_eq!(held.0.resolve().await?.unwrap(), expected_t);
        assert_eq!(held.1.resolve().await?.unwrap(), expected_u);
    }
    staging.install_published(&prepared, selected).await?;
    assert_eq!(current(&staging, "T").await?, expected_t);
    drop((held, prepared, frozen));
    let suffix = staging.freeze(uuid::Uuid::new_v4()).await?.unwrap();
    let prepared = suffix.prepare(&workspace, 8192).await?;
    staging
        .install_published(&prepared, commit(&prepared).await?)
        .await?;
    drop((prepared, suffix));
    let done = staging.observation().await;
    assert_eq!(done.materialized, if cancel { 3 } else { 4 });
    assert_eq!(done.retained_bytes, 0);
    assert_eq!(done.retained_entries, 0);
    Ok(())
}
