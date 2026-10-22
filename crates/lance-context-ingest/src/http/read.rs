//! Session results use Arrow IPC streams, with one output batch buffered at a time.
use super::*;
use crate::{materializer::Storage, reader};
use arrow_ipc::writer::StreamWriter;
use axum::body::Body;
use futures::{stream, TryStreamExt};

#[derive(Clone)]
struct Reads {
    gateway: Gateway,
    storage: Arc<BTreeMap<String, Storage>>,
}

pub(super) fn router(gateway: Gateway, storage: BTreeMap<String, Storage>) -> Router {
    Router::new()
        .route("/api/v1/ingest/{table}/sessions/{session}", get(session))
        .with_state(Reads {
            gateway,
            storage: Arc::new(storage),
        })
}

async fn session(
    State(reads): State<Reads>,
    Path((table, session)): Path<(String, String)>,
) -> HttpResult<Response> {
    let guard = reads.gateway.enter(true)?;
    let ingress = reads.gateway.0.tables.get(&table);
    let storage = reads.storage.get(&table);
    let (Some(ingress), Some(storage)) = (ingress, storage) else {
        return Ok(StatusCode::NOT_FOUND.into_response());
    };
    let read = tokio::time::timeout(
        reads.gateway.0.options.body_total_timeout,
        reader::read_session(ingress, storage, &session),
    )
    .await
    .map_err(|_| Failure::unavailable("session_read_timeout", "not_submitted"))?
    .map_err(|_| Failure::unavailable("session_read_unavailable", "not_submitted"))?;
    let writer = StreamWriter::try_new(Vec::new(), &ingress.schema)
        .map_err(|_| Failure::unavailable("session_encoding_failed", "not_submitted"))?;
    let batches = stream::try_unfold(
        Some((writer, read.batches, Arc::new(guard))),
        |state| async move {
            let Some((mut writer, mut batches, guard)) = state else {
                return Ok::<_, anyhow::Error>(None);
            };
            let batch = batches.try_next().await?;
            let finished = batch.is_none();
            let keep = guard.clone();
            let (writer, bytes, batches) = tokio::task::spawn_blocking(move || {
                let _keep = keep;
                if let Some(batch) = batch {
                    writer.write(&batch)?;
                } else {
                    writer.finish()?;
                }
                let bytes = Bytes::from(std::mem::take(writer.get_mut()));
                // Keep the read stream's reservation with the encoder even
                // if the HTTP client disconnects during encoding.
                Ok::<_, anyhow::Error>((writer, bytes, batches))
            })
            .await??;
            let next = (!finished).then_some((writer, batches, guard));
            Ok(Some((bytes, next)))
        },
    );
    Ok((
        [(header::CONTENT_TYPE, "application/vnd.apache.arrow.stream")],
        Body::from_stream(batches),
    )
        .into_response())
}
