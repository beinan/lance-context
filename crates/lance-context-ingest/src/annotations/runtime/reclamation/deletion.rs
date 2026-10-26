//! Delete only a caller's finite, already sealed candidate batch.
use super::*;
use object_store::ObjectStore;

pub(super) async fn delete_batch(
    store: Arc<dyn ObjectStore>,
    batch: &[(Path, u64)],
) -> Vec<object_store::Result<u64>> {
    let paths = stream::iter(
        batch
            .iter()
            .map(|(path, _)| Ok(path.clone()))
            .collect::<Vec<_>>(),
    );
    let results = store.delete_stream(paths.boxed()).collect::<Vec<_>>().await;
    // An SDK can return a single request-level error for an entire cloud batch.
    // Never zip that with the first path and report the omitted paths as done.
    if results.len() != batch.len() {
        return vec![Err(results
            .into_iter()
            .find_map(Result::err)
            .unwrap_or_else(|| {
                invalid_response("incomplete bulk delete response")
            }))];
    }
    // ObjectStore's stream contract preserves input order. A per-path 404 is
    // successful reclamation; other errors remain failures without a retry.
    results
        .into_iter()
        .zip(batch)
        .map(|(result, (path, bytes))| match result {
            Ok(actual) if actual == *path => Ok(*bytes),
            Ok(_) => Err(invalid_response("bulk delete returned a different path")),
            Err(object_store::Error::NotFound { .. }) => Ok(*bytes),
            Err(error) => Err(error),
        })
        .collect()
}

fn invalid_response(message: &'static str) -> object_store::Error {
    object_store::Error::Generic {
        store: "annotation collection",
        source: InvalidResponse(message).into(),
    }
}

#[derive(Debug)]
struct InvalidResponse(&'static str);

impl std::fmt::Display for InvalidResponse {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.0)
    }
}
impl std::error::Error for InvalidResponse {}

pub(super) fn invalid(error: &object_store::Error) -> bool {
    matches!(error, object_store::Error::Generic { source, .. } if source.is::<InvalidResponse>())
}

#[cfg(test)]
mod tests;
