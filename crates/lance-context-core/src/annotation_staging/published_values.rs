//! Reuse already-published narrow values within the existing session cache.
use super::*;
use lance::cache::{CacheKey, LanceCache};
use lance_core::deepsize::{Context, DeepSizeOf};
use std::borrow::Cow;

#[derive(Clone)]
pub(super) struct PublishedValues(Option<LanceCache>);

pub(super) struct Cached {
    pub value: TurnAnnotation,
    through: u64,
    charge: usize,
}

impl DeepSizeOf for Cached {
    fn deep_size_of_children(&self, _: &mut Context) -> usize {
        // Includes conservative string, key and collection-node overhead.
        self.charge
    }
}

struct Key<'a>(&'a str);

impl CacheKey for Key<'_> {
    type ValueType = Cached;

    fn key(&self) -> Cow<'_, str> {
        Cow::Borrowed(self.0)
    }

    fn type_name() -> &'static str {
        "lance-context.published-annotation"
    }
}

impl PublishedValues {
    pub fn new(base: &Dataset) -> Self {
        let session = base.session();
        let cache = session.file_metadata_cache();
        // A custom backend without a reported byte bound is not a suitable
        // value cache. Do not allocate a new budget or guess its capacity.
        Self(cache.capacity_bytes().filter(|bytes| *bytes > 0).map(|_| {
            // Separate owners/views can share a Session and even a URI. Only
            // this staging instance may update these current-base values.
            cache.with_key_prefix(&format!("annotation-values/{}", uuid::Uuid::new_v4()))
        }))
    }

    pub async fn get(&self, turn: &str) -> Option<Arc<Cached>> {
        match &self.0 {
            Some(cache) => cache.get_with_key(&Key(turn)).await,
            None => None,
        }
    }

    pub async fn get_at(&self, turn: &str, materialized: u64) -> Option<Arc<Cached>> {
        let value = self.get(turn).await?;
        // Warming precedes the base switch, and an older read may resolve after
        // later publications. Never substitute a future value for its base.
        (value.through <= materialized).then_some(value)
    }

    pub async fn remember(&self, values: &Values, through: u64) -> Result<()> {
        let Some(cache) = &self.0 else {
            return Ok(());
        };
        for (turn, stored) in values {
            cache
                .insert_with_key(
                    &Key(turn),
                    Arc::new(Cached {
                        charge: charge(turn, &stored.value)?,
                        value: stored.value.clone(),
                        through,
                    }),
                )
                .await;
        }
        Ok(())
    }
}
