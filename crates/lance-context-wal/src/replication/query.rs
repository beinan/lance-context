//! Bounded read-index queries with an application view captured at completion.
//! A result describes committed state during the request; it is not a lease,
//! authorization token or permission to write an external Lance manifest.
use super::*;

#[cfg(test)]
mod tests;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum Query {
    Publisher {
        table: Uuid,
    },
    Accepted {
        after: u64,
        limit: usize,
    },
    Producer {
        table: Uuid,
        bucket: u32,
        producer: Uuid,
    },
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum Value {
    Publisher(Option<Box<publication::Table>>),
    Accepted(Page),
    Producer(Option<(BatchMeta, u64)>),
    CursorAhead { applied: u64 },
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Cut {
    pub cluster: Uuid,
    pub layout: [u8; 32],
    pub group: u64,
    pub term: u64,
    pub quorum_index: u64,
    /// The query and its cursor are captured together after this position applies.
    pub applied: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum Outcome {
    Observed {
        cut: Cut,
        value: Value,
    },
    /// No read value was released before leadership changed. Retry at a leader.
    Unknown,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Completion {
    pub group: u64,
    pub request: u64,
    pub outcome: Outcome,
}

impl Query {
    pub(super) fn validate(&self, layout: &Layout, group: u64) -> Result<()> {
        match self {
            Self::Publisher { table } => ensure!(
                !table.is_nil() && group == publication::control_group(layout),
                "invalid publisher query scope"
            ),
            Self::Accepted { after, limit } => ensure!(
                *after < u64::MAX && (1..=128).contains(limit),
                "invalid accepted query bounds"
            ),
            Self::Producer {
                table,
                bucket,
                producer,
            } => ensure!(
                !table.is_nil() && !producer.is_nil() && layout.group_for_bucket(*bucket)? == group,
                "invalid producer query scope"
            ),
        }
        Ok(())
    }

    fn evaluate(&self, state: &State) -> Result<Value> {
        Ok(match self {
            Self::Publisher { table } => {
                Value::Publisher(state.catalog.tables.get(table).cloned().map(Box::new))
            }
            Self::Accepted { after, .. } if *after > state.applied => Value::CursorAhead {
                applied: state.applied,
            },
            Self::Accepted { after, limit } => Value::Accepted(state.page(*after, *limit)?),
            Self::Producer {
                table,
                bucket,
                producer,
            } => Value::Producer(state.producers.get(&(*table, *bucket, *producer)).cloned()),
        })
    }
}

struct Pending {
    request: u64,
    term: u64,
    query: Query,
    confirmed: Option<u64>,
}

pub(super) struct Tracker {
    incarnation: Uuid,
    next: u64,
    pending: BTreeMap<[u8; 32], Pending>,
}

impl Default for Tracker {
    fn default() -> Self {
        Self {
            incarnation: Uuid::new_v4(),
            next: 0,
            pending: BTreeMap::new(),
        }
    }
}

impl Tracker {
    pub fn len(&self) -> usize {
        self.pending.len()
    }
    pub fn contains(&self, request: u64) -> bool {
        self.pending.values().any(|p| p.request == request)
    }
    pub fn enqueue(&mut self, request: u64, term: u64, query: Query) -> Result<Vec<u8>> {
        self.next = self.next.checked_add(1).context("read context exhausted")?;
        let mut context = [0; 32];
        context[..8].copy_from_slice(b"LCREAD01");
        context[8..24].copy_from_slice(self.incarnation.as_bytes());
        context[24..].copy_from_slice(&self.next.to_le_bytes());
        ensure!(
            self.pending
                .insert(
                    context,
                    Pending {
                        request,
                        term,
                        query,
                        confirmed: None
                    }
                )
                .is_none(),
            "reused read context"
        );
        Ok(context.to_vec())
    }
    pub fn confirm(&mut self, context: &[u8], index: u64) -> Result<()> {
        let Ok(key) = <[u8; 32]>::try_from(context) else {
            return Ok(());
        };
        let Some(pending) = self.pending.get_mut(&key) else {
            return Ok(());
        };
        ensure!(
            index > 0 && index < u64::MAX,
            "invalid confirmed read index"
        );
        ensure!(
            pending.confirmed.is_none_or(|old| old == index),
            "read index changed"
        );
        pending.confirmed = Some(index);
        Ok(())
    }
    pub fn finish(&mut self, cut: Cut, state: &State, leader: bool) -> Result<Vec<Completion>> {
        let done: Vec<_> = self
            .pending
            .iter()
            .filter_map(|(key, p)| {
                (!leader
                    || p.term != cut.term
                    || p.confirmed.is_some_and(|index| index <= state.applied))
                .then_some(*key)
            })
            .collect();
        let mut results = Vec::with_capacity(done.len());
        for key in done {
            let p = self.pending.remove(&key).expect("selected pending read");
            let outcome = if !leader || p.term != cut.term {
                Outcome::Unknown
            } else {
                Outcome::Observed {
                    cut: Cut {
                        quorum_index: p.confirmed.expect("confirmed completed read"),
                        ..cut.clone()
                    },
                    value: p.query.evaluate(state)?,
                }
            };
            results.push(Completion {
                group: cut.group,
                request: p.request,
                outcome,
            });
        }
        Ok(results)
    }
}
