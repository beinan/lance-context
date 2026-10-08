//! Control-plane (master) library surface.

pub mod admission;
pub mod catchup;
pub mod config;
pub mod demand_publish;
pub mod planner;
pub use lance_context_merge::demand;
pub mod discovery;
pub mod eligibility;
pub mod error;
pub mod executors;
mod maintenance_execution;
mod merge_execution;
mod resident_recovery;
pub mod rollout_append;
pub mod routes;
pub mod scanner;
pub mod scheduler;
pub mod scoring;
pub mod state;
pub mod stats_store;
pub mod task_store;
pub mod wal_tail;
