//! Control-plane (master) library surface.

pub mod config;
pub mod discovery;
pub mod error;
mod merge_execution;
pub mod routes;
pub mod scanner;
pub mod scheduler;
pub mod state;
pub mod stats_store;
pub mod task_store;
