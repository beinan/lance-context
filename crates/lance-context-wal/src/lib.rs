//! Independent Rust WAL experiments owned by lance-context.
//!
//! The optional `lab` feature builds Arrow IPC and raft-engine component
//! benchmarks. It is not connected to production ingestion and makes no
//! distributed durability or end-to-end Lance throughput claim.
//! `placement` exposes immutable session routing and scoped receipt types.

#[cfg(feature = "placement")]
pub mod placement;

#[cfg(feature = "admission")]
pub mod admission;

#[cfg(feature = "ipc")]
pub mod ipc;

#[cfg(feature = "record")]
pub mod record;
