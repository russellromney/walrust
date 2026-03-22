//! walrust: SQLite WAL sync to S3-compatible storage.
//!
//! Provides the embeddable primitives for:
//! - WAL parsing and frame extraction
//! - LTX (Litestream Transaction) encoding/decoding
//! - S3-compatible storage backend
//! - Shadow WAL for decoupled frame uploads
//! - Sync operations: WAL sync, snapshots, restore
//!
//! This crate is designed to be embedded as a library (not run as a sidecar).
//! The caller controls scheduling and lifecycle.

pub mod errors;
pub mod ltx;
pub mod replicator;
pub mod retry;
pub mod s3;
pub mod shadow;
pub mod storage;
pub mod sync;
pub mod wal;

// Re-export key types for convenience
pub use storage::{S3Backend, StorageBackend};
pub use sync::{LtxEntry, Manifest, ReplicationConfig, SyncState};
pub use replicator::Replicator;
pub use retry::{RetryConfig, RetryPolicy};
