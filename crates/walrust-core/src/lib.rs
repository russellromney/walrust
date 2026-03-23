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
pub mod shadow;
pub mod sync;
pub mod wal;

// Shared infrastructure from hadb-io (retry, S3, storage)
pub use hadb_io;
pub use hadb_io::s3;
pub use hadb_io::ObjectStore as StorageBackend;
pub use hadb_io::S3Backend;
pub use hadb_io::{RetryConfig, RetryPolicy};
pub use hadb_io::{CircuitBreaker, CircuitState, ErrorKind, classify_error, is_retryable};

// Re-export walrust-specific types
pub use sync::{LtxEntry, Manifest, ReplicationConfig, SyncState};
pub use replicator::Replicator;
