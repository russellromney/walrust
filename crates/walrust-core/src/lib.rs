//! walrust: SQLite WAL sync to S3-compatible storage.
//!
//! Provides the embeddable primitives for:
//! - WAL parsing and frame extraction
//! - HADBP (hadb-changeset physical) encoding/decoding
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
pub mod snapshot_source;
pub mod sync;
pub mod wal;

// Storage trait (new substrate). Concrete impls (S3, Cinch, local, mem)
// live in separate crates; consumers wire them in.
pub use hadb_storage::StorageBackend;

// Legacy shared infrastructure from hadb-io (retry, S3 helpers). Step i
// of Phase Anvil moves the remaining helpers elsewhere and drops hadb-io.
pub use hadb_io;
pub use hadb_io::s3;
pub use hadb_io::{RetryConfig, RetryPolicy};
pub use hadb_io::{CircuitBreaker, CircuitState, ErrorKind, classify_error, is_retryable};

// Re-export walrust-specific types
pub use sync::{LtxEntry, Manifest, ReplicationConfig, SyncState, restore_with_snapshot_source, run_wal_replication};
pub use replicator::Replicator;
pub use snapshot_source::SnapshotSource;

// Re-export hadb-changeset for consumers
pub use hadb_changeset;
