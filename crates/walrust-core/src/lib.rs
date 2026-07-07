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
pub mod external_delta;
pub mod legacy_cache;
pub mod legacy_ltx;
pub mod legacy_manifest;
pub mod legacy_replica;
pub mod legacy_restore;
pub mod legacy_uploader;
pub mod ltx;
pub mod replay_sink;
pub mod replicator;
pub mod shadow;
pub mod snapshot_source;
pub mod sync;
pub mod wal;

// `StorageBackend` lives in `hadb-storage`; consumers import it directly.
// No re-export from here — one path per trait keeps the dep graph honest.

// Legacy shared infrastructure from hadb-io (retry, circuit breaker). Step i
// of Phase Anvil moves the remaining helpers elsewhere and drops hadb-io.
pub use hadb_io;
pub use hadb_io::{classify_error, is_retryable, CircuitBreaker, CircuitState, ErrorKind};
pub use hadb_io::{RetryConfig, RetryPolicy};

// Raw AWS SDK helpers — gated behind `s3` so AWS-free builds don't get a
// dangling re-export.
#[cfg(feature = "s3")]
pub use hadb_io::s3;

// Re-export walrust-specific types
pub use replay_sink::PageReplaySink;
pub use replicator::Replicator;
pub use snapshot_source::{SnapshotCheckpoint, SnapshotSource};
pub use sync::{
    fetch_delta_envelope, list_delta_envelopes_after, publish_delta_envelope,
    pull_incremental_into_sink, restore_with_snapshot_source, run_wal_replication,
    sync_wal_fenced_delta, DeltaPublishResult, DiscoveredDelta, ExternalBaseCursor,
    FencedDeltaSyncParams, LtxEntry, Manifest, PullCursor, ReplicationConfig, SnapshotOwnership,
    SyncState,
};

// Fenced delta envelope codec.
pub use external_delta::{DeltaPayloadError, DeltaPayloadV1};

// Re-export hadb-changeset for consumers
pub use hadb_changeset;
