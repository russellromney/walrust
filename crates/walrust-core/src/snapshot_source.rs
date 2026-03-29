//! Pluggable snapshot source for restore/recovery.
//!
//! By default, walrust restores from LTX snapshot files stored in S3.
//! When a `SnapshotSource` is provided, walrust calls `materialize()` instead
//! of downloading an LTX snapshot. This enables integration with external
//! page-group storage (e.g., turbolite) where page groups serve as the snapshot.
//!
//! Usage:
//! ```ignore
//! // Standard restore (LTX snapshots from S3):
//! restore(storage, prefix, db_name, output, None, None).await?;
//!
//! // With external snapshot source (turbolite):
//! restore(storage, prefix, db_name, output, None, Some(&turbolite_source)).await?;
//! ```

use anyhow::Result;
use async_trait::async_trait;
use std::path::Path;

/// A source that can materialize a base database file for WAL replay.
///
/// The `restore()` function uses this trait to produce the base DB instead
/// of downloading an LTX snapshot. After materialization, walrust applies
/// any WAL increments (LTX files) with txid > the returned checkpoint version.
///
/// Implementors should produce a valid SQLite database file at the output path
/// representing the state at the last checkpoint. The returned version number
/// is used to determine which WAL increments to apply on top.
#[async_trait]
pub trait SnapshotSource: Send + Sync {
    /// Materialize the database at the latest checkpoint to a local file.
    ///
    /// Writes a complete SQLite database to `output`. Returns the checkpoint
    /// version (e.g., manifest version) so walrust knows which WAL increments
    /// are newer and need to be applied.
    ///
    /// This replaces the LTX snapshot download step in `restore()`.
    async fn materialize(&self, output: &Path) -> Result<u64>;

    /// Return the current checkpoint version without materializing.
    ///
    /// Used to check if WAL increments exist before doing a full materialization.
    async fn checkpoint_version(&self) -> Result<u64>;
}
