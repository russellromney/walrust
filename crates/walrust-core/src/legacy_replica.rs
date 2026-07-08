//! Legacy LTX read-replica apply engine.
//!
//! Root CLI replica polling still owns S3 discovery and local state, but the
//! file mutation invariant lives here: never mutate the live replica unless
//! decode/apply, fsync, integrity check, and atomic publish all succeed.

use anyhow::{anyhow, Result};
use std::fs::{self, File};
use std::io::Cursor;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use crate::legacy_ltx;

fn fsync_parent_dir(path: &Path) -> Result<()> {
    let parent = path.parent().unwrap_or_else(|| Path::new("."));
    File::open(parent)
        .map_err(|e| {
            anyhow!(
                "failed to open directory {} for fsync: {e}",
                parent.display()
            )
        })?
        .sync_all()
        .map_err(|e| anyhow!("failed to fsync directory {}: {e}", parent.display()))
}

fn sync_file(path: &Path) -> Result<()> {
    File::open(path)
        .map_err(|e| anyhow!("failed to open {} for fsync: {e}", path.display()))?
        .sync_all()
        .map_err(|e| anyhow!("failed to fsync {}: {e}", path.display()))
}

fn verify_sqlite_integrity(path: &Path) -> Result<()> {
    let conn = rusqlite::Connection::open(path)
        .map_err(|e| anyhow!("failed to open replica database for integrity_check: {e}"))?;
    let result: String = conn
        .query_row("PRAGMA integrity_check", [], |row| row.get(0))
        .map_err(|e| anyhow!("failed to run integrity_check on replica database: {e}"))?;
    if result != "ok" {
        return Err(anyhow!("replica database failed integrity_check: {result}"));
    }
    Ok(())
}

struct AtomicReplicaFile {
    path: PathBuf,
    published: bool,
}

impl AtomicReplicaFile {
    fn new(target: &Path) -> Self {
        static NEXT_ID: AtomicU64 = AtomicU64::new(0);

        let parent = target.parent().unwrap_or_else(|| Path::new("."));
        let file_name = target
            .file_name()
            .and_then(|name| name.to_str())
            .unwrap_or("replica.db");
        let id = NEXT_ID.fetch_add(1, Ordering::Relaxed);
        let path = parent.join(format!(
            ".{file_name}.replica-{}-{id}.tmp",
            std::process::id()
        ));
        Self {
            path,
            published: false,
        }
    }

    fn path(&self) -> &Path {
        &self.path
    }

    fn copy_from(target: &Path) -> Result<Self> {
        let staged = Self::new(target);
        fs::copy(target, &staged.path).map_err(|e| {
            anyhow!(
                "failed to stage replica copy from {} to {}: {e}",
                target.display(),
                staged.path.display()
            )
        })?;
        sync_file(&staged.path)?;
        fsync_parent_dir(&staged.path)?;
        Ok(staged)
    }

    fn publish(mut self, target: &Path) -> Result<()> {
        sync_file(&self.path)?;
        fs::rename(&self.path, target).map_err(|e| {
            anyhow!(
                "failed to atomically publish replica database {} over {}: {e}",
                self.path.display(),
                target.display()
            )
        })?;
        fsync_parent_dir(target)?;
        self.published = true;
        Ok(())
    }
}

impl Drop for AtomicReplicaFile {
    fn drop(&mut self) {
        if !self.published {
            let _ = fs::remove_file(&self.path);
        }
    }
}

/// Decode a legacy LTX snapshot into a staged replica and publish atomically.
pub fn bootstrap_from_snapshot_bytes(
    ltx_data: &[u8],
    local: &Path,
) -> Result<legacy_ltx::DecodeResult> {
    let staged = AtomicReplicaFile::new(local);
    let decode_result = legacy_ltx::decode_to_db(Cursor::new(ltx_data), staged.path())?;
    sync_file(staged.path())?;
    verify_sqlite_integrity(staged.path())?;
    staged.publish(local)?;
    Ok(decode_result)
}

/// Apply a legacy LTX incremental to a staged copy and publish atomically.
pub fn apply_incremental_atomically(
    ltx_data: &[u8],
    local: &Path,
) -> Result<legacy_ltx::ApplyResult> {
    let staged = AtomicReplicaFile::copy_from(local)?;
    let apply_result = legacy_ltx::apply_ltx_to_db(Cursor::new(ltx_data), staged.path())?;
    sync_file(staged.path())?;
    verify_sqlite_integrity(staged.path())?;
    staged.publish(local)?;
    Ok(apply_result)
}
