//! Restore for the legacy Litestream-derived LTX object layout.

use anyhow::{anyhow, Result};
use hadb_storage::StorageBackend;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use crate::legacy_ltx;
use crate::legacy_manifest::{
    discover_legacy_state, find_latest_legacy_snapshot, find_latest_legacy_snapshot_at_or_before,
    list_legacy_generation_files, GENERATION_LIVE,
};

struct AtomicRestore {
    path: PathBuf,
    published: bool,
}

impl AtomicRestore {
    fn new(output: &Path) -> Self {
        static NEXT_ID: AtomicU64 = AtomicU64::new(0);

        let parent = output.parent().unwrap_or_else(|| Path::new("."));
        let file_name = output
            .file_name()
            .and_then(|name| name.to_str())
            .unwrap_or("restored.db");
        let id = NEXT_ID.fetch_add(1, Ordering::Relaxed);
        let path = parent.join(format!(
            ".{file_name}.legacy-restore-{}-{id}.tmp",
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

    fn publish(mut self, output: &Path) -> Result<()> {
        std::fs::rename(&self.path, output).map_err(|e| {
            anyhow!(
                "failed to atomically publish restored database {} over {}: {e}",
                self.path.display(),
                output.display()
            )
        })?;
        self.published = true;
        Ok(())
    }
}

impl Drop for AtomicRestore {
    fn drop(&mut self) {
        if !self.published {
            let _ = std::fs::remove_file(&self.path);
        }
    }
}

fn verify_sqlite_integrity(path: &Path) -> Result<()> {
    let conn = rusqlite::Connection::open(path)
        .map_err(|e| anyhow!("failed to open restored database for integrity_check: {e}"))?;
    let result: String = conn
        .query_row("PRAGMA integrity_check", [], |row| row.get(0))
        .map_err(|e| anyhow!("failed to run integrity_check on restored database: {e}"))?;
    if result != "ok" {
        return Err(anyhow!(
            "restored database failed integrity_check: {result}"
        ));
    }
    Ok(())
}

fn restore_reached_target(final_txid: u64, target_txid: u64, explicit_pit: bool) -> Result<()> {
    if explicit_pit {
        if final_txid > target_txid {
            return Err(anyhow!(
                "restore overshot: reached TXID {final_txid} but point-in-time target is \
                 {target_txid} (no snapshot/chain at or before the requested point)"
            ));
        }
        return Ok(());
    }
    if final_txid != target_txid {
        return Err(anyhow!(
            "restore incomplete: reached TXID {final_txid} but latest is {target_txid}. \
             An incremental object is missing or the chain has a gap; the restored \
             database does not reflect the latest committed state."
        ));
    }
    Ok(())
}

async fn read_required_object(storage: &dyn StorageBackend, key: &str) -> Result<Vec<u8>> {
    storage
        .get(key)
        .await?
        .ok_or_else(|| anyhow!("missing legacy LTX object: {key}"))
}

/// Restore a database from the legacy LTX object layout.
///
/// Returns the final restored TXID.
pub async fn restore_legacy_ltx(
    storage: &dyn StorageBackend,
    prefix: &str,
    db_name: &str,
    output: &Path,
    point_in_time: Option<u64>,
) -> Result<u64> {
    let (current_txid, _max_generation) = discover_legacy_state(storage, prefix, db_name).await?;
    if current_txid == 0 {
        return Err(anyhow!("No LTX files found for database: {db_name}"));
    }

    let target_txid = point_in_time.unwrap_or(current_txid);
    let snapshot = if point_in_time.is_some() {
        find_latest_legacy_snapshot_at_or_before(storage, prefix, db_name, target_txid).await?
    } else {
        find_latest_legacy_snapshot(storage, prefix, db_name).await?
    }
    .ok_or_else(|| {
        if point_in_time.is_some() {
            anyhow!("No snapshot found for database {db_name} at or before TXID {target_txid}")
        } else {
            anyhow!("No snapshot found for database: {db_name}")
        }
    })?;

    tracing::info!(
        "Restoring from LTX snapshot: {} (TXID: {}-{}, generation: {})",
        snapshot.key,
        snapshot.min_txid,
        snapshot.max_txid,
        snapshot.generation
    );

    let snapshot_bytes = read_required_object(storage, &snapshot.key).await?;
    let staged_restore = AtomicRestore::new(output);
    let staged_output = staged_restore.path();

    let decode_result =
        legacy_ltx::decode_to_db(std::io::Cursor::new(snapshot_bytes), staged_output)?;
    let mut final_txid = snapshot.max_txid;
    let mut expected_pre_checksum = decode_result.post_apply_checksum;

    let incrementals =
        list_legacy_generation_files(storage, prefix, db_name, GENERATION_LIVE).await?;
    let applicable: Vec<_> = incrementals
        .iter()
        .filter(|(_, min_txid, max_txid)| *min_txid > snapshot.max_txid && *max_txid <= target_txid)
        .collect();

    for (key, min_txid, max_txid) in applicable {
        let expected_min = final_txid + 1;
        if *min_txid != expected_min {
            return Err(anyhow!(
                "restore incremental gap: expected next TXID {expected_min}, got \
                 {min_txid}-{max_txid} at {key}"
            ));
        }

        let bytes = read_required_object(storage, key).await?;
        let apply_result = legacy_ltx::apply_ltx_to_db_checked(
            std::io::Cursor::new(bytes),
            staged_output,
            expected_pre_checksum,
        )?;
        final_txid = *max_txid;
        expected_pre_checksum = apply_result.post_apply_checksum;
    }

    restore_reached_target(final_txid, target_txid, point_in_time.is_some())?;
    verify_sqlite_integrity(staged_output)?;
    staged_restore.publish(output)?;
    Ok(final_txid)
}
