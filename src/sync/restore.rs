use crate::cache::LocalCache;
use crate::ltx;
use crate::s3::{self, create_client, parse_bucket};
use anyhow::{anyhow, Result};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use super::manifest::{
    discover_state_from_s3, find_latest_snapshot, find_latest_snapshot_at_or_before,
    list_generation_files, GENERATION_LIVE,
};

/// Decide whether a completed restore actually reached its requested target.
///
/// - **Restore-to-latest** (`explicit_pit == false`): `target_txid` is a real
///   committed boundary discovered from S3, so the restore must reach it
///   exactly. Falling short means a missing incremental / chain gap and is a
///   hard error (otherwise we'd report success at a lower TXID — silent data
///   loss).
/// - **Explicit point-in-time** (`explicit_pit == true`): the requested TXID
///   may fall *between* commit boundaries; landing at the latest commit
///   `<= target` is correct point-in-time behavior, so an exact match is not
///   required. We only reject an *overshoot* (the only available snapshot/chain
///   is already past the requested point, so the result includes changes the
///   caller did not ask for).
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
            ".{file_name}.restore-{}-{id}.tmp",
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

pub async fn restore(
    name: &str,
    output: &Path,
    bucket: &str,
    endpoint: Option<&str>,
    point_in_time: Option<&str>,
    cache_dir: Option<&Path>,
    webhook: Option<std::sync::Arc<crate::webhook::WebhookSender>>,
) -> Result<()> {
    let (bucket_name, prefix) = parse_bucket(bucket);
    let client = create_client(endpoint).await?;

    // Try to open local cache if provided
    let cache = if let Some(dir) = cache_dir {
        match LocalCache::open(dir)? {
            Some(c) => {
                tracing::info!("Opened local cache at {}", dir.display());
                Some(c)
            }
            None => {
                tracing::warn!(
                    "Cache directory {} has no manifest, falling back to S3",
                    dir.display()
                );
                None
            }
        }
    } else {
        None
    };

    // Discover state from S3 file listings (litestream format - no manifest)
    let (current_txid, _max_gen, _) =
        discover_state_from_s3(&client, &bucket_name, &prefix, name).await?;

    if current_txid == 0 {
        return Err(anyhow!("No LTX files found for database: {}", name));
    }

    // Parse point in time if provided (TXID only for litestream format)
    let target_txid = if let Some(pit) = point_in_time {
        pit.parse::<u64>()
            .map_err(|_| anyhow!("Invalid point_in_time format. Use TXID (number)"))?
    } else {
        current_txid
    };

    let snapshot = if point_in_time.is_some() {
        find_latest_snapshot_at_or_before(&client, &bucket_name, &prefix, name, target_txid).await?
    } else {
        find_latest_snapshot(&client, &bucket_name, &prefix, name).await?
    }
    .ok_or_else(|| {
        if point_in_time.is_some() {
            anyhow!("No snapshot found for database {name} at or before TXID {target_txid}")
        } else {
            anyhow!("No snapshot found for database: {}", name)
        }
    })?;

    let (snapshot_gen, snapshot_key, snapshot_min_txid, snapshot_max_txid) = snapshot;

    tracing::info!(
        "Restoring from LTX snapshot: {} (TXID: {}-{}, generation: {})",
        snapshot_key,
        snapshot_min_txid,
        snapshot_max_txid,
        snapshot_gen
    );

    // Download and decode LTX snapshot
    // Try cache first, fall back to S3
    let ltx_data = if let Some(ref cache) = cache {
        if cache.has_txid(snapshot_max_txid) {
            tracing::info!("Reading snapshot TXID {} from cache", snapshot_max_txid);
            cache.read_ltx(snapshot_max_txid)?
        } else {
            tracing::debug!(
                "Snapshot TXID {} not in cache, fetching from S3",
                snapshot_max_txid
            );
            s3::download_bytes(&client, &bucket_name, &snapshot_key).await?
        }
    } else {
        s3::download_bytes(&client, &bucket_name, &snapshot_key).await?
    };

    let staged_restore = AtomicRestore::new(output);
    let staged_output = staged_restore.path();

    let cursor = std::io::Cursor::new(ltx_data);
    let decode_result = ltx::decode_to_db(cursor, staged_output).map_err(|e| {
        if let Some(webhook) = webhook {
            let error_msg = format!("LTX decode failed for snapshot: {}", e);
            let webhook = webhook.clone();
            let name = name.to_string();
            tokio::spawn(async move {
                webhook.notify_corruption(&name, &error_msg).await;
            });
        }
        e
    })?;

    tracing::info!(
        "Restored {} from LTX (page_size: {}, pages: {}, TXID: {}-{}, checksum: {:016x})",
        name,
        decode_result.header.page_size.into_inner(),
        decode_result.header.commit.into_inner(),
        decode_result.header.min_txid.into_inner(),
        decode_result.header.max_txid.into_inner(),
        decode_result.post_apply_checksum.into_inner()
    );

    let mut final_txid = snapshot_max_txid;
    let mut expected_pre_checksum = decode_result.post_apply_checksum;

    // Get incrementals from generation 0 (live folder)
    let incrementals =
        list_generation_files(&client, &bucket_name, &prefix, name, GENERATION_LIVE).await?;

    // Filter to files we need: min_txid > snapshot_max_txid and max_txid <= target_txid
    let applicable: Vec<_> = incrementals
        .iter()
        .filter(|(_, min, max)| *min > snapshot_max_txid && *max <= target_txid)
        .collect();

    if !applicable.is_empty() {
        tracing::info!("Applying {} incremental LTX files", applicable.len());

        // Track cache hits for logging
        let mut cache_hits = 0;
        let mut s3_fetches = 0;

        for (key, min_txid, max_txid) in &applicable {
            let expected_min = final_txid + 1;
            if *min_txid != expected_min {
                return Err(anyhow!(
                    "restore incremental gap: expected next TXID {expected_min}, got \
                     {min_txid}-{max_txid} at {key}"
                ));
            }

            // Try to read from cache first using max_txid as the key
            let ltx_data = if let Some(ref cache) = cache {
                if cache.has_txid(*max_txid) {
                    cache_hits += 1;
                    tracing::debug!("Reading TXID {} from cache", max_txid);
                    cache.read_ltx(*max_txid)?
                } else {
                    s3_fetches += 1;
                    tracing::debug!("TXID {} not in cache, fetching from S3", max_txid);
                    s3::download_bytes(&client, &bucket_name, key).await?
                }
            } else {
                s3_fetches += 1;
                s3::download_bytes(&client, &bucket_name, key).await?
            };

            let cursor = std::io::Cursor::new(ltx_data);
            let apply_result =
                ltx::apply_ltx_to_db_checked(cursor, staged_output, expected_pre_checksum)?;

            tracing::debug!(
                "Applied {} (TXID: {}-{}, checksum: {:016x})",
                key,
                apply_result.header.min_txid.into_inner(),
                apply_result.header.max_txid.into_inner(),
                apply_result.post_apply_checksum.into_inner()
            );

            final_txid = *max_txid;
            expected_pre_checksum = apply_result.post_apply_checksum;
        }

        if cache.is_some() {
            tracing::info!(
                "Applied {} incremental LTX files (cache: {}, S3: {}, final TXID: {})",
                applicable.len(),
                cache_hits,
                s3_fetches,
                final_txid
            );
        } else {
            tracing::info!(
                "Applied {} incremental LTX files (final TXID: {})",
                applicable.len(),
                final_txid
            );
        }
    }

    // The restore must reach the requested target. For "restore to latest"
    // (no explicit point-in-time) `target_txid` is a real committed boundary
    // discovered from S3, so falling short means a missing incremental / chain
    // gap (silent data loss). For an explicit point-in-time the target may fall
    // *between* commit boundaries, where landing at the latest commit <= target
    // is correct — so we only reject an overshoot there.
    restore_reached_target(final_txid, target_txid, point_in_time.is_some())?;
    verify_sqlite_integrity(staged_output)?;
    staged_restore.publish(output)?;

    println!(
        "Restored {} to {} (TXID: {})",
        name,
        output.display(),
        final_txid
    );
    Ok(())
}

/// List databases in bucket
pub async fn list(bucket: &str, endpoint: Option<&str>) -> Result<()> {
    let (bucket_name, prefix) = parse_bucket(bucket);
    let client = create_client(endpoint).await?;

    let objects = s3::list_objects(&client, &bucket_name, &prefix).await?;

    // Extract unique database names (litestream format: db_name/GGGG/file.ltx)
    let mut dbs: std::collections::HashSet<String> = std::collections::HashSet::new();

    for key in &objects {
        if let Some(rest) = key.strip_prefix(&prefix) {
            if let Some(name) = rest.split('/').next() {
                if !name.is_empty() {
                    dbs.insert(name.to_string());
                }
            }
        }
    }

    if dbs.is_empty() {
        println!("No databases found in s3://{}/{}", bucket_name, prefix);
    } else {
        println!("Databases in s3://{}/{}:", bucket_name, prefix);
        for db in &dbs {
            // Discover state from S3 (litestream format)
            let (current_txid, _max_gen, _) =
                discover_state_from_s3(&client, &bucket_name, &prefix, db).await?;

            // Count files in generation 0 (live incrementals)
            let live_files =
                list_generation_files(&client, &bucket_name, &prefix, db, GENERATION_LIVE).await?;

            // Find snapshots (generation 1+)
            let snapshot = find_latest_snapshot(&client, &bucket_name, &prefix, db).await?;
            let snapshot_info = match snapshot {
                Some((gen, _, _, max_txid)) => format!("snapshot gen {} (TXID {})", gen, max_txid),
                None => "no snapshot".to_string(),
            };

            println!(
                "  {} (TXID: {}, {} incrementals, {})",
                db,
                current_txid,
                live_files.len(),
                snapshot_info
            );
        }
    }

    Ok(())
}

#[cfg(test)]
mod restore_target_tests {
    use super::restore_reached_target;

    #[test]
    fn latest_restore_must_reach_target_exactly() {
        // Restore-to-latest: reaching the boundary is OK; falling short is a
        // hard error (the regression this guards: silently reporting success
        // at a lower TXID when an incremental is missing).
        assert!(restore_reached_target(100, 100, false).is_ok());
        assert!(restore_reached_target(99, 100, false).is_err());
        assert!(restore_reached_target(0, 1, false).is_err());
    }

    #[test]
    fn explicit_pit_between_boundaries_is_ok_not_an_error() {
        // The regression fix: an explicit point-in-time target that falls
        // between commit boundaries must land at the latest commit <= target
        // WITHOUT erroring. Before the fix, the unconditional `final == target`
        // check wrongly rejected this legitimate PITR.
        assert!(restore_reached_target(95, 100, true).is_ok()); // landed at 95 for target 100
        assert!(restore_reached_target(100, 100, true).is_ok()); // exact boundary
    }

    #[test]
    fn explicit_pit_overshoot_is_rejected() {
        // The only available snapshot/chain is past the requested point — the
        // result would include changes the caller didn't ask for.
        assert!(restore_reached_target(120, 100, true).is_err());
    }
}
