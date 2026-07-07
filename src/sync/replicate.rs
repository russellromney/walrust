use anyhow::{anyhow, Result};
use chrono::Utc;
use std::fs::{self, File};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use crate::ltx;
use crate::s3::{self, create_client};

use super::manifest::{discover_all_ltx_from_s3, DiscoveredLtx};
use super::types::ReplicaState;

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

pub async fn replicate(
    source: &str,
    local: &Path,
    interval: Duration,
    endpoint: Option<&str>,
) -> Result<()> {
    // Parse source: "s3://bucket/prefix/dbname" or "s3://bucket/dbname"
    let source = source.strip_prefix("s3://").unwrap_or(source);
    let parts: Vec<&str> = source.splitn(2, '/').collect();
    if parts.len() < 2 {
        return Err(anyhow!(
            "Invalid source format. Expected: s3://bucket/dbname or s3://bucket/prefix/dbname"
        ));
    }

    let bucket_name = parts[0];
    let path_part = parts[1];

    // Split path into prefix and dbname (last component is dbname)
    let (prefix, db_name) = if let Some(idx) = path_part.rfind('/') {
        let p = &path_part[..=idx]; // Include trailing slash
        let n = &path_part[idx + 1..];
        (p.to_string(), n.to_string())
    } else {
        (String::new(), path_part.to_string())
    };

    let client = create_client(endpoint).await?;

    tracing::info!(
        "Starting replica: source=s3://{}/{}{}, local={}",
        bucket_name,
        prefix,
        db_name,
        local.display()
    );

    // Track current TXID (0 = not yet initialized)
    let mut current_txid: u64 = 0;

    // Check if local database exists
    if local.exists() {
        // Try to determine current TXID from local state file
        let state_path = local.with_extension("db-replica-state");
        if state_path.exists() {
            if let Ok(data) = std::fs::read_to_string(&state_path) {
                if let Ok(state) = serde_json::from_str::<ReplicaState>(&data) {
                    current_txid = state.current_txid;
                    tracing::info!("Resuming replica from TXID {}", current_txid);
                }
            }
        }
    }

    println!(
        "Replicating s3://{}/{}{} -> {}",
        bucket_name,
        prefix,
        db_name,
        local.display()
    );
    println!("Poll interval: {:?}", interval);
    println!("Press Ctrl+C to stop\n");

    // Main replication loop
    loop {
        match replicate_poll(
            &client,
            bucket_name,
            &prefix,
            &db_name,
            local,
            &mut current_txid,
        )
        .await
        {
            Ok(applied) => {
                if applied > 0 {
                    println!(
                        "[{}] Applied {} LTX file(s), now at TXID {}",
                        chrono::Local::now().format("%H:%M:%S"),
                        applied,
                        current_txid
                    );
                }
            }
            Err(e) => {
                tracing::error!("Replication error: {}", e);
                eprintln!("[{}] Error: {}", chrono::Local::now().format("%H:%M:%S"), e);
            }
        }

        tokio::time::sleep(interval).await;
    }
}

/// Single poll iteration for replication
async fn replicate_poll(
    client: &aws_sdk_s3::Client,
    bucket: &str,
    prefix: &str,
    db_name: &str,
    local: &Path,
    current_txid: &mut u64,
) -> Result<usize> {
    // Discover LTX files from the S3 listing. The production watch path writes
    // litestream-format objects and never a manifest.json, so reading a
    // manifest made replicate fail with "No LTX files found" (F6).
    let files = discover_all_ltx_from_s3(client, bucket, prefix, db_name).await?;

    if files.is_empty() {
        return Err(anyhow!("No LTX files found in S3 for '{}'", db_name));
    }

    // If we haven't initialized yet (current_txid = 0), bootstrap from snapshot
    if *current_txid == 0 || !local.exists() {
        let snapshot =
            latest_snapshot(&files).ok_or_else(|| anyhow!("No snapshot found for bootstrap"))?;
        bootstrap_replica_from_snapshot(client, bucket, local, snapshot).await?;
        // After bootstrap, current_txid is the snapshot's max_txid
        *current_txid = snapshot.max_txid;
        save_replica_state(local, *current_txid)?;
        return Ok(1);
    }

    // Find incremental LTX files we need to apply (min_txid > current_txid)
    let mut incrementals: Vec<_> = files
        .iter()
        .filter(|f| !f.is_snapshot && f.min_txid > *current_txid)
        .collect();

    // Also check for newer snapshots that might be more efficient
    // (e.g., if we're very far behind, a snapshot might be faster)
    let latest_snapshot = latest_snapshot(&files);

    // If there's a snapshot newer than our position + all incrementals we'd apply,
    // and we're far behind, consider using the snapshot instead
    if let Some(snap) = latest_snapshot {
        if snap.max_txid > *current_txid && incrementals.is_empty() {
            // We're behind but no incrementals bridge the gap - need snapshot
            tracing::info!(
                "Gap detected: at TXID {}, latest snapshot at TXID {}. Re-bootstrapping.",
                current_txid,
                snap.max_txid
            );
            bootstrap_replica_from_snapshot(client, bucket, local, snap).await?;
            *current_txid = snap.max_txid;
            save_replica_state(local, *current_txid)?;
            return Ok(1);
        }
    }

    if incrementals.is_empty() {
        return Ok(0); // No new data
    }

    // Sort by min_txid to apply in order
    incrementals.sort_by_key(|f| f.min_txid);

    let mut applied = 0;

    for ltx_entry in incrementals {
        // Verify continuity: min_txid must be exactly current_txid + 1.
        if ltx_entry.min_txid != *current_txid + 1 {
            let gap_snapshot = files
                .iter()
                .filter(|f| f.is_snapshot)
                .filter(|f| f.max_txid >= ltx_entry.min_txid)
                .max_by_key(|f| f.max_txid)
                .ok_or_else(|| {
                    anyhow!(
                        "TXID gap in incremental chain (expected {}, got {} at {}) and no \
                         snapshot at or past TXID {} is available to re-bootstrap from",
                        *current_txid + 1,
                        ltx_entry.min_txid,
                        ltx_entry.key,
                        ltx_entry.min_txid
                    )
                })?;
            tracing::warn!(
                "TXID gap in incremental chain: expected {}, got {}. Re-bootstrapping from \
                 snapshot at TXID {}.",
                *current_txid + 1,
                ltx_entry.min_txid,
                gap_snapshot.max_txid
            );
            bootstrap_replica_from_snapshot(client, bucket, local, gap_snapshot).await?;
            *current_txid = gap_snapshot.max_txid;
            save_replica_state(local, *current_txid)?;
            return Ok(1);
        }

        tracing::debug!("Downloading incremental: {}", ltx_entry.key);

        let ltx_data = s3::download_bytes(client, bucket, &ltx_entry.key).await?;
        let apply_result = apply_incremental_atomically(&ltx_data, local)?;

        tracing::info!(
            "Applied {} (TXID {}-{}, checksum: {:016x})",
            ltx_entry.key,
            apply_result.header.min_txid.into_inner(),
            apply_result.header.max_txid.into_inner(),
            apply_result.post_apply_checksum.into_inner()
        );

        *current_txid = ltx_entry.max_txid;
        applied += 1;

        // Save state after each successful apply
        save_replica_state(local, *current_txid)?;
    }

    Ok(applied)
}

/// Bootstrap replica from latest snapshot
fn latest_snapshot(files: &[DiscoveredLtx]) -> Option<&DiscoveredLtx> {
    files
        .iter()
        .filter(|f| f.is_snapshot)
        .max_by_key(|f| f.max_txid)
}

async fn bootstrap_replica_from_snapshot(
    client: &aws_sdk_s3::Client,
    bucket: &str,
    local: &Path,
    snapshot: &DiscoveredLtx,
) -> Result<()> {
    tracing::info!(
        "Bootstrapping replica from snapshot: {} (TXID: {})",
        snapshot.key,
        snapshot.max_txid
    );

    let ltx_data = s3::download_bytes(client, bucket, &snapshot.key).await?;

    let staged = AtomicReplicaFile::new(local);

    // Decode snapshot to a staged database (verifies checksum)
    let cursor = std::io::Cursor::new(ltx_data);
    let decode_result = ltx::decode_to_db(cursor, staged.path())?;
    sync_file(staged.path())?;
    verify_sqlite_integrity(staged.path())?;
    staged.publish(local)?;

    println!(
        "Bootstrapped from snapshot: {} pages, TXID {}, checksum: {:016x}",
        decode_result.header.commit.into_inner(),
        decode_result.header.max_txid.into_inner(),
        decode_result.post_apply_checksum.into_inner()
    );

    Ok(())
}

fn apply_incremental_atomically(ltx_data: &[u8], local: &Path) -> Result<ltx::ApplyResult> {
    let staged = AtomicReplicaFile::copy_from(local)?;
    let cursor = std::io::Cursor::new(ltx_data);
    let apply_result = ltx::apply_ltx_to_db(cursor, staged.path())?;
    sync_file(staged.path())?;
    verify_sqlite_integrity(staged.path())?;
    staged.publish(local)?;
    Ok(apply_result)
}

/// Save replica state to local file
fn save_replica_state(local: &Path, current_txid: u64) -> Result<()> {
    let state_path = local.with_extension("db-replica-state");
    let state = ReplicaState {
        current_txid,
        last_updated: Utc::now().to_rfc3339(),
    };
    let data = serde_json::to_string_pretty(&state)?;
    let tmp_path = state_path.with_extension("db-replica-state.tmp");
    {
        let mut file = File::create(&tmp_path).map_err(|e| {
            anyhow!(
                "failed to create temporary replica state {}: {e}",
                tmp_path.display()
            )
        })?;
        use std::io::Write;
        file.write_all(data.as_bytes()).map_err(|e| {
            anyhow!(
                "failed to write temporary replica state {}: {e}",
                tmp_path.display()
            )
        })?;
        file.sync_all().map_err(|e| {
            anyhow!(
                "failed to fsync temporary replica state {}: {e}",
                tmp_path.display()
            )
        })?;
    }
    fs::rename(&tmp_path, &state_path).map_err(|e| {
        anyhow!(
            "failed to atomically publish replica state {} over {}: {e}",
            tmp_path.display(),
            state_path.display()
        )
    })?;
    fsync_parent_dir(&state_path)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ltx::Checksum;
    use crate::sync::manifest::{build_ltx_key, GENERATION_LIVE};
    use rusqlite::Connection;
    use std::time::{SystemTime, UNIX_EPOCH};

    fn unique_name(prefix: &str) -> String {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        format!("{prefix}-{nanos}")
    }

    fn test_bucket_config() -> (String, Option<String>) {
        let bucket = std::env::var("WALRUST_TEST_BUCKET")
            .unwrap_or_else(|_| "walrust-test-rr-2026/replicate-test".to_string());
        let endpoint = std::env::var("AWS_ENDPOINT_URL_S3")
            .or_else(|_| std::env::var("AWS_ENDPOINT_URL"))
            .ok();
        (bucket, endpoint)
    }

    fn create_marker_db(path: &Path, marker: &str) -> Result<u32> {
        let conn = Connection::open(path)?;
        conn.execute("CREATE TABLE marker (value TEXT NOT NULL);", [])?;
        conn.execute(
            "INSERT INTO marker (value) VALUES (?1)",
            rusqlite::params![marker],
        )?;
        let page_size = conn.query_row("PRAGMA page_size", [], |row| row.get(0))?;
        drop(conn);
        Ok(page_size)
    }

    fn marker_value(path: &Path) -> Result<String> {
        let conn = Connection::open(path)?;
        Ok(conn.query_row("SELECT value FROM marker", [], |row| row.get(0))?)
    }

    #[tokio::test]
    async fn replica_failed_incremental_apply_preserves_existing_database() -> Result<()> {
        let (bucket_arg, endpoint) = test_bucket_config();
        let (bucket, prefix) = s3::parse_bucket(&bucket_arg);
        let client = create_client(endpoint.as_deref()).await?;
        let db_name = unique_name("replica-failed-apply");
        let tmp = tempfile::tempdir()?;
        let local = tmp.path().join("local.db");
        let page_size = create_marker_db(&local, "local-survives")?;
        let original_bytes = std::fs::read(&local)?;
        let pre_checksum = ltx::compute_checksum_from_file(&local)?;

        let pages = vec![(1, vec![0xAA; page_size as usize])];
        let mut bad_incremental = Vec::new();
        ltx::encode_wal_changes(
            &mut bad_incremental,
            &pages,
            page_size,
            2,
            2,
            1,
            Some(pre_checksum),
            Checksum::new(0x0bad_c0de),
        )?;
        let key = build_ltx_key(&prefix, &db_name, GENERATION_LIVE, 2, 2);
        s3::upload_bytes(&client, &bucket, &key, bad_incremental).await?;

        let mut current_txid = 1;
        let err = replicate_poll(
            &client,
            &bucket,
            &prefix,
            &db_name,
            &local,
            &mut current_txid,
        )
        .await
        .expect_err("corrupt incremental must fail replication");

        assert!(
            err.to_string().contains("Post-apply checksum mismatch"),
            "expected checksum failure, got: {err}"
        );
        assert_eq!(
            std::fs::read(&local)?,
            original_bytes,
            "failed replica apply must not mutate the live local database"
        );
        assert_eq!(marker_value(&local)?, "local-survives");
        assert_eq!(current_txid, 1);
        Ok(())
    }

    #[tokio::test]
    async fn replica_gap_without_future_snapshot_errors_and_preserves_existing_database(
    ) -> Result<()> {
        let (bucket_arg, endpoint) = test_bucket_config();
        let (bucket, prefix) = s3::parse_bucket(&bucket_arg);
        let client = create_client(endpoint.as_deref()).await?;
        let db_name = unique_name("replica-gap");
        let tmp = tempfile::tempdir()?;
        let local = tmp.path().join("local.db");
        let snapshot_db = tmp.path().join("snapshot.db");

        let page_size = create_marker_db(&local, "local-survives")?;
        create_marker_db(&snapshot_db, "old-snapshot")?;
        let original_bytes = std::fs::read(&local)?;

        let mut snapshot = Vec::new();
        ltx::encode_snapshot(&mut snapshot, &snapshot_db, page_size, 1)?;
        let snapshot_key = build_ltx_key(&prefix, &db_name, 1, 1, 1);
        s3::upload_bytes(&client, &bucket, &snapshot_key, snapshot).await?;

        let snapshot_checksum = ltx::compute_checksum_from_file(&snapshot_db)?;
        let pages = vec![(1, vec![0x33; page_size as usize])];
        let post_checksum = ltx::chain_checksum(snapshot_checksum, &pages);
        let mut skipped_incremental = Vec::new();
        ltx::encode_wal_changes(
            &mut skipped_incremental,
            &pages,
            page_size,
            3,
            3,
            1,
            Some(snapshot_checksum),
            post_checksum,
        )?;
        let incremental_key = build_ltx_key(&prefix, &db_name, GENERATION_LIVE, 3, 3);
        s3::upload_bytes(&client, &bucket, &incremental_key, skipped_incremental).await?;

        let mut current_txid = 1;
        let err = replicate_poll(
            &client,
            &bucket,
            &prefix,
            &db_name,
            &local,
            &mut current_txid,
        )
        .await
        .expect_err("gap without a future snapshot must fail hard");

        assert!(
            err.to_string().contains("TXID gap"),
            "expected hard TXID gap error, got: {err}"
        );
        assert_eq!(
            std::fs::read(&local)?,
            original_bytes,
            "gap handling must not re-bootstrap to an old snapshot over the live replica"
        );
        assert_eq!(marker_value(&local)?, "local-survives");
        assert_eq!(current_txid, 1);
        Ok(())
    }
}
