use anyhow::{anyhow, Result};
use chrono::Utc;
use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};
use std::time::Duration;

use crate::cache::LocalCache;
use crate::config::Config;
use crate::ltx;
use crate::s3::{self, create_client, parse_bucket};

use super::compact::compute_file_sha256;
use super::manifest::{build_ltx_key, discover_state_from_s3, find_latest_snapshot, list_generation_files, load_manifest, parse_ltx_filename, GENERATION_LIVE};
use super::types::{Manifest, ReplicaState};

pub async fn restore(
    name: &str,
    output: &Path,
    bucket: &str,
    endpoint: Option<&str>,
    point_in_time: Option<&str>,
    cache_dir: Option<&Path>,
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
                tracing::warn!("Cache directory {} has no manifest, falling back to S3", dir.display());
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

    // Find the latest snapshot (min_txid=1 in generation 1+)
    let snapshot = find_latest_snapshot(&client, &bucket_name, &prefix, name)
        .await?
        .ok_or_else(|| anyhow!("No snapshot found for database: {}", name))?;

    let (snapshot_gen, snapshot_key, snapshot_min_txid, snapshot_max_txid) = snapshot;

    // Parse point in time if provided (TXID only for litestream format)
    let target_txid = if let Some(pit) = point_in_time {
        pit.parse::<u64>().map_err(|_| {
            anyhow!("Invalid point_in_time format. Use TXID (number)")
        })?
    } else {
        current_txid
    };

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
            tracing::debug!("Snapshot TXID {} not in cache, fetching from S3", snapshot_max_txid);
            s3::download_bytes(&client, &bucket_name, &snapshot_key).await?
        }
    } else {
        s3::download_bytes(&client, &bucket_name, &snapshot_key).await?
    };

    let cursor = std::io::Cursor::new(ltx_data);
    let decode_result = ltx::decode_to_db(cursor, output)?;

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

    // Get incrementals from generation 0 (live folder)
    let incrementals = list_generation_files(&client, &bucket_name, &prefix, name, GENERATION_LIVE).await?;

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
            // apply_ltx_to_db now verifies pre_apply and post_apply checksums
            let apply_result = ltx::apply_ltx_to_db(cursor, output)?;

            tracing::debug!(
                "Applied {} (TXID: {}-{}, checksum: {:016x})",
                key,
                apply_result.header.min_txid.into_inner(),
                apply_result.header.max_txid.into_inner(),
                apply_result.post_apply_checksum.into_inner()
            );

            final_txid = *max_txid;
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

    println!(
        "Restored {} to {} (TXID: {})",
        name,
        output.display(),
        final_txid
    );
    Ok(())
}

/// Legacy restore for backwards compatibility with raw .db snapshots
async fn restore_legacy(
    name: &str,
    output: &Path,
    bucket: &str,
    endpoint: Option<&str>,
    point_in_time: Option<&str>,
) -> Result<()> {
    let (bucket_name, prefix) = parse_bucket(bucket);
    let client = create_client(endpoint).await?;

    // Find legacy snapshots
    let snapshots_prefix = format!("{}{}/snapshots/", prefix, name);
    let snapshots = s3::list_objects(&client, &bucket_name, &snapshots_prefix).await?;

    if snapshots.is_empty() {
        return Err(anyhow!("No snapshots found for database: {}", name));
    }

    let pit = point_in_time
        .map(|s| chrono::DateTime::parse_from_rfc3339(s))
        .transpose()?
        .map(|dt| dt.with_timezone(&Utc));

    let snapshot_key = if let Some(pit) = pit {
        snapshots
            .iter()
            .filter(|k| {
                if let Some(ts) = k
                    .strip_prefix(&snapshots_prefix)
                    .and_then(|s| s.strip_suffix(".db"))
                {
                    if let Ok(dt) = chrono::NaiveDateTime::parse_from_str(ts, "%Y%m%d%H%M%S") {
                        return dt.and_utc() <= pit;
                    }
                }
                false
            })
            .max()
            .ok_or_else(|| anyhow!("No snapshot found before {}", pit))?
            .clone()
    } else {
        snapshots
            .last()
            .cloned()
            .ok_or_else(|| anyhow!("No snapshots"))?
    };

    tracing::info!("Restoring from legacy snapshot: {}", snapshot_key);
    s3::download_file(&client, &bucket_name, &snapshot_key, output).await?;

    if let Ok(Some(stored_checksum)) =
        s3::get_checksum(&client, &bucket_name, &snapshot_key).await
    {
        let restored_checksum = compute_file_sha256(output).await?;
        if stored_checksum != restored_checksum {
            return Err(anyhow!(
                "Checksum mismatch! Stored: {}, Restored: {}",
                stored_checksum,
                restored_checksum
            ));
        }
        tracing::info!("Checksum verified: {}", restored_checksum);
    }

    tracing::info!("Restored {} to {}", name, output.display());
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
            let (current_txid, max_gen, _) =
                discover_state_from_s3(&client, &bucket_name, &prefix, db).await?;

            // Count files in generation 0 (live incrementals)
            let live_files = list_generation_files(&client, &bucket_name, &prefix, db, GENERATION_LIVE).await?;

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

/// Compact old snapshots using retention policy (GFS rotation)
///
/// Analyzes snapshots and deletes those that don't fit the retention policy.
/// By default runs in dry-run mode (force=false) to show what would be deleted.
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
                eprintln!(
                    "[{}] Error: {}",
                    chrono::Local::now().format("%H:%M:%S"),
                    e
                );
            }
        }

        tokio::time::sleep(interval).await;
    }
}

/// State tracking for replica

/// Single poll iteration for replication
async fn replicate_poll(
    client: &aws_sdk_s3::Client,
    bucket: &str,
    prefix: &str,
    db_name: &str,
    local: &Path,
    current_txid: &mut u64,
) -> Result<usize> {
    // Load manifest from S3
    let manifest = load_manifest(client, bucket, prefix, db_name).await?;

    if manifest.files.is_empty() {
        return Err(anyhow!("No LTX files found in manifest for '{}'", db_name));
    }

    // If we haven't initialized yet (current_txid = 0), bootstrap from snapshot
    if *current_txid == 0 || !local.exists() {
        bootstrap_replica(client, bucket, prefix, db_name, local, &manifest).await?;
        // After bootstrap, current_txid is the snapshot's max_txid
        let snapshot = manifest
            .files
            .iter()
            .filter(|f| f.is_snapshot)
            .max_by_key(|f| f.max_txid)
            .ok_or_else(|| anyhow!("No snapshot found for bootstrap"))?;
        *current_txid = snapshot.max_txid;
        save_replica_state(local, *current_txid)?;
        return Ok(1);
    }

    // Find incremental LTX files we need to apply (min_txid > current_txid)
    let mut incrementals: Vec<_> = manifest
        .files
        .iter()
        .filter(|f| !f.is_snapshot && f.min_txid > *current_txid)
        .collect();

    // Also check for newer snapshots that might be more efficient
    // (e.g., if we're very far behind, a snapshot might be faster)
    let latest_snapshot = manifest
        .files
        .iter()
        .filter(|f| f.is_snapshot)
        .max_by_key(|f| f.max_txid);

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
            bootstrap_replica(client, bucket, prefix, db_name, local, &manifest).await?;
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
        // Verify continuity: min_txid should be current_txid + 1
        // (or we accept any min_txid > current_txid for robustness)
        if ltx_entry.min_txid != *current_txid + 1 {
            tracing::warn!(
                "TXID gap: expected {}, got {}. Skipping to avoid corruption.",
                *current_txid + 1,
                ltx_entry.min_txid
            );
            // Could trigger re-bootstrap here, but for now just warn and continue
            continue;
        }

        let ltx_key = format!("{}{}/{}", prefix, db_name, ltx_entry.filename);
        tracing::debug!("Downloading incremental: {}", ltx_key);

        let ltx_data = s3::download_bytes(client, bucket, &ltx_key).await?;
        let cursor = std::io::Cursor::new(ltx_data);

        // Apply in-place (verifies checksum chain)
        let apply_result = ltx::apply_ltx_to_db(cursor, local)?;

        tracing::info!(
            "Applied {} (TXID {}-{}, checksum: {:016x})",
            ltx_entry.filename,
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
async fn bootstrap_replica(
    client: &aws_sdk_s3::Client,
    bucket: &str,
    prefix: &str,
    db_name: &str,
    local: &Path,
    manifest: &Manifest,
) -> Result<()> {
    // Find the best (latest) snapshot
    let snapshot = manifest
        .files
        .iter()
        .filter(|f| f.is_snapshot)
        .max_by_key(|f| f.max_txid)
        .ok_or_else(|| anyhow!("No snapshot found for database '{}'", db_name))?;

    tracing::info!(
        "Bootstrapping replica from snapshot: {} (TXID: {})",
        snapshot.filename,
        snapshot.max_txid
    );

    let ltx_key = format!("{}{}/{}", prefix, db_name, snapshot.filename);
    let ltx_data = s3::download_bytes(client, bucket, &ltx_key).await?;

    // Decode snapshot to local database (verifies checksum)
    let cursor = std::io::Cursor::new(ltx_data);
    let decode_result = ltx::decode_to_db(cursor, local)?;

    println!(
        "Bootstrapped from snapshot: {} pages, TXID {}, checksum: {:016x}",
        decode_result.header.commit.into_inner(),
        decode_result.header.max_txid.into_inner(),
        decode_result.post_apply_checksum.into_inner()
    );

    Ok(())
}

/// Save replica state to local file
fn save_replica_state(local: &Path, current_txid: u64) -> Result<()> {
    let state_path = local.with_extension("db-replica-state");
    let state = ReplicaState {
        current_txid,
        last_updated: Utc::now().to_rfc3339(),
    };
    let data = serde_json::to_string_pretty(&state)?;
    std::fs::write(&state_path, data)?;
    Ok(())
}

/// Explain what the current configuration will do without running
///
/// Loads the config file and prints a human-readable summary of:
/// - Databases being watched (resolved from config/globs)
/// - Snapshot triggers (interval, max_changes, on_idle, on_startup)
/// - Compaction settings if enabled
/// - Retention policy tiers
/// - S3 bucket and endpoint
pub fn explain(config: &Option<Config>) -> Result<()> {
    match config {
        None => {
            println!("No configuration file found.");
            println!();
            println!("walrust looks for ./walrust.toml in the current directory,");
            println!("or you can specify a config file with --config <path>.");
            println!();
            println!("Without a config file, you must provide all options via CLI:");
            println!("  walrust watch <database> --bucket <bucket> [options]");
            return Ok(());
        }
        Some(cfg) => {
            println!("Configuration Summary");
            println!("=====================");
            println!();

            // S3 Settings
            println!("S3 Storage:");
            if let Some(bucket) = &cfg.s3.bucket {
                println!("  Bucket:   {}", bucket);
            } else {
                println!("  Bucket:   (not configured - must specify via --bucket)");
            }
            if let Some(endpoint) = &cfg.s3.endpoint {
                println!("  Endpoint: {}", endpoint);
            } else {
                println!("  Endpoint: (default AWS S3)");
            }
            println!();

            // Snapshot Triggers
            println!("Snapshot Triggers (global defaults):");
            println!("  Interval:    {} seconds ({} minutes)",
                cfg.sync.snapshot_interval,
                cfg.sync.snapshot_interval / 60
            );
            if cfg.sync.max_changes > 0 {
                println!("  Max changes: {} WAL frames", cfg.sync.max_changes);
            } else {
                println!("  Max changes: disabled");
            }
            if cfg.sync.max_interval > 0 {
                println!("  Max interval: {} seconds", cfg.sync.max_interval);
            }
            if cfg.sync.on_idle > 0 {
                println!("  On idle:     {} seconds", cfg.sync.on_idle);
            } else {
                println!("  On idle:     disabled");
            }
            println!("  On startup:  {}", if cfg.sync.on_startup { "yes" } else { "no" });
            println!();

            // Compaction Settings
            println!("Compaction:");
            if cfg.sync.compact_after_snapshot {
                println!("  After snapshot: enabled");
            } else {
                println!("  After snapshot: disabled");
            }
            if cfg.sync.compact_interval > 0 {
                println!("  Interval:       {} seconds ({} minutes)",
                    cfg.sync.compact_interval,
                    cfg.sync.compact_interval / 60
                );
            } else {
                println!("  Interval:       disabled");
            }
            println!();

            // Retention Policy
            println!("Retention Policy (GFS rotation):");
            println!("  Hourly:  {} snapshots (last {} hours)", cfg.retention.hourly, cfg.retention.hourly);
            println!("  Daily:   {} snapshots (last {} days)", cfg.retention.daily, cfg.retention.daily);
            println!("  Weekly:  {} snapshots (last {} weeks)", cfg.retention.weekly, cfg.retention.weekly);
            println!("  Monthly: {} snapshots (last {} months)", cfg.retention.monthly, cfg.retention.monthly);
            println!();

            // Databases
            println!("Databases:");
            if cfg.databases.is_empty() {
                println!("  (none configured - must specify via CLI)");
            } else {
                // Resolve databases to show actual paths
                match cfg.resolve_databases() {
                    Ok(resolved) => {
                        if resolved.is_empty() {
                            println!("  (no matching files found for configured patterns)");
                        } else {
                            for db in &resolved {
                                println!("  - {} -> s3://.../{}/*", db.path.display(), db.prefix);

                                // Show per-database overrides if different from global
                                let mut overrides = Vec::new();
                                if db.sync.snapshot_interval != cfg.sync.snapshot_interval {
                                    overrides.push(format!("interval={}s", db.sync.snapshot_interval));
                                }
                                if db.sync.max_changes != cfg.sync.max_changes {
                                    overrides.push(format!("max_changes={}", db.sync.max_changes));
                                }
                                if db.retention.hourly != cfg.retention.hourly
                                    || db.retention.daily != cfg.retention.daily
                                    || db.retention.weekly != cfg.retention.weekly
                                    || db.retention.monthly != cfg.retention.monthly
                                {
                                    overrides.push(format!(
                                        "retention={}/{}/{}/{}",
                                        db.retention.hourly, db.retention.daily,
                                        db.retention.weekly, db.retention.monthly
                                    ));
                                }
                                if !overrides.is_empty() {
                                    println!("    Overrides: {}", overrides.join(", "));
                                }
                            }
                        }
                    }
                    Err(e) => {
                        println!("  (error resolving databases: {})", e);
                        for db in &cfg.databases {
                            println!("  - {} (pattern)", db.path);
                        }
                    }
                }
            }
            println!();

            // Summary
            let total_snapshots = cfg.retention.hourly + cfg.retention.daily
                + cfg.retention.weekly + cfg.retention.monthly;
            println!("Summary:");
            println!("  Max snapshots retained per database: ~{}", total_snapshots);
            if cfg.sync.compact_after_snapshot || cfg.sync.compact_interval > 0 {
                println!("  Automatic compaction: enabled");
            } else {
                println!("  Automatic compaction: disabled (run 'walrust compact' manually)");
            }
        }
    }

    Ok(())
}

/// Verification issue found during verify
#[derive(Debug, Clone)]
pub struct VerifyIssue {
    pub filename: String,
    pub issue: String,
    pub is_orphan: bool,
}

/// Result of backup validation
#[derive(Debug)]
pub struct ValidationResult {
    pub verified_count: usize,
    pub total_files: usize,
    pub issues: Vec<VerifyIssue>,
    pub verified_size_bytes: u64,
    pub is_valid: bool,
}

/// Validate backup integrity for a database (non-blocking, for periodic validation)
pub(crate) async fn validate_backup_integrity(
    client: &aws_sdk_s3::Client,
    bucket: &str,
    prefix: &str,
    db_name: &str,
) -> Result<ValidationResult> {
    // Load manifest
    let manifest = load_manifest(client, bucket, prefix, db_name).await?;

    if manifest.files.is_empty() {
        return Ok(ValidationResult {
            verified_count: 0,
            total_files: 0,
            issues: Vec::new(),
            verified_size_bytes: 0,
            is_valid: true,
        });
    }

    let mut issues: Vec<VerifyIssue> = Vec::new();
    let mut verified_count = 0;
    let mut total_size: u64 = 0;

    // Check each LTX file
    for entry in &manifest.files {
        let ltx_key = format!("{}{}/{}", prefix, db_name, entry.filename);

        match s3::exists(client, bucket, &ltx_key).await {
            Ok(true) => {
                // File exists, download and verify
                match s3::download_bytes(client, bucket, &ltx_key).await {
                    Ok(data) => {
                        let cursor = std::io::Cursor::new(&data);
                        match ltx::verify_ltx(cursor) {
                            Ok(header) => {
                                let header_min = header.min_txid.into_inner();
                                let header_max = header.max_txid.into_inner();

                                if header_min != entry.min_txid || header_max != entry.max_txid {
                                    issues.push(VerifyIssue {
                                        filename: entry.filename.clone(),
                                        issue: format!(
                                            "TXID mismatch: manifest {}-{}, header {}-{}",
                                            entry.min_txid, entry.max_txid,
                                            header_min, header_max
                                        ),
                                        is_orphan: false,
                                    });
                                } else {
                                    verified_count += 1;
                                    total_size += data.len() as u64;
                                }
                            }
                            Err(e) => {
                                issues.push(VerifyIssue {
                                    filename: entry.filename.clone(),
                                    issue: format!("Checksum failed: {}", e),
                                    is_orphan: false,
                                });
                            }
                        }
                    }
                    Err(e) => {
                        issues.push(VerifyIssue {
                            filename: entry.filename.clone(),
                            issue: format!("Download failed: {}", e),
                            is_orphan: false,
                        });
                    }
                }
            }
            Ok(false) => {
                issues.push(VerifyIssue {
                    filename: entry.filename.clone(),
                    issue: "File missing from S3".to_string(),
                    is_orphan: true,
                });
            }
            Err(e) => {
                issues.push(VerifyIssue {
                    filename: entry.filename.clone(),
                    issue: format!("S3 check failed: {}", e),
                    is_orphan: false,
                });
            }
        }
    }

    // Check TXID continuity
    let mut sorted_files: Vec<_> = manifest.files.iter().collect();
    sorted_files.sort_by_key(|f| f.min_txid);

    let mut expected_next_txid: Option<u64> = None;
    for entry in &sorted_files {
        if let Some(expected) = expected_next_txid {
            // For incrementals, check for gaps
            if !entry.is_snapshot && entry.min_txid != expected && entry.min_txid > expected {
                issues.push(VerifyIssue {
                    filename: entry.filename.clone(),
                    issue: format!(
                        "TXID gap: expected {}, got {} (missing {}-{})",
                        expected, entry.min_txid,
                        expected, entry.min_txid - 1
                    ),
                    is_orphan: false,
                });
            }
        }
        expected_next_txid = Some(entry.max_txid + 1);
    }

    Ok(ValidationResult {
        verified_count,
        total_files: manifest.files.len(),
        issues: issues.clone(),
        verified_size_bytes: total_size,
        is_valid: issues.is_empty(),
    })
}

/// Verify integrity of all LTX files in S3 for a database
///
/// Checks:
/// - Each LTX file in manifest exists in S3
/// - LTX headers can be decoded
/// - LTX internal checksums are valid
/// - TXID continuity (no gaps in the chain)
///
/// With --fix, removes orphaned entries from manifest
pub async fn verify(
    name: &str,
    bucket: &str,
    endpoint: Option<&str>,
    _fix: bool, // No longer used - files are source of truth in litestream format
) -> Result<()> {
    let (bucket_name, prefix) = parse_bucket(bucket);
    let client = create_client(endpoint).await?;

    println!("Verifying integrity of '{}' in s3://{}/{}{}...",
        name, bucket_name, prefix, name);
    println!();

    // Discover state from S3 (litestream format - no manifest)
    let (current_txid, max_gen, _) =
        discover_state_from_s3(&client, &bucket_name, &prefix, name).await?;

    if current_txid == 0 {
        println!("No LTX files found for database: {}", name);
        return Ok(());
    }

    // Collect all files from all generations
    let mut all_files: Vec<(String, u64, u64, u64)> = Vec::new(); // (key, gen, min, max)

    // Get files from generation 0 (live incrementals)
    let live_files = list_generation_files(&client, &bucket_name, &prefix, name, GENERATION_LIVE).await?;
    for (key, min, max) in live_files {
        all_files.push((key, GENERATION_LIVE, min, max));
    }

    // Get files from snapshot generations (1+)
    for gen in 1..=max_gen {
        let gen_files = list_generation_files(&client, &bucket_name, &prefix, name, gen).await?;
        for (key, min, max) in gen_files {
            all_files.push((key, gen, min, max));
        }
    }

    println!("Found {} LTX files across {} generations", all_files.len(), max_gen + 1);
    println!("Current TXID: {}", current_txid);
    println!();

    let mut issues: Vec<VerifyIssue> = Vec::new();
    let mut verified_count = 0;
    let mut total_size: u64 = 0;

    // Verify each file
    for (key, _gen, expected_min, expected_max) in &all_files {
        match s3::download_bytes(&client, &bucket_name, key).await {
            Ok(data) => {
                let cursor = std::io::Cursor::new(&data);
                match ltx::verify_ltx(cursor) {
                    Ok(header) => {
                        let header_min = header.min_txid.into_inner();
                        let header_max = header.max_txid.into_inner();

                        // Verify header matches filename
                        if header_min != *expected_min || header_max != *expected_max {
                            issues.push(VerifyIssue {
                                filename: key.clone(),
                                issue: format!(
                                    "TXID mismatch: filename says {}-{}, header says {}-{}",
                                    expected_min, expected_max,
                                    header_min, header_max
                                ),
                                is_orphan: false,
                            });
                        } else {
                            verified_count += 1;
                            total_size += data.len() as u64;
                        }
                    }
                    Err(e) => {
                        issues.push(VerifyIssue {
                            filename: key.clone(),
                            issue: format!("Checksum verification failed: {}", e),
                            is_orphan: false,
                        });
                    }
                }
            }
            Err(e) => {
                issues.push(VerifyIssue {
                    filename: key.clone(),
                    issue: format!("Download failed: {}", e),
                    is_orphan: false,
                });
            }
        }
    }

    // Check TXID continuity in generation 0 (live)
    let mut live_files: Vec<_> = all_files
        .iter()
        .filter(|(_, gen, _, _)| *gen == GENERATION_LIVE)
        .collect();
    live_files.sort_by_key(|(_, _, min, _)| *min);

    let mut expected_next_txid: Option<u64> = None;
    for (key, _, min_txid, max_txid) in &live_files {
        if let Some(expected) = expected_next_txid {
            if *min_txid != expected && *min_txid > expected {
                issues.push(VerifyIssue {
                    filename: key.clone(),
                    issue: format!(
                        "TXID gap: expected min_txid={}, got {} (missing TXIDs {}-{})",
                        expected, min_txid,
                        expected, min_txid - 1
                    ),
                    is_orphan: false,
                });
            }
        }
        expected_next_txid = Some(max_txid + 1);
    }

    // Report results
    println!("Verification Results");
    println!("====================");
    println!("Verified:  {} files ({:.2} MB)", verified_count, total_size as f64 / (1024.0 * 1024.0));
    println!("Issues:    {}", issues.len());
    println!();

    if issues.is_empty() {
        println!("All LTX files verified successfully.");
        return Ok(());
    }

    // Report issues
    println!("Issues Found:");
    for issue in &issues {
        println!("  [ERROR] {}: {}", issue.filename, issue.issue);
    }
    println!();

    if !issues.is_empty() {
        println!("Note: Issues may require manual intervention:");
        println!("  - Checksum failures indicate corrupted files");
        println!("  - TXID gaps may require restoring from an earlier snapshot");
    }

    Ok(())
}

/// Checkpoint mode for SQLite WAL
#[derive(Debug, Clone, Copy)]
enum CheckpointMode {
    /// Non-blocking, best effort checkpoint
    Passive,
    /// Blocking checkpoint that ensures WAL is reset
    Truncate,
}

/// Get WAL page count for size checking
async fn get_wal_page_count(wal_path: &Path) -> Result<u64> {
    if !wal_path.exists() {
        return Ok(0);
    }

    // WAL file size / page size (4096 bytes typically)
    let metadata = tokio::fs::metadata(wal_path).await?;
    let file_size = metadata.len();

    if file_size < 32 {
        // WAL file too small to have a valid header
        return Ok(0);
    }

    // Read page size from WAL header (bytes 8-11)
    let mut file = tokio::fs::File::open(wal_path).await?;
    let mut header = vec![0u8; 32];
    use tokio::io::AsyncReadExt;
    file.read_exact(&mut header).await?;

    let page_size = u32::from_be_bytes([header[8], header[9], header[10], header[11]]) as u64;

    // Account for WAL header (32 bytes) + frame headers (24 bytes each)
    // Approximate: (file_size - 32) / (page_size + 24)
    let approx_pages = if page_size > 0 {
        (file_size.saturating_sub(32)) / (page_size + 24)
    } else {
        0
    };

    Ok(approx_pages)
}

/// Run SQLite checkpoint on database
async fn run_checkpoint(db_path: &Path, mode: CheckpointMode) -> Result<()> {
    // Use blocking task since SQLite operations are synchronous
    let db_path = db_path.to_path_buf();

    tokio::task::spawn_blocking(move || {
        let conn = rusqlite::Connection::open(&db_path)?;

        let pragma = match mode {
            CheckpointMode::Passive => "PRAGMA wal_checkpoint(PASSIVE)",
            CheckpointMode::Truncate => "PRAGMA wal_checkpoint(TRUNCATE)",
        };

        // Returns (busy, checkpointed_frames, log_size)
        let (busy, frames, log_size): (i32, i32, i32) = conn.query_row(pragma, [], |row| {
            Ok((row.get(0)?, row.get(1)?, row.get(2)?))
        })?;

        if busy != 0 {
            tracing::debug!("Checkpoint was busy (concurrent writers)");
        }

        tracing::debug!(
            "Checkpointed {} frames (log size: {})",
            frames,
            log_size
        );
        Ok(())
    })
    .await?
}

// ============================================================================
// StorageBackend-aware functions for testability
// ============================================================================

