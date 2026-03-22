use anyhow::{anyhow, Result};
use chrono::{Duration, Utc};
use std::path::Path;
use tempfile::TempDir;

use crate::config::Config;
use crate::ltx;
use crate::retention::{self, RetentionPolicy, SnapshotEntry};
use crate::s3::{self, create_client, parse_bucket};
use crate::wal;

use super::manifest::{build_ltx_key, discover_state_from_s3, find_latest_snapshot, list_generation_files, load_manifest, parse_ltx_filename, save_manifest, GENERATION_LIVE};
use super::restore::restore;
use super::types::{LtxEntry, Manifest};
use super::wal_sync::{checkpoint_wal, get_page_size};

pub async fn compact(
    name: &str,
    bucket: &str,
    endpoint: Option<&str>,
    policy: &RetentionPolicy,
    force: bool,
) -> Result<()> {
    let (bucket_name, prefix) = parse_bucket(bucket);
    let client = create_client(endpoint).await?;

    // Load manifest to get snapshot info
    let manifest = load_manifest(&client, &bucket_name, &prefix, name).await?;

    if manifest.files.is_empty() {
        println!("No snapshots found for database '{}'", name);
        return Ok(());
    }

    // Filter to only snapshots (not incremental files)
    let snapshot_entries: Vec<SnapshotEntry> = manifest
        .files
        .iter()
        .filter(|f| f.is_snapshot)
        .filter_map(|f| {
            chrono::DateTime::parse_from_rfc3339(&f.created_at)
                .ok()
                .map(|dt| SnapshotEntry {
                    filename: f.filename.clone(),
                    created_at: dt.with_timezone(&Utc),
                    max_txid: f.max_txid,
                    size: f.size,
                })
        })
        .collect();

    if snapshot_entries.is_empty() {
        println!("No snapshots found for database '{}'", name);
        return Ok(());
    }

    let now = Utc::now();
    let plan = retention::analyze_retention(&snapshot_entries, policy, now);

    // Print summary
    println!("Compaction plan for '{}':", name);
    println!("  {}", plan.summary());
    println!();

    if !plan.has_deletions() {
        println!("Nothing to delete - all snapshots fit retention policy.");
        return Ok(());
    }

    // Print what will be kept
    println!("Keeping {} snapshots:", plan.keep.len());
    for entry in &plan.keep {
        println!(
            "  {} (TXID: {}, {})",
            entry.filename,
            entry.max_txid,
            format_age(now, entry.created_at)
        );
    }
    println!();

    // Print what will be deleted
    println!("Deleting {} snapshots:", plan.delete.len());
    for entry in &plan.delete {
        println!(
            "  {} (TXID: {}, {})",
            entry.filename,
            entry.max_txid,
            format_age(now, entry.created_at)
        );
    }
    println!();

    if !force {
        println!("Dry-run mode: no files deleted. Use --force to actually delete.");
        return Ok(());
    }

    // Actually delete files
    println!("Deleting files...");

    let keys_to_delete: Vec<String> = plan
        .delete
        .iter()
        .map(|e| format!("{}{}/{}", prefix, name, e.filename))
        .collect();

    let deleted_count = s3::delete_objects(&client, &bucket_name, &keys_to_delete).await?;

    tracing::info!("Deleted {} snapshot files", deleted_count);

    // Update manifest to remove deleted entries
    let kept_filenames: std::collections::HashSet<_> =
        plan.keep.iter().map(|e| e.filename.as_str()).collect();

    let updated_files: Vec<LtxEntry> = manifest
        .files
        .into_iter()
        .filter(|f| !f.is_snapshot || kept_filenames.contains(f.filename.as_str()))
        .collect();

    let updated_manifest = Manifest {
        files: updated_files,
        ..manifest
    };

    save_manifest(&client, &bucket_name, &prefix, &updated_manifest).await?;

    println!(
        "Compaction complete: deleted {} snapshots, freed {:.2} MB",
        deleted_count,
        plan.bytes_freed as f64 / (1024.0 * 1024.0)
    );

    Ok(())
}

/// Compaction result statistics
#[derive(Debug, Clone)]
pub struct CompactionStats {
    /// Number of incrementals merged
    pub incrementals_merged: usize,
    /// Total bytes of merged incrementals
    pub bytes_merged: u64,
    /// New snapshot TXID range
    pub new_snapshot_txid: u64,
    /// S3 key of new snapshot
    pub new_snapshot_key: String,
    /// Incrementals deleted (if cleanup enabled)
    pub incrementals_deleted: usize,
}

/// Configuration for incremental compaction
#[derive(Debug, Clone)]
pub struct CompactionConfig {
    /// Minimum number of incrementals before compacting
    pub min_incrementals: usize,
    /// Maximum total size of incrementals before compacting (bytes)
    pub max_incremental_bytes: u64,
    /// Maximum age of oldest incremental before compacting (seconds)
    pub max_incremental_age_secs: u64,
    /// Delete incrementals after successful compaction
    pub delete_incrementals: bool,
}

impl Default for CompactionConfig {
    fn default() -> Self {
        Self {
            min_incrementals: 10,
            max_incremental_bytes: 100 * 1024 * 1024, // 100 MB
            max_incremental_age_secs: 3600,            // 1 hour
            delete_incrementals: true,
        }
    }
}

/// Read the change counter (TXID) from SQLite database header
async fn read_database_txid(db_path: &Path) -> Result<u64> {
    use tokio::io::AsyncReadExt;
    let mut file = tokio::fs::File::open(db_path).await?;
    let mut header = [0u8; 100];
    file.read_exact(&mut header).await?;

    // Change counter is at offset 24-27, big-endian
    let change_counter = u32::from_be_bytes([header[24], header[25], header[26], header[27]]);

    Ok(change_counter as u64)
}

/// Compact incrementals in generation 0 into a new snapshot
///
/// This function:
/// 1. Lists all incrementals in generation 0
/// 2. Downloads the latest snapshot (from generation 1+) if exists
/// 3. Applies all incrementals to restore the full database state
/// 4. Creates a new snapshot with all data merged
/// 5. Uploads new snapshot to generation 1 (or higher)
/// 6. Optionally deletes old incrementals from generation 0
pub async fn compact_incrementals(
    name: &str,
    bucket: &str,
    endpoint: Option<&str>,
    config: &CompactionConfig,
    force: bool,
) -> Result<Option<CompactionStats>> {
    let (bucket_name, prefix) = parse_bucket(bucket);
    let client = create_client(endpoint).await?;

    // List all files in generation 0 (incrementals)
    let gen0_prefix = format!("{}{}/0000/", prefix, name);
    let gen0_files = s3::list_objects(&client, &bucket_name, &gen0_prefix).await?;

    // Parse incremental files (key only, size estimated later)
    let mut incrementals: Vec<(String, u64, u64)> = Vec::new(); // (key, min_txid, max_txid)

    for key in &gen0_files {
        if let Some(filename) = key.strip_prefix(&gen0_prefix) {
            if filename.ends_with(".ltx") {
                // Parse TXID range from filename: {min}-{max}.ltx
                if let Some((min_str, rest)) = filename.strip_suffix(".ltx").and_then(|f| f.split_once('-')) {
                    if let (Ok(min_txid), Ok(max_txid)) = (
                        u64::from_str_radix(min_str, 16),
                        u64::from_str_radix(rest, 16),
                    ) {
                        // Skip snapshots (min_txid == 1)
                        if min_txid > 1 {
                            incrementals.push((key.clone(), min_txid, max_txid));
                        }
                    }
                }
            }
        }
    }

    // Sort by min_txid
    incrementals.sort_by_key(|(_, min_txid, _)| *min_txid);

    // Check if compaction is needed (based on count only, no size info available)
    if incrementals.len() < config.min_incrementals {
        tracing::debug!(
            "Compaction not needed: {} incrementals (threshold: {})",
            incrementals.len(),
            config.min_incrementals
        );
        return Ok(None);
    }

    tracing::info!(
        "Compacting {} incrementals for database '{}'",
        incrementals.len(),
        name
    );

    // Create temp directory for restoration
    let temp_dir = tempfile::tempdir()?;
    let restore_path = temp_dir.path().join(format!("{}.db", name));

    // Restore to temp file (this applies snapshot + all incrementals)
    restore(
        name,
        restore_path.as_path(),
        bucket,
        endpoint,
        None,
        None, // No cache for validation
    )
    .await?;

    // Get page size from restored database
    let page_size = get_page_size(&restore_path).await?;

    // Get the max TXID from the restored database
    let restored_txid = read_database_txid(&restore_path).await?;

    // Determine generation for new snapshot (use generation 1 for compacted snapshots)
    let snapshot_gen = 1u32;
    let gen_folder = format!("{:04x}", snapshot_gen);

    // Create LTX snapshot buffer
    let db_path_for_encode = restore_path.clone();
    let (ltx_buffer, _) = tokio::task::spawn_blocking(move || {
        let mut ltx_buffer = Vec::new();
        crate::ltx::encode_snapshot(&mut ltx_buffer, &db_path_for_encode, page_size, restored_txid)
            .map_err(|e| anyhow::anyhow!("Compaction snapshot encode failed: {}", e))?;
        let db_checksum = crate::ltx::compute_checksum_from_file(&db_path_for_encode)?;
        Ok::<_, anyhow::Error>((ltx_buffer, db_checksum))
    })
    .await??;

    let ltx_size = ltx_buffer.len() as u64;

    // Upload new snapshot to S3
    let s3_key = format!("{}{}/{}/{:016x}-{:016x}.ltx", prefix, name, gen_folder, 1u64, restored_txid);

    if !force {
        println!("Dry-run mode: would upload snapshot to {}", s3_key);
        println!("  New snapshot: TXID 1-{}", restored_txid);
        println!("  Size: {} bytes", ltx_size);
        println!("  Would delete {} incrementals", incrementals.len());
        return Ok(None);
    }

    tracing::info!("Uploading compacted snapshot: {}", s3_key);
    s3::upload_bytes(&client, &bucket_name, &s3_key, ltx_buffer).await?;

    // Delete old incrementals if configured
    let mut deleted_count = 0;
    if config.delete_incrementals {
        let keys_to_delete: Vec<String> = incrementals.iter().map(|(k, _, _)| k.clone()).collect();
        deleted_count = s3::delete_objects(&client, &bucket_name, &keys_to_delete).await?;
        tracing::info!("Deleted {} incrementals after compaction", deleted_count);
    }

    Ok(Some(CompactionStats {
        incrementals_merged: incrementals.len(),
        bytes_merged: ltx_size, // Use new snapshot size as proxy
        new_snapshot_txid: restored_txid,
        new_snapshot_key: s3_key,
        incrementals_deleted: deleted_count,
    }))
}

/// Check if compaction should be triggered based on config
pub async fn should_compact(
    name: &str,
    bucket: &str,
    endpoint: Option<&str>,
    config: &CompactionConfig,
) -> Result<bool> {
    let (bucket_name, prefix) = parse_bucket(bucket);
    let client = create_client(endpoint).await?;

    // List files in generation 0
    let gen0_prefix = format!("{}{}/0000/", prefix, name);
    let gen0_files = s3::list_objects(&client, &bucket_name, &gen0_prefix).await?;

    let mut incremental_count = 0;

    for key in &gen0_files {
        if let Some(filename) = key.strip_prefix(&gen0_prefix) {
            if filename.ends_with(".ltx") {
                if let Some((min_str, _rest)) = filename.strip_suffix(".ltx").and_then(|f| f.split_once('-')) {
                    if let Ok(min_txid) = u64::from_str_radix(min_str, 16) {
                        if min_txid > 1 {
                            // It's an incremental
                            incremental_count += 1;
                        }
                    }
                }
            }
        }
    }

    Ok(incremental_count >= config.min_incrementals)
}

/// Format age of a snapshot in human-readable form
fn format_age(now: chrono::DateTime<Utc>, created_at: chrono::DateTime<Utc>) -> String {
    let age = now.signed_duration_since(created_at);

    if age.num_hours() < 1 {
        format!("{} min ago", age.num_minutes())
    } else if age.num_hours() < 24 {
        format!("{} hours ago", age.num_hours())
    } else if age.num_days() < 7 {
        format!("{} days ago", age.num_days())
    } else if age.num_weeks() < 12 {
        format!("{} weeks ago", age.num_weeks())
    } else {
        format!("{} months ago", age.num_days() / 30)
    }
}


/// Take immediate snapshot as LTX file
pub async fn snapshot(database: &Path, bucket: &str, endpoint: Option<&str>) -> Result<()> {
    let (bucket_name, prefix) = parse_bucket(bucket);
    let client = create_client(endpoint).await?;

    if !database.exists() {
        return Err(anyhow!("Database not found: {}", database.display()));
    }

    let name = database
        .file_stem()
        .and_then(|s| s.to_str())
        .ok_or_else(|| anyhow!("Invalid database path"))?;

    // Get page size from database header
    let page_size = get_page_size(database).await?;

    // Discover current state from S3 to get current TXID and generation
    let (current_txid, current_gen, _) = discover_state_from_s3(&client, &bucket_name, &prefix, name).await?;
    let new_txid = current_txid + 1;
    let snapshot_gen = current_gen + 1;

    // Snapshots go to generation 1+ (litestream format)
    let ltx_key = build_ltx_key(&prefix, name, snapshot_gen, 1, new_txid);

    // Encode database as LTX
    // Pre-allocate buffer: estimate 2x db size for compression headroom
    let db_size = std::fs::metadata(database)?.len() as usize;
    let estimated_size = db_size.saturating_mul(2);
    let mut ltx_buffer = Vec::with_capacity(estimated_size);
    ltx::encode_snapshot(&mut ltx_buffer, database, page_size, new_txid)?;

    let ltx_size = ltx_buffer.len() as u64;

    // Upload LTX file
    s3::upload_bytes(&client, &bucket_name, &ltx_key, ltx_buffer).await?;

    tracing::info!(
        "LTX snapshot uploaded (gen {}, TXID 1-{}, {} bytes) -> {}",
        snapshot_gen,
        new_txid,
        ltx_size,
        ltx_key
    );
    println!(
        "Snapshot uploaded: s3://{}/{} (gen {}, TXID 1-{})",
        bucket_name, ltx_key, snapshot_gen, new_txid
    );
    Ok(())
}

/// Run as a read replica, polling S3 for new LTX files and applying them locally
///
/// This command:
/// 1. Bootstraps the local database from the latest snapshot if it doesn't exist
/// 2. Polls S3 at the specified interval for new LTX files
/// 3. Downloads and applies incremental LTX files in-place
/// 4. Tracks progress using TXID to know where we left off
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
async fn validate_backup_integrity(
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
pub(crate) async fn get_wal_page_count(wal_path: &Path) -> Result<u64> {
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
pub(crate) enum CheckpointMode {
    Passive,
    Truncate,
}
pub(crate) async fn run_checkpoint(db_path: &Path, mode: CheckpointMode) -> Result<()> {
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


/// Compute SHA-256 checksum of a file
pub(crate) async fn compute_file_sha256(path: &Path) -> Result<String> {
    use sha2::{Digest, Sha256};
    use tokio::io::AsyncReadExt;

    let mut file = tokio::fs::File::open(path).await?;
    let mut hasher = Sha256::new();
    let mut buffer = vec![0u8; 8192];

    loop {
        let n = file.read(&mut buffer).await?;
        if n == 0 {
            break;
        }
        hasher.update(&buffer[..n]);
    }

    Ok(format!("{:x}", hasher.finalize()))
}
