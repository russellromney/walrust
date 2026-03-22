use anyhow::{anyhow, Result};
use chrono::Utc;
use std::path::Path;

use crate::config::Config;
use crate::ltx;
use crate::retention::{self, RetentionPolicy, SnapshotEntry};
use crate::s3::{self, create_client, parse_bucket};

use super::manifest::{build_ltx_key, discover_state_from_s3, load_manifest, save_manifest};
use super::types::{LtxEntry, Manifest};
use super::wal_sync::get_page_size;

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

            // Validation
            println!("Validation:");
            if cfg.sync.validation_interval > 0 {
                println!("  Interval: {} seconds ({} hours)",
                    cfg.sync.validation_interval,
                    cfg.sync.validation_interval / 3600
                );
                println!("  Checks: File existence, header validity, checksums, TXID continuity");
            } else {
                println!("  Disabled (recommended: enable with --validation-interval 86400 for daily checks)");
            }
            println!();

            // Webhooks
            println!("Webhook Notifications:");
            if cfg.webhooks.is_empty() {
                println!("  None configured");
            } else {
                for (i, webhook) in cfg.webhooks.iter().enumerate() {
                    println!("  {}. {}", i + 1, webhook.url);
                    println!("     Events: {}", webhook.events.join(", "));
                    if webhook.secret.is_some() {
                        println!("     HMAC:   enabled (X-Walrust-Signature header)");
                    }
                }
            }
            println!();

            // Summary with cost estimation
            let total_snapshots = cfg.retention.hourly + cfg.retention.daily
                + cfg.retention.weekly + cfg.retention.monthly;
            println!("Summary:");
            println!("  Max snapshots retained per database: ~{}", total_snapshots);
            if cfg.sync.compact_after_snapshot || cfg.sync.compact_interval > 0 {
                println!("  Automatic compaction: enabled");
            } else {
                println!("  Automatic compaction: disabled (run 'walrust compact' manually)");
            }

            // Cost estimation
            match cfg.resolve_databases() {
                Ok(resolved) if !resolved.is_empty() => {
                    println!();
                    println!("Estimated Storage Costs:");
                    println!("  Note: Assumes average database size of 1GB per database");
                    println!();

                    let db_count = resolved.len();
                    let avg_db_size_gb = 1.0; // Conservative estimate
                    let snapshots_per_db = total_snapshots as f64;

                    // Tigris/S3 pricing (Tigris: ~$0.02/GB/month)
                    let storage_gb = db_count as f64 * avg_db_size_gb * snapshots_per_db;
                    let cost_tigris = storage_gb * 0.02;
                    let cost_s3 = storage_gb * 0.023; // S3 Standard pricing

                    println!("  Total snapshots: {} databases × {} snapshots = {} snapshots",
                        db_count, snapshots_per_db, db_count as f64 * snapshots_per_db);
                    println!("  Estimated storage: {:.1} GB", storage_gb);
                    println!("  Monthly cost (Tigris): ~${:.2}", cost_tigris);
                    println!("  Monthly cost (S3 Standard): ~${:.2}", cost_s3);
                    println!();
                    println!("  Actual costs depend on:");
                    println!("  - Real database sizes (current estimate: {}GB per DB)", avg_db_size_gb);
                    println!("  - Compression ratio (LTX typically compresses well)");
                    println!("  - Incremental file sizes between snapshots");
                }
                _ => {}
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
