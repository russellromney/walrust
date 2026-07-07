use anyhow::{anyhow, Result};
use chrono::Utc;
use hadb_storage_s3::S3Storage;
use std::path::Path;
use walrust_core::legacy_manifest::plan_legacy_compaction;
use walrust_core::legacy_wal_sync::snapshot_database_to_storage;

use crate::retention::{RetentionPolicy, SnapshotEntry};
use crate::s3::{self, create_client, parse_bucket};

use super::manifest::discover_snapshots_from_s3;

pub async fn compact(
    name: &str,
    bucket: &str,
    endpoint: Option<&str>,
    policy: &RetentionPolicy,
    force: bool,
) -> Result<()> {
    let (bucket_name, prefix) = parse_bucket(bucket);
    let client = create_client(endpoint).await?;

    // Discover snapshots from the S3 listing — the production watch path never
    // writes a manifest.json, so reading one made compact a silent no-op (F6).
    // The key here is the FULL S3 key (verify/restore use full keys too).
    let discovered = discover_snapshots_from_s3(&client, &bucket_name, &prefix, name).await?;

    if discovered.is_empty() {
        println!("No snapshots found for database '{}'", name);
        return Ok(());
    }

    // HEAD each snapshot for size + last-modified to build retention entries.
    let mut snapshot_entries: Vec<SnapshotEntry> = Vec::with_capacity(discovered.len());
    for (key, _gen, _min, max) in &discovered {
        let meta = s3::head_object_meta(&client, &bucket_name, key).await?;
        snapshot_entries.push(SnapshotEntry {
            key: key.clone(),
            created_at: meta.last_modified,
            sequence: *max,
            size: meta.size,
        });
    }

    if snapshot_entries.is_empty() {
        println!("No snapshots found for database '{}'", name);
        return Ok(());
    }

    let now = Utc::now();
    let storage = S3Storage::new(client.clone(), bucket_name.clone());
    let plan_before_reachability =
        crate::retention::analyze_retention(&snapshot_entries, policy, now);
    let plan =
        plan_legacy_compaction(&storage, &prefix, name, &snapshot_entries, policy, now).await?;
    let before = plan.delete.len();
    let rescued = plan_before_reachability.delete.len().saturating_sub(before);
    if rescued > 0 {
        tracing::info!(
            "Compaction: retained {} snapshot(s) as reachability base for the incremental chain (F7)",
            rescued
        );
    }

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
            entry.key,
            entry.sequence,
            format_age(now, entry.created_at)
        );
    }
    println!();

    // Print what will be deleted
    println!("Deleting {} snapshots:", plan.delete.len());
    for entry in &plan.delete {
        println!(
            "  {} (TXID: {}, {})",
            entry.key,
            entry.sequence,
            format_age(now, entry.created_at)
        );
    }
    println!();

    if !force {
        println!("Dry-run mode: no files deleted. Use --force to actually delete.");
        return Ok(());
    }

    // Actually delete files. `e.key` is already the full S3 key from listing.
    println!("Deleting files...");

    let keys_to_delete: Vec<String> = plan.delete.iter().map(|e| e.key.clone()).collect();

    let deleted_count = s3::delete_objects(&client, &bucket_name, &keys_to_delete).await?;

    tracing::info!("Deleted {} snapshot files", deleted_count);

    // No manifest to update — discovery is by S3 listing, so the next compact
    // run simply re-lists and sees the deletions reflected.

    println!(
        "Compaction complete: deleted {} snapshots, freed {:.2} MB",
        deleted_count,
        plan.bytes_freed as f64 / (1024.0 * 1024.0)
    );

    Ok(())
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

    let storage = S3Storage::new(client, bucket_name.clone());
    let output = snapshot_database_to_storage(&storage, &prefix, name, database).await?;

    println!(
        "Snapshot uploaded: s3://{}/{} (gen {}, TXID 1-{})",
        bucket_name, output.key, output.generation, output.max_txid
    );
    Ok(())
}
