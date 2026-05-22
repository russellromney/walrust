use anyhow::{anyhow, Result};
use chrono::Utc;
use std::path::Path;

use crate::ltx;
use crate::retention::{self, RetentionPolicy, SnapshotEntry};
use crate::s3::{self, create_client, parse_bucket};

use super::manifest::{
    build_ltx_key, discover_snapshots_from_s3, discover_state_from_s3, list_generation_files,
    GENERATION_LIVE,
};
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
    let mut plan = retention::analyze_retention(&snapshot_entries, policy, now);

    // Chain-reachability guard (F7): a restore replays the latest snapshot whose
    // coverage is <= the target, then the live incrementals after it. Deleting a
    // snapshot that a retained incremental chain still depends on would orphan
    // that chain. Protect any snapshot needed as a base:
    //   - the highest-TXID snapshot overall (the current restore base), and
    //   - for the earliest retained live incremental, the latest snapshot at or
    //     below its start.
    // Such snapshots are pulled out of the delete set even if the time/size
    // policy would drop them.
    let live_incrementals =
        list_generation_files(&client, &bucket_name, &prefix, name, GENERATION_LIVE).await?;
    let earliest_live_min = live_incrementals
        .iter()
        .filter(|(_, min, max)| !(*min == 1 && *max == 1)) // skip the gen-0 base
        .map(|(_, min, _)| *min)
        .min();

    let max_snapshot_txid = snapshot_entries.iter().map(|e| e.sequence).max();
    // The base needed for the live chain: the latest snapshot whose coverage
    // ends at or before the earliest retained incremental's start.
    let base_for_live_chain = earliest_live_min.and_then(|min| {
        snapshot_entries
            .iter()
            .filter(|e| e.sequence < min)
            .map(|e| e.sequence)
            .max()
            .or_else(|| {
                // No snapshot strictly before the chain start: the lowest
                // snapshot is the base.
                snapshot_entries.iter().map(|e| e.sequence).min()
            })
    });

    let protected: std::collections::HashSet<u64> = max_snapshot_txid
        .into_iter()
        .chain(base_for_live_chain)
        .collect();

    let before = plan.delete.len();
    let rescued: Vec<_> = plan
        .delete
        .iter()
        .filter(|e| protected.contains(&e.sequence))
        .cloned()
        .collect();
    if !rescued.is_empty() {
        plan.delete.retain(|e| !protected.contains(&e.sequence));
        for e in &rescued {
            if !plan.keep.iter().any(|k| k.sequence == e.sequence) {
                plan.keep.push(e.clone());
            }
            // Don't count rescued snapshots' bytes as freed.
            plan.bytes_freed = plan.bytes_freed.saturating_sub(e.size);
        }
        tracing::info!(
            "Compaction: retained {} snapshot(s) as reachability base for the incremental chain (F7)",
            before - plan.delete.len()
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

    // Get page size from database header
    let page_size = get_page_size(database).await?;

    // Discover current state from S3 to get current TXID and generation
    let (current_txid, current_gen, _) =
        discover_state_from_s3(&client, &bucket_name, &prefix, name).await?;
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
