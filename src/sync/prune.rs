use anyhow::Result;
use chrono::Utc;
use hadb_storage::StorageBackend;
use hadb_storage_s3::S3Storage;
use std::path::Path;
use std::sync::Arc;
use walrust_core::compaction::{list_level_files, plan_level_prune, RangeLayout};
use walrust_core::legacy_manifest::plan_legacy_prune;
use walrust_core::legacy_wal_sync::{checkpoint_wal_truncate, snapshot_database_to_storage};

use crate::errors::{classify_or_else, WalrustError};
use crate::retention::{RetentionPolicy, SnapshotEntry};
use crate::s3::{self, create_client, parse_bucket};

use super::manifest::discover_snapshots_from_s3;

pub async fn prune(
    name: &str,
    bucket: &str,
    endpoint: Option<&str>,
    policy: &RetentionPolicy,
    force: bool,
) -> Result<()> {
    let (bucket_name, prefix) = parse_bucket(bucket);
    let client = create_client(endpoint)
        .await
        .map_err(|e| classify_or_else(e, WalrustError::s3))?;

    // Discover snapshots from the S3 listing — the production watch path never
    // writes a manifest.json, so reading one made prune a silent no-op (F6).
    // The key here is the FULL S3 key (verify/restore use full keys too).
    let discovered = discover_snapshots_from_s3(&client, &bucket_name, &prefix, name)
        .await
        .map_err(|e| classify_or_else(e, WalrustError::s3))?;

    if discovered.is_empty() {
        println!("No snapshots found for database '{}'", name);
        return Ok(());
    }

    // HEAD each snapshot for size + last-modified to build retention entries.
    let mut snapshot_entries: Vec<SnapshotEntry> = Vec::with_capacity(discovered.len());
    for (key, _gen, _min, max) in &discovered {
        let meta = s3::head_object_meta(&client, &bucket_name, key)
            .await
            .map_err(|e| classify_or_else(e, WalrustError::s3))?;
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
    let plan = plan_legacy_prune(&storage, &prefix, name, &snapshot_entries, policy, now)
        .await
        .map_err(|e| classify_or_else(e, WalrustError::s3))?;
    let before = plan.delete.len();
    let rescued = plan_before_reachability.delete.len().saturating_sub(before);
    if rescued > 0 {
        tracing::info!(
            "Prune: retained {} snapshot(s) as reachability base for the incremental chain (F7)",
            rescued
        );
    }

    // Level-aware watermark prune (E2, now level-aware): the oldest surviving
    // snapshot's seq is the earliest restorable point; merged compaction objects
    // (under `{db}/levels/L*/`) whose whole range ends below it can never be part
    // of a retained restore and are deletable — keeping the newest object per
    // level and never a watermark-straddling object a retained PITR still needs.
    let watermark = plan.keep.iter().map(|e| e.sequence).min().unwrap_or(0);
    let level_storage: Arc<dyn StorageBackend> =
        Arc::new(S3Storage::new(client.clone(), bucket_name.clone()));
    let level_layout = RangeLayout::new(level_storage, &prefix, name);
    // E10 sibling (fail-loud, not fail-silent): this DELETION plan must come
    // from a complete levels listing. Swallowing a failed LIST with
    // `unwrap_or_default()` silently skipped the level prune and misreported
    // "Nothing to delete" — a decision made from a defaulted view. The
    // direction was at least conservative (nothing extra deleted), but a
    // persistent LIST failure would leak level objects forever without a word.
    // Propagate instead; the operator retries the prune.
    let level_files = list_level_files(&level_layout)
        .await
        .map_err(|e| classify_or_else(anyhow::Error::from(e), WalrustError::s3))?;
    let level_delete = plan_level_prune(&level_files, watermark);

    // Print summary
    println!("Prune plan for '{}':", name);
    println!("  {}", plan.summary());
    println!();

    if !plan.has_deletions() && level_delete.is_empty() {
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
    // Declare the retained restore floor explicitly (E2): the oldest kept
    // snapshot is the earliest point-in-time that stays restorable after this
    // prune. Anything below it is being dropped on purpose.
    if let Some(floor) = plan.keep.iter().map(|e| e.sequence).min() {
        println!(
            "Earliest restorable point-in-time after pruning: TXID {} \
             (older point-in-time restores are no longer available)",
            floor
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
    if !level_delete.is_empty() {
        println!(
            "Deleting {} merged compaction object(s) below the retained watermark (TXID {}):",
            level_delete.len(),
            watermark
        );
        for f in &level_delete {
            println!("  {} (seq range {}-{})", f.key, f.range.min, f.range.max);
        }
    }
    println!();

    if !force {
        println!("Dry-run mode: no files deleted. Use --force to actually delete.");
        return Ok(());
    }

    // Actually delete files. `e.key` is already the full S3 key from listing.
    println!("Deleting files...");

    let mut keys_to_delete: Vec<String> = plan.delete.iter().map(|e| e.key.clone()).collect();
    keys_to_delete.extend(level_delete.iter().map(|f| f.key.clone()));

    let deleted_count = s3::delete_objects(&client, &bucket_name, &keys_to_delete)
        .await
        .map_err(|e| classify_or_else(e, WalrustError::s3))?;

    tracing::info!("Deleted {} snapshot files", deleted_count);

    // No manifest to update — discovery is by S3 listing, so the next prune
    // run simply re-lists and sees the deletions reflected.

    println!(
        "Prune complete: deleted {} snapshots, freed {:.2} MB",
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
    if !database.exists() {
        return Err(
            WalrustError::database(format!("Database not found: {}", database.display())).into(),
        );
    }

    let name = database
        .file_stem()
        .and_then(|s| s.to_str())
        .ok_or_else(|| WalrustError::database("Invalid database path"))?;

    // E6: a running watcher pins the WAL via its checkpoint blocker, so a manual
    // snapshot's fail-closed TRUNCATE would surface a confusing "checkpoint
    // incomplete (busy=1...)" error. Detect that case up front via the E5 lock
    // and return an actionable message instead.
    if crate::lock::DbLock::is_held_by_another(database) {
        return Err(WalrustError::database(format!(
            "a walrust watch process holds the checkpoint blocker for {}. \
             Snapshots are taken automatically by the watcher — use its snapshot \
             interval/triggers, or stop the watcher before taking a manual snapshot.",
            database.display()
        ))
        .into());
    }

    let (bucket_name, prefix) = parse_bucket(bucket);
    let client = create_client(endpoint)
        .await
        .map_err(|e| classify_or_else(e, WalrustError::s3))?;
    let storage = S3Storage::new(client, bucket_name.clone());
    // The one-shot `walrust snapshot` command has no shadow WAL to carry
    // un-folded frames as incrementals, so fully fold the WAL into the base image
    // with a completeness-checked TRUNCATE (busy_timeout + result-row check)
    // before encoding. This hard-fails if another process (e.g. a running
    // watcher) pins the WAL — correct fail-closed behavior for a manual snapshot,
    // rather than silently producing a base missing recent commits (B10).
    checkpoint_wal_truncate(database).await?;
    let output = snapshot_database_to_storage(&storage, &prefix, name, database, None).await?;

    println!(
        "Snapshot uploaded: s3://{}/{} (gen {}, TXID 1-{})",
        bucket_name, output.key, output.generation, output.max_txid
    );
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    /// E6: when a watcher holds the single-writer lock, `snapshot` must return
    /// an actionable error naming the watcher — before it ever reaches the
    /// fail-closed WAL TRUNCATE that would otherwise surface a confusing
    /// "checkpoint incomplete (busy=1...)".
    #[tokio::test]
    async fn e6_snapshot_reports_actionable_error_when_watcher_holds_lock() {
        let dir = tempdir().unwrap();
        let db = dir.path().join("app.db");
        std::fs::write(&db, b"SQLite format 3\0").unwrap();

        // Simulate a running watcher owning the DB.
        let _watcher_lock = crate::lock::DbLock::acquire(&db).unwrap();

        let err = snapshot(&db, "s3://bucket/prefix", None)
            .await
            .expect_err("snapshot must refuse while a watcher owns the DB");
        let msg = err.to_string();
        assert!(
            msg.contains("holds the checkpoint blocker") && msg.contains("stop the watcher"),
            "message must be actionable, got: {msg}"
        );
    }
}
