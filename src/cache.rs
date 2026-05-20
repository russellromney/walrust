//! Local disk cache for LTX files.
//!
//! Implements Litestream-style disk-based upload queue:
//! - WAL encoding writes LTX to disk cache (atomic via .tmp rename)
//! - Independent uploader task reads from disk cache and uploads to S3
//! - Crash recovery: pending uploads resume from disk on restart
//! - Fast local restore: cache acts as local backup without S3 fetch
//!
//! Directory Structure:
//! ```text
//! /path/to/.app.db-walrust/
//!   manifest.json              # Upload state tracking
//!   ltx/
//!     00000001.ltx             # TXID 1 (uploaded)
//!     00000002.ltx             # TXID 2 (uploaded)
//!     00000003.ltx             # TXID 3 (pending)
//! ```

use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

/// Manifest tracking upload state and cache metadata
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CacheManifest {
    /// Highest TXID for which a PUT has been confirmed durable. May be ahead of
    /// `last_contiguous_uploaded_txid` when uploads complete out of order, so it
    /// is NOT a safe restore cursor on its own (a lower TXID may still be
    /// missing). Kept for stats / observability.
    pub last_uploaded_txid: u64,
    /// Highest TXID `T` such that EVERY TXID in `1..=T` has a confirmed durable
    /// PUT. This is the durable restore cursor: a node reseeding from remote
    /// state can replay up to here with no gap. Advances only after confirmed
    /// uploads, never on a mere cache write (F10).
    #[serde(default)]
    pub last_contiguous_uploaded_txid: u64,
    /// Set of pending TXIDs (written to cache but not uploaded)
    pub pending_txids: HashSet<u64>,
    /// TXIDs whose upload exhausted retries and failed. A non-empty set is a
    /// permanent gap that must be surfaced, not silently hidden by advancing a
    /// max-based cursor past it (F9).
    #[serde(default)]
    pub failed_txids: HashSet<u64>,
    /// Total cache size in bytes
    pub cache_size_bytes: u64,
    /// Last cleanup timestamp
    pub last_cleanup: DateTime<Utc>,
    /// Per-TXID metadata (size, timestamp)
    pub entries: HashMap<u64, CacheEntry>,
}

/// Metadata for a single cached LTX file
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CacheEntry {
    /// TXID
    pub txid: u64,
    /// File size in bytes
    pub size: u64,
    /// When this LTX was written to cache
    pub created_at: DateTime<Utc>,
    /// Upload status
    pub uploaded: bool,
    /// When uploaded (if uploaded)
    pub uploaded_at: Option<DateTime<Utc>>,
    /// True if this LTX is a full-DB snapshot (a restore base), not an
    /// incremental. The cleanup floor never evicts the latest snapshot or the
    /// incremental chain built on it (F8).
    #[serde(default)]
    pub is_snapshot: bool,
}

impl Default for CacheManifest {
    fn default() -> Self {
        Self {
            last_uploaded_txid: 0,
            last_contiguous_uploaded_txid: 0,
            pending_txids: HashSet::new(),
            failed_txids: HashSet::new(),
            cache_size_bytes: 0,
            last_cleanup: Utc::now(),
            entries: HashMap::new(),
        }
    }
}

/// Local LTX cache with atomic operations and crash recovery
pub struct LocalCache {
    /// Cache directory path
    cache_dir: PathBuf,
    /// Manifest file path
    manifest_path: PathBuf,
    /// In-memory manifest (synchronized to disk)
    manifest: Arc<Mutex<CacheManifest>>,
}

impl LocalCache {
    /// Create a new LocalCache for a database
    ///
    /// Creates directory structure:
    /// - `{db_path}-walrust/manifest.json`
    /// - `{db_path}-walrust/ltx/`
    pub fn new(db_path: &Path) -> Result<Self> {
        let cache_dir = Self::cache_dir_for_db(db_path);
        let manifest_path = cache_dir.join("manifest.json");

        // Create cache directory structure
        fs::create_dir_all(cache_dir.join("ltx")).context("Failed to create cache directory")?;

        // Load or create manifest
        let manifest = if manifest_path.exists() {
            Self::load_manifest(&manifest_path)?
        } else {
            let default_manifest = CacheManifest::default();
            // Save default manifest to disk
            let json = serde_json::to_string_pretty(&default_manifest)
                .context("Failed to serialize default manifest")?;
            fs::write(&manifest_path, json).context("Failed to write default manifest")?;
            default_manifest
        };

        Ok(Self {
            cache_dir,
            manifest_path,
            manifest: Arc::new(Mutex::new(manifest)),
        })
    }

    /// Get cache directory path for a database
    pub fn cache_dir_for_db(db_path: &Path) -> PathBuf {
        let parent = db_path.parent().unwrap_or(Path::new("."));
        let db_name = db_path.file_name().unwrap().to_string_lossy();
        parent.join(format!(".{}-walrust", db_name))
    }

    /// Open an existing cache from a directory path (read-only, for restore)
    ///
    /// Unlike `new()`, this does not create the cache if it doesn't exist.
    /// Returns None if the cache directory or manifest doesn't exist.
    pub fn open(cache_dir: &Path) -> Result<Option<Self>> {
        let manifest_path = cache_dir.join("manifest.json");

        if !manifest_path.exists() {
            return Ok(None);
        }

        let manifest = Self::load_manifest(&manifest_path)?;

        Ok(Some(Self {
            cache_dir: cache_dir.to_path_buf(),
            manifest_path,
            manifest: Arc::new(Mutex::new(manifest)),
        }))
    }

    /// Get all available TXIDs in cache (sorted ascending)
    pub fn available_txids(&self) -> Vec<u64> {
        let manifest = self.manifest.lock().unwrap();
        let mut txids: Vec<u64> = manifest.entries.keys().copied().collect();
        txids.sort();
        txids
    }

    /// Check if a specific TXID is available in cache
    pub fn has_txid(&self, txid: u64) -> bool {
        let manifest = self.manifest.lock().unwrap();
        manifest.entries.contains_key(&txid)
    }

    /// Get the TXID range in cache (min, max)
    ///
    /// Returns None if cache is empty
    pub fn txid_range(&self) -> Option<(u64, u64)> {
        let manifest = self.manifest.lock().unwrap();
        if manifest.entries.is_empty() {
            return None;
        }
        let min = manifest.entries.keys().min().copied().unwrap();
        let max = manifest.entries.keys().max().copied().unwrap();
        Some((min, max))
    }

    /// Check if cache contains a continuous range of TXIDs from start to end (inclusive)
    pub fn has_continuous_range(&self, start: u64, end: u64) -> bool {
        let manifest = self.manifest.lock().unwrap();
        for txid in start..=end {
            if !manifest.entries.contains_key(&txid) {
                return false;
            }
        }
        true
    }

    /// Get cache directory path
    pub fn cache_dir(&self) -> &Path {
        &self.cache_dir
    }

    /// Load manifest from disk
    fn load_manifest(path: &Path) -> Result<CacheManifest> {
        let contents = fs::read_to_string(path).context("Failed to read manifest")?;
        let manifest: CacheManifest =
            serde_json::from_str(&contents).context("Failed to parse manifest JSON")?;
        Ok(manifest)
    }

    /// Save manifest to disk atomically
    fn save_manifest(&self, manifest: &CacheManifest) -> Result<()> {
        let tmp_path = self.manifest_path.with_extension("tmp");

        // Write to temp file
        let json =
            serde_json::to_string_pretty(manifest).context("Failed to serialize manifest")?;
        fs::write(&tmp_path, json).context("Failed to write temporary manifest")?;

        // Atomic rename
        fs::rename(&tmp_path, &self.manifest_path).context("Failed to rename manifest")?;

        Ok(())
    }

    /// Write an incremental LTX to cache atomically.
    pub fn write_ltx(&self, txid: u64, data: &[u8]) -> Result<()> {
        self.write_ltx_inner(txid, data, false)
    }

    /// Write a snapshot (full-DB base) LTX to cache atomically. Marked so the
    /// cleanup floor never evicts the latest restore base (F8).
    pub fn write_snapshot_ltx(&self, txid: u64, data: &[u8]) -> Result<()> {
        self.write_ltx_inner(txid, data, true)
    }

    fn write_ltx_inner(&self, txid: u64, data: &[u8], is_snapshot: bool) -> Result<()> {
        let ltx_path = self.ltx_path(txid);
        let tmp_path = ltx_path.with_extension("tmp");

        // Write to temp file
        fs::write(&tmp_path, data)
            .with_context(|| format!("Failed to write LTX temp file for TXID {}", txid))?;

        // Atomic rename
        fs::rename(&tmp_path, &ltx_path)
            .with_context(|| format!("Failed to rename LTX file for TXID {}", txid))?;

        // Update manifest
        let mut manifest = self.manifest.lock().unwrap();
        manifest.pending_txids.insert(txid);
        manifest.cache_size_bytes += data.len() as u64;
        manifest.entries.insert(
            txid,
            CacheEntry {
                txid,
                size: data.len() as u64,
                created_at: Utc::now(),
                uploaded: false,
                uploaded_at: None,
                is_snapshot,
            },
        );

        self.save_manifest(&manifest)?;

        Ok(())
    }

    /// Read LTX from cache
    pub fn read_ltx(&self, txid: u64) -> Result<Vec<u8>> {
        let ltx_path = self.ltx_path(txid);
        fs::read(&ltx_path).with_context(|| format!("Failed to read LTX file for TXID {}", txid))
    }

    /// Mark TXID as uploaded (PUT confirmed durable).
    ///
    /// Removes from pending/failed sets, records the timestamp, and advances
    /// both cursors: `last_uploaded_txid` to the max seen, and
    /// `last_contiguous_uploaded_txid` forward across the now-complete
    /// gap-free prefix. The contiguous cursor is the only safe restore point
    /// because uploads can complete out of order (F9/F10).
    pub fn mark_uploaded(&self, txid: u64) -> Result<()> {
        let mut manifest = self.manifest.lock().unwrap();

        manifest.pending_txids.remove(&txid);
        manifest.failed_txids.remove(&txid);
        manifest.last_uploaded_txid = manifest.last_uploaded_txid.max(txid);

        if let Some(entry) = manifest.entries.get_mut(&txid) {
            entry.uploaded = true;
            entry.uploaded_at = Some(Utc::now());
        }

        Self::recompute_contiguous(&mut manifest);

        self.save_manifest(&manifest)?;

        Ok(())
    }

    /// Mark a TXID as permanently failed (uploader exhausted retries).
    ///
    /// Records the gap so it cannot be silently swallowed by a max-based
    /// cursor. The contiguous cursor never advances past a failed TXID.
    pub fn mark_failed(&self, txid: u64) -> Result<()> {
        let mut manifest = self.manifest.lock().unwrap();
        manifest.pending_txids.remove(&txid);
        manifest.failed_txids.insert(txid);
        // A failure at or below the current contiguous cursor would mean the
        // cursor was advanced incorrectly; recompute to be safe.
        Self::recompute_contiguous(&mut manifest);
        self.save_manifest(&manifest)?;
        Ok(())
    }

    /// Advance `last_contiguous_uploaded_txid` across the longest gap-free run
    /// of confirmed-uploaded TXIDs starting just after the current cursor.
    /// Stops at the first TXID that is missing, still pending, or failed.
    fn recompute_contiguous(manifest: &mut CacheManifest) {
        let mut next = manifest.last_contiguous_uploaded_txid + 1;
        loop {
            if manifest.failed_txids.contains(&next) {
                break;
            }
            match manifest.entries.get(&next) {
                Some(entry) if entry.uploaded => {
                    manifest.last_contiguous_uploaded_txid = next;
                    next += 1;
                }
                _ => break,
            }
        }
    }

    /// Get list of pending uploads
    ///
    /// Returns TXIDs in sorted order for sequential processing
    pub fn pending_uploads(&self) -> Vec<u64> {
        let manifest = self.manifest.lock().unwrap();
        let mut pending: Vec<u64> = manifest.pending_txids.iter().copied().collect();
        pending.sort();
        pending
    }

    /// Get last uploaded TXID (max-based; may sit above a gap, NOT a safe
    /// restore cursor — use `last_contiguous_uploaded_txid` for that).
    pub fn last_uploaded_txid(&self) -> u64 {
        let manifest = self.manifest.lock().unwrap();
        manifest.last_uploaded_txid
    }

    /// Get the durable restore cursor: the highest TXID with no gap below it.
    pub fn last_contiguous_uploaded_txid(&self) -> u64 {
        let manifest = self.manifest.lock().unwrap();
        manifest.last_contiguous_uploaded_txid
    }

    /// Get the set of TXIDs whose upload permanently failed, sorted ascending.
    /// A non-empty result is a durable gap that callers must surface/alarm.
    pub fn failed_uploads(&self) -> Vec<u64> {
        let manifest = self.manifest.lock().unwrap();
        let mut failed: Vec<u64> = manifest.failed_txids.iter().copied().collect();
        failed.sort();
        failed
    }

    /// Cleanup old uploaded files based on retention policy
    ///
    /// - Keeps files uploaded within `retention_duration`
    /// - Enforces `max_cache_size` by deleting oldest uploaded files first
    pub fn cleanup(
        &self,
        retention_duration: chrono::Duration,
        max_cache_size: u64,
    ) -> Result<CleanupStats> {
        let mut manifest = self.manifest.lock().unwrap();
        let now = Utc::now();

        let mut deleted_count = 0;
        let mut deleted_bytes = 0;
        let mut to_delete_age = Vec::new();
        let mut to_delete_size = Vec::new();

        // Floor: always keep the latest snapshot and the incremental chain built
        // on top of it (every TXID >= that snapshot), regardless of age or size.
        // Evicting these would leave nothing locally restorable (F8). The floor
        // engages only when a snapshot base is present in the cache; with no
        // base there is no safe restore point to anchor on, and the
        // "never delete pending" guard below still protects un-uploaded data.
        let floor_txid = manifest
            .entries
            .values()
            .filter(|e| e.is_snapshot)
            .map(|e| e.txid)
            .max();

        // Collect files to delete based on retention duration
        for (txid, entry) in &manifest.entries {
            if !entry.uploaded {
                continue; // Never delete pending uploads
            }
            // Protect the restore base + its chain (TXIDs at/after the latest
            // snapshot).
            if let Some(floor) = floor_txid {
                if *txid >= floor {
                    continue;
                }
            }

            let age = now.signed_duration_since(entry.uploaded_at.unwrap_or(entry.created_at));
            if age >= retention_duration {
                to_delete_age.push(*txid);
            } else {
                // Files within retention can still be deleted for size constraints
                to_delete_size.push(*txid);
            }
        }

        // Sort both lists by age (oldest first)
        let sort_by_age = |txid: &u64| {
            manifest
                .entries
                .get(txid)
                .and_then(|e| e.uploaded_at)
                .unwrap_or_else(|| Utc::now())
        };
        to_delete_age.sort_by_key(sort_by_age);
        to_delete_size.sort_by_key(sort_by_age);

        // Delete all files outside retention window
        for txid in to_delete_age {
            if let Some(entry) = manifest.entries.remove(&txid) {
                let ltx_path = self.ltx_path(txid);
                if ltx_path.exists() {
                    fs::remove_file(&ltx_path)
                        .with_context(|| format!("Failed to delete LTX file for TXID {}", txid))?;
                }

                manifest.cache_size_bytes = manifest.cache_size_bytes.saturating_sub(entry.size);
                deleted_count += 1;
                deleted_bytes += entry.size;
            }
        }

        // If still over max_cache_size, delete more (oldest first)
        for txid in to_delete_size {
            if manifest.cache_size_bytes <= max_cache_size {
                break; // Under limit now
            }

            if let Some(entry) = manifest.entries.remove(&txid) {
                let ltx_path = self.ltx_path(txid);
                if ltx_path.exists() {
                    fs::remove_file(&ltx_path)
                        .with_context(|| format!("Failed to delete LTX file for TXID {}", txid))?;
                }

                manifest.cache_size_bytes = manifest.cache_size_bytes.saturating_sub(entry.size);
                deleted_count += 1;
                deleted_bytes += entry.size;
            }
        }

        manifest.last_cleanup = now;
        self.save_manifest(&manifest)?;

        Ok(CleanupStats {
            deleted_count,
            deleted_bytes,
            remaining_bytes: manifest.cache_size_bytes,
        })
    }

    /// Get cache statistics
    pub fn stats(&self) -> CacheStats {
        let manifest = self.manifest.lock().unwrap();
        CacheStats {
            total_entries: manifest.entries.len(),
            pending_count: manifest.pending_txids.len(),
            uploaded_count: manifest.entries.values().filter(|e| e.uploaded).count(),
            failed_count: manifest.failed_txids.len(),
            total_bytes: manifest.cache_size_bytes,
            last_uploaded_txid: manifest.last_uploaded_txid,
            last_contiguous_uploaded_txid: manifest.last_contiguous_uploaded_txid,
        }
    }

    /// Get path to LTX file for a TXID
    fn ltx_path(&self, txid: u64) -> PathBuf {
        self.cache_dir.join("ltx").join(format!("{:08}.ltx", txid))
    }

    /// Verify cache integrity
    ///
    /// Checks:
    /// - All manifest entries have corresponding files
    /// - All files have manifest entries
    /// - Pending TXIDs match entries
    pub fn verify(&self) -> Result<Vec<String>> {
        let manifest = self.manifest.lock().unwrap();
        let mut issues = Vec::new();

        // Check manifest entries have files
        for (txid, entry) in &manifest.entries {
            let path = self.ltx_path(*txid);
            if !path.exists() {
                issues.push(format!("TXID {} in manifest but file missing", txid));
            } else {
                let size = fs::metadata(&path)?.len();
                if size != entry.size {
                    issues.push(format!(
                        "TXID {} size mismatch: manifest={}, actual={}",
                        txid, entry.size, size
                    ));
                }
            }
        }

        // Check files have manifest entries
        if let Ok(entries) = fs::read_dir(self.cache_dir.join("ltx")) {
            for entry in entries.flatten() {
                let filename = entry.file_name();
                let filename_str = filename.to_string_lossy();

                if !filename_str.ends_with(".ltx") {
                    continue;
                }

                let txid_str = filename_str.trim_end_matches(".ltx");
                if let Ok(txid) = txid_str.parse::<u64>() {
                    if !manifest.entries.contains_key(&txid) {
                        issues.push(format!("File {:08}.ltx exists but not in manifest", txid));
                    }
                } else {
                    issues.push(format!("Invalid LTX filename: {}", filename_str));
                }
            }
        }

        // Check pending consistency
        for txid in &manifest.pending_txids {
            if let Some(entry) = manifest.entries.get(txid) {
                if entry.uploaded {
                    issues.push(format!(
                        "TXID {} marked pending but entry shows uploaded",
                        txid
                    ));
                }
            } else {
                issues.push(format!(
                    "TXID {} in pending set but no manifest entry",
                    txid
                ));
            }
        }

        Ok(issues)
    }
}

/// Cleanup operation statistics
#[derive(Debug, Clone)]
pub struct CleanupStats {
    pub deleted_count: usize,
    pub deleted_bytes: u64,
    pub remaining_bytes: u64,
}

/// Cache statistics
#[derive(Debug, Clone)]
pub struct CacheStats {
    pub total_entries: usize,
    pub pending_count: usize,
    pub uploaded_count: usize,
    /// Count of TXIDs whose upload permanently failed (a durable gap).
    pub failed_count: usize,
    pub total_bytes: u64,
    pub last_uploaded_txid: u64,
    /// Durable restore cursor (gap-free prefix).
    pub last_contiguous_uploaded_txid: u64,
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    fn setup_cache() -> (LocalCache, TempDir) {
        let temp_dir = TempDir::new().unwrap();
        let db_path = temp_dir.path().join("test.db");
        let cache = LocalCache::new(&db_path).unwrap();
        (cache, temp_dir)
    }

    #[test]
    fn test_cache_creation() {
        let temp_dir = TempDir::new().unwrap();
        let db_path = temp_dir.path().join("test.db");

        let cache = LocalCache::new(&db_path).unwrap();

        // Verify directory structure created
        let cache_dir = temp_dir.path().join(".test.db-walrust");
        assert!(cache_dir.exists());
        assert!(cache_dir.join("ltx").exists());
        assert!(cache_dir.join("manifest.json").exists());
    }

    #[test]
    fn test_write_and_read_ltx() {
        let (cache, _temp) = setup_cache();

        let txid = 1;
        let data = b"test ltx data";

        // Write LTX
        cache.write_ltx(txid, data).unwrap();

        // Verify file exists
        let ltx_path = cache.ltx_path(txid);
        assert!(ltx_path.exists());

        // Read back
        let read_data = cache.read_ltx(txid).unwrap();
        assert_eq!(read_data, data);

        // Verify manifest updated
        let stats = cache.stats();
        assert_eq!(stats.pending_count, 1);
        assert_eq!(stats.total_bytes, data.len() as u64);
    }

    #[test]
    fn test_atomic_write() {
        let (cache, _temp) = setup_cache();

        let txid = 1;
        let data = b"atomic test";

        cache.write_ltx(txid, data).unwrap();

        // Temp file should not exist
        let tmp_path = cache.ltx_path(txid).with_extension("tmp");
        assert!(!tmp_path.exists());

        // Final file should exist
        assert!(cache.ltx_path(txid).exists());
    }

    #[test]
    fn test_mark_uploaded() {
        let (cache, _temp) = setup_cache();

        let txid = 1;
        cache.write_ltx(txid, b"data").unwrap();

        // Initially pending
        assert_eq!(cache.pending_uploads(), vec![1]);
        assert_eq!(cache.last_uploaded_txid(), 0);

        // Mark uploaded
        cache.mark_uploaded(txid).unwrap();

        // No longer pending
        assert_eq!(cache.pending_uploads(), Vec::<u64>::new());
        assert_eq!(cache.last_uploaded_txid(), 1);

        // Stats updated
        let stats = cache.stats();
        assert_eq!(stats.pending_count, 0);
        assert_eq!(stats.uploaded_count, 1);
    }

    #[test]
    fn test_pending_uploads_sorted() {
        let (cache, _temp) = setup_cache();

        // Write out of order
        cache.write_ltx(3, b"data3").unwrap();
        cache.write_ltx(1, b"data1").unwrap();
        cache.write_ltx(2, b"data2").unwrap();

        // Should return sorted
        assert_eq!(cache.pending_uploads(), vec![1, 2, 3]);
    }

    #[test]
    fn test_sequential_upload_tracking() {
        let (cache, _temp) = setup_cache();

        // Write multiple TXIDs
        for txid in 1..=5 {
            cache.write_ltx(txid, b"data").unwrap();
        }

        // Mark uploaded in order
        cache.mark_uploaded(1).unwrap();
        assert_eq!(cache.last_uploaded_txid(), 1);
        assert_eq!(cache.pending_uploads(), vec![2, 3, 4, 5]);

        cache.mark_uploaded(2).unwrap();
        assert_eq!(cache.last_uploaded_txid(), 2);
        assert_eq!(cache.pending_uploads(), vec![3, 4, 5]);

        // Mark out of order (upload 5 before 3)
        cache.mark_uploaded(5).unwrap();
        assert_eq!(cache.last_uploaded_txid(), 5); // Max TXID
        assert_eq!(cache.pending_uploads(), vec![3, 4]);
    }

    #[test]
    fn test_contiguous_cursor_advances_only_over_gap_free_prefix() {
        // F10: out-of-order uploads must not advance the durable cursor past a
        // hole. Upload 1, 3, 4 (2 missing). last_uploaded_txid jumps to 4 but
        // the contiguous cursor stays at 1 until 2 lands.
        let (cache, _temp) = setup_cache();
        for txid in 1..=4 {
            cache.write_ltx(txid, b"data").unwrap();
        }

        cache.mark_uploaded(1).unwrap();
        assert_eq!(cache.last_contiguous_uploaded_txid(), 1);

        cache.mark_uploaded(3).unwrap();
        cache.mark_uploaded(4).unwrap();
        assert_eq!(cache.last_uploaded_txid(), 4, "max-based cursor jumps");
        assert_eq!(
            cache.last_contiguous_uploaded_txid(),
            1,
            "durable cursor must not pass the missing TXID 2"
        );

        // Filling the hole advances the contiguous cursor across 2,3,4 at once.
        cache.mark_uploaded(2).unwrap();
        assert_eq!(cache.last_contiguous_uploaded_txid(), 4);
    }

    #[test]
    fn test_failed_upload_surfaces_gap_and_blocks_cursor() {
        // F9: a permanently failed upload is a durable gap. The contiguous
        // cursor must not advance past it even if later TXIDs upload fine.
        let (cache, _temp) = setup_cache();
        for txid in 1..=4 {
            cache.write_ltx(txid, b"data").unwrap();
        }

        cache.mark_uploaded(1).unwrap();
        cache.mark_failed(2).unwrap();
        cache.mark_uploaded(3).unwrap();
        cache.mark_uploaded(4).unwrap();

        assert_eq!(cache.failed_uploads(), vec![2], "gap is surfaced");
        assert_eq!(cache.stats().failed_count, 1);
        assert_eq!(
            cache.last_contiguous_uploaded_txid(),
            1,
            "durable cursor blocked behind the failed TXID 2"
        );
        // A later retry that succeeds clears the gap and advances the cursor.
        cache.mark_uploaded(2).unwrap();
        assert_eq!(cache.failed_uploads(), Vec::<u64>::new());
        assert_eq!(cache.last_contiguous_uploaded_txid(), 4);
    }

    #[test]
    fn test_contiguous_cursor_survives_restart() {
        let temp_dir = TempDir::new().unwrap();
        let db_path = temp_dir.path().join("test.db");
        {
            let cache = LocalCache::new(&db_path).unwrap();
            for txid in 1..=3 {
                cache.write_ltx(txid, b"data").unwrap();
            }
            cache.mark_uploaded(1).unwrap();
            cache.mark_uploaded(2).unwrap();
        }
        {
            let cache = LocalCache::new(&db_path).unwrap();
            assert_eq!(cache.last_contiguous_uploaded_txid(), 2);
        }
    }

    #[test]
    fn test_cleanup_retention() {
        let (cache, _temp) = setup_cache();

        // Write and upload old files (with delays to ensure different timestamps)
        cache.write_ltx(1, b"data1").unwrap();
        cache.mark_uploaded(1).unwrap();
        std::thread::sleep(std::time::Duration::from_millis(10));

        cache.write_ltx(2, b"data2").unwrap();
        cache.mark_uploaded(2).unwrap();
        std::thread::sleep(std::time::Duration::from_millis(10));

        // Write pending (should not be deleted)
        cache.write_ltx(3, b"data3").unwrap();

        // Cleanup with 0 retention (delete all uploaded)
        let stats = cache.cleanup(chrono::Duration::zero(), u64::MAX).unwrap();

        assert_eq!(stats.deleted_count, 2);
        assert!(cache.ltx_path(1).exists() == false);
        assert!(cache.ltx_path(2).exists() == false);
        assert!(cache.ltx_path(3).exists()); // Pending not deleted
    }

    #[test]
    fn test_cleanup_max_size() {
        let (cache, _temp) = setup_cache();

        // Write 5 files, 100 bytes each (with delays to ensure different timestamps)
        for txid in 1..=5 {
            cache.write_ltx(txid, &vec![0u8; 100]).unwrap();
            cache.mark_uploaded(txid).unwrap();
            std::thread::sleep(std::time::Duration::from_millis(10));
        }

        // Cleanup to max 250 bytes (should keep 2-3 newest files)
        let stats = cache.cleanup(chrono::Duration::hours(24), 250).unwrap();

        assert!(stats.deleted_count >= 2);
        assert!(stats.remaining_bytes <= 250);
    }

    #[test]
    fn test_cleanup_floor_keeps_snapshot_and_chain() {
        // F8: aggressive cleanup must never evict the latest snapshot or the
        // incrementals built on it, even with zero retention and zero max size.
        let (cache, _temp) = setup_cache();

        // Snapshot at TXID 1, incrementals at 2 and 3, all uploaded.
        cache.write_snapshot_ltx(1, &vec![0u8; 1000]).unwrap();
        cache.mark_uploaded(1).unwrap();
        cache.write_ltx(2, &vec![0u8; 1000]).unwrap();
        cache.mark_uploaded(2).unwrap();
        cache.write_ltx(3, &vec![0u8; 1000]).unwrap();
        cache.mark_uploaded(3).unwrap();

        // Most aggressive cleanup possible.
        cache.cleanup(chrono::Duration::zero(), 0).unwrap();

        // The snapshot and its whole chain survive.
        assert!(cache.ltx_path(1).exists(), "snapshot base must be kept");
        assert!(cache.ltx_path(2).exists(), "chain TXID 2 must be kept");
        assert!(cache.ltx_path(3).exists(), "chain TXID 3 must be kept");
    }

    #[test]
    fn test_cleanup_floor_evicts_older_snapshot_chain() {
        // A newer snapshot supersedes an older one; the older base + the
        // incrementals between them are below the floor and may be evicted.
        let (cache, _temp) = setup_cache();

        cache.write_snapshot_ltx(1, &vec![0u8; 1000]).unwrap();
        cache.mark_uploaded(1).unwrap();
        cache.write_ltx(2, &vec![0u8; 1000]).unwrap();
        cache.mark_uploaded(2).unwrap();
        // Newer snapshot at TXID 3 — this is the floor now.
        cache.write_snapshot_ltx(3, &vec![0u8; 1000]).unwrap();
        cache.mark_uploaded(3).unwrap();

        let stats = cache.cleanup(chrono::Duration::zero(), 0).unwrap();

        // TXID 1 and 2 (below the latest snapshot) are evictable.
        assert!(stats.deleted_count >= 1);
        assert!(cache.ltx_path(3).exists(), "latest snapshot must be kept");
        assert!(!cache.ltx_path(1).exists(), "superseded snapshot evicted");
    }

    #[test]
    fn test_cleanup_never_deletes_pending() {
        let (cache, _temp) = setup_cache();

        cache.write_ltx(1, &vec![0u8; 1000]).unwrap();
        cache.write_ltx(2, &vec![0u8; 1000]).unwrap();
        cache.mark_uploaded(1).unwrap();

        // Aggressive cleanup (0 retention, 0 max size)
        let stats = cache.cleanup(chrono::Duration::zero(), 0).unwrap();

        // Should only delete uploaded file
        assert_eq!(stats.deleted_count, 1);
        assert!(!cache.ltx_path(1).exists());
        assert!(cache.ltx_path(2).exists()); // Pending preserved
    }

    #[test]
    fn test_cache_persistence() {
        let temp_dir = TempDir::new().unwrap();
        let db_path = temp_dir.path().join("test.db");

        // Create cache and write data
        {
            let cache = LocalCache::new(&db_path).unwrap();
            cache.write_ltx(1, b"data1").unwrap();
            cache.write_ltx(2, b"data2").unwrap();
            cache.mark_uploaded(1).unwrap();
        }

        // Recreate cache (simulates restart)
        {
            let cache = LocalCache::new(&db_path).unwrap();

            // Should restore state from manifest
            assert_eq!(cache.last_uploaded_txid(), 1);
            assert_eq!(cache.pending_uploads(), vec![2]);
            assert_eq!(cache.stats().total_entries, 2);

            // Files should still be readable
            assert_eq!(cache.read_ltx(1).unwrap(), b"data1");
            assert_eq!(cache.read_ltx(2).unwrap(), b"data2");
        }
    }

    #[test]
    fn test_verify_integrity_clean() {
        let (cache, _temp) = setup_cache();

        cache.write_ltx(1, b"data1").unwrap();
        cache.write_ltx(2, b"data2").unwrap();

        let issues = cache.verify().unwrap();
        assert_eq!(issues.len(), 0);
    }

    #[test]
    fn test_verify_detects_missing_file() {
        let (cache, _temp) = setup_cache();

        cache.write_ltx(1, b"data1").unwrap();

        // Delete file manually
        fs::remove_file(cache.ltx_path(1)).unwrap();

        let issues = cache.verify().unwrap();
        assert!(issues.iter().any(|i| i.contains("file missing")));
    }

    #[test]
    fn test_verify_detects_orphan_file() {
        let (cache, _temp) = setup_cache();

        // Write file directly without updating manifest
        let orphan_path = cache.cache_dir.join("ltx").join("00000099.ltx");
        fs::write(&orphan_path, b"orphan").unwrap();

        let issues = cache.verify().unwrap();
        assert!(issues.iter().any(|i| i.contains("not in manifest")));
    }

    #[test]
    fn test_verify_detects_size_mismatch() {
        let (cache, _temp) = setup_cache();

        cache.write_ltx(1, b"original").unwrap();

        // Overwrite file with different size
        fs::write(cache.ltx_path(1), b"modified longer data").unwrap();

        let issues = cache.verify().unwrap();
        assert!(issues.iter().any(|i| i.contains("size mismatch")));
    }

    #[test]
    fn test_concurrent_access() {
        use std::sync::Arc;
        use std::thread;

        let (cache, _temp) = setup_cache();
        let cache = Arc::new(cache);

        let mut handles = vec![];

        // Spawn 10 threads writing concurrently
        for i in 0..10 {
            let cache = Arc::clone(&cache);
            let handle = thread::spawn(move || {
                cache.write_ltx(i, &vec![i as u8; 100]).unwrap();
            });
            handles.push(handle);
        }

        for handle in handles {
            handle.join().unwrap();
        }

        // All writes should succeed
        let stats = cache.stats();
        assert_eq!(stats.total_entries, 10);
        assert_eq!(stats.pending_count, 10);
    }

    #[test]
    fn test_manifest_corruption_recovery() {
        let temp_dir = TempDir::new().unwrap();
        let db_path = temp_dir.path().join("test.db");

        // Create cache and write data
        {
            let cache = LocalCache::new(&db_path).unwrap();
            cache.write_ltx(1, b"data1").unwrap();
        }

        // Corrupt manifest
        let manifest_path = LocalCache::cache_dir_for_db(&db_path).join("manifest.json");
        fs::write(&manifest_path, b"invalid json {{{").unwrap();

        // Recreate cache should fail to parse
        let result = LocalCache::new(&db_path);
        assert!(result.is_err());
    }

    #[test]
    fn test_empty_cache_stats() {
        let (cache, _temp) = setup_cache();

        let stats = cache.stats();
        assert_eq!(stats.total_entries, 0);
        assert_eq!(stats.pending_count, 0);
        assert_eq!(stats.uploaded_count, 0);
        assert_eq!(stats.total_bytes, 0);
        assert_eq!(stats.last_uploaded_txid, 0);
    }

    #[test]
    fn test_large_txid_values() {
        let (cache, _temp) = setup_cache();

        let large_txid = u64::MAX - 1;
        cache.write_ltx(large_txid, b"data").unwrap();

        assert_eq!(cache.pending_uploads(), vec![large_txid]);
        assert_eq!(cache.read_ltx(large_txid).unwrap(), b"data");
    }

    // Phase 5: Fast Local Restore tests

    #[test]
    fn test_open_existing_cache() {
        let temp_dir = TempDir::new().unwrap();
        let db_path = temp_dir.path().join("test.db");

        // Create cache and write some data
        {
            let cache = LocalCache::new(&db_path).unwrap();
            cache.write_ltx(1, b"data1").unwrap();
            cache.write_ltx(2, b"data2").unwrap();
        }

        // Open existing cache
        let cache_dir = LocalCache::cache_dir_for_db(&db_path);
        let cache = LocalCache::open(&cache_dir).unwrap();

        assert!(cache.is_some());
        let cache = cache.unwrap();
        assert_eq!(cache.available_txids(), vec![1, 2]);
    }

    #[test]
    fn test_open_nonexistent_cache() {
        let temp_dir = TempDir::new().unwrap();
        let cache_dir = temp_dir.path().join("nonexistent-walrust");

        let cache = LocalCache::open(&cache_dir).unwrap();
        assert!(cache.is_none());
    }

    #[test]
    fn test_available_txids() {
        let (cache, _temp) = setup_cache();

        // Empty cache
        assert_eq!(cache.available_txids(), Vec::<u64>::new());

        // Add some TXIDs out of order
        cache.write_ltx(5, b"data5").unwrap();
        cache.write_ltx(2, b"data2").unwrap();
        cache.write_ltx(10, b"data10").unwrap();

        // Should return sorted
        assert_eq!(cache.available_txids(), vec![2, 5, 10]);
    }

    #[test]
    fn test_has_txid() {
        let (cache, _temp) = setup_cache();

        cache.write_ltx(1, b"data1").unwrap();
        cache.write_ltx(3, b"data3").unwrap();

        assert!(cache.has_txid(1));
        assert!(!cache.has_txid(2));
        assert!(cache.has_txid(3));
        assert!(!cache.has_txid(4));
    }

    #[test]
    fn test_txid_range() {
        let (cache, _temp) = setup_cache();

        // Empty cache
        assert_eq!(cache.txid_range(), None);

        // Single TXID
        cache.write_ltx(5, b"data5").unwrap();
        assert_eq!(cache.txid_range(), Some((5, 5)));

        // Multiple TXIDs
        cache.write_ltx(2, b"data2").unwrap();
        cache.write_ltx(10, b"data10").unwrap();
        assert_eq!(cache.txid_range(), Some((2, 10)));
    }

    #[test]
    fn test_has_continuous_range() {
        let (cache, _temp) = setup_cache();

        // Write continuous range 1-5
        for txid in 1..=5 {
            cache.write_ltx(txid, b"data").unwrap();
        }

        // Should have continuous range
        assert!(cache.has_continuous_range(1, 5));
        assert!(cache.has_continuous_range(2, 4));
        assert!(cache.has_continuous_range(1, 1));

        // Should not have range including 6
        assert!(!cache.has_continuous_range(1, 6));
        assert!(!cache.has_continuous_range(5, 7));
    }

    #[test]
    fn test_has_continuous_range_with_gaps() {
        let (cache, _temp) = setup_cache();

        // Write with gap: 1, 2, 4, 5 (missing 3)
        cache.write_ltx(1, b"data1").unwrap();
        cache.write_ltx(2, b"data2").unwrap();
        cache.write_ltx(4, b"data4").unwrap();
        cache.write_ltx(5, b"data5").unwrap();

        // Ranges without gap
        assert!(cache.has_continuous_range(1, 2));
        assert!(cache.has_continuous_range(4, 5));

        // Ranges with gap
        assert!(!cache.has_continuous_range(1, 4));
        assert!(!cache.has_continuous_range(2, 4));
        assert!(!cache.has_continuous_range(1, 5));
    }

    #[test]
    fn test_cache_dir_accessor() {
        let temp_dir = TempDir::new().unwrap();
        let db_path = temp_dir.path().join("test.db");
        let cache = LocalCache::new(&db_path).unwrap();

        let expected = temp_dir.path().join(".test.db-walrust");
        assert_eq!(cache.cache_dir(), expected);
    }
}
