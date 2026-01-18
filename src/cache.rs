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
    /// Last successfully uploaded TXID
    pub last_uploaded_txid: u64,
    /// Set of pending TXIDs (written to cache but not uploaded)
    pub pending_txids: HashSet<u64>,
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
}

impl Default for CacheManifest {
    fn default() -> Self {
        Self {
            last_uploaded_txid: 0,
            pending_txids: HashSet::new(),
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
        fs::create_dir_all(cache_dir.join("ltx"))
            .context("Failed to create cache directory")?;

        // Load or create manifest
        let manifest = if manifest_path.exists() {
            Self::load_manifest(&manifest_path)?
        } else {
            let default_manifest = CacheManifest::default();
            // Save default manifest to disk
            let json = serde_json::to_string_pretty(&default_manifest)
                .context("Failed to serialize default manifest")?;
            fs::write(&manifest_path, json)
                .context("Failed to write default manifest")?;
            default_manifest
        };

        Ok(Self {
            cache_dir,
            manifest_path,
            manifest: Arc::new(Mutex::new(manifest)),
        })
    }

    /// Get cache directory path for a database
    fn cache_dir_for_db(db_path: &Path) -> PathBuf {
        let parent = db_path.parent().unwrap_or(Path::new("."));
        let db_name = db_path.file_name().unwrap().to_string_lossy();
        parent.join(format!(".{}-walrust", db_name))
    }

    /// Load manifest from disk
    fn load_manifest(path: &Path) -> Result<CacheManifest> {
        let contents = fs::read_to_string(path)
            .context("Failed to read manifest")?;
        let manifest: CacheManifest = serde_json::from_str(&contents)
            .context("Failed to parse manifest JSON")?;
        Ok(manifest)
    }

    /// Save manifest to disk atomically
    fn save_manifest(&self, manifest: &CacheManifest) -> Result<()> {
        let tmp_path = self.manifest_path.with_extension("tmp");

        // Write to temp file
        let json = serde_json::to_string_pretty(manifest)
            .context("Failed to serialize manifest")?;
        fs::write(&tmp_path, json)
            .context("Failed to write temporary manifest")?;

        // Atomic rename
        fs::rename(&tmp_path, &self.manifest_path)
            .context("Failed to rename manifest")?;

        Ok(())
    }

    /// Write LTX to cache atomically
    ///
    /// Uses tempfile + rename for atomicity. Updates manifest with pending status.
    pub fn write_ltx(&self, txid: u64, data: &[u8]) -> Result<()> {
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
        manifest.entries.insert(txid, CacheEntry {
            txid,
            size: data.len() as u64,
            created_at: Utc::now(),
            uploaded: false,
            uploaded_at: None,
        });

        self.save_manifest(&manifest)?;

        Ok(())
    }

    /// Read LTX from cache
    pub fn read_ltx(&self, txid: u64) -> Result<Vec<u8>> {
        let ltx_path = self.ltx_path(txid);
        fs::read(&ltx_path)
            .with_context(|| format!("Failed to read LTX file for TXID {}", txid))
    }

    /// Mark TXID as uploaded
    ///
    /// Removes from pending set, updates upload timestamp
    pub fn mark_uploaded(&self, txid: u64) -> Result<()> {
        let mut manifest = self.manifest.lock().unwrap();

        manifest.pending_txids.remove(&txid);
        manifest.last_uploaded_txid = manifest.last_uploaded_txid.max(txid);

        if let Some(entry) = manifest.entries.get_mut(&txid) {
            entry.uploaded = true;
            entry.uploaded_at = Some(Utc::now());
        }

        self.save_manifest(&manifest)?;

        Ok(())
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

    /// Get last uploaded TXID
    pub fn last_uploaded_txid(&self) -> u64 {
        let manifest = self.manifest.lock().unwrap();
        manifest.last_uploaded_txid
    }

    /// Cleanup old uploaded files based on retention policy
    ///
    /// - Keeps files uploaded within `retention_duration`
    /// - Enforces `max_cache_size` by deleting oldest uploaded files first
    pub fn cleanup(&self, retention_duration: chrono::Duration, max_cache_size: u64) -> Result<CleanupStats> {
        let mut manifest = self.manifest.lock().unwrap();
        let now = Utc::now();

        let mut deleted_count = 0;
        let mut deleted_bytes = 0;
        let mut to_delete_age = Vec::new();
        let mut to_delete_size = Vec::new();

        // Collect files to delete based on retention duration
        for (txid, entry) in &manifest.entries {
            if !entry.uploaded {
                continue; // Never delete pending uploads
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
            manifest.entries.get(txid)
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
            total_bytes: manifest.cache_size_bytes,
            last_uploaded_txid: manifest.last_uploaded_txid,
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
                    issues.push(format!("TXID {} marked pending but entry shows uploaded", txid));
                }
            } else {
                issues.push(format!("TXID {} in pending set but no manifest entry", txid));
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
    pub total_bytes: u64,
    pub last_uploaded_txid: u64,
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
}
