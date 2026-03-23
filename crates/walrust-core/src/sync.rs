//! Core sync operations for walrust: WAL sync, snapshots, restore, and manifest management.
//!
//! These are the production-grade primitives for embedding walrust as a library.
//! Each function is a single operation (sync one batch of WAL frames, take one snapshot,
//! restore from S3) — the caller controls scheduling and lifecycle.

use anyhow::{anyhow, Result};
use chrono::Utc;
use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};

use crate::ltx;
use crate::wal;
use hadb_io::ObjectStore as StorageBackend;
use hadb_io::RetryPolicy;

// ============================================================================
// Types
// ============================================================================

/// Entry in the LTX manifest tracking a single LTX file
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LtxEntry {
    /// Filename (e.g., "00000001-00000010.ltx")
    pub filename: String,
    /// Starting transaction ID
    pub min_txid: u64,
    /// Ending transaction ID
    pub max_txid: u64,
    /// File size in bytes
    pub size: u64,
    /// Upload timestamp (ISO 8601)
    pub created_at: String,
    /// Whether this is a snapshot (full DB) or incremental
    pub is_snapshot: bool,
}

/// Manifest tracking all LTX files for a database
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct Manifest {
    /// Database name
    pub name: String,
    /// Current highest TXID
    pub current_txid: u64,
    /// Page size of the database
    pub page_size: u32,
    /// List of LTX files
    pub files: Vec<LtxEntry>,
    /// Last known database checksum (for incremental LTX chaining)
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_checksum: Option<u64>,
}

/// State for a single database being synced
#[derive(Debug, Clone)]
pub struct SyncState {
    /// Database name
    pub name: String,
    /// Path to main db file
    pub db_path: PathBuf,
    /// Path to WAL file
    pub wal_path: PathBuf,
    /// Current WAL sync position
    pub wal_offset: u64,
    /// WAL generation (increments on checkpoint)
    pub wal_generation: u64,
    /// Current transaction ID
    pub current_txid: u64,
    /// Last snapshot time
    pub last_snapshot: Option<chrono::DateTime<Utc>>,
    /// Current database checksum (chained page hash for incrementals, full-DB hash after snapshots)
    pub db_checksum: Option<u64>,
}

impl SyncState {
    /// Create new sync state for a database
    pub fn new(db_path: PathBuf) -> Result<Self> {
        let name = db_path
            .file_stem()
            .and_then(|s| s.to_str())
            .ok_or_else(|| anyhow!("Invalid database path"))?
            .to_string();
        let wal_path = db_path.with_extension("db-wal");
        Ok(Self {
            name,
            db_path,
            wal_path,
            wal_offset: 0,
            wal_generation: 0,
            current_txid: 0,
            last_snapshot: None,
            db_checksum: None,
        })
    }

    /// Initialize checksum from database file
    pub fn init_checksum(&mut self) -> Result<()> {
        match ltx::compute_checksum_from_file(&self.db_path) {
            Ok(cs) => {
                self.db_checksum = Some(cs.into_inner());
                Ok(())
            }
            Err(e) => Err(anyhow!("Failed to compute checksum: {}", e)),
        }
    }
}

// ============================================================================
// Litestream-compatible format helpers
// ============================================================================

/// Format a TXID as 16-char lowercase hex (litestream format)
fn format_txid_hex(txid: u64) -> String {
    format!("{:016x}", txid)
}

/// Format an LTX filename in litestream format
fn format_ltx_filename(min_txid: u64, max_txid: u64) -> String {
    format!(
        "{}-{}.ltx",
        format_txid_hex(min_txid),
        format_txid_hex(max_txid)
    )
}

/// Format generation folder name (4-char hex)
fn format_generation(gen: u64) -> String {
    format!("{:04x}", gen)
}

/// Build S3 key for an LTX file in litestream format
fn build_ltx_key(
    prefix: &str,
    db_name: &str,
    generation: u64,
    min_txid: u64,
    max_txid: u64,
) -> String {
    format!(
        "{}{}/{}/{}",
        prefix,
        db_name,
        format_generation(generation),
        format_ltx_filename(min_txid, max_txid)
    )
}

/// Live incrementals go to generation 0 (0000/)
const GENERATION_LIVE: u64 = 0;

/// An LTX file discovered from S3 listing (no manifest needed).
#[derive(Debug, Clone)]
struct DiscoveredLtx {
    key: String,
    min_txid: u64,
    max_txid: u64,
    is_snapshot: bool,
}

/// Discover LTX files by listing S3 objects and parsing filenames.
/// This replaces manifest-based discovery — filenames encode everything.
///
/// Convention:
///   {prefix}{db_name}/0001/*.ltx → snapshots (generation 1)
///   {prefix}{db_name}/0000/*.ltx → incrementals (generation 0, "live")
async fn discover_ltx_files(
    storage: &dyn StorageBackend,
    prefix: &str,
    db_name: &str,
) -> Result<Vec<DiscoveredLtx>> {
    let db_prefix = format!("{}{}/", prefix, db_name);
    let keys = storage.list_objects(&db_prefix).await?;
    let mut files = Vec::new();
    for key in &keys {
        // key looks like: ha-test/ha-db/0001/00000001-00000001.ltx
        // We need the part after {db_prefix}: 0001/00000001-00000001.ltx
        let relative = match key.strip_prefix(&db_prefix) {
            Some(r) => r,
            None => continue,
        };
        let parts: Vec<&str> = relative.splitn(2, '/').collect();
        if parts.len() != 2 {
            continue;
        }
        let gen_str = parts[0];
        let filename = parts[1];
        if !filename.ends_with(".ltx") {
            continue;
        }
        let name = &filename[..filename.len() - 4]; // strip .ltx
        let txid_parts: Vec<&str> = name.splitn(2, '-').collect();
        if txid_parts.len() != 2 {
            continue;
        }
        let min_txid = match u64::from_str_radix(txid_parts[0], 16) {
            Ok(v) => v,
            Err(_) => continue,
        };
        let max_txid = match u64::from_str_radix(txid_parts[1], 16) {
            Ok(v) => v,
            Err(_) => continue,
        };
        let is_snapshot = gen_str != "0000"; // generation 0 = incremental, anything else = snapshot
        files.push(DiscoveredLtx {
            key: key.clone(),
            min_txid,
            max_txid,
            is_snapshot,
        });
    }
    files.sort_by_key(|f| f.min_txid);
    Ok(files)
}

/// Discover only incrementals newer than `after_txid` using S3 `start_after`.
///
/// Instead of listing everything under `{prefix}{db_name}/`, this lists only
/// `{prefix}{db_name}/0000/` (incrementals) starting after the key for `after_txid`.
/// S3 skips to the right position in the index — no scanning of old keys.
///
/// For a DB with 1M incrementals where we're at TXID 999_990, this lists ~10 keys
/// instead of 1M+.
async fn discover_incrementals_after(
    storage: &dyn StorageBackend,
    prefix: &str,
    db_name: &str,
    after_txid: u64,
) -> Result<Vec<DiscoveredLtx>> {
    let incr_prefix = format!("{}{}/0000/", prefix, db_name);
    // start_after is exclusive — build a key that sorts just before min_txid = after_txid + 1.
    // We use after_txid as min AND max (the exact key may not exist, but S3 skips past it).
    let start_after_key = format!(
        "{}{}-{}.ltx",
        incr_prefix,
        format_txid_hex(after_txid),
        format_txid_hex(after_txid),
    );

    let keys = storage
        .list_objects_after(&incr_prefix, &start_after_key)
        .await?;

    let mut files = Vec::new();
    for key in &keys {
        let filename = match key.strip_prefix(&incr_prefix) {
            Some(f) => f,
            None => continue,
        };
        if !filename.ends_with(".ltx") {
            continue;
        }
        let name = &filename[..filename.len() - 4];
        let txid_parts: Vec<&str> = name.splitn(2, '-').collect();
        if txid_parts.len() != 2 {
            continue;
        }
        let min_txid = match u64::from_str_radix(txid_parts[0], 16) {
            Ok(v) => v,
            Err(_) => continue,
        };
        let max_txid = match u64::from_str_radix(txid_parts[1], 16) {
            Ok(v) => v,
            Err(_) => continue,
        };
        files.push(DiscoveredLtx {
            key: key.clone(),
            min_txid,
            max_txid,
            is_snapshot: false,
        });
    }
    // Already sorted lexicographically by S3, but sort by min_txid to be safe
    files.sort_by_key(|f| f.min_txid);
    Ok(files)
}

/// Get SQLite database page size from header
async fn get_page_size(db_path: &Path) -> Result<u32> {
    use tokio::io::AsyncReadExt;
    let mut file = tokio::fs::File::open(db_path).await?;
    let mut header = [0u8; 100];
    file.read_exact(&mut header).await?;

    // Page size is at offset 16-17, big-endian
    let page_size = u16::from_be_bytes([header[16], header[17]]) as u32;

    // Page size of 1 means 65536
    let page_size = if page_size == 1 { 65536 } else { page_size };

    Ok(page_size)
}

// ============================================================================
// Manifest operations
// ============================================================================

/// Load manifest from storage
pub async fn load_manifest(
    storage: &dyn StorageBackend,
    prefix: &str,
    db_name: &str,
) -> Result<Manifest> {
    let manifest_key = format!("{}{}/manifest.json", prefix, db_name);
    match storage.download_bytes(&manifest_key).await {
        Ok(data) => Ok(serde_json::from_slice(&data)?),
        Err(_) => Ok(Manifest {
            name: db_name.to_string(),
            ..Default::default()
        }),
    }
}

/// Save manifest to storage
pub async fn save_manifest(
    storage: &dyn StorageBackend,
    prefix: &str,
    manifest: &Manifest,
) -> Result<()> {
    let manifest_key = format!("{}{}/manifest.json", prefix, manifest.name);
    storage
        .upload_bytes(&manifest_key, serde_json::to_vec_pretty(manifest)?)
        .await
}

/// Save legacy state.json for backwards compatibility
pub async fn save_state(
    storage: &dyn StorageBackend,
    prefix: &str,
    state: &SyncState,
) -> Result<()> {
    let state_key = format!("{}{}/state.json", prefix, state.name);
    let state_json = serde_json::json!({
        "wal_offset": state.wal_offset,
        "wal_generation": state.wal_generation,
        "current_txid": state.current_txid,
        "last_snapshot": state.last_snapshot,
    });
    storage
        .upload_bytes(&state_key, serde_json::to_vec(&state_json)?)
        .await
}

// ============================================================================
// Core sync operations
// ============================================================================

/// Sync WAL changes to storage as incremental LTX files.
///
/// Reads new WAL frames since last sync, deduplicates pages, encodes as LTX
/// with checksum chaining, uploads to storage, and updates manifest.
///
/// Returns the number of frames synced.
pub async fn sync_wal(
    storage: &dyn StorageBackend,
    prefix: &str,
    state: &mut SyncState,
) -> Result<u64> {
    use litepages::Checksum;

    let header = match wal::read_header(&state.wal_path).await? {
        Some(h) => h,
        None => return Ok(0),
    };

    // Check if WAL was reset (checkpoint happened)
    let current_size = wal::get_wal_size(&state.wal_path).await?;
    if current_size < state.wal_offset {
        tracing::info!("{}: WAL checkpoint detected, resetting offset", state.name);
        state.wal_offset = 0;
        state.wal_generation += 1;
        // Chain continues through checkpoints — no need to recompute from file
    }

    let (page_map, frame_count, new_offset, max_db_size) =
        wal::read_frames_as_page_map(&state.wal_path, header.page_size, state.wal_offset).await?;

    if page_map.is_empty() {
        return Ok(0);
    }

    let pages: Vec<(u32, Vec<u8>)> = page_map.into_iter().collect();

    let pre_checksum = match state.db_checksum {
        Some(cs) => Checksum::new(cs),
        None => ltx::compute_checksum_from_file(&state.db_path)?,
    };

    let min_txid = state.current_txid + 1;
    let max_txid = min_txid + pages.len() as u64 - 1;
    let commit_page = if max_db_size > 0 {
        max_db_size
    } else {
        let db_size = std::fs::metadata(&state.db_path)?.len();
        (db_size / header.page_size as u64) as u32
    };

    let estimated_size = pages
        .len()
        .saturating_mul(header.page_size as usize)
        .saturating_mul(2);

    // Chained page checksum: O(changed pages), no disk read
    let expected_post = ltx::chain_checksum(pre_checksum, &pages);

    let mut ltx_buffer = Vec::with_capacity(estimated_size);
    let post_checksum = ltx::encode_wal_changes(
        &mut ltx_buffer,
        &pages,
        header.page_size,
        min_txid,
        max_txid,
        commit_page,
        Some(pre_checksum),
        expected_post,
    )?;

    let ltx_size = ltx_buffer.len() as u64;
    let ltx_key = build_ltx_key(prefix, &state.name, GENERATION_LIVE, min_txid, max_txid);

    storage.upload_bytes(&ltx_key, ltx_buffer).await?;

    tracing::info!(
        "{}: Synced {} WAL frames as incremental LTX ({} bytes, TXID {}-{}) -> {}",
        state.name,
        frame_count,
        ltx_size,
        min_txid,
        max_txid,
        ltx_key
    );

    state.wal_offset = new_offset;
    state.current_txid = max_txid;
    state.db_checksum = Some(post_checksum.into_inner());

    save_state(storage, prefix, state).await?;

    Ok(frame_count as u64)
}

/// Take a full database snapshot as LTX
pub async fn take_snapshot(
    storage: &dyn StorageBackend,
    prefix: &str,
    state: &mut SyncState,
) -> Result<()> {
    let timestamp = Utc::now();
    let page_size = get_page_size(&state.db_path).await?;
    let new_txid = state.current_txid + 1;
    let ltx_key = build_ltx_key(prefix, &state.name, 1, 1, new_txid);

    let db_size = std::fs::metadata(&state.db_path)?.len() as usize;
    let estimated_size = db_size.saturating_mul(2);
    let mut ltx_buffer = Vec::with_capacity(estimated_size);
    ltx::encode_snapshot(&mut ltx_buffer, &state.db_path, page_size, new_txid)?;

    let ltx_size = ltx_buffer.len() as u64;
    storage.upload_bytes(&ltx_key, ltx_buffer).await?;

    let db_checksum = ltx::compute_checksum_from_file(&state.db_path)?;

    tracing::info!(
        "{}: LTX snapshot uploaded ({} bytes, TXID 1-{}) -> {}",
        state.name,
        ltx_size,
        new_txid,
        ltx_key
    );

    state.current_txid = new_txid;
    state.last_snapshot = Some(timestamp);
    state.db_checksum = Some(db_checksum.into_inner());

    Ok(())
}

/// Restore a database from storage using LTX files.
///
/// Discovers available LTX files by listing S3 objects (no manifest needed).
/// Returns the TXID that was actually restored to.
pub async fn restore(
    storage: &dyn StorageBackend,
    prefix: &str,
    db_name: &str,
    output: &Path,
    _point_in_time: Option<&str>,
) -> Result<u64> {
    use std::io::Cursor;

    let files = discover_ltx_files(storage, prefix, db_name).await?;
    if files.is_empty() {
        return Err(anyhow!("No LTX files found for database '{}'", db_name));
    }

    // Find the latest snapshot
    let snapshot = files
        .iter()
        .filter(|f| f.is_snapshot)
        .max_by_key(|f| f.max_txid)
        .ok_or_else(|| anyhow!("No snapshot found for database '{}'", db_name))?;

    // Find incrementals after the snapshot
    let incrementals: Vec<_> = files
        .iter()
        .filter(|f| !f.is_snapshot && f.min_txid > snapshot.max_txid)
        .collect();

    tracing::info!(
        "Restoring from snapshot (TXID {}-{}) + {} incrementals",
        snapshot.min_txid, snapshot.max_txid, incrementals.len()
    );

    // Apply snapshot
    let snapshot_data = storage.download_bytes(&snapshot.key).await?;
    let decode_result = ltx::decode_to_db(Cursor::new(snapshot_data), output)?;
    tracing::info!(
        "Restored snapshot to {} (checksum: {:016x})",
        output.display(), decode_result.post_apply_checksum.into_inner()
    );

    let mut restored_txid = snapshot.max_txid;

    // Apply incrementals in order. If an incremental's checksum chain doesn't
    // match, it's from a previous lineage (e.g., a prior leader). Stop applying —
    // the snapshot is the latest consistent state.
    for inc in &incrementals {
        let data = storage.download_bytes(&inc.key).await?;
        match ltx::apply_ltx_to_db(Cursor::new(data), output) {
            Ok(result) => {
                tracing::info!(
                    "Applied incremental (TXID {}-{}, checksum: {:016x})",
                    inc.min_txid, inc.max_txid, result.post_apply_checksum.into_inner()
                );
                restored_txid = inc.max_txid;
            }
            Err(e) if e.to_string().contains("checksum mismatch") || e.to_string().contains("Checksum chain broken") => {
                tracing::info!(
                    "Skipping {} stale incrementals from previous lineage (TXID {}+)",
                    incrementals.len() - incrementals.iter().position(|f| f.key == inc.key).unwrap_or(0),
                    inc.min_txid
                );
                break;
            }
            Err(e) => return Err(e),
        }
    }

    Ok(restored_txid)
}

// ============================================================================
// Retry-wrapped versions
// ============================================================================

/// Take a snapshot with automatic retry on transient failures
pub async fn take_snapshot_with_retry(
    storage: &dyn StorageBackend,
    prefix: &str,
    state: &mut SyncState,
    retry_policy: &RetryPolicy,
) -> Result<()> {
    let timestamp = Utc::now();
    let page_size = get_page_size(&state.db_path).await?;
    let new_txid = state.current_txid + 1;
    let ltx_key = build_ltx_key(prefix, &state.name, 1, 1, new_txid);

    let db_size = std::fs::metadata(&state.db_path)?.len() as usize;
    let estimated_size = db_size.saturating_mul(2);
    let mut ltx_buffer = Vec::with_capacity(estimated_size);
    ltx::encode_snapshot(&mut ltx_buffer, &state.db_path, page_size, new_txid)?;

    let ltx_size = ltx_buffer.len() as u64;

    // Share buffer across retry attempts via Arc to avoid per-attempt clones
    let upload_buffer = std::sync::Arc::new(ltx_buffer);
    let upload_key = ltx_key.clone();
    retry_policy
        .execute_with_context("upload snapshot", || {
            let data_arc = std::sync::Arc::clone(&upload_buffer);
            let key = upload_key.clone();
            async move { storage.upload_bytes(&key, (*data_arc).clone()).await }
        })
        .await?;

    let db_checksum = ltx::compute_checksum_from_file(&state.db_path)?;

    tracing::info!(
        "{}: LTX snapshot uploaded ({} bytes, TXID 1-{}) -> {}",
        state.name,
        ltx_size,
        new_txid,
        ltx_key
    );

    state.current_txid = new_txid;
    state.last_snapshot = Some(timestamp);
    state.db_checksum = Some(db_checksum.into_inner());

    Ok(())
}

/// Sync WAL changes with automatic retry on transient failures
pub async fn sync_wal_with_retry(
    storage: &dyn StorageBackend,
    prefix: &str,
    state: &mut SyncState,
    retry_policy: &RetryPolicy,
) -> Result<u64> {
    use litepages::Checksum;

    let header = match wal::read_header(&state.wal_path).await? {
        Some(h) => h,
        None => return Ok(0),
    };

    let current_size = wal::get_wal_size(&state.wal_path).await?;
    if current_size < state.wal_offset {
        tracing::info!("{}: WAL checkpoint detected, resetting offset", state.name);
        state.wal_offset = 0;
        state.wal_generation += 1;
        // Chain continues through checkpoints — no need to recompute from file
    }

    let (page_map, frame_count, new_offset, max_db_size) =
        wal::read_frames_as_page_map(&state.wal_path, header.page_size, state.wal_offset).await?;

    if page_map.is_empty() {
        return Ok(0);
    }

    let pages: Vec<(u32, Vec<u8>)> = page_map.into_iter().collect();

    let pre_checksum = match state.db_checksum {
        Some(cs) => Checksum::new(cs),
        None => ltx::compute_checksum_from_file(&state.db_path)?,
    };

    let min_txid = state.current_txid + 1;
    let max_txid = min_txid + pages.len() as u64 - 1;
    let commit_page = if max_db_size > 0 {
        max_db_size
    } else {
        let db_size = std::fs::metadata(&state.db_path)?.len();
        (db_size / header.page_size as u64) as u32
    };

    let estimated_size = pages
        .len()
        .saturating_mul(header.page_size as usize)
        .saturating_mul(2);

    // Chained page checksum: O(changed pages), no disk read
    let expected_post = ltx::chain_checksum(pre_checksum, &pages);

    let mut ltx_buffer = Vec::with_capacity(estimated_size);
    let post_checksum = ltx::encode_wal_changes(
        &mut ltx_buffer,
        &pages,
        header.page_size,
        min_txid,
        max_txid,
        commit_page,
        Some(pre_checksum),
        expected_post,
    )?;

    let ltx_size = ltx_buffer.len() as u64;
    let ltx_key = build_ltx_key(prefix, &state.name, GENERATION_LIVE, min_txid, max_txid);

    // Share buffer across retry attempts via Arc to avoid cloning
    let upload_buffer = std::sync::Arc::new(ltx_buffer);
    let upload_key = ltx_key.clone();
    retry_policy
        .execute_with_context("upload WAL changes", || {
            let data_arc = std::sync::Arc::clone(&upload_buffer);
            let key = upload_key.clone();
            async move { storage.upload_bytes(&key, (*data_arc).clone()).await }
        })
        .await?;

    tracing::info!(
        "{}: Synced {} WAL frames as incremental LTX ({} bytes, TXID {}-{}) -> {}",
        state.name,
        frame_count,
        ltx_size,
        min_txid,
        max_txid,
        ltx_key
    );

    state.wal_offset = new_offset;
    state.current_txid = max_txid;
    state.db_checksum = Some(post_checksum.into_inner());

    save_state(storage, prefix, state).await?;

    Ok(frame_count as u64)
}

// ============================================================================
// Legacy manifest-based sync (kept for walrust CLI compatibility)
// ============================================================================

/// Sync WAL changes and update the manifest so followers can discover new LTX files.
///
/// Wraps `sync_wal()` and appends the new entry to the manifest, then saves it to S3.
/// The caller should load the manifest once at startup and pass it through.
pub async fn sync_wal_and_manifest(
    storage: &dyn StorageBackend,
    prefix: &str,
    state: &mut SyncState,
    manifest: &mut Manifest,
) -> Result<u64> {
    let prev_txid = state.current_txid;
    let frames = sync_wal(storage, prefix, state).await?;

    if frames > 0 {
        let min_txid = prev_txid + 1;
        let max_txid = state.current_txid;
        let gen_dir = format_generation(GENERATION_LIVE);
        let ltx_filename = format_ltx_filename(min_txid, max_txid);

        manifest.files.push(LtxEntry {
            filename: format!("{}/{}", gen_dir, ltx_filename),
            min_txid,
            max_txid,
            size: 0,
            created_at: Utc::now().to_rfc3339(),
            is_snapshot: false,
        });
        manifest.current_txid = max_txid;
        manifest.last_checksum = state.db_checksum;

        save_manifest(storage, prefix, manifest).await?;
    }

    Ok(frames)
}

/// Maximum concurrent S3 downloads for incremental pulling.
const PULL_CONCURRENCY: usize = 8;

/// Pull and apply new LTX files from S3 that are ahead of `current_txid`.
///
/// This is the follower's replication primitive. Call it in a loop (e.g., every 1s)
/// to stay in sync with the leader. Returns the new highest applied TXID.
///
/// Optimizations:
/// - Uses `start_after` on S3 LIST to skip past already-applied incrementals
/// - Downloads up to 8 files concurrently, applies sequentially (checksum chain is serial)
pub async fn pull_incremental(
    storage: &dyn StorageBackend,
    prefix: &str,
    db_name: &str,
    db_path: &Path,
    current_txid: u64,
) -> Result<u64> {
    let new_files = discover_incrementals_after(storage, prefix, db_name, current_txid).await?;

    if new_files.is_empty() {
        return Ok(current_txid);
    }

    // Download concurrently — files are independent on S3.
    // We collect into a Vec<(min_txid, key, Result<bytes>)> sorted by min_txid,
    // then apply sequentially because the checksum chain is serial.
    let downloaded = download_parallel(storage, &new_files, PULL_CONCURRENCY).await;

    let mut applied_txid = current_txid;
    let mut applied_count = 0u64;
    let mut stale_count = 0u64;
    for (file, data) in new_files.iter().zip(downloaded.into_iter()) {
        let data = data?;
        match crate::ltx::apply_ltx_to_db(std::io::Cursor::new(data), db_path) {
            Ok(_) => {
                applied_txid = file.max_txid;
                applied_count += 1;
            }
            Err(e) if e.to_string().contains("Checksum chain broken") => {
                stale_count += 1;
                tracing::debug!(
                    "Skipping stale incremental at TXID {} (previous lineage)",
                    file.min_txid
                );
                continue;
            }
            Err(e) => return Err(e),
        }
    }

    if applied_count > 0 {
        tracing::info!(
            "Pulled {} LTX files (skipped {} stale), TXID {} -> {}",
            applied_count, stale_count, current_txid, applied_txid
        );
    }

    Ok(applied_txid)
}

/// Download one S3 object, returning its index for ordered reassembly.
async fn download_one(
    storage: &dyn StorageBackend,
    key: &str,
    idx: usize,
) -> (usize, Result<Vec<u8>>) {
    (idx, storage.download_bytes(key).await)
}

/// Download multiple S3 objects concurrently, preserving order.
///
/// Uses a queue+worker model with `FuturesUnordered`: seeds `concurrency` workers,
/// and as each completes, immediately starts the next download. This keeps all
/// workers saturated — unlike batch-based `join_all` which idles fast workers
/// while waiting for the slowest in each batch.
///
/// Results are returned in input order (indexed).
async fn download_parallel(
    storage: &dyn StorageBackend,
    files: &[DiscoveredLtx],
    concurrency: usize,
) -> Vec<Result<Vec<u8>>> {
    use futures::stream::FuturesUnordered;
    use futures::StreamExt;

    if files.len() == 1 {
        return vec![storage.download_bytes(&files[0].key).await];
    }

    let mut pending = FuturesUnordered::new();
    let mut results: Vec<Option<Result<Vec<u8>>>> = (0..files.len()).map(|_| None).collect();
    let mut next_idx = 0;

    // Seed the queue with initial workers
    while next_idx < concurrency.min(files.len()) {
        pending.push(download_one(storage, &files[next_idx].key, next_idx));
        next_idx += 1;
    }

    // As each download completes, slot the result and start the next one
    while let Some((idx, data)) = pending.next().await {
        results[idx] = Some(data);
        if next_idx < files.len() {
            pending.push(download_one(storage, &files[next_idx].key, next_idx));
            next_idx += 1;
        }
    }

    results.into_iter().map(|r| r.expect("all downloads completed")).collect()
}

// ============================================================================
// Continuous replication loop
// ============================================================================

/// Configuration for continuous WAL replication.
#[derive(Debug, Clone)]
pub struct ReplicationConfig {
    /// How often to sync WAL frames to S3 (default: 1s)
    pub sync_interval: std::time::Duration,
    /// How often to take full snapshots (default: 1 hour)
    pub snapshot_interval: std::time::Duration,
    /// Retry policy for transient S3 failures
    pub retry_policy: RetryPolicy,
    /// Override the database name used in S3 paths (default: derived from db filename)
    pub db_name: Option<String>,
}

impl Default for ReplicationConfig {
    fn default() -> Self {
        Self {
            sync_interval: std::time::Duration::from_secs(1),
            snapshot_interval: std::time::Duration::from_secs(3600),
            retry_policy: RetryPolicy::default_policy(),
            db_name: None,
        }
    }
}

/// Run continuous WAL replication for a single database.
///
/// This is the high-level entry point for embedding walrust as a library.
/// It manages all internal state (SyncState, Manifest, checksums) and runs
/// the sync/snapshot loop until cancelled.
///
/// **Requirements:**
/// - The database MUST have `PRAGMA wal_autocheckpoint=0` set by the caller.
///   Without this, SQLite may auto-checkpoint between syncs, causing data gaps
///   between the last sync and the next snapshot.
/// - The `cancel` receiver should be signaled with `true` to initiate graceful
///   shutdown (final sync + exit).
///
/// **Behavior:**
/// 1. Takes an initial snapshot (required — retries on failure, returns error if unrecoverable)
/// 2. Syncs WAL frames every `sync_interval` via `sync_wal` (incrementals are
///    discoverable from S3 filenames — no manifest needed)
/// 3. Takes periodic snapshots every `snapshot_interval` (compacts the chain)
/// 4. On cancel: does a final sync before returning
///
/// Returns `Ok(())` on clean shutdown, `Err` if the initial snapshot fails after retries
/// or on other unrecoverable errors.
pub async fn run_replication(
    storage: &dyn StorageBackend,
    prefix: &str,
    db_path: &Path,
    config: ReplicationConfig,
    mut cancel: tokio::sync::watch::Receiver<bool>,
) -> Result<()> {
    let mut state = SyncState::new(db_path.to_path_buf())?;
    if let Some(ref name) = config.db_name {
        state.name = name.clone();
    }

    // Initialize checksum from existing database
    if db_path.exists() {
        state.init_checksum()?;
    }

    // Initial snapshot is required — without it, incrementals are unrestorable.
    take_snapshot_with_retry(storage, prefix, &mut state, &config.retry_policy).await?;
    tracing::info!("{}: Initial snapshot taken, starting replication loop", state.name);

    let mut sync_timer = tokio::time::interval(config.sync_interval);
    let mut snapshot_timer = tokio::time::interval(config.snapshot_interval);

    // Skip the first tick (we just took the initial snapshot)
    snapshot_timer.tick().await;

    loop {
        tokio::select! {
            _ = cancel.changed() => {
                if *cancel.borrow() {
                    // Final sync before shutdown
                    match sync_wal(storage, prefix, &mut state).await {
                        Ok(frames) if frames > 0 => {
                            tracing::info!("{}: Final sync captured {} frames before shutdown", state.name, frames);
                        }
                        Err(e) => {
                            tracing::warn!("{}: Final sync failed: {}", state.name, e);
                        }
                        _ => {}
                    }
                    break;
                }
            }
            _ = sync_timer.tick() => {
                match sync_wal(storage, prefix, &mut state).await {
                    Ok(frames) => {
                        if frames > 0 {
                            tracing::info!(
                                "{}: Synced {} frames (TXID {})",
                                state.name, frames, state.current_txid
                            );
                        }
                    }
                    Err(e) => {
                        tracing::error!("{}: WAL sync failed: {}", state.name, e);
                    }
                }
            }
            _ = snapshot_timer.tick() => {
                match take_snapshot_with_retry(storage, prefix, &mut state, &config.retry_policy).await {
                    Ok(()) => {
                        tracing::info!("{}: Periodic snapshot taken (TXID {})", state.name, state.current_txid);
                    }
                    Err(e) => {
                        tracing::error!("{}: Periodic snapshot failed: {}", state.name, e);
                    }
                }
            }
        }
    }

    tracing::info!("{}: Replication loop stopped", state.name);
    Ok(())
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use async_trait::async_trait;
    use std::collections::HashMap as StdHashMap;
    use std::sync::{Arc, Mutex};

    // ---- Mock storage for testing ----

    struct TestStorage {
        objects: StdHashMap<String, Vec<u8>>,
    }

    impl TestStorage {
        fn new() -> Self {
            Self { objects: StdHashMap::new() }
        }

        fn put(&mut self, key: &str, data: Vec<u8>) {
            self.objects.insert(key.to_string(), data);
        }
    }

    #[async_trait]
    impl StorageBackend for TestStorage {
        async fn upload_bytes(&self, _key: &str, _data: Vec<u8>) -> Result<()> { Ok(()) }
        async fn upload_bytes_with_checksum(&self, _key: &str, _data: Vec<u8>, _checksum: &str) -> Result<()> { Ok(()) }
        async fn upload_file(&self, _key: &str, _path: &Path) -> Result<()> { Ok(()) }
        async fn upload_file_with_checksum(&self, _key: &str, _path: &Path, _checksum: &str) -> Result<()> { Ok(()) }
        async fn download_bytes(&self, key: &str) -> Result<Vec<u8>> {
            self.objects.get(key).cloned().ok_or_else(|| anyhow!("not found: {}", key))
        }
        async fn download_file(&self, _key: &str, _path: &Path) -> Result<()> { Ok(()) }
        async fn list_objects(&self, prefix: &str) -> Result<Vec<String>> {
            let mut keys: Vec<String> = self.objects.keys()
                .filter(|k| k.starts_with(prefix))
                .cloned()
                .collect();
            keys.sort();
            Ok(keys)
        }
        async fn list_objects_after(&self, prefix: &str, start_after: &str) -> Result<Vec<String>> {
            let mut keys: Vec<String> = self.objects.keys()
                .filter(|k| k.starts_with(prefix) && k.as_str() > start_after)
                .cloned()
                .collect();
            keys.sort();
            Ok(keys)
        }
        async fn exists(&self, key: &str) -> Result<bool> {
            Ok(self.objects.contains_key(key))
        }
        async fn get_checksum(&self, _key: &str) -> Result<Option<String>> { Ok(None) }
        async fn delete_object(&self, _key: &str) -> Result<()> { Ok(()) }
        async fn delete_objects(&self, _keys: &[String]) -> Result<usize> { Ok(0) }
        fn bucket_name(&self) -> &str { "test-bucket" }
    }

    /// Storage that fails on specific keys — for error propagation tests.
    struct FailOnKeyStorage {
        inner: TestStorage,
        fail_key: String,
    }

    #[async_trait]
    impl StorageBackend for FailOnKeyStorage {
        async fn upload_bytes(&self, k: &str, d: Vec<u8>) -> Result<()> { self.inner.upload_bytes(k, d).await }
        async fn upload_bytes_with_checksum(&self, k: &str, d: Vec<u8>, c: &str) -> Result<()> { self.inner.upload_bytes_with_checksum(k, d, c).await }
        async fn upload_file(&self, k: &str, p: &Path) -> Result<()> { self.inner.upload_file(k, p).await }
        async fn upload_file_with_checksum(&self, k: &str, p: &Path, c: &str) -> Result<()> { self.inner.upload_file_with_checksum(k, p, c).await }
        async fn download_bytes(&self, key: &str) -> Result<Vec<u8>> {
            if key == self.fail_key {
                return Err(anyhow!("simulated download failure for {}", key));
            }
            self.inner.download_bytes(key).await
        }
        async fn download_file(&self, k: &str, p: &Path) -> Result<()> { self.inner.download_file(k, p).await }
        async fn list_objects(&self, p: &str) -> Result<Vec<String>> { self.inner.list_objects(p).await }
        async fn list_objects_after(&self, p: &str, s: &str) -> Result<Vec<String>> { self.inner.list_objects_after(p, s).await }
        async fn exists(&self, k: &str) -> Result<bool> { self.inner.exists(k).await }
        async fn get_checksum(&self, k: &str) -> Result<Option<String>> { self.inner.get_checksum(k).await }
        async fn delete_object(&self, k: &str) -> Result<()> { self.inner.delete_object(k).await }
        async fn delete_objects(&self, k: &[String]) -> Result<usize> { self.inner.delete_objects(k).await }
        fn bucket_name(&self) -> &str { self.inner.bucket_name() }
    }

    /// Storage that records download order — for concurrency verification.
    struct OrderTrackingStorage {
        inner: TestStorage,
        download_order: Arc<Mutex<Vec<String>>>,
    }

    #[async_trait]
    impl StorageBackend for OrderTrackingStorage {
        async fn upload_bytes(&self, k: &str, d: Vec<u8>) -> Result<()> { self.inner.upload_bytes(k, d).await }
        async fn upload_bytes_with_checksum(&self, k: &str, d: Vec<u8>, c: &str) -> Result<()> { self.inner.upload_bytes_with_checksum(k, d, c).await }
        async fn upload_file(&self, k: &str, p: &Path) -> Result<()> { self.inner.upload_file(k, p).await }
        async fn upload_file_with_checksum(&self, k: &str, p: &Path, c: &str) -> Result<()> { self.inner.upload_file_with_checksum(k, p, c).await }
        async fn download_bytes(&self, key: &str) -> Result<Vec<u8>> {
            self.download_order.lock().unwrap().push(key.to_string());
            self.inner.download_bytes(key).await
        }
        async fn download_file(&self, k: &str, p: &Path) -> Result<()> { self.inner.download_file(k, p).await }
        async fn list_objects(&self, p: &str) -> Result<Vec<String>> { self.inner.list_objects(p).await }
        async fn list_objects_after(&self, p: &str, s: &str) -> Result<Vec<String>> { self.inner.list_objects_after(p, s).await }
        async fn exists(&self, k: &str) -> Result<bool> { self.inner.exists(k).await }
        async fn get_checksum(&self, k: &str) -> Result<Option<String>> { self.inner.get_checksum(k).await }
        async fn delete_object(&self, k: &str) -> Result<()> { self.inner.delete_object(k).await }
        async fn delete_objects(&self, k: &[String]) -> Result<usize> { self.inner.delete_objects(k).await }
        fn bucket_name(&self) -> &str { self.inner.bucket_name() }
    }

    fn make_ltx(key: &str, min_txid: u64, max_txid: u64) -> DiscoveredLtx {
        DiscoveredLtx { key: key.to_string(), min_txid, max_txid, is_snapshot: false }
    }

    // ---- format_txid_hex tests ----

    #[test]
    fn test_format_txid_hex_zero() {
        assert_eq!(format_txid_hex(0), "0000000000000000");
    }

    #[test]
    fn test_format_txid_hex_one() {
        assert_eq!(format_txid_hex(1), "0000000000000001");
    }

    #[test]
    fn test_format_txid_hex_large() {
        assert_eq!(format_txid_hex(0xdeadbeef), "00000000deadbeef");
    }

    #[test]
    fn test_format_txid_hex_max() {
        assert_eq!(format_txid_hex(u64::MAX), "ffffffffffffffff");
    }

    // ---- download_parallel tests ----

    #[tokio::test]
    async fn test_download_parallel_single_file() {
        let mut storage = TestStorage::new();
        storage.put("file1.ltx", b"data1".to_vec());

        let files = vec![make_ltx("file1.ltx", 1, 1)];
        let results = download_parallel(&storage, &files, 8).await;

        assert_eq!(results.len(), 1);
        assert_eq!(results[0].as_ref().unwrap(), b"data1");
    }

    #[tokio::test]
    async fn test_download_parallel_preserves_order() {
        let mut storage = TestStorage::new();
        for i in 0..5 {
            storage.put(&format!("file{}.ltx", i), format!("data{}", i).into_bytes());
        }

        let files: Vec<_> = (0..5)
            .map(|i| make_ltx(&format!("file{}.ltx", i), i + 1, i + 1))
            .collect();

        let results = download_parallel(&storage, &files, 8).await;

        assert_eq!(results.len(), 5);
        for i in 0..5 {
            assert_eq!(
                results[i].as_ref().unwrap(),
                format!("data{}", i).as_bytes(),
                "result {} should be data{}", i, i
            );
        }
    }

    #[tokio::test]
    async fn test_download_parallel_concurrency_cap() {
        // 10 files with concurrency=3 — all should complete
        let mut storage = TestStorage::new();
        for i in 0..10 {
            storage.put(&format!("f{:02}.ltx", i), vec![i as u8; 100]);
        }

        let files: Vec<_> = (0..10)
            .map(|i| make_ltx(&format!("f{:02}.ltx", i), i + 1, i + 1))
            .collect();

        let results = download_parallel(&storage, &files, 3).await;

        assert_eq!(results.len(), 10);
        for (i, r) in results.iter().enumerate() {
            let data = r.as_ref().unwrap();
            assert_eq!(data.len(), 100);
            assert_eq!(data[0], i as u8);
        }
    }

    #[tokio::test]
    async fn test_download_parallel_error_propagation() {
        let mut inner = TestStorage::new();
        inner.put("good1.ltx", b"ok".to_vec());
        inner.put("bad.ltx", b"will_fail".to_vec());
        inner.put("good2.ltx", b"ok2".to_vec());

        let storage = FailOnKeyStorage {
            inner,
            fail_key: "bad.ltx".to_string(),
        };

        let files = vec![
            make_ltx("good1.ltx", 1, 1),
            make_ltx("bad.ltx", 2, 2),
            make_ltx("good2.ltx", 3, 3),
        ];

        let results = download_parallel(&storage, &files, 8).await;

        assert!(results[0].is_ok());
        assert!(results[1].is_err());
        assert!(results[1].as_ref().unwrap_err().to_string().contains("simulated download failure"));
        assert!(results[2].is_ok());
    }

    #[tokio::test]
    async fn test_download_parallel_all_downloaded() {
        // Verify every file is actually downloaded
        let mut inner = TestStorage::new();
        let order = Arc::new(Mutex::new(Vec::new()));
        for i in 0..6 {
            inner.put(&format!("dl{}.ltx", i), vec![i as u8]);
        }

        let storage = OrderTrackingStorage {
            inner,
            download_order: order.clone(),
        };

        let files: Vec<_> = (0..6)
            .map(|i| make_ltx(&format!("dl{}.ltx", i), i + 1, i + 1))
            .collect();

        let results = download_parallel(&storage, &files, 2).await;

        assert_eq!(results.len(), 6);
        assert!(results.iter().all(|r| r.is_ok()));
        let downloaded = order.lock().unwrap();
        assert_eq!(downloaded.len(), 6);
        // All keys should be present (order may vary due to concurrency)
        for i in 0..6 {
            assert!(downloaded.contains(&format!("dl{}.ltx", i)));
        }
    }

    #[tokio::test]
    async fn test_download_parallel_empty() {
        let storage = TestStorage::new();
        let files: Vec<DiscoveredLtx> = vec![];
        // FuturesUnordered with empty input should work — but we never call it with 0 files
        // in practice (pull_incremental returns early). Test the 1-file path instead.
        // This is a documentation test showing empty vec is technically fine.
        let results = download_parallel(&storage, &files, 8).await;
        assert!(results.is_empty());
    }

    // ---- discover_incrementals_after tests ----

    #[tokio::test]
    async fn test_discover_incrementals_finds_newer_files() {
        let mut storage = TestStorage::new();
        let prefix = "test/";
        let db = "mydb";
        // TXID 1-1, 2-2, 3-3, 4-5
        storage.put("test/mydb/0000/0000000000000001-0000000000000001.ltx", vec![]);
        storage.put("test/mydb/0000/0000000000000002-0000000000000002.ltx", vec![]);
        storage.put("test/mydb/0000/0000000000000003-0000000000000003.ltx", vec![]);
        storage.put("test/mydb/0000/0000000000000004-0000000000000005.ltx", vec![]);

        // After TXID 2 — should find TXID 3 and 4-5
        let files = discover_incrementals_after(&storage, prefix, db, 2).await.unwrap();
        assert_eq!(files.len(), 2);
        assert_eq!(files[0].min_txid, 3);
        assert_eq!(files[1].min_txid, 4);
        assert_eq!(files[1].max_txid, 5);
    }

    #[tokio::test]
    async fn test_discover_incrementals_empty_when_caught_up() {
        let mut storage = TestStorage::new();
        storage.put("test/mydb/0000/0000000000000001-0000000000000001.ltx", vec![]);
        storage.put("test/mydb/0000/0000000000000002-0000000000000002.ltx", vec![]);

        let files = discover_incrementals_after(&storage, "test/", "mydb", 2).await.unwrap();
        assert!(files.is_empty());
    }

    #[tokio::test]
    async fn test_discover_incrementals_empty_storage() {
        let storage = TestStorage::new();
        let files = discover_incrementals_after(&storage, "test/", "mydb", 0).await.unwrap();
        assert!(files.is_empty());
    }

    #[tokio::test]
    async fn test_discover_incrementals_skips_non_ltx() {
        let mut storage = TestStorage::new();
        storage.put("test/mydb/0000/0000000000000001-0000000000000001.ltx", vec![]);
        storage.put("test/mydb/0000/manifest.json", vec![]); // not an LTX file
        storage.put("test/mydb/0000/0000000000000002-0000000000000002.ltx", vec![]);

        let files = discover_incrementals_after(&storage, "test/", "mydb", 0).await.unwrap();
        assert_eq!(files.len(), 2);
        assert_eq!(files[0].min_txid, 1);
        assert_eq!(files[1].min_txid, 2);
    }

    #[tokio::test]
    async fn test_discover_incrementals_skips_snapshots_prefix() {
        let mut storage = TestStorage::new();
        // Snapshots are in 0001/, incrementals in 0000/
        storage.put("test/mydb/0001/0000000000000001-0000000000000001.ltx", vec![]);
        storage.put("test/mydb/0000/0000000000000002-0000000000000002.ltx", vec![]);

        let files = discover_incrementals_after(&storage, "test/", "mydb", 0).await.unwrap();
        assert_eq!(files.len(), 1);
        assert_eq!(files[0].min_txid, 2);
    }

    #[tokio::test]
    async fn test_discover_incrementals_sorted_by_txid() {
        let mut storage = TestStorage::new();
        // Insert in reverse order — results should be sorted
        storage.put("test/mydb/0000/0000000000000005-0000000000000005.ltx", vec![]);
        storage.put("test/mydb/0000/0000000000000003-0000000000000003.ltx", vec![]);
        storage.put("test/mydb/0000/0000000000000001-0000000000000001.ltx", vec![]);

        let files = discover_incrementals_after(&storage, "test/", "mydb", 0).await.unwrap();
        assert_eq!(files.len(), 3);
        assert_eq!(files[0].min_txid, 1);
        assert_eq!(files[1].min_txid, 3);
        assert_eq!(files[2].min_txid, 5);
    }

    // ---- list_objects_after behavior test ----

    #[tokio::test]
    async fn test_list_objects_after_filters_correctly() {
        let mut storage = TestStorage::new();
        storage.put("prefix/a", vec![]);
        storage.put("prefix/b", vec![]);
        storage.put("prefix/c", vec![]);
        storage.put("prefix/d", vec![]);
        storage.put("other/x", vec![]);

        let result = storage.list_objects_after("prefix/", "prefix/b").await.unwrap();
        assert_eq!(result, vec!["prefix/c", "prefix/d"]);
    }
}
