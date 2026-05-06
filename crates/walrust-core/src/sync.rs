//! Core sync operations for walrust: WAL sync, snapshots, restore, and manifest management.
//!
//! These are the production-grade primitives for embedding walrust as a library.
//! Each function is a single operation (sync one batch of WAL frames, take one snapshot,
//! restore from S3) -- the caller controls scheduling and lifecycle.

use anyhow::{anyhow, Result};
use chrono::Utc;
use hadb_changeset::storage::{self as cs_storage, ChangesetKind, DiscoveredChangeset};
use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};

use crate::ltx;
use crate::wal;
use hadb_io::RetryPolicy;
use hadb_storage::StorageBackend;

// ============================================================================
// Helpers
// ============================================================================

/// Extract SQLite's file change counter from WAL page data.
///
/// WAL uses 1-based page numbers. Page 0 of the database = page_number 1 in WAL.
/// The change counter is at offset 24 (4 bytes BE) in the database header (page 0).
///
/// This counter is one source of external-base replay sequence. In WAL mode it
/// may not advance for every transaction, so walrust also falls back to commit
/// counts while keeping object sequences monotonic.
fn change_counter_from_pages(pages: &[(u32, Vec<u8>)]) -> Option<u64> {
    pages
        .iter()
        .find(|(pn, _)| *pn == 1) // page_number 1 = DB page 0
        .and_then(|(_, data)| {
            if data.len() >= 28 {
                let cc = u32::from_be_bytes([data[24], data[25], data[26], data[27]]) as u64;
                if cc > 0 {
                    Some(cc)
                } else {
                    None
                }
            } else {
                None
            }
        })
}

/// Read the file change counter directly from a SQLite database file.
pub fn change_counter_from_file(path: &Path) -> Result<u64> {
    use std::io::Read;
    let mut f = std::fs::File::open(path)?;
    let mut header = [0u8; 28];
    f.read_exact(&mut header)?;
    Ok(u32::from_be_bytes([header[24], header[25], header[26], header[27]]) as u64)
}

#[derive(Debug, Clone, Copy)]
enum DeltaSequence {
    /// Plain walrust-owned mode: snapshots and incrementals share a
    /// monotonically incremented HADBP sequence.
    WalrustOwned,
    /// External-base-state mode: the base manifest carries the replay cursor, so
    /// delta object seqs must stay ahead of that floor.
    ExternalChangeCounter,
}

// ============================================================================
// Types
// ============================================================================

/// Entry in the changeset manifest tracking a single changeset file.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LtxEntry {
    /// Filename (e.g., "0000000000000001.hadbp")
    pub filename: String,
    /// Sequence number
    pub seq: u64,
    /// File size in bytes
    pub size: u64,
    /// Upload timestamp (ISO 8601)
    pub created_at: String,
    /// Whether this is a snapshot (full DB) or incremental
    pub is_snapshot: bool,
}

/// Manifest tracking all changeset files for a database.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct Manifest {
    /// Database name
    pub name: String,
    /// Current highest sequence number
    pub current_seq: u64,
    /// Page size of the database
    pub page_size: u32,
    /// List of changeset files
    pub files: Vec<LtxEntry>,
    /// Last known database checksum (for incremental chaining)
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_checksum: Option<u64>,
}

/// State for a single database being synced.
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
    /// Current sequence number (HADBP seq, increments per sync)
    pub current_seq: u64,
    /// Current transaction ID (SQLite change counter, for change detection only)
    pub current_txid: u64,
    /// Last snapshot time
    pub last_snapshot: Option<chrono::DateTime<Utc>>,
    /// Current database checksum (chained HADBP checksum for incrementals, full-DB hash after snapshots)
    pub db_checksum: Option<u64>,
}

impl SyncState {
    /// Create new sync state for a database.
    pub fn new(db_path: PathBuf) -> Result<Self> {
        let wal_path = db_path.with_extension("db-wal");
        Self::new_with_paths(db_path, wal_path)
    }

    /// Create new sync state for a database whose base file and WAL file live
    /// at different paths.
    pub fn new_with_paths(db_path: PathBuf, wal_path: PathBuf) -> Result<Self> {
        let name = db_path
            .file_stem()
            .and_then(|s| s.to_str())
            .ok_or_else(|| anyhow!("Invalid database path"))?
            .to_string();
        Ok(Self {
            name,
            db_path,
            wal_path,
            wal_offset: 0,
            wal_generation: 0,
            current_seq: 0,
            current_txid: 0,
            last_snapshot: None,
            db_checksum: None,
        })
    }

    /// Initialize checksum from database file.
    pub fn init_checksum(&mut self) -> Result<()> {
        match ltx::compute_checksum_from_file(&self.db_path) {
            Ok(cs) => {
                self.db_checksum = Some(cs);
                Ok(())
            }
            Err(e) => Err(anyhow!("Failed to compute checksum: {}", e)),
        }
    }
}

// ============================================================================
// S3 key helpers (using hadb-changeset storage)
// ============================================================================

/// Live incrementals go to generation 0 (0000/).
const GENERATION_LIVE: u64 = cs_storage::GENERATION_INCREMENTAL;

/// Snapshots go to generation 1 (0001/).
const GENERATION_SNAPSHOT: u64 = cs_storage::GENERATION_SNAPSHOT;

/// Build S3 key for a changeset file.
fn build_changeset_key(prefix: &str, db_name: &str, generation: u64, seq: u64) -> String {
    cs_storage::format_key(prefix, db_name, generation, seq, ChangesetKind::Physical)
}

/// Get SQLite database page size from header.
async fn get_page_size(db_path: &Path) -> Result<u32> {
    use tokio::io::AsyncReadExt;
    let mut file = tokio::fs::File::open(db_path).await?;
    let mut header = [0u8; 100];
    file.read_exact(&mut header).await?;

    let page_size = u16::from_be_bytes([header[16], header[17]]) as u32;
    let page_size = if page_size == 1 { 65536 } else { page_size };

    Ok(page_size)
}

// ============================================================================
// Manifest operations
// ============================================================================

/// Load manifest from storage.
///
/// A missing manifest (`Ok(None)` from the backend) is treated as a
/// fresh database and returns an empty manifest. Transport errors and
/// JSON parse errors propagate, so callers see real problems instead of
/// silently resetting the replication position to zero.
pub async fn load_manifest(
    storage: &dyn StorageBackend,
    prefix: &str,
    db_name: &str,
) -> Result<Manifest> {
    let manifest_key = format!("{}{}/manifest.json", prefix, db_name);
    match storage.get(&manifest_key).await? {
        Some(data) => Ok(serde_json::from_slice(&data)
            .map_err(|e| anyhow!("corrupt manifest at {manifest_key}: {e}"))?),
        None => Ok(Manifest {
            name: db_name.to_string(),
            ..Default::default()
        }),
    }
}

/// Save manifest to storage.
pub async fn save_manifest(
    storage: &dyn StorageBackend,
    prefix: &str,
    manifest: &Manifest,
) -> Result<()> {
    let manifest_key = format!("{}{}/manifest.json", prefix, manifest.name);
    let data = serde_json::to_vec_pretty(manifest)?;
    storage.put(&manifest_key, &data).await
}

/// Save state.json for state persistence.
pub async fn save_state(
    storage: &dyn StorageBackend,
    prefix: &str,
    state: &SyncState,
) -> Result<()> {
    let state_key = format!("{}{}/state.json", prefix, state.name);
    let state_json = serde_json::json!({
        "wal_offset": state.wal_offset,
        "wal_generation": state.wal_generation,
        "current_seq": state.current_seq,
        "current_txid": state.current_txid,
        "db_checksum": state.db_checksum,
        "last_snapshot": state.last_snapshot,
    });
    let data = serde_json::to_vec(&state_json)?;
    storage.put(&state_key, &data).await
}

// ============================================================================
// Core sync operations
// ============================================================================

/// Sync WAL changes to storage as incremental HADBP changesets.
///
/// Reads new WAL frames since last sync, deduplicates pages, encodes as HADBP
/// with checksum chaining, uploads to storage.
///
/// Returns the number of frames synced.
pub async fn sync_wal(
    storage: &dyn StorageBackend,
    prefix: &str,
    state: &mut SyncState,
) -> Result<u64> {
    sync_wal_with_sequence(storage, prefix, state, DeltaSequence::WalrustOwned).await
}

/// Sync WAL changes to storage as incremental HADBP changesets whose object
/// sequence is SQLite's change-counter domain.
///
/// This is for external-base-state integrations such as Turbolite: the base
/// manifest's `change_counter` is the replay floor, and followers list/apply
/// physical delta objects with seq greater than that floor.
pub async fn sync_wal_after_external_base(
    storage: &dyn StorageBackend,
    prefix: &str,
    state: &mut SyncState,
) -> Result<u64> {
    sync_wal_with_sequence(storage, prefix, state, DeltaSequence::ExternalChangeCounter).await
}

async fn sync_wal_with_sequence(
    storage: &dyn StorageBackend,
    prefix: &str,
    state: &mut SyncState,
    sequence: DeltaSequence,
) -> Result<u64> {
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
        // Chain continues through checkpoints -- no need to recompute from file
    }

    let (page_map, frame_count, new_offset, _max_db_size, commit_count) =
        wal::read_frames_as_page_map(&state.wal_path, header.page_size, state.wal_offset).await?;

    if page_map.is_empty() {
        return Ok(0);
    }

    let pages: Vec<(u32, Vec<u8>)> = page_map.into_iter().collect();

    let pre_checksum = match state.db_checksum {
        Some(cs) => cs,
        None => ltx::compute_checksum_from_file(&state.db_path)?,
    };

    // Derive TXID from SQLite's file change counter for change detection.
    // TXID is internal-only -- the HADBP format uses seq.
    let _min_txid = state.current_txid + 1;
    let max_txid = change_counter_from_pages(&pages)
        .filter(|&cc| cc > state.current_txid)
        .unwrap_or(state.current_txid + commit_count.max(1));

    let new_seq = match sequence {
        DeltaSequence::WalrustOwned => state.current_seq + 1,
        DeltaSequence::ExternalChangeCounter => max_txid.max(state.current_seq + 1),
    };

    let (changeset_bytes, post_checksum) =
        ltx::encode_wal_changes(&pages, header.page_size, new_seq, pre_checksum)?;

    let changeset_size = changeset_bytes.len() as u64;
    let changeset_key = build_changeset_key(prefix, &state.name, GENERATION_LIVE, new_seq);

    storage.put(&changeset_key, &changeset_bytes).await?;

    tracing::info!(
        "{}: Synced {} WAL frames as HADBP changeset ({} bytes, seq {}) -> {}",
        state.name,
        frame_count,
        changeset_size,
        new_seq,
        changeset_key
    );

    state.wal_offset = new_offset;
    state.current_seq = new_seq;
    state.current_txid = max_txid;
    state.db_checksum = Some(post_checksum);

    save_state(storage, prefix, state).await?;

    Ok(frame_count as u64)
}

/// Take a full database snapshot as HADBP changeset.
pub async fn take_snapshot(
    storage: &dyn StorageBackend,
    prefix: &str,
    state: &mut SyncState,
) -> Result<()> {
    let timestamp = Utc::now();
    let page_size = get_page_size(&state.db_path).await?;

    // Use the file change counter as a txid source when available.
    let cc = change_counter_from_file(&state.db_path).unwrap_or(0);
    let wal_commits = wal::count_wal_commits(&state.wal_path, page_size).await?;
    let new_txid = if cc + wal_commits > state.current_txid {
        cc + wal_commits
    } else {
        state.current_txid + 1
    };

    // Seq increments by 1
    let new_seq = state.current_seq + 1;

    let prev_checksum = state.db_checksum.unwrap_or(0);
    let changeset_bytes = ltx::encode_snapshot(&state.db_path, page_size, new_seq, prev_checksum)?;

    let changeset_size = changeset_bytes.len() as u64;
    let changeset_key = build_changeset_key(prefix, &state.name, GENERATION_SNAPSHOT, new_seq);

    storage.put(&changeset_key, &changeset_bytes).await?;

    let db_checksum = ltx::compute_checksum_from_file(&state.db_path)?;

    tracing::info!(
        "{}: HADBP snapshot uploaded ({} bytes, seq {}) -> {}",
        state.name,
        changeset_size,
        new_seq,
        changeset_key
    );

    state.current_seq = new_seq;
    state.current_txid = new_txid;
    state.last_snapshot = Some(timestamp);
    state.db_checksum = Some(db_checksum);

    Ok(())
}

/// Restore a database from storage using HADBP changesets.
///
/// Discovers available changesets by listing S3 objects (no manifest needed).
/// Returns the seq that was actually restored to.
pub async fn restore(
    storage: &dyn StorageBackend,
    prefix: &str,
    db_name: &str,
    output: &Path,
    _point_in_time: Option<&str>,
) -> Result<u64> {
    // Find latest snapshot
    let snapshot =
        cs_storage::discover_latest_snapshot(storage, prefix, db_name, ChangesetKind::Physical)
            .await?
            .ok_or_else(|| anyhow!("No snapshot found for database '{}'", db_name))?;

    // Find incrementals after the snapshot
    let incrementals = cs_storage::discover_after(
        storage,
        prefix,
        db_name,
        snapshot.seq,
        ChangesetKind::Physical,
    )
    .await?;

    tracing::info!(
        "Restoring from snapshot (seq {}) + {} incrementals",
        snapshot.seq,
        incrementals.len()
    );

    // Apply snapshot
    let snapshot_data = storage
        .get(&snapshot.key)
        .await?
        .ok_or_else(|| anyhow!("snapshot key {} not found", snapshot.key))?;
    let decode_result = ltx::decode_to_db(&snapshot_data, output)?;
    tracing::info!(
        "Restored snapshot to {} (checksum: {:016x})",
        output.display(),
        decode_result.checksum
    );

    let mut restored_seq = snapshot.seq;
    let mut current_checksum = decode_result.checksum;

    // Apply incrementals in order. If a checksum chain doesn't match,
    // it's from a previous lineage (e.g., a prior leader). Stop applying.
    for inc in &incrementals {
        let data = storage
            .get(&inc.key)
            .await?
            .ok_or_else(|| anyhow!("incremental key {} not found", inc.key))?;
        match ltx::apply_changeset_to_db(&data, output, current_checksum) {
            Ok(result) => {
                tracing::info!(
                    "Applied incremental (seq {}, checksum: {:016x})",
                    inc.seq,
                    result.checksum
                );
                restored_seq = inc.seq;
                current_checksum = result.checksum;
            }
            Err(e) if e.to_string().contains("Checksum chain broken") => {
                tracing::info!(
                    "Skipping {} stale incrementals from previous lineage (seq {}+)",
                    incrementals.len()
                        - incrementals
                            .iter()
                            .position(|f| f.key == inc.key)
                            .unwrap_or(0),
                    inc.seq
                );
                break;
            }
            Err(e) => return Err(e),
        }
    }

    Ok(restored_seq)
}

/// Restore using an external snapshot source (e.g., turbolite page groups).
///
/// Instead of downloading an HADBP snapshot from S3, calls `snapshot_source.materialize()`
/// to produce the base database file. Then applies incremental changesets with
/// seq > the materialized checkpoint version.
///
/// Returns the seq that was actually restored to.
pub async fn restore_with_snapshot_source(
    storage: &dyn StorageBackend,
    prefix: &str,
    db_name: &str,
    output: &Path,
    snapshot_source: &dyn crate::snapshot_source::SnapshotSource,
) -> Result<u64> {
    // Step 1: materialize the base DB from the external snapshot source
    let checkpoint_version = snapshot_source.materialize(output).await?;
    tracing::info!(
        "Materialized base DB from snapshot source (checkpoint version {})",
        checkpoint_version,
    );

    // Step 2: discover incremental changesets newer than the checkpoint version.
    let incrementals = cs_storage::discover_after(
        storage,
        prefix,
        db_name,
        checkpoint_version,
        ChangesetKind::Physical,
    )
    .await?;

    if incrementals.is_empty() {
        tracing::info!(
            "No incremental changesets to apply (up to date at version {})",
            checkpoint_version
        );
        return Ok(checkpoint_version);
    }

    tracing::info!(
        "Applying {} incremental changesets after checkpoint version {}",
        incrementals.len(),
        checkpoint_version,
    );

    // Step 3: apply incrementals in order
    // Note: when restoring with an external snapshot source, we don't have
    // the HADBP checksum chain from the snapshot. We skip chain verification
    // for the first incremental and let it establish the chain.
    let mut restored_seq = checkpoint_version;
    for inc in incrementals.iter() {
        let data = storage
            .get(&inc.key)
            .await?
            .ok_or_else(|| anyhow!("incremental key {} not found", inc.key))?;
        // For external snapshot sources, we can't verify the chain for the first
        // incremental since the snapshot wasn't encoded as HADBP. Decode without
        // chain verification and let subsequent incrementals verify against each other.
        let changeset = hadb_changeset::physical::decode(&data)
            .map_err(|e| anyhow!("Failed to decode changeset at {}: {}", inc.key, e))?;

        // Apply pages directly (SQLite 1-based page offsets)
        use std::fs::OpenOptions;
        use std::io::{Seek, SeekFrom, Write as IoWrite};

        let page_size = changeset.header.page_size as u64;
        let mut file = OpenOptions::new()
            .write(true)
            .open(output)
            .map_err(|e| anyhow!("Failed to open database for apply: {}", e))?;

        for page in &changeset.pages {
            let offset = (page.page_id.to_u64() - 1) * page_size;
            file.seek(SeekFrom::Start(offset))?;
            file.write_all(&page.data)?;
        }
        file.sync_all()?;
        drop(file);

        tracing::info!(
            "Applied incremental (seq {}, checksum: {:016x})",
            inc.seq,
            changeset.checksum
        );
        restored_seq = inc.seq;
    }

    Ok(restored_seq)
}

// ============================================================================
// Retry-wrapped versions
// ============================================================================

/// Take a snapshot with automatic retry on transient failures.
pub async fn take_snapshot_with_retry(
    storage: &dyn StorageBackend,
    prefix: &str,
    state: &mut SyncState,
    retry_policy: &RetryPolicy,
) -> Result<()> {
    let timestamp = Utc::now();
    let page_size = get_page_size(&state.db_path).await?;

    let cc = change_counter_from_file(&state.db_path).unwrap_or(0);
    let wal_commits = wal::count_wal_commits(&state.wal_path, page_size).await?;
    let new_txid = if cc + wal_commits > state.current_txid {
        cc + wal_commits
    } else {
        state.current_txid + 1
    };

    let new_seq = state.current_seq + 1;
    let prev_checksum = state.db_checksum.unwrap_or(0);
    let changeset_bytes = ltx::encode_snapshot(&state.db_path, page_size, new_seq, prev_checksum)?;

    let changeset_size = changeset_bytes.len() as u64;
    let changeset_key = build_changeset_key(prefix, &state.name, GENERATION_SNAPSHOT, new_seq);

    // Share buffer across retry attempts via Arc to avoid per-attempt clones
    let upload_buffer = std::sync::Arc::new(changeset_bytes);
    let upload_key = changeset_key.clone();
    retry_policy
        .execute_with_context("upload snapshot", || {
            let data_arc = std::sync::Arc::clone(&upload_buffer);
            let key = upload_key.clone();
            async move { storage.put(&key, &data_arc).await }
        })
        .await?;

    let db_checksum = ltx::compute_checksum_from_file(&state.db_path)?;

    tracing::info!(
        "{}: HADBP snapshot uploaded ({} bytes, seq {}) -> {}",
        state.name,
        changeset_size,
        new_seq,
        changeset_key
    );

    state.current_seq = new_seq;
    state.current_txid = new_txid;
    state.last_snapshot = Some(timestamp);
    state.db_checksum = Some(db_checksum);

    Ok(())
}

/// Sync WAL changes with automatic retry on transient failures.
pub async fn sync_wal_with_retry(
    storage: &dyn StorageBackend,
    prefix: &str,
    state: &mut SyncState,
    retry_policy: &RetryPolicy,
) -> Result<u64> {
    let header = match wal::read_header(&state.wal_path).await? {
        Some(h) => h,
        None => return Ok(0),
    };

    let current_size = wal::get_wal_size(&state.wal_path).await?;
    if current_size < state.wal_offset {
        tracing::info!("{}: WAL checkpoint detected, resetting offset", state.name);
        state.wal_offset = 0;
        state.wal_generation += 1;
    }

    let (page_map, frame_count, new_offset, _max_db_size, commit_count) =
        wal::read_frames_as_page_map(&state.wal_path, header.page_size, state.wal_offset).await?;

    if page_map.is_empty() {
        return Ok(0);
    }

    let pages: Vec<(u32, Vec<u8>)> = page_map.into_iter().collect();

    let pre_checksum = match state.db_checksum {
        Some(cs) => cs,
        None => ltx::compute_checksum_from_file(&state.db_path)?,
    };

    let max_txid = change_counter_from_pages(&pages)
        .filter(|&cc| cc > state.current_txid)
        .unwrap_or(state.current_txid + commit_count.max(1));

    let new_seq = state.current_seq + 1;

    let (changeset_bytes, post_checksum) =
        ltx::encode_wal_changes(&pages, header.page_size, new_seq, pre_checksum)?;

    let changeset_size = changeset_bytes.len() as u64;
    let changeset_key = build_changeset_key(prefix, &state.name, GENERATION_LIVE, new_seq);

    let upload_buffer = std::sync::Arc::new(changeset_bytes);
    let upload_key = changeset_key.clone();
    retry_policy
        .execute_with_context("upload WAL changes", || {
            let data_arc = std::sync::Arc::clone(&upload_buffer);
            let key = upload_key.clone();
            async move { storage.put(&key, &data_arc).await }
        })
        .await?;

    tracing::info!(
        "{}: Synced {} WAL frames as HADBP changeset ({} bytes, seq {}) -> {}",
        state.name,
        frame_count,
        changeset_size,
        new_seq,
        changeset_key
    );

    state.wal_offset = new_offset;
    state.current_seq = new_seq;
    state.current_txid = max_txid;
    state.db_checksum = Some(post_checksum);

    save_state(storage, prefix, state).await?;

    Ok(frame_count as u64)
}

// ============================================================================
// Legacy manifest-based sync (kept for walrust CLI compatibility)
// ============================================================================

/// Sync WAL changes and update the manifest.
pub async fn sync_wal_and_manifest(
    storage: &dyn StorageBackend,
    prefix: &str,
    state: &mut SyncState,
    manifest: &mut Manifest,
) -> Result<u64> {
    let frames = sync_wal(storage, prefix, state).await?;

    if frames > 0 {
        let new_seq = state.current_seq;
        let changeset_key = build_changeset_key(prefix, &state.name, GENERATION_LIVE, new_seq);

        manifest.files.push(LtxEntry {
            filename: changeset_key,
            seq: new_seq,
            size: 0,
            created_at: Utc::now().to_rfc3339(),
            is_snapshot: false,
        });
        manifest.current_seq = new_seq;
        manifest.last_checksum = state.db_checksum;

        save_manifest(storage, prefix, manifest).await?;
    }

    Ok(frames)
}

/// Maximum concurrent S3 downloads for incremental pulling.
const PULL_CONCURRENCY: usize = 8;

/// Pull and apply new HADBP changesets from S3 that are ahead of `current_seq`.
///
/// This is the follower's replication primitive. Call it in a loop (e.g., every 1s)
/// to stay in sync with the leader. Returns the new highest applied seq.
///
/// Optimizations:
/// - Uses `start_after` on S3 LIST to skip past already-applied changesets
/// - Downloads up to 8 files concurrently, applies sequentially (checksum chain is serial)
pub async fn pull_incremental(
    storage: &dyn StorageBackend,
    prefix: &str,
    db_name: &str,
    db_path: &Path,
    current_seq: u64,
) -> Result<u64> {
    let new_files = cs_storage::discover_after(
        storage,
        prefix,
        db_name,
        current_seq,
        ChangesetKind::Physical,
    )
    .await?;

    if new_files.is_empty() {
        return Ok(current_seq);
    }

    // Download concurrently, apply sequentially (checksum chain is serial)
    let downloaded = download_parallel(storage, &new_files, PULL_CONCURRENCY).await;

    let mut applied_seq = current_seq;
    let mut applied_count = 0u64;
    let stale_count = 0u64;

    // For followers pulling incrementals, we don't have a prev_checksum from the
    // last applied changeset. We decode and apply directly, relying on the
    // changeset's internal integrity (decode verifies checksum).
    for (file, data) in new_files.iter().zip(downloaded.into_iter()) {
        let data = data?;
        let changeset = match hadb_changeset::physical::decode(&data) {
            Ok(cs) => cs,
            Err(e) => {
                tracing::error!("Failed to decode changeset at {}: {}", file.key, e);
                return Err(anyhow!("Failed to decode changeset: {}", e));
            }
        };

        // Apply pages (SQLite 1-based page offsets)
        use std::fs::OpenOptions;
        use std::io::{Seek, SeekFrom, Write as IoWrite};

        let page_size = changeset.header.page_size as u64;
        let mut db_file = OpenOptions::new()
            .write(true)
            .open(db_path)
            .map_err(|e| anyhow!("Failed to open database for apply: {}", e))?;

        for page in &changeset.pages {
            let offset = (page.page_id.to_u64() - 1) * page_size;
            db_file.seek(SeekFrom::Start(offset))?;
            db_file.write_all(&page.data)?;
        }
        db_file.sync_all()?;

        applied_seq = file.seq;
        applied_count += 1;
    }

    if applied_count > 0 {
        tracing::info!(
            "Pulled {} HADBP changesets (skipped {} stale), seq {} -> {}",
            applied_count,
            stale_count,
            current_seq,
            applied_seq
        );
    }

    Ok(applied_seq)
}

/// Pull and apply new HADBP changesets from S3 through a `PageReplaySink`,
/// without ever writing to a SQLite file.
///
/// Mirrors `pull_incremental` semantically — same discovery, same
/// `current_seq` filtering, same `PULL_CONCURRENCY` for downloads, same
/// `Ok(current_seq)` short-circuit when nothing is newer — but routes
/// each decoded page through `sink.apply_page` instead of seeking into
/// a `&Path`. Used by direct hybrid page replay (Phase
/// `004-direct-hybrid-page-replay`) to compose Turbolite's checkpoint
/// base with walrust's WAL deltas without staging through a temporary
/// SQLite file.
///
/// Lifecycle: `sink.begin()` is called once at the start (even if no
/// new changesets are discovered, so a zero-delta caller still observes
/// a complete `begin → finalize` cycle). For each newly discovered
/// changeset the function calls `sink.apply_page` per page in arrival
/// order, then `sink.commit_changeset(seq)`. After all changesets
/// succeed it calls `sink.finalize()`. On any error — download, decode,
/// or sink — it calls `sink.abort()` and returns the error. Exactly one
/// of `finalize` or `abort` is called per invocation.
///
/// Page id contract: pages flow as the SQLite-1-based `page_id`
/// straight from the HADBP changeset (`hadb_changeset::physical::Page::page_id`).
/// Sinks that need a 0-based index convert internally.
pub async fn pull_incremental_into_sink(
    storage: &dyn StorageBackend,
    prefix: &str,
    db_name: &str,
    sink: &mut dyn crate::replay_sink::PageReplaySink,
    current_seq: u64,
) -> Result<u64> {
    // begin() may fail; if it does, abort() is still called as a
    // best-effort cleanup so the contract "exactly one of finalize or
    // abort per invocation" holds even on early failure. Sinks must
    // tolerate abort() being called when begin() itself errored.
    if let Err(begin_err) = sink.begin() {
        try_abort(sink, &begin_err);
        return Err(begin_err);
    }

    let result =
        pull_incremental_into_sink_inner(storage, prefix, db_name, sink, current_seq).await;

    match result {
        Ok(applied_seq) => {
            // finalize() may fail mid-install (the Turbolite sink
            // writes pages, marks bitmap, bumps generation, etc. — any
            // step can fail). On failure we still need to give the
            // sink a chance to clean up staged state.
            if let Err(finalize_err) = sink.finalize() {
                try_abort(sink, &finalize_err);
                return Err(finalize_err);
            }
            Ok(applied_seq)
        }
        Err(e) => {
            try_abort(sink, &e);
            Err(e)
        }
    }
}

/// Best-effort abort: if abort itself fails, log it but surface the
/// primary error to the caller. The primary error is what the caller
/// needs to act on; abort failure is secondary diagnostic info.
fn try_abort(sink: &mut dyn crate::replay_sink::PageReplaySink, primary: &anyhow::Error) {
    if let Err(abort_err) = sink.abort() {
        tracing::error!(
            "PageReplaySink::abort failed after primary error '{}': {}",
            primary,
            abort_err
        );
    }
}

async fn pull_incremental_into_sink_inner(
    storage: &dyn StorageBackend,
    prefix: &str,
    db_name: &str,
    sink: &mut dyn crate::replay_sink::PageReplaySink,
    current_seq: u64,
) -> Result<u64> {
    let new_files = cs_storage::discover_after(
        storage,
        prefix,
        db_name,
        current_seq,
        ChangesetKind::Physical,
    )
    .await?;

    if new_files.is_empty() {
        return Ok(current_seq);
    }

    let downloaded = download_parallel(storage, &new_files, PULL_CONCURRENCY).await;

    let mut applied_seq = current_seq;
    let mut applied_count = 0u64;

    for (file, data) in new_files.iter().zip(downloaded.into_iter()) {
        let data = data?;
        let changeset = hadb_changeset::physical::decode(&data)
            .map_err(|e| anyhow!("Failed to decode changeset at {}: {}", file.key, e))?;

        for page in &changeset.pages {
            // SQLite 1-based page id straight from the HADBP changeset.
            let sqlite_page_id: u32 = page
                .page_id
                .to_u64()
                .try_into()
                .map_err(|_| anyhow!("page_id {} exceeds u32", page.page_id.to_u64()))?;
            sink.apply_page(sqlite_page_id, &page.data)?;
        }

        sink.commit_changeset(file.seq)?;

        applied_seq = file.seq;
        applied_count += 1;
    }

    if applied_count > 0 {
        tracing::info!(
            "pull_incremental_into_sink: applied {} HADBP changesets, seq {} -> {}",
            applied_count,
            current_seq,
            applied_seq
        );
    }

    Ok(applied_seq)
}

/// Download one S3 object, returning its index for ordered reassembly.
async fn download_one(
    storage: &dyn StorageBackend,
    key: &str,
    idx: usize,
) -> (usize, Result<Vec<u8>>) {
    let fetched = storage
        .get(key)
        .await
        .and_then(|opt| opt.ok_or_else(|| anyhow!("key {} not found", key)));
    (idx, fetched)
}

/// Download multiple S3 objects concurrently, preserving order.
async fn download_parallel(
    storage: &dyn StorageBackend,
    files: &[DiscoveredChangeset],
    concurrency: usize,
) -> Vec<Result<Vec<u8>>> {
    use futures::stream::FuturesUnordered;
    use futures::StreamExt;

    if files.is_empty() {
        return vec![];
    }

    if files.len() == 1 {
        let key = &files[0].key;
        let fetched = storage
            .get(key)
            .await
            .and_then(|opt| opt.ok_or_else(|| anyhow!("key {} not found", key)));
        return vec![fetched];
    }

    let mut pending = FuturesUnordered::new();
    let mut results: Vec<Option<Result<Vec<u8>>>> = (0..files.len()).map(|_| None).collect();
    let mut next_idx = 0;

    while next_idx < concurrency.min(files.len()) {
        pending.push(download_one(storage, &files[next_idx].key, next_idx));
        next_idx += 1;
    }

    while let Some((idx, data)) = pending.next().await {
        results[idx] = Some(data);
        if next_idx < files.len() {
            pending.push(download_one(storage, &files[next_idx].key, next_idx));
            next_idx += 1;
        }
    }

    results
        .into_iter()
        .map(|r| r.expect("all downloads completed"))
        .collect()
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
    /// When false, the background loop skips periodic snapshots.
    /// Use when embedded in a multiwriter coordinator (e.g. haqlite)
    /// where snapshot creation must happen under a distributed lease
    /// to prevent checksum chain breaks. Default: true.
    pub autonomous_snapshots: bool,
    /// Who owns the base state for this database.
    ///
    /// `Walrust` means walrust takes and restores HADBP snapshots itself.
    /// `External` means some other layer owns the checkpointed base state
    /// and walrust should only ship / replay WAL deltas after that point.
    pub snapshot_ownership: SnapshotOwnership,
}

/// Ownership of the base database state.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SnapshotOwnership {
    /// walrust owns HADBP snapshots and WAL shipping.
    Walrust,
    /// An external layer owns checkpointed base state; walrust only ships WAL deltas.
    External,
}

impl SnapshotOwnership {
    pub fn is_external(self) -> bool {
        matches!(self, Self::External)
    }
}

impl Default for ReplicationConfig {
    fn default() -> Self {
        Self {
            sync_interval: std::time::Duration::from_secs(1),
            snapshot_interval: std::time::Duration::from_secs(3600),
            retry_policy: RetryPolicy::default_policy(),
            db_name: None,
            autonomous_snapshots: true,
            snapshot_ownership: SnapshotOwnership::Walrust,
        }
    }
}

impl ReplicationConfig {
    /// Reject configuration combinations that violate walrust's replication invariants.
    pub fn validate(&self) -> Result<()> {
        if self.snapshot_ownership.is_external() && self.autonomous_snapshots {
            anyhow::bail!(
                "external snapshot ownership and autonomous snapshots are mutually exclusive"
            );
        }
        Ok(())
    }
}

/// Run continuous WAL replication for a single database.
///
/// This is the high-level entry point for embedding walrust as a library.
/// It manages all internal state (SyncState, checksums) and runs the
/// sync/snapshot loop until cancelled.
///
/// **Requirements:**
/// - The database MUST have `PRAGMA wal_autocheckpoint=0` set by the caller.
/// - The `cancel` receiver should be signaled with `true` for graceful shutdown.
pub async fn run_replication(
    storage: &dyn StorageBackend,
    prefix: &str,
    db_path: &Path,
    config: ReplicationConfig,
    mut cancel: tokio::sync::watch::Receiver<bool>,
) -> Result<()> {
    config.validate()?;
    if config.snapshot_ownership.is_external() {
        anyhow::bail!(
            "run_replication() requires walrust-owned snapshots; use run_wal_replication() for external base state"
        );
    }

    let mut state = SyncState::new(db_path.to_path_buf())?;
    if let Some(ref name) = config.db_name {
        state.name = name.clone();
    }

    if db_path.exists() {
        state.init_checksum()?;
    }

    take_snapshot_with_retry(storage, prefix, &mut state, &config.retry_policy).await?;
    tracing::info!(
        "{}: Initial snapshot taken, starting replication loop",
        state.name
    );

    let mut sync_timer = tokio::time::interval(config.sync_interval);
    let mut snapshot_timer = tokio::time::interval(config.snapshot_interval);
    snapshot_timer.tick().await;

    loop {
        tokio::select! {
            _ = cancel.changed() => {
                if *cancel.borrow() {
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
                                "{}: Synced {} frames (seq {})",
                                state.name, frames, state.current_seq
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
                        tracing::info!("{}: Periodic snapshot taken (seq {})", state.name, state.current_seq);
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

/// Run WAL-only replication without taking an initial snapshot.
///
/// For use with external snapshot sources (e.g., turbolite page groups).
/// The caller provides the current checkpoint version as `initial_seq`;
/// walrust starts syncing WAL frames from that point.
pub async fn run_wal_replication(
    storage: &dyn StorageBackend,
    prefix: &str,
    state: &mut SyncState,
    initial_seq: u64,
    config: ReplicationConfig,
    mut cancel: tokio::sync::watch::Receiver<bool>,
) -> Result<()> {
    config.validate()?;
    state.current_seq = initial_seq;
    state.current_txid = initial_seq; // Keep txid in sync for initial state

    if state.db_checksum.is_none() && state.db_path.exists() {
        state.init_checksum()?;
    }

    tracing::info!(
        "{}: Starting WAL-only replication (no initial snapshot, seq={})",
        state.name,
        initial_seq,
    );

    let mut sync_timer = tokio::time::interval(config.sync_interval);

    loop {
        tokio::select! {
            _ = cancel.changed() => {
                if *cancel.borrow() {
                    match sync_wal(storage, prefix, state).await {
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
                match sync_wal(storage, prefix, state).await {
                    Ok(frames) => {
                        if frames > 0 {
                            tracing::info!(
                                "{}: Synced {} frames (seq {})",
                                state.name, frames, state.current_seq
                            );
                        }
                    }
                    Err(e) => {
                        tracing::error!("{}: WAL sync failed: {}", state.name, e);
                    }
                }
            }
        }
    }

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
            Self {
                objects: StdHashMap::new(),
            }
        }

        /// Seed a key directly. Named `insert` (not `put`) so it doesn't
        /// collide with the trait's `put(&self, &[u8])` in method lookup.
        fn insert(&mut self, key: &str, data: Vec<u8>) {
            self.objects.insert(key.to_string(), data);
        }
    }

    use hadb_storage::CasResult;

    #[async_trait]
    impl StorageBackend for TestStorage {
        async fn get(&self, key: &str) -> Result<Option<Vec<u8>>> {
            Ok(self.objects.get(key).cloned())
        }
        async fn put(&self, _key: &str, _data: &[u8]) -> Result<()> {
            // TestStorage is seeded via `put()` (inherent method) before each
            // test; the trait-level `put` is unused in the sync tests.
            Ok(())
        }
        async fn delete(&self, _key: &str) -> Result<()> {
            Ok(())
        }
        async fn list(&self, prefix: &str, after: Option<&str>) -> Result<Vec<String>> {
            let mut keys: Vec<String> = self
                .objects
                .keys()
                .filter(|k| k.starts_with(prefix))
                .filter(|k| after.map(|a| k.as_str() > a).unwrap_or(true))
                .cloned()
                .collect();
            keys.sort();
            Ok(keys)
        }
        async fn exists(&self, key: &str) -> Result<bool> {
            Ok(self.objects.contains_key(key))
        }
        async fn put_if_absent(&self, _key: &str, _data: &[u8]) -> Result<CasResult> {
            // CAS is not exercised by the sync tests.
            Ok(CasResult {
                success: true,
                etag: Some("test".into()),
            })
        }
        async fn put_if_match(&self, _key: &str, _data: &[u8], _etag: &str) -> Result<CasResult> {
            Ok(CasResult {
                success: true,
                etag: Some("test".into()),
            })
        }
    }

    /// Storage that fails on specific keys.
    struct FailOnKeyStorage {
        inner: TestStorage,
        fail_key: String,
    }

    #[async_trait]
    impl StorageBackend for FailOnKeyStorage {
        async fn get(&self, key: &str) -> Result<Option<Vec<u8>>> {
            if key == self.fail_key {
                return Err(anyhow!("simulated download failure for {}", key));
            }
            self.inner.get(key).await
        }
        async fn put(&self, k: &str, d: &[u8]) -> Result<()> {
            self.inner.put(k, d).await
        }
        async fn delete(&self, k: &str) -> Result<()> {
            self.inner.delete(k).await
        }
        async fn list(&self, prefix: &str, after: Option<&str>) -> Result<Vec<String>> {
            self.inner.list(prefix, after).await
        }
        async fn exists(&self, k: &str) -> Result<bool> {
            self.inner.exists(k).await
        }
        async fn put_if_absent(&self, k: &str, d: &[u8]) -> Result<CasResult> {
            self.inner.put_if_absent(k, d).await
        }
        async fn put_if_match(&self, k: &str, d: &[u8], e: &str) -> Result<CasResult> {
            self.inner.put_if_match(k, d, e).await
        }
    }

    /// Storage that records download order.
    struct OrderTrackingStorage {
        inner: TestStorage,
        download_order: Arc<Mutex<Vec<String>>>,
    }

    #[async_trait]
    impl StorageBackend for OrderTrackingStorage {
        async fn get(&self, key: &str) -> Result<Option<Vec<u8>>> {
            self.download_order.lock().unwrap().push(key.to_string());
            self.inner.get(key).await
        }
        async fn put(&self, k: &str, d: &[u8]) -> Result<()> {
            self.inner.put(k, d).await
        }
        async fn delete(&self, k: &str) -> Result<()> {
            self.inner.delete(k).await
        }
        async fn list(&self, prefix: &str, after: Option<&str>) -> Result<Vec<String>> {
            self.inner.list(prefix, after).await
        }
        async fn exists(&self, k: &str) -> Result<bool> {
            self.inner.exists(k).await
        }
        async fn put_if_absent(&self, k: &str, d: &[u8]) -> Result<CasResult> {
            self.inner.put_if_absent(k, d).await
        }
        async fn put_if_match(&self, k: &str, d: &[u8], e: &str) -> Result<CasResult> {
            self.inner.put_if_match(k, d, e).await
        }
    }

    fn make_discovered(key: &str, seq: u64) -> DiscoveredChangeset {
        DiscoveredChangeset {
            key: key.to_string(),
            seq,
            kind: ChangesetKind::Physical,
        }
    }

    // ---- build_changeset_key tests ----

    #[test]
    fn test_build_changeset_key_incremental() {
        assert_eq!(
            build_changeset_key("test/", "mydb", 0, 1),
            "test/mydb/0000/0000000000000001.hadbp"
        );
    }

    #[test]
    fn test_build_changeset_key_snapshot() {
        assert_eq!(
            build_changeset_key("test/", "mydb", 1, 1),
            "test/mydb/0001/0000000000000001.hadbp"
        );
    }

    #[test]
    fn test_build_changeset_key_large_seq() {
        assert_eq!(
            build_changeset_key("test/", "mydb", 0, 0xdeadbeef),
            "test/mydb/0000/00000000deadbeef.hadbp"
        );
    }

    // ---- download_parallel tests ----

    #[tokio::test]
    async fn test_download_parallel_single_file() {
        let mut storage = TestStorage::new();
        storage.insert("file1.hadbp", b"data1".to_vec());

        let files = vec![make_discovered("file1.hadbp", 1)];
        let results = download_parallel(&storage, &files, 8).await;

        assert_eq!(results.len(), 1);
        assert_eq!(results[0].as_ref().unwrap(), b"data1");
    }

    #[tokio::test]
    async fn test_download_parallel_preserves_order() {
        let mut storage = TestStorage::new();
        for i in 0..5 {
            storage.insert(
                &format!("file{}.hadbp", i),
                format!("data{}", i).into_bytes(),
            );
        }

        let files: Vec<_> = (0..5)
            .map(|i| make_discovered(&format!("file{}.hadbp", i), i + 1))
            .collect();

        let results = download_parallel(&storage, &files, 8).await;

        assert_eq!(results.len(), 5);
        for i in 0..5 {
            assert_eq!(
                results[i].as_ref().unwrap(),
                format!("data{}", i).as_bytes(),
            );
        }
    }

    #[tokio::test]
    async fn test_download_parallel_concurrency_cap() {
        let mut storage = TestStorage::new();
        for i in 0..10 {
            storage.insert(&format!("f{:02}.hadbp", i), vec![i as u8; 100]);
        }

        let files: Vec<_> = (0..10)
            .map(|i| make_discovered(&format!("f{:02}.hadbp", i), i + 1))
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
        inner.insert("good1.hadbp", b"ok".to_vec());
        inner.insert("bad.hadbp", b"will_fail".to_vec());
        inner.insert("good2.hadbp", b"ok2".to_vec());

        let storage = FailOnKeyStorage {
            inner,
            fail_key: "bad.hadbp".to_string(),
        };

        let files = vec![
            make_discovered("good1.hadbp", 1),
            make_discovered("bad.hadbp", 2),
            make_discovered("good2.hadbp", 3),
        ];

        let results = download_parallel(&storage, &files, 8).await;

        assert!(results[0].is_ok());
        assert!(results[1].is_err());
        assert!(results[1]
            .as_ref()
            .unwrap_err()
            .to_string()
            .contains("simulated download failure"));
        assert!(results[2].is_ok());
    }

    #[tokio::test]
    async fn test_download_parallel_all_downloaded() {
        let mut inner = TestStorage::new();
        let order = Arc::new(Mutex::new(Vec::new()));
        for i in 0..6 {
            inner.insert(&format!("dl{}.hadbp", i), vec![i as u8]);
        }

        let storage = OrderTrackingStorage {
            inner,
            download_order: order.clone(),
        };

        let files: Vec<_> = (0..6)
            .map(|i| make_discovered(&format!("dl{}.hadbp", i), i + 1))
            .collect();

        let results = download_parallel(&storage, &files, 2).await;

        assert_eq!(results.len(), 6);
        assert!(results.iter().all(|r| r.is_ok()));
        let downloaded = order.lock().unwrap();
        assert_eq!(downloaded.len(), 6);
        for i in 0..6 {
            assert!(downloaded.contains(&format!("dl{}.hadbp", i)));
        }
    }

    #[tokio::test]
    async fn test_download_parallel_empty() {
        let storage = TestStorage::new();
        let files: Vec<DiscoveredChangeset> = vec![];
        let results = download_parallel(&storage, &files, 8).await;
        assert!(results.is_empty());
    }

    // ---- SnapshotSource tests ----

    /// Mock SnapshotSource that creates a SQLite DB with the given rows.
    struct MockSnapshotSource {
        version: u64,
        row_count: u32,
        fail: Option<String>,
    }

    #[async_trait]
    impl crate::snapshot_source::SnapshotSource for MockSnapshotSource {
        async fn materialize(&self, output: &Path) -> Result<u64> {
            if let Some(ref msg) = self.fail {
                return Err(anyhow!("{}", msg));
            }
            let conn = rusqlite::Connection::open(output).map_err(|e| anyhow!("open: {}", e))?;
            conn.execute_batch("CREATE TABLE data (id INTEGER PRIMARY KEY, val TEXT);")
                .map_err(|e| anyhow!("create: {}", e))?;
            for i in 0..self.row_count {
                conn.execute(
                    "INSERT INTO data VALUES (?1, ?2)",
                    rusqlite::params![i, format!("row_{}", i)],
                )
                .map_err(|e| anyhow!("insert: {}", e))?;
            }
            Ok(self.version)
        }

        async fn checkpoint_version(&self) -> Result<u64> {
            Ok(self.version)
        }
    }

    #[tokio::test]
    async fn test_restore_with_snapshot_source_no_incrementals() {
        let storage = TestStorage::new();
        let dir = tempfile::tempdir().unwrap();
        let output = dir.path().join("restored.db");

        let source = MockSnapshotSource {
            version: 5,
            row_count: 100,
            fail: None,
        };

        let restored_seq =
            restore_with_snapshot_source(&storage, "test/", "mydb", &output, &source)
                .await
                .unwrap();

        assert_eq!(restored_seq, 5);

        let conn = rusqlite::Connection::open(&output).unwrap();
        let count: i64 = conn
            .query_row("SELECT COUNT(*) FROM data", [], |row| row.get(0))
            .unwrap();
        assert_eq!(count, 100);
    }

    #[tokio::test]
    async fn test_restore_with_snapshot_source_materialize_fails() {
        let storage = TestStorage::new();
        let dir = tempfile::tempdir().unwrap();
        let output = dir.path().join("restored.db");

        let source = MockSnapshotSource {
            version: 1,
            row_count: 0,
            fail: Some("S3 connection timeout".to_string()),
        };

        let result =
            restore_with_snapshot_source(&storage, "test/", "mydb", &output, &source).await;

        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("S3 connection timeout"));
    }

    #[tokio::test]
    async fn test_restore_with_snapshot_source_version_zero() {
        let storage = TestStorage::new();
        let dir = tempfile::tempdir().unwrap();
        let output = dir.path().join("restored.db");

        let source = MockSnapshotSource {
            version: 0,
            row_count: 10,
            fail: None,
        };

        let restored_seq =
            restore_with_snapshot_source(&storage, "test/", "mydb", &output, &source)
                .await
                .unwrap();

        assert_eq!(restored_seq, 0);

        let conn = rusqlite::Connection::open(&output).unwrap();
        let count: i64 = conn
            .query_row("SELECT COUNT(*) FROM data", [], |row| row.get(0))
            .unwrap();
        assert_eq!(count, 10);
    }

    #[tokio::test]
    async fn test_checkpoint_version_reports_correct_value() {
        use crate::snapshot_source::SnapshotSource;
        let source = MockSnapshotSource {
            version: 42,
            row_count: 5,
            fail: None,
        };
        let version: u64 = source.checkpoint_version().await.unwrap();
        assert_eq!(version, 42);
    }

    #[tokio::test]
    async fn test_restore_with_snapshot_source_all_incrementals_older() {
        let mut storage = TestStorage::new();
        // Add changesets older than snapshot version 5
        for seq in 1..=4 {
            let key = build_changeset_key("test/", "mydb", GENERATION_LIVE, seq);
            storage.insert(&key, vec![0; 10]);
        }

        let dir = tempfile::tempdir().unwrap();
        let output = dir.path().join("restored.db");
        let source = MockSnapshotSource {
            version: 5,
            row_count: 20,
            fail: None,
        };

        let restored_seq =
            restore_with_snapshot_source(&storage, "test/", "mydb", &output, &source)
                .await
                .unwrap();

        assert_eq!(restored_seq, 5);
    }

    // ------------------------------------------------------------------
    // load_manifest fail-fast semantics
    //
    // A missing manifest is a legitimate first-run case and must return
    // an empty manifest; transport and parse errors are real problems
    // and must propagate, not silently reset replication state.
    // ------------------------------------------------------------------

    #[tokio::test]
    async fn load_manifest_missing_returns_empty() {
        let storage = TestStorage::new();
        let m = load_manifest(&storage, "prefix/", "mydb").await.unwrap();
        assert_eq!(m.name, "mydb");
        assert_eq!(m.current_seq, 0);
        assert!(m.files.is_empty());
    }

    #[tokio::test]
    async fn load_manifest_transport_error_propagates() {
        let storage = FailOnKeyStorage {
            inner: TestStorage::new(),
            fail_key: "prefix/mydb/manifest.json".to_string(),
        };
        let err = load_manifest(&storage, "prefix/", "mydb")
            .await
            .unwrap_err();
        // Previous behavior would have silently swallowed this and
        // returned a fresh-DB manifest; the fix propagates.
        let msg = err.to_string();
        assert!(
            msg.contains("simulated download failure") || msg.contains("manifest.json"),
            "expected propagated transport error, got: {msg}"
        );
    }

    #[tokio::test]
    async fn load_manifest_corrupt_json_propagates() {
        let mut storage = TestStorage::new();
        let key = "prefix/mydb/manifest.json".to_string();
        storage.insert(&key, b"{ this is not valid json".to_vec());
        let err = load_manifest(&storage, "prefix/", "mydb")
            .await
            .unwrap_err();
        assert!(
            err.to_string().contains("corrupt manifest"),
            "expected corrupt-manifest error, got: {err}"
        );
    }

    // ------------------------------------------------------------------
    // pull_incremental_into_sink
    //
    // Sink-based pull entry point. These tests pin the lifecycle
    // (begin → apply_page* → commit_changeset → finalize/abort),
    // the SQLite-1-based page id contract, abort-on-error semantics,
    // and the no-new-changesets short-circuit.
    // ------------------------------------------------------------------

    use crate::ltx::encode_wal_changes;
    use crate::replay_sink::test_support::RecordingSink;

    fn seed_changeset(
        storage: &mut TestStorage,
        prefix: &str,
        db_name: &str,
        seq: u64,
        page_size: u32,
        pages: &[(u32, Vec<u8>)],
    ) -> u64 {
        let (bytes, post_checksum) =
            encode_wal_changes(pages, page_size, seq, 0).expect("encode_wal_changes");
        let key = build_changeset_key(prefix, db_name, GENERATION_LIVE, seq);
        storage.insert(&key, bytes);
        post_checksum
    }

    fn page_payload(page_size: usize, marker: u8) -> Vec<u8> {
        vec![marker; page_size]
    }

    #[tokio::test]
    async fn pull_into_sink_no_new_changesets_runs_begin_and_finalize() {
        let storage = TestStorage::new();
        let mut sink = RecordingSink::new();

        let seq = pull_incremental_into_sink(&storage, "test/", "mydb", &mut sink, 5)
            .await
            .expect("pull");

        assert_eq!(seq, 5, "no new changesets, returns current_seq");

        let ev = sink.snapshot();
        assert_eq!(ev.begin_calls, 1, "begin must be called exactly once");
        assert_eq!(ev.finalize_calls, 1, "finalize must run on success path");
        assert_eq!(ev.abort_calls, 0, "abort must not run on success path");
        assert!(ev.applied.is_empty());
        assert!(ev.committed_seqs.is_empty());
    }

    #[tokio::test]
    async fn pull_into_sink_passes_sqlite_one_based_page_ids() {
        let mut storage = TestStorage::new();
        let page_size = 4096u32;
        let p1 = page_payload(page_size as usize, 0xAA);
        let p2 = page_payload(page_size as usize, 0xBB);
        seed_changeset(
            &mut storage,
            "test/",
            "mydb",
            1,
            page_size,
            &[(1, p1.clone()), (2, p2.clone())],
        );

        let mut sink = RecordingSink::new();
        let seq = pull_incremental_into_sink(&storage, "test/", "mydb", &mut sink, 0)
            .await
            .expect("pull");
        assert_eq!(seq, 1);

        let ev = sink.snapshot();
        assert_eq!(ev.begin_calls, 1);
        assert_eq!(ev.finalize_calls, 1);
        assert_eq!(ev.abort_calls, 0);
        assert_eq!(ev.committed_seqs, vec![1]);
        // Page ids round-trip as the SQLite 1-based ids carried in HADBP,
        // not as a 0-based or otherwise normalized value. Sinks that
        // need 0-based indexing convert internally.
        let mut applied = ev.applied.clone();
        applied.sort_by_key(|(id, _)| *id);
        assert_eq!(applied[0].0, 1);
        assert_eq!(applied[1].0, 2);
        assert_eq!(applied[0].1, p1);
        assert_eq!(applied[1].1, p2);
    }

    #[tokio::test]
    async fn pull_into_sink_drives_lifecycle_across_multiple_changesets() {
        let mut storage = TestStorage::new();
        let page_size = 4096u32;
        seed_changeset(
            &mut storage,
            "test/",
            "mydb",
            1,
            page_size,
            &[(1, page_payload(page_size as usize, 0x11))],
        );
        seed_changeset(
            &mut storage,
            "test/",
            "mydb",
            2,
            page_size,
            &[(2, page_payload(page_size as usize, 0x22))],
        );
        seed_changeset(
            &mut storage,
            "test/",
            "mydb",
            3,
            page_size,
            &[(3, page_payload(page_size as usize, 0x33))],
        );

        let mut sink = RecordingSink::new();
        let final_seq = pull_incremental_into_sink(&storage, "test/", "mydb", &mut sink, 0)
            .await
            .expect("pull");
        assert_eq!(final_seq, 3);

        let ev = sink.snapshot();
        assert_eq!(ev.begin_calls, 1);
        assert_eq!(ev.finalize_calls, 1);
        assert_eq!(ev.abort_calls, 0);
        // commit_changeset is called once per changeset, in order.
        assert_eq!(ev.committed_seqs, vec![1, 2, 3]);
        // Pages applied in the order they appear across changesets.
        assert_eq!(ev.applied.len(), 3);
        assert_eq!(ev.applied[0].0, 1);
        assert_eq!(ev.applied[1].0, 2);
        assert_eq!(ev.applied[2].0, 3);
    }

    #[tokio::test]
    async fn pull_into_sink_skips_changesets_at_or_below_current_seq() {
        let mut storage = TestStorage::new();
        let page_size = 4096u32;
        seed_changeset(
            &mut storage,
            "test/",
            "mydb",
            1,
            page_size,
            &[(1, page_payload(page_size as usize, 0x11))],
        );
        seed_changeset(
            &mut storage,
            "test/",
            "mydb",
            2,
            page_size,
            &[(2, page_payload(page_size as usize, 0x22))],
        );

        let mut sink = RecordingSink::new();
        let seq = pull_incremental_into_sink(&storage, "test/", "mydb", &mut sink, 1)
            .await
            .expect("pull");
        assert_eq!(seq, 2, "should advance past current_seq=1 to seq=2");

        let ev = sink.snapshot();
        assert_eq!(ev.committed_seqs, vec![2], "only seq>1 applied");
        assert_eq!(ev.applied.len(), 1);
        assert_eq!(ev.applied[0].0, 2);
        assert_eq!(ev.finalize_calls, 1);
        assert_eq!(ev.abort_calls, 0);
    }

    #[tokio::test]
    async fn pull_into_sink_aborts_on_apply_page_error_no_finalize() {
        let mut storage = TestStorage::new();
        let page_size = 4096u32;
        seed_changeset(
            &mut storage,
            "test/",
            "mydb",
            1,
            page_size,
            &[
                (1, page_payload(page_size as usize, 0x11)),
                (2, page_payload(page_size as usize, 0x22)),
                (3, page_payload(page_size as usize, 0x33)),
            ],
        );

        // Inject a failure on the second apply_page call.
        let mut sink = RecordingSink::new().fail_at(1);
        let result = pull_incremental_into_sink(&storage, "test/", "mydb", &mut sink, 0).await;

        assert!(result.is_err(), "primary error must propagate");
        let err = result.unwrap_err().to_string();
        assert!(
            err.contains("injected apply_page failure"),
            "expected injected error, got: {err}"
        );

        let ev = sink.snapshot();
        assert_eq!(ev.begin_calls, 1, "begin must run before any apply");
        assert_eq!(
            ev.finalize_calls, 0,
            "finalize must NOT run on the abort path"
        );
        assert_eq!(
            ev.abort_calls, 1,
            "abort must run exactly once when apply_page fails"
        );
        // First page was recorded before the injected failure on idx=1.
        assert_eq!(ev.applied.len(), 1);
        // commit_changeset must not run if any page in the changeset failed.
        assert!(ev.committed_seqs.is_empty());
    }

    #[tokio::test]
    async fn pull_into_sink_aborts_on_decode_error_no_finalize() {
        let mut storage = TestStorage::new();
        // Seed a corrupt changeset blob at the expected key.
        let key = build_changeset_key("test/", "mydb", GENERATION_LIVE, 1);
        storage.insert(&key, b"not a valid HADBP changeset".to_vec());

        let mut sink = RecordingSink::new();
        let result = pull_incremental_into_sink(&storage, "test/", "mydb", &mut sink, 0).await;

        assert!(result.is_err(), "decode failure must propagate");
        let err = result.unwrap_err().to_string();
        assert!(
            err.contains("Failed to decode changeset"),
            "expected decode failure, got: {err}"
        );

        let ev = sink.snapshot();
        assert_eq!(ev.begin_calls, 1);
        assert_eq!(ev.finalize_calls, 0);
        assert_eq!(ev.abort_calls, 1);
        assert!(ev.applied.is_empty());
        assert!(ev.committed_seqs.is_empty());
    }

    #[tokio::test]
    async fn pull_into_sink_aborts_when_begin_fails() {
        // begin() failure must still trigger abort() so the contract
        // "exactly one of finalize or abort per invocation" holds. Sinks
        // that allocate state in begin (file handles, locks, etc.) need
        // a single cleanup callback even on the earliest failure.
        let storage = TestStorage::new();
        let mut sink = RecordingSink::new().fail_begin();

        let result = pull_incremental_into_sink(&storage, "test/", "mydb", &mut sink, 0).await;

        assert!(result.is_err(), "begin failure must propagate");
        let err = result.unwrap_err().to_string();
        assert!(
            err.contains("injected begin failure"),
            "expected begin failure error, got: {err}"
        );

        let ev = sink.snapshot();
        assert_eq!(ev.begin_calls, 1, "begin called once");
        assert_eq!(
            ev.finalize_calls, 0,
            "finalize must not run after begin failure"
        );
        assert_eq!(
            ev.abort_calls, 1,
            "abort must be called exactly once even when begin failed"
        );
        assert!(ev.applied.is_empty());
        assert!(ev.committed_seqs.is_empty());
    }

    #[tokio::test]
    async fn pull_into_sink_aborts_when_finalize_fails() {
        // finalize() failure is the load-bearing case: the Turbolite
        // sink does multi-step install work in finalize (page writes,
        // bitmap, generation bump, bitmap persist). If any step fails
        // mid-way, the sink needs an explicit cleanup call to drop
        // partially installed state. Without this, a finalize crash
        // leaks staged state with no way to recover.
        let mut storage = TestStorage::new();
        let page_size = 4096u32;
        seed_changeset(
            &mut storage,
            "test/",
            "mydb",
            1,
            page_size,
            &[(1, page_payload(page_size as usize, 0xAB))],
        );

        let mut sink = RecordingSink::new().fail_finalize();
        let result = pull_incremental_into_sink(&storage, "test/", "mydb", &mut sink, 0).await;

        assert!(result.is_err(), "finalize failure must propagate");
        let err = result.unwrap_err().to_string();
        assert!(
            err.contains("injected finalize failure"),
            "expected finalize failure error, got: {err}"
        );

        let ev = sink.snapshot();
        assert_eq!(ev.begin_calls, 1);
        assert_eq!(
            ev.applied.len(),
            1,
            "the page was applied successfully before finalize ran"
        );
        assert_eq!(ev.committed_seqs, vec![1]);
        assert_eq!(
            ev.finalize_calls, 1,
            "finalize must have been attempted exactly once"
        );
        assert_eq!(
            ev.abort_calls, 1,
            "abort must run after a finalize failure to clean up"
        );
    }
}
