//! Core sync operations for walrust: WAL sync, snapshots, restore, and manifest management.
//!
//! These are the production-grade primitives for embedding walrust as a library.
//! Each function is a single operation (sync one batch of WAL frames, take one snapshot,
//! restore from S3) -- the caller controls scheduling and lifecycle.

use anyhow::{anyhow, Result};
use chrono::Utc;
use hadb_changeset::storage::{self as cs_storage, ChangesetKind, DiscoveredChangeset};
use serde::{Deserialize, Serialize};
use std::fs::{self, File};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use tokio::sync::Mutex;

use crate::errors::WalrustError;
use crate::ltx;
use crate::shadow;
use crate::wal;
use hadb_io::RetryPolicy;
use hadb_storage::StorageBackend;

// ============================================================================
// Helpers
// ============================================================================

struct AtomicRestore {
    path: PathBuf,
    published: bool,
}

impl AtomicRestore {
    fn new(output: &Path) -> Self {
        static NEXT_ID: AtomicU64 = AtomicU64::new(0);

        let parent = output.parent().unwrap_or_else(|| Path::new("."));
        let file_name = output
            .file_name()
            .and_then(|name| name.to_str())
            .unwrap_or("restored.db");
        let id = NEXT_ID.fetch_add(1, Ordering::Relaxed);
        let path = parent.join(format!(
            ".{file_name}.restore-{}-{id}.tmp",
            std::process::id()
        ));
        Self {
            path,
            published: false,
        }
    }

    fn path(&self) -> &Path {
        &self.path
    }

    fn publish(mut self, output: &Path) -> Result<()> {
        std::fs::rename(&self.path, output).map_err(|e| {
            anyhow!(
                "failed to atomically publish restored database {} over {}: {e}",
                self.path.display(),
                output.display()
            )
        })?;
        self.published = true;
        Ok(())
    }
}

impl Drop for AtomicRestore {
    fn drop(&mut self) {
        if !self.published {
            let _ = std::fs::remove_file(&self.path);
        }
    }
}

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
    /// External-base-state mode: the base manifest carries the replay cursor.
    /// Delta object seqs continue contiguously from that floor; the SQLite
    /// change counter is tracked separately as `current_txid`.
    ExternalChangeCounter,
}

// ============================================================================
// Types
// ============================================================================

/// Explicit base cursor for callers whose checkpoint/base state is owned by
/// another layer, such as Turbolite.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ExternalBaseCursor {
    /// The durable base replay sequence. Delta objects must start at `seq + 1`.
    pub seq: u64,
    /// Checksum of the materialized base. The first delta must chain from this.
    pub checksum: u64,
}

/// Follower cursor for incremental pull APIs.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PullCursor {
    /// Highest HADBP sequence already applied by the follower.
    pub seq: u64,
    /// Running HADBP checksum at `seq`. The next changeset must chain from it.
    pub checksum: u64,
}

/// A loud replication event the core surfaces to an embedding process (the CLI
/// binary, haqlite, ...). The core library has no webhook client of its own, so
/// this is the plumbing an embedder wires to its own alert channel.
#[derive(Debug, Clone)]
pub struct RolloverEvent {
    /// Database name.
    pub db_name: String,
    /// Which sync mode observed the rollover.
    pub mode: &'static str,
    /// Whether walrust recovered (re-anchored with a snapshot) or refused
    /// (hard-failed pending an external re-anchor).
    pub recovered: bool,
    /// Human-readable detail for the alert.
    pub message: String,
}

/// Optional sink for [`RolloverEvent`]s. Cloneable (shares one `Arc`) and
/// `Debug` (so it can live on `SyncState`/`ReplicationConfig`) without exposing
/// the closure. Default is a no-op.
#[derive(Clone, Default)]
pub struct RolloverObserver(Option<Arc<dyn Fn(RolloverEvent) + Send + Sync>>);

impl std::fmt::Debug for RolloverObserver {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(if self.0.is_some() {
            "RolloverObserver(set)"
        } else {
            "RolloverObserver(none)"
        })
    }
}

impl RolloverObserver {
    /// Build an observer from a callback.
    pub fn new(f: impl Fn(RolloverEvent) + Send + Sync + 'static) -> Self {
        Self(Some(Arc::new(f)))
    }

    /// Deliver an event if an observer is installed.
    pub fn emit(&self, event: RolloverEvent) {
        if let Some(f) = &self.0 {
            f(event);
        }
    }
}

/// State for a single database being synced.
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
    /// Walrust-owned stream lineage. When set, HADBP objects are written under
    /// a lineage namespace so cold starts cannot silently reuse an old chain.
    pub lineage_id: Option<String>,
    /// External-base cursor this state is anchored to, if snapshot ownership is
    /// outside walrust. Used to persist a local WAL-offset proof for the remote
    /// object-chain head without writing remote `state.json`.
    pub external_base: Option<ExternalBaseCursor>,
    /// Current transaction ID (SQLite change counter, for change detection only)
    pub current_txid: u64,
    /// Last snapshot time
    pub last_snapshot: Option<chrono::DateTime<Utc>>,
    /// Current database checksum (chained HADBP checksum for incrementals, full-DB hash after snapshots)
    pub db_checksum: Option<u64>,
    /// WAL header salt of the generation `wal_offset` indexes into. An in-place
    /// WAL reset re-salts the header at the same/larger size; comparing salt
    /// (not just size) catches that rollover so the new prefix is not skipped.
    pub wal_salt: Option<(u32, u32)>,
    /// Running SQLite WAL checksum `(s0, s1)` at `wal_offset`. Seeds per-frame
    /// validation for the next incremental read so a torn tail frame is rejected
    /// rather than shipped.
    pub wal_checksum_chain: Option<(u32, u32)>,
    /// Runtime-only sink for loud rollover events. Not persisted; the embedder
    /// installs it (e.g. wired to the CLI's webhook sender).
    pub rollover_observer: RolloverObserver,
    /// Optional long-running read transaction that pins the live WAL so an
    /// external checkpoint cannot restart it. Only set in walrust-owned mode
    /// (D2). Not persisted; rebuilt on add()/reopen.
    pub checkpoint_blocker: Option<Arc<Mutex<rusqlite::Connection>>>,
}

impl std::fmt::Debug for SyncState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SyncState")
            .field("name", &self.name)
            .field("db_path", &self.db_path)
            .field("wal_path", &self.wal_path)
            .field("wal_offset", &self.wal_offset)
            .field("wal_generation", &self.wal_generation)
            .field("current_seq", &self.current_seq)
            .field("lineage_id", &self.lineage_id)
            .field("external_base", &self.external_base)
            .field("current_txid", &self.current_txid)
            .field("last_snapshot", &self.last_snapshot)
            .field("db_checksum", &self.db_checksum)
            .field("wal_salt", &self.wal_salt)
            .field("wal_checksum_chain", &self.wal_checksum_chain)
            .field("rollover_observer", &self.rollover_observer)
            .field("checkpoint_blocker", &self.checkpoint_blocker.is_some())
            .finish()
    }
}

impl Clone for SyncState {
    fn clone(&self) -> Self {
        Self {
            name: self.name.clone(),
            db_path: self.db_path.clone(),
            wal_path: self.wal_path.clone(),
            wal_offset: self.wal_offset,
            wal_generation: self.wal_generation,
            current_seq: self.current_seq,
            lineage_id: self.lineage_id.clone(),
            external_base: self.external_base.clone(),
            current_txid: self.current_txid,
            last_snapshot: self.last_snapshot,
            db_checksum: self.db_checksum,
            wal_salt: self.wal_salt,
            wal_checksum_chain: self.wal_checksum_chain,
            rollover_observer: self.rollover_observer.clone(),
            checkpoint_blocker: self.checkpoint_blocker.clone(),
        }
    }
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
            lineage_id: None,
            external_base: None,
            current_txid: 0,
            last_snapshot: None,
            db_checksum: None,
            wal_salt: None,
            wal_checksum_chain: None,
            rollover_observer: RolloverObserver::default(),
            checkpoint_blocker: None,
        })
    }

    /// Ensure this walrust-owned stream has a durable object namespace.
    pub fn ensure_lineage_id(&mut self) -> &str {
        self.lineage_id
            .get_or_insert_with(|| uuid::Uuid::new_v4().simple().to_string())
            .as_str()
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

fn build_lineage_changeset_key(
    prefix: &str,
    db_name: &str,
    lineage_id: &str,
    generation: u64,
    seq: u64,
) -> String {
    format!(
        "{}{}/lineages/{}/{:04x}/{:016x}.{}",
        prefix,
        db_name,
        lineage_id,
        generation,
        seq,
        ChangesetKind::Physical.extension()
    )
}

fn build_state_changeset_key(prefix: &str, state: &SyncState, generation: u64, seq: u64) -> String {
    match state.lineage_id.as_deref() {
        Some(lineage_id) => {
            build_lineage_changeset_key(prefix, &state.name, lineage_id, generation, seq)
        }
        None => build_changeset_key(prefix, &state.name, generation, seq),
    }
}

fn lineage_generation_prefix(
    prefix: &str,
    db_name: &str,
    lineage_id: &str,
    generation: u64,
) -> String {
    format!(
        "{}{}/lineages/{}/{:04x}/",
        prefix, db_name, lineage_id, generation
    )
}

fn state_key(prefix: &str, db_name: &str) -> String {
    format!("{}{}/state.json", prefix, db_name)
}

#[derive(Debug, Default, Deserialize)]
struct RemoteSyncState {
    #[serde(default)]
    lineage_id: Option<String>,
}

async fn active_lineage_id(
    storage: &dyn StorageBackend,
    prefix: &str,
    db_name: &str,
) -> Result<Option<String>> {
    let key = state_key(prefix, db_name);
    let Some(data) = storage
        .get(&key)
        .await
        .map_err(|e| anyhow!("failed to load replication state {key}: {e}"))?
    else {
        return Ok(None);
    };
    let remote = serde_json::from_slice::<RemoteSyncState>(&data)
        .map_err(|e| anyhow!("failed to parse replication state {key}: {e}"))?;
    Ok(remote.lineage_id)
}

async fn discover_after_in_namespace(
    storage: &dyn StorageBackend,
    prefix: &str,
    db_name: &str,
    lineage_id: Option<&str>,
    generation: u64,
    after_seq: u64,
    kind: ChangesetKind,
) -> Result<Vec<DiscoveredChangeset>> {
    if generation == GENERATION_LIVE && lineage_id.is_none() {
        return cs_storage::discover_after(storage, prefix, db_name, after_seq, kind).await;
    }

    let ext = kind.extension();
    let gen_prefix = match lineage_id {
        Some(lineage_id) => lineage_generation_prefix(prefix, db_name, lineage_id, generation),
        None => format!("{}{}/{:04x}/", prefix, db_name, generation),
    };
    let start_after_key = format!("{}{:016x}.{}", gen_prefix, after_seq, ext);
    let keys = storage.list(&gen_prefix, Some(&start_after_key)).await?;

    let mut changesets = Vec::new();
    for key in keys {
        let Some(filename) = key.strip_prefix(&gen_prefix) else {
            continue;
        };
        if !filename.ends_with(&format!(".{}", ext)) {
            continue;
        }
        let hex_part = &filename[..filename.len() - ext.len() - 1];
        let Ok(seq) = u64::from_str_radix(hex_part, 16) else {
            continue;
        };
        if seq <= after_seq {
            continue;
        }
        changesets.push(DiscoveredChangeset { key, seq, kind });
    }

    changesets.sort_by_key(|c| c.seq);
    Ok(changesets)
}

async fn discover_latest_snapshot_in_namespace(
    storage: &dyn StorageBackend,
    prefix: &str,
    db_name: &str,
    lineage_id: Option<&str>,
    kind: ChangesetKind,
) -> Result<Option<DiscoveredChangeset>> {
    if lineage_id.is_none() {
        return cs_storage::discover_latest_snapshot(storage, prefix, db_name, kind).await;
    }

    let mut snapshots = discover_after_in_namespace(
        storage,
        prefix,
        db_name,
        lineage_id,
        GENERATION_SNAPSHOT,
        0,
        kind,
    )
    .await?;
    snapshots.sort_by_key(|c| c.seq);
    Ok(snapshots.pop())
}

async fn discover_latest_snapshot_at_or_before_in_namespace(
    storage: &dyn StorageBackend,
    prefix: &str,
    db_name: &str,
    lineage_id: Option<&str>,
    target_seq: u64,
    kind: ChangesetKind,
) -> Result<Option<DiscoveredChangeset>> {
    let mut snapshots = discover_after_in_namespace(
        storage,
        prefix,
        db_name,
        lineage_id,
        GENERATION_SNAPSHOT,
        0,
        kind,
    )
    .await?;
    snapshots.retain(|changeset| changeset.seq <= target_seq);
    snapshots.sort_by_key(|c| c.seq);
    Ok(snapshots.pop())
}

fn parse_point_in_time_seq(point_in_time: Option<&str>) -> Result<Option<u64>> {
    Ok(point_in_time
        .map(|pit| {
            pit.parse::<u64>().map_err(|_| {
                anyhow::Error::from(WalrustError::restore(
                    "Invalid point_in_time format. Use sequence/TXID number",
                ))
            })
        })
        .transpose()?)
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

async fn checkpoint_wal(db_path: &Path) -> Result<()> {
    let db_path = db_path.to_path_buf();
    tokio::task::spawn_blocking(move || -> Result<()> {
        let conn = rusqlite::Connection::open(&db_path)?;
        conn.busy_timeout(std::time::Duration::from_secs(5))?;
        let (busy, log_frames, checkpointed_frames): (i64, i64, i64) =
            conn.query_row("PRAGMA wal_checkpoint(TRUNCATE);", [], |row| {
                Ok((row.get(0)?, row.get(1)?, row.get(2)?))
            })?;
        if busy != 0 || checkpointed_frames < log_frames {
            anyhow::bail!(
                "{}: snapshot checkpoint incomplete (busy={}, log_frames={}, checkpointed_frames={})",
                db_path.display(),
                busy,
                log_frames,
                checkpointed_frames
            );
        }
        Ok(())
    })
    .await?
}

async fn reset_wal_cursor_after_snapshot(state: &mut SyncState) {
    state.wal_offset = 0;
    state.wal_generation += 1;
    state.wal_salt = wal::read_header(&state.wal_path)
        .await
        .ok()
        .flatten()
        .map(|h| h.salt());
    state.wal_checksum_chain = None;
}

/// Release the walrust-owned checkpoint blocker so walrust can run its own
/// checkpoint. The read transaction must be rolled back before another
/// connection can checkpoint the WAL (D2).
async fn release_checkpoint_blocker(state: &mut SyncState) -> Result<()> {
    if let Some(blocker) = state.checkpoint_blocker.take() {
        let guard = blocker.lock().await;
        guard.execute_batch("ROLLBACK;")?;
    }
    Ok(())
}

/// Re-establish the walrust-owned checkpoint blocker after a walrust-controlled
/// checkpoint. Writing to `_walrust_seq` and opening a read transaction pins a
/// real frame in the fresh WAL immediately (D2).
async fn reacquire_checkpoint_blocker(state: &mut SyncState) -> Result<()> {
    if state.checkpoint_blocker.is_some() {
        return Ok(());
    }
    let conn = shadow::ShadowWal::open_checkpoint_blocker(&state.db_path)?;
    state.checkpoint_blocker = Some(Arc::new(Mutex::new(conn)));
    Ok(())
}

// ============================================================================
// Manifest operations
// ============================================================================

/// Save state.json for state persistence.
pub async fn save_state(
    storage: &dyn StorageBackend,
    prefix: &str,
    state: &SyncState,
) -> Result<()> {
    let state_key = state_key(prefix, &state.name);
    let data = state_json_bytes(state)?;
    storage.put(&state_key, &data).await
}

fn state_json_bytes(state: &SyncState) -> Result<Vec<u8>> {
    let state_json = state_json_value(state);
    Ok(serde_json::to_vec(&state_json)?)
}

fn state_json_value(state: &SyncState) -> serde_json::Value {
    serde_json::json!({
        "wal_offset": state.wal_offset,
        "wal_generation": state.wal_generation,
        "current_seq": state.current_seq,
        "lineage_id": state.lineage_id.as_deref(),
        "current_txid": state.current_txid,
        "db_checksum": state.db_checksum,
        "last_snapshot": state.last_snapshot,
        "wal_salt": state.wal_salt,
        "wal_checksum_chain": state.wal_checksum_chain,
    })
}

/// Fail if a walrust-owned database already has an active remote state.
///
/// Fresh walrust-owned `add()` creates a new lineage. If `state.json` already
/// exists, another lineage is active and callers must restore/reopen explicitly
/// instead of silently replacing the active namespace.
pub async fn ensure_no_saved_state(
    storage: &dyn StorageBackend,
    prefix: &str,
    db_name: &str,
) -> Result<()> {
    let state_key = state_key(prefix, db_name);
    if storage.exists(&state_key).await? {
        anyhow::bail!(
            "{}: database already has replication state at {}; use add_without_snapshot after restoring/reopening instead of creating a new walrust-owned lineage",
            db_name,
            state_key
        );
    }
    Ok(())
}

/// Save the initial walrust-owned state only if no active state exists.
///
/// This is the race-closing half of [`ensure_no_saved_state`]: two creators can
/// both observe absence, but only one may publish the active `state.json`.
pub async fn save_initial_state(
    storage: &dyn StorageBackend,
    prefix: &str,
    state: &SyncState,
) -> Result<()> {
    let state_key = state_key(prefix, &state.name);
    let data = state_json_bytes(state)?;
    let cas = storage.put_if_absent(&state_key, &data).await?;
    if cas.success {
        return Ok(());
    }

    anyhow::bail!(
        "{}: database already has replication state at {}; refusing to replace active walrust-owned lineage",
        state.name,
        state_key
    );
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct ExternalBaseLocalProgress {
    version: u32,
    base_seq: u64,
    base_checksum: u64,
    current_seq: u64,
    db_checksum: u64,
    wal_offset: u64,
    wal_generation: u64,
    wal_salt: Option<(u32, u32)>,
    wal_checksum_chain: Option<(u32, u32)>,
}

impl ExternalBaseLocalProgress {
    fn from_state(state: &SyncState) -> Result<Self> {
        let base = state.external_base.ok_or_else(|| {
            anyhow!(
                "{}: cannot save external-base progress without an external base anchor",
                state.name
            )
        })?;
        let db_checksum = state.db_checksum.ok_or_else(|| {
            anyhow!(
                "{}: cannot save external-base progress without current checksum",
                state.name
            )
        })?;
        Ok(Self {
            version: 1,
            base_seq: base.seq,
            base_checksum: base.checksum,
            current_seq: state.current_seq,
            db_checksum,
            wal_offset: state.wal_offset,
            wal_generation: state.wal_generation,
            wal_salt: state.wal_salt,
            wal_checksum_chain: state.wal_checksum_chain,
        })
    }
}

fn local_progress_dir_for(db_path: &Path) -> PathBuf {
    let parent = db_path.parent().unwrap_or_else(|| Path::new("."));
    let stem = db_path
        .file_stem()
        .and_then(|name| name.to_str())
        .unwrap_or("database");
    parent.join(format!(".walrust-{stem}"))
}

fn external_base_progress_path(state: &SyncState) -> PathBuf {
    local_progress_dir_for(&state.db_path).join("external-base-progress.json")
}

fn fsync_dir(path: &Path) -> Result<()> {
    File::open(path)?
        .sync_all()
        .map_err(|e| anyhow!("failed to fsync directory {}: {}", path.display(), e))
}

fn save_external_base_progress(state: &SyncState) -> Result<()> {
    let progress_path = external_base_progress_path(state);
    let progress_dir = progress_path.parent().ok_or_else(|| {
        anyhow!(
            "invalid external-base progress path {}",
            progress_path.display()
        )
    })?;
    fs::create_dir_all(progress_dir).map_err(|e| {
        anyhow!(
            "{}: failed to create local external-base progress directory {}: {}",
            state.name,
            progress_dir.display(),
            e
        )
    })?;

    let tmp_path = progress_path.with_extension("json.tmp");
    let progress = ExternalBaseLocalProgress::from_state(state)?;
    let bytes = serde_json::to_vec_pretty(&progress)?;

    {
        let mut file = File::create(&tmp_path).map_err(|e| {
            anyhow!(
                "{}: failed to create temporary local external-base progress file {}: {}",
                state.name,
                tmp_path.display(),
                e
            )
        })?;
        file.write_all(&bytes).map_err(|e| {
            anyhow!(
                "{}: failed to write temporary local external-base progress file {}: {}",
                state.name,
                tmp_path.display(),
                e
            )
        })?;
        file.sync_all().map_err(|e| {
            anyhow!(
                "{}: failed to fsync temporary local external-base progress file {}: {}",
                state.name,
                tmp_path.display(),
                e
            )
        })?;
    }

    fs::rename(&tmp_path, &progress_path).map_err(|e| {
        anyhow!(
            "{}: failed to install local external-base progress file {}: {}",
            state.name,
            progress_path.display(),
            e
        )
    })?;
    fsync_dir(progress_dir)?;
    Ok(())
}

fn load_external_base_progress(state: &SyncState) -> Result<Option<ExternalBaseLocalProgress>> {
    let progress_path = external_base_progress_path(state);
    let bytes = match fs::read(&progress_path) {
        Ok(bytes) => bytes,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(e) => {
            return Err(anyhow!(
                "{}: failed to read local external-base progress file {}: {}",
                state.name,
                progress_path.display(),
                e
            ));
        }
    };

    let progress: ExternalBaseLocalProgress = serde_json::from_slice(&bytes).map_err(|e| {
        anyhow!(
            "{}: failed to parse local external-base progress file {}: {}",
            state.name,
            progress_path.display(),
            e
        )
    })?;
    if progress.version != 1 {
        anyhow::bail!(
            "{}: unsupported local external-base progress version {} in {}",
            state.name,
            progress.version,
            progress_path.display()
        );
    }
    Ok(Some(progress))
}

/// Local, fsynced write-ahead record of the walrust-owned changeset we are
/// about to publish. It is the self-authorship proof the B11 crash-window
/// recovery is gated on: a same-seq CAS conflict is only adopted as *our own*
/// prior crashed publish if the object at that seq is byte-for-byte the one THIS
/// process recorded here (matching lineage, seq, base checksum, AND the exact
/// changeset checksum we computed). A second live writer sharing the same
/// lineage/base -- the split-brain case -- records its OWN intent on its OWN
/// disk, so its object's checksum never matches ours and the conflict correctly
/// hard-fails instead of silently re-legitimizing split-brain.
#[derive(Debug, Clone, Serialize, Deserialize)]
struct PublishIntent {
    version: u32,
    lineage_id: Option<String>,
    seq: u64,
    pre_checksum: u64,
    changeset_checksum: u64,
}

fn publish_intent_path(state: &SyncState) -> PathBuf {
    local_progress_dir_for(&state.db_path).join("publish-intent.json")
}

/// Persist (temp + fsync + rename + dir fsync) the publish intent BEFORE the
/// remote CAS put, so a crash in the put-then-save_state window leaves durable
/// proof of what we published.
fn save_publish_intent(
    state: &SyncState,
    seq: u64,
    pre_checksum: u64,
    changeset_checksum: u64,
) -> Result<()> {
    let intent_path = publish_intent_path(state);
    let intent_dir = intent_path
        .parent()
        .ok_or_else(|| anyhow!("invalid publish-intent path {}", intent_path.display()))?;
    fs::create_dir_all(intent_dir).map_err(|e| {
        anyhow!(
            "{}: failed to create local publish-intent directory {}: {}",
            state.name,
            intent_dir.display(),
            e
        )
    })?;

    let tmp_path = intent_path.with_extension("json.tmp");
    let intent = PublishIntent {
        version: 1,
        lineage_id: state.lineage_id.clone(),
        seq,
        pre_checksum,
        changeset_checksum,
    };
    let bytes = serde_json::to_vec_pretty(&intent)?;
    {
        let mut file = File::create(&tmp_path).map_err(|e| {
            anyhow!(
                "{}: failed to create temporary publish-intent file {}: {}",
                state.name,
                tmp_path.display(),
                e
            )
        })?;
        file.write_all(&bytes).map_err(|e| {
            anyhow!(
                "{}: failed to write temporary publish-intent file {}: {}",
                state.name,
                tmp_path.display(),
                e
            )
        })?;
        file.sync_all().map_err(|e| {
            anyhow!(
                "{}: failed to fsync temporary publish-intent file {}: {}",
                state.name,
                tmp_path.display(),
                e
            )
        })?;
    }
    fs::rename(&tmp_path, &intent_path).map_err(|e| {
        anyhow!(
            "{}: failed to install publish-intent file {}: {}",
            state.name,
            intent_path.display(),
            e
        )
    })?;
    fsync_dir(intent_dir)?;
    Ok(())
}

fn load_publish_intent(state: &SyncState) -> Result<Option<PublishIntent>> {
    let intent_path = publish_intent_path(state);
    let bytes = match fs::read(&intent_path) {
        Ok(bytes) => bytes,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(e) => {
            return Err(anyhow!(
                "{}: failed to read local publish-intent file {}: {}",
                state.name,
                intent_path.display(),
                e
            ));
        }
    };
    let intent: PublishIntent = serde_json::from_slice(&bytes).map_err(|e| {
        anyhow!(
            "{}: failed to parse local publish-intent file {}: {}",
            state.name,
            intent_path.display(),
            e
        )
    })?;
    if intent.version != 1 {
        anyhow::bail!(
            "{}: unsupported publish-intent version {} in {}",
            state.name,
            intent.version,
            intent_path.display()
        );
    }
    Ok(Some(intent))
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

async fn get_existing_after_failed_cas(
    storage: &dyn StorageBackend,
    key: &str,
) -> Result<Option<Vec<u8>>> {
    const RETRY_DELAYS_MS: [u64; 4] = [10, 25, 50, 100];

    for delay_ms in [0].into_iter().chain(RETRY_DELAYS_MS) {
        if delay_ms > 0 {
            tokio::time::sleep(std::time::Duration::from_millis(delay_ms)).await;
        }
        if let Some(existing) = storage.get(key).await? {
            return Ok(Some(existing));
        }
    }

    Ok(None)
}

async fn put_changeset_if_absent(
    storage: &dyn StorageBackend,
    key: &str,
    bytes: &[u8],
    db_name: &str,
    seq: u64,
    mode: &str,
) -> Result<()> {
    let cas = storage.put_if_absent(key, bytes).await?;
    if cas.success {
        return Ok(());
    }

    let existing = get_existing_after_failed_cas(storage, key)
        .await?
        .ok_or_else(|| {
            anyhow::anyhow!(
                "{}: {} duplicate changeset seq {} vanished after CAS failure at {}",
                db_name,
                mode,
                seq,
                key
            )
        })?;
    if existing != bytes {
        return Err(WalrustError::equivocation(format!(
            "{}: {} duplicate changeset seq {}; refusing overwrite at {}",
            db_name, mode, seq, key
        ))
        .into());
    }

    tracing::info!(
        "{}: {} changeset seq {} already exists with identical bytes; treating publish as idempotent",
        db_name,
        mode,
        seq
    );
    Ok(())
}

/// B11 crash-window discriminator. Returns `Some(post_checksum)` only when the
/// object already at `key` is provably *our own* prior publish for `seq` — the
/// signature of a put that succeeded durably but whose `save_state` never ran
/// before a crash. Two conditions must BOTH hold:
///
/// 1. `prior_intent` — the local, fsynced write-ahead record THIS process wrote
///    before that put — names this exact publish (matching lineage, seq, and
///    base checksum). A process that never published this object has no such
///    record; a different writer records its own intent on its own disk.
/// 2. The stored object decodes as a physical changeset at `seq` whose
///    `prev_checksum` equals our base checksum AND whose checksum is
///    byte-for-byte the one we recorded in the intent.
///
/// This is what distinguishes a self-crash from a second live writer sharing the
/// same lineage/base (split-brain): the second writer's object encodes different
/// bytes, so its checksum never matches our recorded intent, and the conflict
/// correctly hard-fails instead of being silently adopted. Returns `None` (=>
/// hard equivocation) for a missing/mismatched intent, undecodable bytes, a
/// different base, or a checksum that is not the one we authored.
async fn existing_changeset_is_our_publish(
    storage: &dyn StorageBackend,
    key: &str,
    seq: u64,
    our_pre_checksum: u64,
    lineage_id: Option<&str>,
    prior_intent: Option<&PublishIntent>,
) -> Result<Option<u64>> {
    let Some(intent) = prior_intent else {
        return Ok(None);
    };
    // The local write-ahead record must name THIS publish (self-authorship).
    if intent.seq != seq
        || intent.pre_checksum != our_pre_checksum
        || intent.lineage_id.as_deref() != lineage_id
    {
        return Ok(None);
    }
    let Some(existing) = storage.get(key).await? else {
        return Ok(None);
    };
    let decoded = match ltx::decode_sqlite_changeset(&existing) {
        Ok(d) => d,
        Err(_) => return Ok(None),
    };
    if decoded.header.seq == seq
        && decoded.header.prev_checksum == our_pre_checksum
        && decoded.checksum == intent.changeset_checksum
    {
        Ok(Some(decoded.checksum))
    } else {
        Ok(None)
    }
}

/// Result of reading the next batch of WAL frames for a sync site.
struct WalBatch {
    page_map: std::collections::HashMap<u32, Vec<u8>>,
    frame_count: usize,
    new_offset: u64,
    final_db_size: u32,
    commit_count: u64,
    rollover_detected: bool,
}

/// Detect WAL rollover (by size *or* salt change) and read the next batch of
/// committed frames with full SQLite checksum-chain validation.
///
/// Rollover detection is two-pronged: a shrink (`current_size <
/// wal_offset`) is the classic checkpoint, but SQLite can also reset the WAL
/// in place with a *new salt* at the same or larger size. Comparing the header
/// salt against `state.wal_salt` catches that case so the new generation's
/// prefix is read from offset 0 instead of being skipped as a continuation.
///
/// The running checksum chain (`state.wal_checksum_chain`) seeds per-frame
/// validation; a torn tail frame is rejected (see [`wal::read_frames_as_page_map_checked`]).
async fn read_next_wal_batch(state: &mut SyncState, header: &wal::WalHeader) -> Result<WalBatch> {
    let current_salt = header.salt();

    // Two-pronged rollover detection: size shrink OR salt change.
    let size = wal::get_wal_size(&state.wal_path).await?;
    let size_rollover = size < state.wal_offset;
    let salt_rollover = matches!(state.wal_salt, Some(prev) if prev != current_salt);

    let rollover_detected = size_rollover || salt_rollover;

    if rollover_detected {
        tracing::info!(
            "{}: WAL rollover detected (size_rollover={}, salt_rollover={}); resetting offset",
            state.name,
            size_rollover,
            salt_rollover
        );
        state.wal_offset = 0;
        // New generation: chain must re-seed from the new header.
        state.wal_checksum_chain = None;
    }
    state.wal_salt = Some(current_salt);

    // Seed the checked read with the running chain when continuing mid-WAL.
    let chain_seed = if state.wal_offset == 0 || state.wal_offset == wal::WAL_HEADER_SIZE {
        None
    } else {
        state.wal_checksum_chain
    };

    let (page_map, frame_count, new_offset, final_db_size, commit_count, new_chain) =
        wal::read_frames_as_page_map_checked(
            &state.wal_path,
            header.page_size,
            state.wal_offset,
            chain_seed,
        )
        .await?;

    // Advance the running chain only for the frames actually consumed.
    if let Some(chain) = new_chain {
        state.wal_checksum_chain = Some(chain);
    }

    Ok(WalBatch {
        page_map,
        frame_count,
        new_offset,
        final_db_size,
        commit_count,
        rollover_detected,
    })
}

async fn ensure_database_in_wal_mode(db_path: &Path, db_name: &str) -> Result<()> {
    let db_path = db_path.to_path_buf();
    let mode = tokio::task::spawn_blocking(move || -> Result<String> {
        let conn = rusqlite::Connection::open(&db_path)?;
        let mode: String = conn.query_row("PRAGMA journal_mode", [], |row| row.get(0))?;
        Ok(mode)
    })
    .await??;

    if mode.eq_ignore_ascii_case("wal") {
        Ok(())
    } else {
        Err(anyhow!(
            "{}: SQLite journal_mode is '{}', expected WAL; replication cannot continue",
            db_name,
            mode
        ))
    }
}

async fn sync_wal_with_sequence(
    storage: &dyn StorageBackend,
    prefix: &str,
    state: &mut SyncState,
    sequence: DeltaSequence,
) -> Result<u64> {
    let header = match wal::read_header(&state.wal_path).await? {
        Some(h) => h,
        None => {
            ensure_database_in_wal_mode(&state.db_path, &state.name).await?;
            return Ok(0);
        }
    };

    let previous_wal_offset = state.wal_offset;
    let previous_wal_generation = state.wal_generation;
    let previous_wal_salt = state.wal_salt;
    let previous_wal_checksum_chain = state.wal_checksum_chain;

    let WalBatch {
        page_map,
        frame_count,
        new_offset,
        final_db_size,
        commit_count,
        rollover_detected,
    } = read_next_wal_batch(state, &header).await?;

    if rollover_detected {
        match sequence {
            DeltaSequence::WalrustOwned => {
                // External checkpoint reset a walrust-owned WAL — unexpected (we own
                // it, autocheckpoint should be 0). Log loudly and re-anchor. The core
                // library has no webhook channel; the binary surfaces rollovers on
                // its own direct/independent path via notify_upload_failed.
                tracing::error!(
                    "{}: WAL rollover detected; publishing a new snapshot instead of an incremental across the gap",
                    state.name
                );
                state.rollover_observer.emit(RolloverEvent {
                    db_name: state.name.clone(),
                    mode: "walrust-owned",
                    recovered: true,
                    message: format!(
                        "{}: WAL rollover detected; re-anchoring with a fresh snapshot",
                        state.name
                    ),
                });
                take_snapshot(storage, prefix, state).await?;
                save_state(storage, prefix, state).await?;
                return Ok(1);
            }
            DeltaSequence::ExternalChangeCounter => {
                state.wal_offset = previous_wal_offset;
                state.wal_generation = previous_wal_generation;
                state.wal_salt = previous_wal_salt;
                state.wal_checksum_chain = previous_wal_checksum_chain;
                let message = format!(
                    "{}: WAL rollover detected after external base; refusing to publish deltas until the external base is re-anchored",
                    state.name
                );
                state.rollover_observer.emit(RolloverEvent {
                    db_name: state.name.clone(),
                    mode: "external-base",
                    recovered: false,
                    message: message.clone(),
                });
                anyhow::bail!(message);
            }
        }
    }

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
        DeltaSequence::WalrustOwned | DeltaSequence::ExternalChangeCounter => state.current_seq + 1,
    };

    if final_db_size == 0 {
        anyhow::bail!(
            "{}: WAL commit produced end_page_count=0 with {} dirty pages; refusing to publish a truncating changeset",
            state.name,
            pages.len()
        );
    }
    let (changeset_bytes, post_checksum) = ltx::encode_wal_changes_with_end_page_count(
        &pages,
        header.page_size,
        new_seq,
        pre_checksum,
        final_db_size as u64,
    )?;

    let changeset_size = changeset_bytes.len() as u64;
    let changeset_key = build_state_changeset_key(prefix, state, GENERATION_LIVE, new_seq);

    match sequence {
        DeltaSequence::ExternalChangeCounter => {
            put_changeset_if_absent(
                storage,
                &changeset_key,
                &changeset_bytes,
                &state.name,
                new_seq,
                "external-base",
            )
            .await?;
        }
        DeltaSequence::WalrustOwned => {
            // B11: load the PRIOR local publish-intent (the pre-crash one, if
            // any) BEFORE overwriting it -- that is the self-authorship proof a
            // same-seq conflict is checked against -- then durably record the
            // intent for THIS publish ahead of the CAS put so a crash in the
            // put-then-save_state window leaves proof of what we published.
            let prior_intent = load_publish_intent(state)?;
            save_publish_intent(state, new_seq, pre_checksum, post_checksum)?;
            if let Err(e) = put_changeset_if_absent(
                storage,
                &changeset_key,
                &changeset_bytes,
                &state.name,
                new_seq,
                "walrust-owned",
            )
            .await
            {
                // B11 recovery: close the put-then-save_state crash window.
                // If the conflicting object is PROVABLY our own prior publish at
                // this seq (matching local publish-intent + byte-identical
                // checksum, see existing_changeset_is_our_publish), adopt it as
                // committed and re-anchor with a fresh snapshot so any frames
                // written after the crash are folded in. A foreign object, a
                // second live writer's divergent object, or garbage still
                // propagates as a hard equivocation error.
                if WalrustError::is_equivocation(&e) {
                    if let Some(adopted_post) = existing_changeset_is_our_publish(
                        storage,
                        &changeset_key,
                        new_seq,
                        pre_checksum,
                        state.lineage_id.as_deref(),
                        prior_intent.as_ref(),
                    )
                    .await?
                    {
                        tracing::error!(
                            "{}: same-seq changeset conflict at seq {} from a prior crashed publish; \
                             adopting it and re-anchoring with a fresh snapshot",
                            state.name,
                            new_seq
                        );
                        state.rollover_observer.emit(RolloverEvent {
                            db_name: state.name.clone(),
                            mode: "walrust-owned",
                            recovered: true,
                            message: format!(
                                "{}: same-seq changeset conflict at seq {} from a prior crashed publish; re-anchoring with a fresh snapshot",
                                state.name, new_seq
                            ),
                        });
                        state.current_seq = new_seq;
                        state.current_txid = max_txid;
                        state.db_checksum = Some(adopted_post);
                        take_snapshot(storage, prefix, state).await?;
                        save_state(storage, prefix, state).await?;
                        return Ok(1);
                    }
                }
                return Err(e);
            }
        }
    }

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

    match sequence {
        DeltaSequence::WalrustOwned => {
            save_state(storage, prefix, state).await?;
        }
        DeltaSequence::ExternalChangeCounter => {
            if state.external_base.is_some() {
                save_external_base_progress(state)?;
            }
        }
    }

    Ok(frame_count as u64)
}

/// Initialize external-base state from an explicit base cursor and the
/// already-published physical delta object chain. Remote `state.json` is not
/// consulted here; the base plus `.hadbp` chain is the protocol truth.
pub async fn initialize_external_base_state(
    storage: &dyn StorageBackend,
    prefix: &str,
    state: &mut SyncState,
    base: ExternalBaseCursor,
) -> Result<()> {
    state.current_seq = base.seq;
    state.current_txid = base.seq;
    state.db_checksum = Some(base.checksum);
    state.wal_offset = 0;
    state.wal_generation = 0;
    state.external_base = Some(base);

    let head = match cs_storage::discover_strict_physical_chain(
        storage,
        prefix,
        &state.name,
        cs_storage::StrictChainBase {
            seq: base.seq,
            checksum: base.checksum,
        },
    )
    .await
    {
        Ok(head) => head,
        Err(base_err) => {
            let Some(same_seq_checksum) =
                external_same_seq_changeset_checksum(storage, prefix, &state.name, base.seq)
                    .await?
            else {
                return Err(base_err);
            };
            cs_storage::discover_strict_physical_chain(
                storage,
                prefix,
                &state.name,
                cs_storage::StrictChainBase {
                    seq: base.seq,
                    checksum: same_seq_checksum,
                },
            )
            .await
            .map_err(|same_seq_err| {
                anyhow!(
                    "external base chain failed from page-base checksum ({base_err}) and same-seq changeset checksum ({same_seq_err})"
                )
            })?
        }
    };
    state.current_seq = head.seq;
    state.current_txid = head.seq;
    state.db_checksum = Some(head.checksum);

    // The object chain is authoritative for seq/checksum. If no external
    // delta object exists after the base, locally present WAL bytes are not
    // proven durable remotely and must be read from the beginning. If the
    // chain is ahead, a local fsynced progress record must map that exact
    // chain head to the local WAL cursor; guessing from current WAL size would
    // skip unpublished bytes after restart.
    if head.count == 0 {
        state.wal_offset = 0;
        state.wal_generation = 0;
        state.wal_salt = None;
        state.wal_checksum_chain = None;
    } else {
        let progress = load_external_base_progress(state)?.ok_or_else(|| {
            anyhow!(
                "{}: remote external-base chain is at seq {} but no matching local external-base progress file exists; refusing to guess WAL offset",
                state.name,
                head.seq
            )
        })?;
        if progress.base_seq != base.seq || progress.base_checksum != base.checksum {
            anyhow::bail!(
                "{}: local external-base progress anchor mismatch (progress base seq/checksum {}:{:016x}, requested {}:{:016x})",
                state.name,
                progress.base_seq,
                progress.base_checksum,
                base.seq,
                base.checksum
            );
        }
        if progress.current_seq != head.seq || progress.db_checksum != head.checksum {
            anyhow::bail!(
                "{}: local external-base progress does not match remote chain head (progress seq/checksum {}:{:016x}, remote {}:{:016x})",
                state.name,
                progress.current_seq,
                progress.db_checksum,
                head.seq,
                head.checksum
            );
        }
        state.wal_offset = progress.wal_offset;
        state.wal_generation = progress.wal_generation;
        state.wal_salt = progress.wal_salt;
        state.wal_checksum_chain = progress.wal_checksum_chain;
    }

    Ok(())
}

async fn external_same_seq_changeset_checksum(
    storage: &dyn StorageBackend,
    prefix: &str,
    name: &str,
    seq: u64,
) -> Result<Option<u64>> {
    if seq == 0 {
        return Ok(None);
    }
    let base_key = build_changeset_key(prefix, name, GENERATION_LIVE, seq);
    let Some(data) = storage.get(&base_key).await? else {
        return Ok(None);
    };
    let changeset = ltx::decode_sqlite_changeset(&data).map_err(|e| {
        anyhow!(
            "failed to decode external base changeset at {}: {}",
            base_key,
            e
        )
    })?;
    if changeset.header.seq != seq {
        anyhow::bail!(
            "external base changeset seq mismatch at {}: expected {}, found {}",
            base_key,
            seq,
            changeset.header.seq
        );
    }
    Ok(Some(changeset.checksum))
}

// ============================================================================
// Fenced TLM_DELTA envelope publish + discovery
// ============================================================================

use crate::external_delta::{self, DeltaPayloadV1};

/// File extension for fenced delta envelope objects. Distinct from the
/// legacy `.hadbp` extension so the two never collide in one prefix and a
/// follower can tell them apart from a plain listing.
const DELTA_ENVELOPE_EXT: &str = "tlmd";

/// Key for a fenced TLM_DELTA envelope object.
///
/// Layout: `{prefix}{db_name}/{generation:04x}/{seq:016x}.tlmd`,
/// matching hadb-changeset's key shape (zero-padded hex seq sorts
/// lexicographically = numerically) but with the `.tlmd` extension.
fn delta_envelope_key(prefix: &str, db_name: &str, seq: u64) -> String {
    format!(
        "{}{}/{:04x}/{:016x}.{}",
        prefix, db_name, GENERATION_LIVE, seq, DELTA_ENVELOPE_EXT
    )
}

/// Parse the seq out of a `.tlmd` envelope key. `None` for keys that
/// aren't delta envelopes (wrong extension, malformed hex).
///
/// Requires the exact 16-char zero-padded width `delta_envelope_key`
/// emits: lexical object-store list order only equals numeric seq order at
/// a fixed width, so a foreign/short `.tlmd` key must be rejected rather
/// than parsed and mis-ordered.
fn parse_delta_envelope_seq(key: &str) -> Option<u64> {
    let file = key.rsplit('/').next()?;
    let hex = file.strip_suffix(&format!(".{DELTA_ENVELOPE_EXT}"))?;
    if hex.len() != 16 {
        return None;
    }
    u64::from_str_radix(hex, 16).ok()
}

/// Parameters the caller supplies to publish one fenced delta.
///
/// `epoch` + `writer_id` come from the caller's lease; followers use
/// them to fence stale writers. `prev_envelope_checksum` is the chain
/// link — the BLAKE3 of the prior object's envelope (the published
/// base for the first delta after a base, or the prior delta
/// otherwise). `end_page_count` is computed by walrust from the WAL
/// commit, not supplied here.
#[derive(Debug, Clone)]
pub struct FencedDeltaSyncParams {
    pub epoch: u64,
    pub writer_id: String,
    pub prev_envelope_checksum: [u8; 32],
}

/// Result of publishing one delta envelope.
#[derive(Debug, Clone)]
pub struct DeltaPublishResult {
    pub seq: u64,
    /// BLAKE3 of the envelope just written — the caller threads this
    /// back in as the next delta's `prev_envelope_checksum`.
    pub envelope_checksum: [u8; 32],
    /// Page count at the end of this delta (shrink/grow aware).
    pub end_page_count: u64,
    pub frame_count: u64,
}

/// A discovered fenced delta envelope (decoded, NOT chain-verified).
///
/// The integration layer does the candidate filter + equivocation detection
/// + chain verification on these; walrust only provides raw, ordered,
/// decoded access.
#[derive(Debug, Clone)]
pub struct DiscoveredDelta {
    pub key: String,
    pub seq: u64,
    pub payload: DeltaPayloadV1,
    /// BLAKE3 of this envelope's bytes — what the *next* delta's
    /// `prev_checksum` must equal.
    pub envelope_checksum: [u8; 32],
}

/// Publish a fenced delta envelope.
///
/// Enforces two writer-side invariants:
/// - **Per-prefix monotonic seq**: the seq is in the object key, so a
///   second writer at the same seq collides on the same key.
/// - **No same-seq equivocation**: if `put_if_absent` fails, the
///   existing object's bytes must match exactly. Identical bytes →
///   idempotent re-publish (retry safety). Different bytes → bail; the
///   caller published two different deltas at one seq, which would be
///   fatal equivocation on the follower side.
pub async fn publish_delta_envelope(
    storage: &dyn StorageBackend,
    prefix: &str,
    db_name: &str,
    payload: &DeltaPayloadV1,
) -> Result<[u8; 32]> {
    let envelope =
        external_delta::encode(payload).map_err(|e| anyhow!("encode delta envelope: {e}"))?;
    let envelope_checksum = external_delta::checksum(&envelope);
    let key = delta_envelope_key(prefix, db_name, payload.seq);

    let cas = storage.put_if_absent(&key, &envelope).await?;
    if !cas.success {
        let existing = get_existing_after_failed_cas(storage, &key)
            .await?
            .ok_or_else(|| {
                anyhow!(
                    "{db_name}: delta envelope seq {} vanished after CAS failure at {key}",
                    payload.seq
                )
            })?;
        if existing != envelope {
            return Err(WalrustError::equivocation(format!(
                "{db_name}: delta envelope seq {} already exists with different bytes at {key}; \
                 refusing overwrite (writer equivocation at the same seq)",
                payload.seq
            ))
            .into());
        }
        tracing::info!(
            "{db_name}: delta envelope seq {} already present with identical bytes; idempotent re-publish",
            payload.seq
        );
    }
    Ok(envelope_checksum)
}

/// List fenced delta envelopes with `seq > after_seq`, ascending.
///
/// Decodes each envelope so callers can read the full tuple for
/// filtering + chain verification. Skips non-`.tlmd` objects (e.g.
/// legacy `.hadbp` leftovers). Does **not** filter by epoch/writer or
/// verify the chain — that is the integration layer's responsibility.
pub async fn list_delta_envelopes_after(
    storage: &dyn StorageBackend,
    prefix: &str,
    db_name: &str,
    after_seq: u64,
) -> Result<Vec<DiscoveredDelta>> {
    let dir_prefix = format!("{}{}/{:04x}/", prefix, db_name, GENERATION_LIVE);

    // Pass an `after` marker so the backend skips already-applied
    // objects instead of returning the whole prefix every poll. Keys
    // are `{dir}{seq:016x}.tlmd`; seq is zero-padded hex so lexical
    // order == numeric order. The marker is the largest key at
    // `after_seq` (its `.tlmd`), so `list` returns strictly-greater
    // keys — i.e. seq > after_seq. `None` when after_seq is 0 (start
    // from the beginning). The `seq <= after_seq` filter below is kept
    // as a correctness backstop in case a backend's `after` is
    // inclusive or ignores the marker.
    let after_marker =
        (after_seq > 0).then(|| format!("{}{:016x}.{}", dir_prefix, after_seq, DELTA_ENVELOPE_EXT));
    let keys = storage.list(&dir_prefix, after_marker.as_deref()).await?;

    let mut out = Vec::new();
    for key in keys {
        let Some(seq) = parse_delta_envelope_seq(&key) else {
            continue; // not a .tlmd object
        };
        if seq <= after_seq {
            continue;
        }
        let Some(bytes) = storage.get(&key).await? else {
            // Listed-then-vanished; skip. A real gap surfaces in chain verify.
            continue;
        };
        let payload =
            external_delta::decode(&bytes).map_err(|e| anyhow!("decode delta at {key}: {e}"))?;
        // B14: bind the decoded payload seq to the key-derived seq. The key
        // carries the ordering authority (zero-padded hex sorts numerically),
        // so a mislabeled envelope whose inner `payload.seq` disagrees with its
        // key would be applied out of order — or silently substituted for a
        // different seq — by a follower that trusts either field. Fail closed.
        if payload.seq != seq {
            return Err(WalrustError::integrity(format!(
                "delta envelope seq mismatch at {key}: key seq {seq} but payload seq {}",
                payload.seq
            ))
            .into());
        }
        let envelope_checksum = external_delta::checksum(&bytes);
        out.push(DiscoveredDelta {
            key,
            seq,
            payload,
            envelope_checksum,
        });
    }
    out.sort_by_key(|d| d.seq);
    Ok(out)
}

/// Fetch and decode a single fenced delta envelope by seq.
pub async fn fetch_delta_envelope(
    storage: &dyn StorageBackend,
    prefix: &str,
    db_name: &str,
    seq: u64,
) -> Result<Option<DiscoveredDelta>> {
    let key = delta_envelope_key(prefix, db_name, seq);
    let Some(bytes) = storage.get(&key).await? else {
        return Ok(None);
    };
    let payload =
        external_delta::decode(&bytes).map_err(|e| anyhow!("decode delta at {key}: {e}"))?;
    let envelope_checksum = external_delta::checksum(&bytes);
    Ok(Some(DiscoveredDelta {
        key,
        seq,
        payload,
        envelope_checksum,
    }))
}

/// The externally-owned base state a fenced-delta follower anchors on.
///
/// A follower already holds the base database image and knows, from the
/// external base manifest, the epoch and writer identity that own the fenced
/// chain, the seq the base was published at, and the BLAKE3 checksum of the
/// base object's envelope (the anchor the first delta's `prev_checksum` must
/// equal).
#[derive(Debug, Clone)]
pub struct FencedFollowerCursor {
    /// Seq of the published external base. Fenced deltas after it are
    /// contiguous: `base_seq + 1`, `base_seq + 2`, ...
    pub base_seq: u64,
    /// Lease epoch that owns the chain. Deltas whose `epoch` differs are
    /// rejected (stale-writer fence), even if the chain link verifies.
    pub epoch: u64,
    /// Lease-holder identity that owns the chain. Deltas from a different
    /// `writer_id` are rejected (writer fence).
    pub writer_id: String,
    /// BLAKE3 of the base object's envelope bytes — the chain anchor. The
    /// first delta's `prev_checksum` must equal this.
    pub base_envelope_checksum: [u8; 32],
}

/// Result of a fenced follower reconstruction.
#[derive(Debug, Clone)]
pub struct FencedFollowerResult {
    /// Number of fenced deltas applied on top of the base.
    pub applied: u64,
    /// Seq of the last applied delta (`base_seq` if none were applied).
    pub head_seq: u64,
    /// BLAKE3 of the last applied delta's envelope — the anchor a resuming
    /// follower threads back in as its next `base_envelope_checksum`.
    pub head_envelope_checksum: [u8; 32],
    /// HADBP running checksum of the reconstructed database.
    pub db_checksum: u64,
}

/// Reconstruct a database on a follower from the published fenced-delta
/// envelope sequence, enforcing every fence before applying anything.
///
/// This is the production follower/restore path for external-base fenced
/// (TLM_DELTA) mode — the counterpart of the writer's
/// [`sync_wal_fenced_delta`]. It copies the caller's base image to
/// `output_path`, discovers the published envelopes after `cursor.base_seq`
/// with [`list_delta_envelopes_after`], and for each one enforces, in order:
///
/// 1. **Seq contiguity** — the delta must be exactly the next seq after the
///    running head; a gap is a hard error (never a silent short reconstruct).
/// 2. **Epoch fence** — `payload.epoch == cursor.epoch`, else reject. This is
///    how stale-writer leftover is fenced even if a chain link happens to
///    verify.
/// 3. **Writer fence** — `payload.writer_id == cursor.writer_id`, else reject.
/// 4. **Envelope chain** — `payload.prev_checksum` must equal the running
///    envelope anchor (BLAKE3 of the prior object). A break is a hard error.
/// 5. **Byte-identity chain** — the recomputed BLAKE3 of the re-encoded
///    payload must equal the checksum discovery reported for the stored bytes.
///
/// Only then is the LTX payload applied with the running DB checksum threaded
/// through [`ltx::apply_changeset_to_db`], whose own chain verify catches any
/// base/page divergence. Every fence rejection is a typed
/// [`WalrustError::Integrity`] whose message names the fence, and the function
/// returns `Err` **before** any apply on the first offending envelope — a
/// forged delta never reaches the database.
///
/// `output_path` is treated as staging owned by the caller; on success it
/// holds the reconstructed database. On error its contents are unspecified
/// (partially applied) — callers restoring a live database should stage to a
/// temp path and publish atomically only on `Ok`.
pub async fn reconstruct_fenced_follower(
    storage: &dyn StorageBackend,
    prefix: &str,
    db_name: &str,
    cursor: &FencedFollowerCursor,
    base_db_path: &Path,
    output_path: &Path,
) -> Result<FencedFollowerResult> {
    tokio::fs::copy(base_db_path, output_path)
        .await
        .map_err(|e| {
            anyhow!(
                "copy fenced base {} -> {}: {e}",
                base_db_path.display(),
                output_path.display()
            )
        })?;

    let mut running_db = ltx::compute_checksum_from_file(output_path)?;
    let mut running_env = cursor.base_envelope_checksum;

    let deltas = list_delta_envelopes_after(storage, prefix, db_name, cursor.base_seq).await?;
    let mut applied: u64 = 0;
    for (i, d) in deltas.iter().enumerate() {
        let expected_seq = cursor.base_seq + (i as u64) + 1;
        if d.seq != expected_seq {
            return Err(WalrustError::integrity(format!(
                "{db_name}: fenced envelope seq gap: expected {expected_seq}, got {} at position {i}",
                d.seq
            ))
            .into());
        }
        if d.payload.epoch != cursor.epoch {
            return Err(WalrustError::integrity(format!(
                "{db_name}: epoch fence rejected envelope seq {} (epoch {} != cursor epoch {})",
                d.seq, d.payload.epoch, cursor.epoch
            ))
            .into());
        }
        if d.payload.writer_id != cursor.writer_id {
            return Err(WalrustError::integrity(format!(
                "{db_name}: writer fence rejected envelope seq {} (writer {:?} != {:?})",
                d.seq, d.payload.writer_id, cursor.writer_id
            ))
            .into());
        }
        if d.payload.prev_checksum.as_slice() != running_env.as_slice() {
            return Err(WalrustError::integrity(format!(
                "{db_name}: envelope chain break at seq {}",
                d.seq
            ))
            .into());
        }
        // Byte-identity chain: the checksum discovery reported for the STORED
        // bytes must equal a fresh BLAKE3 of the re-encoded payload. This
        // pins the discovered checksum to the payload we are about to apply.
        let recomputed = external_delta::checksum(
            &external_delta::encode(&d.payload)
                .map_err(|e| anyhow!("re-encode fenced envelope seq {}: {e}", d.seq))?,
        );
        if recomputed != d.envelope_checksum {
            return Err(WalrustError::integrity(format!(
                "{db_name}: envelope checksum mismatch at seq {} (recomputed != discovered)",
                d.seq
            ))
            .into());
        }

        let step = ltx::apply_changeset_to_db(&d.payload.ltx_payload, output_path, running_db)?;
        running_db = step.checksum;
        running_env = d.envelope_checksum;
        applied += 1;
    }

    Ok(FencedFollowerResult {
        applied,
        head_seq: cursor.base_seq + applied,
        head_envelope_checksum: running_env,
        db_checksum: running_db,
    })
}

/// Read pending WAL frames, encode them as an LTX changeset, wrap in a
/// fenced TLM_DELTA envelope, and publish.
///
/// This is the fenced-envelope analogue of [`sync_wal_after_external_base`].
/// Differences:
/// - The delta object is a TLMD envelope, not a bare `.hadbp`.
/// - Each delta carries `(epoch, writer_id, prev_checksum,
///   end_page_count)`. `prev_checksum` is the BLAKE3 of the prior
///   envelope (supplied by the caller via `params`); the others are
///   stamped here.
/// - `end_page_count` is the WAL commit's database size in pages
///   (shrink/grow aware), read straight from the commit frame header.
/// - No `state.json` is written — the replay cursor lives in the
///   externally owned base manifest.
///
/// Returns `None` when there are no new frames to publish.
pub async fn sync_wal_fenced_delta(
    storage: &dyn StorageBackend,
    prefix: &str,
    state: &mut SyncState,
    params: &FencedDeltaSyncParams,
) -> Result<Option<DeltaPublishResult>> {
    let header = match wal::read_header(&state.wal_path).await? {
        Some(h) => h,
        None => {
            ensure_database_in_wal_mode(&state.db_path, &state.name).await?;
            return Ok(None);
        }
    };

    let previous_wal_offset = state.wal_offset;
    let previous_wal_generation = state.wal_generation;
    let previous_wal_salt = state.wal_salt;
    let previous_wal_checksum_chain = state.wal_checksum_chain;

    let WalBatch {
        page_map,
        frame_count,
        new_offset,
        final_db_size,
        commit_count,
        rollover_detected,
    } = read_next_wal_batch(state, &header).await?;

    if rollover_detected {
        state.wal_offset = previous_wal_offset;
        state.wal_generation = previous_wal_generation;
        state.wal_salt = previous_wal_salt;
        state.wal_checksum_chain = previous_wal_checksum_chain;
        let message = format!(
            "{}: WAL rollover detected in fenced external mode; refusing to publish deltas until the external base is re-anchored",
            state.name
        );
        state.rollover_observer.emit(RolloverEvent {
            db_name: state.name.clone(),
            mode: "fenced-external",
            recovered: false,
            message: message.clone(),
        });
        anyhow::bail!(message);
    }

    if page_map.is_empty() {
        return Ok(None);
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

    // `final_db_size` is the database size in pages after the last
    // commit frame in this batch — the authoritative end_page_count.
    // Guard against a zero: page_map is non-empty here (we returned
    // early otherwise), so a committed frame exists and its db-size
    // field must be positive. A zero means a malformed/incomplete WAL
    // commit; refuse to publish a delta that would truncate the
    // follower's database to zero pages.
    if final_db_size == 0 {
        anyhow::bail!(
            "{}: WAL commit produced end_page_count=0 with {} dirty pages; refusing to publish a truncating delta",
            state.name,
            pages.len()
        );
    }
    let end_page_count = final_db_size as u64;
    let (ltx_bytes, post_checksum) = ltx::encode_wal_changes_with_end_page_count(
        &pages,
        header.page_size,
        new_seq,
        pre_checksum,
        end_page_count,
    )?;

    let payload = DeltaPayloadV1 {
        seq: new_seq,
        epoch: params.epoch,
        writer_id: params.writer_id.clone(),
        prev_checksum: params.prev_envelope_checksum.to_vec(),
        end_page_count,
        ltx_payload: ltx_bytes,
    };

    let envelope_checksum = publish_delta_envelope(storage, prefix, &state.name, &payload).await?;

    tracing::info!(
        "{}: published fenced delta seq {} ({} frames, epoch {}, writer {}, end_pages {}) -> {}",
        state.name,
        new_seq,
        frame_count,
        params.epoch,
        params.writer_id,
        end_page_count,
        delta_envelope_key(prefix, &state.name, new_seq)
    );

    state.wal_offset = new_offset;
    state.current_seq = new_seq;
    state.current_txid = max_txid;
    state.db_checksum = Some(post_checksum);
    // Intentionally no save_state: the replay cursor lives in the externally
    // owned base manifest, not a remote state.json sidecar.

    Ok(Some(DeltaPublishResult {
        seq: new_seq,
        envelope_checksum,
        end_page_count,
        frame_count: frame_count as u64,
    }))
}

/// Take a full database snapshot as HADBP changeset.
pub async fn take_snapshot(
    storage: &dyn StorageBackend,
    prefix: &str,
    state: &mut SyncState,
) -> Result<()> {
    let timestamp = Utc::now();
    // Walrust-owned mode holds a read transaction to pin the WAL. Release it
    // around our own checkpoint, then re-pin the fresh WAL immediately (D2).
    release_checkpoint_blocker(state).await?;
    checkpoint_wal(&state.db_path).await?;
    reacquire_checkpoint_blocker(state).await?;
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
    let snapshot = ltx::encode_sqlite_snapshot(&state.db_path, page_size, new_seq, prev_checksum)?;
    let db_checksum = snapshot.checksum;
    let changeset_bytes = snapshot.bytes;

    let changeset_size = changeset_bytes.len() as u64;
    let changeset_key = build_state_changeset_key(prefix, state, GENERATION_SNAPSHOT, new_seq);

    put_changeset_if_absent(
        storage,
        &changeset_key,
        &changeset_bytes,
        &state.name,
        new_seq,
        "walrust-owned snapshot",
    )
    .await?;

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
    reset_wal_cursor_after_snapshot(state).await;

    Ok(())
}

/// Restore a database from storage using HADBP changesets.
///
/// Discovers available changesets by listing S3 objects (no manifest needed).
/// Returns the seq that was actually restored to.
pub async fn restore(
    storage: Arc<dyn StorageBackend>,
    prefix: &str,
    db_name: &str,
    output: &Path,
    point_in_time: Option<&str>,
) -> Result<u64> {
    let target_seq = parse_point_in_time_seq(point_in_time)?;
    let storage_ref = storage.as_ref();

    let lineage_id = active_lineage_id(storage_ref, prefix, db_name).await?;
    let snapshot = match target_seq {
        Some(target) => discover_latest_snapshot_at_or_before_in_namespace(
            storage_ref,
            prefix,
            db_name,
            lineage_id.as_deref(),
            target,
            ChangesetKind::Physical,
        )
        .await?
        .ok_or_else(|| {
            WalrustError::restore_not_found(format!(
                "snapshot unavailable for database '{}' at or before seq {}",
                db_name, target
            ))
        })?,
        None => discover_latest_snapshot_in_namespace(
            storage_ref,
            prefix,
            db_name,
            lineage_id.as_deref(),
            ChangesetKind::Physical,
        )
        .await?
        .ok_or_else(|| {
            WalrustError::restore_not_found(format!(
                "snapshot unavailable for database '{}'",
                db_name
            ))
        })?,
    };

    let staged_restore = AtomicRestore::new(output);
    let staged_output = staged_restore.path();

    // Apply snapshot
    let snapshot_data = storage_ref
        .get(&snapshot.key)
        .await?
        .ok_or_else(|| anyhow!("snapshot key {} not found", snapshot.key))?;
    let decode_result = ltx::decode_to_db(&snapshot_data, staged_output)?;
    tracing::info!(
        "Restored snapshot to {} (checksum: {:016x})",
        staged_output.display(),
        decode_result.checksum
    );

    // Level-aware restore. Compaction only ever runs on the non-lineage owned
    // path (see `maybe_compact_owned`), so leveled buckets always have
    // `lineage_id == None`. If any merged level exists, the greedy planner picks
    // coarse merged ranges over the fine L0 points they supersede and the
    // executor applies the plan (bounded prefetch, strict-order apply, chain
    // linkage through `chain_end`). An un-leveled bucket takes the original
    // linear path below, byte-identically.
    let leveled = if lineage_id.is_none() {
        let layout = crate::compaction::SeqLayout::new(storage.clone(), prefix, db_name);
        !crate::compaction::list_merged_ranges(&layout)
            .await
            .unwrap_or_default()
            .is_empty()
    } else {
        false
    };

    let restored_seq = if leveled {
        let layout = crate::compaction::SeqLayout::new(storage.clone(), prefix, db_name);
        let candidates =
            crate::compaction::gather_candidates(&layout, snapshot.seq, u64::MAX).await?;
        let max_available = candidates
            .iter()
            .map(|c| c.range.max)
            .max()
            .unwrap_or(snapshot.seq)
            .max(snapshot.seq);
        let target = target_seq.unwrap_or(max_available);
        if target > max_available {
            return Err(WalrustError::restore_not_found(format!(
                "requested point-in-time seq {target} is beyond the newest available seq \
                 {max_available} for database '{db_name}'"
            ))
            .into());
        }
        let plan = match crate::compaction::plan_restore(&candidates, snapshot.seq, target) {
            Ok(plan) => plan,
            Err(e) => {
                // Same decay refinement as the legacy path: a later full
                // snapshot absorbing the target is granularity decay, not a
                // missing object.
                let later: Vec<u64> = discover_latest_snapshot_in_namespace(
                    storage_ref,
                    prefix,
                    db_name,
                    lineage_id.as_deref(),
                    ChangesetKind::Physical,
                )
                .await
                .ok()
                .flatten()
                .map(|s| s.seq)
                .into_iter()
                .filter(|m| *m > snapshot.seq)
                .collect();
                let refined = crate::compaction::refine_gap_with_snapshot_spans(e, &later, target);
                // Same typing rule as the legacy path: decay -> RestoreNotFound,
                // genuine gap -> restore error.
                let err = if matches!(
                    refined,
                    crate::compaction::PlanError::PitrInsideSnapshotSpan { .. }
                ) {
                    WalrustError::restore_not_found(refined.to_string())
                } else {
                    WalrustError::restore(refined.to_string())
                };
                return Err(anyhow::Error::from(err));
            }
        };
        tracing::info!(
            "Restoring from snapshot (seq {}) + leveled plan of {} objects to seq {}",
            snapshot.seq,
            plan.files.len(),
            target
        );
        crate::compaction::apply_plan(
            &layout,
            &plan,
            staged_output,
            decode_result.checksum,
            crate::compaction::DEFAULT_PREFETCH_DEPTH,
        )
        .await?
    } else {
        // Find incrementals after the snapshot (un-leveled bucket: the original
        // linear path, unchanged).
        let mut incrementals = discover_after_in_namespace(
            storage_ref,
            prefix,
            db_name,
            lineage_id.as_deref(),
            GENERATION_LIVE,
            snapshot.seq,
            ChangesetKind::Physical,
        )
        .await?;
        if let Some(target) = target_seq {
            incrementals.retain(|changeset| changeset.seq <= target);
        }

        tracing::info!(
            "Restoring from snapshot (seq {}) + {} incrementals",
            snapshot.seq,
            incrementals.len()
        );

        let mut restored_seq = snapshot.seq;
        let mut current_checksum = decode_result.checksum;

        // Apply incrementals in order. A gap or checksum-chain break means the
        // restore cannot prove success, so it is a hard error.
        for inc in &incrementals {
            let expected_seq = restored_seq + 1;
            if inc.seq != expected_seq {
                return Err(anyhow!(
                    "restore incremental gap: expected seq {expected_seq}, got seq {} at {}",
                    inc.seq,
                    inc.key
                ));
            }

            let data = storage_ref
                .get(&inc.key)
                .await?
                .ok_or_else(|| anyhow!("incremental key {} not found", inc.key))?;
            let result = ltx::apply_changeset_to_db(&data, staged_output, current_checksum)?;
            tracing::info!(
                "Applied incremental (seq {}, checksum: {:016x})",
                inc.seq,
                result.checksum
            );
            restored_seq = inc.seq;
            current_checksum = result.checksum;
        }
        restored_seq
    };

    verify_sqlite_integrity(staged_output)?;
    staged_restore.publish(output)?;
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
    let staged_restore = AtomicRestore::new(output);
    let staged_output = staged_restore.path();

    // Step 1: materialize the base DB from the external snapshot source
    let checkpoint = snapshot_source.materialize(staged_output).await?;
    tracing::info!(
        "Materialized base DB from snapshot source (checkpoint version {}, checksum {:016x})",
        checkpoint.seq,
        checkpoint.checksum,
    );

    // Step 2: discover incremental changesets newer than the checkpoint version.
    let incrementals = cs_storage::discover_after(
        storage,
        prefix,
        db_name,
        checkpoint.seq,
        ChangesetKind::Physical,
    )
    .await?;

    if incrementals.is_empty() {
        tracing::info!(
            "No incremental changesets to apply (up to date at version {})",
            checkpoint.seq
        );
        verify_sqlite_integrity(staged_output)?;
        staged_restore.publish(output)?;
        return Ok(checkpoint.seq);
    }

    tracing::info!(
        "Applying {} incremental changesets after checkpoint version {}",
        incrementals.len(),
        checkpoint.seq,
    );

    // Step 3: apply incrementals in order, verifying every file against the
    // materialized base checksum. A gap or chain break means the restore cannot
    // prove success, so it is a hard error.
    let mut cursor = PullCursor {
        seq: checkpoint.seq,
        checksum: checkpoint.checksum,
    };
    for inc in incrementals.iter() {
        let expected_seq = cursor.seq + 1;
        if inc.seq != expected_seq {
            return Err(anyhow!(
                "restore incremental gap: expected seq {expected_seq}, got seq {} at {}",
                inc.seq,
                inc.key
            ));
        }

        let data = storage
            .get(&inc.key)
            .await?
            .ok_or_else(|| anyhow!("incremental key {} not found", inc.key))?;
        let changeset = ltx::decode_sqlite_changeset(&data)
            .map_err(|e| anyhow!("Failed to decode changeset at {}: {}", inc.key, e))?;

        hadb_changeset::physical::verify_chain(cursor.checksum, &changeset)
            .map_err(|e| anyhow!("Checksum chain broken at seq {}: {}", inc.seq, e))?;

        ltx::apply_decoded_changeset_to_db(&changeset, staged_output)?;

        tracing::info!(
            "Applied incremental (seq {}, checksum: {:016x})",
            inc.seq,
            changeset.checksum
        );
        cursor = PullCursor {
            seq: inc.seq,
            checksum: changeset.checksum,
        };
    }

    verify_sqlite_integrity(staged_output)?;
    staged_restore.publish(output)?;
    Ok(cursor.seq)
}

fn verify_sqlite_integrity(path: &Path) -> Result<()> {
    let conn = rusqlite::Connection::open(path)
        .map_err(|e| anyhow!("failed to open restored database for integrity_check: {e}"))?;
    let result: String = conn
        .query_row("PRAGMA integrity_check", [], |row| row.get(0))
        .map_err(|e| anyhow!("failed to run integrity_check on restored database: {e}"))?;
    if result != "ok" {
        return Err(anyhow!(
            "restored database failed integrity_check: {result}"
        ));
    }
    Ok(())
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
    release_checkpoint_blocker(state).await?;
    checkpoint_wal(&state.db_path).await?;
    reacquire_checkpoint_blocker(state).await?;
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
    let snapshot = ltx::encode_sqlite_snapshot(&state.db_path, page_size, new_seq, prev_checksum)?;
    let db_checksum = snapshot.checksum;
    let changeset_bytes = snapshot.bytes;

    let changeset_size = changeset_bytes.len() as u64;
    let changeset_key = build_state_changeset_key(prefix, state, GENERATION_SNAPSHOT, new_seq);

    // Share buffer across retry attempts via Arc to avoid per-attempt clones
    let upload_buffer = std::sync::Arc::new(changeset_bytes);
    let upload_key = changeset_key.clone();
    let upload_name = state.name.clone();
    retry_policy
        .execute_with_context("upload snapshot", || {
            let data_arc = std::sync::Arc::clone(&upload_buffer);
            let key = upload_key.clone();
            let name = upload_name.clone();
            async move {
                put_changeset_if_absent(
                    storage,
                    &key,
                    data_arc.as_slice(),
                    &name,
                    new_seq,
                    "walrust-owned snapshot",
                )
                .await
            }
        })
        .await?;

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
    reset_wal_cursor_after_snapshot(state).await;

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
        None => {
            ensure_database_in_wal_mode(&state.db_path, &state.name).await?;
            return Ok(0);
        }
    };

    let WalBatch {
        page_map,
        frame_count,
        new_offset,
        final_db_size,
        commit_count,
        rollover_detected,
    } = read_next_wal_batch(state, &header).await?;

    if rollover_detected {
        // An external checkpoint reset a walrust-owned WAL — unexpected for a DB we
        // own (autocheckpoint should be 0). Log loudly (the core library has no
        // webhook channel; the binary surfaces it on its own paths) and re-anchor.
        tracing::error!(
            "{}: WAL rollover detected; publishing a new snapshot instead of an incremental across the gap",
            state.name
        );
        take_snapshot_with_retry(storage, prefix, state, retry_policy).await?;
        save_state(storage, prefix, state).await?;
        return Ok(1);
    }

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

    if final_db_size == 0 {
        anyhow::bail!(
            "{}: WAL commit produced end_page_count=0 with {} dirty pages; refusing to publish a truncating changeset",
            state.name,
            pages.len()
        );
    }
    let (changeset_bytes, post_checksum) = ltx::encode_wal_changes_with_end_page_count(
        &pages,
        header.page_size,
        new_seq,
        pre_checksum,
        final_db_size as u64,
    )?;

    let changeset_size = changeset_bytes.len() as u64;
    let changeset_key = build_state_changeset_key(prefix, state, GENERATION_LIVE, new_seq);

    let upload_buffer = std::sync::Arc::new(changeset_bytes);
    let upload_key = changeset_key.clone();
    let upload_name = state.name.clone();
    retry_policy
        .execute_with_context("upload WAL changes", || {
            let data_arc = std::sync::Arc::clone(&upload_buffer);
            let key = upload_key.clone();
            let name = upload_name.clone();
            async move {
                put_changeset_if_absent(
                    storage,
                    &key,
                    data_arc.as_slice(),
                    &name,
                    new_seq,
                    "walrust-owned",
                )
                .await
            }
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

/// Maximum concurrent S3 downloads for incremental pulling.
const PULL_CONCURRENCY: usize = 8;

struct DecodedPullChangeset {
    seq: u64,
    key: String,
    changeset: hadb_changeset::physical::PhysicalChangeset,
}

async fn download_decode_pull_changesets(
    storage: &dyn StorageBackend,
    files: &[DiscoveredChangeset],
) -> Result<Vec<DecodedPullChangeset>> {
    let downloaded = download_parallel(storage, files, PULL_CONCURRENCY).await;
    let mut decoded = Vec::with_capacity(files.len());

    for (file, data) in files.iter().zip(downloaded.into_iter()) {
        let data = data?;
        let changeset = ltx::decode_sqlite_changeset(&data)
            .map_err(|e| anyhow!("Failed to decode changeset at {}: {}", file.key, e))?;
        decoded.push(DecodedPullChangeset {
            seq: file.seq,
            key: file.key.clone(),
            changeset,
        });
    }

    Ok(decoded)
}

fn verify_decoded_pull_chain(
    decoded: &[DecodedPullChangeset],
    current: PullCursor,
    context: &str,
) -> Result<PullCursor> {
    let mut cursor = current;
    for entry in decoded {
        let expected_seq = cursor.seq + 1;
        if entry.seq != expected_seq {
            return Err(anyhow!(
                "{context} incremental gap: expected seq {expected_seq}, got seq {} at {}",
                entry.seq,
                entry.key
            ));
        }

        hadb_changeset::physical::verify_chain(cursor.checksum, &entry.changeset).map_err(|e| {
            anyhow!(
                "{context} checksum chain broken at seq {} ({}): {}",
                entry.seq,
                entry.key,
                e
            )
        })?;
        cursor = PullCursor {
            seq: entry.seq,
            checksum: entry.changeset.checksum,
        };
    }
    Ok(cursor)
}

/// Pull and apply new HADBP changesets from S3 that are ahead of `current_seq`.
///
/// This is the follower's replication primitive. Call it in a loop (e.g., every 1s)
/// to stay in sync with the leader. Returns the new highest applied cursor.
///
/// Optimizations:
/// - Uses `start_after` on S3 LIST to skip past already-applied changesets
/// - Downloads up to 8 files concurrently, applies sequentially (checksum chain is serial)
pub async fn pull_incremental(
    storage: &dyn StorageBackend,
    prefix: &str,
    db_name: &str,
    db_path: &Path,
    current: PullCursor,
) -> Result<PullCursor> {
    let lineage_id = active_lineage_id(storage, prefix, db_name).await?;
    let new_files = discover_after_in_namespace(
        storage,
        prefix,
        db_name,
        lineage_id.as_deref(),
        GENERATION_LIVE,
        current.seq,
        ChangesetKind::Physical,
    )
    .await?;

    if new_files.is_empty() {
        return Ok(current);
    }

    let decoded = download_decode_pull_changesets(storage, &new_files).await?;
    let verified_cursor = verify_decoded_pull_chain(&decoded, current, "pull_incremental")?;

    for entry in &decoded {
        ltx::apply_decoded_changeset_to_db(&entry.changeset, db_path)?;
    }

    if !decoded.is_empty() {
        tracing::info!(
            "Pulled {} HADBP changesets, seq {} -> {}",
            decoded.len(),
            current.seq,
            verified_cursor.seq
        );
    }

    Ok(verified_cursor)
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
    current: PullCursor,
) -> Result<PullCursor> {
    // begin() may fail; if it does, abort() is still called as a
    // best-effort cleanup so the contract "exactly one of finalize or
    // abort per invocation" holds even on early failure. Sinks must
    // tolerate abort() being called when begin() itself errored.
    if let Err(begin_err) = sink.begin() {
        try_abort(sink, &begin_err);
        return Err(begin_err);
    }

    let result = pull_incremental_into_sink_inner(storage, prefix, db_name, sink, current).await;

    match result {
        Ok(applied_cursor) => {
            // finalize() may fail mid-install (the Turbolite sink
            // writes pages, marks bitmap, bumps generation, etc. — any
            // step can fail). On failure we still need to give the
            // sink a chance to clean up staged state.
            if let Err(finalize_err) = sink.finalize() {
                try_abort(sink, &finalize_err);
                return Err(finalize_err);
            }
            Ok(applied_cursor)
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
    current: PullCursor,
) -> Result<PullCursor> {
    let lineage_id = active_lineage_id(storage, prefix, db_name).await?;
    let new_files = discover_after_in_namespace(
        storage,
        prefix,
        db_name,
        lineage_id.as_deref(),
        GENERATION_LIVE,
        current.seq,
        ChangesetKind::Physical,
    )
    .await?;

    if new_files.is_empty() {
        return Ok(current);
    }

    let decoded = download_decode_pull_changesets(storage, &new_files).await?;
    let verified_cursor =
        verify_decoded_pull_chain(&decoded, current, "pull_incremental_into_sink")?;

    for entry in &decoded {
        for page in &entry.changeset.pages {
            if page.data.is_empty() {
                continue;
            }
            // SQLite 1-based page id straight from the HADBP changeset.
            let sqlite_page_id: u32 = page
                .page_id
                .to_u64()
                .try_into()
                .map_err(|_| anyhow!("page_id {} exceeds u32", page.page_id.to_u64()))?;
            sink.apply_page(sqlite_page_id, &page.data)?;
        }

        sink.commit_changeset(entry.seq)?;
    }

    if !decoded.is_empty() {
        tracing::info!(
            "pull_incremental_into_sink: applied {} HADBP changesets, seq {} -> {}",
            decoded.len(),
            current.seq,
            verified_cursor.seq
        );
    }

    Ok(verified_cursor)
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
    /// Optional sink for loud rollover events (re-anchor / refusal). The core
    /// has no webhook client; an embedder wires this to its own alert channel.
    pub rollover_observer: RolloverObserver,
    /// Leveled-compaction control (experimental). Default: **off**
    /// (`enabled = false`) per version-skew safety — see the compaction module
    /// header. Set `compaction.enabled = true` (and tune `keep_fine_window`,
    /// `l1_batch`, `l2_batch`) to fold long incremental histories into a few
    /// merged levels for fast restore. This is the **single** control; there is
    /// no separate internal gate.
    pub compaction: crate::compaction::CompactionSettings,
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
            rollover_observer: RolloverObserver::default(),
            compaction: crate::compaction::CompactionSettings::default(),
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
    state.rollover_observer = config.rollover_observer.clone();

    if db_path.exists() {
        state.init_checksum()?;
    }

    state.ensure_lineage_id();
    take_snapshot_with_retry(storage, prefix, &mut state, &config.retry_policy).await?;
    save_state(storage, prefix, &state).await?;
    tracing::info!(
        "{}: Initial snapshot taken, starting replication loop",
        state.name
    );

    let mut sync_timer = tokio::time::interval(config.sync_interval);
    let mut snapshot_timer = tokio::time::interval(config.snapshot_interval);
    sync_timer.tick().await;
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
                            return Err(anyhow!("{}: Final sync failed: {}", state.name, e));
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
                        return Err(anyhow!("{}: WAL sync failed: {}", state.name, e));
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
    if state.rollover_observer.0.is_none() {
        state.rollover_observer = config.rollover_observer.clone();
    }

    if state.db_checksum.is_none() && state.db_path.exists() {
        state.init_checksum()?;
    }

    tracing::info!(
        "{}: Starting WAL-only replication (no initial snapshot, seq={})",
        state.name,
        initial_seq,
    );

    let mut sync_timer = tokio::time::interval(config.sync_interval);
    sync_timer.tick().await;

    loop {
        tokio::select! {
            _ = cancel.changed() => {
                if *cancel.borrow() {
                    match sync_wal(storage, prefix, state).await {
                        Ok(frames) if frames > 0 => {
                            tracing::info!("{}: Final sync captured {} frames before shutdown", state.name, frames);
                        }
                        Err(e) => {
                            return Err(anyhow!("{}: Final sync failed: {}", state.name, e));
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
                        return Err(anyhow!("{}: WAL sync failed: {}", state.name, e));
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
        async fn materialize(
            &self,
            output: &Path,
        ) -> Result<crate::snapshot_source::SnapshotCheckpoint> {
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
            drop(conn);
            Ok(crate::snapshot_source::SnapshotCheckpoint {
                seq: self.version,
                checksum: ltx::compute_checksum_from_file(output)?,
            })
        }

        async fn checkpoint(&self) -> Result<crate::snapshot_source::SnapshotCheckpoint> {
            Err(anyhow!(
                "MockSnapshotSource cannot report a checksum without materializing"
            ))
        }
    }

    struct CopySnapshotSource {
        version: u64,
        path: PathBuf,
    }

    #[async_trait]
    impl crate::snapshot_source::SnapshotSource for CopySnapshotSource {
        async fn materialize(
            &self,
            output: &Path,
        ) -> Result<crate::snapshot_source::SnapshotCheckpoint> {
            std::fs::copy(&self.path, output)?;
            Ok(crate::snapshot_source::SnapshotCheckpoint {
                seq: self.version,
                checksum: ltx::compute_checksum_from_file(output)?,
            })
        }

        async fn checkpoint(&self) -> Result<crate::snapshot_source::SnapshotCheckpoint> {
            Ok(crate::snapshot_source::SnapshotCheckpoint {
                seq: self.version,
                checksum: ltx::compute_checksum_from_file(&self.path)?,
            })
        }
    }

    fn create_sqlite_source(path: &Path) -> Result<u32> {
        let conn = rusqlite::Connection::open(path)?;
        conn.execute_batch(
            "
            CREATE TABLE data (id INTEGER PRIMARY KEY, val TEXT NOT NULL);
            INSERT INTO data (id, val) VALUES (1, 'base-1');
            INSERT INTO data (id, val) VALUES (2, 'base-2');
            ",
        )?;
        let page_size = conn.query_row("PRAGMA page_size", [], |row| row.get(0))?;
        drop(conn);
        Ok(page_size)
    }

    fn create_marker_db(path: &Path, marker: &str) -> Result<()> {
        let conn = rusqlite::Connection::open(path)?;
        conn.execute("CREATE TABLE marker (value TEXT NOT NULL);", [])?;
        conn.execute(
            "INSERT INTO marker (value) VALUES (?1)",
            rusqlite::params![marker],
        )?;
        Ok(())
    }

    fn create_delete_journal_db(path: &Path) -> Result<()> {
        let conn = rusqlite::Connection::open(path)?;
        conn.execute_batch(
            "
            PRAGMA journal_mode=DELETE;
            CREATE TABLE items (id INTEGER PRIMARY KEY, value TEXT NOT NULL);
            INSERT INTO items (value) VALUES ('base');
            ",
        )?;
        let mode: String = conn.query_row("PRAGMA journal_mode", [], |row| row.get(0))?;
        if mode.to_lowercase() == "wal" {
            return Err(anyhow!("test database unexpectedly in WAL mode"));
        }
        Ok(())
    }

    #[tokio::test]
    async fn sync_wal_rejects_database_out_of_wal_mode() {
        let storage = TestStorage::new();
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("delete-mode.db");
        create_delete_journal_db(&db_path).unwrap();

        let mut state = SyncState::new(db_path).unwrap();
        state.current_seq = 1;
        state.current_txid = 1;
        state.db_checksum = Some(0);

        let err = sync_wal(&storage, "test/", &mut state)
            .await
            .expect_err("sync must fail closed when SQLite is not in WAL mode");
        let msg = err.to_string();
        assert!(msg.contains("journal_mode"), "{msg}");
        assert!(msg.contains("WAL"), "{msg}");
    }

    #[tokio::test]
    async fn restore_no_snapshot_returns_typed_restore_error() {
        let storage = TestStorage::new();
        let dir = tempfile::tempdir().unwrap();
        let output = dir.path().join("restored.db");

        let err = restore(Arc::new(storage), "test/", "missing", &output, None)
            .await
            .expect_err("missing snapshot must be a typed restore error");

        assert_eq!(
            crate::errors::classify_error(&err),
            crate::errors::ExitStatus::Restore
        );
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
        let dir = tempfile::tempdir().unwrap();
        let base = dir.path().join("base.db");
        create_marker_db(&base, "checkpoint").unwrap();
        let source = CopySnapshotSource {
            version: 42,
            path: base.clone(),
        };
        let checkpoint = source.checkpoint().await.unwrap();
        assert_eq!(checkpoint.seq, 42);
        assert_eq!(
            checkpoint.checksum,
            ltx::compute_checksum_from_file(&base).unwrap()
        );
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

    #[tokio::test]
    async fn restore_errors_on_noncontiguous_incremental_sequence() {
        let mut storage = TestStorage::new();
        let dir = tempfile::tempdir().unwrap();
        let source = dir.path().join("source.db");
        let output = dir.path().join("restored.db");
        let page_size = 4096u32;
        std::fs::write(&source, vec![0x11; page_size as usize * 2]).unwrap();

        let snapshot = ltx::encode_snapshot(&source, page_size, 1, 0).unwrap();
        let snapshot_key = build_changeset_key("test/", "mydb", GENERATION_SNAPSHOT, 1);
        storage.insert(&snapshot_key, snapshot);

        let snapshot_checksum = ltx::compute_checksum_from_file(&source).unwrap();
        let pages = vec![(1, vec![0x33; page_size as usize])];
        let (incremental, _) =
            ltx::encode_wal_changes(&pages, page_size, 3, snapshot_checksum).unwrap();
        let incremental_key = build_changeset_key("test/", "mydb", GENERATION_LIVE, 3);
        storage.insert(&incremental_key, incremental);

        let err = restore(Arc::new(storage), "test/", "mydb", &output, None)
            .await
            .expect_err("restore must reject a gap from seq 1 to seq 3");

        let msg = err.to_string();
        assert!(
            msg.contains("gap") || msg.contains("contiguous") || msg.contains("seq"),
            "expected gap/contiguity error, got: {msg}"
        );
    }

    #[tokio::test]
    async fn restore_point_in_time_uses_latest_snapshot_not_after_target() {
        let mut storage = TestStorage::new();
        let dir = tempfile::tempdir().unwrap();
        let old_db = dir.path().join("old.db");
        let new_db = dir.path().join("new.db");
        let output = dir.path().join("restored.db");
        let page_size = 4096u32;

        create_marker_db(&old_db, "old-snapshot").unwrap();
        create_marker_db(&new_db, "newer-snapshot").unwrap();

        let old_snapshot = ltx::encode_snapshot(&old_db, page_size, 1, 0).unwrap();
        let old_key = build_changeset_key("test/", "mydb", GENERATION_SNAPSHOT, 1);
        storage.insert(&old_key, old_snapshot);

        let new_snapshot = ltx::encode_snapshot(&new_db, page_size, 5, 0).unwrap();
        let new_key = build_changeset_key("test/", "mydb", GENERATION_SNAPSHOT, 5);
        storage.insert(&new_key, new_snapshot);

        let restored_seq = restore(Arc::new(storage), "test/", "mydb", &output, Some("3"))
            .await
            .expect("core restore should choose the latest snapshot <= target");

        assert_eq!(restored_seq, 1);
        let conn = rusqlite::Connection::open(&output).unwrap();
        let marker: String = conn
            .query_row("SELECT value FROM marker", [], |row| row.get(0))
            .unwrap();
        assert_eq!(marker, "old-snapshot");
    }

    #[tokio::test]
    async fn restore_failure_preserves_existing_output_database() {
        let mut storage = TestStorage::new();
        let dir = tempfile::tempdir().unwrap();
        let source = dir.path().join("source.db");
        let output = dir.path().join("restored.db");
        let page_size = create_sqlite_source(&source).unwrap();
        create_marker_db(&output, "must-survive").unwrap();
        let original_output = std::fs::read(&output).unwrap();

        let snapshot = ltx::encode_snapshot(&source, page_size, 1, 0).unwrap();
        let snapshot_key = build_changeset_key("test/", "mydb", GENERATION_SNAPSHOT, 1);
        storage.insert(&snapshot_key, snapshot);

        let snapshot_checksum = ltx::compute_checksum_from_file(&source).unwrap();
        let pages = vec![(1, vec![0x66; page_size as usize])];
        let (incremental, _) =
            ltx::encode_wal_changes(&pages, page_size, 3, snapshot_checksum).unwrap();
        let incremental_key = build_changeset_key("test/", "mydb", GENERATION_LIVE, 3);
        storage.insert(&incremental_key, incremental);

        restore(Arc::new(storage), "test/", "mydb", &output, None)
            .await
            .expect_err("restore must fail before publishing over the existing output");

        assert_eq!(
            std::fs::read(&output).unwrap(),
            original_output,
            "failed restore must leave the existing output database untouched"
        );
    }

    #[tokio::test]
    async fn restore_with_snapshot_source_failure_preserves_existing_output_database() {
        let mut storage = TestStorage::new();
        let dir = tempfile::tempdir().unwrap();
        let output = dir.path().join("restored.db");
        create_marker_db(&output, "must-survive").unwrap();
        let original_output = std::fs::read(&output).unwrap();

        let incremental_key = build_changeset_key("test/", "mydb", GENERATION_LIVE, 7);
        storage.insert(&incremental_key, vec![0xff]);
        let source = MockSnapshotSource {
            version: 5,
            row_count: 2,
            fail: None,
        };

        restore_with_snapshot_source(&storage, "test/", "mydb", &output, &source)
            .await
            .expect_err("restore must fail before publishing over the existing output");

        assert_eq!(
            std::fs::read(&output).unwrap(),
            original_output,
            "failed snapshot-source restore must leave the existing output database untouched"
        );
    }

    #[tokio::test]
    async fn restore_with_snapshot_source_rejects_first_incremental_with_wrong_anchor_checksum() {
        let mut storage = TestStorage::new();
        let dir = tempfile::tempdir().unwrap();
        let base = dir.path().join("base.db");
        let output = dir.path().join("restored.db");
        let existing = dir.path().join("existing.db");
        create_marker_db(&base, "base").unwrap();
        create_marker_db(&existing, "must-survive").unwrap();
        std::fs::copy(&existing, &output).unwrap();
        let original_output = std::fs::read(&output).unwrap();

        let source = CopySnapshotSource {
            version: 5,
            path: base.clone(),
        };
        let page_size = 4096u32;
        let base_data = std::fs::read(&base).unwrap();
        let base_page_1 = base_data[0..page_size as usize].to_vec();
        let actual_anchor = ltx::compute_checksum_from_file(&base).unwrap();
        let wrong_anchor = actual_anchor ^ 0xfeed_face_dead_beef;
        let (incremental, _) =
            ltx::encode_wal_changes(&[(1, base_page_1)], page_size, 6, wrong_anchor).unwrap();
        let incremental_key = build_changeset_key("test/", "mydb", GENERATION_LIVE, 6);
        storage.insert(&incremental_key, incremental);

        restore_with_snapshot_source(&storage, "test/", "mydb", &output, &source)
            .await
            .expect_err(
                "snapshot-source restore must reject a first incremental that does not chain \
                 from the materialized base checksum",
            );

        assert_eq!(
            std::fs::read(&output).unwrap(),
            original_output,
            "failed snapshot-source restore must leave existing output untouched"
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

    fn seed_chained_changeset(
        storage: &mut TestStorage,
        prefix: &str,
        db_name: &str,
        seq: u64,
        prev_checksum: u64,
        page_size: u32,
        pages: &[(u32, Vec<u8>)],
    ) -> u64 {
        let (bytes, post_checksum) =
            encode_wal_changes(pages, page_size, seq, prev_checksum).expect("encode_wal_changes");
        let key = build_changeset_key(prefix, db_name, GENERATION_LIVE, seq);
        storage.insert(&key, bytes);
        post_checksum
    }

    fn page_payload(page_size: usize, marker: u8) -> Vec<u8> {
        vec![marker; page_size]
    }

    #[tokio::test]
    async fn external_base_state_ignores_same_seq_changeset_checksum() {
        let mut storage = TestStorage::new();
        let page_size = 4096u32;
        let _stale_checksum3 = seed_chained_changeset(
            &mut storage,
            "test/",
            "mydb",
            3,
            0x1111,
            page_size,
            &[(1, page_payload(page_size as usize, 0x33))],
        );
        let checksum_from_current_page_base = 0x2222;
        let checksum4 = seed_chained_changeset(
            &mut storage,
            "test/",
            "mydb",
            4,
            checksum_from_current_page_base,
            page_size,
            &[(2, page_payload(page_size as usize, 0x44))],
        );

        let dir = tempfile::tempdir().unwrap();
        let mut state =
            SyncState::new_with_paths(dir.path().join("mydb.db"), dir.path().join("mydb.db-wal"))
                .expect("sync state");
        state.name = "mydb".to_string();
        let base = ExternalBaseCursor {
            seq: 3,
            checksum: checksum_from_current_page_base,
        };
        state.external_base = Some(base);
        state.current_seq = 4;
        state.db_checksum = Some(checksum4);
        save_external_base_progress(&state).expect("seed local external progress");

        initialize_external_base_state(&storage, "test/", &mut state, base)
            .await
            .expect("external base init should chain from the external page-base checksum");

        assert_eq!(state.current_seq, 4);
        assert_eq!(
            state.db_checksum,
            Some(checksum4),
            "writer state must ignore stale same-seq objects and trust the external page base"
        );
    }

    #[tokio::test]
    async fn external_base_state_falls_back_to_same_seq_checksum_when_it_extends_chain() {
        let mut storage = TestStorage::new();
        let page_size = 4096u32;
        let checksum3 = seed_chained_changeset(
            &mut storage,
            "test/",
            "mydb",
            3,
            0x1111,
            page_size,
            &[(1, page_payload(page_size as usize, 0x33))],
        );
        let checksum4 = seed_chained_changeset(
            &mut storage,
            "test/",
            "mydb",
            4,
            checksum3,
            page_size,
            &[(2, page_payload(page_size as usize, 0x44))],
        );

        let dir = tempfile::tempdir().unwrap();
        let mut state =
            SyncState::new_with_paths(dir.path().join("mydb.db"), dir.path().join("mydb.db-wal"))
                .expect("sync state");
        state.name = "mydb".to_string();
        let base = ExternalBaseCursor {
            seq: 3,
            checksum: 0x2222,
        };
        state.external_base = Some(base);
        state.current_seq = 4;
        state.db_checksum = Some(checksum4);
        save_external_base_progress(&state).expect("seed local external progress");

        initialize_external_base_state(&storage, "test/", &mut state, base)
            .await
            .expect("same-seq checksum should be accepted only when it extends the chain");

        assert_eq!(state.current_seq, 4);
        assert_eq!(state.db_checksum, Some(checksum4));
    }

    #[tokio::test]
    async fn external_base_state_uses_file_checksum_when_base_changeset_is_absent() {
        let storage = TestStorage::new();
        let dir = tempfile::tempdir().unwrap();
        let mut state =
            SyncState::new_with_paths(dir.path().join("mydb.db"), dir.path().join("mydb.db-wal"))
                .expect("sync state");
        state.name = "mydb".to_string();

        initialize_external_base_state(
            &storage,
            "test/",
            &mut state,
            ExternalBaseCursor {
                seq: 3,
                checksum: 0x2222,
            },
        )
        .await
        .expect("missing same-seq object falls back to materialized base checksum");

        assert_eq!(state.current_seq, 3);
        assert_eq!(state.db_checksum, Some(0x2222));
    }

    #[tokio::test]
    async fn pull_into_sink_no_new_changesets_runs_begin_and_finalize() {
        let storage = TestStorage::new();
        let mut sink = RecordingSink::new();

        let cursor = pull_incremental_into_sink(
            &storage,
            "test/",
            "mydb",
            &mut sink,
            PullCursor {
                seq: 5,
                checksum: 0x55,
            },
        )
        .await
        .expect("pull");

        assert_eq!(cursor.seq, 5, "no new changesets, returns current seq");
        assert_eq!(
            cursor.checksum, 0x55,
            "no new changesets, returns current checksum"
        );

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
        let cursor = pull_incremental_into_sink(
            &storage,
            "test/",
            "mydb",
            &mut sink,
            PullCursor {
                seq: 0,
                checksum: 0,
            },
        )
        .await
        .expect("pull");
        assert_eq!(cursor.seq, 1);

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
    async fn pull_incremental_rejects_page_id_zero_without_mutating_database() {
        let mut storage = TestStorage::new();
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("follower.db");
        let page_size = 4096u32;
        let original = vec![0x11u8; page_size as usize * 2];
        std::fs::write(&db_path, &original).unwrap();

        let pre_checksum = ltx::compute_checksum_from_file(&db_path).unwrap();
        let (bytes, _) = ltx::encode_wal_changes(
            &[(0, page_payload(page_size as usize, 0xAA))],
            page_size,
            1,
            pre_checksum,
        )
        .unwrap();
        let key = build_changeset_key("test/", "mydb", GENERATION_LIVE, 1);
        storage.insert(&key, bytes);

        let err = pull_incremental(
            &storage,
            "test/",
            "mydb",
            &db_path,
            PullCursor {
                seq: 0,
                checksum: pre_checksum,
            },
        )
        .await
        .expect_err("pull_incremental must reject page_id 0");

        assert!(
            err.to_string().contains("page number 0"),
            "expected invalid page id error, got: {err}"
        );
        assert_eq!(std::fs::read(&db_path).unwrap(), original);
    }

    #[tokio::test]
    async fn pull_incremental_truncates_database_to_encoded_end_page_count() {
        let mut storage = TestStorage::new();
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("follower.db");
        let page_size = 4096u32;
        let mut original = Vec::new();
        original.extend(vec![0x11; page_size as usize]);
        original.extend(vec![0x22; page_size as usize]);
        original.extend(vec![0x33; page_size as usize]);
        std::fs::write(&db_path, &original).unwrap();

        let pre_checksum = ltx::compute_checksum_from_file(&db_path).unwrap();
        let mut changeset = ltx::HadbChangeset::new(
            1,
            pre_checksum,
            ltx::SQLITE_PAGE_ID_SIZE,
            page_size,
            vec![
                ltx::HadbPageEntry {
                    page_id: ltx::HadbPageId::U32(1),
                    data: vec![0xAA; page_size as usize],
                },
                ltx::HadbPageEntry {
                    page_id: ltx::HadbPageId::U32(2),
                    data: Vec::new(),
                },
            ],
        );
        changeset.header.flags = 0x01;
        let key = build_changeset_key("test/", "mydb", GENERATION_LIVE, 1);
        storage.insert(&key, hadb_changeset::physical::encode(&changeset));

        let cursor = pull_incremental(
            &storage,
            "test/",
            "mydb",
            &db_path,
            PullCursor {
                seq: 0,
                checksum: pre_checksum,
            },
        )
        .await
        .expect("pull should apply shrink marker");

        assert_eq!(cursor.seq, 1);
        let data = std::fs::read(&db_path).unwrap();
        assert_eq!(data.len(), page_size as usize);
        assert_eq!(data, vec![0xAA; page_size as usize]);
    }

    #[tokio::test]
    async fn pull_incremental_rejects_first_changeset_with_wrong_anchor_checksum() {
        let mut storage = TestStorage::new();
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("follower.db");
        let page_size = 4096u32;
        let original = vec![0x11u8; page_size as usize];
        std::fs::write(&db_path, &original).unwrap();

        let actual_anchor = ltx::compute_checksum_from_file(&db_path).unwrap();
        let wrong_anchor = actual_anchor ^ 0xfeed_face_dead_beef;
        let (bytes, _) = ltx::encode_wal_changes(
            &[(1, page_payload(page_size as usize, 0xAA))],
            page_size,
            1,
            wrong_anchor,
        )
        .unwrap();
        let key = build_changeset_key("test/", "mydb", GENERATION_LIVE, 1);
        storage.insert(&key, bytes);

        pull_incremental(
            &storage,
            "test/",
            "mydb",
            &db_path,
            PullCursor {
                seq: 0,
                checksum: actual_anchor,
            },
        )
            .await
            .expect_err("pull_incremental must reject a first changeset that does not chain from the follower checksum");

        assert_eq!(
            std::fs::read(&db_path).unwrap(),
            original,
            "failed pull must not mutate the follower database"
        );
    }

    #[tokio::test]
    async fn pull_into_sink_drives_lifecycle_across_multiple_changesets() {
        let mut storage = TestStorage::new();
        let page_size = 4096u32;
        // Properly chained changesets: each prev_checksum is the prior post.
        let ck1 = seed_chained_changeset(
            &mut storage,
            "test/",
            "mydb",
            1,
            0,
            page_size,
            &[(1, page_payload(page_size as usize, 0x11))],
        );
        let ck2 = seed_chained_changeset(
            &mut storage,
            "test/",
            "mydb",
            2,
            ck1,
            page_size,
            &[(2, page_payload(page_size as usize, 0x22))],
        );
        seed_chained_changeset(
            &mut storage,
            "test/",
            "mydb",
            3,
            ck2,
            page_size,
            &[(3, page_payload(page_size as usize, 0x33))],
        );

        let mut sink = RecordingSink::new();
        let final_cursor = pull_incremental_into_sink(
            &storage,
            "test/",
            "mydb",
            &mut sink,
            PullCursor {
                seq: 0,
                checksum: 0,
            },
        )
        .await
        .expect("pull");
        assert_eq!(final_cursor.seq, 3);

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
    async fn pull_into_sink_errors_on_broken_chain_without_applying_pages() {
        // A stale changeset from a different lineage sitting at an in-range seq
        // must NOT be applied. The pull prevalidates the whole discovered chain
        // before routing any pages into the sink, so a later chain break fails
        // closed without partially advancing local state.
        let mut storage = TestStorage::new();
        let page_size = 4096u32;
        let ck1 = seed_chained_changeset(
            &mut storage,
            "test/",
            "mydb",
            1,
            0,
            page_size,
            &[(1, page_payload(page_size as usize, 0x11))],
        );
        let _ck2 = seed_chained_changeset(
            &mut storage,
            "test/",
            "mydb",
            2,
            ck1,
            page_size,
            &[(2, page_payload(page_size as usize, 0x22))],
        );
        // Seq 3 chains from a wrong prev (different lineage).
        seed_chained_changeset(
            &mut storage,
            "test/",
            "mydb",
            3,
            0xDEAD_BEEF_DEAD_BEEF,
            page_size,
            &[(3, page_payload(page_size as usize, 0x33))],
        );

        let mut sink = RecordingSink::new();
        let err = pull_incremental_into_sink(
            &storage,
            "test/",
            "mydb",
            &mut sink,
            PullCursor {
                seq: 0,
                checksum: 0,
            },
        )
        .await
        .expect_err("pull must hard-error on the mis-chained seq 3");
        assert!(
            err.to_string().contains("checksum chain broken"),
            "expected checksum-chain error, got: {err}"
        );

        let ev = sink.snapshot();
        assert!(ev.committed_seqs.is_empty());
        assert!(ev.applied.is_empty());
        assert_eq!(ev.finalize_calls, 0);
        assert_eq!(ev.abort_calls, 1);
    }

    #[tokio::test]
    async fn pull_into_sink_rejects_first_changeset_with_wrong_anchor_checksum() {
        let mut storage = TestStorage::new();
        let page_size = 4096u32;
        seed_chained_changeset(
            &mut storage,
            "test/",
            "mydb",
            1,
            0xfeed_face_dead_beef,
            page_size,
            &[(1, page_payload(page_size as usize, 0x11))],
        );

        let mut sink = RecordingSink::new();
        pull_incremental_into_sink(
            &storage,
            "test/",
            "mydb",
            &mut sink,
            PullCursor {
                seq: 0,
                checksum: 0,
            },
        )
        .await
        .expect_err(
            "sink pull must reject a first changeset that does not chain from the caller checksum",
        );

        let ev = sink.snapshot();
        assert_eq!(ev.begin_calls, 1);
        assert_eq!(ev.finalize_calls, 0);
        assert_eq!(ev.abort_calls, 1);
        assert!(
            ev.applied.is_empty(),
            "invalid first changeset must be rejected before pages reach the sink"
        );
        assert!(ev.committed_seqs.is_empty());
    }

    #[tokio::test]
    async fn pull_into_sink_skips_changesets_at_or_below_current_seq() {
        let mut storage = TestStorage::new();
        let page_size = 4096u32;
        let ck1 = seed_changeset(
            &mut storage,
            "test/",
            "mydb",
            1,
            page_size,
            &[(1, page_payload(page_size as usize, 0x11))],
        );
        seed_chained_changeset(
            &mut storage,
            "test/",
            "mydb",
            2,
            ck1,
            page_size,
            &[(2, page_payload(page_size as usize, 0x22))],
        );

        let mut sink = RecordingSink::new();
        let cursor = pull_incremental_into_sink(
            &storage,
            "test/",
            "mydb",
            &mut sink,
            PullCursor {
                seq: 1,
                checksum: ck1,
            },
        )
        .await
        .expect("pull");
        assert_eq!(cursor.seq, 2, "should advance past current_seq=1 to seq=2");

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
        let result = pull_incremental_into_sink(
            &storage,
            "test/",
            "mydb",
            &mut sink,
            PullCursor {
                seq: 0,
                checksum: 0,
            },
        )
        .await;

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
        let result = pull_incremental_into_sink(
            &storage,
            "test/",
            "mydb",
            &mut sink,
            PullCursor {
                seq: 0,
                checksum: 0,
            },
        )
        .await;

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

        let result = pull_incremental_into_sink(
            &storage,
            "test/",
            "mydb",
            &mut sink,
            PullCursor {
                seq: 0,
                checksum: 0,
            },
        )
        .await;

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
        let result = pull_incremental_into_sink(
            &storage,
            "test/",
            "mydb",
            &mut sink,
            PullCursor {
                seq: 0,
                checksum: 0,
            },
        )
        .await;

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

    // ---- Fenced delta envelope publish + discovery ----

    /// Mutable in-memory storage with real `put` / `put_if_absent`
    /// semantics, for exercising the publish CAS path. The earlier
    /// `TestStorage` no-ops those, which is fine for the chain-replay
    /// tests but useless for the equivocation guard.
    struct MutStorage {
        objects: Arc<Mutex<StdHashMap<String, Vec<u8>>>>,
    }

    impl MutStorage {
        fn new() -> Self {
            Self {
                objects: Arc::new(Mutex::new(StdHashMap::new())),
            }
        }
    }

    #[async_trait]
    impl StorageBackend for MutStorage {
        async fn get(&self, key: &str) -> Result<Option<Vec<u8>>> {
            Ok(self.objects.lock().unwrap().get(key).cloned())
        }
        async fn put(&self, key: &str, data: &[u8]) -> Result<()> {
            self.objects
                .lock()
                .unwrap()
                .insert(key.to_string(), data.to_vec());
            Ok(())
        }
        async fn delete(&self, key: &str) -> Result<()> {
            self.objects.lock().unwrap().remove(key);
            Ok(())
        }
        async fn list(&self, prefix: &str, after: Option<&str>) -> Result<Vec<String>> {
            let mut keys: Vec<String> = self
                .objects
                .lock()
                .unwrap()
                .keys()
                .filter(|k| k.starts_with(prefix))
                .filter(|k| after.map(|a| k.as_str() > a).unwrap_or(true))
                .cloned()
                .collect();
            keys.sort();
            Ok(keys)
        }
        async fn exists(&self, key: &str) -> Result<bool> {
            Ok(self.objects.lock().unwrap().contains_key(key))
        }
        async fn put_if_absent(&self, key: &str, data: &[u8]) -> Result<CasResult> {
            let mut objs = self.objects.lock().unwrap();
            if objs.contains_key(key) {
                return Ok(CasResult {
                    success: false,
                    etag: None,
                });
            }
            objs.insert(key.to_string(), data.to_vec());
            Ok(CasResult {
                success: true,
                etag: Some("mut".into()),
            })
        }
        async fn put_if_match(&self, key: &str, data: &[u8], _etag: &str) -> Result<CasResult> {
            self.objects
                .lock()
                .unwrap()
                .insert(key.to_string(), data.to_vec());
            Ok(CasResult {
                success: true,
                etag: Some("mut".into()),
            })
        }
    }

    fn delta_payload(seq: u64, epoch: u64, writer: &str, ltx: Vec<u8>) -> DeltaPayloadV1 {
        DeltaPayloadV1 {
            seq,
            epoch,
            writer_id: writer.to_string(),
            prev_checksum: vec![0u8; 32],
            end_page_count: 100 + seq,
            ltx_payload: ltx,
        }
    }

    #[tokio::test]
    async fn publish_delta_envelope_is_idempotent_on_identical_bytes() {
        let storage = MutStorage::new();
        let payload = delta_payload(1, 5, "w", vec![1, 2, 3]);

        let c1 = publish_delta_envelope(&storage, "wal/", "db", &payload)
            .await
            .expect("first publish");
        // Re-publishing identical bytes is a no-op success (retry safety).
        let c2 = publish_delta_envelope(&storage, "wal/", "db", &payload)
            .await
            .expect("idempotent re-publish");
        assert_eq!(c1, c2, "idempotent re-publish yields the same checksum");
    }

    #[tokio::test]
    async fn publish_delta_envelope_rejects_same_seq_divergent_bytes() {
        let storage = MutStorage::new();
        let first = delta_payload(7, 5, "w", vec![1, 1, 1]);
        publish_delta_envelope(&storage, "wal/", "db", &first)
            .await
            .expect("first publish");

        // Same seq, different content = equivocation. Must bail.
        let divergent = delta_payload(7, 5, "w", vec![2, 2, 2]);
        let err = publish_delta_envelope(&storage, "wal/", "db", &divergent)
            .await
            .expect_err("must refuse divergent same-seq publish");
        assert!(
            err.to_string().contains("equivocation"),
            "error must name equivocation, got: {err}"
        );
    }

    struct MutateSourceOnPutStorage {
        objects: Arc<Mutex<StdHashMap<String, Vec<u8>>>>,
        db_path: PathBuf,
        mutated: Arc<Mutex<bool>>,
    }

    impl MutateSourceOnPutStorage {
        fn new(db_path: PathBuf) -> Self {
            Self {
                objects: Arc::new(Mutex::new(StdHashMap::new())),
                db_path,
                mutated: Arc::new(Mutex::new(false)),
            }
        }
    }

    #[async_trait]
    impl StorageBackend for MutateSourceOnPutStorage {
        async fn get(&self, key: &str) -> Result<Option<Vec<u8>>> {
            Ok(self.objects.lock().unwrap().get(key).cloned())
        }

        async fn put(&self, key: &str, data: &[u8]) -> Result<()> {
            self.objects
                .lock()
                .unwrap()
                .insert(key.to_string(), data.to_vec());

            let mut mutated = self.mutated.lock().unwrap();
            if !*mutated {
                let conn = rusqlite::Connection::open(&self.db_path)?;
                conn.execute_batch(
                    "
                    PRAGMA journal_mode=WAL;
                    INSERT INTO items (id, value) VALUES (2, 'after-upload');
                    ",
                )?;
                *mutated = true;
            }

            Ok(())
        }

        async fn delete(&self, key: &str) -> Result<()> {
            self.objects.lock().unwrap().remove(key);
            Ok(())
        }

        async fn list(&self, prefix: &str, after: Option<&str>) -> Result<Vec<String>> {
            let mut keys: Vec<String> = self
                .objects
                .lock()
                .unwrap()
                .keys()
                .filter(|k| k.starts_with(prefix))
                .filter(|k| after.map(|a| k.as_str() > a).unwrap_or(true))
                .cloned()
                .collect();
            keys.sort();
            Ok(keys)
        }

        async fn exists(&self, key: &str) -> Result<bool> {
            Ok(self.objects.lock().unwrap().contains_key(key))
        }

        async fn put_if_absent(&self, key: &str, data: &[u8]) -> Result<CasResult> {
            self.put(key, data).await?;
            Ok(CasResult {
                success: true,
                etag: Some("test".into()),
            })
        }

        async fn put_if_match(&self, key: &str, data: &[u8], _etag: &str) -> Result<CasResult> {
            self.put(key, data).await?;
            Ok(CasResult {
                success: true,
                etag: Some("test".into()),
            })
        }
    }

    #[tokio::test]
    async fn take_snapshot_state_checksum_matches_uploaded_snapshot_bytes() {
        let dir = tempfile::TempDir::new().unwrap();
        let db_path = dir.path().join("checksum-race.db");
        let restored_path = dir.path().join("restored.db");
        let conn = rusqlite::Connection::open(&db_path).unwrap();
        conn.execute_batch(
            "
            PRAGMA journal_mode=WAL;
            PRAGMA wal_autocheckpoint=0;
            CREATE TABLE items (id INTEGER PRIMARY KEY, value TEXT NOT NULL);
            INSERT INTO items (id, value) VALUES (1, 'uploaded');
            ",
        )
        .unwrap();
        drop(conn);

        let storage = MutateSourceOnPutStorage::new(db_path.clone());
        let mut state = SyncState::new(db_path.clone()).unwrap();
        state.name = "checksum_race".to_string();
        state.init_checksum().unwrap();

        take_snapshot_with_retry(
            &storage,
            "prefix/",
            &mut state,
            &RetryPolicy::default_policy(),
        )
        .await
        .unwrap();

        let key = build_changeset_key(
            "prefix/",
            &state.name,
            GENERATION_SNAPSHOT,
            state.current_seq,
        );
        let uploaded = storage.get(&key).await.unwrap().expect("snapshot uploaded");
        let decoded = ltx::decode_to_db(&uploaded, &restored_path).unwrap();

        assert_eq!(
            state.db_checksum,
            Some(decoded.checksum),
            "state checksum must describe the uploaded snapshot bytes, not a later live-file read"
        );
    }

    #[tokio::test]
    async fn walrust_owned_sync_rejects_divergent_existing_changeset() {
        let dir = tempfile::TempDir::new().unwrap();
        let db_path = dir.path().join("owned-cas.db");
        let conn = rusqlite::Connection::open(&db_path).unwrap();
        conn.execute_batch(
            "
            PRAGMA journal_mode=WAL;
            PRAGMA wal_autocheckpoint=0;
            CREATE TABLE items (id INTEGER PRIMARY KEY, value TEXT NOT NULL);
            INSERT INTO items (id, value) VALUES (1, 'base');
            ",
        )
        .unwrap();

        let storage = MutStorage::new();
        let mut state = SyncState::new(db_path.clone()).unwrap();
        state.name = "owned_cas".to_string();
        state.init_checksum().unwrap();
        take_snapshot_with_retry(
            &storage,
            "prefix/",
            &mut state,
            &RetryPolicy::default_policy(),
        )
        .await
        .unwrap();

        conn.execute("INSERT INTO items (id, value) VALUES (2, 'delta')", [])
            .unwrap();

        let next_seq = state.current_seq + 1;
        let key = build_changeset_key("prefix/", &state.name, GENERATION_LIVE, next_seq);
        let existing = b"conflicting existing object".to_vec();
        storage.put(&key, &existing).await.unwrap();

        let err = sync_wal(&storage, "prefix/", &mut state)
            .await
            .expect_err("walrust-owned sync must not overwrite a divergent existing object");
        let msg = err.to_string();
        assert!(
            msg.contains("duplicate changeset seq") || msg.contains("refusing overwrite"),
            "expected duplicate overwrite refusal, got: {msg}"
        );
        assert_eq!(
            storage.get(&key).await.unwrap(),
            Some(existing),
            "failed CAS publish must leave the existing object intact"
        );
    }

    #[tokio::test]
    async fn walrust_owned_reanchors_after_crash_window_same_seq_conflict() {
        // B11: a crash between a durable changeset put and its save_state
        // leaves our own object at seq N. On restart we reload the stale
        // cursor; if more commits landed we re-encode *different* bytes at
        // seq N. Recovery must adopt the durable object and re-anchor with a
        // fresh snapshot (no wedge, no data loss), not hard-fail.
        let dir = tempfile::TempDir::new().unwrap();
        let db_path = dir.path().join("owned-crash.db");
        let restored_path = dir.path().join("restored.db");
        let conn = rusqlite::Connection::open(&db_path).unwrap();
        conn.execute_batch(
            "
            PRAGMA journal_mode=WAL;
            PRAGMA wal_autocheckpoint=0;
            CREATE TABLE items (id INTEGER PRIMARY KEY, value TEXT NOT NULL);
            INSERT INTO items (id, value) VALUES (1, 'base');
            ",
        )
        .unwrap();

        let storage = MutStorage::new();
        let mut state = SyncState::new(db_path.clone()).unwrap();
        state.name = "owned_crash".to_string();
        state.init_checksum().unwrap();
        take_snapshot_with_retry(&storage, "p/", &mut state, &RetryPolicy::default_policy())
            .await
            .unwrap();

        // Snapshot of the reload cursor as it would be persisted BEFORE the
        // seq-2 incremental publishes (i.e. what a crashed process reloads).
        let crashed_seq = state.current_seq;
        let crashed_txid = state.current_txid;
        let crashed_checksum = state.db_checksum;
        let crashed_offset = state.wal_offset;
        let crashed_gen = state.wal_generation;
        let crashed_salt = state.wal_salt;
        let crashed_chain = state.wal_checksum_chain;

        // Durable publish of seq 2 (this is the write that survives the crash).
        conn.execute("INSERT INTO items (id, value) VALUES (2, 'delta')", [])
            .unwrap();
        sync_wal(&storage, "p/", &mut state).await.unwrap();
        let seq2_key = build_changeset_key("p/", &state.name, GENERATION_LIVE, 2);
        assert!(
            storage.get(&seq2_key).await.unwrap().is_some(),
            "seq 2 incremental must be durably published pre-crash"
        );

        // Simulate the crash: save_state never ran, so reload the stale cursor.
        state.current_seq = crashed_seq;
        state.current_txid = crashed_txid;
        state.db_checksum = crashed_checksum;
        state.wal_offset = crashed_offset;
        state.wal_generation = crashed_gen;
        state.wal_salt = crashed_salt;
        state.wal_checksum_chain = crashed_chain;

        // A new commit lands after the crash, before the next sync.
        conn.execute(
            "INSERT INTO items (id, value) VALUES (3, 'after-crash')",
            [],
        )
        .unwrap();

        // The next sync re-encodes different bytes at seq 2 -> CAS conflict ->
        // recovery re-anchors with a snapshot at seq 3.
        sync_wal(&storage, "p/", &mut state)
            .await
            .expect("crash-window conflict must recover, not wedge");
        assert_eq!(
            state.current_seq, 3,
            "re-anchor must land a snapshot at seq 3"
        );
        let snap3_key = build_changeset_key("p/", &state.name, GENERATION_SNAPSHOT, 3);
        assert!(
            storage.get(&snap3_key).await.unwrap().is_some(),
            "recovery must publish a fresh snapshot at seq 3"
        );

        // Restore must round-trip all three committed rows.
        restore(Arc::new(storage), "p/", &state.name, &restored_path, None)
            .await
            .expect("restore after re-anchor");
        let rconn = rusqlite::Connection::open(&restored_path).unwrap();
        let count: i64 = rconn
            .query_row("SELECT COUNT(*) FROM items", [], |r| r.get(0))
            .unwrap();
        assert_eq!(
            count, 3,
            "all rows including the post-crash commit must restore"
        );
        let integrity: String = rconn
            .query_row("PRAGMA integrity_check", [], |r| r.get(0))
            .unwrap();
        assert_eq!(integrity, "ok");
    }

    #[tokio::test]
    async fn walrust_owned_sync_rejects_foreign_same_seq_changeset() {
        // B11 discriminator: a same-seq object that is NOT our own prefix
        // (undecodable/foreign bytes) must remain a hard equivocation error,
        // never silently re-anchored.
        let dir = tempfile::TempDir::new().unwrap();
        let db_path = dir.path().join("owned-foreign.db");
        let conn = rusqlite::Connection::open(&db_path).unwrap();
        conn.execute_batch(
            "
            PRAGMA journal_mode=WAL;
            PRAGMA wal_autocheckpoint=0;
            CREATE TABLE items (id INTEGER PRIMARY KEY, value TEXT NOT NULL);
            INSERT INTO items (id, value) VALUES (1, 'base');
            ",
        )
        .unwrap();

        let storage = MutStorage::new();
        let mut state = SyncState::new(db_path.clone()).unwrap();
        state.name = "owned_foreign".to_string();
        state.init_checksum().unwrap();
        take_snapshot_with_retry(&storage, "p/", &mut state, &RetryPolicy::default_policy())
            .await
            .unwrap();

        conn.execute("INSERT INTO items (id, value) VALUES (2, 'delta')", [])
            .unwrap();
        let next_seq = state.current_seq + 1;
        let key = build_changeset_key("p/", &state.name, GENERATION_LIVE, next_seq);
        storage
            .put(&key, b"foreign non-changeset bytes")
            .await
            .unwrap();

        let err = sync_wal(&storage, "p/", &mut state)
            .await
            .expect_err("foreign same-seq object must hard-fail");
        assert!(
            WalrustError::is_equivocation(&err),
            "must be a typed equivocation error, got: {err}"
        );
    }

    #[tokio::test]
    async fn walrust_owned_sync_rejects_second_writer_same_base_same_seq() {
        // B11 split-brain: TWO live writers that share the same lineage/base
        // (e.g. an HA failover where the promoted node restored from the same
        // backup and the original node came back) each publish a DIFFERENT
        // changeset at the same seq with the SAME prev_checksum (both anchored
        // at the shared base state). The crash-window discriminator must NOT
        // misclassify the OTHER writer's object as our own crashed publish and
        // silently adopt it -- that would re-legitimize split-brain. It must
        // hard-fail loudly so the operator sees the equivocation, because a
        // process that never itself published this object has no self-authorship
        // proof for it.
        let storage = MutStorage::new();

        // Writer B establishes the shared stream: base + snapshot at seq 1.
        let dir_b = tempfile::TempDir::new().unwrap();
        let db_b = dir_b.path().join("split.db");
        let conn_b = rusqlite::Connection::open(&db_b).unwrap();
        conn_b
            .execute_batch(
                "
            PRAGMA journal_mode=WAL;
            PRAGMA wal_autocheckpoint=0;
            CREATE TABLE items (id INTEGER PRIMARY KEY, value TEXT NOT NULL);
            INSERT INTO items (id, value) VALUES (1, 'base');
            ",
            )
            .unwrap();
        let mut state_b = SyncState::new(db_b.clone()).unwrap();
        state_b.name = "split".to_string();
        state_b.init_checksum().unwrap();
        take_snapshot_with_retry(&storage, "p/", &mut state_b, &RetryPolicy::default_policy())
            .await
            .unwrap();

        // The shared base cursor both writers restore to.
        let base_seq = state_b.current_seq;
        let base_checksum = state_b.db_checksum;

        // Writer A is a SECOND live writer on its own physical DB, anchored to
        // the SAME lineage (same name/prefix => same key namespace) and the SAME
        // base checksum -- the split-brain precondition after a shared restore.
        let dir_a = tempfile::TempDir::new().unwrap();
        let db_a = dir_a.path().join("split.db");
        let conn_a = rusqlite::Connection::open(&db_a).unwrap();
        conn_a
            .execute_batch(
                "
            PRAGMA journal_mode=WAL;
            PRAGMA wal_autocheckpoint=0;
            CREATE TABLE items (id INTEGER PRIMARY KEY, value TEXT NOT NULL);
            INSERT INTO items (id, value) VALUES (1, 'base');
            INSERT INTO items (id, value) VALUES (2, 'from-A');
            ",
            )
            .unwrap();
        let mut state_a = SyncState::new(db_a.clone()).unwrap();
        state_a.name = "split".to_string();
        state_a.current_seq = base_seq;
        state_a.db_checksum = base_checksum;

        // Writer B publishes a real seq-2 changeset (its own divergent data).
        conn_b
            .execute("INSERT INTO items (id, value) VALUES (2, 'from-B')", [])
            .unwrap();
        sync_wal(&storage, "p/", &mut state_b).await.unwrap();
        let seq2_key = build_changeset_key("p/", "split", GENERATION_LIVE, base_seq + 1);
        assert!(
            storage.get(&seq2_key).await.unwrap().is_some(),
            "writer B must have published seq {}",
            base_seq + 1
        );

        // Writer A now tries to publish ITS OWN seq-2 changeset. Same seq, same
        // prev_checksum (shared base), different bytes => CAS conflict. A did not
        // publish B's object, so it must NOT adopt it -- hard-fail loudly.
        let err = sync_wal(&storage, "p/", &mut state_a)
            .await
            .expect_err("a second writer's same-seq object must hard-fail, not be adopted");
        assert!(
            WalrustError::is_equivocation(&err),
            "second-writer conflict must surface as a typed equivocation, got: {err}"
        );
    }

    #[tokio::test]
    async fn walrust_owned_snapshot_rejects_divergent_existing_changeset() {
        let dir = tempfile::TempDir::new().unwrap();
        let db_path = dir.path().join("owned-snapshot-cas.db");
        let conn = rusqlite::Connection::open(&db_path).unwrap();
        conn.execute_batch(
            "
            PRAGMA journal_mode=WAL;
            PRAGMA wal_autocheckpoint=0;
            CREATE TABLE items (id INTEGER PRIMARY KEY, value TEXT NOT NULL);
            INSERT INTO items (id, value) VALUES (1, 'base');
            ",
        )
        .unwrap();
        drop(conn);

        let storage = MutStorage::new();
        let mut state = SyncState::new(db_path).unwrap();
        state.name = "owned_snapshot_cas".to_string();
        state.init_checksum().unwrap();

        let next_seq = state.current_seq + 1;
        let key = build_changeset_key("prefix/", &state.name, GENERATION_SNAPSHOT, next_seq);
        let existing = b"conflicting existing snapshot object".to_vec();
        storage.put(&key, &existing).await.unwrap();

        let err = take_snapshot_with_retry(
            &storage,
            "prefix/",
            &mut state,
            &RetryPolicy::default_policy(),
        )
        .await
        .expect_err("walrust-owned snapshot must not overwrite a divergent existing object");
        let msg = err.to_string();
        assert!(
            msg.contains("duplicate changeset seq") || msg.contains("refusing overwrite"),
            "expected duplicate overwrite refusal, got: {msg}"
        );
        assert_eq!(
            storage.get(&key).await.unwrap(),
            Some(existing),
            "failed snapshot CAS publish must leave the existing object intact"
        );
        assert_eq!(
            state.current_seq, 0,
            "failed snapshot publish must not advance state"
        );
    }

    #[tokio::test]
    async fn publish_then_list_after_filters_and_sorts() {
        let storage = MutStorage::new();
        // Publish out of order to prove the listing sorts.
        for seq in [3u64, 1, 2] {
            let p = delta_payload(seq, 5, "w", vec![seq as u8]);
            publish_delta_envelope(&storage, "wal/", "db", &p)
                .await
                .expect("publish");
        }

        // after_seq = 1 -> only seq 2 and 3, ascending.
        let found = list_delta_envelopes_after(&storage, "wal/", "db", 1)
            .await
            .expect("list");
        let seqs: Vec<u64> = found.iter().map(|d| d.seq).collect();
        assert_eq!(seqs, vec![2, 3]);
        // Decoded payloads carry the stamped fields.
        assert_eq!(found[0].payload.epoch, 5);
        assert_eq!(found[0].payload.writer_id, "w");
        // envelope_checksum matches a fresh hash of the re-encoded payload.
        let reencoded = external_delta::encode(&found[0].payload).unwrap();
        assert_eq!(
            found[0].envelope_checksum,
            external_delta::checksum(&reencoded)
        );
    }

    #[tokio::test]
    async fn list_skips_non_tlmd_objects() {
        let storage = MutStorage::new();
        // A legacy .hadbp object in the same directory must be ignored.
        storage
            .put("wal/db/0000/0000000000000001.hadbp", b"legacy-ltx")
            .await
            .unwrap();
        let p = delta_payload(2, 5, "w", vec![9]);
        publish_delta_envelope(&storage, "wal/", "db", &p)
            .await
            .expect("publish tlmd");

        let found = list_delta_envelopes_after(&storage, "wal/", "db", 0)
            .await
            .expect("list");
        let seqs: Vec<u64> = found.iter().map(|d| d.seq).collect();
        assert_eq!(seqs, vec![2], "only the .tlmd object is returned");
    }

    #[tokio::test]
    async fn fetch_delta_envelope_round_trips() {
        let storage = MutStorage::new();
        let p = delta_payload(42, 9, "leader", vec![7, 7, 7]);
        publish_delta_envelope(&storage, "wal/", "db", &p)
            .await
            .expect("publish");

        let fetched = fetch_delta_envelope(&storage, "wal/", "db", 42)
            .await
            .expect("fetch ok")
            .expect("present");
        assert_eq!(fetched.seq, 42);
        assert_eq!(fetched.payload, p);

        // Missing seq returns None, not an error.
        let missing = fetch_delta_envelope(&storage, "wal/", "db", 999)
            .await
            .expect("fetch ok");
        assert!(missing.is_none());
    }

    /// The chain link from one published envelope to the next is exactly
    /// BLAKE3 of the prior envelope — the property the follower-side
    /// chain verifier (step 5) walks. Proven here at the storage layer.
    #[tokio::test]
    async fn published_chain_links_by_blake3_of_prior_envelope() {
        let storage = MutStorage::new();

        let first = delta_payload(1, 5, "w", vec![1]);
        let first_ck = publish_delta_envelope(&storage, "wal/", "db", &first)
            .await
            .expect("publish first");

        let mut second = delta_payload(2, 5, "w", vec![2]);
        second.prev_checksum = first_ck.to_vec();
        publish_delta_envelope(&storage, "wal/", "db", &second)
            .await
            .expect("publish second");

        let found = list_delta_envelopes_after(&storage, "wal/", "db", 0)
            .await
            .expect("list");
        assert_eq!(found.len(), 2);
        // second.prev_checksum == BLAKE3(first envelope) == first.envelope_checksum
        assert_eq!(
            found[1].payload.prev_checksum,
            found[0].envelope_checksum.to_vec()
        );
    }

    /// B14: a mislabeled envelope (stored at the key for one seq but carrying a
    /// different inner `payload.seq`) is a hard, typed integrity error — not
    /// silently returned for a follower to apply out of order.
    #[tokio::test]
    async fn list_delta_envelopes_after_rejects_seq_key_mismatch() {
        let storage = MutStorage::new();
        // payload claims seq 99 but is planted at the key for seq 5.
        let payload = delta_payload(99, 5, "w", vec![1, 2, 3]);
        let bytes = external_delta::encode(&payload).unwrap();
        let key = delta_envelope_key("wal/", "db", 5);
        storage.put(&key, &bytes).await.unwrap();

        let err = list_delta_envelopes_after(&storage, "wal/", "db", 0)
            .await
            .expect_err("seq/key mismatch must be rejected");
        assert!(
            err.to_string().contains("seq mismatch"),
            "expected seq mismatch rejection, got: {err}"
        );
        assert_eq!(
            crate::errors::classify_error(&err),
            crate::errors::ExitStatus::Integrity
        );
    }

    /// Real-WAL helper: create a fresh WAL-mode DB, fold `base` rows into the
    /// main file via a TRUNCATE checkpoint, and copy that stable image as the
    /// follower's base. Returns the live connection (WAL still owned) plus the
    /// db path and the base-copy path.
    fn setup_fenced_source(
        dir: &Path,
    ) -> (rusqlite::Connection, std::path::PathBuf, std::path::PathBuf) {
        let db_path = dir.join("fenced.db");
        let base_copy = dir.join("base.db");
        let conn = rusqlite::Connection::open(&db_path).unwrap();
        conn.execute_batch(
            "PRAGMA journal_mode=WAL;
             PRAGMA wal_autocheckpoint=0;
             PRAGMA page_size=4096;
             CREATE TABLE items (id INTEGER PRIMARY KEY, value TEXT NOT NULL);
             INSERT INTO items (id, value) VALUES (1, 'base-1'), (2, 'base-2');",
        )
        .unwrap();
        conn.execute_batch("PRAGMA wal_checkpoint(TRUNCATE);")
            .unwrap();
        std::fs::copy(&db_path, &base_copy).unwrap();
        (conn, db_path, base_copy)
    }

    fn read_items(path: &Path) -> Vec<(i64, String)> {
        let conn = rusqlite::Connection::open(path).unwrap();
        let mut stmt = conn
            .prepare("SELECT id, value FROM items ORDER BY id")
            .unwrap();
        let rows: Vec<(i64, String)> = stmt
            .query_map([], |r| Ok((r.get(0)?, r.get(1)?)))
            .unwrap()
            .map(|r| r.unwrap())
            .collect();
        rows
    }

    /// Publish `batches` real fenced deltas from `conn`'s WAL into `storage`,
    /// threading the envelope chain. Returns the last envelope checksum and the
    /// applied count so the follower can anchor.
    async fn publish_fenced_deltas(
        storage: &dyn StorageBackend,
        prefix: &str,
        state: &mut SyncState,
        conn: &rusqlite::Connection,
        epoch: u64,
        writer: &str,
        anchor: [u8; 32],
        batches: u64,
    ) -> [u8; 32] {
        let mut prev_env = anchor;
        for batch in 0..batches {
            conn.execute(
                "INSERT INTO items (id, value) VALUES (?1, ?2)",
                rusqlite::params![10 + batch as i64, format!("d{batch}")],
            )
            .unwrap();
            let params = FencedDeltaSyncParams {
                epoch,
                writer_id: writer.to_string(),
                prev_envelope_checksum: prev_env,
            };
            if let Some(res) = sync_wal_fenced_delta(storage, prefix, state, &params)
                .await
                .unwrap()
            {
                prev_env = res.envelope_checksum;
            }
        }
        prev_env
    }

    /// Production-path proof: a follower reconstructs the EXACT database from
    /// the published fenced-delta sequence via the production
    /// `reconstruct_fenced_follower` API. Drives the real `sync_wal_fenced_delta`
    /// writer over a real rusqlite WAL — not a fixture.
    #[tokio::test]
    async fn reconstruct_fenced_follower_replays_published_deltas() {
        const EPOCH: u64 = 7;
        const WRITER: &str = "leader-A";
        const ANCHOR: [u8; 32] = [0x11; 32];

        let dir = tempfile::TempDir::new().unwrap();
        let (conn, db_path, base_copy) = setup_fenced_source(dir.path());
        let follower = dir.path().join("follower.db");

        let storage = MutStorage::new();
        let mut state = SyncState::new(db_path.clone()).unwrap();
        state.name = "fenced".to_string();
        state.init_checksum().unwrap();
        let base_seq = state.current_seq;

        publish_fenced_deltas(
            &storage, "fenced/", &mut state, &conn, EPOCH, WRITER, ANCHOR, 3,
        )
        .await;
        drop(conn);

        let cursor = FencedFollowerCursor {
            base_seq,
            epoch: EPOCH,
            writer_id: WRITER.to_string(),
            base_envelope_checksum: ANCHOR,
        };
        let result = reconstruct_fenced_follower(
            &storage, "fenced/", "fenced", &cursor, &base_copy, &follower,
        )
        .await
        .expect("honest fenced follower reconstruct");
        assert!(result.applied >= 1, "at least one delta must be published");
        assert_eq!(result.head_seq, base_seq + result.applied);

        // integrity_check + exact row equality against the source DB.
        let conn = rusqlite::Connection::open(&follower).unwrap();
        let integ: String = conn
            .query_row("PRAGMA integrity_check", [], |r| r.get(0))
            .unwrap();
        assert_eq!(
            integ, "ok",
            "reconstructed follower must pass integrity_check"
        );
        drop(conn);
        assert_eq!(
            read_items(&follower),
            read_items(&db_path),
            "fenced follower must match source rows exactly"
        );
    }

    /// The production follower API enforces every fence: a forged head+1
    /// envelope (wrong epoch / wrong writer / broken chain — each otherwise
    /// valid) is rejected with a typed integrity error BEFORE any apply.
    #[tokio::test]
    async fn reconstruct_fenced_follower_rejects_forged_envelopes() {
        const EPOCH: u64 = 7;
        const WRITER: &str = "leader-A";
        const ANCHOR: [u8; 32] = [0x11; 32];

        let dir = tempfile::TempDir::new().unwrap();
        let (conn, _db_path, base_copy) = setup_fenced_source(dir.path());
        let follower = dir.path().join("follower.db");

        let storage = MutStorage::new();
        let mut state = SyncState::new(_db_path.clone()).unwrap();
        state.name = "fenced".to_string();
        state.init_checksum().unwrap();
        let base_seq = state.current_seq;

        publish_fenced_deltas(
            &storage, "fenced/", &mut state, &conn, EPOCH, WRITER, ANCHOR, 2,
        )
        .await;
        drop(conn);

        let cursor = FencedFollowerCursor {
            base_seq,
            epoch: EPOCH,
            writer_id: WRITER.to_string(),
            base_envelope_checksum: ANCHOR,
        };
        let honest = reconstruct_fenced_follower(
            &storage, "fenced/", "fenced", &cursor, &base_copy, &follower,
        )
        .await
        .expect("honest reconstruct");
        let head_seq = base_seq + honest.applied;
        let head = fetch_delta_envelope(&storage, "fenced/", "fenced", head_seq)
            .await
            .expect("fetch head")
            .expect("head exists");
        let forged_seq = head_seq + 1;
        let forged_key = delta_envelope_key("fenced/", "fenced", forged_seq);
        let forge = |epoch: u64, writer: &str, prev: Vec<u8>| DeltaPayloadV1 {
            seq: forged_seq,
            epoch,
            writer_id: writer.to_string(),
            prev_checksum: prev,
            end_page_count: head.payload.end_page_count,
            ltx_payload: head.payload.ltx_payload.clone(),
        };
        let cases = [
            (
                "epoch fence",
                forge(EPOCH + 1, WRITER, honest.head_envelope_checksum.to_vec()),
            ),
            (
                "writer fence",
                forge(EPOCH, "stale-B", honest.head_envelope_checksum.to_vec()),
            ),
            ("envelope chain break", forge(EPOCH, WRITER, vec![0xEE; 32])),
        ];
        for (label, payload) in cases {
            let bytes = external_delta::encode(&payload).unwrap();
            storage.put(&forged_key, &bytes).await.unwrap();
            let poisoned = dir
                .path()
                .join(format!("poisoned-{}.db", label.replace(' ', "-")));
            let err = reconstruct_fenced_follower(
                &storage, "fenced/", "fenced", &cursor, &base_copy, &poisoned,
            )
            .await
            .expect_err("forged envelope must be rejected");
            assert!(
                err.to_string().contains(label),
                "expected rejection by {label}, got: {err}"
            );
            assert_eq!(
                crate::errors::classify_error(&err),
                crate::errors::ExitStatus::Integrity,
                "fence violation must be a typed integrity error ({label})"
            );
            storage.delete(&forged_key).await.unwrap();
        }
    }

    /// Atomicity of rejection, MID-STREAM: a forged envelope planted at a seq in
    /// the MIDDLE of an otherwise-valid chain must leave the follower DB exactly
    /// at the state produced by the deltas BEFORE it — no page from the forged
    /// envelope (nor any later one) is applied — and, once the bad envelope is
    /// replaced with the honest one, a fresh reconstruct resumes cleanly to the
    /// full head. This guards the "fences enforced before any apply" claim
    /// against the harder case than head+1: the forge is not at the end.
    #[tokio::test]
    async fn reconstruct_fenced_follower_rejects_midstream_forge_atomically() {
        const EPOCH: u64 = 7;
        const WRITER: &str = "leader-A";
        const ANCHOR: [u8; 32] = [0x11; 32];

        let dir = tempfile::TempDir::new().unwrap();
        let (conn, _db_path, base_copy) = setup_fenced_source(dir.path());

        let storage = MutStorage::new();
        let mut state = SyncState::new(_db_path.clone()).unwrap();
        state.name = "fenced".to_string();
        state.init_checksum().unwrap();
        let base_seq = state.current_seq;

        // Four real deltas: batch b inserts row (10 + b, "d{b}") at seq base+b+1.
        publish_fenced_deltas(
            &storage, "fenced/", &mut state, &conn, EPOCH, WRITER, ANCHOR, 4,
        )
        .await;
        drop(conn);

        let cursor = FencedFollowerCursor {
            base_seq,
            epoch: EPOCH,
            writer_id: WRITER.to_string(),
            base_envelope_checksum: ANCHOR,
        };

        // Honest full reconstruct: the target the resume must reach.
        let full_follower = dir.path().join("full.db");
        let full = reconstruct_fenced_follower(
            &storage,
            "fenced/",
            "fenced",
            &cursor,
            &base_copy,
            &full_follower,
        )
        .await
        .expect("honest full reconstruct");
        assert!(full.applied >= 4, "expected 4 published deltas");
        let full_rows = read_items(&full_follower);

        // Forge the MIDDLE envelope (seq base+2, position 1 = the delta that
        // inserts row (11, "d1")). Keep its chain link (prev_checksum) honest so
        // ONLY the epoch fence trips — proving the fence, not the chain hash,
        // stops a delta buried inside an otherwise-valid stream.
        let mid_seq = base_seq + 2;
        let real_mid = fetch_delta_envelope(&storage, "fenced/", "fenced", mid_seq)
            .await
            .expect("fetch mid")
            .expect("mid exists");
        let real_mid_bytes = external_delta::encode(&real_mid.payload).unwrap();
        let mid_key = delta_envelope_key("fenced/", "fenced", mid_seq);
        let forged = DeltaPayloadV1 {
            epoch: EPOCH + 1,
            ..real_mid.payload.clone()
        };
        storage
            .put(&mid_key, &external_delta::encode(&forged).unwrap())
            .await
            .unwrap();

        let poisoned = dir.path().join("poisoned-mid.db");
        let err = reconstruct_fenced_follower(
            &storage, "fenced/", "fenced", &cursor, &base_copy, &poisoned,
        )
        .await
        .expect_err("mid-stream forged envelope must be rejected");
        assert!(
            err.to_string().contains("epoch fence"),
            "expected epoch fence rejection at mid seq, got: {err}"
        );
        assert_eq!(
            crate::errors::classify_error(&err),
            crate::errors::ExitStatus::Integrity
        );

        // Atomicity: the follower DB holds ONLY the deltas before the forge
        // (base + row (10,"d0")). The forged seq's row (11,"d1") and every later
        // row (12,"d2"), (13,"d3") must be absent — the forged envelope's pages
        // never reached the file, and no later delta was applied past the break.
        let expected_before_forge = vec![
            (1_i64, "base-1".to_string()),
            (2, "base-2".to_string()),
            (10, "d0".to_string()),
        ];
        assert_eq!(
            read_items(&poisoned),
            expected_before_forge,
            "mid-stream rejection must leave the DB at the pre-forge (seq N-1) state exactly"
        );

        // Clean resume: replace the forged envelope with the honest bytes and
        // reconstruct fresh — the follower reaches the full head.
        storage.put(&mid_key, &real_mid_bytes).await.unwrap();
        let resumed_follower = dir.path().join("resumed.db");
        let resumed = reconstruct_fenced_follower(
            &storage,
            "fenced/",
            "fenced",
            &cursor,
            &base_copy,
            &resumed_follower,
        )
        .await
        .expect("resume after replacing the bad envelope");
        assert_eq!(resumed.head_seq, full.head_seq, "resume reaches full head");
        assert_eq!(
            read_items(&resumed_follower),
            full_rows,
            "resumed follower must match the honest full reconstruct exactly"
        );
    }

    #[tokio::test]
    async fn test_owned_replicator_holds_checkpoint_blocker_after_add() {
        // D2: walrust-owned mode must pin the live WAL with a long-running read
        // transaction, so an external TRUNCATE checkpoint cannot restart it.
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("owned.db");
        let conn = rusqlite::Connection::open(&db_path).unwrap();
        conn.execute_batch(
            "PRAGMA journal_mode=WAL; \
             PRAGMA wal_autocheckpoint=0; \
             CREATE TABLE t (id INTEGER PRIMARY KEY); \
             INSERT INTO t VALUES (1);",
        )
        .unwrap();
        drop(conn);

        let storage: Arc<dyn StorageBackend> = Arc::new(TestStorage::new());
        let rep = crate::Replicator::new(storage, "test/", ReplicationConfig::default());
        rep.add("owned", &db_path).await.unwrap();

        // `add()` takes an initial snapshot; the blocker must be reacquired
        // afterwards so a concurrent external checkpoint is rejected.
        let conn2 = rusqlite::Connection::open(&db_path).unwrap();
        conn2
            .busy_timeout(std::time::Duration::from_millis(100))
            .unwrap();
        let (busy, _log, _ckpt): (i64, i64, i64) = conn2
            .query_row("PRAGMA wal_checkpoint(TRUNCATE);", [], |row| {
                Ok((row.get(0)?, row.get(1)?, row.get(2)?))
            })
            .unwrap();
        assert_ne!(
            busy, 0,
            "external TRUNCATE must be blocked while walrust holds the pinned-frame read transaction"
        );
    }
}
