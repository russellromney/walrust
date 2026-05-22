use chrono::Utc;
use serde::{Deserialize, Serialize};
use std::path::PathBuf;
use std::sync::Arc;
use tokio::sync::mpsc;

use crate::cache::LocalCache;
use crate::config::SyncConfig;
use crate::shadow::ShadowWal;

/// State for a single watched database
pub(crate) struct DbState {
    /// Database name (filename without extension)
    pub(crate) name: String,
    /// Path to main db file
    pub(crate) db_path: PathBuf,
    /// Path to WAL file
    pub(crate) wal_path: PathBuf,
    /// Current WAL sync position
    pub(crate) wal_offset: u64,
    /// WAL generation (increments on checkpoint)
    pub(crate) wal_generation: u64,
    /// Current transaction ID (for LTX files)
    pub(crate) current_txid: u64,
    /// Last snapshot time
    pub(crate) last_snapshot: Option<chrono::DateTime<Utc>>,
    /// Current database checksum (for incremental LTX chaining)
    /// Computed from database on startup, updated after each LTX upload
    pub(crate) db_checksum: Option<u64>,
    /// WAL header salt of the generation `wal_offset` indexes into. Comparing
    /// salt (not just size) detects an in-place WAL reset that re-salts the
    /// header at the same/larger size, so the new prefix is not skipped.
    pub(crate) wal_salt: Option<(u32, u32)>,
    /// Running SQLite WAL checksum at `wal_offset`; seeds per-frame validation
    /// so a torn tail frame is rejected rather than shipped.
    pub(crate) wal_checksum_chain: Option<(u32, u32)>,
}

/// Input for concurrent WAL sync (immutable snapshot of state)
#[derive(Clone)]
pub(crate) struct SyncInput {
    pub(crate) db_path: PathBuf,
    pub(crate) name: String,
    pub(crate) wal_path: PathBuf,
    pub(crate) wal_offset: u64,
    pub(crate) wal_generation: u64,
    pub(crate) current_txid: u64,
    pub(crate) db_checksum: Option<u64>,
    pub(crate) wal_salt: Option<(u32, u32)>,
    pub(crate) wal_checksum_chain: Option<(u32, u32)>,
}

impl From<&DbState> for SyncInput {
    fn from(state: &DbState) -> Self {
        Self {
            db_path: state.db_path.clone(),
            name: state.name.clone(),
            wal_path: state.wal_path.clone(),
            wal_offset: state.wal_offset,
            wal_generation: state.wal_generation,
            current_txid: state.current_txid,
            db_checksum: state.db_checksum,
            wal_salt: state.wal_salt,
            wal_checksum_chain: state.wal_checksum_chain,
        }
    }
}

/// Output from concurrent WAL sync (changes to apply to state)
pub(crate) struct SyncOutput {
    pub(crate) db_path: PathBuf,
    pub(crate) frame_count: u64,
    pub(crate) new_wal_offset: u64,
    pub(crate) new_current_txid: u64,
    pub(crate) new_db_checksum: Option<u64>,
    /// If checkpoint was detected, new generation
    pub(crate) checkpoint_detected: bool,
    pub(crate) new_wal_generation: u64,
    pub(crate) new_wal_salt: Option<(u32, u32)>,
    pub(crate) new_wal_checksum_chain: Option<(u32, u32)>,
}

/// Entry in the manifest tracking LTX files
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
    /// This is the post_apply_checksum from the most recent LTX file
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_checksum: Option<u64>,
}

/// State for sync trigger tracking
pub(crate) struct TriggerState {
    /// WAL frames synced since last snapshot
    pub(crate) frames_since_snapshot: u64,
    /// When the first change was detected (for max_interval)
    pub(crate) first_change_time: Option<std::time::Instant>,
    /// When the last WAL activity occurred (for on_idle)
    pub(crate) last_wal_activity: Option<std::time::Instant>,
}

impl Default for TriggerState {
    fn default() -> Self {
        Self {
            frames_since_snapshot: 0,
            first_change_time: None,
            last_wal_activity: None,
        }
    }
}

/// State for shadow WAL mode databases
pub(crate) struct ShadowDbState {
    /// Base database state
    pub(crate) name: String,
    pub(crate) db_path: PathBuf,
    pub(crate) wal_path: PathBuf,
    pub(crate) current_txid: u64,
    pub(crate) last_snapshot: Option<chrono::DateTime<Utc>>,
    pub(crate) db_checksum: Option<u64>,
    /// Shadow WAL manager (owns the checkpoint blocker)
    pub(crate) shadow: ShadowWal,
    /// Offset within shadow segments for upload tracking
    pub(crate) shadow_sync_offset: u64,
    /// WAL offset for copy_frames tracking
    pub(crate) wal_copy_offset: u64,
}

/// Input for concurrent shadow sync
#[derive(Clone)]
pub(crate) struct ShadowSyncInput {
    pub(crate) db_path: PathBuf,
    pub(crate) name: String,
    pub(crate) current_txid: u64,
    pub(crate) db_checksum: Option<u64>,
    pub(crate) generation: u64,
    pub(crate) shadow_sync_offset: u64,
    pub(crate) page_size: u32,
    pub(crate) shadow_dir: PathBuf,
}

/// Output from concurrent shadow sync
#[derive(Debug)]
pub(crate) struct ShadowSyncOutput {
    pub(crate) db_path: PathBuf,
    pub(crate) frame_count: u64,
    pub(crate) new_shadow_sync_offset: u64,
    pub(crate) new_current_txid: u64,
    pub(crate) new_db_checksum: Option<u64>,
}

/// State for independent database task
pub(crate) struct DbTaskState {
    /// Database state (owned, not shared)
    pub(crate) db_state: DbState,
    /// Trigger state for snapshots
    pub(crate) trigger_state: TriggerState,
    /// Per-DB sync config
    pub(crate) sync_config: SyncConfig,
}

/// Optional cache state for disk-based upload queue
pub(crate) struct CacheState {
    /// Local disk cache for LTX files
    pub(crate) cache: Arc<LocalCache>,
    /// Shadow WAL for checkpoint-safe frame copying
    pub(crate) shadow: Arc<tokio::sync::Mutex<ShadowWal>>,
    /// Channel to send upload notifications to uploader task
    pub(crate) upload_tx: mpsc::Sender<crate::uploader::UploadMessage>,
    /// Cache config for cleanup parameters
    pub(crate) retention_duration: chrono::Duration,
    pub(crate) max_cache_size: u64,
}

/// State for replica mode
#[derive(Serialize, Deserialize)]
pub(crate) struct ReplicaState {
    pub(crate) current_txid: u64,
    pub(crate) last_updated: String,
}
