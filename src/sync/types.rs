use serde::{Deserialize, Serialize};

/// State for sync trigger tracking.
pub(crate) struct TriggerState {
    /// WAL frames synced since last snapshot.
    pub(crate) frames_since_snapshot: u64,
    /// When the first change was detected.
    pub(crate) first_change_time: Option<std::time::Instant>,
    /// When the last WAL activity occurred.
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

pub(crate) use walrust_core::shadow_watch::ShadowWatchState as ShadowDbState;

/// State persisted next to a native read replica.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct ReplicaState {
    pub(crate) source: String,
    pub(crate) stream_digest: String,
    pub(crate) lineage_id: String,
    pub(crate) current_txid: u64,
    pub(crate) last_updated: String,
}
