//! Legacy shadow-watch lifecycle helpers shared by the root CLI wrapper.

use crate::legacy_cache::LocalCache;
use crate::legacy_shadow::{ShadowSyncInput, ShadowSyncOutput};
use crate::shadow::ShadowWal;
use anyhow::{Context, Result};
use chrono::Utc;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fs::{self, File};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::time::Duration;

const SHADOW_PROGRESS_FILE: &str = "progress.json";

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct ShadowProgress {
    pub version: u32,
    pub current_txid: u64,
    pub last_snapshot: Option<chrono::DateTime<Utc>>,
    pub db_checksum: Option<u64>,
    pub shadow_sync_generation: u64,
    pub shadow_sync_offset: u64,
    /// Live-WAL read cursor (byte offset into the active WAL) at the last
    /// durable sync. Restored on restart so the shadow resumes reading from
    /// here instead of re-reading (and re-appending) the whole live WAL from
    /// offset 0. `serde(default)` keeps pre-B4 progress records loadable.
    #[serde(default)]
    pub wal_copy_offset: u64,
    /// WAL header salt at `wal_copy_offset`. Lets a restart detect a checkpoint
    /// that occurred while the process was down (salt mismatch => rollover).
    #[serde(default)]
    pub wal_salt: Option<(u32, u32)>,
    /// Running SQLite WAL checksum `(s0, s1)` at `wal_copy_offset`, so the
    /// first post-restart read validates the frame checksum chain per-frame
    /// from the resumed offset instead of skipping validation (B4).
    #[serde(default)]
    pub wal_checksum_chain: Option<(u32, u32)>,
}

pub struct ShadowWatchState {
    pub name: String,
    pub db_path: PathBuf,
    pub wal_path: PathBuf,
    pub current_txid: u64,
    pub last_snapshot: Option<chrono::DateTime<Utc>>,
    pub db_checksum: Option<u64>,
    pub shadow: ShadowWal,
    pub shadow_sync_generation: u64,
    pub shadow_sync_offset: u64,
    pub wal_copy_offset: u64,
}

pub fn shadow_progress_path(shadow_dir: &Path) -> PathBuf {
    shadow_dir.join(SHADOW_PROGRESS_FILE)
}

fn fsync_dir(path: &Path) -> Result<()> {
    File::open(path)
        .with_context(|| format!("failed to open directory {} for fsync", path.display()))?
        .sync_all()
        .with_context(|| format!("failed to fsync directory {}", path.display()))
}

pub fn save_shadow_progress(
    shadow_dir: &Path,
    db_name: &str,
    progress: &ShadowProgress,
) -> Result<()> {
    fs::create_dir_all(shadow_dir).with_context(|| {
        format!(
            "{}: failed to create shadow progress directory {}",
            db_name,
            shadow_dir.display()
        )
    })?;

    let progress_path = shadow_progress_path(shadow_dir);
    let tmp_path = progress_path.with_extension("json.tmp");
    let json = serde_json::to_vec_pretty(progress)
        .with_context(|| format!("{}: failed to serialize shadow progress", db_name))?;

    {
        let mut file = File::create(&tmp_path).with_context(|| {
            format!(
                "{}: failed to create temporary shadow progress file {}",
                db_name,
                tmp_path.display()
            )
        })?;
        file.write_all(&json).with_context(|| {
            format!(
                "{}: failed to write temporary shadow progress file {}",
                db_name,
                tmp_path.display()
            )
        })?;
        file.sync_all().with_context(|| {
            format!(
                "{}: failed to fsync temporary shadow progress file {}",
                db_name,
                tmp_path.display()
            )
        })?;
    }

    fs::rename(&tmp_path, &progress_path).with_context(|| {
        format!(
            "{}: failed to install shadow progress file {}",
            db_name,
            progress_path.display()
        )
    })?;
    fsync_dir(shadow_dir).with_context(|| {
        format!(
            "{}: failed to durably commit shadow progress file {}",
            db_name,
            progress_path.display()
        )
    })?;

    Ok(())
}

pub fn load_shadow_progress(shadow: &ShadowWal, db_name: &str) -> Result<Option<ShadowProgress>> {
    let progress_path = shadow_progress_path(shadow.shadow_dir());
    let data = match fs::read(&progress_path) {
        Ok(data) => data,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(e) => {
            return Err(e).with_context(|| {
                format!(
                    "{}: failed to read shadow progress file {}",
                    db_name,
                    progress_path.display()
                )
            });
        }
    };

    let progress: ShadowProgress = serde_json::from_slice(&data).with_context(|| {
        format!(
            "{}: failed to parse shadow progress file {}",
            db_name,
            progress_path.display()
        )
    })?;

    if progress.version != 1 {
        anyhow::bail!(
            "{}: unsupported shadow progress version {} in {}",
            db_name,
            progress.version,
            progress_path.display()
        );
    }
    if progress.shadow_sync_generation > shadow.generation() {
        anyhow::bail!(
            "{}: shadow progress generation {} is ahead of live generation {} in {}",
            db_name,
            progress.shadow_sync_generation,
            shadow.generation(),
            progress_path.display()
        );
    }

    Ok(Some(progress))
}

pub async fn wait_for_cache_checkpoint_durability(
    cache: &LocalCache,
    db_name: &str,
    required_txid: u64,
    timeout: Duration,
) -> Result<()> {
    let deadline = tokio::time::Instant::now() + timeout;

    loop {
        let failed = cache.failed_uploads();
        if !failed.is_empty() {
            anyhow::bail!(
                "{}: cannot checkpoint shadow WAL; cache upload failures remain: {:?}",
                db_name,
                failed
            );
        }

        let pending = cache.pending_uploads();
        let uploaded_txid = cache.last_uploaded_txid();
        if pending.is_empty() && uploaded_txid >= required_txid {
            return Ok(());
        }

        if tokio::time::Instant::now() >= deadline {
            anyhow::bail!(
                "{}: cannot checkpoint shadow WAL; durable upload confirmation timed out after {:?} (required_txid={}, uploaded_txid={}, pending={:?})",
                db_name,
                timeout,
                required_txid,
                uploaded_txid,
                pending
            );
        }

        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

pub fn shadow_sync_input(state: &ShadowWatchState) -> ShadowSyncInput {
    ShadowSyncInput {
        db_path: state.db_path.clone(),
        name: state.name.clone(),
        current_txid: state.current_txid,
        db_checksum: state.db_checksum,
        generation: state.shadow_sync_generation,
        shadow_sync_offset: state.shadow_sync_offset,
        page_size: state.shadow.page_size(),
        shadow_dir: state.shadow.shadow_dir().to_path_buf(),
    }
}

fn progress_from_state(state: &ShadowWatchState) -> ShadowProgress {
    ShadowProgress {
        version: 1,
        current_txid: state.current_txid,
        last_snapshot: state.last_snapshot,
        db_checksum: state.db_checksum,
        shadow_sync_generation: state.shadow_sync_generation,
        shadow_sync_offset: state.shadow_sync_offset,
        wal_copy_offset: state.wal_copy_offset,
        wal_salt: state.shadow.wal_read_salt(),
        wal_checksum_chain: state.shadow.wal_read_chain(),
    }
}

/// Restore the live-WAL read cursor from a durable progress record so the first
/// post-restart `copy_frames` resumes from the persisted offset with per-frame
/// checksum validation (B4), instead of re-reading and re-appending the whole
/// live WAL from offset 0. Call after `load_shadow_progress`, before the first
/// sync tick.
pub fn restore_read_cursor_from_progress(state: &mut ShadowWatchState, progress: &ShadowProgress) {
    state.wal_copy_offset = progress.wal_copy_offset;
    state
        .shadow
        .restore_read_cursor(progress.wal_salt, progress.wal_checksum_chain);
}

pub fn save_shadow_watch_progress(state: &ShadowWatchState) -> Result<()> {
    save_shadow_progress(
        state.shadow.shadow_dir(),
        &state.name,
        &progress_from_state(state),
    )
}

pub async fn advance_shadow_sync_cursor_if_drained(state: &mut ShadowWatchState) -> Result<()> {
    loop {
        let live_generation = state.shadow.generation();
        if state.shadow_sync_generation >= live_generation {
            return Ok(());
        }

        let segments = state
            .shadow
            .list_segments(state.shadow_sync_generation)
            .await
            .with_context(|| {
                format!(
                    "{}: failed to list shadow generation {}",
                    state.name, state.shadow_sync_generation
                )
            })?;
        let generation_size: u64 = segments.iter().map(|segment| segment.size).sum();
        if state.shadow_sync_offset < generation_size {
            return Ok(());
        }

        tracing::debug!(
            "{}: shadow generation {} fully synced ({} bytes); advancing upload cursor to generation {}",
            state.name,
            state.shadow_sync_generation,
            generation_size,
            state.shadow_sync_generation + 1
        );
        state.shadow_sync_generation += 1;
        state.shadow_sync_offset = 0;
    }
}

pub fn apply_shadow_sync_output_to_state(state: &mut ShadowWatchState, output: &ShadowSyncOutput) {
    if output.frame_count == 0 {
        return;
    }

    state.shadow_sync_offset = output.new_shadow_sync_offset;
    state.current_txid = output.new_current_txid;
    state.db_checksum = output.new_db_checksum;
}

pub async fn apply_shadow_sync_result_to_state(
    state: &mut ShadowWatchState,
    output: &ShadowSyncOutput,
) -> Result<()> {
    apply_shadow_sync_output_to_state(state, output);
    advance_shadow_sync_cursor_if_drained(state).await?;
    save_shadow_watch_progress(state)?;
    Ok(())
}

pub async fn apply_shadow_sync_results_strict(
    db_states: &mut HashMap<PathBuf, ShadowWatchState>,
    results: Vec<Result<ShadowSyncOutput>>,
) -> Result<()> {
    let mut first_error = None;

    for result in results {
        match result {
            Ok(output) => {
                if let Some(state) = db_states.get_mut(&output.db_path) {
                    apply_shadow_sync_result_to_state(state, &output).await?;
                }
            }
            Err(e) => {
                if first_error.is_none() {
                    first_error = Some(e);
                }
            }
        }
    }

    match first_error {
        Some(e) => Err(e).context("final shadow sync failed"),
        None => Ok(()),
    }
}
