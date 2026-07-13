//! Legacy shadow-watch lifecycle helpers shared by the root CLI wrapper.

use crate::errors::WalrustError;
use crate::legacy_cache::LocalCache;
use crate::legacy_shadow::{ShadowSyncInput, ShadowSyncOutput};
use crate::shadow::ShadowWal;
use crate::wal::{
    validate_header_checksum, verify_frame_checksum, FRAME_HEADER_SIZE, WAL_MAGIC_BE, WAL_MAGIC_LE,
};
use anyhow::{anyhow, Context, Result};
use chrono::Utc;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fs::{self, File};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::time::Duration;
use tokio::fs::File as AsyncFile;
use tokio::io::{AsyncReadExt, AsyncSeekExt, SeekFrom};

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
    /// CLI shadow watch's explicitly owned checkpoint blocker. This connection
    /// lives with the per-database watch state rather than the file tailer.
    pub checkpoint_blocker: Option<rusqlite::Connection>,
    /// Long-lived connection used only for `PRAGMA data_version`. It detects
    /// app commits across controlled release/reacquire windows and is replaced
    /// immediately before the blocker at operation-boundary handoffs.
    pub data_version_monitor: Option<rusqlite::Connection>,
    pub shadow_sync_generation: u64,
    pub shadow_sync_offset: u64,
    pub wal_copy_offset: u64,
}

/// Replace CLI shadow watch's checkpoint blocker after all one-shot SQLite
/// connections for a controlled operation have closed.
///
/// POSIX advisory locks are process-scoped. The old connection must close
/// before `open_checkpoint_blocker` writes and pins a fresh committed heartbeat;
/// this proven primitive is the final source-DB handle opened in the process.
pub fn rearm_checkpoint_blocker(state: &mut ShadowWatchState) -> Result<()> {
    if let Some(old_blocker) = state.checkpoint_blocker.take() {
        if !old_blocker.is_autocommit() {
            old_blocker.execute_batch("ROLLBACK;")?;
        }
        drop(old_blocker);
    }
    let mut last_open_error = None;
    for attempt in 1..=3 {
        state.checkpoint_blocker = match ShadowWal::open_checkpoint_blocker(&state.db_path) {
            Ok(blocker) => Some(blocker),
            Err(error) => {
                tracing::error!(
                    "{}: checkpoint blocker open failed (attempt {}/3): {}",
                    state.name,
                    attempt,
                    error
                );
                last_open_error = Some(error);
                continue;
            }
        };
        if checkpoint_blocker_heartbeat_is_live(state)? {
            tracing::info!("{}: CLI checkpoint blocker rearmed", state.name);
            return Ok(());
        }
        tracing::error!(
            "{}: checkpoint blocker heartbeat was reset in the release/reacquire window (attempt {}/3)",
            state.name,
            attempt
        );
        let failed_blocker = state
            .checkpoint_blocker
            .take()
            .expect("blocker was assigned above");
        if !failed_blocker.is_autocommit() {
            failed_blocker.execute_batch("ROLLBACK;")?;
        }
        drop(failed_blocker);
    }
    Err(anyhow!(
        "{}: checkpoint blocker heartbeat was reset during all rearm attempts{}",
        state.name,
        last_open_error
            .as_ref()
            .map(|error| format!(": {error}"))
            .unwrap_or_default()
    ))
}

pub fn checkpoint_data_version(state: &ShadowWatchState) -> Result<i64> {
    let monitor = state
        .data_version_monitor
        .as_ref()
        .ok_or_else(|| anyhow!("{}: CLI data_version monitor was not held", state.name))?;
    Ok(monitor.query_row("PRAGMA data_version;", [], |row| row.get(0))?)
}

/// Replace the monitor before a final blocker handoff so the blocker is the
/// last SQLite connection operation against the source database.
pub fn refresh_checkpoint_data_version_monitor(state: &mut ShadowWatchState) -> Result<()> {
    drop(state.data_version_monitor.take());
    let monitor = rusqlite::Connection::open(&state.db_path)?;
    monitor.busy_timeout(Duration::from_secs(5))?;
    state.data_version_monitor = Some(monitor);
    Ok(())
}

/// Verify that the replacement blocker's heartbeat is still a live frame in
/// the current WAL generation. Once this returns true, the active read mark
/// prevents a later reset. A checkpoint that slipped between heartbeat COMMIT
/// and BEGIN removes that frame (or changes the WAL salt), so callers must
/// retry/re-anchor rather than trust the gap.
pub fn checkpoint_blocker_heartbeat_is_live(state: &ShadowWatchState) -> Result<bool> {
    let blocker = state
        .checkpoint_blocker
        .as_ref()
        .ok_or_else(|| anyhow!("{}: CLI checkpoint blocker was not held", state.name))?;
    let root_page: u32 = blocker.query_row(
        "SELECT rootpage FROM sqlite_schema WHERE name = '_walrust_seq' AND type = 'table'",
        [],
        |row| row.get(0),
    )?;

    let wal = match std::fs::read(&state.wal_path) {
        Ok(wal) => wal,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(false),
        Err(error) => return Err(error.into()),
    };
    if wal.len() < 32 {
        return Ok(false);
    }
    let header: [u8; 32] = wal[0..32].try_into().expect("32-byte WAL header");
    let magic = u32::from_be_bytes(header[0..4].try_into().expect("four-byte WAL magic"));
    if magic != WAL_MAGIC_LE && magic != WAL_MAGIC_BE {
        return Err(anyhow!(
            "{}: invalid WAL magic while verifying checkpoint blocker: {magic:#x}",
            state.name
        ));
    }
    let page_size = u32::from_be_bytes(header[8..12].try_into().expect("four-byte page size"));
    if page_size < 512 || page_size > 65_536 || !page_size.is_power_of_two() {
        return Err(anyhow!(
            "{}: invalid WAL page size while verifying checkpoint blocker: {}",
            state.name,
            page_size
        ));
    }
    let big_endian = magic == WAL_MAGIC_BE;
    let mut checksum = validate_header_checksum(&header, big_endian)?;
    let salt1 = &header[16..20];
    let salt2 = &header[20..24];
    let frame_size = 24usize + page_size as usize;
    let mut root_frame_pending_commit = false;
    for frame in wal[32..].chunks_exact(frame_size) {
        let frame_header: [u8; 24] = frame[0..24].try_into().expect("24-byte frame header");
        if &frame_header[8..12] != salt1 || &frame_header[12..16] != salt2 {
            return Err(anyhow!(
                "{}: WAL frame salt changed while verifying checkpoint blocker",
                state.name
            ));
        }
        checksum = verify_frame_checksum(
            checksum,
            &frame_header,
            &frame[24..24 + page_size as usize],
            big_endian,
        )
        .ok_or_else(|| {
            anyhow!(
                "{}: WAL frame checksum failed while verifying checkpoint blocker",
                state.name
            )
        })?;
        if u32::from_be_bytes(
            frame_header[0..4]
                .try_into()
                .expect("four-byte page number"),
        ) == root_page
        {
            root_frame_pending_commit = true;
        }
        let db_size = u32::from_be_bytes(
            frame_header[4..8]
                .try_into()
                .expect("four-byte commit size"),
        );
        if db_size != 0 {
            if root_frame_pending_commit {
                return Ok(true);
            }
            root_frame_pending_commit = false;
        }
    }
    Ok(false)
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

/// Read the `db_size` field of the last frame in a shadow generation.
///
/// Returns `Ok(None)` if the generation has no segment data. A non-zero
/// `db_size` marks a commit frame; zero means the generation ends mid-
/// transaction.
async fn last_frame_db_size_in_generation(
    shadow: &ShadowWal,
    generation: u64,
) -> Result<Option<u32>> {
    let segments = shadow.list_segments(generation).await?;
    let Some(last) = segments.last() else {
        return Ok(None);
    };
    if last.size == 0 {
        return Ok(None);
    }

    let frame_size = FRAME_HEADER_SIZE + shadow.page_size() as u64;
    if last.size < frame_size {
        anyhow::bail!(
            "shadow segment {:?} is smaller than one frame ({} < {})",
            last.path,
            last.size,
            frame_size
        );
    }

    let header_offset = last.size - frame_size;
    let mut file = AsyncFile::open(&last.path).await?;
    file.seek(SeekFrom::Start(header_offset)).await?;
    let mut header = [0u8; FRAME_HEADER_SIZE as usize];
    file.read_exact(&mut header).await?;

    let db_size = u32::from_be_bytes([header[4], header[5], header[6], header[7]]);
    Ok(Some(db_size))
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

        // The shadow sync cursor assumes each generation ends at a commit
        // boundary. If we have drained a generation whose tail frame is not a
        // commit, something is wrong; fail loudly instead of silently stalling.
        if generation_size > 0 {
            let tail_db_size =
                last_frame_db_size_in_generation(&state.shadow, state.shadow_sync_generation)
                    .await
                    .with_context(|| {
                        format!(
                            "{}: failed to read tail frame of shadow generation {}",
                            state.name, state.shadow_sync_generation
                        )
                    })?;
            if tail_db_size.map(|v| v == 0).unwrap_or(true) {
                return Err(WalrustError::ShadowGenerationNotAtCommitBoundary(format!(
                    "{}: shadow generation {} tail is not at a commit boundary (last frame db_size={:?})",
                    state.name,
                    state.shadow_sync_generation,
                    tail_db_size
                ))
                .into());
            }
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

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    fn write_shadow_frame(page_number: u32, db_size: u32, page_size: u32) -> Vec<u8> {
        let mut frame = Vec::with_capacity((FRAME_HEADER_SIZE + page_size as u64) as usize);
        let mut header = [0u8; FRAME_HEADER_SIZE as usize];
        header[0..4].copy_from_slice(&page_number.to_be_bytes());
        header[4..8].copy_from_slice(&db_size.to_be_bytes());
        frame.extend_from_slice(&header);
        frame.extend_from_slice(&vec![0u8; page_size as usize]);
        frame
    }

    async fn shadow_state_with_generation_tail(
        dir: &tempfile::TempDir,
        tail_db_size: u32,
    ) -> ShadowWatchState {
        let db_path = dir.path().join("test.db");
        let conn = rusqlite::Connection::open(&db_path).unwrap();
        conn.execute_batch(
            "PRAGMA journal_mode=WAL; \
             PRAGMA wal_autocheckpoint=0; \
             CREATE TABLE t (id INTEGER PRIMARY KEY); \
             INSERT INTO t VALUES (1);",
        )
        .unwrap();
        drop(conn);

        let shadow = ShadowWal::new(&db_path).await.unwrap();
        let page_size = shadow.page_size();
        let shadow_dir = shadow.shadow_dir().to_path_buf();
        drop(shadow);

        let gen0_path = shadow_dir.join(format!("{:016x}-{:016x}.wal", 0u64, 0u64));
        let gen1_path = shadow_dir.join(format!("{:016x}-{:016x}.wal", 1u64, 0u64));

        // Generation 0 ends with the requested tail frame.
        let gen0_segment = write_shadow_frame(1, tail_db_size, page_size);
        tokio::fs::write(&gen0_path, &gen0_segment).await.unwrap();

        // A later generation exists so the drain cursor tries to advance past gen 0.
        let gen1_segment = write_shadow_frame(1, 1, page_size);
        tokio::fs::write(&gen1_path, &gen1_segment).await.unwrap();

        let shadow = ShadowWal::new(&db_path).await.unwrap();
        ShadowWatchState {
            name: "test".to_string(),
            db_path,
            wal_path: shadow.shadow_dir().join("test.db-wal"),
            current_txid: 0,
            last_snapshot: None,
            db_checksum: None,
            shadow,
            checkpoint_blocker: None,
            data_version_monitor: None,
            shadow_sync_generation: 0,
            shadow_sync_offset: gen0_segment.len() as u64,
            wal_copy_offset: 0,
        }
    }

    #[tokio::test]
    async fn test_advance_cursor_rejects_generation_tail_not_at_commit_boundary() {
        let dir = tempdir().unwrap();
        let mut state = shadow_state_with_generation_tail(&dir, 0).await;

        let err = advance_shadow_sync_cursor_if_drained(&mut state)
            .await
            .unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("not at a commit boundary"),
            "expected commit-boundary error, got: {msg}"
        );
        assert!(
            msg.contains("db_size=Some(0)"),
            "error should report the offending db_size: {msg}"
        );
    }

    #[tokio::test]
    async fn test_advance_cursor_accepts_generation_tail_at_commit_boundary() {
        let dir = tempdir().unwrap();
        let mut state = shadow_state_with_generation_tail(&dir, 3).await;

        advance_shadow_sync_cursor_if_drained(&mut state)
            .await
            .expect("commit boundary should allow advancement");
        assert_eq!(state.shadow_sync_generation, 1);
        assert_eq!(state.shadow_sync_offset, 0);
    }
}
