//! Shadow WAL implementation for walrust
//!
//! The shadow WAL provides a staging area for WAL frames that decouples
//! the upload process from SQLite's active WAL file. This matches Litestream's
//! architecture and provides several benefits:
//!
//! 1. No file contention with SQLite during uploads
//! 2. Checkpoint control - we prevent auto-checkpoints
//! 3. Preserved history - shadow keeps frames even after checkpoint
//! 4. Decoupled I/O - upload doesn't block write path

use anyhow::{anyhow, Result};
use rusqlite::Connection;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;
use tokio::fs::{self, File, OpenOptions};
use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt, SeekFrom};
use tokio::sync::Mutex;

use crate::wal::{self, ParsedFrame, FRAME_HEADER_SIZE};

/// Hex width for both the generation and index components of a shadow segment
/// filename. Both are `u64`, so 16 hex digits keeps lexical order == numeric
/// order across the full range.
pub(crate) const SEGMENT_HEX_WIDTH: usize = 16;
const DURABLE_TAIL_VERSION: u32 = 1;
const DURABLE_TAIL_FILE: &str = "durable-tail-v1.json";

#[derive(Debug, Clone, Serialize, Deserialize)]
struct DurableShadowTail {
    version: u32,
    segments: BTreeMap<String, u64>,
}

/// Format a shadow segment filename: `{generation:016x}-{index:016x}.wal`.
fn format_segment_name(generation: u64, index: u64) -> String {
    format!(
        "{:0width$x}-{:0width$x}.wal",
        generation,
        index,
        width = SEGMENT_HEX_WIDTH
    )
}

/// Read the authoritative page size from a SQLite database file header.
///
/// The page size lives at byte offset 16 (big-endian u16) of the main DB file,
/// where the special value `1` means 65536. Returns `None` if the file is
/// missing or is not a SQLite database (e.g. before the first write), in which
/// case callers fall back to the WAL-header-derived page size.
fn db_header_page_size(db_path: &Path) -> Option<u32> {
    use std::io::Read;
    let mut file = std::fs::File::open(db_path).ok()?;
    let mut header = [0u8; 18];
    file.read_exact(&mut header).ok()?;
    if &header[0..16] != b"SQLite format 3\0" {
        return None;
    }
    let raw = u16::from_be_bytes([header[16], header[17]]);
    Some(if raw == 1 { 65_536 } else { raw as u32 })
}

pub(crate) fn ensure_connection_in_wal_mode(conn: &Connection, db_path: &Path) -> Result<()> {
    let mode: String = conn.query_row("PRAGMA journal_mode", [], |row| row.get(0))?;
    if mode.eq_ignore_ascii_case("wal") {
        Ok(())
    } else {
        Err(anyhow!(
            "{}: SQLite journal_mode is '{}', expected WAL; shadow replication cannot continue",
            db_path.display(),
            mode
        ))
    }
}

/// Shadow WAL manager for a single database
pub struct ShadowWal {
    /// Path to the original database
    db_path: PathBuf,
    /// Path to the shadow WAL directory
    shadow_dir: PathBuf,
    /// Current shadow WAL generation (increments on checkpoint)
    generation: u64,
    /// Current shadow WAL segment index within generation
    segment_index: u64,
    /// Bytes written to current segment
    segment_offset: u64,
    /// Page size from WAL header
    page_size: u32,
    /// Read connection that prevents auto-checkpoint
    /// Wrapped in Option<Arc<Mutex>> so we can close it for checkpoint
    checkpoint_blocker: Option<Arc<Mutex<Connection>>>,
    /// Salt values from current WAL header (used to detect checkpoint)
    wal_salt: (u32, u32),
    /// Running WAL checksum chain of the last frame copied into the shadow. Used
    /// to validate incremental reads (torn-tail / stale-frame rejection, B4).
    /// `None` means "seed from the WAL header on the next read" — a fresh start
    /// or immediately after a generation rollover.
    wal_chain: Option<(u32, u32)>,
    /// Whether a real WAL header has been observed yet. Until then `page_size`
    /// and `wal_salt` hold defaults; the first real header seeds them without
    /// being mistaken for a checkpoint rollover (B5).
    header_seeded: bool,
    /// Whether lifecycle states may be probed by opening a one-shot SQLite
    /// connection. Owned/library mode permits this because `ShadowWal` also
    /// owns and can restore its blocker. CLI file-tailer mode must remain
    /// connection-free: closing any SQLite handle in that process can erase
    /// the separately owned blocker's process-scoped POSIX locks.
    probe_wal_mode_on_lifecycle_state: bool,
    /// Startup removed bytes beyond the last fsynced durable-tail marker.
    /// The caller must either recopy them from the pinned WAL or re-anchor.
    discarded_unproven_tail: bool,
    /// A failed rollback means this process can no longer prove the append
    /// boundary. Refuse every later write until restart recovery succeeds.
    append_poisoned: Option<String>,
    #[cfg(test)]
    append_failure: Option<TestAppendFailure>,
}

#[cfg(test)]
#[derive(Debug, Clone, Copy)]
enum TestAppendFailure {
    Write,
    Sync,
    Marker,
}

/// A segment file in the shadow WAL
#[derive(Debug, Clone)]
pub struct ShadowSegment {
    /// Generation number
    pub generation: u64,
    /// Segment index within generation
    pub index: u64,
    /// Path to segment file
    pub path: PathBuf,
    /// Size in bytes
    pub size: u64,
}

impl ShadowWal {
    /// Create a new shadow WAL for a database
    pub async fn new(db_path: &Path) -> Result<Self> {
        Self::new_inner(db_path, true).await
    }

    /// Create a shadow WAL file tailer without owning a checkpoint blocker.
    /// CLI shadow watch uses this constructor because its per-database state
    /// owns the blocker connection explicitly.
    pub async fn new_without_checkpoint_blocker(db_path: &Path) -> Result<Self> {
        Self::new_inner(db_path, false).await
    }

    async fn new_inner(db_path: &Path, hold_checkpoint_blocker: bool) -> Result<Self> {
        let shadow_dir = Self::shadow_dir_for(db_path);

        // Create shadow directory if it doesn't exist
        fs::create_dir_all(&shadow_dir).await?;

        // Read current WAL header to get page size and salt
        let wal_path = db_path.with_extension("db-wal");
        let (page_size, salt1, salt2, header_seeded) = match wal::read_header(&wal_path).await? {
            Some(header) => (header.page_size, header.salt1, header.salt2, true),
            None => (4096, 0, 0, false), // Defaults until a WAL header appears (B5)
        };

        // Find highest existing generation
        let mut generation = Self::find_latest_generation(&shadow_dir)
            .await?
            .unwrap_or(0);

        // Recover strictly to the atomically fsynced marker. Alignment alone
        // cannot prove a tail survived power loss; markerless/unowned segments
        // force a fresh source-cursor generation and full snapshot boundary.
        let frame_page_size = db_header_page_size(db_path).unwrap_or(page_size);
        let (discarded_unproven_tail, reset_generation) =
            Self::recover_to_durable_tail(&shadow_dir, frame_page_size).await?;
        if reset_generation {
            // Never reuse the logical offset domain whose tail was discarded.
            // The watch layer resets its source cursor to this generation only
            // through the required full-snapshot boundary.
            generation = generation.checked_add(1).ok_or_else(|| {
                anyhow!("shadow generation exhausted while rotating unproven markerless tail")
            })?;
        }
        let mut segment_offset = 0u64;
        let mut entries = fs::read_dir(&shadow_dir).await?;
        while let Some(entry) = entries.next_entry().await? {
            let name = entry.file_name();
            let text = name.to_string_lossy();
            let Some((file_generation, _)) = text.trim_end_matches(".wal").split_once('-') else {
                continue;
            };
            if text.ends_with(".wal")
                && u64::from_str_radix(file_generation, 16).ok() == Some(generation)
            {
                segment_offset = segment_offset.saturating_add(entry.metadata().await?.len());
            }
        }

        let checkpoint_blocker = if hold_checkpoint_blocker {
            Some(Arc::new(Mutex::new(Self::open_checkpoint_blocker(
                db_path,
            )?)))
        } else {
            None
        };

        Ok(Self {
            db_path: db_path.to_path_buf(),
            shadow_dir,
            generation,
            segment_index: 0,
            segment_offset,
            page_size,
            checkpoint_blocker,
            wal_salt: (salt1, salt2),
            wal_chain: None,
            header_seeded,
            probe_wal_mode_on_lifecycle_state: hold_checkpoint_blocker,
            discarded_unproven_tail,
            append_poisoned: None,
            #[cfg(test)]
            append_failure: None,
        })
    }

    /// Get the shadow directory path for a database
    pub fn shadow_dir_for(db_path: &Path) -> PathBuf {
        let parent = db_path.parent().unwrap_or(Path::new("."));
        let db_name = db_path.file_stem().unwrap_or_default();
        parent.join(format!(".walrust-{}", db_name.to_string_lossy()))
    }

    /// Open a read connection that prevents SQLite from auto-checkpointing.
    /// Also used by walrust-owned replication to pin the live WAL (D2).
    pub fn open_checkpoint_blocker(db_path: &Path) -> Result<Connection> {
        let conn = Connection::open(db_path)?;

        Self::write_checkpoint_heartbeat(&conn, db_path)?;
        Self::begin_checkpoint_blocker(&conn, db_path)?;

        tracing::debug!("Opened checkpoint blocker for {}", db_path.display());

        Ok(conn)
    }

    /// Commit the frame a later blocker transaction will pin.
    ///
    /// Controlled checkpoint handoffs call this on the long-lived
    /// `data_version` monitor. A connection does not advance its own
    /// `PRAGMA data_version`, so any observed change still proves that an
    /// application connection committed during the unblocked window.
    pub fn write_checkpoint_heartbeat(conn: &Connection, db_path: &Path) -> Result<()> {
        ensure_connection_in_wal_mode(conn, db_path)?;

        Self::enable_persistent_wal(conn, db_path)?;

        // Disable auto-checkpoint on this connection without changing journal_mode.
        conn.execute_batch(
            "
            PRAGMA busy_timeout=5000;
            PRAGMA wal_autocheckpoint=0;
            CREATE TABLE IF NOT EXISTS _walrust_seq (
                id INTEGER PRIMARY KEY CHECK (id = 1),
                value INTEGER NOT NULL
            );
            INSERT INTO _walrust_seq (id, value)
            VALUES (1, 1)
            ON CONFLICT(id) DO UPDATE SET value = value + 1;
            ",
        )?;

        Ok(())
    }

    /// Open and pin the heartbeat already committed by another retained
    /// connection. This deliberately performs no write of its own.
    pub fn open_checkpoint_blocker_after_heartbeat(db_path: &Path) -> Result<Connection> {
        let conn = Connection::open(db_path)?;
        ensure_connection_in_wal_mode(&conn, db_path)?;
        Self::enable_persistent_wal(&conn, db_path)?;
        conn.execute_batch("PRAGMA busy_timeout=5000; PRAGMA wal_autocheckpoint=0;")?;
        Self::begin_checkpoint_blocker(&conn, db_path)?;
        tracing::debug!("Opened checkpoint blocker for {}", db_path.display());
        Ok(conn)
    }

    fn begin_checkpoint_blocker(conn: &Connection, _db_path: &Path) -> Result<()> {
        // Pin a real WAL frame. Reading sqlite_master can leave the blocker at
        // read-mark 0, which does not prevent walRestartLog on later frames.
        conn.execute_batch("BEGIN DEFERRED;")?;
        let _: i64 = conn.query_row("SELECT value FROM _walrust_seq WHERE id = 1", [], |row| {
            row.get(0)
        })?;
        Ok(())
    }

    /// Enable SQLite's connection-level persistent-WAL file control on every
    /// long-lived watcher handle for a source database.
    pub fn enable_persistent_wal(conn: &Connection, db_path: &Path) -> Result<()> {
        let mut persist_wal: std::os::raw::c_int = 1;
        let rc = unsafe {
            rusqlite::ffi::sqlite3_file_control(
                conn.handle(),
                b"main\0".as_ptr().cast(),
                rusqlite::ffi::SQLITE_FCNTL_PERSIST_WAL,
                (&mut persist_wal as *mut std::os::raw::c_int).cast(),
            )
        };
        if rc != rusqlite::ffi::SQLITE_OK {
            return Err(anyhow!(
                "failed to enable SQLITE_FCNTL_PERSIST_WAL for {}: SQLite result {}",
                db_path.display(),
                rc
            ));
        }
        Ok(())
    }

    async fn ensure_database_in_wal_mode(db_path: &Path) -> Result<()> {
        let db_path = db_path.to_path_buf();
        tokio::task::spawn_blocking(move || -> Result<()> {
            let conn = Connection::open(&db_path)?;
            ensure_connection_in_wal_mode(&conn, &db_path)
        })
        .await?
    }

    /// Find the latest generation number in the shadow directory
    async fn find_latest_generation(shadow_dir: &Path) -> Result<Option<u64>> {
        let mut max_gen: Option<u64> = None;

        if !shadow_dir.exists() {
            return Ok(None);
        }

        let mut entries = fs::read_dir(shadow_dir).await?;
        while let Some(entry) = entries.next_entry().await? {
            let name = entry.file_name();
            let name_str = name.to_string_lossy();

            // Segment files are named: {generation:08x}-{index:08x}.wal
            if name_str.ends_with(".wal") {
                if let Some(gen_str) = name_str.split('-').next() {
                    if let Ok(gen) = u64::from_str_radix(gen_str, 16) {
                        max_gen = Some(max_gen.map_or(gen, |m| m.max(gen)));
                    }
                }
            }
        }

        Ok(max_gen)
    }

    /// Recover only the prefix whose marker was atomically persisted after the
    /// segment and directory fsync. A frame-aligned tail is not evidence of
    /// durability: it may have been fully buffered but never reached storage.
    async fn recover_to_durable_tail(shadow_dir: &Path, _page_size: u32) -> Result<(bool, bool)> {
        let marker_path = shadow_dir.join(DURABLE_TAIL_FILE);
        let marker = match fs::read(&marker_path).await {
            Ok(bytes) => {
                let marker: DurableShadowTail = serde_json::from_slice(&bytes)?;
                if marker.version != DURABLE_TAIL_VERSION {
                    return Err(anyhow!(
                        "unsupported shadow durable-tail version {}",
                        marker.version
                    ));
                }
                marker
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                // A pre-marker shadow directory has no durable proof for even
                // a frame-aligned tail. Never bless those bytes by fsyncing
                // them during upgrade: remove the entire unproven segment set,
                // install an empty marker, and force the caller through a full
                // native snapshot boundary. Any already-admitted HADBP object
                // remains authoritative in the spool journal.
                let mut discarded = false;
                let mut entries = fs::read_dir(shadow_dir).await?;
                while let Some(entry) = entries.next_entry().await? {
                    let name = entry.file_name().to_string_lossy().into_owned();
                    if !name.ends_with(".wal") {
                        continue;
                    }
                    fs::remove_file(entry.path()).await?;
                    discarded = true;
                }
                Self::fsync_dir(shadow_dir).await?;
                let marker = DurableShadowTail {
                    version: DURABLE_TAIL_VERSION,
                    segments: BTreeMap::new(),
                };
                Self::persist_durable_tail(shadow_dir, &marker).await?;
                return Ok((discarded, discarded));
            }
            Err(error) => return Err(error.into()),
        };

        let mut discarded = false;
        let mut reset_generation = false;
        let mut entries = fs::read_dir(shadow_dir).await?;
        let mut seen = BTreeMap::new();
        while let Some(entry) = entries.next_entry().await? {
            let name = entry.file_name().to_string_lossy().into_owned();
            if !name.ends_with(".wal") {
                continue;
            }
            let Some(durable) = marker.segments.get(&name).copied() else {
                // Either append crashed before publishing the durable marker,
                // or cleanup committed its marker before unlinking the old
                // segment. In both cases the marker is authoritative.
                fs::remove_file(entry.path()).await?;
                discarded = true;
                reset_generation = true;
                continue;
            };
            let actual = entry.metadata().await?.len();
            if actual < durable {
                return Err(anyhow!(
                    "shadow segment {} is shorter than durable marker ({} < {})",
                    entry.path().display(),
                    actual,
                    durable
                ));
            }
            if actual > durable {
                let file = OpenOptions::new().write(true).open(entry.path()).await?;
                file.set_len(durable).await?;
                file.sync_all().await?;
                discarded = true;
            }
            seen.insert(name, durable);
        }
        for (name, durable) in &marker.segments {
            if *durable > 0 && !seen.contains_key(name) {
                return Err(anyhow!(
                    "shadow durable marker references missing segment {}",
                    shadow_dir.join(name).display()
                ));
            }
        }
        Self::fsync_dir(shadow_dir).await?;
        Ok((discarded, reset_generation))
    }

    async fn read_durable_tail(shadow_dir: &Path) -> Result<DurableShadowTail> {
        let bytes = fs::read(shadow_dir.join(DURABLE_TAIL_FILE)).await?;
        let marker: DurableShadowTail = serde_json::from_slice(&bytes)?;
        if marker.version != DURABLE_TAIL_VERSION {
            return Err(anyhow!("unsupported shadow durable-tail version"));
        }
        Ok(marker)
    }

    async fn persist_durable_tail(shadow_dir: &Path, marker: &DurableShadowTail) -> Result<()> {
        let path = shadow_dir.join(DURABLE_TAIL_FILE);
        let tmp = shadow_dir.join(format!(".durable-tail-v1-{}.tmp", std::process::id()));
        let bytes = serde_json::to_vec_pretty(marker)?;
        let mut file = OpenOptions::new()
            .create(true)
            .truncate(true)
            .write(true)
            .open(&tmp)
            .await?;
        file.write_all(&bytes).await?;
        file.flush().await?;
        file.sync_all().await?;
        drop(file);
        fs::rename(&tmp, &path).await?;
        Self::fsync_dir(shadow_dir).await
    }

    /// Copy new WAL frames to the shadow WAL
    ///
    /// Returns the number of frames copied
    pub async fn copy_frames(&mut self, offset: u64) -> Result<(Vec<ParsedFrame>, u64)> {
        let wal_path = self.db_path.with_extension("db-wal");

        // Check if WAL exists
        if !wal_path.exists() {
            if self.probe_wal_mode_on_lifecycle_state {
                Self::ensure_database_in_wal_mode(&self.db_path).await?;
            }
            return Ok((Vec::new(), offset));
        }

        // Read WAL header to check for checkpoint (salt change)
        let header = match wal::read_header(&wal_path).await? {
            Some(h) => h,
            None => {
                if self.probe_wal_mode_on_lifecycle_state {
                    Self::ensure_database_in_wal_mode(&self.db_path).await?;
                }
                return Ok((Vec::new(), offset));
            }
        };
        let cursor_before = (
            self.generation,
            self.segment_index,
            self.segment_offset,
            self.page_size,
            self.wal_salt,
            self.wal_chain,
            self.header_seeded,
        );

        // Detect checkpoint by salt change
        let current_salt = (header.salt1, header.salt2);
        let mut effective_offset = offset;

        if !self.header_seeded {
            // First real WAL header after a fresh start (DB created with no WAL):
            // seed salt + page_size from the actual header instead of the
            // (0,0)/4096 defaults. This is initialization, NOT a rollover, so we
            // do not bump the generation. Without this, wal_salt stays (0,0)
            // forever and later genuine checkpoints are never detected (B5).
            self.wal_salt = current_salt;
            self.page_size = header.page_size;
            self.header_seeded = true;
            self.wal_chain = None;
        } else if current_salt != self.wal_salt {
            // Checkpoint occurred - start new generation
            let next_generation = self
                .generation
                .checked_add(1)
                .ok_or_else(|| anyhow!("shadow generation exhausted during checkpoint rollover"))?;
            tracing::info!(
                "Shadow WAL: checkpoint detected (salt changed), starting generation {}",
                next_generation
            );
            self.generation = next_generation;
            self.segment_index = 0;
            self.segment_offset = 0;
            self.wal_salt = current_salt;
            // A new generation may in principle use a different page size; refresh
            // so the readback/upload path frames segments correctly (B5).
            self.page_size = header.page_size;
            effective_offset = 0;
            // New generation: re-seed the checksum chain from the WAL header.
            self.wal_chain = None;
        }

        // Read new frames from the active WAL through the CHECKED reader so torn
        // or stale (salt-mismatched) frames never enter the shadow (B4). The
        // running chain is carried across incremental reads within this process.
        let (frames, new_offset, _max_db_size, out_chain) = wal::read_frames_as_pages_checked(
            &wal_path,
            header.page_size,
            effective_offset,
            self.wal_chain,
        )
        .await?;

        if frames.is_empty() {
            self.wal_chain = out_chain;
            return Ok((Vec::new(), new_offset));
        }

        // Write frames to current shadow segment
        if let Err(error) = self
            .write_frames_to_segment(&frames, header.page_size)
            .await
        {
            (
                self.generation,
                self.segment_index,
                self.segment_offset,
                self.page_size,
                self.wal_salt,
                self.wal_chain,
                self.header_seeded,
            ) = cursor_before;
            return Err(error);
        }
        self.wal_chain = out_chain;

        tracing::debug!(
            "Shadow WAL: copied {} frames to gen {} segment {} (offset {} -> {})",
            frames.len(),
            self.generation,
            self.segment_index,
            effective_offset,
            new_offset
        );

        Ok((frames, new_offset))
    }

    /// Write frames to the current shadow segment file
    async fn write_frames_to_segment(
        &mut self,
        frames: &[ParsedFrame],
        page_size: u32,
    ) -> Result<()> {
        if let Some(error) = &self.append_poisoned {
            return Err(anyhow!(
                "shadow append is fail-closed after rollback failure; restart required: {error}"
            ));
        }
        let segment_path = self.current_segment_path();
        let marker_before = Self::read_durable_tail(&self.shadow_dir).await?;
        let segment_name = segment_path
            .file_name()
            .expect("shadow segment has a filename")
            .to_string_lossy()
            .into_owned();
        let durable_len = marker_before
            .segments
            .get(&segment_name)
            .copied()
            .unwrap_or(0);
        let offset_before = self.segment_offset;

        let write_result = async {
            let mut file = OpenOptions::new()
                .create(true)
                .append(true)
                .open(&segment_path)
                .await?;
            for frame in frames {
                let mut header = [0u8; 24];
                header[0..4].copy_from_slice(&frame.page_number.to_be_bytes());
                header[4..8].copy_from_slice(&frame.db_size.to_be_bytes());
                file.write_all(&header).await?;
                #[cfg(test)]
                if matches!(self.append_failure, Some(TestAppendFailure::Write)) {
                    return Err(anyhow!("injected shadow write failure"));
                }
                file.write_all(&frame.data).await?;
                self.segment_offset += FRAME_HEADER_SIZE + page_size as u64;
            }
            file.flush().await?;
            crate::native_spool::durability_failpoint("shadow_before_fsync");
            #[cfg(test)]
            if matches!(self.append_failure, Some(TestAppendFailure::Sync)) {
                return Err(anyhow!("injected shadow sync failure"));
            }
            file.sync_all().await?;
            Self::fsync_dir(&self.shadow_dir).await?;
            let mut marker = marker_before.clone();
            marker
                .segments
                .insert(segment_name.clone(), file.metadata().await?.len());
            Self::persist_durable_tail(&self.shadow_dir, &marker).await?;
            #[cfg(test)]
            if matches!(self.append_failure, Some(TestAppendFailure::Marker)) {
                return Err(anyhow!("injected shadow marker failure"));
            }
            crate::native_spool::durability_failpoint("shadow_fsync_complete");
            Ok(())
        }
        .await;

        if let Err(error) = write_result {
            self.segment_offset = offset_before;
            // Restore the old marker first. A crash during rollback then leaves
            // an authoritative shorter marker and an overlong file, which
            // normal startup recovery safely truncates.
            let rollback = async {
                Self::persist_durable_tail(&self.shadow_dir, &marker_before).await?;
                match OpenOptions::new().write(true).open(&segment_path).await {
                    Ok(file) => {
                        file.set_len(durable_len).await?;
                        file.sync_all().await?;
                    }
                    Err(open_error)
                        if open_error.kind() == std::io::ErrorKind::NotFound
                            && durable_len == 0 => {}
                    Err(open_error) => return Err(open_error.into()),
                }
                Self::fsync_dir(&self.shadow_dir).await
            }
            .await;
            if let Err(rollback_error) = rollback {
                let message =
                    format!("append error: {error:#}; rollback error: {rollback_error:#}");
                self.append_poisoned = Some(message.clone());
                return Err(anyhow!(message));
            }
            return Err(error);
        }
        Ok(())
    }

    async fn fsync_dir(path: &Path) -> Result<()> {
        let dir = File::open(path).await?;
        dir.sync_all().await?;
        Ok(())
    }

    /// Get path to current shadow segment file
    fn current_segment_path(&self) -> PathBuf {
        self.shadow_dir
            .join(format_segment_name(self.generation, self.segment_index))
    }

    /// List all shadow segments for a generation
    pub async fn list_segments(&self, generation: u64) -> Result<Vec<ShadowSegment>> {
        let mut segments = Vec::new();

        let mut entries = fs::read_dir(&self.shadow_dir).await?;
        while let Some(entry) = entries.next_entry().await? {
            let name = entry.file_name();
            let name_str = name.to_string_lossy();

            if name_str.ends_with(".wal") {
                let parts: Vec<&str> = name_str.trim_end_matches(".wal").split('-').collect();
                if parts.len() == 2 {
                    if let (Ok(gen), Ok(idx)) = (
                        u64::from_str_radix(parts[0], 16),
                        u64::from_str_radix(parts[1], 16),
                    ) {
                        if gen == generation {
                            let path = entry.path();
                            let size = fs::metadata(&path).await?.len();
                            segments.push(ShadowSegment {
                                generation: gen,
                                index: idx,
                                path,
                                size,
                            });
                        }
                    }
                }
            }
        }

        // Sort by index
        segments.sort_by_key(|s| s.index);
        Ok(segments)
    }

    /// Read frames from shadow segments for upload
    pub async fn read_frames_from_shadow(
        &self,
        generation: u64,
        start_offset: u64,
    ) -> Result<(Vec<ParsedFrame>, u64)> {
        let segments = self.list_segments(generation).await?;
        let mut frames = Vec::new();
        let mut total_offset = 0u64;
        let frame_size = FRAME_HEADER_SIZE + self.page_size as u64;

        for segment in segments {
            let segment_start = total_offset;
            let segment_end = segment_start + segment.size;

            // Skip segments before our offset
            if segment_end <= start_offset {
                total_offset = segment_end;
                continue;
            }

            // Read frames from this segment
            let mut file = File::open(&segment.path).await?;
            let relative_offset = if start_offset > segment_start {
                start_offset - segment_start
            } else {
                0
            };

            file.seek(SeekFrom::Start(relative_offset)).await?;

            let bytes_to_read = segment.size - relative_offset;
            let frame_count = bytes_to_read / frame_size;

            for _ in 0..frame_count {
                // Read frame header
                let mut header = [0u8; 24];
                file.read_exact(&mut header).await?;

                let page_number = u32::from_be_bytes([header[0], header[1], header[2], header[3]]);
                let db_size = u32::from_be_bytes([header[4], header[5], header[6], header[7]]);

                // Read page data
                let mut data = vec![0u8; self.page_size as usize];
                file.read_exact(&mut data).await?;

                frames.push(ParsedFrame {
                    page_number,
                    db_size,
                    data,
                });
            }

            total_offset = segment_end;
        }

        let new_offset = start_offset + (frames.len() as u64 * frame_size);
        Ok((frames, new_offset))
    }

    /// Trigger a manual PASSIVE checkpoint and rotate shadow WAL.
    ///
    /// This releases the read transaction, runs checkpoint, then re-establishes the blocker.
    pub async fn checkpoint(&mut self) -> Result<()> {
        self.checkpoint_with_mode("PASSIVE").await
    }

    /// Trigger a manual TRUNCATE checkpoint and rotate shadow WAL.
    ///
    /// Used as the emergency WAL-growth safety brake after every shadow frame
    /// has been durably synced. The blocker is released only for the controlled
    /// checkpoint and is re-established before this returns.
    pub async fn checkpoint_truncate(&mut self) -> Result<()> {
        self.checkpoint_with_mode("TRUNCATE").await
    }

    async fn checkpoint_with_mode(&mut self, mode: &'static str) -> Result<()> {
        // Release checkpoint blocker
        if let Some(blocker) = self.checkpoint_blocker.take() {
            let conn = blocker.lock().await;
            conn.execute_batch("ROLLBACK;")?;
            drop(conn);
        }

        let checkpoint_result = self.execute_checkpoint(mode);

        // Re-establish checkpoint blocker
        let reopen_result = Self::open_checkpoint_blocker(&self.db_path);
        match (checkpoint_result, reopen_result) {
            (Ok(()), Ok(new_blocker)) => {
                self.checkpoint_blocker = Some(Arc::new(Mutex::new(new_blocker)));
            }
            (Err(checkpoint_err), Ok(new_blocker)) => {
                self.checkpoint_blocker = Some(Arc::new(Mutex::new(new_blocker)));
                return Err(checkpoint_err);
            }
            (Ok(()), Err(reopen_err)) => return Err(reopen_err),
            (Err(checkpoint_err), Err(reopen_err)) => {
                return Err(anyhow!(
                    "{}; additionally failed to re-open shadow checkpoint blocker: {}",
                    checkpoint_err,
                    reopen_err
                ));
            }
        }

        tracing::debug!(
            "Shadow WAL: {} checkpoint complete for {}",
            mode,
            self.db_path.display()
        );

        Ok(())
    }

    fn execute_checkpoint(&self, mode: &'static str) -> Result<()> {
        let conn = Connection::open(&self.db_path)?;
        conn.busy_timeout(Duration::from_secs(5))?;
        let (busy, log_frames, checkpointed_frames): (i64, i64, i64) =
            conn.query_row(&format!("PRAGMA wal_checkpoint({mode});"), [], |row| {
                Ok((row.get(0)?, row.get(1)?, row.get(2)?))
            })?;
        if busy != 0 || checkpointed_frames < log_frames {
            Err(anyhow!(
                "{}: shadow {} checkpoint incomplete (busy={}, log_frames={}, checkpointed_frames={})",
                self.db_path.display(),
                mode,
                busy,
                log_frames,
                checkpointed_frames
            ))
        } else {
            Ok(())
        }
    }

    /// Clean up old shadow segments after successful upload
    pub async fn cleanup_segments(&self, up_to_generation: u64) -> Result<usize> {
        let mut victims = Vec::new();

        let mut entries = fs::read_dir(&self.shadow_dir).await?;
        while let Some(entry) = entries.next_entry().await? {
            let name = entry.file_name();
            let name_str = name.to_string_lossy();

            if name_str.ends_with(".wal") {
                if let Some(gen_str) = name_str.split('-').next() {
                    if let Ok(gen) = u64::from_str_radix(gen_str, 16) {
                        if gen < up_to_generation {
                            victims.push((name_str.into_owned(), entry.path()));
                        }
                    }
                }
            }
        }

        if !victims.is_empty() {
            // Commit the ownership change before unlinking. A crash before
            // unlink leaves harmless unmarked files that startup removes; a
            // crash after unlink can never leave the marker naming a missing
            // segment.
            let mut marker = Self::read_durable_tail(&self.shadow_dir).await?;
            for (name, _) in &victims {
                marker.segments.remove(name.as_str());
            }
            Self::persist_durable_tail(&self.shadow_dir, &marker).await?;
            crate::native_spool::durability_failpoint("shadow_cleanup_marker_committed");
            for (_, path) in &victims {
                fs::remove_file(path).await?;
            }
            Self::fsync_dir(&self.shadow_dir).await?;
            tracing::debug!(
                "Shadow WAL: cleaned up {} old segments (generations < {})",
                victims.len(),
                up_to_generation
            );
        }

        Ok(victims.len())
    }

    /// Get current generation
    pub fn generation(&self) -> u64 {
        self.generation
    }

    /// Current byte offset within the active shadow segment generation.
    pub fn segment_offset(&self) -> u64 {
        self.segment_offset
    }

    /// Get page size
    pub fn page_size(&self) -> u32 {
        self.page_size
    }

    /// Get shadow directory path
    pub fn shadow_dir(&self) -> &Path {
        &self.shadow_dir
    }

    pub fn discarded_unproven_tail(&self) -> bool {
        self.discarded_unproven_tail
    }

    /// The WAL header salt of the generation the live-WAL read cursor indexes
    /// into, once a real header has been observed. `None` before the first
    /// header appears (mirrors the seeding logic in `copy_frames`). Persisted
    /// in the shadow progress record so a restart can detect a checkpoint that
    /// happened while the process was down.
    pub fn wal_read_salt(&self) -> Option<(u32, u32)> {
        self.header_seeded.then_some(self.wal_salt)
    }

    /// The running SQLite WAL checksum `(s0, s1)` at the live-WAL read cursor,
    /// or `None` at a generation boundary. Persisted so the first post-restart
    /// read validates the checksum chain per-frame from the resumed offset.
    pub fn wal_read_chain(&self) -> Option<(u32, u32)> {
        self.wal_chain
    }

    /// Restore the live-WAL read cursor after a process restart (B4). Seeding
    /// the persisted salt + running chain lets the next `copy_frames` resume
    /// from the persisted offset with full per-frame checksum validation and
    /// detect a checkpoint that occurred during downtime (salt mismatch), in
    /// place of re-reading the entire live WAL from offset 0. `salt == None`
    /// means no header was ever observed durably, so nothing is restored.
    pub fn restore_read_cursor(&mut self, salt: Option<(u32, u32)>, chain: Option<(u32, u32)>) {
        if let Some(salt) = salt {
            self.wal_salt = salt;
            self.header_seeded = true;
            self.wal_chain = chain;
        }
    }

    /// Test-only: whether the WAL salt has been seeded off the `(0,0)` sentinel.
    #[cfg(test)]
    pub(crate) fn wal_salt_seeded(&self) -> bool {
        self.header_seeded && self.wal_salt != (0, 0)
    }
}

impl Drop for ShadowWal {
    fn drop(&mut self) {
        // Clean up checkpoint blocker connection
        if let Some(blocker) = self.checkpoint_blocker.take() {
            if let Ok(mutex) = Arc::try_unwrap(blocker) {
                let conn = mutex.into_inner();
                let _ = conn.execute_batch("ROLLBACK;");
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn test_checkpoint_blocker_prevents_concurrent_truncate_across_supported_wal_configs() {
        let dir = tempdir().unwrap();

        for page_size in [512u32, 1024, 2048, 4096, 8192, 16384, 32768, 65536] {
            for synchronous in ["OFF", "NORMAL", "FULL", "EXTRA"] {
                let expected_synchronous = match synchronous {
                    "OFF" => 0,
                    "NORMAL" => 1,
                    "FULL" => 2,
                    "EXTRA" => 3,
                    _ => unreachable!("test matrix only contains supported synchronous levels"),
                };
                let db_path = dir
                    .path()
                    .join(format!("blocker-{page_size}-{synchronous}.db"));
                let wal_path = db_path.with_extension("db-wal");
                let app = Connection::open(&db_path).unwrap();
                app.execute_batch(&format!(
                    "
                    PRAGMA page_size={page_size};
                    PRAGMA journal_mode=WAL;
                    PRAGMA synchronous={synchronous};
                    PRAGMA wal_autocheckpoint=0;
                    PRAGMA busy_timeout=0;
                    CREATE TABLE app_data (id INTEGER PRIMARY KEY, value TEXT NOT NULL);
                    INSERT INTO app_data (value) VALUES ('before-blocker');
                    "
                ))
                .unwrap();
                assert_eq!(
                    app.query_row("PRAGMA page_size;", [], |row| row.get::<_, u32>(0))
                        .unwrap(),
                    page_size,
                    "test setup must apply page_size={page_size}"
                );
                assert_eq!(
                    app.query_row("PRAGMA journal_mode;", [], |row| row.get::<_, String>(0))
                        .unwrap(),
                    "wal",
                    "test setup must enter WAL mode for page_size={page_size}, synchronous={synchronous}"
                );
                assert_eq!(
                    app.query_row("PRAGMA synchronous;", [], |row| row.get::<_, i64>(0))
                        .unwrap(),
                    expected_synchronous,
                    "test setup must apply synchronous={synchronous} for page_size={page_size}"
                );

                let blocker = ShadowWal::open_checkpoint_blocker(&db_path).unwrap();

                // This commit lands after the blocker's pinned read-mark. It is the
                // unread work a concurrent application checkpoint must not erase.
                app.execute("INSERT INTO app_data (value) VALUES ('after-blocker')", [])
                    .unwrap();
                let wal_before = std::fs::read(&wal_path).unwrap();
                assert!(
                    !wal_before.is_empty(),
                    "test setup must create a WAL for page_size={page_size}, synchronous={synchronous}"
                );

                let (busy, _log_frames, _checkpointed_frames): (i64, i64, i64) = app
                    .query_row("PRAGMA wal_checkpoint(TRUNCATE);", [], |row| {
                        Ok((row.get(0)?, row.get(1)?, row.get(2)?))
                    })
                    .unwrap();
                assert_eq!(
                    busy, 1,
                    "checkpoint blocker must make concurrent TRUNCATE report busy for page_size={page_size}, synchronous={synchronous}"
                );
                assert_eq!(
                    std::fs::read(&wal_path).unwrap(),
                    wal_before,
                    "blocked TRUNCATE must preserve the WAL bytes for page_size={page_size}, synchronous={synchronous}"
                );

                blocker.execute_batch("ROLLBACK;").unwrap();
                drop(blocker);

                let (busy, _log_frames, _checkpointed_frames): (i64, i64, i64) = app
                    .query_row("PRAGMA wal_checkpoint(TRUNCATE);", [], |row| {
                        Ok((row.get(0)?, row.get(1)?, row.get(2)?))
                    })
                    .unwrap();
                assert_eq!(
                    busy, 0,
                    "TRUNCATE must succeed after releasing the blocker for page_size={page_size}, synchronous={synchronous}"
                );
                assert_eq!(
                    std::fs::metadata(&wal_path).unwrap().len(),
                    0,
                    "successful TRUNCATE must empty the WAL for page_size={page_size}, synchronous={synchronous}"
                );
            }
        }
    }

    #[test]
    fn test_checkpoint_blocker_preserves_wal_across_short_lived_writer_sessions() {
        let dir = tempdir().unwrap();
        let db_path = dir.path().join("ephemeral-writers.db");
        let setup = Connection::open(&db_path).unwrap();
        setup
            .execute_batch(
                "
                PRAGMA journal_mode=WAL;
                PRAGMA wal_autocheckpoint=0;
                CREATE TABLE app_data (id INTEGER PRIMARY KEY, value TEXT NOT NULL);
                ",
            )
            .unwrap();
        drop(setup);

        let blocker = ShadowWal::open_checkpoint_blocker(&db_path).unwrap();
        for snapshot_index in 0..2 {
            {
                let snapshot_checkpoint = Connection::open(&db_path).unwrap();
                let _: (i64, i64, i64) = snapshot_checkpoint
                    .query_row("PRAGMA wal_checkpoint(PASSIVE);", [], |row| {
                        Ok((row.get(0)?, row.get(1)?, row.get(2)?))
                    })
                    .unwrap();
            }
            let snapshot_path = dir.path().join(format!("snapshot-{snapshot_index}.db"));
            let snapshot_reader = Connection::open(&db_path).unwrap();
            snapshot_reader
                .execute("VACUUM INTO ?1", [snapshot_path.to_str().unwrap()])
                .unwrap();
        }
        {
            let first_writer = Connection::open(&db_path).unwrap();
            first_writer
                .execute_batch("PRAGMA journal_mode=WAL; PRAGMA busy_timeout=0;")
                .unwrap();
            first_writer
                .execute(
                    "INSERT INTO app_data (id, value) VALUES (1, 'ephemeral-1')",
                    [],
                )
                .unwrap();
            let busy: i64 = first_writer
                .query_row("PRAGMA wal_checkpoint(TRUNCATE);", [], |row| row.get(0))
                .unwrap();
            assert_eq!(busy, 1, "the pinned blocker must reject the first TRUNCATE");
        }
        for id in 2..=10i64 {
            let writer = Connection::open(&db_path).unwrap();
            writer.execute_batch("PRAGMA journal_mode=WAL;").unwrap();
            writer
                .execute(
                    "INSERT INTO app_data (id, value) VALUES (?1, ?2)",
                    rusqlite::params![id, format!("ephemeral-{id}")],
                )
                .unwrap();
        }

        let wal_path = db_path.with_extension("db-wal");
        assert!(
            std::fs::metadata(&wal_path).unwrap().len() > 32,
            "short-lived writer closes must leave their frames in the pinned WAL"
        );
        let checkpoint = Connection::open(&db_path).unwrap();
        checkpoint.execute_batch("PRAGMA busy_timeout=0;").unwrap();
        let busy: i64 = checkpoint
            .query_row("PRAGMA wal_checkpoint(TRUNCATE);", [], |row| row.get(0))
            .unwrap();
        assert_eq!(
            busy, 1,
            "the blocker must remain pinned across writer closes"
        );

        blocker.execute_batch("ROLLBACK;").unwrap();
    }

    #[test]
    fn test_reopened_checkpoint_blocker_preserves_wal_across_writer_close() {
        let dir = tempdir().unwrap();
        let db_path = dir.path().join("reopened-ephemeral-writer.db");
        let setup = Connection::open(&db_path).unwrap();
        setup
            .execute_batch(
                "PRAGMA journal_mode=WAL;
                 PRAGMA wal_autocheckpoint=0;
                 CREATE TABLE app_data (id INTEGER PRIMARY KEY, value TEXT NOT NULL);",
            )
            .unwrap();
        drop(setup);

        let monitor = Connection::open(&db_path).unwrap();
        let blocker = ShadowWal::open_checkpoint_blocker(&db_path).unwrap();
        blocker.execute_batch("ROLLBACK;").unwrap();
        drop(blocker);
        let _: (i64, i64, i64) = monitor
            .query_row("PRAGMA wal_checkpoint(PASSIVE);", [], |row| {
                Ok((row.get(0)?, row.get(1)?, row.get(2)?))
            })
            .unwrap();
        drop(monitor);

        // This is the CLI controlled-checkpoint handoff: refresh the lifetime
        // monitor first, then make replacement blocker acquisition the final
        // SQLite operation in the watcher process.
        let monitor = Connection::open(&db_path).unwrap();
        let replacement = ShadowWal::open_checkpoint_blocker(&db_path).unwrap();
        let _: i64 = monitor
            .query_row("PRAGMA data_version", [], |row| row.get(0))
            .unwrap();
        drop(replacement);
        let replacement = ShadowWal::open_checkpoint_blocker(&db_path).unwrap();
        {
            let writer = Connection::open(&db_path).unwrap();
            writer
                .execute("INSERT INTO app_data (value) VALUES ('after-rearm')", [])
                .unwrap();
        }

        let wal_path = db_path.with_extension("db-wal");
        assert!(
            std::fs::metadata(&wal_path).unwrap().len() > 32,
            "a short-lived writer close reset the WAL after blocker reacquisition"
        );
        let checkpoint = Connection::open(&db_path).unwrap();
        checkpoint.execute_batch("PRAGMA busy_timeout=0;").unwrap();
        let busy: i64 = checkpoint
            .query_row("PRAGMA wal_checkpoint(TRUNCATE);", [], |row| row.get(0))
            .unwrap();
        assert_eq!(busy, 1, "replacement blocker did not refuse TRUNCATE");
        replacement.execute_batch("ROLLBACK;").unwrap();
    }

    #[test]
    fn test_blocker_data_version_detects_only_other_connection_commits() {
        let dir = tempdir().unwrap();
        let db_path = dir.path().join("data-version.db");
        let setup = Connection::open(&db_path).unwrap();
        setup
            .execute_batch(
                "PRAGMA journal_mode=WAL;
                 CREATE TABLE app_data (id INTEGER PRIMARY KEY, value TEXT NOT NULL);",
            )
            .unwrap();
        drop(setup);

        let monitor = Connection::open(&db_path).unwrap();
        let before: i64 = monitor
            .query_row("PRAGMA data_version;", [], |row| row.get(0))
            .unwrap();
        monitor
            .execute("INSERT INTO app_data (id, value) VALUES (1, 'monitor')", [])
            .unwrap();
        let after_own_commit: i64 = monitor
            .query_row("PRAGMA data_version;", [], |row| row.get(0))
            .unwrap();
        assert_eq!(
            after_own_commit, before,
            "a connection's own commit must not dirty its connection-local data_version"
        );

        let app = Connection::open(&db_path).unwrap();
        app.execute(
            "INSERT INTO app_data (id, value) VALUES (2, 'external')",
            [],
        )
        .unwrap();
        drop(app);
        let after_app_commit: i64 = monitor
            .query_row("PRAGMA data_version;", [], |row| row.get(0))
            .unwrap();
        assert_ne!(
            after_app_commit, after_own_commit,
            "another connection's commit must dirty the checkpoint-window detector"
        );

        monitor
            .query_row("PRAGMA wal_checkpoint(TRUNCATE);", [], |_| Ok(()))
            .unwrap();
        let after_own_checkpoint: i64 = monitor
            .query_row("PRAGMA data_version;", [], |row| row.get(0))
            .unwrap();
        assert_eq!(
            after_own_checkpoint, after_app_commit,
            "a checkpoint on the monitored connection must not dirty its own data_version"
        );
    }

    #[tokio::test]
    async fn test_shadow_dir_path() {
        let db_path = PathBuf::from("/data/myapp.db");
        let shadow_dir = ShadowWal::shadow_dir_for(&db_path);
        assert_eq!(shadow_dir, PathBuf::from("/data/.walrust-myapp"));
    }

    #[test]
    fn test_segment_name_width_keeps_lexical_order_past_u32() {
        fn segment_name(generation: u64) -> String {
            let shadow = ShadowWal {
                db_path: PathBuf::from("test.db"),
                shadow_dir: PathBuf::from(".walrust-test"),
                generation,
                segment_index: 0,
                segment_offset: 0,
                page_size: 4096,
                checkpoint_blocker: None,
                wal_salt: (0, 0),
                wal_chain: None,
                header_seeded: false,
                probe_wal_mode_on_lifecycle_state: false,
                discarded_unproven_tail: false,
                append_poisoned: None,
                append_failure: None,
            };
            shadow
                .current_segment_path()
                .file_name()
                .unwrap()
                .to_string_lossy()
                .into_owned()
        }

        let before_wrap = segment_name(0xffff_ffff);
        let after_wrap = segment_name(0x1_0000_0000);
        assert!(
            before_wrap < after_wrap,
            "lexical order must follow numeric order: {before_wrap} vs {after_wrap}"
        );
        assert_eq!(before_wrap.len(), after_wrap.len(), "fixed width");
    }

    #[tokio::test]
    async fn test_shadow_wal_creation() {
        let dir = tempdir().unwrap();
        let db_path = dir.path().join("test.db");

        // Create a test database
        let conn = Connection::open(&db_path).unwrap();
        conn.execute_batch(
            "PRAGMA journal_mode=WAL;
             CREATE TABLE test (id INTEGER PRIMARY KEY);
             INSERT INTO test VALUES (1);",
        )
        .unwrap();
        drop(conn);

        // Create shadow WAL
        let shadow = ShadowWal::new(&db_path).await.unwrap();
        assert!(shadow.shadow_dir().exists());
        assert_eq!(shadow.generation(), 0);
    }

    // --- E1: torn shadow-segment tail after a crash-under-load restart ---

    const E1_PAGE_SIZE: u32 = 4096;

    fn e1_frame(page: u32, db_size: u32, fill: u8) -> Vec<u8> {
        let mut frame = vec![0u8; (FRAME_HEADER_SIZE + E1_PAGE_SIZE as u64) as usize];
        frame[0..4].copy_from_slice(&page.to_be_bytes());
        frame[4..8].copy_from_slice(&db_size.to_be_bytes());
        for b in frame[24..].iter_mut() {
            *b = fill;
        }
        frame
    }

    /// Reproduces the exact drill symptom at the read layer: when a shadow
    /// segment has a torn (non-frame-aligned) tail followed by more frame bytes,
    /// the frame-by-frame reader misaligns and decodes a header out of a frame's
    /// zero-padded region, producing page number 0 — which litepages surfaces as
    /// "Invalid page num: transaction ID must be non-zero".
    #[tokio::test]
    async fn e1_torn_segment_tail_decodes_as_invalid_page_num() {
        use crate::legacy_shadow::{encode_shadow_to_ltx, ShadowSyncInput};

        let dir = tempdir().unwrap();
        let shadow_dir = dir.path().join(".walrust-app");
        std::fs::create_dir_all(&shadow_dir).unwrap();

        let mut bytes = e1_frame(1, 1, 0xAB); // committed frame, page 1
        bytes.extend_from_slice(&[0u8; 4]); // torn 4-byte tail (all zero)
        bytes.extend_from_slice(&e1_frame(2, 2, 0xCD)); // appended committed frame
        std::fs::write(shadow_dir.join(format_segment_name(0, 0)), &bytes).unwrap();

        let input = ShadowSyncInput {
            db_path: dir.path().join("app.db"),
            name: "app".into(),
            current_txid: 10,
            db_checksum: Some(123), // avoid reading the (absent) db file for the checksum
            generation: 0,
            shadow_sync_offset: 0,
            page_size: E1_PAGE_SIZE,
            shadow_dir: shadow_dir.clone(),
        };

        let err = match encode_shadow_to_ltx(&input) {
            Err(e) => e,
            Ok(_) => panic!("misaligned torn tail must fail to encode"),
        };
        assert!(
            err.to_string().contains("Invalid page num"),
            "misaligned torn tail must surface the page-num error, got: {err}"
        );
    }

    /// Upgrade gate: a markerless aligned tail is not proof of fsync. It may be
    /// a complete corrupt frame from a power-loss image, so startup must remove
    /// it and force the watch layer through a full snapshot before any delta.
    #[tokio::test]
    async fn markerless_aligned_tail_is_discarded_and_requires_snapshot() {
        let dir = tempdir().unwrap();
        let db_path = dir.path().join("app.db");
        let conn = Connection::open(&db_path).unwrap();
        conn.execute_batch(
            "PRAGMA journal_mode=WAL;
             CREATE TABLE t (id INTEGER PRIMARY KEY);
             INSERT INTO t VALUES (1);",
        )
        .unwrap();
        drop(conn);

        let shadow_dir = ShadowWal::shadow_dir_for(&db_path);
        std::fs::create_dir_all(&shadow_dir).unwrap();
        let seg = shadow_dir.join(format_segment_name(0, 0));
        std::fs::write(&seg, e1_frame(999, 999, 0xDE)).unwrap();

        let shadow = ShadowWal::new(&db_path).await.unwrap();
        assert!(shadow.discarded_unproven_tail());
        assert_eq!(shadow.generation(), 1, "upgrade must rotate cursor domain");
        assert!(!seg.exists(), "markerless bytes must never enter a delta");
        let marker = ShadowWal::read_durable_tail(&shadow_dir).await.unwrap();
        assert!(marker.segments.is_empty());
    }

    #[tokio::test]
    async fn durable_tail_discards_aligned_prefsync_crash_image() {
        let dir = tempdir().unwrap();
        let db_path = dir.path().join("durable-tail.db");
        let conn = Connection::open(&db_path).unwrap();
        conn.execute_batch(
            "PRAGMA journal_mode=WAL;
             PRAGMA wal_autocheckpoint=0;
             CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
             INSERT INTO t(v) VALUES ('durable');",
        )
        .unwrap();

        let mut shadow = ShadowWal::new(&db_path).await.unwrap();
        shadow.copy_frames(0).await.unwrap();
        let segment = shadow.current_segment_path();
        let durable_len = std::fs::metadata(&segment).unwrap().len();
        let marker_before = ShadowWal::read_durable_tail(shadow.shadow_dir())
            .await
            .unwrap();
        assert_eq!(
            marker_before.segments[segment.file_name().unwrap().to_str().unwrap()],
            durable_len
        );
        drop(shadow);

        // Construct the power-loss image SIGKILL cannot model: a complete,
        // frame-aligned tail reached the file length but not the durable marker.
        use std::io::Write as _;
        let mut file = std::fs::OpenOptions::new()
            .append(true)
            .open(&segment)
            .unwrap();
        file.write_all(&e1_frame(999, 999, 0xDE)).unwrap();
        drop(file);
        assert_eq!(
            std::fs::metadata(&segment).unwrap().len(),
            durable_len + FRAME_HEADER_SIZE + E1_PAGE_SIZE as u64
        );

        let restarted = ShadowWal::new(&db_path).await.unwrap();
        assert!(restarted.discarded_unproven_tail());
        assert_eq!(
            std::fs::metadata(&segment).unwrap().len(),
            durable_len,
            "startup must trust the fsynced marker, not aligned file length"
        );
        assert_eq!(
            restarted.segment_offset(),
            durable_len,
            "direct snapshots must freeze the full recovered generation, not only bytes appended after restart"
        );
    }

    #[tokio::test]
    async fn append_failures_rollback_before_retry_can_bless_bytes() {
        let dir = tempdir().unwrap();
        let db_path = dir.path().join("append-rollback.db");
        let conn = Connection::open(&db_path).unwrap();
        conn.execute_batch(
            "PRAGMA journal_mode=WAL;
             CREATE TABLE t(id INTEGER PRIMARY KEY);",
        )
        .unwrap();
        let mut shadow = ShadowWal::new(&db_path).await.unwrap();
        let frame = |page, fill| ParsedFrame {
            page_number: page,
            db_size: page,
            data: vec![fill; E1_PAGE_SIZE as usize],
        };
        shadow
            .write_frames_to_segment(&[frame(1, 0x11)], E1_PAGE_SIZE)
            .await
            .unwrap();

        for (index, failure) in [
            TestAppendFailure::Write,
            TestAppendFailure::Sync,
            TestAppendFailure::Marker,
        ]
        .into_iter()
        .enumerate()
        {
            let segment = shadow.current_segment_path();
            let durable_before = std::fs::metadata(&segment).unwrap().len();
            let offset_before = shadow.segment_offset;
            let marker_before = ShadowWal::read_durable_tail(shadow.shadow_dir())
                .await
                .unwrap();
            shadow.append_failure = Some(failure);
            let error = shadow
                .write_frames_to_segment(
                    &[frame(index as u32 + 2, 0x20 + index as u8)],
                    E1_PAGE_SIZE,
                )
                .await
                .unwrap_err();
            assert!(format!("{error:#}").contains("injected shadow"));
            assert_eq!(std::fs::metadata(&segment).unwrap().len(), durable_before);
            assert_eq!(shadow.segment_offset, offset_before);
            assert_eq!(
                ShadowWal::read_durable_tail(shadow.shadow_dir())
                    .await
                    .unwrap()
                    .segments,
                marker_before.segments,
                "{failure:?} must not leave a marker that blesses failed bytes"
            );

            shadow.append_failure = None;
            shadow
                .write_frames_to_segment(
                    &[frame(index as u32 + 2, 0x20 + index as u8)],
                    E1_PAGE_SIZE,
                )
                .await
                .unwrap();
            assert_eq!(
                std::fs::metadata(&segment).unwrap().len(),
                durable_before + FRAME_HEADER_SIZE + E1_PAGE_SIZE as u64,
                "retry after {failure:?} must append at the prior durable boundary"
            );
        }
    }

    #[tokio::test]
    async fn failed_append_does_not_advance_wal_checksum_cursor() {
        let dir = tempdir().unwrap();
        let db_path = dir.path().join("append-chain.db");
        let conn = Connection::open(&db_path).unwrap();
        conn.execute_batch(
            "PRAGMA journal_mode=WAL;
             PRAGMA wal_autocheckpoint=0;
             CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
             INSERT INTO t(v) VALUES ('retry');",
        )
        .unwrap();
        let mut shadow = ShadowWal::new(&db_path).await.unwrap();
        let chain_before = shadow.wal_read_chain();
        shadow.append_failure = Some(TestAppendFailure::Sync);
        shadow.copy_frames(0).await.unwrap_err();
        assert_eq!(shadow.wal_read_chain(), chain_before);
        shadow.append_failure = None;
        let (frames, offset) = shadow.copy_frames(0).await.unwrap();
        assert!(!frames.is_empty());
        assert!(offset > 0);
        assert!(shadow.wal_read_chain().is_some());
    }

    #[tokio::test]
    async fn durable_tail_finishes_marker_first_cleanup_after_crash() {
        let dir = tempdir().unwrap();
        let db_path = dir.path().join("cleanup-tail.db");
        let conn = Connection::open(&db_path).unwrap();
        conn.execute_batch(
            "PRAGMA journal_mode=WAL;
             PRAGMA wal_autocheckpoint=0;
             CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
             INSERT INTO t(v) VALUES ('durable');",
        )
        .unwrap();

        let shadow = ShadowWal::new(&db_path).await.unwrap();
        let orphan = shadow.shadow_dir().join(format_segment_name(7, 0));
        std::fs::write(&orphan, e1_frame(1, 1, 0xAA)).unwrap();
        drop(shadow);

        let restarted = ShadowWal::new(&db_path).await.unwrap();
        assert!(restarted.discarded_unproven_tail());
        assert!(
            !orphan.exists(),
            "startup must finish a marker-first cleanup instead of adopting or retaining its orphan"
        );
    }

    /// Priority 1(b) — the REWIND question. A torn trailing frame was a
    /// partially-copied REAL committed WAL frame. After healing, resume must
    /// RE-COPY that frame from the live WAL (no silent data loss) AND must NOT
    /// duplicate the whole frames that preceded the tear in the same
    /// not-yet-persisted copy batch (no TXID-stream corruption). Both directions
    /// are asserted against a clean-copy oracle built from the same live WAL.
    #[tokio::test]
    async fn e1_heal_then_resume_recopies_torn_frame_without_loss_or_corruption() {
        use crate::legacy_shadow::{encode_shadow_to_ltx, ShadowSyncInput};

        let dir = tempdir().unwrap();
        let db_path = dir.path().join("app.db");
        let conn = Connection::open(&db_path).unwrap();
        conn.execute_batch(
            "PRAGMA journal_mode=WAL;
             PRAGMA wal_autocheckpoint=0;
             CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT);
             INSERT INTO t (v) VALUES ('a0');",
        )
        .unwrap();

        // --- Durable checkpoint: copy commit A, persist its cursor. ---
        let mut shadow = ShadowWal::new(&db_path).await.unwrap();
        let (_frames_a, off_a) = shadow.copy_frames(0).await.unwrap();
        let salt_a = shadow.wal_read_salt();
        let chain_a = shadow.wal_read_chain();
        let page_size = shadow.page_size();
        let seg = shadow.shadow_dir().join(format_segment_name(0, 0));
        assert!(off_a > 0, "commit A must have produced frames");

        // --- Commit B: several more frames land in the live WAL. ---
        for i in 0..5 {
            conn.execute("INSERT INTO t (v) VALUES (?1)", [format!("b{i}")])
                .unwrap();
        }

        // The interrupted copy of B: its whole frames reach the shadow on disk,
        // but the crash happens before wal_copy_offset is persisted (it stays
        // off_a) and the final frame is left torn.
        let (_frames_b, _off_b) = shadow.copy_frames(off_a).await.unwrap();
        let clean_len = std::fs::metadata(&seg).unwrap().len();
        {
            use std::io::Write;
            let mut f = std::fs::OpenOptions::new().append(true).open(&seg).unwrap();
            f.write_all(&[0u8; 30]).unwrap(); // torn partial frame
        }
        drop(shadow);

        // --- Restart: heal must drop the torn tail back to whole frames. ---
        let mut restarted = ShadowWal::new(&db_path).await.unwrap();
        assert_eq!(
            std::fs::metadata(&seg).unwrap().len(),
            clean_len,
            "heal must truncate the torn tail to the whole-frame boundary"
        );

        // Resume from the PERSISTED (pre-B) cursor — the rewind under test.
        restarted.restore_read_cursor(salt_a, chain_a);
        let (_resumed, _new_off) = restarted.copy_frames(off_a).await.unwrap();

        // Oracle: a pristine shadow built once from the same final live WAL.
        let oracle_dir = tempdir().unwrap();
        let oracle_db = oracle_dir.path().join("app.db");
        std::fs::copy(&db_path, &oracle_db).unwrap();
        std::fs::copy(
            db_path.with_extension("db-wal"),
            oracle_db.with_extension("db-wal"),
        )
        .unwrap();
        let mut oracle = ShadowWal::new(&oracle_db).await.unwrap();
        oracle.copy_frames(0).await.unwrap();
        let oracle_seg = oracle.shadow_dir().join(format_segment_name(0, 0));
        let oracle_len = std::fs::metadata(&oracle_seg).unwrap().len();

        let healed_len = std::fs::metadata(&seg).unwrap().len();

        // The encoded LTX is what actually ships. It must carry every distinct
        // page exactly once with the same TXID range as a clean copy — the torn
        // frame present (no loss) and any structurally duplicated frames
        // collapsed by the page-dedup reader (no TXID-stream corruption).
        let encode_one = |sdir: &std::path::Path, gen| {
            encode_shadow_to_ltx(&ShadowSyncInput {
                db_path: db_path.clone(),
                name: "app".into(),
                current_txid: 50,
                db_checksum: Some(123),
                generation: gen,
                shadow_sync_offset: 0,
                page_size,
                shadow_dir: sdir.to_path_buf(),
            })
            .unwrap()
            .map(|(r, _)| (r.unique_pages, r.min_txid, r.max_txid))
        };
        let healed = encode_one(restarted.shadow_dir(), restarted.generation());
        let oracle_res = encode_one(oracle.shadow_dir(), oracle.generation());
        assert_eq!(
            healed, oracle_res,
            "healed shadow must decode the same unique pages and TXID range as a \
             clean copy (healed_seg_len={healed_len}, oracle_seg_len={oracle_len})"
        );
    }

    #[tokio::test]
    async fn test_shadow_wal_new_rejects_delete_mode_without_converting() {
        let dir = tempdir().unwrap();
        let db_path = dir.path().join("delete-mode.db");

        let conn = Connection::open(&db_path).unwrap();
        conn.execute_batch(
            "
            PRAGMA journal_mode=DELETE;
            CREATE TABLE test (id INTEGER PRIMARY KEY);
            INSERT INTO test VALUES (1);
            ",
        )
        .unwrap();
        drop(conn);

        let err = match ShadowWal::new(&db_path).await {
            Ok(_) => panic!("shadow mode must fail closed instead of converting DELETE to WAL"),
            Err(err) => err,
        };
        let msg = err.to_string();
        assert!(msg.contains("journal_mode"), "{msg}");
        assert!(msg.contains("WAL"), "{msg}");

        let conn = Connection::open(&db_path).unwrap();
        let mode: String = conn
            .query_row("PRAGMA journal_mode", [], |row| row.get(0))
            .unwrap();
        assert_eq!(mode.to_lowercase(), "delete");
    }

    #[tokio::test]
    async fn test_restore_read_cursor_resumes_without_re_reading_wal() {
        // B4 restart-window: after a restart, seeding the persisted read cursor
        // (offset + salt + running chain) must resume the live-WAL read from the
        // persisted offset — reading only NEW frames — instead of re-reading
        // (and re-appending) the whole live WAL from offset 0.
        let dir = tempdir().unwrap();
        let db_path = dir.path().join("resume.db");
        let conn = Connection::open(&db_path).unwrap();
        conn.execute_batch(
            "
            PRAGMA journal_mode=WAL;
            PRAGMA wal_autocheckpoint=0;
            CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT);
            ",
        )
        .unwrap();
        // Many separate commits so the live WAL holds a large, countable set of
        // frames to (not) re-read.
        for i in 0..30 {
            conn.execute("INSERT INTO t (v) VALUES (?1)", [format!("row-{i}")])
                .unwrap();
        }

        // Pre-restart process copies the current WAL frames.
        let mut shadow = ShadowWal::new(&db_path).await.unwrap();
        let (frames, offset) = shadow.copy_frames(0).await.unwrap();
        assert!(
            frames.len() >= 10,
            "initial copy must read the committed frames"
        );
        let saved_salt = shadow.wal_read_salt();
        let saved_chain = shadow.wal_read_chain();
        assert!(
            saved_salt.is_some(),
            "a real header must have been observed"
        );
        drop(shadow);

        // Baseline restart WITHOUT the persisted cursor: reading from offset 0
        // re-reads the whole live WAL (the pre-B4 behavior).
        let mut fresh = ShadowWal::new(&db_path).await.unwrap();
        let (reread, _) = fresh.copy_frames(0).await.unwrap();
        drop(fresh);

        // Restart WITH the persisted cursor: resume from `offset`, reading only
        // frames committed since (here just the checkpoint-blocker's own frame),
        // never the already-copied data frames.
        let mut restarted = ShadowWal::new(&db_path).await.unwrap();
        restarted.restore_read_cursor(saved_salt, saved_chain);
        let (resumed_frames, resumed_offset) = restarted.copy_frames(offset).await.unwrap();
        assert!(
            resumed_frames.len() < reread.len(),
            "resume ({}) must read far fewer frames than a from-0 re-read ({})",
            resumed_frames.len(),
            reread.len()
        );
        assert!(
            resumed_offset >= offset,
            "the resumed cursor must never rewind below the persisted offset"
        );

        // A new commit after restart is picked up from the resumed cursor.
        conn.execute("INSERT INTO t (v) VALUES ('after-restart')", [])
            .unwrap();
        let (new_frames, _) = restarted.copy_frames(resumed_offset).await.unwrap();
        assert!(
            !new_frames.is_empty(),
            "frames committed after restart must be copied from the resumed cursor"
        );
    }

    #[tokio::test]
    async fn test_restore_read_cursor_detects_downtime_checkpoint() {
        // B4 restart-window: if an external checkpoint reset the WAL while the
        // process was down, the persisted salt no longer matches the live WAL.
        // Seeding the persisted salt via restore_read_cursor lets the first
        // post-restart copy_frames detect the rollover (generation bump) instead
        // of silently continuing on the wrong generation.
        let dir = tempdir().unwrap();
        let db_path = dir.path().join("downtime-ckpt.db");
        let conn = Connection::open(&db_path).unwrap();
        conn.execute_batch(
            "
            PRAGMA journal_mode=WAL;
            PRAGMA wal_autocheckpoint=0;
            CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT);
            INSERT INTO t (v) VALUES ('a');
            INSERT INTO t (v) VALUES ('b');
            ",
        )
        .unwrap();

        let mut shadow = ShadowWal::new(&db_path).await.unwrap();
        let (_frames, offset) = shadow.copy_frames(0).await.unwrap();
        let saved_salt = shadow.wal_read_salt();
        let saved_chain = shadow.wal_read_chain();
        drop(shadow);

        // Simulate a checkpoint during downtime: TRUNCATE resets the WAL and the
        // next writes take a fresh salt.
        let _: (i64, i64, i64) = conn
            .query_row("PRAGMA wal_checkpoint(TRUNCATE);", [], |row| {
                Ok((row.get(0)?, row.get(1)?, row.get(2)?))
            })
            .unwrap();
        conn.execute("INSERT INTO t (v) VALUES ('c')", []).unwrap();

        let mut restarted = ShadowWal::new(&db_path).await.unwrap();
        let base_generation = restarted.generation();
        restarted.restore_read_cursor(saved_salt, saved_chain);
        restarted.copy_frames(offset).await.unwrap();
        assert!(
            restarted.generation() > base_generation,
            "seeding the persisted salt must let the restart detect the downtime checkpoint (generation must advance from {base_generation})"
        );
    }

    #[tokio::test]
    async fn test_shadow_reseeds_salt_and_page_size_when_wal_appears_after_startup() {
        // B5: a DB whose WAL is absent/empty at ShadowWal construction (e.g. just
        // after a TRUNCATE checkpoint) must re-read the real page_size and salt
        // from the header when the WAL reappears — instead of being stuck at the
        // 4096 / (0,0) defaults, which would (a) mis-frame the readback path and
        // (b) make every later genuine checkpoint invisible.
        let dir = tempdir().unwrap();
        let db_path = dir.path().join("reseed.db");

        // Non-default page size so the 4096 default is detectably wrong.
        let conn = Connection::open(&db_path).unwrap();
        conn.execute_batch(
            "
            PRAGMA page_size=8192;
            PRAGMA journal_mode=WAL;
            PRAGMA wal_autocheckpoint=0;
            CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT);
            INSERT INTO t VALUES (1, 'a');
            ",
        )
        .unwrap();
        // Truncate the WAL so no header exists at construction time.
        let _: (i64, i64, i64) = conn
            .query_row("PRAGMA wal_checkpoint(TRUNCATE);", [], |row| {
                Ok((row.get(0)?, row.get(1)?, row.get(2)?))
            })
            .unwrap();
        drop(conn);

        // WAL is now empty (< 32 bytes) -> ShadowWal seeds defaults.
        let mut shadow = ShadowWal::new(&db_path).await.unwrap();
        assert_eq!(shadow.generation(), 0);

        // Write new frames so a real WAL header appears.
        let writer = Connection::open(&db_path).unwrap();
        writer
            .execute_batch("PRAGMA wal_autocheckpoint=0;")
            .unwrap();
        writer.execute("INSERT INTO t VALUES (2, 'b')", []).unwrap();

        let (frames, _offset) = shadow.copy_frames(0).await.unwrap();
        assert!(!frames.is_empty(), "new WAL frames should be copied");
        // B5: page_size re-seeded from the real header, not stuck at 4096. Because
        // the `!header_seeded` seeding branch sets page_size AND wal_salt together
        // from the same header, a correct page_size proves the salt was also
        // seeded off the (0,0) sentinel (so later checkpoints are now detectable).
        assert_eq!(shadow.page_size(), 8192, "page_size must be re-seeded");
        assert!(shadow.wal_salt_seeded(), "salt must be seeded off (0,0)");
        // First appearance is initialization, not a rollover.
        assert_eq!(shadow.generation(), 0, "seeding must not bump generation");
    }
}
