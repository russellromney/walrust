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

/// Format a shadow segment filename: `{generation:016x}-{index:016x}.wal`.
fn format_segment_name(generation: u64, index: u64) -> String {
    format!(
        "{:0width$x}-{:0width$x}.wal",
        generation,
        index,
        width = SEGMENT_HEX_WIDTH
    )
}

fn ensure_connection_in_wal_mode(conn: &Connection, db_path: &Path) -> Result<()> {
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
        let generation = Self::find_latest_generation(&shadow_dir)
            .await?
            .unwrap_or(0);

        // Open read connection to prevent auto-checkpoint
        let checkpoint_blocker = Self::open_checkpoint_blocker(db_path)?;

        Ok(Self {
            db_path: db_path.to_path_buf(),
            shadow_dir,
            generation,
            segment_index: 0,
            segment_offset: 0,
            page_size,
            checkpoint_blocker: Some(Arc::new(Mutex::new(checkpoint_blocker))),
            wal_salt: (salt1, salt2),
            wal_chain: None,
            header_seeded,
        })
    }

    /// Get the shadow directory path for a database
    pub fn shadow_dir_for(db_path: &Path) -> PathBuf {
        let parent = db_path.parent().unwrap_or(Path::new("."));
        let db_name = db_path.file_stem().unwrap_or_default();
        parent.join(format!(".walrust-{}", db_name.to_string_lossy()))
    }

    /// Open a read connection that prevents SQLite from auto-checkpointing
    fn open_checkpoint_blocker(db_path: &Path) -> Result<Connection> {
        let conn = Connection::open(db_path)?;

        ensure_connection_in_wal_mode(&conn, db_path)?;

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

        // Pin a real WAL frame. Reading sqlite_master can leave the blocker at
        // read-mark 0, which does not prevent walRestartLog on later frames.
        conn.execute_batch("BEGIN DEFERRED;")?;
        let _: i64 = conn.query_row("SELECT value FROM _walrust_seq WHERE id = 1", [], |row| {
            row.get(0)
        })?;

        tracing::debug!("Opened checkpoint blocker for {}", db_path.display());

        Ok(conn)
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

    /// Copy new WAL frames to the shadow WAL
    ///
    /// Returns the number of frames copied
    pub async fn copy_frames(&mut self, offset: u64) -> Result<(Vec<ParsedFrame>, u64)> {
        let wal_path = self.db_path.with_extension("db-wal");

        // Check if WAL exists
        if !wal_path.exists() {
            Self::ensure_database_in_wal_mode(&self.db_path).await?;
            return Ok((Vec::new(), offset));
        }

        // Read WAL header to check for checkpoint (salt change)
        let header = match wal::read_header(&wal_path).await? {
            Some(h) => h,
            None => {
                Self::ensure_database_in_wal_mode(&self.db_path).await?;
                return Ok((Vec::new(), offset));
            }
        };

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
            tracing::info!(
                "Shadow WAL: checkpoint detected (salt changed), starting generation {}",
                self.generation + 1
            );
            self.generation += 1;
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
        self.wal_chain = out_chain;

        if frames.is_empty() {
            return Ok((Vec::new(), new_offset));
        }

        // Write frames to current shadow segment
        self.write_frames_to_segment(&frames, header.page_size)
            .await?;

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
        let segment_path = self.current_segment_path();

        // Open or create segment file
        let mut file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&segment_path)
            .await?;

        // Write each frame (header + page data)
        for frame in frames {
            // Write frame header (24 bytes)
            let mut header = [0u8; 24];
            header[0..4].copy_from_slice(&frame.page_number.to_be_bytes());
            header[4..8].copy_from_slice(&frame.db_size.to_be_bytes());
            // Salt and checksum can be zeros for shadow (we verify on read)
            file.write_all(&header).await?;

            // Write page data
            file.write_all(&frame.data).await?;

            self.segment_offset += FRAME_HEADER_SIZE + page_size as u64;
        }

        file.flush().await?;
        file.sync_all().await?;
        Self::fsync_dir(&self.shadow_dir).await?;
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

    /// Trigger a manual checkpoint and rotate shadow WAL
    ///
    /// This releases the read transaction, runs checkpoint, then re-establishes the blocker
    pub async fn checkpoint(&mut self) -> Result<()> {
        // Release checkpoint blocker
        if let Some(blocker) = self.checkpoint_blocker.take() {
            let conn = blocker.lock().await;
            conn.execute_batch("ROLLBACK;")?;
            drop(conn);
        }

        let checkpoint_result = {
            let conn = Connection::open(&self.db_path)?;
            conn.busy_timeout(Duration::from_secs(5))?;
            let (busy, log_frames, checkpointed_frames): (i64, i64, i64) =
                conn.query_row("PRAGMA wal_checkpoint(PASSIVE);", [], |row| {
                    Ok((row.get(0)?, row.get(1)?, row.get(2)?))
                })?;
            if busy != 0 || checkpointed_frames < log_frames {
                Err(anyhow!(
                    "{}: shadow checkpoint incomplete (busy={}, log_frames={}, checkpointed_frames={})",
                    self.db_path.display(),
                    busy,
                    log_frames,
                    checkpointed_frames
                ))
            } else {
                Ok(())
            }
        };

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
            "Shadow WAL: checkpoint complete for {}",
            self.db_path.display()
        );

        Ok(())
    }

    /// Clean up old shadow segments after successful upload
    pub async fn cleanup_segments(&self, up_to_generation: u64) -> Result<usize> {
        let mut deleted = 0;

        let mut entries = fs::read_dir(&self.shadow_dir).await?;
        while let Some(entry) = entries.next_entry().await? {
            let name = entry.file_name();
            let name_str = name.to_string_lossy();

            if name_str.ends_with(".wal") {
                if let Some(gen_str) = name_str.split('-').next() {
                    if let Ok(gen) = u64::from_str_radix(gen_str, 16) {
                        if gen < up_to_generation {
                            fs::remove_file(entry.path()).await?;
                            deleted += 1;
                        }
                    }
                }
            }
        }

        if deleted > 0 {
            tracing::debug!(
                "Shadow WAL: cleaned up {} old segments (generations < {})",
                deleted,
                up_to_generation
            );
        }

        Ok(deleted)
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
