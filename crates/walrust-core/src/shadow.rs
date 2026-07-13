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
    /// Set when `copy_frames` observed an event that requires the stream to be
    /// re-anchored with a fresh snapshot instead of chaining incrementals: the
    /// live WAL vanished / went zero-length / read back with an all-zero header
    /// after a header had been observed — SQLite's last-close checkpoint on an
    /// ephemeral-connection writer (DF1). The checkpoint folded pages into the
    /// `.db` that may never have been copied to the shadow; only a fresh
    /// snapshot provably captures them. Consumed via `take_reanchor_pending`;
    /// the consumer must publish an eager snapshot, exactly like the D3
    /// downtime-checkpoint path. (A plain mid-run salt roll does NOT set this:
    /// that is the routine own-checkpoint shape, already copied + synced.)
    reanchor_pending: bool,
    /// True while the live WAL has vanished after frames were observed and has
    /// not yet reappeared with a real header (DF2). An ephemeral-connection
    /// writer folds each commit straight into the `.db` and deletes/truncates
    /// the WAL before the next tick, so the WAL is empty every tick and the
    /// per-tick re-arm (guarded by the `.db` change counter in the watch loop)
    /// is the only way those folded writes get captured.
    wal_absent: bool,
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

        // Read current WAL header to get page size and salt. Missing, zero-
        // length, and zeroed-header WALs are all legal lifecycle states at
        // construction time (last-close checkpoint, ephemeral-connection
        // writers — DF1); only a nonzero garbage magic errors.
        let wal_path = db_path.with_extension("db-wal");
        let (page_size, salt1, salt2, header_seeded) =
            match wal::read_header_classified(&wal_path).await? {
                wal::WalHeaderRead::Valid(header) => {
                    (header.page_size, header.salt1, header.salt2, true)
                }
                // Defaults until a WAL header appears (B5)
                wal::WalHeaderRead::Missing
                | wal::WalHeaderRead::TooShort
                | wal::WalHeaderRead::Zeroed => (4096, 0, 0, false),
            };

        // Find highest existing generation
        let generation = Self::find_latest_generation(&shadow_dir)
            .await?
            .unwrap_or(0);

        // E1: heal any torn shadow-segment tail left by a crash mid-write.
        //
        // Shadow segments are a stream of fixed-size frames (24-byte header +
        // page_size data). A `kill -9` between the header write and the page
        // write (or partway through either) leaves a partial trailing frame.
        // On restart `copy_frames` appends fresh frames after that torn tail in
        // append mode, which shifts every following frame off its boundary. The
        // reader (`encode_shadow_to_ltx`) then decodes garbage headers, and a
        // header landing on a frame's zero-padded region yields page number 0,
        // which litepages rejects as "Invalid page num: transaction ID must be
        // non-zero" — sync then fails forever. Truncating every segment down to
        // a whole-frame boundary before the first append removes the torn tail;
        // the durable `wal_copy_offset` re-copies any dropped frames cleanly.
        let frame_page_size = db_header_page_size(db_path).unwrap_or(page_size);
        Self::align_truncate_segments(&shadow_dir, frame_page_size).await?;

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
            reanchor_pending: false,
            wal_absent: false,
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
    pub(crate) fn open_checkpoint_blocker(db_path: &Path) -> Result<Connection> {
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

    /// Truncate every shadow segment down to a whole-frame boundary, dropping
    /// any partial trailing frame left by a crash mid-write (E1). Segments that
    /// are already frame-aligned are left untouched, so this is a cheap no-op on
    /// a clean start. `page_size` determines the frame size; a `page_size` of 0
    /// (no header ever seen) means there can be no real frames yet, so nothing
    /// is done.
    async fn align_truncate_segments(shadow_dir: &Path, page_size: u32) -> Result<()> {
        if page_size == 0 {
            return Ok(());
        }
        let frame_size = FRAME_HEADER_SIZE + page_size as u64;

        let mut entries = fs::read_dir(shadow_dir).await?;
        while let Some(entry) = entries.next_entry().await? {
            let name = entry.file_name();
            if !name.to_string_lossy().ends_with(".wal") {
                continue;
            }
            let path = entry.path();
            let size = match fs::metadata(&path).await {
                Ok(meta) => meta.len(),
                Err(_) => continue,
            };
            let aligned = (size / frame_size) * frame_size;
            if aligned != size {
                tracing::warn!(
                    "Shadow WAL: healing torn segment {} ({} -> {} bytes, dropped {} partial-frame byte(s))",
                    path.display(),
                    size,
                    aligned,
                    size - aligned
                );
                let file = OpenOptions::new().write(true).open(&path).await?;
                file.set_len(aligned).await?;
                file.sync_all().await?;
            }
        }
        Self::fsync_dir(shadow_dir).await?;
        Ok(())
    }

    /// The live WAL vanished / zeroed out from under an established read cursor
    /// (DF1): SQLite's last-close checkpoint folded every frame into the main
    /// `.db` file, deleted the WAL, and the next ephemeral writer will recreate
    /// it with a fresh salt. Roll the shadow to a new generation (the old WAL's
    /// frames can never legally continue) and flag the stream for an eager
    /// snapshot re-anchor — the `.db` file now holds pages the shadow may never
    /// have copied, and only a fresh snapshot captures them (same reasoning as
    /// the D3 downtime-checkpoint and WAL-rollover paths).
    fn begin_vanished_wal_reanchor(&mut self, observed: &str) {
        tracing::warn!(
            "{}: live WAL {} after frames had been observed — SQLite last-close \
             checkpoint (ephemeral-connection writer) deleted/recreated the WAL; \
             starting shadow generation {} and re-anchoring with a fresh snapshot",
            self.db_path.display(),
            observed,
            self.generation + 1
        );
        self.generation += 1;
        self.segment_index = 0;
        self.segment_offset = 0;
        self.wal_chain = None;
        // The next real header is a fresh initialization (B5 seeding), not a
        // second rollover.
        self.header_seeded = false;
        // Stay in the "absent" state until a real WAL header reappears, so that
        // while an ephemeral-connection writer keeps folding rows into the .db
        // (WAL deleted before every tick), each tick re-arms the re-anchor
        // (DF2) instead of falling into the quiet never-seeded idle branch.
        self.wal_absent = true;
        self.reanchor_pending = true;
    }

    /// Copy new WAL frames to the shadow WAL
    ///
    /// Returns the number of frames copied
    pub async fn copy_frames(&mut self, offset: u64) -> Result<(Vec<ParsedFrame>, u64)> {
        let wal_path = self.db_path.with_extension("db-wal");

        // Read + classify the WAL header. Missing / zero-length / zeroed-header
        // are legal lifecycle states of an ephemeral-connection writer (DF1),
        // never process death; a NONZERO garbage magic still errors loudly here.
        let header = match wal::read_header_classified(&wal_path).await? {
            wal::WalHeaderRead::Valid(h) => h,
            observed @ (wal::WalHeaderRead::Missing
            | wal::WalHeaderRead::TooShort
            | wal::WalHeaderRead::Zeroed) => {
                let observed = match observed {
                    wal::WalHeaderRead::Missing => "missing",
                    wal::WalHeaderRead::TooShort => "zero-length/truncated",
                    _ => "zeroed (header read back all-zero, magic 0x0)",
                };
                if self.header_seeded {
                    // First detection of this vanish: roll the generation and
                    // flag the eager re-anchor (DF1).
                    self.begin_vanished_wal_reanchor(observed);
                    return Ok((Vec::new(), 0));
                }
                if self.wal_absent {
                    // DF2: the WAL is STILL gone from an earlier vanish and has
                    // not reappeared with frames. An ephemeral-connection
                    // writer (last-close checkpoint every commit) keeps folding
                    // rows straight into the `.db` and deleting/truncating the
                    // WAL before the next tick can read a single frame — the
                    // silent-stall the 0.7.0 fresh-user drill hit. Re-arm the
                    // re-anchor every tick so the watch loop re-checks the
                    // `.db` content checksum and snapshots when it actually
                    // advanced (the checksum guard stops an idle DB from
                    // snapshot-storming). Do NOT re-roll the generation here.
                    self.reanchor_pending = true;
                    return Ok((Vec::new(), offset));
                }
                // No WAL ever observed: the normal idle state of a database
                // with nothing to replicate yet.
                Self::ensure_database_in_wal_mode(&self.db_path).await?;
                return Ok((Vec::new(), offset));
            }
        };
        // A real WAL header is back: clear the absent state so a later vanish is
        // detected fresh (rolls a new generation) rather than only re-armed.
        self.wal_absent = false;

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
            // NOTE (DF1 adjacency): a mid-run salt roll is the ROUTINE shape —
            // the shadow's own checkpoint path copies + durably syncs every
            // frame before it lets the WAL restart, so no re-anchor is needed
            // here. The vanish/zeroed lifecycle above is different: there the
            // fold cannot be proven to have been preceded by a complete copy,
            // so it flags an eager snapshot re-anchor.
        }

        // Read new frames from the active WAL through the CHECKED reader so torn
        // or stale (salt-mismatched) frames never enter the shadow (B4). The
        // running chain is carried across incremental reads within this process.
        let read_result = wal::read_frames_as_pages_checked(
            &wal_path,
            header.page_size,
            effective_offset,
            self.wal_chain,
        )
        .await;
        let (frames, new_offset, _max_db_size, out_chain) = match read_result {
            Ok(result) => result,
            Err(read_err) => {
                // The read raced the last-close delete/recreate window (DF1):
                // if the WAL is no longer the one whose header we just
                // validated (gone, zeroed, or already re-created with a new
                // salt), the parsed bytes are untrustworthy AND the event is
                // the legal lifecycle state — re-anchor instead of dying. If
                // the same WAL is still there, the failure is genuine.
                let raced = match wal::read_header_classified(&wal_path).await {
                    Ok(wal::WalHeaderRead::Valid(now)) => (now.salt1, now.salt2) != current_salt,
                    Ok(_) => true,
                    Err(_) => false,
                };
                if raced {
                    self.begin_vanished_wal_reanchor(
                        "reset while its frames were being read (frame read failed)",
                    );
                    return Ok((Vec::new(), 0));
                }
                return Err(read_err);
            }
        };
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

    /// Consume the pending re-anchor flag (DF1). Returns `true` when
    /// `copy_frames` observed a WAL delete/recreate lifecycle event since the
    /// last consumption. The caller MUST react
    /// by publishing an eager snapshot — the same re-anchor the D3
    /// downtime-checkpoint startup path performs — and resetting its WAL copy
    /// cursor to 0; the `.db` file may hold checkpoint-folded pages the shadow
    /// never copied.
    pub fn take_reanchor_pending(&mut self) -> bool {
        std::mem::take(&mut self.reanchor_pending)
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
                reanchor_pending: false,
                wal_absent: false,
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

    /// Fix gate: `ShadowWal::new` heals a torn segment tail on restart, so the
    /// next append stays frame-aligned and the reader decodes the real pages
    /// instead of a bogus page 0. With the healing disabled this fails because
    /// the torn tail persists and `encode_shadow_to_ltx` errors.
    #[tokio::test]
    async fn e1_shadow_new_heals_torn_tail_so_sync_resumes() {
        use crate::legacy_shadow::{encode_shadow_to_ltx, ShadowSyncInput};

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

        let frame_size = FRAME_HEADER_SIZE + E1_PAGE_SIZE as u64;
        let shadow_dir = ShadowWal::shadow_dir_for(&db_path);
        std::fs::create_dir_all(&shadow_dir).unwrap();
        let seg = shadow_dir.join(format_segment_name(0, 0));

        // One committed frame plus a 30-byte torn tail (crash mid-write).
        let mut bytes = e1_frame(1, 1, 0xAB);
        bytes.extend_from_slice(&[0u8; 30]);
        std::fs::write(&seg, &bytes).unwrap();

        // Restart discovery must heal the torn tail.
        let _shadow = ShadowWal::new(&db_path).await.unwrap();
        assert_eq!(
            std::fs::metadata(&seg).unwrap().len(),
            frame_size,
            "restart must truncate the torn tail to a whole-frame boundary"
        );

        // Simulate the post-restart append that copy_frames performs.
        {
            use std::io::Write;
            let mut f = std::fs::OpenOptions::new().append(true).open(&seg).unwrap();
            f.write_all(&e1_frame(2, 2, 0xCD)).unwrap();
        }

        let input = ShadowSyncInput {
            db_path: db_path.clone(),
            name: "app".into(),
            current_txid: 10,
            db_checksum: Some(123),
            generation: 0,
            shadow_sync_offset: 0,
            page_size: E1_PAGE_SIZE,
            shadow_dir: shadow_dir.clone(),
        };

        let (result, _offset) = encode_shadow_to_ltx(&input)
            .expect("healed segment must encode cleanly")
            .expect("committed frames must produce an LTX buffer");
        assert_eq!(
            result.unique_pages, 2,
            "both real pages (1 and 2) must decode, with no bogus page 0"
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

    // ── DF1: ephemeral-connection writer WAL delete/recreate lifecycle ──────
    //
    // SQLite deletes the WAL when the last connection on a WAL database closes
    // and recreates it on the next write. A writer that opens one connection
    // per INSERT (shell scripts, cron jobs) cycles the WAL through missing /
    // zero-length / zeroed-header states routinely. The 2026-07-11 dogfooding
    // run of the published 0.7.0 binary died on exactly this:
    // "ERROR Shadow copy failed: Invalid WAL magic number: 0x0" → process exit.

    /// Open → write → close, one connection per statement — the exact writer
    /// shape every prior drill missed (they all held one long-lived connection).
    fn ephemeral_exec(db_path: &Path, sql: &str) {
        let conn = Connection::open(db_path).unwrap();
        conn.execute_batch("PRAGMA journal_mode=WAL;").unwrap();
        conn.execute_batch(sql).unwrap();
        // conn drops here: if it is the last connection, SQLite checkpoints
        // and deletes the WAL (the DF1 lifecycle).
    }

    /// Build a real DB via ephemeral connections and a ShadowWal that has
    /// observed its WAL (seeded header + copied frames).
    async fn df1_seeded_shadow(dir: &tempfile::TempDir) -> (PathBuf, ShadowWal, u64) {
        let db_path = dir.path().join("ephemeral.db");
        ephemeral_exec(&db_path, "CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT);");
        ephemeral_exec(&db_path, "INSERT INTO t (v) VALUES ('a');");

        let mut shadow = ShadowWal::new(&db_path).await.unwrap();
        // The blocker connection keeps the DB open now, so further ephemeral
        // closes no longer delete the WAL; write one more row and copy.
        ephemeral_exec(&db_path, "INSERT INTO t (v) VALUES ('b');");
        let (frames, offset) = shadow.copy_frames(0).await.unwrap();
        assert!(!frames.is_empty(), "setup must observe real WAL frames");
        assert!(shadow.wal_read_salt().is_some(), "header must be seeded");
        assert!(
            !shadow.take_reanchor_pending(),
            "setup must not leave a re-anchor pending"
        );
        (db_path, shadow, offset)
    }

    /// THE DF1 repro: the WAL read back with an all-zero header (magic 0x0),
    /// the transient state of SQLite's last-close checkpoint + recreate. The
    /// shadow copy must treat it as a re-anchor trigger — generation roll +
    /// pending eager snapshot — never an error (which killed the watch process).
    /// Revert-proof: with the fix disabled, `copy_frames` here returns
    /// Err("Invalid WAL magic number: 0x0") and this test fails.
    #[tokio::test]
    async fn df1_zeroed_wal_header_reanchors_instead_of_dying() {
        let dir = tempdir().unwrap();
        let (db_path, mut shadow, offset) = df1_seeded_shadow(&dir).await;
        let generation_before = shadow.generation();

        // Drive the exact interleaving: between ticks, the WAL header region is
        // zeroed (delete/recreate midpoint observed by the dogfooding run).
        let wal_path = db_path.with_extension("db-wal");
        {
            use std::io::{Seek, SeekFrom, Write};
            let mut f = std::fs::OpenOptions::new()
                .write(true)
                .open(&wal_path)
                .unwrap();
            f.seek(SeekFrom::Start(0)).unwrap();
            f.write_all(&[0u8; 32]).unwrap();
            f.sync_all().unwrap();
        }

        let (frames, new_offset) = shadow
            .copy_frames(offset)
            .await
            .expect("a zeroed WAL header is a legal lifecycle state, not process death");
        assert!(frames.is_empty(), "no frames can come from a zeroed WAL");
        assert_eq!(new_offset, 0, "the read cursor must reset for the new WAL");
        assert!(
            shadow.generation() > generation_before,
            "the dead WAL's generation must be rolled"
        );
        assert!(
            shadow.take_reanchor_pending(),
            "a zeroed WAL must flag an eager snapshot re-anchor"
        );
    }

    /// The unlinked-WAL variant of the same lifecycle: the file is gone
    /// entirely. Pre-fix code returned Ok silently with NO re-anchor — the
    /// folded-but-never-copied frames would be missing from the stream until
    /// the next periodic snapshot (a silent-loss window). Revert-proof: with
    /// the fix disabled the re-anchor flag stays false and this test fails.
    #[tokio::test]
    async fn df1_missing_wal_after_observed_frames_reanchors() {
        let dir = tempdir().unwrap();
        let (db_path, mut shadow, offset) = df1_seeded_shadow(&dir).await;
        let generation_before = shadow.generation();

        // SQLite's last-close checkpoint removes the WAL and its shm together.
        std::fs::remove_file(db_path.with_extension("db-wal")).unwrap();
        std::fs::remove_file(db_path.with_extension("db-shm")).unwrap();

        let (frames, new_offset) = shadow
            .copy_frames(offset)
            .await
            .expect("a missing WAL is a legal lifecycle state");
        assert!(frames.is_empty());
        assert_eq!(new_offset, 0);
        assert!(shadow.generation() > generation_before);
        assert!(
            shadow.take_reanchor_pending(),
            "a WAL that vanished mid-stream must flag an eager snapshot re-anchor"
        );
        assert!(
            shadow.wal_read_salt().is_none(),
            "the vanish must clear the seeded header (fresh init on WAL return)"
        );

        // DF2: while the WAL stays absent, every subsequent tick must RE-ARM the
        // re-anchor (an ephemeral-connection writer keeps folding rows into the
        // .db with the WAL deleted before each tick — the silent-stall shape).
        // The generation is NOT re-rolled; the watch loop's .db-change-counter
        // guard, not a quiet idle branch, is what prevents a snapshot storm.
        let gen_after_first = shadow.generation();
        let (frames, _off) = shadow.copy_frames(0).await.unwrap();
        assert!(frames.is_empty());
        assert_eq!(
            shadow.generation(),
            gen_after_first,
            "a still-absent WAL must not re-roll the generation each tick"
        );
        assert!(
            shadow.take_reanchor_pending(),
            "DF2: a still-absent WAL must re-arm the re-anchor every tick so folded \
             writes get captured (pre-DF2-fix this fell into the quiet idle branch)"
        );
    }

    /// A fresh start (no WAL observed before this process's own blocker wrote
    /// one) is initialization, not a re-anchor: no flag, no generation roll.
    /// Guards against a re-anchor snapshot storm on freshly-watched databases.
    #[tokio::test]
    async fn df1_fresh_start_does_not_reanchor() {
        let dir = tempdir().unwrap();
        let db_path = dir.path().join("idle.db");
        ephemeral_exec(&db_path, "CREATE TABLE t (id INTEGER PRIMARY KEY);");
        // The ephemeral close deleted the WAL; ShadowWal starts unseeded (its
        // own checkpoint-blocker write then recreates the WAL fresh).
        let mut shadow = ShadowWal::new(&db_path).await.unwrap();
        assert!(shadow.wal_read_salt().is_none(), "setup: no WAL observed");

        let (_frames, _offset) = shadow.copy_frames(0).await.unwrap();
        assert_eq!(
            shadow.generation(),
            0,
            "first header observation is B5 seeding, not a rollover"
        );
        assert!(
            !shadow.take_reanchor_pending(),
            "a fresh start must not trigger re-anchor snapshots"
        );
    }

    /// Corruption pin: a NONZERO garbage magic is NOT the lifecycle state and
    /// must keep failing loudly. The DF1 fix must not have widened into
    /// swallowing real corruption.
    #[tokio::test]
    async fn df1_nonzero_garbage_wal_magic_still_fails_loudly() {
        let dir = tempdir().unwrap();
        let (db_path, mut shadow, offset) = df1_seeded_shadow(&dir).await;

        let wal_path = db_path.with_extension("db-wal");
        {
            use std::io::{Seek, SeekFrom, Write};
            let mut f = std::fs::OpenOptions::new()
                .write(true)
                .open(&wal_path)
                .unwrap();
            f.seek(SeekFrom::Start(0)).unwrap();
            f.write_all(&0xDEAD_BEEFu32.to_be_bytes()).unwrap();
            f.sync_all().unwrap();
        }

        let err = shadow
            .copy_frames(offset)
            .await
            .expect_err("garbage WAL magic must stay a loud error");
        assert!(err.to_string().contains("Invalid WAL magic"), "got: {err}");
        assert!(
            !shadow.take_reanchor_pending(),
            "corruption must not be blurred into the re-anchor lifecycle path"
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
