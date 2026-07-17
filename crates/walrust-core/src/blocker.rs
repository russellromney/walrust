//! Checkpoint-blocker lifecycle: the retained per-database handles.
//!
//! The checkpoint blocker's protection has TWO parts, and both must survive
//! for the whole watch lifetime:
//!
//! 1. **The pinned read transaction** (WAL read mark). This is an `fcntl`
//!    lock taken on the `-shm` file's inode. It makes another process's
//!    `wal_checkpoint(TRUNCATE)`/`RESTART` report busy, and it is robust:
//!    the `-shm` descriptor is a per-process singleton inside SQLite's unix
//!    VFS and is never closed while a connection lives.
//!
//! 2. **The SHARED POSIX lock on the main database inode**, held by the
//!    blocker connection for its whole lifetime (measured: `F_RDLCK` at
//!    `PENDING_BYTE+2`..`+510`, held across ROLLBACK/BEGIN/COMMIT). This is
//!    the ONLY thing that stops another process's last-connection close from
//!    acquiring the EXCLUSIVE main-db lock its close-time checkpoint needs.
//!    Without it, SQLite's close path runs a PASSIVE checkpoint (which
//!    returns SQLITE_OK even when readers pin part of the WAL) and then
//!    **unlinks the `-wal` and `-shm` files** — silently discarding frames
//!    the blocker pinned, and zombifying the blocker (new connections
//!    attach to a fresh `-shm` whose wal-index knows nothing of the old
//!    read mark).
//!
//! Classic POSIX semantics are the hazard: closing ANY file descriptor a
//! process holds for an inode releases ALL of the process's `fcntl` locks on
//! that inode. So one stray RAW `File::open(db)` + close after arming — a
//! page-size read, a change-counter read, a direct snapshot encode —
//! destroys part 2 while the blocker object still appears alive (SQLite's
//! bookkeeping still shows the lock; the kernel no longer has it). The next
//! short-lived writer's close then unlinks the WAL (measured on macOS:
//! external TRUNCATE still `busy=1`, and the WAL gone one close later).
//! SQLite-level connection churn is different: the unix VFS parks a closing
//! connection's fd while the inode has outstanding locks, so same-process
//! `Connection::open`/close cycles do not release the blocker's locks. The
//! lifecycle still routes every main-DB access through retained handles —
//! there is no reason to reopen what is already open.
//!
//! The lifecycle contract (D2 hardening):
//!
//! - Open every retained handle BEFORE arming the blocker; arm the blocker
//!   LAST.
//! - While armed, never open, clone, or close another descriptor for the
//!   main DB. All main-db access borrows the retained handles.
//! - The blocker connection is only ROLLBACKed and re-pinned — never dropped
//!   and reopened — so the SHARED main-db lock never leaves the process. The
//!   controlled checkpoint runs on it after ROLLBACK (it is in autocommit
//!   then).
//! - `PRAGMA data_version` on the idle observer connection detects
//!   application commits that land in the release/re-acquire window (the
//!   sample is taken before the heartbeat/re-pin writes that would absorb
//!   the change).
//! - Close order on shutdown: blocker, then monitor, then source descriptor
//!   (struct field declaration order).

use std::path::Path;

use anyhow::{anyhow, Context, Result};
use rusqlite::Connection;

use crate::shadow::ensure_connection_in_wal_mode;

/// The retained per-database handles for the checkpoint-blocker lifecycle.
/// See the module docs for the contract.
pub struct BlockerLifecycle {
    /// The armed checkpoint blocker: holds the pinned read transaction
    /// (read mark on the `-shm` inode) and, for its whole lifetime, the
    /// process's SHARED `fcntl` lock on the main database inode. Declared
    /// first so it closes first on drop (module docs: close order). Between
    /// dances it only pins/re-pins; the controlled checkpoint and heartbeat
    /// run on it after ROLLBACK (it is in autocommit then).
    blocker_conn: Connection,
    /// The data-version observer connection (autocommit): samples
    /// `PRAGMA data_version` around the controlled-checkpoint window and
    /// serves snapshot-encoding borrows (`PRAGMA page_size`, passive folds,
    /// `VACUUM INTO`). It NEVER runs the controlled checkpoint itself: a
    /// connection's own checkpoint absorbs wal-index changes into its cached
    /// header without a pager reset, which would mask a window commit from
    /// `PRAGMA data_version`.
    monitor_conn: Connection,
    /// Retained read-only descriptor for raw main-db reads (snapshot
    /// encoding, checksum, change counter). Opened before the blocker arms
    /// and never closed while armed.
    source_fd: std::fs::File,
}

/// Outcome of a [`BlockerLifecycle::controlled_checkpoint`] dance.
#[derive(Debug, Clone, Copy)]
pub struct ControlledCheckpoint {
    /// `PRAGMA data_version` proved an application commit landed in the
    /// release window (PASSIVE dance only; always false for TRUNCATE — see
    /// [`BlockerLifecycle::controlled_checkpoint`]).
    pub commit_in_window: bool,
    /// Frames the checkpoint folded (third `PRAGMA wal_checkpoint` column).
    /// Callers holding a copied WAL prefix compare this against their copied
    /// extent to catch commits folded **before** the window sample: a commit
    /// that lands between the last shadow copy and the window is folded by
    /// this checkpoint and then erased by the re-pin WAL restart, but it is
    /// invisible to `data_version` (v0 already includes it).
    pub checkpointed_frames: u64,
}

impl BlockerLifecycle {
    /// Open the retained handles and arm the blocker LAST (module docs).
    pub fn open(db_path: &Path) -> Result<Self> {
        // Retained raw descriptor first: every later raw read borrows this.
        let source_fd = std::fs::File::open(db_path)
            .with_context(|| format!("failed to open {} read-only", db_path.display()))?;

        // The observer connection: autocommit, never a long transaction.
        let monitor_conn = Connection::open(db_path)?;
        ensure_connection_in_wal_mode(&monitor_conn, db_path)?;
        monitor_conn.execute_batch(
            "PRAGMA busy_timeout=5000;
             PRAGMA wal_autocheckpoint=0;",
        )?;

        // Arm the blocker LAST: after this point no other descriptor for the
        // main DB may be opened or closed by this process.
        let blocker_conn = open_checkpoint_blocker_conn(db_path)?;

        Ok(Self {
            blocker_conn,
            monitor_conn,
            source_fd,
        })
    }

    /// The observer connection, for `PRAGMA page_size` and snapshot-encoding
    /// borrows (`VACUUM INTO`, passive folds).
    pub fn monitor_conn(&self) -> &Connection {
        &self.monitor_conn
    }

    /// The retained read-only descriptor for raw main-db reads.
    pub fn source_fd(&self) -> &std::fs::File {
        &self.source_fd
    }

    /// The database's `PRAGMA page_size`, read through the retained observer
    /// connection (never a fresh descriptor).
    pub fn page_size(&self) -> Result<u32> {
        let page_size: u32 = self
            .monitor_conn
            .query_row("PRAGMA page_size;", [], |row| row.get(0))?;
        Ok(page_size)
    }

    /// Sample `PRAGMA data_version` on the observer connection. The observer
    /// is in autocommit and otherwise idle, so the value advances whenever
    /// ANY other connection commits.
    fn data_version(&self) -> Result<u64> {
        let v: u64 = self
            .monitor_conn
            .query_row("PRAGMA data_version;", [], |row| row.get(0))?;
        Ok(v)
    }

    /// Release the pinned read transaction so walrust can run its own
    /// checkpoint. The blocker CONNECTION is kept alive: its SHARED main-db
    /// lock survives the ROLLBACK (verified empirically), so another
    /// process's last-connection close still cannot unlink the WAL in the
    /// release window.
    fn release_pin(&self) -> Result<()> {
        self.blocker_conn.execute_batch("ROLLBACK;")?;
        Ok(())
    }

    /// Re-pin the blocker after a controlled checkpoint: the blocker writes
    /// the heartbeat frame itself (there is a real frame to pin), then opens
    /// its read transaction again on the same connection.
    fn repin(&self) -> Result<()> {
        self.blocker_conn.execute_batch(
            "INSERT INTO _walrust_seq (id, value)
             VALUES (1, 1)
             ON CONFLICT(id) DO UPDATE SET value = value + 1;",
        )?;
        repin_blocker_conn(&self.blocker_conn)?;
        Ok(())
    }

    /// Roll the blocker's read transaction back (best-effort, for drop).
    pub fn rollback_blocker(&self) {
        let _ = self.blocker_conn.execute_batch("ROLLBACK;");
    }

    /// Run a controlled checkpoint through the release/re-acquire dance and
    /// report whether an application commit landed in the window.
    ///
    /// - `truncate == false`: PASSIVE checkpoint (CLI shadow mode), folded
    ///   frames must cover the whole log or the call fails (same semantics
    ///   as the old one-shot `ShadowWal::checkpoint`).
    /// - `truncate == true`: TRUNCATE checkpoint (owned snapshot mode), same
    ///   completeness check.
    ///
    /// Window detection runs only for the PASSIVE dance: `PRAGMA
    /// data_version` is sampled on the idle observer connection around the
    /// checkpoint, before the heartbeat/re-pin writes that would absorb the
    /// change. A TRUNCATE restarts the wal-index header, which itself forces
    /// a data_version bump on any observer — indistinguishable from a real
    /// commit — so the owned dance does not detect; it is safe by
    /// construction instead (the snapshot taken right after the dance covers
    /// any commit the TRUNCATE folded, and a commit after the TRUNCATE rides
    /// the fresh WAL into the next incremental).
    ///
    /// On a checkpoint error the blocker is still re-pinned before the error
    /// returns (the old code re-armed on failure too). The returned outcome
    /// reports the window commit detection and the folded frame count (see
    /// [`ControlledCheckpoint`]).
    pub fn controlled_checkpoint(&self, truncate: bool) -> Result<ControlledCheckpoint> {
        let v0 = if truncate {
            None
        } else {
            Some(self.data_version()?)
        };
        self.release_pin()?;

        let checkpoint_result = self.run_checkpoint(truncate);
        let dirty = match (&checkpoint_result, v0) {
            (Ok(_), Some(v0)) => Some(self.data_version().map(|v1| v1 != v0)),
            _ => None,
        };
        // Always re-pin before returning, even when the checkpoint failed.
        let repin_result = self.repin();
        match (checkpoint_result, repin_result) {
            (Ok(checkpointed_frames), Ok(())) => Ok(ControlledCheckpoint {
                commit_in_window: match dirty {
                    Some(d) => d?,
                    None => false,
                },
                checkpointed_frames,
            }),
            (Err(checkpoint_err), Ok(())) => Err(checkpoint_err),
            (Ok(_), Err(repin_err)) => Err(repin_err),
            (Err(checkpoint_err), Err(repin_err)) => Err(anyhow!(
                "{checkpoint_err}; additionally failed to re-pin checkpoint blocker: {repin_err}"
            )),
        }
    }

    /// The controlled checkpoint runs on the BLOCKER connection: after
    /// ROLLBACK it is in autocommit, and running it anywhere else (notably
    /// the observer) would either mask window commits from `data_version` or
    /// require a third connection. Returns the folded frame count.
    fn run_checkpoint(&self, truncate: bool) -> Result<u64> {
        let pragma = if truncate {
            "PRAGMA wal_checkpoint(TRUNCATE);"
        } else {
            "PRAGMA wal_checkpoint(PASSIVE);"
        };
        let (busy, log_frames, checkpointed_frames): (i64, i64, i64) =
            self.blocker_conn.query_row(pragma, [], |row| {
                Ok((row.get(0)?, row.get(1)?, row.get(2)?))
            })?;
        if busy != 0 || checkpointed_frames < log_frames {
            return Err(anyhow!(
                "controlled checkpoint incomplete (busy={}, log_frames={}, checkpointed_frames={})",
                busy,
                log_frames,
                checkpointed_frames
            ));
        }
        Ok(checkpointed_frames as u64)
    }
}

/// Open and arm a checkpoint-blocker connection for `db_path`: a read
/// transaction pinning a real WAL frame so external checkpoints cannot
/// truncate past the mark (D2).
///
/// Standalone primitive for callers that only need the blocker; the
/// production watch lifetime goes through [`BlockerLifecycle`].
pub fn open_checkpoint_blocker_conn(db_path: &Path) -> Result<Connection> {
    let conn = Connection::open(db_path)?;

    ensure_connection_in_wal_mode(&conn, db_path)?;

    // Disable auto-checkpoint on this connection without changing journal_mode.
    conn.execute_batch(
        "PRAGMA busy_timeout=5000;
         PRAGMA wal_autocheckpoint=0;
         CREATE TABLE IF NOT EXISTS _walrust_seq (
             id INTEGER PRIMARY KEY CHECK (id = 1),
             value INTEGER NOT NULL
         );
         INSERT INTO _walrust_seq (id, value)
         VALUES (1, 1)
         ON CONFLICT(id) DO UPDATE SET value = value + 1;",
    )?;

    repin_blocker_conn(&conn)?;

    tracing::debug!("Opened checkpoint blocker for {}", db_path.display());

    Ok(conn)
}

/// Pin a real WAL frame with a fresh read transaction. Reading sqlite_master
/// can leave the blocker at read-mark 0, which does not prevent walRestartLog
/// on later frames, so the read targets the heartbeat row.
fn repin_blocker_conn(conn: &Connection) -> Result<()> {
    conn.execute_batch("BEGIN DEFERRED;")?;
    let _: i64 = conn.query_row("SELECT value FROM _walrust_seq WHERE id = 1", [], |row| {
        row.get(0)
    })?;
    Ok(())
}
