//! Single-writer guard for a database (E5).
//!
//! walrust is single-writer per database by design: two live instances writing
//! backups for the same DB corrupt each other's state (interleaved shadow
//! progress, duplicate/torn segments, racing snapshots). Supervisors that
//! double-start `watch` reproduced severe unrecoverable corruption in drills.
//!
//! This enforces the invariant locally with an advisory `flock` on a lock file
//! next to the database (`.walrust-<db>.lock`). The lock is held for the
//! lifetime of the process (until the file descriptor closes on drop or exit).
//! A second instance on the same host fails fast with a clear message.
//!
//! Scope: this guards **same-host** double-start only, which matches the
//! single-writer design. Cross-host coordination remains the operator's
//! contract (advisory file locks do not span machines or most network
//! filesystems).

use std::fs::{File, OpenOptions};
use std::os::unix::io::AsRawFd;
use std::path::{Path, PathBuf};

use anyhow::Result;

use crate::errors::WalrustError;

/// Held exclusive lock on a database. Dropping it (or exiting) releases the
/// lock. Keep it alive for as long as the process writes backups for the DB.
#[derive(Debug)]
pub struct DbLock {
    // Held only for its file descriptor; the flock is released when it closes.
    _file: File,
    path: PathBuf,
}

impl DbLock {
    /// The lock-file path for a database: `.walrust-<db-file-name>.lock` next
    /// to the database file.
    pub fn lock_path_for(db_path: &Path) -> PathBuf {
        let parent = db_path.parent().unwrap_or_else(|| Path::new("."));
        let name = db_path
            .file_name()
            .map(|s| s.to_string_lossy().into_owned())
            .unwrap_or_else(|| "db".to_string());
        parent.join(format!(".walrust-{name}.lock"))
    }

    /// Try to acquire the exclusive single-writer lock for `db_path`. Returns a
    /// typed error (fast, non-retryable) if another instance already holds it.
    pub fn acquire(db_path: &Path) -> Result<Self> {
        let path = Self::lock_path_for(db_path);
        let file = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .open(&path)
            .map_err(|e| {
                WalrustError::config(format!("failed to open lock file {}: {e}", path.display()))
            })?;

        // Non-blocking exclusive advisory lock.
        let rc = unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) };
        if rc != 0 {
            let err = std::io::Error::last_os_error();
            let already_held = matches!(
                err.raw_os_error(),
                Some(code) if code == libc::EWOULDBLOCK || code == libc::EAGAIN
            );
            if already_held {
                return Err(WalrustError::config(format!(
                    "another walrust instance is already running on {} (lock {} is held). \
                     walrust is single-writer per database — stop the other instance first. \
                     This guard is same-host only; cross-host single-writer is the operator's contract.",
                    db_path.display(),
                    path.display()
                ))
                .into());
            }
            return Err(WalrustError::config(format!(
                "failed to acquire single-writer lock {}: {err}",
                path.display()
            ))
            .into());
        }

        Ok(Self { _file: file, path })
    }

    /// Whether a lock file exists for this database (a cheap, best-effort hint
    /// that a watcher may own it; used by E6 to give an actionable snapshot
    /// error). Presence does not prove the lock is currently held.
    pub fn lock_file_exists(db_path: &Path) -> bool {
        Self::lock_path_for(db_path).exists()
    }

    /// Whether another live process currently holds the single-writer lock for
    /// `db_path`. Probes by trying to take the lock non-blockingly and
    /// immediately releasing it; a failure means someone else holds it.
    pub fn is_held_by_another(db_path: &Path) -> bool {
        let path = Self::lock_path_for(db_path);
        let Ok(file) = OpenOptions::new().read(true).write(true).open(&path) else {
            // No lock file (or unopenable) => treat as not held.
            return false;
        };
        let fd = file.as_raw_fd();
        let rc = unsafe { libc::flock(fd, libc::LOCK_EX | libc::LOCK_NB) };
        if rc == 0 {
            // We got it — nobody else holds it. Release immediately.
            unsafe {
                libc::flock(fd, libc::LOCK_UN);
            }
            false
        } else {
            true
        }
    }

    /// Path of the held lock file.
    pub fn path(&self) -> &Path {
        &self.path
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn e5_second_lock_on_same_db_fails_fast() {
        let dir = tempdir().unwrap();
        let db_path = dir.path().join("app.db");
        std::fs::write(&db_path, b"").unwrap();

        let _first = DbLock::acquire(&db_path).expect("first lock acquires");
        assert!(DbLock::is_held_by_another(&db_path));

        let err = DbLock::acquire(&db_path).expect_err("second lock must fail fast");
        let msg = err.to_string();
        assert!(
            msg.contains("already running") && msg.contains("single-writer"),
            "message must be clear and actionable, got: {msg}"
        );
    }

    #[test]
    fn e5_lock_released_on_drop_allows_reacquire() {
        let dir = tempdir().unwrap();
        let db_path = dir.path().join("app.db");
        std::fs::write(&db_path, b"").unwrap();

        {
            let _first = DbLock::acquire(&db_path).unwrap();
        }
        // After the first guard drops, a new instance can acquire again.
        let _second = DbLock::acquire(&db_path).expect("lock must be reacquirable after drop");
    }

    /// Priority-6: the single-writer guard relies on the OS releasing the
    /// advisory flock when the holding process dies, even on SIGKILL (no
    /// graceful drop runs). Fork a child that takes the lock, SIGKILL it, and
    /// prove a fresh instance can then reacquire. The child touches only
    /// async-signal-safe syscalls after fork.
    #[test]
    fn e5_lock_reacquires_after_holder_is_sigkilled() {
        use std::ffi::CString;
        use std::time::Duration;

        let dir = tempdir().unwrap();
        let db_path = dir.path().join("app.db");
        std::fs::write(&db_path, b"").unwrap();
        let lock_path = DbLock::lock_path_for(&db_path);
        let c_lock = CString::new(lock_path.to_str().unwrap()).unwrap();

        let pid = unsafe { libc::fork() };
        assert!(pid >= 0, "fork failed");
        if pid == 0 {
            // Child: pure syscalls only (safe after fork in a threaded parent).
            unsafe {
                let fd = libc::open(c_lock.as_ptr(), libc::O_CREAT | libc::O_RDWR, 0o644);
                if fd < 0 {
                    libc::_exit(1);
                }
                if libc::flock(fd, libc::LOCK_EX | libc::LOCK_NB) != 0 {
                    libc::_exit(2);
                }
                libc::sleep(30);
                libc::_exit(0);
            }
        }

        // Parent: wait until the child owns the lock.
        let mut held = false;
        for _ in 0..200 {
            if DbLock::is_held_by_another(&db_path) {
                held = true;
                break;
            }
            std::thread::sleep(Duration::from_millis(20));
        }
        assert!(held, "child must acquire the lock before we SIGKILL it");
        assert!(
            DbLock::acquire(&db_path).is_err(),
            "lock must be contended while the child holds it"
        );

        // Hard-kill the holder; the OS must release its flock on process death.
        unsafe {
            libc::kill(pid, libc::SIGKILL);
            let mut status = 0i32;
            libc::waitpid(pid, &mut status, 0);
        }

        let mut reacquired = None;
        for _ in 0..200 {
            match DbLock::acquire(&db_path) {
                Ok(lock) => {
                    reacquired = Some(lock);
                    break;
                }
                Err(_) => std::thread::sleep(Duration::from_millis(20)),
            }
        }
        assert!(
            reacquired.is_some(),
            "lock must be reacquirable after the holder is SIGKILLed"
        );
    }

    #[test]
    fn e5_lock_path_is_next_to_db() {
        let p = DbLock::lock_path_for(Path::new("/data/app.db"));
        assert_eq!(p, Path::new("/data/.walrust-app.db.lock"));
    }
}
