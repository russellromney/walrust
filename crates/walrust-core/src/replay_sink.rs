//! Page-replay sink trait for direct hybrid page replay.
//!
//! Consumed by `sync::pull_incremental_into_sink`. Lets a caller (e.g.
//! haqlite-turbolite, where the SQLite base lives in Turbolite's tiered
//! cache rather than a plain file) receive decoded HADBP physical pages
//! one at a time and route them into a non-SQLite-backed sink.
//!
//! Lifecycle: `begin` once, then per discovered changeset
//! `apply_page` for each page followed by `commit_changeset(seq)`,
//! then exactly one of `finalize` (success) or `abort` (any error).
//! `pull_incremental_into_sink` is responsible for driving this.
//!
//! `abort` exists explicitly so a failed mid-batch replay cannot leave
//! a half-applied local state marked final. Sinks that stage to a side
//! buffer until `finalize` (the recommended shape) drop the buffer in
//! `abort` without touching the live readable state.

use anyhow::Result;

/// A sink that receives decoded HADBP physical pages.
///
/// Page id contract: `apply_page` receives the **SQLite 1-based page id**
/// from the HADBP changeset (`hadb_changeset::physical::Page::page_id`).
/// SQLite's database header lives at byte 0 of page id `1`. Sinks that
/// index into a zero-based store (e.g. Turbolite's cache) must subtract
/// one internally.
pub trait PageReplaySink: Send {
    /// Called once before the first `apply_page`. Sinks that stage to a
    /// buffer should arm the buffer here.
    fn begin(&mut self) -> Result<()>;

    /// Called for each page in each changeset, in arrival order.
    ///
    /// `sqlite_page_id` is 1-based per the SQLite/HADBP convention. Sinks
    /// that need 0-based indexing must convert.
    fn apply_page(&mut self, sqlite_page_id: u32, data: &[u8]) -> Result<()>;

    /// Called after all pages of a single changeset have been applied,
    /// with the changeset's `seq`. Sinks may use this to checkpoint
    /// progress; the recommended shape just records `seq` for telemetry
    /// and defers any durable install to `finalize`.
    fn commit_changeset(&mut self, seq: u64) -> Result<()>;

    /// Called once after the last successfully applied changeset.
    /// Atomically installs the staged state.
    fn finalize(&mut self) -> Result<()>;

    /// Called instead of `finalize` on any error. Drops staged state
    /// without touching the live readable state. Must be safe to call
    /// after `begin` even if no `apply_page` calls happened.
    fn abort(&mut self) -> Result<()>;
}

#[cfg(test)]
pub(crate) mod test_support {
    use super::*;
    use std::sync::{Arc, Mutex};

    #[derive(Default, Debug, Clone)]
    pub struct LifecycleEvents {
        pub begin_calls: u32,
        pub finalize_calls: u32,
        pub abort_calls: u32,
        pub committed_seqs: Vec<u64>,
        pub applied: Vec<(u32, Vec<u8>)>,
    }

    /// In-memory sink that records every call. Used by unit tests in
    /// `sync.rs` to assert the call shape `pull_incremental_into_sink`
    /// drives.
    pub struct RecordingSink {
        pub events: Arc<Mutex<LifecycleEvents>>,
        pub fail_at_apply_index: Option<usize>,
        next_apply_index: usize,
    }

    impl RecordingSink {
        pub fn new() -> Self {
            Self {
                events: Arc::new(Mutex::new(LifecycleEvents::default())),
                fail_at_apply_index: None,
                next_apply_index: 0,
            }
        }

        pub fn fail_at(mut self, idx: usize) -> Self {
            self.fail_at_apply_index = Some(idx);
            self
        }

        pub fn snapshot(&self) -> LifecycleEvents {
            self.events.lock().unwrap().clone()
        }
    }

    impl PageReplaySink for RecordingSink {
        fn begin(&mut self) -> Result<()> {
            self.events.lock().unwrap().begin_calls += 1;
            Ok(())
        }

        fn apply_page(&mut self, sqlite_page_id: u32, data: &[u8]) -> Result<()> {
            let idx = self.next_apply_index;
            self.next_apply_index += 1;
            if Some(idx) == self.fail_at_apply_index {
                return Err(anyhow::anyhow!("injected apply_page failure at {}", idx));
            }
            self.events
                .lock()
                .unwrap()
                .applied
                .push((sqlite_page_id, data.to_vec()));
            Ok(())
        }

        fn commit_changeset(&mut self, seq: u64) -> Result<()> {
            self.events.lock().unwrap().committed_seqs.push(seq);
            Ok(())
        }

        fn finalize(&mut self) -> Result<()> {
            self.events.lock().unwrap().finalize_calls += 1;
            Ok(())
        }

        fn abort(&mut self) -> Result<()> {
            self.events.lock().unwrap().abort_calls += 1;
            Ok(())
        }
    }
}
