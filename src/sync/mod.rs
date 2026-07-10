// Module declarations
pub(crate) mod manifest;
mod prune;
mod shadow;
mod types;
mod wal_sync;

// Watch modes
mod watch_independent;
mod watch_shadow;

// Restore commands
mod explain;
mod replicate;
mod restore;
mod verify;

// Public API re-exports
pub use explain::explain;
pub use prune::{prune, snapshot};
pub use replicate::replicate;
pub use restore::{list, restore};
pub use verify::verify;
pub use watch_independent::watch_with_independent_tasks;
pub use watch_shadow::watch_with_shadow;

use crate::config::ResolvedDbConfig;
use crate::errors::WalrustError;

/// Refuse to run the CLI watch with leveled compaction enabled.
///
/// **The exposure-vs-read-path gap (C2b sever).** The CLI watch compacts through
/// the litestream-heritage *range* layout (`maybe_compact_legacy`): it merges L0
/// into `{db}/levels/L*/` and **deletes the superseded L0 objects**. But the CLI
/// restore command (`walrust restore` → `legacy_restore::restore_legacy_ltx`) is
/// **not** wired to the leveled planner — it only walks the raw `0000/`
/// incremental pool + snapshots. So a bucket the CLI watch compacts becomes
/// **unrestorable by `walrust restore`**: the L0 tail it needs was folded into a
/// merged object the CLI restore never reads. Enabling compaction here would let
/// the write side produce backups the read side cannot restore.
///
/// Until the CLI restore/PITR path speaks the planner (LTX-vs-HADBP payloads,
/// TXID PITR, cache substitution, and snapshot discovery all differ — its own
/// wave), compaction is supported **only in library / owned mode** (the
/// `Replicator`, whose `sync::restore` *is* fully wired to the planner). The
/// config still parses; the CLI watch just refuses loudly instead of silently
/// writing an unrestorable bucket.
fn reject_cli_compaction(databases: &[ResolvedDbConfig]) -> Result<(), WalrustError> {
    if databases.iter().any(|d| d.compaction.enabled) {
        return Err(WalrustError::config(
            "[compaction] enabled = true is not yet supported for the CLI watch/restore \
             layout: the CLI restore path cannot read leveled buckets, so compacting here \
             would produce backups `walrust restore` cannot restore. Leveled compaction is \
             available only in library (owned) mode via the Replicator for now. Set \
             [compaction] enabled = false to run the CLI watch.",
        ));
    }
    Ok(())
}

#[cfg(test)]
mod compaction_gate_tests {
    use super::*;
    use walrust_core::compaction::CompactionSettings;

    fn db(enabled: bool) -> ResolvedDbConfig {
        ResolvedDbConfig {
            path: std::path::PathBuf::from("/tmp/x.db"),
            prefix: "x".into(),
            sync: Default::default(),
            retention: Default::default(),
            compaction: CompactionSettings {
                enabled,
                keep_fine_window: std::time::Duration::from_secs(3600),
                l1_batch: 8,
                l2_batch: 8,
            },
        }
    }

    #[test]
    fn cli_watch_refuses_when_compaction_enabled() {
        let err = reject_cli_compaction(&[db(false), db(true)]).unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("not yet supported for the CLI") && msg.contains("library (owned) mode"),
            "message must name the CLI limitation and the library-mode alternative: {msg}"
        );
    }

    #[test]
    fn cli_watch_allows_when_compaction_disabled() {
        assert!(reject_cli_compaction(&[db(false), db(false)]).is_ok());
        assert!(reject_cli_compaction(&[]).is_ok());
    }
}
