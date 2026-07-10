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

// ── Compaction on the CLI layout (wave C3a) ──────────────────────────────────
// The C2b `reject_cli_compaction` sever is gone (wave C3a): the CLI
// restore/verify/PITR path now speaks the compaction planner
// (`legacy_restore::restore_legacy_ltx` replays across the LTX→HADBP seam), so
// `[compaction] enabled = true` is supported for the CLI watch too — write and
// read are consistent. `enabled` still defaults to false (version-skew safety:
// an old binary cannot restore a leveled bucket).
