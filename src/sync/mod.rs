// Module declarations
mod types;
mod manifest;
mod wal_sync;
mod shadow;
mod watch;
mod restore;
mod compact;

// Public API re-exports
pub use watch::{watch, watch_with_config, watch_with_independent_tasks, watch_with_shadow};
pub use restore::{restore, list, replicate, verify, explain};
pub use compact::{compact, snapshot};
pub use types::{Manifest, LtxEntry};

// Re-export for testing (testable module from original sync.rs)
#[cfg(test)]
pub mod testable {
    // Add any test-only exports here if needed
}
