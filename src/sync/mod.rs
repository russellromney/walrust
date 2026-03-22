// Module declarations
mod types;
mod manifest;
mod wal_sync;
mod shadow;
mod compact;

// Watch modes
mod watch_independent;
mod watch_shadow;

// Restore commands
mod restore;
mod replicate;
mod verify;
mod explain;

// Public API re-exports
pub use watch_independent::watch_with_independent_tasks;
pub use watch_shadow::watch_with_shadow;
pub use restore::{restore, list};
pub use replicate::replicate;
pub use verify::verify;
pub use explain::explain;
pub use compact::{compact, snapshot};
