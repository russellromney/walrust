// Module declarations
mod compact;
mod manifest;
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
pub use compact::{compact, snapshot};
pub use explain::explain;
pub use replicate::replicate;
pub use restore::{list, restore};
pub use verify::verify;
pub use watch_independent::watch_with_independent_tasks;
pub use watch_shadow::watch_with_shadow;
