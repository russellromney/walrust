//! Re-export storage trait from hadb-io.
//!
//! `StorageBackend` is an alias for `hadb_io::ObjectStore` to preserve
//! downstream import compatibility.
pub use hadb_io::ObjectStore as StorageBackend;
pub use hadb_io::S3Backend;
