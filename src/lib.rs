//! Walrust - Lightweight SQLite WAL sync to S3/Tigris
//!
//! This library provides Python bindings for syncing SQLite WAL files to S3-compatible storage.

pub mod config;
pub mod dashboard;
pub mod ltx;
pub mod retention;
pub mod s3;
pub mod storage;
pub mod sync;
pub mod wal;

// Re-export storage types for convenience
pub use storage::{S3Backend, StorageBackend};

// Re-export testable sync module for DST
pub use sync::testable;

#[cfg(feature = "python")]
mod python;

#[cfg(feature = "python")]
pub use python::*;
