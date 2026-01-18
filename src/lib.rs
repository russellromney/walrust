//! Walrust - Lightweight SQLite WAL sync to S3/Tigris
//!
//! This library provides Python bindings for syncing SQLite WAL files to S3-compatible storage.

pub mod cache;
pub mod config;
pub mod dashboard;
pub mod errors;
pub mod ltx;
pub mod retention;
pub mod retry;
pub mod s3;
pub mod shadow;
pub mod storage;
pub mod sync;
pub mod uploader;
pub mod wal;
pub mod webhook;

// Re-export storage types for convenience
pub use storage::{S3Backend, StorageBackend};

// Re-export retry types for DST
pub use retry::{RetryConfig, RetryPolicy};

// Re-export testable sync module for DST
pub use sync::testable;

#[cfg(feature = "python")]
mod python;

#[cfg(feature = "python")]
pub use python::*;
