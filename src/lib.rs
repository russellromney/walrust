//! Walrust - Lightweight SQLite WAL sync to S3/Tigris
//!
//! This library provides Python bindings for syncing SQLite WAL files to S3-compatible storage.
//!
//! The byte-level storage trait (`StorageBackend`) lives in `hadb-storage` and
//! is what walrust composes on top of. Consumers wire in any concrete impl
//! (`hadb-storage-s3`, `hadb-storage-cinch`, `hadb-storage-mem`, ...). The
//! `s3` feature (default) pulls in `hadb-storage-s3` and exposes the
//! convenience constructor [`s3_backend_from_env`].

#[cfg(feature = "s3")]
pub mod cache;
pub mod config;
#[cfg(feature = "s3")]
pub mod dashboard;
pub mod errors;
pub mod ltx;
#[cfg(feature = "s3")]
pub mod retention;
pub mod retry;
#[cfg(feature = "s3")]
pub mod s3;
#[cfg(feature = "s3")]
pub mod shadow;
#[cfg(feature = "s3")]
pub mod sync;
#[cfg(feature = "s3")]
pub mod uploader;
pub mod wal;
#[cfg(feature = "s3")]
pub mod webhook;

// Re-export retry types for DST
pub use retry::{RetryConfig, RetryPolicy};

// Re-export walrust-core for library consumers
pub use walrust_core;

/// Convenience: build an S3-backed `StorageBackend` from `AWS_*` env vars.
///
/// `bucket` is the S3 bucket name. `endpoint` overrides `AWS_ENDPOINT_URL_S3`
/// for S3-compatible services (Tigris, MinIO, R2, Wasabi). Pass `None` for
/// real AWS S3.
///
/// Gated behind the `s3` feature (on by default). Without `s3`, walrust pulls
/// in no aws-sdk-* deps and the caller must construct their own
/// `Arc<dyn hadb_storage::StorageBackend>` (e.g. `hadb-storage-cinch`,
/// `hadb-storage-mem`).
#[cfg(feature = "s3")]
pub async fn s3_backend_from_env(
    bucket: impl Into<String>,
    endpoint: Option<&str>,
) -> anyhow::Result<std::sync::Arc<dyn hadb_storage::StorageBackend>> {
    let s3 = hadb_storage_s3::S3Storage::from_env(bucket, endpoint).await?;
    Ok(std::sync::Arc::new(s3))
}

#[cfg(feature = "python")]
mod python;

#[cfg(feature = "python")]
pub use python::*;
