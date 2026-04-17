//! Storage abstraction for walrust.
//!
//! Re-exports the byte-level `StorageBackend` trait from `hadb-storage`.
//! Callers that need a concrete S3 implementation construct
//! `hadb_storage_s3::S3Storage` themselves and hand it to walrust as
//! `Arc<dyn walrust::StorageBackend>`.
pub use hadb_storage::StorageBackend;
