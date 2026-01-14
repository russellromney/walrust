//! Storage backend abstraction for walrust
//!
//! Provides a trait-based abstraction over different cloud storage providers:
//! - S3 (AWS, Tigris, Wasabi, MinIO, Cloudflare R2, etc.)
//! - Azure Blob Storage (future)
//! - Google Cloud Storage (future)
//!
//! All S3-compatible services use the same `S3Backend` implementation.

use anyhow::Result;
use async_trait::async_trait;
use std::path::Path;

/// Storage backend trait for cloud object storage
///
/// Implementations handle bucket/container configuration internally.
/// All methods operate on keys within the configured bucket.
#[async_trait]
pub trait StorageBackend: Send + Sync {
    /// Upload bytes to storage
    async fn upload_bytes(&self, key: &str, data: Vec<u8>) -> Result<()>;

    /// Upload bytes with a checksum for integrity verification
    async fn upload_bytes_with_checksum(
        &self,
        key: &str,
        data: Vec<u8>,
        checksum: &str,
    ) -> Result<()>;

    /// Upload a file to storage
    async fn upload_file(&self, key: &str, path: &Path) -> Result<()>;

    /// Upload a file with checksum
    async fn upload_file_with_checksum(&self, key: &str, path: &Path, checksum: &str)
        -> Result<()>;

    /// Download bytes from storage
    async fn download_bytes(&self, key: &str) -> Result<Vec<u8>>;

    /// Download to a file
    async fn download_file(&self, key: &str, path: &Path) -> Result<()>;

    /// List objects with a prefix
    async fn list_objects(&self, prefix: &str) -> Result<Vec<String>>;

    /// Check if an object exists
    async fn exists(&self, key: &str) -> Result<bool>;

    /// Get checksum metadata from an object
    async fn get_checksum(&self, key: &str) -> Result<Option<String>>;

    /// Delete a single object
    async fn delete_object(&self, key: &str) -> Result<()>;

    /// Delete multiple objects (batch operation)
    /// Returns the number of successfully deleted objects
    async fn delete_objects(&self, keys: &[String]) -> Result<usize>;

    /// Get the bucket/container name for logging
    fn bucket_name(&self) -> &str;
}

// ============================================================================
// S3 Backend Implementation
// ============================================================================

use aws_sdk_s3::Client;

/// S3-compatible storage backend
///
/// Works with AWS S3, Tigris, Wasabi, MinIO, Cloudflare R2, and any S3-compatible service.
pub struct S3Backend {
    client: Client,
    bucket: String,
}

impl S3Backend {
    /// Create a new S3 backend
    pub fn new(client: Client, bucket: String) -> Self {
        Self { client, bucket }
    }

    /// Create from environment with optional custom endpoint
    pub async fn from_env(bucket: String, endpoint: Option<&str>) -> Result<Self> {
        let client = crate::s3::create_client(endpoint).await?;
        Ok(Self::new(client, bucket))
    }

    /// Get a reference to the underlying S3 client
    pub fn client(&self) -> &Client {
        &self.client
    }
}

#[async_trait]
impl StorageBackend for S3Backend {
    async fn upload_bytes(&self, key: &str, data: Vec<u8>) -> Result<()> {
        crate::s3::upload_bytes(&self.client, &self.bucket, key, data).await
    }

    async fn upload_bytes_with_checksum(
        &self,
        key: &str,
        data: Vec<u8>,
        checksum: &str,
    ) -> Result<()> {
        crate::s3::upload_bytes_with_checksum(&self.client, &self.bucket, key, data, checksum).await
    }

    async fn upload_file(&self, key: &str, path: &Path) -> Result<()> {
        crate::s3::upload_file(&self.client, &self.bucket, key, path).await
    }

    async fn upload_file_with_checksum(
        &self,
        key: &str,
        path: &Path,
        checksum: &str,
    ) -> Result<()> {
        crate::s3::upload_file_with_checksum(&self.client, &self.bucket, key, path, checksum).await
    }

    async fn download_bytes(&self, key: &str) -> Result<Vec<u8>> {
        crate::s3::download_bytes(&self.client, &self.bucket, key).await
    }

    async fn download_file(&self, key: &str, path: &Path) -> Result<()> {
        crate::s3::download_file(&self.client, &self.bucket, key, path).await
    }

    async fn list_objects(&self, prefix: &str) -> Result<Vec<String>> {
        crate::s3::list_objects(&self.client, &self.bucket, prefix).await
    }

    async fn exists(&self, key: &str) -> Result<bool> {
        crate::s3::exists(&self.client, &self.bucket, key).await
    }

    async fn get_checksum(&self, key: &str) -> Result<Option<String>> {
        crate::s3::get_checksum(&self.client, &self.bucket, key).await
    }

    async fn delete_object(&self, key: &str) -> Result<()> {
        crate::s3::delete_object(&self.client, &self.bucket, key).await
    }

    async fn delete_objects(&self, keys: &[String]) -> Result<usize> {
        crate::s3::delete_objects(&self.client, &self.bucket, keys).await
    }

    fn bucket_name(&self) -> &str {
        &self.bucket
    }
}

// ============================================================================
// Future: Azure Blob Storage
// ============================================================================

// pub struct AzureBlobBackend { ... }
// impl StorageBackend for AzureBlobBackend { ... }

// ============================================================================
// Future: Google Cloud Storage
// ============================================================================

// pub struct GcsBackend { ... }
// impl StorageBackend for GcsBackend { ... }

#[cfg(test)]
mod tests {
    use super::*;

    // Compile-time check that S3Backend implements StorageBackend
    fn _assert_send_sync<T: StorageBackend>() {}

    #[allow(dead_code)]
    fn _check_s3_backend() {
        _assert_send_sync::<S3Backend>();
    }
}
