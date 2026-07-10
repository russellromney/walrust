//! Re-export S3 helpers from hadb-io, plus walrust-local additions.
pub use hadb_io::s3::*;

use anyhow::Result;
use aws_sdk_s3::Client;
use chrono::{DateTime, Utc};

/// Object metadata from a HEAD request: size in bytes and last-modified time.
#[derive(Debug, Clone)]
pub struct ObjectMeta {
    pub size: u64,
    pub last_modified: DateTime<Utc>,
}

/// HEAD an object to read its size and last-modified timestamp.
///
/// Used by `prune` to build retention entries from S3 listing rather than a
/// `manifest.json` the production watch path never writes. Falls back to "now"
/// if the backend omits `LastModified`.
pub async fn head_object_meta(client: &Client, bucket: &str, key: &str) -> Result<ObjectMeta> {
    let head = client.head_object().bucket(bucket).key(key).send().await?;
    let size = head.content_length().unwrap_or(0).max(0) as u64;
    let last_modified = head
        .last_modified()
        .and_then(|t| {
            let secs = t.secs();
            let nanos = t.subsec_nanos();
            DateTime::<Utc>::from_timestamp(secs, nanos)
        })
        .unwrap_or_else(Utc::now);
    Ok(ObjectMeta {
        size,
        last_modified,
    })
}
