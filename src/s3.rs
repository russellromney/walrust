//! Re-export S3 helpers from hadb-io, plus walrust-local additions.
pub use hadb_io::s3::*;

use anyhow::Result;
use aws_sdk_s3::Client;
use chrono::{DateTime, Utc};

/// Whether an error returned by [`download_bytes`] is a CONFIRMED
/// object-not-found (a GET against a key that does not exist), classified from
/// the **typed** AWS SDK error — the `NoSuchKey` service error, or a service
/// response carrying HTTP status 404 (S3-compatible backends that omit the
/// error code). Never message-string matching: free text like a DNS "host not
/// found" or a proxy body mentioning "404" must not read as a missing object.
///
/// Everything else — dispatch/connect failures, timeouts, 5xx, body-read
/// errors, non-SDK errors — is NOT not-found, so callers fail safe toward a
/// loud retry instead of misreading a transient as an absent object.
pub fn download_error_is_not_found(err: &anyhow::Error) -> bool {
    use aws_sdk_s3::error::SdkError;
    use aws_sdk_s3::operation::get_object::GetObjectError;
    err.chain().any(
        |cause| match cause.downcast_ref::<SdkError<GetObjectError>>() {
            Some(SdkError::ServiceError(ctx)) => {
                ctx.err().is_no_such_key() || ctx.raw().status().as_u16() == 404
            }
            _ => false,
        },
    )
}

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
