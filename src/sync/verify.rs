use crate::errors::{classify_or_else, WalrustError};
use crate::s3::{create_client, parse_bucket};
use anyhow::Result;
use hadb_storage_s3::S3Storage;

#[derive(Debug, Clone)]
pub struct VerifyIssue {
    pub filename: String,
    pub issue: String,
}

#[derive(Debug)]
pub struct ValidationResult {
    pub verified_count: usize,
    pub issues: Vec<VerifyIssue>,
    pub verified_size_bytes: u64,
    pub is_valid: bool,
}

pub(crate) async fn validate_backup_integrity(
    client: &aws_sdk_s3::Client,
    bucket: &str,
    prefix: &str,
    db_name: &str,
) -> Result<ValidationResult> {
    let storage = S3Storage::new(client.clone(), bucket.to_string());
    let verified =
        walrust_core::native_restore::verify_native_v1(&storage, bucket, prefix, db_name)
            .await?
            .ok_or_else(|| {
                WalrustError::integrity(format!(
                    "{}: no contiguous native-v1 HADBP recovery stream found",
                    db_name
                ))
            })?;
    Ok(ValidationResult {
        verified_count: verified,
        issues: Vec::new(),
        verified_size_bytes: 0,
        is_valid: true,
    })
}

pub async fn verify(
    name: &str,
    bucket: &str,
    endpoint: Option<&str>,
    webhook: Option<std::sync::Arc<crate::webhook::WebhookSender>>,
) -> Result<()> {
    let (bucket_name, prefix) = parse_bucket(bucket);
    let client = create_client(endpoint)
        .await
        .map_err(|error| classify_or_else(error, WalrustError::s3))?;
    println!(
        "Verifying native HADBP integrity of '{}' in s3://{}/{}{}...",
        name, bucket_name, prefix, name
    );
    match validate_backup_integrity(&client, &bucket_name, &prefix, name).await {
        Ok(result) => {
            println!(
                "Verified {} contiguous published native HADBP object(s)",
                result.verified_count
            );
            Ok(())
        }
        Err(error) => {
            if let Some(webhook) = webhook {
                webhook
                    .notify_corruption(name, &format!("native HADBP verify failed: {error:#}"))
                    .await;
            }
            if error
                .chain()
                .any(|cause| cause.is::<walrust_core::native_restore::NativeStorageError>())
            {
                Err(classify_or_else(error, WalrustError::s3))
            } else {
                Err(classify_or_else(error, WalrustError::integrity))
            }
        }
    }
}
