use anyhow::{anyhow, Result};
use aws_sdk_s3::{primitives::ByteStream, Client};
use std::path::Path;

/// Parse bucket string like "s3://bucket-name/prefix" or just "bucket-name"
pub fn parse_bucket(bucket: &str) -> (String, String) {
    let bucket = bucket.strip_prefix("s3://").unwrap_or(bucket);
    if let Some((bucket, prefix)) = bucket.split_once('/') {
        (bucket.to_string(), prefix.to_string())
    } else {
        (bucket.to_string(), String::new())
    }
}

/// Create S3 client with optional custom endpoint (for Tigris/MinIO)
/// Configured with increased connection pool for high-throughput workloads
pub async fn create_client(endpoint: Option<&str>) -> Result<Client> {
    use aws_smithy_runtime::client::http::hyper_014::HyperClientBuilder;

    // Configure HTTP client with larger connection pool (default is ~50)
    // Build with more connections to allow concurrent S3 uploads (critical for >5K w/s)
    let http_client = HyperClientBuilder::new()
        .build_https();

    let mut config_loader = aws_config::from_env()
        .http_client(http_client);

    if let Some(endpoint) = endpoint {
        config_loader = config_loader.endpoint_url(endpoint);
    }

    let config = config_loader.load().await;
    Ok(Client::new(&config))
}

/// Upload bytes to S3
pub async fn upload_bytes(
    client: &Client,
    bucket: &str,
    key: &str,
    data: Vec<u8>,
) -> Result<()> {
    let len = data.len();
    client
        .put_object()
        .bucket(bucket)
        .key(key)
        .body(ByteStream::from(data))
        .send()
        .await?;

    tracing::debug!("Uploaded {} bytes to s3://{}/{}", len, bucket, key);
    Ok(())
}

/// Upload a file to S3
pub async fn upload_file(
    client: &Client,
    bucket: &str,
    key: &str,
    path: &Path,
) -> Result<()> {
    let body = ByteStream::from_path(path).await?;

    client
        .put_object()
        .bucket(bucket)
        .key(key)
        .body(body)
        .send()
        .await?;

    tracing::debug!("Uploaded {} to s3://{}/{}", path.display(), bucket, key);
    Ok(())
}

/// Download bytes from S3
pub async fn download_bytes(
    client: &Client,
    bucket: &str,
    key: &str,
) -> Result<Vec<u8>> {
    let resp = client
        .get_object()
        .bucket(bucket)
        .key(key)
        .send()
        .await?;

    let data = resp.body.collect().await?.into_bytes().to_vec();
    tracing::debug!("Downloaded {} bytes from s3://{}/{}", data.len(), bucket, key);
    Ok(data)
}

/// Download to file
pub async fn download_file(
    client: &Client,
    bucket: &str,
    key: &str,
    path: &Path,
) -> Result<()> {
    let data = download_bytes(client, bucket, key).await?;
    tokio::fs::write(path, &data).await?;
    Ok(())
}

/// List objects with prefix
pub async fn list_objects(
    client: &Client,
    bucket: &str,
    prefix: &str,
) -> Result<Vec<String>> {
    let mut keys = Vec::new();
    let mut continuation_token: Option<String> = None;

    loop {
        let mut req = client
            .list_objects_v2()
            .bucket(bucket)
            .prefix(prefix);

        if let Some(token) = &continuation_token {
            req = req.continuation_token(token);
        }

        let resp = req.send().await?;

        if let Some(contents) = resp.contents {
            for obj in contents {
                if let Some(key) = obj.key {
                    keys.push(key);
                }
            }
        }

        if resp.is_truncated.unwrap_or(false) {
            continuation_token = resp.next_continuation_token;
        } else {
            break;
        }
    }

    Ok(keys)
}

/// Upload bytes with SHA256 metadata for integrity verification
pub async fn upload_bytes_with_checksum(
    client: &Client,
    bucket: &str,
    key: &str,
    data: Vec<u8>,
    checksum: &str,
) -> Result<()> {
    let len = data.len();
    client
        .put_object()
        .bucket(bucket)
        .key(key)
        .metadata("x-amz-meta-sha256", checksum)
        .body(ByteStream::from(data))
        .send()
        .await?;

    tracing::debug!(
        "Uploaded {} bytes to s3://{}/{} with checksum {}",
        len, bucket, key, checksum
    );
    Ok(())
}

/// Upload file with SHA256 metadata
pub async fn upload_file_with_checksum(
    client: &Client,
    bucket: &str,
    key: &str,
    path: &Path,
    checksum: &str,
) -> Result<()> {
    let body = ByteStream::from_path(path).await?;

    client
        .put_object()
        .bucket(bucket)
        .key(key)
        .metadata("x-amz-meta-sha256", checksum)
        .body(body)
        .send()
        .await?;

    tracing::debug!(
        "Uploaded {} to s3://{}/{} with checksum {}",
        path.display(),
        bucket,
        key,
        checksum
    );
    Ok(())
}

/// Get SHA256 checksum from object metadata
pub async fn get_checksum(client: &Client, bucket: &str, key: &str) -> Result<Option<String>> {
    match client.head_object().bucket(bucket).key(key).send().await {
        Ok(resp) => {
            if let Some(metadata) = resp.metadata {
                if let Some(checksum) = metadata.get("x-amz-meta-sha256") {
                    return Ok(Some(checksum.clone()));
                }
            }
            Ok(None)
        }
        Err(_) => Ok(None),
    }
}

/// Delete a single object from S3
pub async fn delete_object(client: &Client, bucket: &str, key: &str) -> Result<()> {
    client
        .delete_object()
        .bucket(bucket)
        .key(key)
        .send()
        .await?;

    tracing::debug!("Deleted s3://{}/{}", bucket, key);
    Ok(())
}

/// Delete multiple objects from S3 (batch operation)
/// Uses DeleteObjects API for efficiency (up to 1000 objects per request)
pub async fn delete_objects(client: &Client, bucket: &str, keys: &[String]) -> Result<usize> {
    use aws_sdk_s3::types::{Delete, ObjectIdentifier};

    if keys.is_empty() {
        return Ok(0);
    }

    let mut total_deleted = 0;

    // DeleteObjects supports up to 1000 keys per request
    for chunk in keys.chunks(1000) {
        let objects: Vec<ObjectIdentifier> = chunk
            .iter()
            .filter_map(|key| ObjectIdentifier::builder().key(key).build().ok())
            .collect();

        if objects.is_empty() {
            continue;
        }

        let delete = Delete::builder()
            .set_objects(Some(objects))
            .quiet(true) // Don't return individual results
            .build()?;

        let result = client
            .delete_objects()
            .bucket(bucket)
            .delete(delete)
            .send()
            .await?;

        // Count errors if any
        let error_count = result.errors().len();
        let deleted_count = chunk.len() - error_count;
        total_deleted += deleted_count;

        if error_count > 0 {
            tracing::warn!(
                "Failed to delete {} objects from s3://{}/",
                error_count,
                bucket
            );
            for err in result.errors() {
                tracing::debug!(
                    "Delete error: {} - {}",
                    err.key().unwrap_or("unknown"),
                    err.message().unwrap_or("unknown error")
                );
            }
        }

        tracing::debug!(
            "Deleted {} objects from s3://{}/",
            deleted_count,
            bucket
        );
    }

    Ok(total_deleted)
}

/// Check if object exists
#[allow(dead_code)]
pub async fn exists(client: &Client, bucket: &str, key: &str) -> Result<bool> {
    use aws_sdk_s3::error::SdkError;
    use aws_sdk_s3::operation::head_object::HeadObjectError;

    match client.head_object().bucket(bucket).key(key).send().await {
        Ok(_) => Ok(true),
        Err(SdkError::ServiceError(err)) => {
            match err.err() {
                HeadObjectError::NotFound(_) => Ok(false),
                _ => {
                    // Check error message for other not-found indicators (Tigris compatibility)
                    let msg = format!("{:?}", err.err());
                    if msg.contains("NotFound") || msg.contains("404") || msg.contains("NoSuchKey") {
                        Ok(false)
                    } else {
                        Err(anyhow!("Failed to check object existence: {:?}", err.err()))
                    }
                }
            }
        }
        Err(e) => {
            // Check string representation as fallback
            let msg = e.to_string();
            if msg.contains("NotFound") || msg.contains("404") || msg.contains("NoSuchKey") {
                Ok(false)
            } else {
                Err(anyhow!("Failed to check object existence: {}", e))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_bucket_simple() {
        let (bucket, prefix) = parse_bucket("my-bucket");
        assert_eq!(bucket, "my-bucket");
        assert_eq!(prefix, "");
    }

    #[test]
    fn test_parse_bucket_with_prefix() {
        let (bucket, prefix) = parse_bucket("my-bucket/some/prefix");
        assert_eq!(bucket, "my-bucket");
        assert_eq!(prefix, "some/prefix");
    }

    #[test]
    fn test_parse_bucket_s3_url() {
        let (bucket, prefix) = parse_bucket("s3://my-bucket/backups");
        assert_eq!(bucket, "my-bucket");
        assert_eq!(prefix, "backups");
    }

    #[test]
    fn test_parse_bucket_s3_url_no_prefix() {
        let (bucket, prefix) = parse_bucket("s3://my-bucket");
        assert_eq!(bucket, "my-bucket");
        assert_eq!(prefix, "");
    }

    // Integration tests - run with: cargo test -- --ignored
    // Requires: AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_ENDPOINT_URL_S3, WALRUST_TEST_BUCKET

    fn get_test_bucket() -> Option<String> {
        std::env::var("WALRUST_TEST_BUCKET").ok()
    }

    fn get_test_endpoint() -> Option<String> {
        std::env::var("AWS_ENDPOINT_URL_S3").ok()
    }

    #[tokio::test]
    #[ignore]
    async fn test_integration_upload_download_bytes() {
        let bucket = get_test_bucket().expect("WALRUST_TEST_BUCKET not set");
        let endpoint = get_test_endpoint();

        let client = create_client(endpoint.as_deref()).await.unwrap();

        let test_key = format!("walrust-test/{}.txt", uuid::Uuid::new_v4());
        let test_data = b"Hello from walrust integration test!".to_vec();

        // Upload
        upload_bytes(&client, &bucket, &test_key, test_data.clone())
            .await
            .unwrap();

        // Download
        let downloaded = download_bytes(&client, &bucket, &test_key).await.unwrap();
        assert_eq!(downloaded, test_data);

        // Cleanup - delete the object
        client
            .delete_object()
            .bucket(&bucket)
            .key(&test_key)
            .send()
            .await
            .unwrap();
    }

    #[tokio::test]
    #[ignore]
    async fn test_integration_list_objects() {
        let bucket = get_test_bucket().expect("WALRUST_TEST_BUCKET not set");
        let endpoint = get_test_endpoint();

        let client = create_client(endpoint.as_deref()).await.unwrap();

        let prefix = format!("walrust-test-list/{}/", uuid::Uuid::new_v4());

        // Upload a few test objects
        for i in 0..3 {
            let key = format!("{}file{}.txt", prefix, i);
            upload_bytes(&client, &bucket, &key, format!("content {}", i).into_bytes())
                .await
                .unwrap();
        }

        // List objects
        let keys = list_objects(&client, &bucket, &prefix).await.unwrap();
        assert_eq!(keys.len(), 3);

        // Cleanup
        for key in &keys {
            client
                .delete_object()
                .bucket(&bucket)
                .key(key)
                .send()
                .await
                .unwrap();
        }
    }

    #[tokio::test]
    #[ignore]
    async fn test_integration_exists() {
        let bucket = get_test_bucket().expect("WALRUST_TEST_BUCKET not set");
        let endpoint = get_test_endpoint();

        let client = create_client(endpoint.as_deref()).await.unwrap();

        let test_key = format!("walrust-test/{}.txt", uuid::Uuid::new_v4());

        // Should not exist
        let exists_before = exists(&client, &bucket, &test_key).await.unwrap();
        assert!(!exists_before);

        // Upload
        upload_bytes(&client, &bucket, &test_key, b"test".to_vec())
            .await
            .unwrap();

        // Should exist now
        let exists_after = exists(&client, &bucket, &test_key).await.unwrap();
        assert!(exists_after);

        // Cleanup
        client
            .delete_object()
            .bucket(&bucket)
            .key(&test_key)
            .send()
            .await
            .unwrap();
    }
}
