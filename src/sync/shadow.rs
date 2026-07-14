use anyhow::Result;
use hadb_storage_s3::S3Storage;
use std::sync::Arc;
use walrust_core::legacy_shadow;
#[cfg(test)]
use walrust_core::legacy_shadow::ShadowEncodeResult;

use crate::cache::LocalCache;
use crate::retry::{classify_error, ErrorKind, RetryPolicy};
#[cfg(test)]
use crate::s3;
use crate::uploader::UploadMessage;
use crate::webhook::WebhookSender;

use super::types::{ShadowSyncInput, ShadowSyncOutput};

#[cfg(test)]
fn encode_shadow_to_ltx(input: &ShadowSyncInput) -> Result<Option<(ShadowEncodeResult, u64)>> {
    legacy_shadow::encode_shadow_to_ltx(input)
}

#[cfg(test)]
fn build_empty_output(input: &ShadowSyncInput) -> ShadowSyncOutput {
    legacy_shadow::build_empty_shadow_output(input)
}

/// Sync shadow WAL segments to S3 (direct upload, no cache)
pub(crate) async fn sync_shadow_concurrent(
    client: &aws_sdk_s3::Client,
    bucket: &str,
    prefix: &str,
    input: ShadowSyncInput,
) -> Result<ShadowSyncOutput> {
    let storage = S3Storage::new(client.clone(), bucket.to_string());
    legacy_shadow::sync_shadow_to_storage(&storage, prefix, input).await
}

/// Sync shadow WAL segments to disk cache (cache + uploader)
///
/// Same as sync_shadow_concurrent but writes LTX to local cache
/// and notifies the uploader task instead of uploading directly to S3.
pub(crate) async fn sync_shadow_to_cache(
    cache: &LocalCache,
    upload_tx: &tokio::sync::mpsc::Sender<UploadMessage>,
    input: ShadowSyncInput,
) -> Result<ShadowSyncOutput> {
    legacy_shadow::sync_shadow_to_cache(cache, upload_tx, input).await
}

/// A shadow-sync failure that will never succeed on retry (E1).
///
/// These come from the LTX encoder rejecting structurally invalid frames — a
/// bad page number, TXID, page size, or changeset shape. The canonical case is
/// the post-crash torn-tail corruption that decodes as page 0 and surfaces as
/// "Invalid page num: transaction ID must be non-zero". Retrying such an error
/// cannot help: it only burns the retry budget and buries a hard durability
/// fault under transient-looking WARN spam. We must fail fast and loud instead
/// (single error log + webhook + propagate, which exits the watcher nonzero).
pub(crate) fn is_permanent_encode_error(error: &anyhow::Error) -> bool {
    let msg = error.to_string();
    msg.contains("Invalid page num")
        || msg.contains("Invalid min TXID")
        || msg.contains("Invalid max TXID")
        || msg.contains("Invalid commit page")
        || msg.contains("Invalid page size")
        || msg.contains("Invalid TXID")
        || msg.contains("changeset contains invalid page number")
        || msg.contains("changeset page")
        || msg.contains("is invalid for a SQLite changeset")
}

/// Shared retry driver for the shadow-sync paths. Retries transient errors with
/// backoff, but fails fast on auth errors and on permanent encode/validation
/// errors (E1) so a corrupt-frame fault surfaces hard instead of spinning
/// forever. `label` distinguishes the cache vs direct path in logs.
async fn run_shadow_sync_with_retry<F, Fut>(
    label: &str,
    db_name: &str,
    retry_policy: &RetryPolicy,
    webhook_sender: &WebhookSender,
    mut op: F,
) -> Result<ShadowSyncOutput>
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = Result<ShadowSyncOutput>>,
{
    let mut attempts = 0u32;

    loop {
        attempts += 1;
        match op().await {
            Ok(output) => return Ok(output),
            Err(e) => {
                let error_kind = classify_error(&e);

                if error_kind == ErrorKind::AuthError {
                    tracing::error!("{}: Auth error during {}: {}", db_name, label, e);
                    webhook_sender
                        .notify_auth_failure(db_name, &e.to_string())
                        .await;
                    return Err(e);
                }

                if is_permanent_encode_error(&e) {
                    tracing::error!(
                        "{}: {} hit a non-retryable encode error (frame corruption); \
                         failing fast after {} attempt(s): {}",
                        db_name,
                        label,
                        attempts,
                        e
                    );
                    webhook_sender
                        .notify_upload_failed(db_name, &e.to_string(), attempts)
                        .await;
                    return Err(e);
                }

                let is_retryable = matches!(error_kind, ErrorKind::Transient | ErrorKind::Unknown);
                if !is_retryable || attempts > retry_policy.config().max_retries + 1 {
                    tracing::error!(
                        "{}: {} failed after {} attempts: {}",
                        db_name,
                        label,
                        attempts,
                        e
                    );
                    webhook_sender
                        .notify_upload_failed(db_name, &e.to_string(), attempts)
                        .await;
                    return Err(e);
                }

                let delay = retry_policy.calculate_delay(attempts - 1);
                tracing::warn!(
                    "{}: {} attempt {}/{} failed, retrying in {:?}: {}",
                    db_name,
                    label,
                    attempts,
                    retry_policy.config().max_retries + 1,
                    delay,
                    e
                );
                tokio::time::sleep(delay).await;
            }
        }
    }
}

/// Sync shadow WAL to cache with retry logic
pub(crate) async fn sync_shadow_to_cache_with_retry(
    cache: Arc<LocalCache>,
    upload_tx: tokio::sync::mpsc::Sender<UploadMessage>,
    input: ShadowSyncInput,
    retry_policy: RetryPolicy,
    webhook_sender: Arc<WebhookSender>,
) -> Result<ShadowSyncOutput> {
    let db_name = input.name.clone();
    run_shadow_sync_with_retry(
        "Shadow cache sync",
        &db_name,
        &retry_policy,
        &webhook_sender,
        || sync_shadow_to_cache(&cache, &upload_tx, input.clone()),
    )
    .await
}

/// Sync shadow WAL with retry logic
pub(crate) async fn sync_shadow_concurrent_with_retry(
    client: Arc<aws_sdk_s3::Client>,
    bucket: String,
    prefix: String,
    input: ShadowSyncInput,
    retry_policy: RetryPolicy,
    webhook_sender: Arc<WebhookSender>,
) -> Result<ShadowSyncOutput> {
    let db_name = input.name.clone();
    run_shadow_sync_with_retry(
        "Shadow sync",
        &db_name,
        &retry_policy,
        &webhook_sender,
        || sync_shadow_concurrent(&client, &bucket, &prefix, input.clone()),
    )
    .await
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cache::LocalCache;
    use crate::ltx;
    use crate::retention::RetentionPolicy;
    use crate::sync::manifest::build_ltx_key;
    use crate::sync::prune::prune_with_client;
    use crate::uploader::UploadMessage;
    use std::io::Write;
    use std::time::{SystemTime, UNIX_EPOCH};
    use tempfile::TempDir;
    use tokio::sync::mpsc;

    const PAGE_SIZE: u32 = 4096;

    fn unique_s3_name(prefix: &str) -> String {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        format!("{prefix}-{nanos}")
    }

    fn test_bucket_config() -> (String, Option<String>) {
        let bucket = std::env::var("WALRUST_TEST_BUCKET")
            .unwrap_or_else(|_| "walrust-test-rr-2026/verify-test".to_string());
        let endpoint = std::env::var("AWS_ENDPOINT_URL_S3")
            .or_else(|_| std::env::var("AWS_ENDPOINT_URL"))
            .ok();
        (bucket, endpoint)
    }

    #[test]
    fn e1_classifies_corrupt_frame_error_as_permanent() {
        // The exact drill string, plus the encoder's sibling validation errors.
        assert!(is_permanent_encode_error(&anyhow::anyhow!(
            "Invalid page num: transaction ID must be non-zero"
        )));
        assert!(is_permanent_encode_error(&anyhow::anyhow!(
            "Invalid min TXID: transaction ID must be non-zero"
        )));
        assert!(is_permanent_encode_error(&anyhow::anyhow!(
            "changeset contains invalid page number 0"
        )));
        // Transient S3-shaped errors must stay retryable.
        assert!(!is_permanent_encode_error(&anyhow::anyhow!(
            "connection reset by peer"
        )));
        assert!(!is_permanent_encode_error(&anyhow::anyhow!(
            "503 Service Unavailable"
        )));
    }

    /// E1 loud-failure posture: a non-retryable encode error must fail fast
    /// (exactly one attempt, no retry spin) and still fire the webhook, so the
    /// watcher surfaces the fault hard and exits nonzero instead of silently
    /// retrying forever.
    #[tokio::test]
    async fn e1_permanent_encode_error_fails_fast_without_spinning() {
        let retry_policy = RetryPolicy::new(crate::retry::RetryConfig {
            max_retries: 5,
            base_delay_ms: 100,
            ..Default::default()
        });
        let webhook = WebhookSender::new(vec![]);
        let calls = std::cell::Cell::new(0u32);

        let result =
            run_shadow_sync_with_retry("Shadow sync", "app", &retry_policy, &webhook, || {
                calls.set(calls.get() + 1);
                async {
                    Err(anyhow::anyhow!(
                        "Invalid page num: transaction ID must be non-zero"
                    ))
                }
            })
            .await;

        assert!(result.is_err(), "permanent encode error must propagate");
        assert_eq!(
            calls.get(),
            1,
            "permanent encode error must NOT be retried (no silent spinning)"
        );
    }

    /// Control: a transient error is retried up to the configured budget, then
    /// gives up — proving the fast-fail path above is specific to permanent
    /// encode faults and did not disable normal retry behaviour.
    #[tokio::test]
    async fn e1_transient_error_still_retries_to_budget() {
        let retry_policy = RetryPolicy::new(crate::retry::RetryConfig {
            max_retries: 2,
            base_delay_ms: 1,
            ..Default::default()
        });
        let webhook = WebhookSender::new(vec![]);
        let calls = std::cell::Cell::new(0u32);

        let result =
            run_shadow_sync_with_retry("Shadow sync", "app", &retry_policy, &webhook, || {
                calls.set(calls.get() + 1);
                async { Err(anyhow::anyhow!("connection reset by peer")) }
            })
            .await;

        assert!(result.is_err());
        assert_eq!(
            calls.get(),
            retry_policy.config().max_retries + 2,
            "transient errors must exhaust the retry budget before giving up"
        );
    }

    #[tokio::test]
    async fn test_watch_auto_prune_uses_listing_without_manifest() {
        if std::env::var("AWS_ENDPOINT_URL_S3").is_err()
            && std::env::var("AWS_ENDPOINT_URL").is_err()
            && std::env::var("AWS_ACCESS_KEY_ID").is_err()
        {
            eprintln!("SKIP test_watch_auto_prune_uses_listing_without_manifest: no S3 endpoint/credentials configured");
            return;
        }
        let (bucket_arg, endpoint) = test_bucket_config();
        let (bucket, prefix) = s3::parse_bucket(&bucket_arg);
        let client = s3::create_client(endpoint.as_deref()).await.unwrap();
        let name = unique_s3_name("watch-compact-no-manifest");

        let keep_old = build_ltx_key(&prefix, &name, 1, 1, 1);
        let delete_middle = build_ltx_key(&prefix, &name, 2, 1, 2);
        let keep_latest = build_ltx_key(&prefix, &name, 3, 1, 3);
        let keys = vec![keep_old.clone(), delete_middle.clone(), keep_latest.clone()];

        for key in &keys {
            s3::upload_bytes(&client, &bucket, key, b"snapshot".to_vec())
                .await
                .unwrap();
        }

        let policy = RetentionPolicy::new(0, 0, 0, 0);
        prune_with_client(&client, &bucket, &prefix, &name, &policy, true)
            .await
            .unwrap();

        assert!(
            !s3::exists(&client, &bucket, &delete_middle).await.unwrap(),
            "watch auto-pruning must delete eligible listing-discovered snapshots even without manifest.json"
        );
        assert!(s3::exists(&client, &bucket, &keep_old).await.unwrap());
        assert!(s3::exists(&client, &bucket, &keep_latest).await.unwrap());

        let _ = s3::delete_objects(&client, &bucket, &keys).await;
    }

    #[tokio::test]
    async fn test_watch_auto_prune_preserves_legacy_history_until_native_base_is_visible() {
        if std::env::var("AWS_ENDPOINT_URL_S3").is_err()
            && std::env::var("AWS_ENDPOINT_URL").is_err()
            && std::env::var("AWS_ACCESS_KEY_ID").is_err()
        {
            eprintln!("SKIP test_watch_auto_prune_preserves_legacy_history_until_native_base_is_visible: no S3 endpoint/credentials configured");
            return;
        }
        let (bucket_arg, endpoint) = test_bucket_config();
        let (bucket, prefix) = s3::parse_bucket(&bucket_arg);
        let client = s3::create_client(endpoint.as_deref()).await.unwrap();
        let name = unique_s3_name("watch-prune-native-not-visible");

        let keep_old = build_ltx_key(&prefix, &name, 1, 1, 1);
        let would_delete = build_ltx_key(&prefix, &name, 2, 1, 2);
        let keep_latest = build_ltx_key(&prefix, &name, 3, 1, 3);
        let temp = TempDir::new().unwrap();
        let db_path = temp.path().join("source.db");
        std::fs::write(&db_path, []).unwrap();
        let identity = walrust_core::native_spool::SpoolIdentity::new(
            &db_path,
            bucket.clone(),
            prefix.clone(),
            name.clone(),
            "lineage-without-visible-base",
            4,
            Some(3),
            true,
        )
        .unwrap();
        let descriptor = walrust_core::native_publish::StreamDescriptor::from(&identity);
        let descriptor_key = descriptor.key();
        let keys = vec![
            keep_old.clone(),
            would_delete.clone(),
            keep_latest.clone(),
            descriptor_key.clone(),
        ];
        for key in [&keep_old, &would_delete, &keep_latest] {
            s3::upload_bytes(&client, &bucket, key, b"snapshot".to_vec())
                .await
                .unwrap();
        }
        s3::upload_bytes(
            &client,
            &bucket,
            &descriptor_key,
            descriptor.bytes().unwrap(),
        )
        .await
        .unwrap();

        let policy = RetentionPolicy::new(0, 0, 0, 0);
        prune_with_client(&client, &bucket, &prefix, &name, &policy, true)
            .await
            .unwrap();

        for key in [&keep_old, &would_delete, &keep_latest] {
            assert!(
                s3::exists(&client, &bucket, key).await.unwrap(),
                "watch retention deleted legacy recovery object {key} before a contiguous native snapshot became visible"
            );
        }

        let _ = s3::delete_objects(&client, &bucket, &keys).await;
    }

    /// Create a shadow WAL segment file with the given frames.
    /// Each frame is (page_number, db_size, page_data).
    fn write_shadow_segment(
        dir: &std::path::Path,
        generation: u64,
        index: u32,
        frames: &[(u32, u32, &[u8])],
    ) {
        let filename = crate::shadow::format_segment_name(generation, index as u64);
        let path = dir.join(filename);
        let mut file = std::fs::File::create(&path).unwrap();

        for (page_number, db_size, page_data) in frames {
            // 24-byte header: page_number(4) + db_size(4) + padding(16)
            let mut header = [0u8; 24];
            header[0..4].copy_from_slice(&page_number.to_be_bytes());
            header[4..8].copy_from_slice(&db_size.to_be_bytes());
            file.write_all(&header).unwrap();
            file.write_all(page_data).unwrap();
        }
    }

    fn make_page(fill: u8) -> Vec<u8> {
        vec![fill; PAGE_SIZE as usize]
    }

    fn make_input(
        db_path: &std::path::Path,
        shadow_dir: &std::path::Path,
        generation: u64,
        current_txid: u64,
        shadow_sync_offset: u64,
    ) -> ShadowSyncInput {
        // current_txid must be > 0 so that min_txid > 1, otherwise LTX treats
        // it as a snapshot and requires no pre-checksum.
        let current_txid = if current_txid == 0 { 10 } else { current_txid };
        ShadowSyncInput {
            db_path: db_path.to_path_buf(),
            name: "test_db".to_string(),
            current_txid,
            db_checksum: Some(0x12345678), // Provide checksum to avoid reading db file
            generation,
            shadow_sync_offset,
            page_size: PAGE_SIZE,
            shadow_dir: shadow_dir.to_path_buf(),
        }
    }

    // ============================================
    // encode_shadow_to_ltx tests
    // ============================================

    #[test]
    fn test_encode_empty_shadow_dir() {
        let temp = TempDir::new().unwrap();
        let shadow_dir = temp.path().join("shadow");
        std::fs::create_dir_all(&shadow_dir).unwrap();
        let db_path = temp.path().join("test.db");
        std::fs::write(&db_path, &[0u8; 4096]).unwrap();

        let input = make_input(&db_path, &shadow_dir, 1, 0, 0);
        let result = encode_shadow_to_ltx(&input).unwrap();
        assert!(result.is_none(), "Empty shadow dir should return None");
    }

    #[test]
    fn test_encode_single_frame() {
        let temp = TempDir::new().unwrap();
        let shadow_dir = temp.path().join("shadow");
        std::fs::create_dir_all(&shadow_dir).unwrap();
        let db_path = temp.path().join("test.db");
        std::fs::write(&db_path, &make_page(0x00)).unwrap();

        let page_data = make_page(0xAA);
        write_shadow_segment(&shadow_dir, 1, 0, &[(1, 1, &page_data)]);

        let input = make_input(&db_path, &shadow_dir, 1, 0, 0);
        let result = encode_shadow_to_ltx(&input).unwrap();
        assert!(result.is_some(), "Should encode single frame");

        let (encoded, new_offset) = result.unwrap();
        assert_eq!(encoded.unique_pages, 1);
        assert_eq!(encoded.frame_count, 1);
        assert_eq!(encoded.min_txid, 11); // current_txid=10, so min=11
        assert_eq!(encoded.max_txid, 11);
        assert!(!encoded.ltx_buffer.is_empty());
        assert_eq!(new_offset, 24 + PAGE_SIZE as u64); // one frame
    }

    #[test]
    fn test_encode_multiple_frames_deduplicates() {
        let temp = TempDir::new().unwrap();
        let shadow_dir = temp.path().join("shadow");
        std::fs::create_dir_all(&shadow_dir).unwrap();
        let db_path = temp.path().join("test.db");
        std::fs::write(&db_path, &make_page(0x00)).unwrap();

        let page1_v1 = make_page(0xAA);
        let page1_v2 = make_page(0xBB);
        let page2 = make_page(0xCC);

        // page 1 written twice — only last write should be in output
        write_shadow_segment(
            &shadow_dir,
            1,
            0,
            &[
                (1, 2, &page1_v1),
                (2, 2, &page2),
                (1, 2, &page1_v2), // overwrites page 1
            ],
        );

        let input = make_input(&db_path, &shadow_dir, 1, 0, 0);
        let result = encode_shadow_to_ltx(&input).unwrap().unwrap();

        let (encoded, _) = result;
        // 3 frames read, but only 2 unique pages
        assert_eq!(encoded.frame_count, 3);
        assert_eq!(encoded.unique_pages, 2);
        assert_eq!(encoded.min_txid, 11); // current_txid=10
        assert_eq!(encoded.max_txid, 12); // 2 unique pages
    }

    #[test]
    fn test_encode_uses_last_commit_db_size_not_max() {
        let temp = TempDir::new().unwrap();
        let shadow_dir = temp.path().join("shadow");
        std::fs::create_dir_all(&shadow_dir).unwrap();
        let db_path = temp.path().join("test.db");
        std::fs::write(&db_path, &make_page(0x00)).unwrap();

        let page1 = make_page(0xAA);
        let page2 = make_page(0xBB);
        write_shadow_segment(&shadow_dir, 1, 0, &[(1, 5, &page1), (2, 3, &page2)]);

        let input = make_input(&db_path, &shadow_dir, 1, 0, 0);
        let (encoded, _) = encode_shadow_to_ltx(&input).unwrap().unwrap();
        let header = ltx::verify_ltx(std::io::Cursor::new(&encoded.ltx_buffer)).unwrap();

        assert_eq!(header.commit.into_inner(), 3);
    }

    #[test]
    fn test_encode_waits_for_commit_frame() {
        let temp = TempDir::new().unwrap();
        let shadow_dir = temp.path().join("shadow");
        std::fs::create_dir_all(&shadow_dir).unwrap();
        let db_path = temp.path().join("test.db");
        std::fs::write(&db_path, &make_page(0x00)).unwrap();

        let page1 = make_page(0xAA);
        let page2 = make_page(0xBB);
        write_shadow_segment(&shadow_dir, 1, 0, &[(1, 0, &page1), (2, 0, &page2)]);

        let input = make_input(&db_path, &shadow_dir, 1, 0, 0);
        let result = encode_shadow_to_ltx(&input).unwrap();

        assert!(
            result.is_none(),
            "uncommitted shadow frames must not publish LTX"
        );
    }

    #[test]
    fn test_encode_ignores_trailing_uncommitted_frames() {
        let temp = TempDir::new().unwrap();
        let shadow_dir = temp.path().join("shadow");
        std::fs::create_dir_all(&shadow_dir).unwrap();
        let db_path = temp.path().join("test.db");
        std::fs::write(&db_path, &make_page(0x00)).unwrap();

        let page1 = make_page(0xAA);
        let page2 = make_page(0xBB);
        let page3 = make_page(0xCC);
        write_shadow_segment(
            &shadow_dir,
            1,
            0,
            &[(1, 0, &page1), (2, 2, &page2), (3, 0, &page3)],
        );

        let frame_size = 24 + PAGE_SIZE as u64;
        let input = make_input(&db_path, &shadow_dir, 1, 0, 0);
        let result = encode_shadow_to_ltx(&input).unwrap().unwrap();

        let (encoded, new_offset) = result;
        assert_eq!(encoded.frame_count, 2);
        assert_eq!(encoded.unique_pages, 2);
        assert_eq!(new_offset, frame_size * 2);
    }

    #[test]
    fn test_encode_skips_wrong_generation() {
        let temp = TempDir::new().unwrap();
        let shadow_dir = temp.path().join("shadow");
        std::fs::create_dir_all(&shadow_dir).unwrap();
        let db_path = temp.path().join("test.db");
        std::fs::write(&db_path, &make_page(0x00)).unwrap();

        let page = make_page(0xAA);
        // Generation 1 segment, but input asks for generation 2
        write_shadow_segment(&shadow_dir, 1, 0, &[(1, 1, &page)]);

        let input = make_input(&db_path, &shadow_dir, 2, 0, 0);
        let result = encode_shadow_to_ltx(&input).unwrap();
        assert!(result.is_none(), "Wrong generation should be skipped");
    }

    #[test]
    fn test_encode_respects_shadow_sync_offset() {
        let temp = TempDir::new().unwrap();
        let shadow_dir = temp.path().join("shadow");
        std::fs::create_dir_all(&shadow_dir).unwrap();
        let db_path = temp.path().join("test.db");
        std::fs::write(&db_path, &make_page(0x00)).unwrap();

        let page1 = make_page(0xAA);
        let page2 = make_page(0xBB);
        write_shadow_segment(&shadow_dir, 1, 0, &[(1, 2, &page1), (2, 2, &page2)]);

        let frame_size = 24 + PAGE_SIZE as u64;

        // Offset past first frame — should only encode second frame
        let input = make_input(&db_path, &shadow_dir, 1, 0, frame_size);
        let result = encode_shadow_to_ltx(&input).unwrap().unwrap();

        let (encoded, new_offset) = result;
        assert_eq!(encoded.frame_count, 1);
        assert_eq!(encoded.unique_pages, 1);
        assert_eq!(new_offset, frame_size * 2);
    }

    #[test]
    fn test_encode_multiple_segments() {
        let temp = TempDir::new().unwrap();
        let shadow_dir = temp.path().join("shadow");
        std::fs::create_dir_all(&shadow_dir).unwrap();
        let db_path = temp.path().join("test.db");
        std::fs::write(&db_path, &make_page(0x00)).unwrap();

        let page1 = make_page(0xAA);
        let page2 = make_page(0xBB);

        write_shadow_segment(&shadow_dir, 1, 0, &[(1, 2, &page1)]);
        write_shadow_segment(&shadow_dir, 1, 1, &[(2, 2, &page2)]);

        let input = make_input(&db_path, &shadow_dir, 1, 0, 0);
        let result = encode_shadow_to_ltx(&input).unwrap().unwrap();

        let (encoded, _) = result;
        assert_eq!(encoded.frame_count, 2);
        assert_eq!(encoded.unique_pages, 2);
    }

    #[test]
    fn test_encode_offset_spans_segments() {
        let temp = TempDir::new().unwrap();
        let shadow_dir = temp.path().join("shadow");
        std::fs::create_dir_all(&shadow_dir).unwrap();
        let db_path = temp.path().join("test.db");
        std::fs::write(&db_path, &make_page(0x00)).unwrap();

        let page1 = make_page(0xAA);
        let page2 = make_page(0xBB);
        let page3 = make_page(0xCC);

        // Segment 0: 2 frames, segment 1: 1 frame
        write_shadow_segment(&shadow_dir, 1, 0, &[(1, 3, &page1), (2, 3, &page2)]);
        write_shadow_segment(&shadow_dir, 1, 1, &[(3, 3, &page3)]);

        let frame_size = 24 + PAGE_SIZE as u64;
        // Offset past first segment entirely (2 frames)
        let input = make_input(&db_path, &shadow_dir, 1, 2, frame_size * 2);
        let result = encode_shadow_to_ltx(&input).unwrap().unwrap();

        let (encoded, _) = result;
        assert_eq!(encoded.frame_count, 1);
        assert_eq!(encoded.unique_pages, 1);
        assert_eq!(encoded.min_txid, 3);
        assert_eq!(encoded.max_txid, 3);
    }

    // ============================================
    // sync_shadow_to_cache tests
    // ============================================

    #[tokio::test]
    async fn test_sync_shadow_to_cache_basic() {
        let temp = TempDir::new().unwrap();
        let shadow_dir = temp.path().join("shadow");
        std::fs::create_dir_all(&shadow_dir).unwrap();
        let db_path = temp.path().join("test.db");
        std::fs::write(&db_path, &make_page(0x00)).unwrap();
        let cache_dir = temp.path().join("cache");
        std::fs::create_dir_all(&cache_dir).unwrap();

        let cache = LocalCache::new(&cache_dir).unwrap();
        let (tx, mut rx) = mpsc::channel::<UploadMessage>(10);

        let page = make_page(0xAA);
        write_shadow_segment(&shadow_dir, 1, 0, &[(1, 1, &page)]);

        let input = make_input(&db_path, &shadow_dir, 1, 0, 0);
        let output = sync_shadow_to_cache(&cache, &tx, input).await.unwrap();

        assert_eq!(output.frame_count, 1);
        assert_eq!(output.new_current_txid, 11); // current_txid=10, 1 unique page
        assert!(output.new_db_checksum.is_some());

        // Verify cache has the LTX (keyed by max_txid)
        let pending = cache.pending_uploads();
        assert_eq!(pending.len(), 1);
        assert_eq!(pending[0], 11);

        // Verify uploader was notified
        let msg = rx.try_recv().unwrap();
        assert!(matches!(msg, UploadMessage::Upload(11)));
    }

    #[tokio::test]
    async fn test_sync_shadow_to_cache_no_frames() {
        let temp = TempDir::new().unwrap();
        let shadow_dir = temp.path().join("shadow");
        std::fs::create_dir_all(&shadow_dir).unwrap();
        let db_path = temp.path().join("test.db");
        std::fs::write(&db_path, &make_page(0x00)).unwrap();
        let cache_dir = temp.path().join("cache");
        std::fs::create_dir_all(&cache_dir).unwrap();

        let cache = LocalCache::new(&cache_dir).unwrap();
        let (tx, mut rx) = mpsc::channel::<UploadMessage>(10);

        // Empty shadow dir — no frames
        let input = make_input(&db_path, &shadow_dir, 1, 0, 0);
        let output = sync_shadow_to_cache(&cache, &tx, input).await.unwrap();

        assert_eq!(output.frame_count, 0);
        assert_eq!(output.new_current_txid, 10); // preserved from input (make_input remaps 0→10)
        assert!(cache.pending_uploads().is_empty());
        assert!(
            rx.try_recv().is_err(),
            "No upload notification for empty sync"
        );
    }

    #[tokio::test]
    async fn test_sync_shadow_to_cache_multiple_frames() {
        let temp = TempDir::new().unwrap();
        let shadow_dir = temp.path().join("shadow");
        std::fs::create_dir_all(&shadow_dir).unwrap();
        let db_path = temp.path().join("test.db");
        std::fs::write(&db_path, &make_page(0x00)).unwrap();
        let cache_dir = temp.path().join("cache");
        std::fs::create_dir_all(&cache_dir).unwrap();

        let cache = LocalCache::new(&cache_dir).unwrap();
        let (tx, mut rx) = mpsc::channel::<UploadMessage>(10);

        let page1 = make_page(0xAA);
        let page2 = make_page(0xBB);
        let page3 = make_page(0xCC);
        write_shadow_segment(
            &shadow_dir,
            1,
            0,
            &[(1, 3, &page1), (2, 3, &page2), (3, 3, &page3)],
        );

        let input = make_input(&db_path, &shadow_dir, 1, 0, 0);
        let output = sync_shadow_to_cache(&cache, &tx, input).await.unwrap();

        assert_eq!(output.frame_count, 3);
        assert_eq!(output.new_current_txid, 13); // current_txid=10, 3 unique pages
        assert!(output.new_db_checksum.is_some());

        // Cache has the LTX file (keyed by max_txid)
        let ltx_data = cache.read_ltx(13).unwrap();
        assert!(!ltx_data.is_empty());

        // Uploader notified with max_txid
        let msg = rx.try_recv().unwrap();
        assert!(matches!(msg, UploadMessage::Upload(13)));
    }

    #[tokio::test]
    async fn test_sync_shadow_to_cache_incremental() {
        let temp = TempDir::new().unwrap();
        let shadow_dir = temp.path().join("shadow");
        std::fs::create_dir_all(&shadow_dir).unwrap();
        let db_path = temp.path().join("test.db");
        std::fs::write(&db_path, &make_page(0x00)).unwrap();
        let cache_dir = temp.path().join("cache");
        std::fs::create_dir_all(&cache_dir).unwrap();

        let cache = LocalCache::new(&cache_dir).unwrap();
        let (tx, mut rx) = mpsc::channel::<UploadMessage>(10);

        let page1 = make_page(0xAA);
        let page2 = make_page(0xBB);
        write_shadow_segment(&shadow_dir, 1, 0, &[(1, 2, &page1), (2, 2, &page2)]);

        // First sync
        let input = make_input(&db_path, &shadow_dir, 1, 0, 0);
        let output1 = sync_shadow_to_cache(&cache, &tx, input).await.unwrap();
        assert_eq!(output1.frame_count, 2);
        let _ = rx.try_recv().unwrap(); // consume notification

        // Add more frames
        let page3 = make_page(0xCC);
        write_shadow_segment(&shadow_dir, 1, 1, &[(3, 3, &page3)]);

        // Second sync with updated offset and txid
        let input2 = ShadowSyncInput {
            db_path: db_path.clone(),
            name: "test_db".to_string(),
            current_txid: output1.new_current_txid,
            db_checksum: output1.new_db_checksum,
            generation: 1,
            shadow_sync_offset: output1.new_shadow_sync_offset,
            page_size: PAGE_SIZE,
            shadow_dir: shadow_dir.clone(),
        };
        let output2 = sync_shadow_to_cache(&cache, &tx, input2).await.unwrap();

        assert_eq!(output2.frame_count, 1);
        assert!(output2.new_current_txid > output1.new_current_txid);
        assert!(output2.new_shadow_sync_offset > output1.new_shadow_sync_offset);

        // Second upload notification
        let msg = rx.try_recv().unwrap();
        assert!(matches!(msg, UploadMessage::Upload(_)));
    }

    #[tokio::test]
    async fn test_sync_shadow_to_cache_closed_channel() {
        let temp = TempDir::new().unwrap();
        let shadow_dir = temp.path().join("shadow");
        std::fs::create_dir_all(&shadow_dir).unwrap();
        let db_path = temp.path().join("test.db");
        std::fs::write(&db_path, &make_page(0x00)).unwrap();
        let cache_dir = temp.path().join("cache");
        std::fs::create_dir_all(&cache_dir).unwrap();

        let cache = LocalCache::new(&cache_dir).unwrap();
        let (tx, rx) = mpsc::channel::<UploadMessage>(10);
        drop(rx); // Close receiver — send should fail

        let page = make_page(0xAA);
        write_shadow_segment(&shadow_dir, 1, 0, &[(1, 1, &page)]);

        let input = make_input(&db_path, &shadow_dir, 1, 0, 0);
        let result = sync_shadow_to_cache(&cache, &tx, input).await;

        assert!(
            result.is_err(),
            "Should fail when uploader channel is closed"
        );
        let err = result.unwrap_err().to_string();
        assert!(err.contains("Failed to notify uploader"), "Error: {}", err);
    }

    // ============================================
    // build_output / build_empty_output tests
    // ============================================

    #[test]
    fn test_build_empty_output_preserves_state() {
        let input = ShadowSyncInput {
            db_path: std::path::PathBuf::from("/tmp/test.db"),
            name: "test".to_string(),
            current_txid: 42,
            db_checksum: Some(0xDEADBEEF),
            generation: 1,
            shadow_sync_offset: 1000,
            page_size: PAGE_SIZE,
            shadow_dir: std::path::PathBuf::from("/tmp/shadow"),
        };
        let output = build_empty_output(&input);

        assert_eq!(output.frame_count, 0);
        assert_eq!(output.new_current_txid, 42);
        assert_eq!(output.new_shadow_sync_offset, 1000);
        assert_eq!(output.new_db_checksum, Some(0xDEADBEEF));
    }
}
