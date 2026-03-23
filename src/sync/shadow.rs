use anyhow::Result;
use chrono::Utc;
use std::sync::Arc;

use crate::cache::LocalCache;
use crate::ltx;
use crate::retention::{analyze_retention, RetentionPolicy, SnapshotEntry};
use crate::retry::{classify_error, ErrorKind, RetryPolicy};
use crate::s3;
use crate::uploader::UploadMessage;
use crate::webhook::WebhookSender;

use super::manifest::{build_ltx_key, load_manifest, save_manifest, GENERATION_LIVE};
use super::types::{LtxEntry, Manifest, ShadowSyncInput, ShadowSyncOutput};

/// Result of encoding shadow WAL segments into LTX
struct ShadowEncodeResult {
    ltx_buffer: Vec<u8>,
    post_checksum: litepages::Checksum,
    frame_count: usize,
    unique_pages: usize,
    min_txid: u64,
    max_txid: u64,
}

/// Read shadow WAL segments and encode into LTX buffer.
/// Returns None if no new frames to sync.
fn encode_shadow_to_ltx(input: &ShadowSyncInput) -> Result<Option<(ShadowEncodeResult, u64)>> {
    use litepages::Checksum;

    let shadow_dir = &input.shadow_dir;
    let mut page_map: std::collections::HashMap<u32, Vec<u8>> = std::collections::HashMap::new();
    let mut max_db_size = 0u32;
    let mut frame_count = 0usize;
    let mut total_offset = 0u64;
    let frame_size = 24u64 + input.page_size as u64;

    let mut entries: Vec<_> = std::fs::read_dir(shadow_dir)?
        .filter_map(|e| e.ok())
        .filter(|e| e.file_name().to_string_lossy().ends_with(".wal"))
        .collect();
    entries.sort_by_key(|e| e.file_name());

    for entry in entries {
        let name = entry.file_name();
        let name_str = name.to_string_lossy();

        let parts: Vec<&str> = name_str.trim_end_matches(".wal").split('-').collect();
        if parts.len() != 2 {
            continue;
        }
        let gen = u64::from_str_radix(parts[0], 16).unwrap_or(u64::MAX);
        if gen != input.generation {
            continue;
        }

        let path = entry.path();
        let metadata = std::fs::metadata(&path)?;
        let segment_size = metadata.len();
        let segment_start = total_offset;
        let segment_end = segment_start + segment_size;

        if segment_end <= input.shadow_sync_offset {
            total_offset = segment_end;
            continue;
        }

        let mut file = std::fs::File::open(&path)?;
        use std::io::{Read, Seek, SeekFrom};

        let relative_offset = if input.shadow_sync_offset > segment_start {
            input.shadow_sync_offset - segment_start
        } else {
            0
        };

        file.seek(SeekFrom::Start(relative_offset))?;

        let bytes_to_read = segment_size - relative_offset;
        let segment_frames = bytes_to_read / frame_size;

        let mut page_data = vec![0u8; input.page_size as usize];
        for _ in 0..segment_frames {
            let mut header = [0u8; 24];
            file.read_exact(&mut header)?;

            let page_number = u32::from_be_bytes([header[0], header[1], header[2], header[3]]);
            let db_size = u32::from_be_bytes([header[4], header[5], header[6], header[7]]);

            file.read_exact(&mut page_data)?;

            max_db_size = max_db_size.max(db_size);
            page_map.insert(page_number, page_data.clone());
            frame_count += 1;
        }

        total_offset = segment_end;
    }

    if page_map.is_empty() {
        return Ok(None);
    }

    let pages: Vec<(u32, Vec<u8>)> = page_map.into_iter().collect();
    let pre_checksum = input.db_checksum.map(|cs| Checksum::new(cs));

    let min_txid = input.current_txid + 1;
    let max_txid = min_txid + pages.len() as u64 - 1;
    let commit_page = if max_db_size > 0 { max_db_size } else { 1 };

    let unique_pages = pages.len();
    let estimated_size = unique_pages.saturating_mul(input.page_size as usize).saturating_mul(2);
    let page_size = input.page_size;

    let expected_post = if let Some(pre) = pre_checksum {
        ltx::chain_checksum(pre, &pages)
    } else {
        ltx::compute_checksum_from_file(&input.db_path)?
    };

    let mut ltx_buffer = Vec::with_capacity(estimated_size);
    let post_checksum = ltx::encode_wal_changes(
        &mut ltx_buffer,
        &pages,
        page_size,
        min_txid,
        max_txid,
        commit_page,
        pre_checksum,
        expected_post,
    )?;

    let new_offset = input.shadow_sync_offset + (frame_count as u64 * frame_size);

    Ok(Some((
        ShadowEncodeResult {
            ltx_buffer,
            post_checksum,
            frame_count,
            unique_pages,
            min_txid,
            max_txid,
        },
        new_offset,
    )))
}

/// Build ShadowSyncOutput from encode result
fn build_output(
    input: &ShadowSyncInput,
    encoded: &ShadowEncodeResult,
    new_offset: u64,
) -> ShadowSyncOutput {
    ShadowSyncOutput {
        db_path: input.db_path.clone(),
        frame_count: encoded.unique_pages as u64,
        new_shadow_sync_offset: new_offset,
        new_current_txid: encoded.max_txid,
        new_db_checksum: Some(encoded.post_checksum.into_inner()),
    }
}

/// Build empty ShadowSyncOutput (no frames to sync)
fn build_empty_output(input: &ShadowSyncInput) -> ShadowSyncOutput {
    ShadowSyncOutput {
        db_path: input.db_path.clone(),
        frame_count: 0,
        new_shadow_sync_offset: input.shadow_sync_offset,
        new_current_txid: input.current_txid,
        new_db_checksum: input.db_checksum,
    }
}

/// Sync shadow WAL segments to S3 (direct upload, no cache)
pub(crate) async fn sync_shadow_concurrent(
    client: &aws_sdk_s3::Client,
    bucket: &str,
    prefix: &str,
    input: ShadowSyncInput,
) -> Result<ShadowSyncOutput> {
    // Encoding is CPU-bound, run in blocking thread pool
    let input_clone = input.clone();
    let result = tokio::task::spawn_blocking(move || {
        encode_shadow_to_ltx(&input_clone)
    }).await??;

    let (encoded, new_offset) = match result {
        Some(r) => r,
        None => return Ok(build_empty_output(&input)),
    };

    let ltx_key = build_ltx_key(prefix, &input.name, GENERATION_LIVE, encoded.min_txid, encoded.max_txid);
    let ltx_size = encoded.ltx_buffer.len() as u64;
    let output = build_output(&input, &encoded, new_offset);

    s3::upload_bytes(client, bucket, &ltx_key, encoded.ltx_buffer).await?;

    tracing::info!(
        "{}: Shadow sync uploaded {} frames ({} bytes, {} unique pages, TXID {}-{}) -> {}",
        input.name,
        encoded.frame_count,
        ltx_size,
        encoded.unique_pages,
        encoded.min_txid,
        encoded.max_txid,
        ltx_key
    );

    Ok(output)
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
    let input_clone = input.clone();
    let result = tokio::task::spawn_blocking(move || {
        encode_shadow_to_ltx(&input_clone)
    }).await??;

    let (encoded, new_offset) = match result {
        Some(r) => r,
        None => return Ok(build_empty_output(&input)),
    };

    let ltx_size = encoded.ltx_buffer.len() as u64;

    // Write to disk cache (atomic: temp file + rename)
    cache.write_ltx(encoded.max_txid, &encoded.ltx_buffer)?;

    // Notify uploader task
    upload_tx
        .send(UploadMessage::Upload(encoded.max_txid))
        .await
        .map_err(|e| anyhow::anyhow!("Failed to notify uploader: {}", e))?;

    tracing::info!(
        "{}: Shadow sync cached {} frames ({} bytes, {} unique pages, TXID {}-{})",
        input.name,
        encoded.frame_count,
        ltx_size,
        encoded.unique_pages,
        encoded.min_txid,
        encoded.max_txid,
    );

    Ok(build_output(&input, &encoded, new_offset))
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
    let mut attempts = 0u32;

    loop {
        attempts += 1;
        match sync_shadow_to_cache(&cache, &upload_tx, input.clone()).await {
            Ok(output) => return Ok(output),
            Err(e) => {
                let error_kind = classify_error(&e);
                let is_retryable = matches!(error_kind, ErrorKind::Transient | ErrorKind::Unknown);

                if error_kind == ErrorKind::AuthError {
                    tracing::error!("{}: Auth error during shadow cache sync: {}", db_name, e);
                    webhook_sender
                        .notify_auth_failure(&db_name, &e.to_string())
                        .await;
                    return Err(e);
                }

                if !is_retryable || attempts > retry_policy.config().max_retries + 1 {
                    tracing::error!(
                        "{}: Shadow cache sync failed after {} attempts: {}",
                        db_name,
                        attempts,
                        e
                    );
                    webhook_sender
                        .notify_sync_failed(&db_name, &e.to_string(), attempts)
                        .await;
                    return Err(e);
                }

                let delay = retry_policy.calculate_delay(attempts - 1);
                tracing::warn!(
                    "{}: Shadow cache sync attempt {}/{} failed, retrying in {:?}: {}",
                    db_name,
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
    let mut attempts = 0u32;

    loop {
        attempts += 1;
        match sync_shadow_concurrent(&client, &bucket, &prefix, input.clone()).await {
            Ok(output) => return Ok(output),
            Err(e) => {
                let error_kind = classify_error(&e);
                let is_retryable = matches!(error_kind, ErrorKind::Transient | ErrorKind::Unknown);

                if error_kind == ErrorKind::AuthError {
                    tracing::error!("{}: Auth error during shadow sync: {}", db_name, e);
                    webhook_sender
                        .notify_auth_failure(&db_name, &e.to_string())
                        .await;
                    return Err(e);
                }

                if !is_retryable || attempts > retry_policy.config().max_retries + 1 {
                    tracing::error!(
                        "{}: Shadow sync failed after {} attempts: {}",
                        db_name,
                        attempts,
                        e
                    );
                    webhook_sender
                        .notify_sync_failed(&db_name, &e.to_string(), attempts)
                        .await;
                    return Err(e);
                }

                let delay = retry_policy.calculate_delay(attempts - 1);
                tracing::warn!(
                    "{}: Shadow sync attempt {}/{} failed, retrying in {:?}: {}",
                    db_name,
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

/// Internal compaction for watch mode (non-interactive, always force)
pub(crate) async fn run_compaction(
    client: &aws_sdk_s3::Client,
    bucket: &str,
    prefix: &str,
    name: &str,
    policy: &RetentionPolicy,
) -> Result<()> {
    // Load manifest to get snapshot info
    let manifest = load_manifest(client, bucket, prefix, name).await?;

    if manifest.files.is_empty() {
        return Ok(());
    }

    // Filter to only snapshots (not incremental files)
    let snapshot_entries: Vec<SnapshotEntry> = manifest
        .files
        .iter()
        .filter(|f| f.is_snapshot)
        .filter_map(|f| {
            chrono::DateTime::parse_from_rfc3339(&f.created_at)
                .ok()
                .map(|dt| SnapshotEntry {
                    filename: f.filename.clone(),
                    created_at: dt.with_timezone(&Utc),
                    max_txid: f.max_txid,
                    size: f.size,
                })
        })
        .collect();

    if snapshot_entries.is_empty() {
        return Ok(());
    }

    let now = Utc::now();
    let plan = analyze_retention(&snapshot_entries, policy, now);

    if !plan.has_deletions() {
        tracing::debug!("Compaction for {}: nothing to delete", name);
        return Ok(());
    }

    tracing::info!(
        "Compacting {}: deleting {} snapshots, keeping {}",
        name,
        plan.delete.len(),
        plan.keep.len()
    );

    // Delete files
    let keys_to_delete: Vec<String> = plan
        .delete
        .iter()
        .map(|e| format!("{}{}/{}", prefix, name, e.filename))
        .collect();

    let deleted_count = s3::delete_objects(client, bucket, &keys_to_delete).await?;

    // Update manifest to remove deleted entries
    let kept_filenames: std::collections::HashSet<_> =
        plan.keep.iter().map(|e| e.filename.as_str()).collect();

    let updated_files: Vec<LtxEntry> = manifest
        .files
        .into_iter()
        .filter(|f| !f.is_snapshot || kept_filenames.contains(f.filename.as_str()))
        .collect();

    let updated_manifest = Manifest {
        files: updated_files,
        ..manifest
    };

    save_manifest(client, bucket, prefix, &updated_manifest).await?;

    tracing::info!(
        "Compaction complete for {}: deleted {} snapshots, freed {:.2} MB",
        name,
        deleted_count,
        plan.bytes_freed as f64 / (1024.0 * 1024.0)
    );

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cache::LocalCache;
    use crate::uploader::UploadMessage;
    use std::io::Write;
    use tempfile::TempDir;
    use tokio::sync::mpsc;

    const PAGE_SIZE: u32 = 4096;

    /// Create a shadow WAL segment file with the given frames.
    /// Each frame is (page_number, db_size, page_data).
    fn write_shadow_segment(
        dir: &std::path::Path,
        generation: u64,
        index: u32,
        frames: &[(u32, u32, &[u8])],
    ) {
        let filename = format!("{:016x}-{:08x}.wal", generation, index);
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
        write_shadow_segment(&shadow_dir, 1, 0, &[
            (1, 2, &page1_v1),
            (2, 2, &page2),
            (1, 2, &page1_v2), // overwrites page 1
        ]);

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
        write_shadow_segment(&shadow_dir, 1, 0, &[
            (1, 2, &page1),
            (2, 2, &page2),
        ]);

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
        assert!(rx.try_recv().is_err(), "No upload notification for empty sync");
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
        write_shadow_segment(&shadow_dir, 1, 0, &[
            (1, 3, &page1),
            (2, 3, &page2),
            (3, 3, &page3),
        ]);

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
        write_shadow_segment(&shadow_dir, 1, 0, &[
            (1, 2, &page1),
            (2, 2, &page2),
        ]);

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

        assert!(result.is_err(), "Should fail when uploader channel is closed");
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
