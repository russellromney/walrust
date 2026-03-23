use anyhow::Result;
use chrono::Utc;
use std::sync::Arc;

use crate::ltx;
use crate::retention::{analyze_retention, RetentionPolicy, SnapshotEntry};
use crate::retry::{classify_error, ErrorKind, RetryPolicy};
use crate::s3;
use crate::webhook::WebhookSender;

use super::manifest::{build_ltx_key, load_manifest, save_manifest, GENERATION_LIVE};
use super::types::{LtxEntry, Manifest, ShadowSyncInput, ShadowSyncOutput};

/// Sync shadow WAL segments to S3
pub(crate) async fn sync_shadow_concurrent(
    client: &aws_sdk_s3::Client,
    bucket: &str,
    prefix: &str,
    input: ShadowSyncInput,
) -> Result<ShadowSyncOutput> {
    use litepages::Checksum;

    // Read frames from shadow segments, deduplicating into page_map during read.
    // Peak memory = unique pages, not total frames.
    let shadow_dir = &input.shadow_dir;
    let mut page_map: std::collections::HashMap<u32, Vec<u8>> = std::collections::HashMap::new();
    let mut max_db_size = 0u32;
    let mut frame_count = 0usize;
    let mut total_offset = 0u64;
    let frame_size = 24u64 + input.page_size as u64;

    // List segment files for the current generation
    let mut entries: Vec<_> = std::fs::read_dir(shadow_dir)?
        .filter_map(|e| e.ok())
        .filter(|e| e.file_name().to_string_lossy().ends_with(".wal"))
        .collect();
    entries.sort_by_key(|e| e.file_name());

    for entry in entries {
        let name = entry.file_name();
        let name_str = name.to_string_lossy();

        // Parse generation from filename: {gen:08x}-{idx:08x}.wal
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

        // Skip if we've already synced past this segment
        if segment_end <= input.shadow_sync_offset {
            total_offset = segment_end;
            continue;
        }

        // Read frames from this segment directly into page_map
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
        return Ok(ShadowSyncOutput {
            db_path: input.db_path,
            frame_count: 0,
            new_shadow_sync_offset: input.shadow_sync_offset,
            new_current_txid: input.current_txid,
            new_db_checksum: input.db_checksum,
        });
    }

    // Convert to format expected by encode_wal_changes
    let pages: Vec<(u32, Vec<u8>)> = page_map.into_iter().collect();

    // Get pre_apply_checksum from state
    let pre_checksum = input.db_checksum.map(|cs| Checksum::new(cs));

    // Calculate TXIDs
    let min_txid = input.current_txid + 1;
    let max_txid = min_txid + pages.len() as u64 - 1;
    let commit_page = if max_db_size > 0 {
        max_db_size
    } else {
        // Fallback: estimate from input
        1
    };

    // Encode as incremental LTX (CPU-bound, run in blocking thread pool)
    // Pre-allocate buffer: estimate 2x pages * page_size for compression headroom
    let unique_pages = pages.len();
    let estimated_size = unique_pages.saturating_mul(input.page_size as usize).saturating_mul(2);
    let page_size = input.page_size;

    // Chained page checksum: O(changed pages), no disk read
    let expected_post = if let Some(pre) = pre_checksum {
        ltx::chain_checksum(pre, &pages)
    } else {
        // First sync — no pre_checksum yet, compute from file
        ltx::compute_checksum_from_file(&input.db_path)?
    };

    let (ltx_buffer, post_checksum) = tokio::task::spawn_blocking(move || {
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
        Ok::<_, anyhow::Error>((ltx_buffer, post_checksum))
    }).await??;

    let ltx_size = ltx_buffer.len() as u64;

    // Incrementals go to generation 0 (live folder, litestream format)
    let ltx_key = build_ltx_key(prefix, &input.name, GENERATION_LIVE, min_txid, max_txid);

    s3::upload_bytes(client, bucket, &ltx_key, ltx_buffer).await?;

    tracing::info!(
        "{}: Shadow sync uploaded {} frames ({} bytes, {} unique pages, TXID {}-{}) -> {}",
        input.name,
        frame_count,
        ltx_size,
        unique_pages,
        min_txid,
        max_txid,
        ltx_key
    );

    let new_offset = input.shadow_sync_offset + (frame_count as u64 * frame_size);

    Ok(ShadowSyncOutput {
        db_path: input.db_path,
        frame_count: unique_pages as u64,
        new_shadow_sync_offset: new_offset,
        new_current_txid: max_txid,
        new_db_checksum: Some(post_checksum.into_inner()),
    })
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
