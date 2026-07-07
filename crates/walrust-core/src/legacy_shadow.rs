//! Legacy shadow-WAL to LTX sync engine.

use crate::legacy_cache::LocalCache;
use crate::legacy_ltx::{self as ltx, Checksum};
use crate::legacy_manifest::{build_ltx_key, GENERATION_LIVE};
use crate::legacy_uploader::UploadMessage;
use anyhow::Result;
use hadb_storage::StorageBackend;
use std::collections::HashMap;
use std::path::PathBuf;

/// Input for legacy shadow WAL sync.
#[derive(Clone)]
pub struct ShadowSyncInput {
    pub db_path: PathBuf,
    pub name: String,
    pub current_txid: u64,
    pub db_checksum: Option<u64>,
    pub generation: u64,
    pub shadow_sync_offset: u64,
    pub page_size: u32,
    pub shadow_dir: PathBuf,
}

/// Output from legacy shadow WAL sync.
#[derive(Debug)]
pub struct ShadowSyncOutput {
    pub db_path: PathBuf,
    pub frame_count: u64,
    pub new_shadow_sync_offset: u64,
    pub new_current_txid: u64,
    pub new_db_checksum: Option<u64>,
}

/// Result of encoding shadow WAL segments into LTX.
pub struct ShadowEncodeResult {
    pub ltx_buffer: Vec<u8>,
    pub post_checksum: Checksum,
    pub frame_count: usize,
    pub unique_pages: usize,
    pub min_txid: u64,
    pub max_txid: u64,
}

/// Read shadow WAL segments and encode into an LTX buffer.
///
/// Returns `Ok(None)` if no committed frames are available to sync.
pub fn encode_shadow_to_ltx(input: &ShadowSyncInput) -> Result<Option<(ShadowEncodeResult, u64)>> {
    let shadow_dir = &input.shadow_dir;
    let mut page_map: HashMap<u32, Vec<u8>> = HashMap::new();
    let mut pending_page_map: HashMap<u32, Vec<u8>> = HashMap::new();
    let mut final_db_size = 0u32;
    let mut frame_count = 0usize;
    let mut pending_frame_count = 0usize;
    let mut committed_frame_count = 0usize;
    let mut total_offset = 0u64;
    let frame_size = 24u64 + input.page_size as u64;

    let mut entries: Vec<_> = std::fs::read_dir(shadow_dir)?
        .filter_map(|entry| entry.ok())
        .filter(|entry| entry.file_name().to_string_lossy().ends_with(".wal"))
        .collect();
    entries.sort_by_key(|entry| entry.file_name());

    for entry in entries {
        let name = entry.file_name();
        let name_str = name.to_string_lossy();

        let parts: Vec<&str> = name_str.trim_end_matches(".wal").split('-').collect();
        if parts.len() != 2 {
            continue;
        }

        let generation = u64::from_str_radix(parts[0], 16).unwrap_or(u64::MAX);
        if generation != input.generation {
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

        let relative_offset = input.shadow_sync_offset.saturating_sub(segment_start);
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

            pending_page_map.insert(page_number, page_data.clone());
            pending_frame_count += 1;

            if db_size > 0 {
                final_db_size = db_size;
                page_map.extend(pending_page_map.drain());
                frame_count += pending_frame_count;
                committed_frame_count += pending_frame_count;
                pending_frame_count = 0;
            }
        }

        total_offset = segment_end;
    }

    if page_map.is_empty() {
        return Ok(None);
    }

    let pages: Vec<(u32, Vec<u8>)> = page_map.into_iter().collect();
    let pre_checksum = input.db_checksum.map(Checksum::new);

    let min_txid = input.current_txid + 1;
    let max_txid = min_txid + pages.len() as u64 - 1;
    let commit_page = if final_db_size > 0 { final_db_size } else { 1 };

    let unique_pages = pages.len();
    let estimated_size = unique_pages
        .saturating_mul(input.page_size as usize)
        .saturating_mul(2);
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

    let new_offset = input.shadow_sync_offset + (committed_frame_count as u64 * frame_size);

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

pub fn build_shadow_output(
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

pub fn build_empty_shadow_output(input: &ShadowSyncInput) -> ShadowSyncOutput {
    ShadowSyncOutput {
        db_path: input.db_path.clone(),
        frame_count: 0,
        new_shadow_sync_offset: input.shadow_sync_offset,
        new_current_txid: input.current_txid,
        new_db_checksum: input.db_checksum,
    }
}

/// Sync legacy shadow WAL segments to object storage.
pub async fn sync_shadow_to_storage(
    storage: &dyn StorageBackend,
    prefix: &str,
    input: ShadowSyncInput,
) -> Result<ShadowSyncOutput> {
    let input_clone = input.clone();
    let result = tokio::task::spawn_blocking(move || encode_shadow_to_ltx(&input_clone)).await??;

    let (encoded, new_offset) = match result {
        Some(result) => result,
        None => return Ok(build_empty_shadow_output(&input)),
    };

    let ltx_key = build_ltx_key(
        prefix,
        &input.name,
        GENERATION_LIVE,
        encoded.min_txid,
        encoded.max_txid,
    );
    let ltx_size = encoded.ltx_buffer.len() as u64;
    let output = build_shadow_output(&input, &encoded, new_offset);

    storage.put(&ltx_key, &encoded.ltx_buffer).await?;

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

/// Sync legacy shadow WAL segments to disk cache and notify the uploader.
pub async fn sync_shadow_to_cache(
    cache: &LocalCache,
    upload_tx: &tokio::sync::mpsc::Sender<UploadMessage>,
    input: ShadowSyncInput,
) -> Result<ShadowSyncOutput> {
    let input_clone = input.clone();
    let result = tokio::task::spawn_blocking(move || encode_shadow_to_ltx(&input_clone)).await??;

    let (encoded, new_offset) = match result {
        Some(result) => result,
        None => return Ok(build_empty_shadow_output(&input)),
    };

    let ltx_size = encoded.ltx_buffer.len() as u64;

    cache.write_ltx(encoded.max_txid, &encoded.ltx_buffer)?;

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

    Ok(build_shadow_output(&input, &encoded, new_offset))
}
