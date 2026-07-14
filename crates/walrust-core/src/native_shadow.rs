//! Native HADBP encoder for fsynced shadow-WAL segments.

use crate::ltx;
use anyhow::{bail, Context, Result};
use std::collections::HashMap;
use std::fs;
use std::io::{Read, Seek, SeekFrom};
use std::path::PathBuf;

#[derive(Debug, Clone)]
pub struct NativeShadowInput {
    pub seq: u64,
    pub previous_chain_checksum: u64,
    pub generation: u64,
    pub shadow_sync_offset: u64,
    pub page_size: u32,
    pub shadow_dir: PathBuf,
}

#[derive(Debug, Clone)]
pub struct NativeShadowOutput {
    pub payload: Vec<u8>,
    pub seq: u64,
    pub previous_chain_checksum: u64,
    pub ending_chain_checksum: u64,
    pub end_page_count: u64,
    pub frame_count: u64,
    pub unique_pages: u64,
    pub new_shadow_sync_offset: u64,
}

/// Encode only the committed prefix after `shadow_sync_offset`. Frames after
/// the last commit marker remain for the next admission.
pub fn encode_shadow_to_hadbp(input: &NativeShadowInput) -> Result<Option<NativeShadowOutput>> {
    let frame_size = 24u64 + input.page_size as u64;
    let mut committed_pages = HashMap::<u32, Vec<u8>>::new();
    let mut transaction_pages = HashMap::<u32, Vec<u8>>::new();
    let mut committed_frames = 0u64;
    let mut transaction_frames = 0u64;
    let mut end_page_count = 0u64;
    let mut logical_offset = 0u64;

    let mut entries = fs::read_dir(&input.shadow_dir)?
        .filter_map(|entry| entry.ok())
        .filter(|entry| entry.file_name().to_string_lossy().ends_with(".wal"))
        .collect::<Vec<_>>();
    entries.sort_by_key(|entry| entry.file_name());

    for entry in entries {
        let file_name = entry.file_name();
        let text = file_name.to_string_lossy();
        let Some((generation, _index)) = text.trim_end_matches(".wal").split_once('-') else {
            continue;
        };
        let Ok(generation) = u64::from_str_radix(generation, 16) else {
            continue;
        };
        if generation != input.generation {
            continue;
        }

        let segment_size = entry.metadata()?.len();
        let segment_start = logical_offset;
        let segment_end = segment_start.saturating_add(segment_size);
        logical_offset = segment_end;
        if segment_end <= input.shadow_sync_offset {
            continue;
        }
        let relative = input.shadow_sync_offset.saturating_sub(segment_start);
        if relative % frame_size != 0 {
            bail!(
                "native shadow cursor {} is not frame-aligned for page size {}",
                input.shadow_sync_offset,
                input.page_size
            );
        }
        let readable = segment_size.saturating_sub(relative);
        if readable % frame_size != 0 {
            bail!("fsynced shadow segment {} has a torn frame", entry.path().display());
        }
        let mut file = fs::File::open(entry.path())?;
        file.seek(SeekFrom::Start(relative))?;
        let mut page = vec![0u8; input.page_size as usize];
        for _ in 0..(readable / frame_size) {
            let mut header = [0u8; 24];
            file.read_exact(&mut header)?;
            let page_number = u32::from_be_bytes(header[0..4].try_into().unwrap());
            let db_size = u32::from_be_bytes(header[4..8].try_into().unwrap());
            if page_number == 0 {
                bail!("native shadow frame contains invalid page number 0");
            }
            file.read_exact(&mut page)?;
            transaction_pages.insert(page_number, page.clone());
            transaction_frames += 1;
            if db_size != 0 {
                end_page_count = db_size as u64;
                committed_pages.extend(transaction_pages.drain());
                committed_frames += transaction_frames;
                transaction_frames = 0;
            }
        }
    }

    if committed_frames == 0 {
        return Ok(None);
    }
    if end_page_count == 0 {
        bail!("committed native shadow batch has no declared end-page count");
    }
    let mut pages = committed_pages.into_iter().collect::<Vec<_>>();
    pages.sort_by_key(|(page, _)| *page);
    let (payload, ending_chain_checksum) = ltx::encode_wal_changes_with_end_page_count(
        &pages,
        input.page_size,
        input.seq,
        input.previous_chain_checksum,
        end_page_count,
    )
    .context("encode native HADBP shadow delta")?;
    Ok(Some(NativeShadowOutput {
        payload,
        seq: input.seq,
        previous_chain_checksum: input.previous_chain_checksum,
        ending_chain_checksum,
        end_page_count,
        frame_count: committed_frames,
        unique_pages: pages.len() as u64,
        new_shadow_sync_offset: input
            .shadow_sync_offset
            .saturating_add(committed_frames.saturating_mul(frame_size)),
    }))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    use tempfile::tempdir;

    fn frame(page: u32, db_size: u32, byte: u8, page_size: usize) -> Vec<u8> {
        let mut out = vec![0u8; 24 + page_size];
        out[0..4].copy_from_slice(&page.to_be_bytes());
        out[4..8].copy_from_slice(&db_size.to_be_bytes());
        out[24..].fill(byte);
        out
    }

    #[test]
    fn emits_native_delta_only_through_last_commit() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("0000000000000000-0000000000000000.wal");
        let mut file = fs::File::create(path).unwrap();
        file.write_all(&frame(1, 0, 1, 512)).unwrap();
        file.write_all(&frame(2, 2, 2, 512)).unwrap();
        file.write_all(&frame(2, 0, 3, 512)).unwrap();
        file.sync_all().unwrap();
        let output = encode_shadow_to_hadbp(&NativeShadowInput {
            seq: 2,
            previous_chain_checksum: 11,
            generation: 0,
            shadow_sync_offset: 0,
            page_size: 512,
            shadow_dir: dir.path().to_path_buf(),
        })
        .unwrap()
        .unwrap();
        assert_eq!(output.frame_count, 2);
        assert_eq!(output.unique_pages, 2);
        assert_eq!(output.end_page_count, 2);
        assert_eq!(output.new_shadow_sync_offset, 2 * (24 + 512));
        let decoded = ltx::decode_sqlite_changeset(&output.payload).unwrap();
        assert_eq!(decoded.header.seq, 2);
        assert_eq!(decoded.header.prev_checksum, 11);
        assert_eq!(ltx::changeset_end_page_count(&decoded).unwrap(), Some(2));
    }
}
