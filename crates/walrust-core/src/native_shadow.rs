//! Native HADBP encoder for fsynced shadow-WAL segments.

use crate::ltx;
use anyhow::{bail, Context, Result};
use std::collections::HashMap;
use std::fs;
use std::io::{Read, Seek, SeekFrom};
use std::path::{Path, PathBuf};

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

#[derive(Debug, Clone)]
pub struct NativeSnapshotInput {
    pub db_path: PathBuf,
    pub seq: u64,
    pub previous_chain_checksum: u64,
    pub generation: u64,
    /// Exact fsynced committed boundary, measured in shadow frame bytes from
    /// the beginning of `generation`.
    pub shadow_end_offset: u64,
    pub page_size: u32,
    pub shadow_dir: PathBuf,
    #[cfg(unix)]
    pub expected_db_file_identity: (u64, u64),
}

#[derive(Debug, Clone)]
pub struct NativeSnapshotOutput {
    pub payload: Vec<u8>,
    pub ending_chain_checksum: u64,
    pub end_page_count: u64,
    pub frame_count: u64,
    pub unique_shadow_pages: u64,
}

#[derive(Debug, Clone)]
pub struct NativeSnapshotFileOutput {
    pub ending_chain_checksum: u64,
    pub end_page_count: u64,
    pub frame_count: u64,
    pub unique_shadow_pages: u64,
    pub payload_length: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NativeSnapshotSourceProof {
    pub ending_chain_checksum: u64,
    pub end_page_count: u64,
    pub page_image_sha256: String,
}

/// Hash the exact main-file + pinned-shadow page image before persisting the
/// snapshot intent. The same retained source descriptor is used by encoding,
/// so this does not open/close the SQLite inode after blocker acquisition.
pub fn snapshot_source_proof(
    input: &NativeSnapshotInput,
    db: &mut fs::File,
) -> Result<NativeSnapshotSourceProof> {
    use sha2::{Digest, Sha256};

    let (shadow_pages, _, end_page_count) = snapshot_source(input)?;
    let mut hasher = Sha256::new();
    for page in 1..=u32::try_from(end_page_count)
        .map_err(|_| anyhow::anyhow!("native snapshot source exceeds U32 page range"))?
    {
        hasher.update(read_resolved_page(input, &shadow_pages, db, page)?);
    }
    let digest: [u8; 32] = hasher.finalize().into();
    let ending_chain_checksum =
        u64::from_be_bytes(digest[0..8].try_into().expect("sha256 is 32 bytes"));
    Ok(NativeSnapshotSourceProof {
        ending_chain_checksum,
        end_page_count,
        page_image_sha256: hex_digest(&digest),
    })
}

fn hex_digest(bytes: &[u8]) -> String {
    let mut out = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        use std::fmt::Write as _;
        let _ = write!(&mut out, "{byte:02x}");
    }
    out
}

/// Resolve a complete SQLite image at one immutable, fsynced shadow commit
/// boundary. This is the Litestream page-selection rule with a native HADBP
/// encoder: the latest shadow frame wins; pages absent from the WAL generation
/// come from the main database file.
pub fn encode_snapshot_from_shadow(input: &NativeSnapshotInput) -> Result<NativeSnapshotOutput> {
    let (shadow_pages, frame_count, end_page_count) = snapshot_source(input)?;
    let mut db = fs::File::open(&input.db_path)
        .with_context(|| format!("open native snapshot database {}", input.db_path.display()))?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        let metadata = db.metadata()?;
        if (metadata.dev(), metadata.ino()) != input.expected_db_file_identity {
            bail!("native snapshot database identity changed before encoding");
        }
    }
    let encoded = ltx::encode_resolved_snapshot_with(
        end_page_count as u32,
        input.page_size,
        input.seq,
        input.previous_chain_checksum,
        |page| read_resolved_page(input, &shadow_pages, &mut db, page),
    )?;
    Ok(NativeSnapshotOutput {
        payload: encoded.bytes,
        ending_chain_checksum: encoded.checksum,
        end_page_count,
        frame_count,
        unique_shadow_pages: shadow_pages.len() as u64,
    })
}

/// Encode directly into the exact spool payload temporary. The caller owns
/// atomic installation and canonical decode, but a successful return guarantees
/// the complete native-format file has been fsynced.
pub fn write_snapshot_from_shadow(
    input: &NativeSnapshotInput,
    output_path: &Path,
) -> Result<NativeSnapshotFileOutput> {
    let mut db = fs::File::open(&input.db_path)
        .with_context(|| format!("open native snapshot database {}", input.db_path.display()))?;
    write_snapshot_from_shadow_file(input, &mut db, output_path)
}

/// Encode a native snapshot using an already-open source database descriptor.
///
/// Watchers on platforms without open-file-description locks must keep this
/// descriptor open for the entire SQLite blocker lifetime. Opening and closing
/// another descriptor for the database inode after blocker acquisition can
/// release the process's SQLite locks even though the blocker transaction is
/// still represented by a live connection.
pub fn write_snapshot_from_shadow_file(
    input: &NativeSnapshotInput,
    db: &mut fs::File,
    output_path: &Path,
) -> Result<NativeSnapshotFileOutput> {
    let (shadow_pages, frame_count, end_page_count) = snapshot_source(input)?;
    #[cfg(unix)]
    let db_identity = {
        use std::os::unix::fs::MetadataExt;
        let metadata = db.metadata()?;
        (metadata.dev(), metadata.ino())
    };
    #[cfg(unix)]
    if db_identity != input.expected_db_file_identity {
        bail!("native snapshot database identity changed before payload creation");
    }
    let mut output = fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(output_path)
        .with_context(|| {
            format!(
                "create native HADBP snapshot temp {}",
                output_path.display()
            )
        })?;
    let ending_chain_checksum = ltx::write_resolved_snapshot_with(
        &mut output,
        end_page_count as u32,
        input.page_size,
        input.seq,
        input.previous_chain_checksum,
        |page| read_resolved_page(input, &shadow_pages, db, page),
    )?;
    output.sync_all()?;
    let output_dir = output_path
        .parent()
        .ok_or_else(|| anyhow::anyhow!("native snapshot temporary has no parent directory"))?;
    fs::File::open(output_dir)
        .with_context(|| {
            format!(
                "open native snapshot temp directory {}",
                output_dir.display()
            )
        })?
        .sync_all()
        .with_context(|| {
            format!(
                "fsync native snapshot temp directory {}",
                output_dir.display()
            )
        })?;
    crate::native_spool::durability_failpoint("snapshot_payload_temp_fsynced");
    let payload_length = output.metadata()?.len();
    drop(output);
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        let path_metadata = fs::metadata(&input.db_path)?;
        if (path_metadata.dev(), path_metadata.ino()) != db_identity {
            bail!(
                "native snapshot database path was replaced while encoding {}; refusing mixed source identity",
                input.db_path.display()
            );
        }
    }
    Ok(NativeSnapshotFileOutput {
        ending_chain_checksum,
        end_page_count,
        frame_count,
        unique_shadow_pages: shadow_pages.len() as u64,
        payload_length,
    })
}

fn snapshot_source(input: &NativeSnapshotInput) -> Result<(HashMap<u32, Vec<u8>>, u64, u64)> {
    let frame_size = 24u64 + input.page_size as u64;
    if input.shadow_end_offset % frame_size != 0 {
        bail!(
            "native snapshot shadow cursor {} is not frame-aligned for page size {}",
            input.shadow_end_offset,
            input.page_size
        );
    }

    let (shadow_pages, frame_count, end_page_count) = read_shadow_prefix(
        &input.shadow_dir,
        input.generation,
        input.shadow_end_offset,
        input.page_size,
    )?;
    let end_page_count = match end_page_count {
        Some(value) => value,
        None if input.shadow_end_offset == 0 => {
            let len = fs::metadata(&input.db_path)?.len();
            if len == 0 || !len.is_multiple_of(input.page_size as u64) {
                bail!(
                    "main SQLite database length {} is invalid for page size {}",
                    len,
                    input.page_size
                );
            }
            len / input.page_size as u64
        }
        None => bail!(
            "native snapshot shadow boundary {} does not end at a commit marker",
            input.shadow_end_offset
        ),
    };
    if end_page_count == 0 || end_page_count > u32::MAX as u64 {
        bail!("native snapshot has invalid end-page count {end_page_count}");
    }
    Ok((shadow_pages, frame_count, end_page_count))
}

fn read_resolved_page(
    input: &NativeSnapshotInput,
    shadow_pages: &HashMap<u32, Vec<u8>>,
    db: &mut fs::File,
    page: u32,
) -> Result<Vec<u8>> {
    if let Some(data) = shadow_pages.get(&page) {
        return Ok(data.clone());
    }
    let offset = u64::from(page - 1).saturating_mul(input.page_size as u64);
    db.seek(SeekFrom::Start(offset))?;
    let mut data = vec![0u8; input.page_size as usize];
    db.read_exact(&mut data).with_context(|| {
        format!(
            "read native snapshot base page {} at offset {} from {}",
            page,
            offset,
            input.db_path.display()
        )
    })?;
    Ok(data)
}

fn read_shadow_prefix(
    shadow_dir: &Path,
    wanted_generation: u64,
    end_offset: u64,
    page_size: u32,
) -> Result<(HashMap<u32, Vec<u8>>, u64, Option<u64>)> {
    let frame_size = 24u64 + page_size as u64;
    let mut pages = HashMap::new();
    let mut logical_offset = 0u64;
    let mut final_db_size = None;
    let mut last_db_size = None;
    let mut frame_count = 0u64;
    let mut entries = fs::read_dir(shadow_dir)?
        .filter_map(|entry| entry.ok())
        .filter(|entry| entry.file_name().to_string_lossy().ends_with(".wal"))
        .collect::<Vec<_>>();
    entries.sort_by_key(|entry| entry.file_name());

    for entry in entries {
        let text = entry.file_name().to_string_lossy().into_owned();
        let Some((generation, _)) = text.trim_end_matches(".wal").split_once('-') else {
            continue;
        };
        let Ok(generation) = u64::from_str_radix(generation, 16) else {
            continue;
        };
        if generation != wanted_generation || logical_offset >= end_offset {
            continue;
        }
        let available = entry.metadata()?.len();
        let take = available.min(end_offset.saturating_sub(logical_offset));
        if take % frame_size != 0 {
            bail!(
                "native snapshot shadow prefix crosses a torn frame in {}",
                entry.path().display()
            );
        }
        let mut file = fs::File::open(entry.path())?;
        for _ in 0..take / frame_size {
            let mut header = [0u8; 24];
            file.read_exact(&mut header)?;
            let page = u32::from_be_bytes(header[0..4].try_into().unwrap());
            let db_size = u32::from_be_bytes(header[4..8].try_into().unwrap());
            if page == 0 {
                bail!("native snapshot shadow contains invalid page number 0");
            }
            let mut data = vec![0u8; page_size as usize];
            file.read_exact(&mut data)?;
            pages.insert(page, data);
            frame_count += 1;
            final_db_size = (db_size != 0).then_some(db_size as u64).or(final_db_size);
            last_db_size = Some(db_size);
        }
        logical_offset = logical_offset.saturating_add(available);
    }
    if end_offset != 0 && frame_count.saturating_mul(frame_size) != end_offset {
        bail!(
            "native snapshot cursor {} exceeds readable generation {} bytes {}",
            end_offset,
            wanted_generation,
            frame_count.saturating_mul(frame_size)
        );
    }
    if end_offset != 0 && last_db_size == Some(0) {
        bail!("native snapshot shadow boundary ends inside a transaction");
    }
    Ok((pages, frame_count, final_db_size))
}

/// Select the last transaction boundary at or before a durable shadow tail.
/// Snapshot timers may fire while SQLite has spilled an uncommitted
/// transaction into WAL; those frames stay in the shadow for the next delta
/// but must not make snapshot creation fail or leak uncommitted pages.
pub fn committed_shadow_prefix_offset(
    shadow_dir: &Path,
    wanted_generation: u64,
    durable_end_offset: u64,
    page_size: u32,
) -> Result<u64> {
    let frame_size = 24u64 + page_size as u64;
    if !durable_end_offset.is_multiple_of(frame_size) {
        bail!("durable native shadow tail is not frame-aligned");
    }
    let mut entries = fs::read_dir(shadow_dir)?
        .filter_map(|entry| entry.ok())
        .filter(|entry| entry.file_name().to_string_lossy().ends_with(".wal"))
        .collect::<Vec<_>>();
    entries.sort_by_key(|entry| entry.file_name());
    let mut logical_offset = 0u64;
    let mut last_commit = 0u64;
    for entry in entries {
        let text = entry.file_name().to_string_lossy().into_owned();
        let Some((generation, _)) = text.trim_end_matches(".wal").split_once('-') else {
            continue;
        };
        if u64::from_str_radix(generation, 16).ok() != Some(wanted_generation)
            || logical_offset >= durable_end_offset
        {
            continue;
        }
        let available = entry.metadata()?.len();
        let take = available.min(durable_end_offset.saturating_sub(logical_offset));
        if !take.is_multiple_of(frame_size) {
            bail!(
                "durable native shadow prefix crosses a torn frame in {}",
                entry.path().display()
            );
        }
        let mut file = fs::File::open(entry.path())?;
        for _ in 0..take / frame_size {
            let mut header = [0u8; 24];
            file.read_exact(&mut header)?;
            let page = u32::from_be_bytes(header[0..4].try_into().unwrap());
            if page == 0 {
                bail!("native shadow frame contains invalid page number 0");
            }
            let db_size = u32::from_be_bytes(header[4..8].try_into().unwrap());
            file.seek(SeekFrom::Current(i64::from(page_size)))?;
            logical_offset = logical_offset.saturating_add(frame_size);
            if db_size != 0 {
                last_commit = logical_offset;
            }
        }
    }
    if logical_offset != durable_end_offset {
        bail!(
            "durable native shadow cursor {} exceeds readable generation {} bytes {}",
            durable_end_offset,
            wanted_generation,
            logical_offset
        );
    }
    Ok(last_commit)
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
            bail!(
                "fsynced shadow segment {} has a torn frame",
                entry.path().display()
            );
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

    #[test]
    fn snapshot_boundary_selects_last_commit_before_uncommitted_tail() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("0000000000000000-0000000000000000.wal");
        let mut file = fs::File::create(path).unwrap();
        file.write_all(&frame(1, 2, 1, 512)).unwrap();
        file.write_all(&frame(2, 0, 2, 512)).unwrap();
        file.write_all(&frame(3, 0, 3, 512)).unwrap();
        file.sync_all().unwrap();
        let frame_size = (24 + 512) as u64;
        assert_eq!(
            committed_shadow_prefix_offset(dir.path(), 0, frame_size * 3, 512).unwrap(),
            frame_size
        );

        file.write_all(&frame(4, 4, 4, 512)).unwrap();
        file.sync_all().unwrap();
        let delta = encode_shadow_to_hadbp(&NativeShadowInput {
            seq: 2,
            previous_chain_checksum: 11,
            generation: 0,
            shadow_sync_offset: frame_size,
            page_size: 512,
            shadow_dir: dir.path().to_path_buf(),
        })
        .unwrap()
        .expect("committed successor must emit the formerly in-flight tail");
        assert_eq!(delta.frame_count, 3);
        assert_eq!(delta.new_shadow_sync_offset, frame_size * 4);
        let decoded = ltx::decode_sqlite_changeset(&delta.payload).unwrap();
        assert_eq!(ltx::changeset_end_page_count(&decoded).unwrap(), Some(4));
        assert_eq!(
            decoded
                .pages
                .iter()
                .filter(|page| page.page_id.to_u64() <= 4)
                .count(),
            3
        );
    }

    #[test]
    fn snapshot_resolves_shadow_over_main_at_frozen_commit_boundary() {
        let dir = tempdir().unwrap();
        let db = dir.path().join("db.sqlite");
        let shadow = dir.path().join("shadow");
        fs::create_dir(&shadow).unwrap();
        let page_size = 512usize;
        let mut base = Vec::new();
        for byte in [1u8, 2, 3] {
            base.extend(vec![byte; page_size]);
        }
        fs::write(&db, &base).unwrap();
        let segment = shadow.join("0000000000000007-0000000000000000.wal");
        let mut file = fs::File::create(segment).unwrap();
        file.write_all(&frame(2, 0, 20, page_size)).unwrap();
        file.write_all(&frame(4, 4, 40, page_size)).unwrap();
        // A later committed transaction exists durably but is outside the
        // frozen cursor and must not leak into the snapshot/PITR boundary.
        file.write_all(&frame(1, 4, 99, page_size)).unwrap();
        file.sync_all().unwrap();

        let boundary = 2 * (24 + page_size) as u64;
        let output = encode_snapshot_from_shadow(&NativeSnapshotInput {
            db_path: db,
            seq: 9,
            previous_chain_checksum: 17,
            generation: 7,
            shadow_end_offset: boundary,
            page_size: page_size as u32,
            shadow_dir: shadow,
            #[cfg(unix)]
            expected_db_file_identity: {
                use std::os::unix::fs::MetadataExt;
                let metadata = fs::metadata(dir.path().join("db.sqlite")).unwrap();
                (metadata.dev(), metadata.ino())
            },
        })
        .unwrap();
        assert_eq!(output.end_page_count, 4);
        assert_eq!(output.frame_count, 2);
        let restored = dir.path().join("restored.sqlite");
        let decoded = ltx::decode_to_db(&output.payload, &restored).unwrap();
        assert_eq!(decoded.header.seq, 9);
        assert_eq!(decoded.header.prev_checksum, 17);
        let bytes = fs::read(restored).unwrap();
        assert_eq!(&bytes[0..page_size], vec![1u8; page_size]);
        assert_eq!(&bytes[page_size..2 * page_size], vec![20u8; page_size]);
        assert_eq!(&bytes[2 * page_size..3 * page_size], vec![3u8; page_size]);
        assert_eq!(&bytes[3 * page_size..4 * page_size], vec![40u8; page_size]);
    }

    #[test]
    fn snapshot_uses_commit_size_and_rejects_mid_transaction_cursor() {
        let dir = tempdir().unwrap();
        let db = dir.path().join("db.sqlite");
        let shadow = dir.path().join("shadow");
        fs::create_dir(&shadow).unwrap();
        fs::write(&db, vec![1u8; 4 * 512]).unwrap();
        let segment = shadow.join("0000000000000000-0000000000000000.wal");
        let mut file = fs::File::create(segment).unwrap();
        file.write_all(&frame(1, 2, 7, 512)).unwrap();
        file.write_all(&frame(2, 0, 8, 512)).unwrap();
        file.sync_all().unwrap();
        let base = NativeSnapshotInput {
            db_path: db,
            seq: 1,
            previous_chain_checksum: 0,
            generation: 0,
            shadow_end_offset: (24 + 512) as u64,
            page_size: 512,
            shadow_dir: shadow,
            #[cfg(unix)]
            expected_db_file_identity: {
                use std::os::unix::fs::MetadataExt;
                let metadata = fs::metadata(dir.path().join("db.sqlite")).unwrap();
                (metadata.dev(), metadata.ino())
            },
        };
        let output = encode_snapshot_from_shadow(&base).unwrap();
        assert_eq!(output.end_page_count, 2);
        let mut mid_transaction = base;
        mid_transaction.shadow_end_offset *= 2;
        assert!(encode_snapshot_from_shadow(&mid_transaction)
            .unwrap_err()
            .to_string()
            .contains("inside a transaction"));
    }

    #[test]
    fn direct_snapshot_supports_every_sqlite_page_size() {
        for page_size in [512u32, 1024, 2048, 4096, 8192, 16384, 32768, 65536] {
            let dir = tempdir().unwrap();
            let db = dir.path().join("db.sqlite");
            let shadow = dir.path().join("shadow");
            fs::create_dir(&shadow).unwrap();
            let mut expected = vec![0x31; page_size as usize];
            expected.extend(vec![0x62; page_size as usize]);
            fs::write(&db, &expected).unwrap();
            let output = encode_snapshot_from_shadow(&NativeSnapshotInput {
                db_path: db.clone(),
                seq: 1,
                previous_chain_checksum: 0,
                generation: 0,
                shadow_end_offset: 0,
                page_size,
                shadow_dir: shadow,
                #[cfg(unix)]
                expected_db_file_identity: {
                    use std::os::unix::fs::MetadataExt;
                    let metadata = fs::metadata(&db).unwrap();
                    (metadata.dev(), metadata.ino())
                },
            })
            .unwrap();
            let restored = dir.path().join("restored.sqlite");
            ltx::decode_to_db(&output.payload, &restored).unwrap();
            assert_eq!(fs::read(restored).unwrap(), expected);
        }
    }
}
