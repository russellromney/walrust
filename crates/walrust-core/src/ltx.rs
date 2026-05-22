//! HADBP (hadb-changeset physical) format support for SQLite WAL replication.
//!
//! This module provides utilities for encoding and decoding HADBP changesets
//! containing SQLite pages. It wraps hadb-changeset with SQLite-specific
//! defaults (U32 page IDs, 4KB pages).
//!
//! Replaces the former litepages/LTX format. The on-disk format is .hadbp,
//! but all replication semantics (WAL parsing, checksum chaining, snapshot +
//! incremental restore) remain identical.

use anyhow::{anyhow, Result};
use hadb_changeset::physical::{
    self, PageEntry, PageId, PageIdSize, PhysicalChangeset, PhysicalHeader,
};
use std::io::Read;
use std::path::Path;

/// SQLite page ID size (u32).
pub const SQLITE_PAGE_ID_SIZE: PageIdSize = PageIdSize::U32;

// Re-export hadb-changeset types used by sync.rs and consumers.
pub use hadb_changeset::physical::{
    compute_checksum, decode, encode, verify_chain, PageEntry as HadbPageEntry,
    PageId as HadbPageId, PageIdSize as HadbPageIdSize, PhysicalChangeset as HadbChangeset,
    PhysicalHeader as HadbHeader,
};

/// Create an HADBP changeset from a SQLite database snapshot.
///
/// Reads the DB page-by-page. Peak memory is ~1MB (BufReader) + one page buffer
/// + the page entries vec (all pages must be in memory for HADBP encoding).
pub fn encode_snapshot(
    db_path: &Path,
    page_size: u32,
    seq: u64,
    prev_checksum: u64,
) -> Result<Vec<u8>> {
    use std::io::BufReader;

    let file = std::fs::File::open(db_path)
        .map_err(|e| anyhow!("Failed to open database for snapshot: {}", e))?;
    let file_size = file.metadata()?.len() as usize;
    let num_pages = file_size / page_size as usize;

    let mut reader = BufReader::with_capacity(1024 * 1024, file);
    let mut pages = Vec::with_capacity(num_pages);

    for i in 0..num_pages {
        let mut page_buf = vec![0u8; page_size as usize];
        reader.read_exact(&mut page_buf)?;
        // SQLite uses 1-based page numbers
        pages.push(PageEntry {
            page_id: PageId::U32((i + 1) as u32),
            data: page_buf,
        });
    }

    let changeset =
        PhysicalChangeset::new(seq, prev_checksum, SQLITE_PAGE_ID_SIZE, page_size, pages);
    Ok(physical::encode(&changeset))
}

/// Result of decoding an HADBP snapshot, including the post-apply checksum for chain verification.
#[derive(Debug)]
pub struct DecodeResult {
    pub header: PhysicalHeader,
    pub checksum: u64,
}

/// Decode an HADBP changeset and reconstruct the database (full write).
///
/// Returns the header and checksum for chain tracking.
pub fn decode_to_db(data: &[u8], output_path: &Path) -> Result<DecodeResult> {
    let changeset =
        physical::decode(data).map_err(|e| anyhow!("Failed to decode HADBP changeset: {}", e))?;

    let page_size = changeset.header.page_size as usize;

    // Find the maximum page number to determine DB size
    let max_page = changeset
        .pages
        .iter()
        .map(|p| p.page_id.to_u64())
        .max()
        .unwrap_or(0);
    // Bound the allocation against an untrusted/corrupt max page id.
    let db_size = (max_page as usize).checked_mul(page_size).ok_or_else(|| {
        anyhow!("changeset max_page * page_size overflows usize (corrupt changeset)")
    })?;

    let mut db_data = vec![0u8; db_size];

    for page in &changeset.pages {
        let page_num = page.page_id.to_u64();
        // SQLite uses 1-based page numbers: page 1 is at offset 0. A
        // page id of 0 is invalid; reject rather than underflow. A page
        // beyond the image is corruption — reject rather than silently
        // drop it (which would produce a wrong byte image).
        if page_num < 1 {
            return Err(anyhow!("changeset contains invalid page number 0"));
        }
        let idx = (page_num - 1) as usize;
        let start = idx * page_size;
        let end = start + page.data.len();
        if end > db_data.len() {
            return Err(anyhow!(
                "changeset page {page_num} (len {}) exceeds db image size {}; \
                 refusing to decode (corrupt changeset)",
                page.data.len(),
                db_data.len()
            ));
        }
        db_data[start..end].copy_from_slice(&page.data);
    }

    std::fs::write(output_path, &db_data)?;

    // Verify actual checksum matches what was computed
    let actual_checksum = compute_db_checksum_raw(&db_data);
    tracing::debug!(
        "Decoded snapshot (seq {}, checksum: {:016x})",
        changeset.header.seq,
        actual_checksum
    );

    Ok(DecodeResult {
        header: changeset.header,
        checksum: actual_checksum,
    })
}

/// Result of applying an HADBP changeset to an existing database.
#[derive(Debug)]
pub struct ApplyResult {
    pub header: PhysicalHeader,
    pub checksum: u64,
}

/// Apply an incremental HADBP changeset to an existing database (in-place page writes).
///
/// Verifies the checksum chain before writing anything (fail-fast via hadb-changeset).
///
/// Returns the header and checksum for chain tracking.
pub fn apply_changeset_to_db(
    data: &[u8],
    db_path: &Path,
    expected_prev_checksum: u64,
) -> Result<ApplyResult> {
    let changeset =
        physical::decode(data).map_err(|e| anyhow!("Failed to decode HADBP changeset: {}", e))?;

    // Verify checksum chain before writing
    physical::verify_chain(expected_prev_checksum, &changeset)
        .map_err(|e| anyhow!("Checksum chain broken: {}", e))?;

    // Apply pages using hadb-changeset's apply_physical
    // But we need SQLite 1-based page offset: page N is at offset (N-1) * page_size
    use std::fs::OpenOptions;
    use std::io::{Seek, SeekFrom, Write as IoWrite};

    let page_size = changeset.header.page_size as u64;

    let mut file = OpenOptions::new()
        .write(true)
        .open(db_path)
        .map_err(|e| anyhow!("Failed to open database for in-place apply: {}", e))?;

    for page in &changeset.pages {
        // SQLite 1-based: page_id 1 is at offset 0
        let offset = (page.page_id.to_u64() - 1) * page_size;
        file.seek(SeekFrom::Start(offset))?;
        file.write_all(&page.data)?;
    }

    file.sync_all()?;

    tracing::debug!(
        "Applied {} pages in-place (seq {})",
        changeset.header.page_count,
        changeset.header.seq,
    );

    Ok(ApplyResult {
        header: changeset.header,
        checksum: changeset.checksum,
    })
}

/// Encode WAL changes as an HADBP changeset (incremental, not snapshot).
///
/// Converts (page_num, page_data) pairs to PageEntry with U32 IDs,
/// creates a PhysicalChangeset, and encodes to bytes.
///
/// Returns the encoded bytes and the changeset checksum.
pub fn encode_wal_changes(
    pages: &[(u32, Vec<u8>)],
    page_size: u32,
    seq: u64,
    prev_checksum: u64,
) -> Result<(Vec<u8>, u64)> {
    let entries: Vec<PageEntry> = pages
        .iter()
        .map(|(page_num, data)| PageEntry {
            page_id: PageId::U32(*page_num),
            data: data.clone(),
        })
        .collect();

    let changeset =
        PhysicalChangeset::new(seq, prev_checksum, SQLITE_PAGE_ID_SIZE, page_size, entries);
    let checksum = changeset.checksum;
    let encoded = physical::encode(&changeset);
    Ok((encoded, checksum))
}

/// Chained page checksum using HADBP format.
///
/// Computes the checksum for a set of pages given a previous checksum.
/// Pages are sorted by page number for determinism.
///
/// Note: HADBP checksum includes data_len in the hash, unlike the old LTX format.
/// Checksums are NOT compatible between formats.
pub fn chain_checksum(prev: u64, pages: &[(u32, Vec<u8>)]) -> u64 {
    let entries: Vec<PageEntry> = pages
        .iter()
        .map(|(page_num, data)| PageEntry {
            page_id: PageId::U32(*page_num),
            data: data.clone(),
        })
        .collect();

    compute_checksum(prev, SQLITE_PAGE_ID_SIZE, &entries)
}

/// Compute checksum from database file (streaming, no full-DB read).
///
/// Returns a raw u64 (SHA256 truncated to 8 bytes). This is a full-DB hash
/// used for snapshots, NOT the HADBP chain checksum.
pub fn compute_checksum_from_file(db_path: &Path) -> Result<u64> {
    use sha2::{Digest, Sha256};
    use std::io::BufReader;

    let file = std::fs::File::open(db_path)
        .map_err(|e| anyhow!("Failed to open database for checksum: {}", e))?;
    let mut reader = BufReader::with_capacity(1024 * 1024, file);
    let mut hasher = Sha256::new();
    let mut buf = [0u8; 65536];

    loop {
        let n = reader.read(&mut buf)?;
        if n == 0 {
            break;
        }
        hasher.update(&buf[..n]);
    }

    let result = hasher.finalize();
    Ok(u64::from_be_bytes(
        result[0..8].try_into().expect("sha256 is 32 bytes"),
    ))
}

/// Compute database checksum from raw bytes (single u64 from SHA256).
pub fn compute_db_checksum_raw(data: &[u8]) -> u64 {
    use sha2::{Digest, Sha256};
    let mut hasher = Sha256::new();
    hasher.update(data);
    let result = hasher.finalize();
    u64::from_be_bytes(result[0..8].try_into().expect("sha256 is 32 bytes"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn test_snapshot_roundtrip_single_page() {
        let dir = tempdir().unwrap();
        let db_path = dir.path().join("test.db");
        let restored_path = dir.path().join("restored.db");

        let page_size = 4096u32;
        let db_data = vec![0x42u8; page_size as usize];
        std::fs::write(&db_path, &db_data).unwrap();

        let encoded = encode_snapshot(&db_path, page_size, 1, 0).unwrap();

        let result = decode_to_db(&encoded, &restored_path).unwrap();

        let restored_data = std::fs::read(&restored_path).unwrap();
        assert_eq!(db_data, restored_data);
        assert_eq!(result.header.page_size, page_size);
        assert_eq!(result.header.seq, 1);
    }

    #[test]
    fn test_snapshot_roundtrip_multiple_pages() {
        let dir = tempdir().unwrap();
        let db_path = dir.path().join("test.db");
        let restored_path = dir.path().join("restored.db");

        let page_size = 4096u32;
        let num_pages = 10;

        let mut db_data = Vec::new();
        for i in 0..num_pages {
            let mut page = vec![(i as u8).wrapping_mul(17); page_size as usize];
            page[0..4].copy_from_slice(&(i as u32).to_be_bytes());
            db_data.extend(page);
        }
        std::fs::write(&db_path, &db_data).unwrap();

        let encoded = encode_snapshot(&db_path, page_size, 100, 0).unwrap();
        let result = decode_to_db(&encoded, &restored_path).unwrap();

        let restored_data = std::fs::read(&restored_path).unwrap();
        assert_eq!(db_data.len(), restored_data.len());
        assert_eq!(db_data, restored_data);
        assert_eq!(result.header.page_count, num_pages as u32);
        assert_eq!(result.header.seq, 100);
    }

    #[test]
    fn test_snapshot_various_page_sizes() {
        let dir = tempdir().unwrap();

        for page_size in [512u32, 1024, 2048, 4096, 8192, 16384, 32768] {
            let db_path = dir.path().join(format!("test_{}.db", page_size));
            let restored_path = dir.path().join(format!("restored_{}.db", page_size));

            let db_data: Vec<u8> = (0..3)
                .flat_map(|i| vec![(i * 50) as u8; page_size as usize])
                .collect();
            std::fs::write(&db_path, &db_data).unwrap();

            let encoded = encode_snapshot(&db_path, page_size, 1, 0).unwrap();
            let result = decode_to_db(&encoded, &restored_path).unwrap();

            let restored_data = std::fs::read(&restored_path).unwrap();
            assert_eq!(
                db_data, restored_data,
                "Mismatch for page_size={}",
                page_size
            );
            assert_eq!(result.header.page_size, page_size);
        }
    }

    #[test]
    fn test_snapshot_preserves_binary_data() {
        let dir = tempdir().unwrap();
        let db_path = dir.path().join("binary.db");
        let restored_path = dir.path().join("restored.db");

        let page_size = 4096u32;

        let mut db_data = Vec::new();
        for page_num in 0..4 {
            let mut page = vec![0u8; page_size as usize];
            for (i, byte) in page.iter_mut().enumerate() {
                *byte = ((page_num * 256 + i) % 256) as u8;
            }
            db_data.extend(page);
        }
        std::fs::write(&db_path, &db_data).unwrap();

        let encoded = encode_snapshot(&db_path, page_size, 50, 0).unwrap();
        decode_to_db(&encoded, &restored_path).unwrap();

        let restored_data = std::fs::read(&restored_path).unwrap();

        for (i, (orig, rest)) in db_data.iter().zip(restored_data.iter()).enumerate() {
            assert_eq!(
                orig, rest,
                "Byte mismatch at offset {}: expected 0x{:02x}, got 0x{:02x}",
                i, orig, rest
            );
        }
    }

    #[test]
    fn test_incremental_encoding() {
        let page_size = 4096u32;

        let pages: Vec<(u32, Vec<u8>)> = vec![
            (1, vec![0xAA; page_size as usize]),
            (2, vec![0xBB; page_size as usize]),
            (3, vec![0xCC; page_size as usize]),
        ];

        let prev_checksum = 0x123456789ABCDEF0u64;

        let (encoded, checksum) = encode_wal_changes(&pages, page_size, 1, prev_checksum).unwrap();

        assert!(!encoded.is_empty());
        assert!(encoded.len() > 100);
        assert_ne!(checksum, 0);

        // Verify roundtrip decode
        let decoded = physical::decode(&encoded).unwrap();
        assert_eq!(decoded.header.page_count, 3);
        assert_eq!(decoded.checksum, checksum);
    }

    #[test]
    fn test_txid_ranges() {
        let dir = tempdir().unwrap();
        let db_path = dir.path().join("test.db");
        let restored_path = dir.path().join("restored.db");

        let page_size = 4096u32;
        let db_data = vec![0x42u8; page_size as usize];
        std::fs::write(&db_path, &db_data).unwrap();

        for seq in [1u64, 100, 1000, 999999, u32::MAX as u64] {
            let encoded = encode_snapshot(&db_path, page_size, seq, 0).unwrap();
            let result = decode_to_db(&encoded, &restored_path).unwrap();
            assert_eq!(result.header.seq, seq);
        }
    }

    #[test]
    fn test_checksum_computation() {
        let data1 = b"hello world";
        let data2 = b"hello world";
        let data3 = b"hello worlD";

        let cs1 = compute_db_checksum_raw(data1);
        let cs2 = compute_db_checksum_raw(data2);
        let cs3 = compute_db_checksum_raw(data3);

        assert_eq!(cs1, cs2);
        assert_ne!(cs1, cs3);
    }

    #[test]
    fn test_large_database() {
        let dir = tempdir().unwrap();
        let db_path = dir.path().join("large.db");
        let restored_path = dir.path().join("restored.db");

        let page_size = 4096u32;
        let num_pages = 100;

        let mut db_data = Vec::with_capacity(num_pages * page_size as usize);
        for i in 0..num_pages {
            let pattern = (i as u8).wrapping_mul(37);
            let mut page = vec![pattern; page_size as usize];
            let page_num_bytes = (i as u32).to_le_bytes();
            page[0..4].copy_from_slice(&page_num_bytes);
            db_data.extend(page);
        }
        std::fs::write(&db_path, &db_data).unwrap();

        let encoded = encode_snapshot(&db_path, page_size, 1000, 0).unwrap();

        let result = decode_to_db(&encoded, &restored_path).unwrap();
        let restored_data = std::fs::read(&restored_path).unwrap();
        assert_eq!(db_data, restored_data);
    }

    #[test]
    fn test_encode_to_memory_buffer() {
        let dir = tempdir().unwrap();
        let db_path = dir.path().join("test.db");
        let restored_path = dir.path().join("restored.db");

        let page_size = 4096u32;
        let db_data = vec![0x42u8; page_size as usize * 5];
        std::fs::write(&db_path, &db_data).unwrap();

        let encoded = encode_snapshot(&db_path, page_size, 1, 0).unwrap();
        decode_to_db(&encoded, &restored_path).unwrap();

        let restored_data = std::fs::read(&restored_path).unwrap();
        assert_eq!(db_data, restored_data);
    }

    #[test]
    fn test_apply_changeset_in_place_basic() {
        let dir = tempdir().unwrap();
        let db_path = dir.path().join("test.db");

        let page_size = 4096u32;
        let num_pages = 5;

        let db_data = vec![0x00u8; (page_size as usize) * num_pages];
        std::fs::write(&db_path, &db_data).unwrap();

        // Create incremental that updates pages 2 and 4
        let pages: Vec<(u32, Vec<u8>)> = vec![
            (2, vec![0xAA; page_size as usize]),
            (4, vec![0xBB; page_size as usize]),
        ];

        let pre_checksum = compute_checksum_from_file(&db_path).unwrap();
        let expected_post = chain_checksum(pre_checksum, &pages);

        let (encoded, _checksum) = encode_wal_changes(&pages, page_size, 1, pre_checksum).unwrap();

        let result = apply_changeset_to_db(&encoded, &db_path, pre_checksum).unwrap();

        let result_data = std::fs::read(&db_path).unwrap();

        // Page 1 (index 0): unchanged
        assert_eq!(
            &result_data[0..page_size as usize],
            &vec![0x00u8; page_size as usize][..]
        );
        // Page 2 (index 1): updated to 0xAA
        let page2_start = page_size as usize;
        assert_eq!(
            &result_data[page2_start..page2_start + page_size as usize],
            &vec![0xAAu8; page_size as usize][..]
        );
        // Page 3 (index 2): unchanged
        let page3_start = 2 * page_size as usize;
        assert_eq!(
            &result_data[page3_start..page3_start + page_size as usize],
            &vec![0x00u8; page_size as usize][..]
        );
        // Page 4 (index 3): updated to 0xBB
        let page4_start = 3 * page_size as usize;
        assert_eq!(
            &result_data[page4_start..page4_start + page_size as usize],
            &vec![0xBBu8; page_size as usize][..]
        );
        // Page 5 (index 4): unchanged
        let page5_start = 4 * page_size as usize;
        assert_eq!(
            &result_data[page5_start..page5_start + page_size as usize],
            &vec![0x00u8; page_size as usize][..]
        );

        assert_eq!(result.header.seq, 1);
        assert_eq!(result.checksum, expected_post);
    }

    #[test]
    fn test_apply_changeset_preserves_other_data() {
        let dir = tempdir().unwrap();
        let db_path = dir.path().join("test.db");

        let page_size = 4096u32;

        let mut db_data = Vec::new();
        for i in 0..4u8 {
            db_data.extend(vec![i * 10; page_size as usize]);
        }
        std::fs::write(&db_path, &db_data).unwrap();

        let pages: Vec<(u32, Vec<u8>)> = vec![(3, vec![0xFF; page_size as usize])];

        let pre_checksum = compute_checksum_from_file(&db_path).unwrap();

        let (encoded, _) = encode_wal_changes(&pages, page_size, 1, pre_checksum).unwrap();
        apply_changeset_to_db(&encoded, &db_path, pre_checksum).unwrap();

        let result_data = std::fs::read(&db_path).unwrap();

        // Pages 1, 2, 4 unchanged
        assert_eq!(
            &result_data[0..page_size as usize],
            &vec![0u8; page_size as usize][..]
        );
        assert_eq!(
            &result_data[page_size as usize..2 * page_size as usize],
            &vec![10u8; page_size as usize][..]
        );
        // Page 3 updated
        assert_eq!(
            &result_data[2 * page_size as usize..3 * page_size as usize],
            &vec![0xFFu8; page_size as usize][..]
        );
        // Page 4 unchanged
        assert_eq!(
            &result_data[3 * page_size as usize..4 * page_size as usize],
            &vec![30u8; page_size as usize][..]
        );
    }

    #[test]
    fn test_compute_checksum_from_file() {
        let dir = tempdir().unwrap();
        let db_path = dir.path().join("test.db");

        let data = vec![0x42u8; 4096];
        std::fs::write(&db_path, &data).unwrap();

        let checksum1 = compute_checksum_from_file(&db_path).unwrap();
        let checksum2 = compute_checksum_from_file(&db_path).unwrap();

        assert_eq!(checksum1, checksum2);

        std::fs::write(&db_path, vec![0x43u8; 4096]).unwrap();
        let checksum3 = compute_checksum_from_file(&db_path).unwrap();
        assert_ne!(checksum1, checksum3);
    }

    #[test]
    fn test_apply_chain_simulation() {
        // Simulate: snapshot -> incremental -> incremental
        let dir = tempdir().unwrap();
        let db_path = dir.path().join("test.db");

        let page_size = 4096u32;
        let num_pages = 3;

        let initial_data: Vec<u8> = (0..num_pages)
            .flat_map(|i| vec![(i as u8) * 10; page_size as usize])
            .collect();
        std::fs::write(&db_path, &initial_data).unwrap();

        // Snapshot (seq 1)
        let snap_encoded = encode_snapshot(&db_path, page_size, 1, 0).unwrap();
        let snap_result =
            decode_to_db(&snap_encoded, &dir.path().join("snap_restored.db")).unwrap();

        // First incremental: update page 1 (seq 2)
        let pre_checksum1 = compute_checksum_from_file(&db_path).unwrap();
        let pages1: Vec<(u32, Vec<u8>)> = vec![(1, vec![0xAA; page_size as usize])];
        let expected_post1 = chain_checksum(pre_checksum1, &pages1);

        let (inc1_encoded, post1) =
            encode_wal_changes(&pages1, page_size, 2, pre_checksum1).unwrap();

        let result1 = apply_changeset_to_db(&inc1_encoded, &db_path, pre_checksum1).unwrap();
        assert_eq!(result1.checksum, expected_post1);

        // Second incremental: update page 2 (seq 3), chain from post1
        let pre_checksum2 = result1.checksum;
        let pages2: Vec<(u32, Vec<u8>)> = vec![(2, vec![0xBB; page_size as usize])];
        let expected_post2 = chain_checksum(pre_checksum2, &pages2);

        let (inc2_encoded, _) = encode_wal_changes(&pages2, page_size, 3, pre_checksum2).unwrap();

        let result2 = apply_changeset_to_db(&inc2_encoded, &db_path, pre_checksum2).unwrap();
        assert_eq!(result2.checksum, expected_post2);

        // Final verification
        let final_data = std::fs::read(&db_path).unwrap();
        assert_eq!(
            &final_data[0..page_size as usize],
            &vec![0xAAu8; page_size as usize][..]
        );
        assert_eq!(
            &final_data[page_size as usize..2 * page_size as usize],
            &vec![0xBBu8; page_size as usize][..]
        );
        assert_eq!(
            &final_data[2 * page_size as usize..3 * page_size as usize],
            &vec![20u8; page_size as usize][..]
        );
    }

    #[test]
    fn test_chain_checksum_determinism_and_sorting() {
        let pre = 0xDEADBEEFu64;

        let page1 = (1u32, vec![0xAA; 4096]);
        let page2 = (2u32, vec![0xBB; 4096]);
        let page3 = (3u32, vec![0xCC; 4096]);

        let forward = chain_checksum(pre, &[page1.clone(), page2.clone(), page3.clone()]);
        let reverse = chain_checksum(pre, &[page3.clone(), page2.clone(), page1.clone()]);
        let shuffled = chain_checksum(pre, &[page2.clone(), page3.clone(), page1.clone()]);

        assert_eq!(forward, reverse);
        assert_eq!(forward, shuffled);

        let again = chain_checksum(pre, &[page1.clone(), page2.clone(), page3.clone()]);
        assert_eq!(forward, again);

        let different_pre =
            chain_checksum(0xCAFEBABE, &[page1.clone(), page2.clone(), page3.clone()]);
        assert_ne!(forward, different_pre);

        let page1_modified = (1u32, vec![0xFF; 4096]);
        let different_data = chain_checksum(pre, &[page1_modified, page2.clone(), page3.clone()]);
        assert_ne!(forward, different_data);

        let page1_renumbered = (99u32, vec![0xAA; 4096]);
        let different_num = chain_checksum(pre, &[page1_renumbered, page2, page3]);
        assert_ne!(forward, different_num);

        let empty = chain_checksum(pre, &[]);
        assert_ne!(forward, empty);

        let single = chain_checksum(pre, &[page1]);
        assert_ne!(forward, single);
        assert_ne!(empty, single);
    }

    #[test]
    fn test_checkpoint_mid_chain_continuity() {
        let dir = tempdir().unwrap();
        let db_path = dir.path().join("test.db");
        let page_size = 4096u32;

        let initial_data: Vec<u8> = (0..3)
            .flat_map(|i| vec![(i as u8) * 10; page_size as usize])
            .collect();
        std::fs::write(&db_path, &initial_data).unwrap();

        // Snapshot
        let snap_encoded = encode_snapshot(&db_path, page_size, 1, 0).unwrap();
        let restored_path = dir.path().join("restored.db");
        let snap_result = decode_to_db(&snap_encoded, &restored_path).unwrap();
        let checksum_after_snap = snap_result.checksum;

        // Incremental 1: modify page 1
        let pages1: Vec<(u32, Vec<u8>)> = vec![(1, vec![0xAA; page_size as usize])];
        let post1 = chain_checksum(checksum_after_snap, &pages1);
        let (buf1, _) = encode_wal_changes(&pages1, page_size, 2, checksum_after_snap).unwrap();
        let r1 = apply_changeset_to_db(&buf1, &restored_path, checksum_after_snap).unwrap();
        assert_eq!(r1.checksum, post1);

        // === CHECKPOINT HAPPENS HERE ===

        // Incremental 2: modify page 2, chain from post1
        let pages2: Vec<(u32, Vec<u8>)> = vec![(2, vec![0xBB; page_size as usize])];
        let post2 = chain_checksum(post1, &pages2);
        let (buf2, _) = encode_wal_changes(&pages2, page_size, 3, post1).unwrap();
        let r2 = apply_changeset_to_db(&buf2, &restored_path, post1).unwrap();
        assert_eq!(r2.checksum, post2);

        // Chain links are intact
        assert_eq!(r1.checksum, r2.header.prev_checksum);

        // Incremental 3: another post-checkpoint write
        let pages3: Vec<(u32, Vec<u8>)> = vec![(3, vec![0xCC; page_size as usize])];
        let post3 = chain_checksum(post2, &pages3);
        let (buf3, _) = encode_wal_changes(&pages3, page_size, 4, post2).unwrap();
        let r3 = apply_changeset_to_db(&buf3, &restored_path, post2).unwrap();
        assert_eq!(r3.checksum, post3);
        assert_eq!(r2.checksum, r3.header.prev_checksum);
    }

    #[test]
    fn test_restore_chain_verification_snapshot_plus_incrementals() {
        let dir = tempdir().unwrap();
        let db_path = dir.path().join("source.db");
        let restore_path = dir.path().join("restored.db");
        let page_size = 4096u32;

        let source_data: Vec<u8> = (0..5)
            .flat_map(|i| vec![(i as u8) * 11; page_size as usize])
            .collect();
        std::fs::write(&db_path, &source_data).unwrap();

        // Snapshot
        let snap_encoded = encode_snapshot(&db_path, page_size, 1, 0).unwrap();
        let snap_result = decode_to_db(&snap_encoded, &restore_path).unwrap();
        let mut last_checksum = snap_result.checksum;

        // Apply 5 incrementals
        let mut encoded_bufs = Vec::new();
        for i in 0..5u32 {
            let page_num = (i % 5) + 1;
            let data = vec![(0xF0 + i) as u8; page_size as usize];
            let pages: Vec<(u32, Vec<u8>)> = vec![(page_num, data)];

            let (buf, checksum) =
                encode_wal_changes(&pages, page_size, (i + 2) as u64, last_checksum).unwrap();
            encoded_bufs.push(buf);
            last_checksum = checksum;
        }

        // Apply all incrementals, verifying chain at each step
        let mut prev_checksum = snap_result.checksum;
        for (i, buf) in encoded_bufs.iter().enumerate() {
            let result = apply_changeset_to_db(buf, &restore_path, prev_checksum).unwrap();

            assert_eq!(
                prev_checksum, result.header.prev_checksum,
                "Chain broken at incremental {}: prev {:016x} != header prev {:016x}",
                i, prev_checksum, result.header.prev_checksum
            );

            prev_checksum = result.checksum;
        }

        let restored_bytes = std::fs::read(&restore_path).unwrap();
        assert_eq!(restored_bytes.len(), 5 * page_size as usize);

        // Last incremental wrote page 5 with 0xF4
        let page5_start = 4 * page_size as usize;
        assert!(
            restored_bytes[page5_start..page5_start + 10]
                .iter()
                .all(|&b| b == 0xF4),
            "Page 5 should contain the last incremental's data"
        );
    }

    #[test]
    fn test_apply_many_pages() {
        // Verify correctness with many pages
        let dir = tempdir().unwrap();
        let db_path = dir.path().join("test.db");
        let page_size = 4096u32;
        let num_pages = 500;

        let initial_data = vec![0x00u8; page_size as usize * num_pages];
        std::fs::write(&db_path, &initial_data).unwrap();

        let pre_checksum = compute_checksum_from_file(&db_path).unwrap();

        let pages: Vec<(u32, Vec<u8>)> = (1..=num_pages as u32)
            .map(|i| (i, vec![(i % 256) as u8; page_size as usize]))
            .collect();

        let expected_post = chain_checksum(pre_checksum, &pages);

        let (encoded, _) = encode_wal_changes(&pages, page_size, 2, pre_checksum).unwrap();
        let result = apply_changeset_to_db(&encoded, &db_path, pre_checksum).unwrap();

        assert_eq!(result.checksum, expected_post);

        let final_data = std::fs::read(&db_path).unwrap();
        for i in 1..=num_pages as u32 {
            let start = (i as usize - 1) * page_size as usize;
            assert_eq!(
                final_data[start],
                (i % 256) as u8,
                "Page {} should have been updated",
                i
            );
        }
    }

    #[test]
    fn test_chain_broken_detection() {
        let dir = tempdir().unwrap();
        let db_path = dir.path().join("test.db");
        let page_size = 4096u32;

        let initial_data = vec![0x00u8; page_size as usize * 3];
        std::fs::write(&db_path, &initial_data).unwrap();

        let pages: Vec<(u32, Vec<u8>)> = vec![(1, vec![0xAA; page_size as usize])];

        let (encoded, _) = encode_wal_changes(&pages, page_size, 1, 0).unwrap();

        // Apply with wrong expected_prev_checksum
        let result = apply_changeset_to_db(&encoded, &db_path, 0xDEADBEEF);
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("Checksum chain broken"));
    }
}
