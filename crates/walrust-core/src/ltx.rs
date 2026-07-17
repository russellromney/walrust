//! HADBP (hadb-changeset physical) format support for SQLite WAL replication.
//!
//! This module provides utilities for encoding and decoding HADBP changesets
//! containing SQLite pages. It wraps hadb-changeset with SQLite-specific
//! defaults (U32 page IDs, 4KB pages).
//!
//! Replaces the former litepages/LTX format. The on-disk format is .hadbp,
//! but all replication semantics (WAL parsing, checksum chaining, snapshot +
//! incremental restore) remain identical.
//!
//! Naming: the module path `walrust_core::ltx` (and the `ltx_*`/`Ltx*`
//! symbols it exposes) is a LEGACY ALIAS retained for public-API stability
//! across the root crate, the DST harness, and downstream embedders. It does
//! NOT implement the litepages LTX wire format — the checksum is computed
//! differently (HADBP folds `data_len` into the hash) and the two are not
//! byte-compatible. Renaming the module/types would be pure churn touching the
//! public crate API in multiple trees, so the name stays and this note removes
//! the ambiguity: read "ltx" here as "the HADBP changeset codec".

use anyhow::{anyhow, Result};
use hadb_changeset::physical::{
    self, PageEntry, PageId, PageIdSize, PhysicalChangeset, PhysicalHeader,
};
use std::io::Read;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

/// SQLite page ID size (u32).
pub const SQLITE_PAGE_ID_SIZE: PageIdSize = PageIdSize::U32;
pub const FLAG_END_PAGE_COUNT_MARKER: u8 = 0x01;
const HADBP_HEADER_SIZE: usize = 40;
const HADBP_TRAILER_SIZE: usize = 8;
const MIN_SQLITE_PAGE_SIZE: u32 = 512;
const MAX_SQLITE_PAGE_SIZE: u32 = 65_536;
const MAX_DECODED_DB_BYTES: u64 = 1 << 40; // 1 TiB sanity cap for untrusted objects.

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
    encode_snapshot_with_checksum(db_path, page_size, seq, prev_checksum)
        .map(|encoded| encoded.bytes)
}

#[derive(Debug)]
pub struct EncodedSnapshot {
    pub bytes: Vec<u8>,
    pub checksum: u64,
}

/// Create an HADBP snapshot from a stable SQLite backup copy.
pub fn encode_sqlite_snapshot(
    db_path: &Path,
    page_size: u32,
    seq: u64,
    prev_checksum: u64,
) -> Result<EncodedSnapshot> {
    let snapshot = StableSqliteSnapshot::create(db_path)?;
    encode_snapshot_with_checksum(snapshot.path(), page_size, seq, prev_checksum)
}

/// Create an HADBP changeset from an already-stable database image and return
/// the checksum of the database bytes that were encoded.
pub fn encode_snapshot_with_checksum(
    db_path: &Path,
    page_size: u32,
    seq: u64,
    prev_checksum: u64,
) -> Result<EncodedSnapshot> {
    let file = std::fs::File::open(db_path)
        .map_err(|e| anyhow!("Failed to open database for snapshot: {}", e))?;
    encode_snapshot_with_checksum_fd(&file, page_size, seq, prev_checksum)
}

/// [`encode_snapshot_with_checksum`] through an already-open descriptor.
///
/// Snapshot encoding while the checkpoint blocker is armed MUST borrow the
/// retained source descriptor this way: opening and closing a fresh
/// descriptor for the main DB would release the process's POSIX locks on the
/// inode (see the `blocker` module docs).
pub fn encode_snapshot_with_checksum_fd(
    file: &std::fs::File,
    page_size: u32,
    seq: u64,
    prev_checksum: u64,
) -> Result<EncodedSnapshot> {
    use sha2::{Digest, Sha256};
    use std::io::{BufReader, Seek, SeekFrom};

    let page_size_usize = validate_sqlite_page_size(page_size)?;
    let file_size = file.metadata()?.len() as usize;
    if !file_size.is_multiple_of(page_size_usize) {
        return Err(anyhow!(
            "database size {} is not a multiple of SQLite page_size {}",
            file_size,
            page_size
        ));
    }
    let num_pages = file_size / page_size_usize;
    if num_pages > u32::MAX as usize {
        return Err(anyhow!(
            "database has {} pages, exceeds SQLite U32 page-id limit",
            num_pages
        ));
    }

    let mut file_ref = file;
    file_ref.seek(SeekFrom::Start(0))?;
    let mut reader = BufReader::with_capacity(1024 * 1024, file_ref);
    let mut pages = Vec::with_capacity(num_pages);
    let mut hasher = Sha256::new();

    for i in 0..num_pages {
        let mut page_buf = vec![0u8; page_size_usize];
        reader.read_exact(&mut page_buf)?;
        hasher.update(&page_buf);
        // SQLite uses 1-based page numbers
        pages.push(PageEntry {
            page_id: PageId::U32((i + 1) as u32),
            data: page_buf,
        });
    }

    let changeset =
        PhysicalChangeset::new(seq, prev_checksum, SQLITE_PAGE_ID_SIZE, page_size, pages);
    let checksum = {
        let result = hasher.finalize();
        u64::from_be_bytes(result[0..8].try_into().expect("sha256 is 32 bytes"))
    };
    Ok(EncodedSnapshot {
        bytes: physical::encode(&changeset),
        checksum,
    })
}

struct StableSqliteSnapshot {
    path: PathBuf,
}

impl StableSqliteSnapshot {
    fn create(source: &Path) -> Result<Self> {
        static NEXT_ID: AtomicU64 = AtomicU64::new(0);

        let parent = source.parent().unwrap_or_else(|| Path::new("."));
        let file_name = source
            .file_name()
            .and_then(|name| name.to_str())
            .unwrap_or("database");
        let id = NEXT_ID.fetch_add(1, Ordering::Relaxed);
        let path = parent.join(format!(
            ".{file_name}.walrust-snapshot-{}-{id}.db",
            std::process::id()
        ));

        if path.exists() {
            std::fs::remove_file(&path)?;
        }

        let dest = path
            .to_str()
            .ok_or_else(|| anyhow!("snapshot path is not valid UTF-8: {}", path.display()))?;
        let conn = rusqlite::Connection::open(source)
            .map_err(|e| anyhow!("Failed to open database for stable snapshot: {}", e))?;
        conn.busy_timeout(std::time::Duration::from_secs(30))?;
        conn.execute("VACUUM INTO ?1", [dest])
            .map_err(|e| anyhow!("Failed to create stable SQLite snapshot: {}", e))?;

        Ok(Self { path })
    }

    fn path(&self) -> &Path {
        &self.path
    }
}

impl Drop for StableSqliteSnapshot {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.path);
    }
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
    let changeset = decode_sqlite_changeset(data)?;
    if changeset_end_page_count(&changeset)?.is_some() {
        return Err(anyhow!(
            "HADBP snapshot decode does not accept incremental end-page-count markers"
        ));
    }
    let page_size = validate_sqlite_page_size(changeset.header.page_size)? as u64;

    // Find the maximum page number to determine DB size
    let max_page = changeset
        .pages
        .iter()
        .map(|p| p.page_id.to_u64())
        .max()
        .unwrap_or(0);
    let db_size = max_page.checked_mul(page_size).ok_or_else(|| {
        anyhow!("changeset max_page * page_size overflows u64 (corrupt changeset)")
    })?;
    if db_size > MAX_DECODED_DB_BYTES {
        return Err(anyhow!(
            "decoded database size {} exceeds safety cap {} bytes",
            db_size,
            MAX_DECODED_DB_BYTES
        ));
    }

    write_snapshot_changeset_atomically(output_path, &changeset, db_size)?;

    // Verify actual checksum from the bytes written, without materializing the
    // decoded database in memory.
    let actual_checksum = compute_checksum_from_file(output_path)?;
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

fn write_snapshot_changeset_atomically(
    output_path: &Path,
    changeset: &PhysicalChangeset,
    db_size: u64,
) -> Result<()> {
    use std::io::{Seek, SeekFrom, Write};

    let tmp_path = output_path.with_extension("tmp");
    let write_result = (|| -> Result<()> {
        let mut file = std::fs::File::create(&tmp_path)?;
        file.set_len(db_size)?;
        for page in &changeset.pages {
            let offset = sqlite_page_offset(page.page_id.to_u64(), changeset.header.page_size)?;
            file.seek(SeekFrom::Start(offset))?;
            file.write_all(&page.data)?;
        }
        file.sync_all()?;
        Ok(())
    })();
    if let Err(err) = write_result {
        let _ = std::fs::remove_file(&tmp_path);
        return Err(err);
    }
    std::fs::rename(&tmp_path, output_path)?;
    fsync_parent_dir(output_path)?;
    Ok(())
}

fn validate_sqlite_page_size(page_size: u32) -> Result<usize> {
    if !(MIN_SQLITE_PAGE_SIZE..=MAX_SQLITE_PAGE_SIZE).contains(&page_size)
        || !page_size.is_power_of_two()
    {
        return Err(anyhow!(
            "Invalid SQLite page_size {page_size}; expected power of two between \
             {MIN_SQLITE_PAGE_SIZE} and {MAX_SQLITE_PAGE_SIZE}"
        ));
    }
    Ok(page_size as usize)
}

fn preflight_hadbp_header(data: &[u8]) -> Result<()> {
    if data.len() < HADBP_HEADER_SIZE {
        return Ok(());
    }

    let page_id_size = data[7];
    let pid_len = match page_id_size {
        4 | 8 => page_id_size as usize,
        _ => return Ok(()),
    };
    let page_size = u32::from_be_bytes(data[8..12].try_into().expect("4 bytes"));
    validate_sqlite_page_size(page_size)?;
    let page_count = u32::from_be_bytes(data[28..32].try_into().expect("4 bytes")) as usize;
    let max_possible_pages = data
        .len()
        .saturating_sub(HADBP_HEADER_SIZE + HADBP_TRAILER_SIZE)
        / (pid_len + 4);
    if page_count > max_possible_pages {
        return Err(anyhow!(
            "HADBP page_count {} exceeds encoded body capacity {}",
            page_count,
            max_possible_pages
        ));
    }
    Ok(())
}

pub fn decode_sqlite_changeset(data: &[u8]) -> Result<PhysicalChangeset> {
    preflight_hadbp_header(data)?;
    let changeset =
        physical::decode(data).map_err(|e| anyhow!("Failed to decode HADBP changeset: {}", e))?;
    validate_sqlite_changeset(&changeset)?;
    Ok(changeset)
}

pub fn validate_sqlite_changeset(changeset: &PhysicalChangeset) -> Result<()> {
    let page_size = validate_sqlite_page_size(changeset.header.page_size)?;
    if changeset.header.flags & !FLAG_END_PAGE_COUNT_MARKER != 0 {
        return Err(anyhow!(
            "Unsupported HADBP flags for SQLite changeset: 0x{:02x}",
            changeset.header.flags
        ));
    }
    if changeset.header.page_id_size != SQLITE_PAGE_ID_SIZE {
        return Err(anyhow!(
            "Invalid SQLite page_id_size {:?}; expected {:?}",
            changeset.header.page_id_size,
            SQLITE_PAGE_ID_SIZE
        ));
    }
    if changeset.header.page_count as usize != changeset.pages.len() {
        return Err(anyhow!(
            "HADBP page_count {} does not match decoded page count {}",
            changeset.header.page_count,
            changeset.pages.len()
        ));
    }
    let end_page_count = changeset_end_page_count(changeset)?;
    for page in &changeset.pages {
        let page_num = page.page_id.to_u64();
        if page_num == 0 {
            return Err(anyhow!("changeset contains invalid page number 0"));
        }
        if page.data.is_empty() {
            continue;
        }
        if let Some(end_page_count) = end_page_count {
            if page_num > end_page_count {
                return Err(anyhow!(
                    "changeset page {} is past encoded end_page_count {}",
                    page_num,
                    end_page_count
                ));
            }
        }
        if page.data.len() != page_size {
            return Err(anyhow!(
                "changeset page {} has {} bytes, expected SQLite page_size {}",
                page_num,
                page.data.len(),
                page_size
            ));
        }
    }
    Ok(())
}

pub fn changeset_end_page_count(changeset: &PhysicalChangeset) -> Result<Option<u64>> {
    let mut marker = None;
    for page in &changeset.pages {
        if !page.data.is_empty() {
            continue;
        }
        if changeset.header.flags & FLAG_END_PAGE_COUNT_MARKER == 0 {
            return Err(anyhow!(
                "changeset page {} is empty but end-page-count marker flag is not set",
                page.page_id.to_u64()
            ));
        }
        let page_num = page.page_id.to_u64();
        if page_num == 0 {
            return Err(anyhow!("changeset contains invalid page number 0"));
        }
        if marker.replace(page_num - 1).is_some() {
            return Err(anyhow!(
                "changeset contains multiple end-page-count markers"
            ));
        }
    }
    if changeset.header.flags & FLAG_END_PAGE_COUNT_MARKER != 0 && marker.is_none() {
        return Err(anyhow!(
            "HADBP end-page-count marker flag set without marker page"
        ));
    }
    Ok(marker)
}

fn sqlite_page_offset(page_num: u64, page_size: u32) -> Result<u64> {
    if page_num == 0 {
        return Err(anyhow!("changeset contains invalid page number 0"));
    }
    let page_size = validate_sqlite_page_size(page_size)? as u64;
    page_num
        .checked_sub(1)
        .and_then(|idx| idx.checked_mul(page_size))
        .ok_or_else(|| anyhow!("SQLite page offset overflow for page {page_num}"))
}

pub fn apply_decoded_changeset_to_db(changeset: &PhysicalChangeset, db_path: &Path) -> Result<()> {
    validate_sqlite_changeset(changeset)?;
    use std::fs::OpenOptions;
    use std::io::{Seek, SeekFrom, Write as IoWrite};

    let mut file = OpenOptions::new()
        .write(true)
        .open(db_path)
        .map_err(|e| anyhow!("Failed to open database for in-place apply: {}", e))?;

    for page in &changeset.pages {
        if page.data.is_empty() {
            continue;
        }
        let offset = sqlite_page_offset(page.page_id.to_u64(), changeset.header.page_size)?;
        file.seek(SeekFrom::Start(offset))?;
        file.write_all(&page.data)?;
    }
    if let Some(end_page_count) = changeset_end_page_count(changeset)? {
        let page_size = validate_sqlite_page_size(changeset.header.page_size)? as u64;
        let len = end_page_count
            .checked_mul(page_size)
            .ok_or_else(|| anyhow!("end_page_count * page_size overflows u64"))?;
        file.set_len(len)?;
    }

    file.sync_all()?;
    Ok(())
}

fn fsync_parent_dir(path: &Path) -> Result<()> {
    let parent = path
        .parent()
        .ok_or_else(|| anyhow!("path has no parent: {}", path.display()))?;
    let dir = std::fs::File::open(parent)?;
    dir.sync_all()?;
    Ok(())
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
    let changeset = decode_sqlite_changeset(data)?;

    // Verify checksum chain before writing
    physical::verify_chain(expected_prev_checksum, &changeset)
        .map_err(|e| anyhow!("Checksum chain broken: {}", e))?;

    apply_decoded_changeset_to_db(&changeset, db_path)?;

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
    encode_wal_changes_inner(pages, page_size, seq, prev_checksum, None)
}

pub fn encode_wal_changes_with_end_page_count(
    pages: &[(u32, Vec<u8>)],
    page_size: u32,
    seq: u64,
    prev_checksum: u64,
    end_page_count: u64,
) -> Result<(Vec<u8>, u64)> {
    encode_wal_changes_inner(pages, page_size, seq, prev_checksum, Some(end_page_count))
}

fn encode_wal_changes_inner(
    pages: &[(u32, Vec<u8>)],
    page_size: u32,
    seq: u64,
    prev_checksum: u64,
    end_page_count: Option<u64>,
) -> Result<(Vec<u8>, u64)> {
    validate_sqlite_page_size(page_size)?;
    let entries: Vec<PageEntry> = pages
        .iter()
        .map(|(page_num, data)| PageEntry {
            page_id: PageId::U32(*page_num),
            data: data.clone(),
        })
        .collect();

    let mut entries = entries;
    if let Some(end_page_count) = end_page_count {
        if end_page_count == 0 {
            return Err(anyhow!(
                "end_page_count=0 is invalid for a SQLite changeset"
            ));
        }
        if end_page_count >= u32::MAX as u64 {
            return Err(anyhow!(
                "end_page_count {} exceeds SQLite U32 page-id marker capacity",
                end_page_count
            ));
        }
        for (page_num, data) in pages {
            if *page_num == 0 {
                return Err(anyhow!("changeset contains invalid page number 0"));
            }
            if *page_num as u64 > end_page_count {
                return Err(anyhow!(
                    "changeset page {} is past encoded end_page_count {}",
                    page_num,
                    end_page_count
                ));
            }
            if data.len() != page_size as usize {
                return Err(anyhow!(
                    "changeset page {} has {} bytes, expected SQLite page_size {}",
                    page_num,
                    data.len(),
                    page_size
                ));
            }
        }
        entries.push(PageEntry {
            page_id: PageId::U32((end_page_count + 1) as u32),
            data: Vec::new(),
        });
    }

    let mut changeset =
        PhysicalChangeset::new(seq, prev_checksum, SQLITE_PAGE_ID_SIZE, page_size, entries);
    if end_page_count.is_some() {
        changeset.header.flags |= FLAG_END_PAGE_COUNT_MARKER;
    }
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
    fn test_encode_sqlite_snapshot_includes_wal_and_returns_encoded_checksum() {
        let dir = tempdir().unwrap();
        let db_path = dir.path().join("wal-resident.db");
        let restored_path = dir.path().join("restored.db");
        let conn = rusqlite::Connection::open(&db_path).unwrap();
        conn.execute_batch(
            "
            PRAGMA page_size=4096;
            PRAGMA journal_mode=WAL;
            PRAGMA wal_autocheckpoint=0;
            CREATE TABLE items (id INTEGER PRIMARY KEY, value TEXT NOT NULL);
            INSERT INTO items (id, value) VALUES (1, 'base');
            INSERT INTO items (id, value) VALUES (2, 'wal-resident');
            ",
        )
        .unwrap();

        let encoded = encode_sqlite_snapshot(&db_path, 4096, 1, 0).unwrap();
        let decoded = decode_to_db(&encoded.bytes, &restored_path).unwrap();

        let restored = rusqlite::Connection::open(&restored_path).unwrap();
        let count: i64 = restored
            .query_row("SELECT COUNT(*) FROM items", [], |row| row.get(0))
            .unwrap();

        assert_eq!(count, 2);
        assert_eq!(encoded.checksum, decoded.checksum);
        assert_eq!(
            encoded.checksum,
            compute_checksum_from_file(&restored_path).unwrap()
        );
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

        for page_size in [512u32, 1024, 2048, 4096, 8192, 16384, 32768, 65536] {
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
    fn test_sqlite_snapshot_over_100mb_smoke() {
        let dir = tempdir().unwrap();
        let db_path = dir.path().join("large-sqlite.db");
        let restored_path = dir.path().join("restored-large-sqlite.db");

        let conn = rusqlite::Connection::open(&db_path).unwrap();
        conn.execute_batch(
            "
            PRAGMA page_size=4096;
            PRAGMA journal_mode=WAL;
            PRAGMA wal_autocheckpoint=0;
            CREATE TABLE blobs (id INTEGER PRIMARY KEY, data BLOB NOT NULL);
            BEGIN IMMEDIATE;
            ",
        )
        .unwrap();
        for id in 1..=101i64 {
            conn.execute(
                "INSERT INTO blobs (id, data) VALUES (?1, zeroblob(1048576))",
                rusqlite::params![id],
            )
            .unwrap();
        }
        conn.execute_batch("COMMIT; PRAGMA wal_checkpoint(TRUNCATE);")
            .unwrap();
        drop(conn);

        assert!(
            std::fs::metadata(&db_path).unwrap().len() > 100 * 1024 * 1024,
            "fixture must exercise a database larger than 100 MiB"
        );

        let encoded = encode_sqlite_snapshot(&db_path, 4096, 1, 0).unwrap();
        let decoded = decode_to_db(&encoded.bytes, &restored_path).unwrap();

        let restored = rusqlite::Connection::open(&restored_path).unwrap();
        let integrity: String = restored
            .query_row("PRAGMA integrity_check", [], |row| row.get(0))
            .unwrap();
        let count: i64 = restored
            .query_row("SELECT COUNT(*) FROM blobs", [], |row| row.get(0))
            .unwrap();
        let bytes: i64 = restored
            .query_row("SELECT SUM(length(data)) FROM blobs", [], |row| row.get(0))
            .unwrap();

        assert_eq!(integrity, "ok");
        assert_eq!(count, 101);
        assert_eq!(bytes, 101 * 1024 * 1024);
        assert_eq!(encoded.checksum, decoded.checksum);
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
    fn test_apply_rejects_page_id_zero_without_mutating_database() {
        let dir = tempdir().unwrap();
        let db_path = dir.path().join("test.db");
        let page_size = 4096u32;
        let original = vec![0x11u8; page_size as usize * 2];
        std::fs::write(&db_path, &original).unwrap();

        let pre_checksum = compute_checksum_from_file(&db_path).unwrap();
        let (encoded, _) = encode_wal_changes(
            &[(0, vec![0xAA; page_size as usize])],
            page_size,
            2,
            pre_checksum,
        )
        .unwrap();

        let result = apply_changeset_to_db(&encoded, &db_path, pre_checksum);

        assert!(result.is_err(), "page_id 0 must be rejected");
        assert!(
            result.unwrap_err().to_string().contains("page number 0"),
            "error should identify invalid page 0"
        );
        assert_eq!(std::fs::read(&db_path).unwrap(), original);
    }

    #[test]
    fn test_apply_rejects_invalid_sqlite_page_size_without_mutating_database() {
        let dir = tempdir().unwrap();
        let db_path = dir.path().join("test.db");
        let original = vec![0x11u8; 4096 * 2];
        std::fs::write(&db_path, &original).unwrap();

        let pre_checksum = compute_checksum_from_file(&db_path).unwrap();
        let changeset = PhysicalChangeset::new(
            2,
            pre_checksum,
            SQLITE_PAGE_ID_SIZE,
            1000,
            vec![PageEntry {
                page_id: PageId::U32(1),
                data: vec![0xAA; 1000],
            }],
        );
        let encoded = physical::encode(&changeset);

        let result = apply_changeset_to_db(&encoded, &db_path, pre_checksum);

        assert!(result.is_err(), "non-SQLite page size must be rejected");
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("Invalid SQLite page_size"),
            "error should identify invalid page size"
        );
        assert_eq!(std::fs::read(&db_path).unwrap(), original);
    }

    #[test]
    fn test_decode_rejects_invalid_sqlite_page_size() {
        let dir = tempdir().unwrap();
        let restored_path = dir.path().join("restored.db");
        let changeset = PhysicalChangeset::new(
            1,
            0,
            SQLITE_PAGE_ID_SIZE,
            0,
            vec![PageEntry {
                page_id: PageId::U32(1),
                data: Vec::new(),
            }],
        );
        let encoded = physical::encode(&changeset);

        let result = decode_to_db(&encoded, &restored_path);

        assert!(result.is_err(), "zero page_size must be rejected");
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("Invalid SQLite page_size"),
            "error should identify invalid page size"
        );
        assert!(!restored_path.exists());
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
