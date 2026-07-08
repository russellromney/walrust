//! Benchmarks for walrust
//!
//! Run with: cargo bench

use brunch::{benches, Bench};
use sha2::{Digest, Sha256};
use std::hint::black_box;

/// Create a valid WAL header (32 bytes)
fn create_wal_header() -> [u8; 32] {
    let mut buf = [0u8; 32];
    // Magic number (big-endian): 0x377F0682
    buf[0..4].copy_from_slice(&0x377F0682_u32.to_be_bytes());
    // Format version: 3007000
    buf[4..8].copy_from_slice(&3007000_u32.to_be_bytes());
    // Page size: 4096
    buf[8..12].copy_from_slice(&4096_u32.to_be_bytes());
    // Checkpoint seq: 1
    buf[12..16].copy_from_slice(&1_u32.to_be_bytes());
    // Salt1, Salt2, Checksum1, Checksum2 (random values)
    buf[16..20].copy_from_slice(&0x12345678_u32.to_be_bytes());
    buf[20..24].copy_from_slice(&0x9ABCDEF0_u32.to_be_bytes());
    buf[24..28].copy_from_slice(&0xDEADBEEF_u32.to_be_bytes());
    buf[28..32].copy_from_slice(&0xCAFEBABE_u32.to_be_bytes());
    buf
}

/// Parse WAL header from bytes (the hot path)
fn parse_wal_header(buf: &[u8; 32]) -> (u32, u32, u32, u32, u32, u32, u32, u32) {
    let magic = u32::from_be_bytes([buf[0], buf[1], buf[2], buf[3]]);
    let format_version = u32::from_be_bytes([buf[4], buf[5], buf[6], buf[7]]);
    let page_size = u32::from_be_bytes([buf[8], buf[9], buf[10], buf[11]]);
    let checkpoint_seq = u32::from_be_bytes([buf[12], buf[13], buf[14], buf[15]]);
    let salt1 = u32::from_be_bytes([buf[16], buf[17], buf[18], buf[19]]);
    let salt2 = u32::from_be_bytes([buf[20], buf[21], buf[22], buf[23]]);
    let checksum1 = u32::from_be_bytes([buf[24], buf[25], buf[26], buf[27]]);
    let checksum2 = u32::from_be_bytes([buf[28], buf[29], buf[30], buf[31]]);
    (
        magic,
        format_version,
        page_size,
        checkpoint_seq,
        salt1,
        salt2,
        checksum1,
        checksum2,
    )
}

/// Create a WAL frame header (24 bytes)
fn create_frame_header() -> [u8; 24] {
    let mut buf = [0u8; 24];
    buf[0..4].copy_from_slice(&1_u32.to_be_bytes()); // page_number
    buf[4..8].copy_from_slice(&100_u32.to_be_bytes()); // db_size
    buf[8..12].copy_from_slice(&0x12345678_u32.to_be_bytes()); // salt1
    buf[12..16].copy_from_slice(&0x9ABCDEF0_u32.to_be_bytes()); // salt2
    buf[16..20].copy_from_slice(&0xDEADBEEF_u32.to_be_bytes()); // checksum1
    buf[20..24].copy_from_slice(&0xCAFEBABE_u32.to_be_bytes()); // checksum2
    buf
}

/// Parse frame header from bytes
fn parse_frame_header(buf: &[u8; 24]) -> (u32, u32, u32, u32, u32, u32) {
    let page_number = u32::from_be_bytes([buf[0], buf[1], buf[2], buf[3]]);
    let db_size = u32::from_be_bytes([buf[4], buf[5], buf[6], buf[7]]);
    let salt1 = u32::from_be_bytes([buf[8], buf[9], buf[10], buf[11]]);
    let salt2 = u32::from_be_bytes([buf[12], buf[13], buf[14], buf[15]]);
    let checksum1 = u32::from_be_bytes([buf[16], buf[17], buf[18], buf[19]]);
    let checksum2 = u32::from_be_bytes([buf[20], buf[21], buf[22], buf[23]]);
    (page_number, db_size, salt1, salt2, checksum1, checksum2)
}

// Test data for SHA256 benchmarks
fn create_test_db(size_kb: usize) -> Vec<u8> {
    vec![0x42u8; size_kb * 1024]
}

/// OLD: Full-DB checksum — reads entire DB, applies overlay pages, hashes everything.
/// This is what compute_expected_post_with_overlay() did on every sync cycle.
fn full_db_checksum(db_data: &[u8], pages: &[(u32, Vec<u8>)], page_size: u32) -> u64 {
    let mut data = db_data.to_vec();
    for (page_num, page_data) in pages {
        let offset = (*page_num as usize - 1) * page_size as usize;
        let end = offset + page_size as usize;
        if end > data.len() {
            data.resize(end, 0);
        }
        data[offset..end].copy_from_slice(page_data);
    }
    let mut hasher = Sha256::new();
    hasher.update(&data);
    let result = hasher.finalize();
    u64::from_be_bytes(result[0..8].try_into().unwrap())
}

/// NEW: Chain checksum — hashes only the pre_checksum + dirty pages.
/// No full-DB read needed.
fn chain_checksum(pre: u64, pages: &[(u32, Vec<u8>)]) -> u64 {
    let mut hasher = Sha256::new();
    hasher.update(pre.to_be_bytes());
    let mut sorted_indices: Vec<usize> = (0..pages.len()).collect();
    sorted_indices.sort_by_key(|&i| pages[i].0);
    for &i in &sorted_indices {
        hasher.update(pages[i].0.to_be_bytes());
        hasher.update(&pages[i].1);
    }
    let result = hasher.finalize();
    u64::from_be_bytes(result[0..8].try_into().unwrap())
}

/// Create N dirty pages (4KB each)
fn create_dirty_pages(n: usize) -> Vec<(u32, Vec<u8>)> {
    (0..n)
        .map(|i| ((i as u32 + 1), vec![(i as u8).wrapping_mul(0x37); 4096]))
        .collect()
}

benches!(
    // WAL header parsing (CPU-bound, hot path)
    Bench::new("wal_header_parse").with_samples(10_000).run(|| {
        let header = create_wal_header();
        black_box(parse_wal_header(&header))
    }),
    // WAL frame header parsing
    Bench::new("wal_frame_header_parse")
        .with_samples(10_000)
        .run(|| {
            let frame = create_frame_header();
            black_box(parse_frame_header(&frame))
        }),
    // SHA256 checksum - 1KB (small db)
    Bench::new("sha256_1kb").with_samples(5_000).run(|| {
        let data = create_test_db(1);
        let mut hasher = Sha256::new();
        hasher.update(&data);
        black_box(hasher.finalize())
    }),
    // SHA256 checksum - 100KB (typical small db)
    Bench::new("sha256_100kb").with_samples(1_000).run(|| {
        let data = create_test_db(100);
        let mut hasher = Sha256::new();
        hasher.update(&data);
        black_box(hasher.finalize())
    }),
    // SHA256 checksum - 1MB (medium db)
    Bench::new("sha256_1mb").with_samples(500).run(|| {
        let data = create_test_db(1024);
        let mut hasher = Sha256::new();
        hasher.update(&data);
        black_box(hasher.finalize())
    }),
    // SHA256 checksum - 10MB (larger db)
    Bench::new("sha256_10mb").with_samples(100).run(|| {
        let data = create_test_db(10 * 1024);
        let mut hasher = Sha256::new();
        hasher.update(&data);
        black_box(hasher.finalize())
    }),
    // =========================================================
    // v0.5.0 Before/After: Full-DB checksum vs chain checksum
    // =========================================================
    //
    // OLD (before): Read entire DB + apply overlay + SHA-256 hash everything
    // NEW (after): SHA-256 hash only (pre_checksum + dirty pages)
    //
    // 10 dirty pages is typical for a 1-second sync cycle

    // --- 1MB database, 10 dirty pages ---
    Bench::new("OLD_full_db_checksum_1mb_10pages")
        .with_samples(500)
        .run(|| {
            let db = create_test_db(1024); // 1MB
            let pages = create_dirty_pages(10);
            black_box(full_db_checksum(&db, &pages, 4096))
        }),
    Bench::new("NEW_chain_checksum_10pages")
        .with_samples(5_000)
        .run(|| {
            let pages = create_dirty_pages(10);
            black_box(chain_checksum(0xDEADBEEF, &pages))
        }),
    // --- 10MB database, 10 dirty pages ---
    Bench::new("OLD_full_db_checksum_10mb_10pages")
        .with_samples(100)
        .run(|| {
            let db = create_test_db(10 * 1024); // 10MB
            let pages = create_dirty_pages(10);
            black_box(full_db_checksum(&db, &pages, 4096))
        }),
    // --- 50MB database, 10 dirty pages ---
    Bench::new("OLD_full_db_checksum_50mb_10pages")
        .with_samples(50)
        .run(|| {
            let db = create_test_db(50 * 1024); // 50MB
            let pages = create_dirty_pages(10);
            black_box(full_db_checksum(&db, &pages, 4096))
        }),
    // --- 100MB database, 10 dirty pages ---
    Bench::new("OLD_full_db_checksum_100mb_10pages")
        .with_samples(20)
        .run(|| {
            let db = create_test_db(100 * 1024); // 100MB
            let pages = create_dirty_pages(10);
            black_box(full_db_checksum(&db, &pages, 4096))
        }),
    // --- Chain checksum with more pages (50 dirty pages) ---
    Bench::new("NEW_chain_checksum_50pages")
        .with_samples(2_000)
        .run(|| {
            let pages = create_dirty_pages(50);
            black_box(chain_checksum(0xDEADBEEF, &pages))
        }),
    // --- Chain checksum with 100 dirty pages (worst case) ---
    Bench::new("NEW_chain_checksum_100pages")
        .with_samples(1_000)
        .run(|| {
            let pages = create_dirty_pages(100);
            black_box(chain_checksum(0xDEADBEEF, &pages))
        }),
);
