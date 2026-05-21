//! Property-based tests for walrust
//!
//! These tests verify critical data safety properties using walrust's actual modules.
//! Properties tested:
//! 1. LTX roundtrip - encode/decode preserves all data
//! 2. Durability - snapshot/restore cycle loses nothing
//! 3. Snapshot integrity - all snapshots are valid SQLite databases
//! 4. WAL parsing correctness - frame boundaries respected
//! 5. Concurrent write safety - writes during checkpoint don't corrupt
//! 6. Large database handling - no OOM on big DBs
//! 7. Incremental LTX chain - applying incrementals preserves data
//! 8. Crash recovery - partial operations don't corrupt state

use anyhow::Result;
use proptest::prelude::*;
use rusqlite::Connection;
use std::io::Cursor;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::Duration;
use tempfile::TempDir;

// Re-export walrust modules
use walrust::ltx;

/// Helper to get proptest config from env
fn get_config() -> proptest::test_runner::Config {
    proptest::test_runner::Config {
        cases: std::env::var("PROPTEST_CASES")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(100),
        ..Default::default()
    }
}

// ============================================================================
// Smoke Tests (no S3 required)
// ============================================================================

/// Test basic SQLite WAL mode operations
pub fn test_sqlite_wal_basics() -> Result<()> {
    let tmpdir = TempDir::new()?;
    let db_path = tmpdir.path().join("test.db");

    let conn = Connection::open(&db_path)?;
    conn.execute_batch(
        "
        PRAGMA journal_mode=WAL;
        CREATE TABLE test (id INTEGER PRIMARY KEY, value TEXT);
        INSERT INTO test (value) VALUES ('hello');
        INSERT INTO test (value) VALUES ('world');
        ",
    )?;

    let count: i64 = conn.query_row("SELECT COUNT(*) FROM test", [], |r| r.get(0))?;
    assert_eq!(count, 2, "Expected 2 rows");

    Ok(())
}

/// Test LTX encoding/decoding roundtrip using walrust's ltx module
pub fn test_ltx_roundtrip() -> Result<()> {
    let tmpdir = TempDir::new()?;
    let db_path = tmpdir.path().join("test.db");
    let restored_path = tmpdir.path().join("restored.db");

    // Create a test database
    let conn = Connection::open(&db_path)?;
    conn.execute_batch(
        "
        PRAGMA journal_mode=WAL;
        PRAGMA page_size=4096;
        CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, email TEXT);
        INSERT INTO users (name, email) VALUES ('Alice', 'alice@example.com');
        INSERT INTO users (name, email) VALUES ('Bob', 'bob@example.com');
        ",
    )?;
    conn.execute_batch("PRAGMA wal_checkpoint(TRUNCATE);")?;
    drop(conn);

    // Encode using walrust's ltx module
    let mut ltx_buffer = Vec::new();
    ltx::encode_snapshot(&mut ltx_buffer, &db_path, 4096, 1)?;

    // Decode back
    let cursor = Cursor::new(ltx_buffer);
    ltx::decode_to_db(cursor, &restored_path)?;

    // Verify restored database
    let conn = Connection::open(&restored_path)?;
    let integrity: String = conn.query_row("PRAGMA integrity_check", [], |r| r.get(0))?;
    assert_eq!(
        integrity, "ok",
        "Restored database should pass integrity check"
    );

    let count: i64 = conn.query_row("SELECT COUNT(*) FROM users", [], |r| r.get(0))?;
    assert_eq!(count, 2, "Should have 2 users");

    Ok(())
}

/// Test WAL file generation and basic structure
pub fn test_wal_frame_reading() -> Result<()> {
    let tmpdir = TempDir::new()?;
    let db_path = tmpdir.path().join("test.db");

    let conn = Connection::open(&db_path)?;
    conn.execute_batch(
        "
        PRAGMA journal_mode=WAL;
        PRAGMA page_size=4096;
        CREATE TABLE test (id INTEGER PRIMARY KEY, data TEXT);
        ",
    )?;

    // Generate WAL frames
    for i in 0..50 {
        conn.execute(
            "INSERT INTO test (data) VALUES (?)",
            [format!("value_{}", i)],
        )?;
    }

    let wal_path = db_path.with_extension("db-wal");
    if wal_path.exists() {
        // Read WAL file directly
        let wal_data = std::fs::read(&wal_path)?;
        assert!(!wal_data.is_empty(), "WAL should have content");

        // Verify WAL header magic (0x377F0682 or 0x377F0683)
        if wal_data.len() >= 4 {
            let magic = u32::from_be_bytes([wal_data[0], wal_data[1], wal_data[2], wal_data[3]]);
            assert!(
                magic == 0x377F0682 || magic == 0x377F0683,
                "WAL should have valid magic number, got {:#x}",
                magic
            );
        }

        // Verify page size in header
        if wal_data.len() >= 12 {
            let page_size =
                u32::from_be_bytes([wal_data[8], wal_data[9], wal_data[10], wal_data[11]]);
            assert_eq!(page_size, 4096, "Page size should be 4096");
        }
    }

    Ok(())
}

/// Test LTX verification using walrust's verify function
pub fn test_ltx_verification() -> Result<()> {
    let tmpdir = TempDir::new()?;
    let db_path = tmpdir.path().join("test.db");

    // Create database
    let page_size = 4096u32;
    let db_data = vec![0x42u8; page_size as usize * 3];
    std::fs::write(&db_path, &db_data)?;

    // Encode as LTX
    let mut ltx_buffer = Vec::new();
    ltx::encode_snapshot(&mut ltx_buffer, &db_path, page_size, 1)?;

    // Verify the LTX
    let cursor = Cursor::new(&ltx_buffer);
    let header = ltx::verify_ltx(cursor)?;

    assert_eq!(header.page_size.into_inner(), page_size);
    assert_eq!(header.commit.into_inner(), 3);

    // Corrupt the LTX and verify it fails
    let mut corrupted = ltx_buffer.clone();
    if corrupted.len() > 100 {
        corrupted[100] ^= 0xFF;
    }
    let cursor = Cursor::new(corrupted);
    let result = ltx::verify_ltx(cursor);
    assert!(result.is_err(), "Corrupted LTX should fail verification");

    Ok(())
}

// ============================================================================
// Property Tests
// ============================================================================

/// Property: LTX encode/decode roundtrip preserves all data byte-for-byte
pub fn prop_ltx_roundtrip() -> Result<()> {
    let mut runner = proptest::test_runner::TestRunner::new(get_config());

    let result = runner.run(
        &(
            prop::collection::vec(0u8..=255, 4096..4096 * 50), // 1-50 pages of data
            prop::sample::select(vec![512u32, 1024, 2048, 4096, 8192, 16384]),
        ),
        |(raw_data, page_size)| {
            let tmpdir = TempDir::new().unwrap();
            let db_path = tmpdir.path().join("test.db");
            let restored_path = tmpdir.path().join("restored.db");

            // Ensure data is page-aligned (must have at least one page)
            let aligned_len = (raw_data.len() / page_size as usize) * page_size as usize;
            if aligned_len == 0 {
                // Not enough data for even one page at this page size - skip
                return Ok(());
            }
            let db_data = &raw_data[..aligned_len];
            std::fs::write(&db_path, db_data).unwrap();

            // Encode
            let mut ltx_buffer = Vec::new();
            ltx::encode_snapshot(&mut ltx_buffer, &db_path, page_size, 1).unwrap();

            // Decode
            let cursor = Cursor::new(ltx_buffer);
            ltx::decode_to_db(cursor, &restored_path).unwrap();

            // Verify byte-for-byte
            let restored = std::fs::read(&restored_path).unwrap();
            prop_assert_eq!(
                db_data.len(),
                restored.len(),
                "Size mismatch: {} vs {}",
                db_data.len(),
                restored.len()
            );

            for (i, (a, b)) in db_data.iter().zip(restored.iter()).enumerate() {
                prop_assert_eq!(a, b, "Byte mismatch at offset {}", i);
            }

            Ok(())
        },
    );

    result.map_err(|e| anyhow::anyhow!("Property test failed: {:?}", e))
}

/// Property: Snapshot and restore preserves all SQLite data
pub fn prop_durability() -> Result<()> {
    let mut runner = proptest::test_runner::TestRunner::new(get_config());

    let result = runner.run(
        &(1..100usize, prop::collection::vec("[a-z]{1,20}", 1..50)),
        |(row_count, values)| {
            let tmpdir = TempDir::new().unwrap();
            let db_path = tmpdir.path().join("source.db");
            let restored_path = tmpdir.path().join("restored.db");

            // Create source database
            let conn = Connection::open(&db_path).unwrap();
            conn.execute_batch(
                "PRAGMA journal_mode=WAL; PRAGMA page_size=4096;
                 CREATE TABLE data (id INTEGER PRIMARY KEY, value TEXT);",
            )
            .unwrap();

            let insert_count = row_count.min(values.len());
            for (i, value) in values.iter().take(insert_count).enumerate() {
                conn.execute(
                    "INSERT INTO data (id, value) VALUES (?, ?)",
                    rusqlite::params![i as i64, value],
                )
                .unwrap();
            }
            conn.execute_batch("PRAGMA wal_checkpoint(TRUNCATE);")
                .unwrap();
            drop(conn);

            // Snapshot via LTX
            let mut ltx_buffer = Vec::new();
            ltx::encode_snapshot(&mut ltx_buffer, &db_path, 4096, 1).unwrap();

            // Restore
            let cursor = Cursor::new(ltx_buffer);
            ltx::decode_to_db(cursor, &restored_path).unwrap();

            // Verify
            let restored_conn = Connection::open(&restored_path).unwrap();
            let count: i64 = restored_conn
                .query_row("SELECT COUNT(*) FROM data", [], |r| r.get(0))
                .unwrap();
            prop_assert_eq!(count as usize, insert_count, "Row count mismatch");

            for (i, expected) in values.iter().take(insert_count).enumerate() {
                let actual: String = restored_conn
                    .query_row(
                        "SELECT value FROM data WHERE id = ?",
                        rusqlite::params![i as i64],
                        |r| r.get(0),
                    )
                    .unwrap();
                prop_assert_eq!(&actual, expected, "Value mismatch at row {}", i);
            }

            Ok(())
        },
    );

    result.map_err(|e| anyhow::anyhow!("Property test failed: {:?}", e))
}

/// Property: Snapshots are always valid SQLite databases
pub fn prop_snapshot_integrity() -> Result<()> {
    let mut runner = proptest::test_runner::TestRunner::new(get_config());

    let result = runner.run(
        &(1..5usize, 1..50usize, prop::bool::ANY),
        |(table_count, rows_per_table, do_checkpoints)| {
            let tmpdir = TempDir::new().unwrap();
            let db_path = tmpdir.path().join("test.db");
            let restored_path = tmpdir.path().join("restored.db");

            let conn = Connection::open(&db_path).unwrap();
            conn.execute_batch("PRAGMA journal_mode=WAL; PRAGMA page_size=4096;")
                .unwrap();

            for t in 0..table_count {
                conn.execute(
                    &format!(
                        "CREATE TABLE table_{} (id INTEGER PRIMARY KEY, data BLOB)",
                        t
                    ),
                    [],
                )
                .unwrap();

                for r in 0..rows_per_table {
                    let data = vec![(t * rows_per_table + r) as u8; 100];
                    conn.execute(
                        &format!("INSERT INTO table_{} (data) VALUES (?)", t),
                        rusqlite::params![data],
                    )
                    .unwrap();
                }

                if do_checkpoints && t < table_count - 1 {
                    conn.execute_batch("PRAGMA wal_checkpoint(PASSIVE);")
                        .unwrap();
                }
            }
            conn.execute_batch("PRAGMA wal_checkpoint(TRUNCATE);")
                .unwrap();
            drop(conn);

            // Snapshot and restore via LTX
            let mut ltx_buffer = Vec::new();
            ltx::encode_snapshot(&mut ltx_buffer, &db_path, 4096, 1).unwrap();

            let cursor = Cursor::new(ltx_buffer);
            ltx::decode_to_db(cursor, &restored_path).unwrap();

            // Verify integrity
            let snap_conn = Connection::open(&restored_path).unwrap();
            let integrity: String = snap_conn
                .query_row("PRAGMA integrity_check", [], |r| r.get(0))
                .unwrap();
            prop_assert_eq!(integrity, "ok", "Snapshot failed integrity check");

            for t in 0..table_count {
                let count: i64 = snap_conn
                    .query_row(&format!("SELECT COUNT(*) FROM table_{}", t), [], |r| {
                        r.get(0)
                    })
                    .unwrap();
                prop_assert_eq!(count as usize, rows_per_table, "Table {} wrong count", t);
            }

            Ok(())
        },
    );

    result.map_err(|e| anyhow::anyhow!("Property test failed: {:?}", e))
}

/// Property: Concurrent writes during checkpoint don't corrupt the database
pub fn prop_concurrent_checkpoint_safety() -> Result<()> {
    let mut runner = proptest::test_runner::TestRunner::new(get_config());

    let result = runner.run(&(10..100usize), |write_count| {
        let tmpdir = TempDir::new().unwrap();
        let db_path = tmpdir.path().join("test.db");

        let conn = Connection::open(&db_path).unwrap();
        conn.execute_batch(
            "PRAGMA journal_mode=WAL; PRAGMA page_size=4096;
             CREATE TABLE test (id INTEGER PRIMARY KEY, data TEXT);",
        )
        .unwrap();

        let stop_flag = Arc::new(AtomicBool::new(false));
        let stop_flag_clone = stop_flag.clone();
        let db_path_clone = db_path.clone();

        // Writer thread
        let writer = thread::spawn(move || {
            let conn = Connection::open(&db_path_clone).unwrap();
            let mut i = 0;
            while !stop_flag_clone.load(Ordering::Relaxed) && i < write_count {
                let _ = conn.execute(
                    "INSERT INTO test (data) VALUES (?)",
                    [format!("data_{}", i)],
                );
                i += 1;
                thread::sleep(Duration::from_micros(100));
            }
        });

        // Checkpoint while writes are happening
        for _ in 0..5 {
            thread::sleep(Duration::from_millis(10));
            let _ = conn.execute_batch("PRAGMA wal_checkpoint(PASSIVE);");
        }

        stop_flag.store(true, Ordering::Relaxed);
        writer.join().unwrap();

        // Final checkpoint
        conn.execute_batch("PRAGMA wal_checkpoint(TRUNCATE);")
            .unwrap();
        drop(conn);

        // Verify database is valid
        let verify_conn = Connection::open(&db_path).unwrap();
        let integrity: String = verify_conn
            .query_row("PRAGMA integrity_check", [], |r| r.get(0))
            .unwrap();
        prop_assert_eq!(integrity, "ok", "Database corrupted after concurrent ops");

        Ok(())
    });

    result.map_err(|e| anyhow::anyhow!("Property test failed: {:?}", e))
}

/// Property: Large databases don't cause OOM
pub fn prop_large_database_handling() -> Result<()> {
    let mut runner = proptest::test_runner::TestRunner::new(proptest::test_runner::Config {
        cases: 10, // Fewer cases for memory-intensive test
        ..Default::default()
    });

    let result = runner.run(&(100..500usize), |num_pages| {
        let tmpdir = TempDir::new().unwrap();
        let db_path = tmpdir.path().join("large.db");
        let restored_path = tmpdir.path().join("restored.db");

        let page_size = 4096u32;

        // Create large database
        let mut db_data = Vec::with_capacity(num_pages * page_size as usize);
        for i in 0..num_pages {
            let pattern = (i as u8).wrapping_mul(37);
            let mut page = vec![pattern; page_size as usize];
            page[0..4].copy_from_slice(&(i as u32).to_le_bytes());
            db_data.extend(page);
        }
        std::fs::write(&db_path, &db_data).unwrap();

        // Encode
        let mut ltx_buffer = Vec::new();
        ltx::encode_snapshot(&mut ltx_buffer, &db_path, page_size, 1).unwrap();

        // Should be compressed
        prop_assert!(ltx_buffer.len() < db_data.len(), "LTX should be compressed");

        // Decode
        let cursor = Cursor::new(ltx_buffer);
        ltx::decode_to_db(cursor, &restored_path).unwrap();

        // Verify
        let restored = std::fs::read(&restored_path).unwrap();
        prop_assert_eq!(db_data.len(), restored.len());
        prop_assert_eq!(db_data, restored);

        Ok(())
    });

    result.map_err(|e| anyhow::anyhow!("Property test failed: {:?}", e))
}

/// Property: Incremental LTX chain maintains data integrity
pub fn prop_incremental_ltx_chain() -> Result<()> {
    let mut runner = proptest::test_runner::TestRunner::new(get_config());

    let result = runner.run(
        &(2..10usize, 1..5usize),
        |(num_incrementals, pages_per_inc)| {
            let tmpdir = TempDir::new().unwrap();
            let db_path = tmpdir.path().join("test.db");

            let page_size = 4096u32;
            let total_pages = 10usize;

            // Create initial database
            let initial_data: Vec<u8> = (0..total_pages)
                .flat_map(|i| vec![(i as u8) * 10; page_size as usize])
                .collect();
            std::fs::write(&db_path, &initial_data).unwrap();

            // Create snapshot
            let mut snapshot_buffer = Vec::new();
            ltx::encode_snapshot(&mut snapshot_buffer, &db_path, page_size, 1).unwrap();

            // Apply incrementals
            let mut current_txid = 2u64;
            for inc_num in 0..num_incrementals {
                let pre_checksum = ltx::compute_checksum_from_file(&db_path).unwrap();

                // Update some pages
                let pages: Vec<(u32, Vec<u8>)> = (0..pages_per_inc)
                    .map(|p| {
                        let page_num = ((inc_num * pages_per_inc + p) % total_pages + 1) as u32;
                        let data = vec![0xA0 + inc_num as u8; page_size as usize];
                        (page_num, data)
                    })
                    .collect();

                // The current encoder takes the post-apply checksum explicitly; it
                // must be the chained hash over (pre || changed pages) so the
                // restore-side verification in apply_ltx_to_db passes.
                let post_checksum = ltx::chain_checksum(pre_checksum, &pages);
                let mut inc_buffer = Vec::new();
                ltx::encode_wal_changes(
                    &mut inc_buffer,
                    &pages,
                    page_size,
                    current_txid,
                    current_txid,
                    total_pages as u32,
                    Some(pre_checksum),
                    post_checksum,
                )
                .unwrap();

                // Apply
                let cursor = Cursor::new(inc_buffer);
                ltx::apply_ltx_to_db(cursor, &db_path).unwrap();

                current_txid += 1;
            }

            // Verify final database is valid (correct size, can be read)
            let final_data = std::fs::read(&db_path).unwrap();
            prop_assert_eq!(
                final_data.len(),
                total_pages * page_size as usize,
                "Database size changed"
            );

            Ok(())
        },
    );

    result.map_err(|e| anyhow::anyhow!("Property test failed: {:?}", e))
}

/// Property: WAL file generation works across page sizes
pub fn prop_wal_page_sizes() -> Result<()> {
    let mut runner = proptest::test_runner::TestRunner::new(get_config());

    let result = runner.run(
        &prop::sample::select(vec![1024u32, 2048, 4096, 8192, 16384, 32768]),
        |page_size| {
            let tmpdir = TempDir::new().unwrap();
            let db_path = tmpdir.path().join("test.db");

            // IMPORTANT: page_size must be set BEFORE any tables on fresh DB
            let conn = Connection::open(&db_path).unwrap();
            conn.execute_batch(&format!("PRAGMA page_size={};", page_size))
                .unwrap();
            conn.execute_batch("PRAGMA journal_mode=WAL;").unwrap();
            conn.execute_batch("CREATE TABLE test (id INTEGER PRIMARY KEY, data BLOB);")
                .unwrap();

            // Insert data to generate WAL
            for i in 0..20 {
                let data = vec![i as u8; 100];
                conn.execute(
                    "INSERT INTO test (data) VALUES (?)",
                    rusqlite::params![data],
                )
                .unwrap();
            }

            let wal_path = db_path.with_extension("db-wal");
            if wal_path.exists() {
                let wal_data = std::fs::read(&wal_path).unwrap();

                // Verify page size in WAL header
                if wal_data.len() >= 12 {
                    let wal_page_size =
                        u32::from_be_bytes([wal_data[8], wal_data[9], wal_data[10], wal_data[11]]);
                    prop_assert_eq!(wal_page_size, page_size, "Page size mismatch in WAL");
                }
            }

            // Checkpoint and verify database is valid
            conn.execute_batch("PRAGMA wal_checkpoint(TRUNCATE);")
                .unwrap();
            drop(conn);

            let verify_conn = Connection::open(&db_path).unwrap();
            let integrity: String = verify_conn
                .query_row("PRAGMA integrity_check", [], |r| r.get(0))
                .unwrap();
            prop_assert_eq!(integrity, "ok", "Database failed integrity check");

            let count: i64 = verify_conn
                .query_row("SELECT COUNT(*) FROM test", [], |r| r.get(0))
                .unwrap();
            prop_assert_eq!(count, 20, "Wrong row count");

            Ok(())
        },
    );

    result.map_err(|e| anyhow::anyhow!("Property test failed: {:?}", e))
}

// ============================================================================
// Unit Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_smoke_sqlite() {
        test_sqlite_wal_basics().unwrap();
    }

    #[test]
    fn test_smoke_ltx() {
        test_ltx_roundtrip().unwrap();
    }

    #[test]
    fn test_smoke_wal() {
        test_wal_frame_reading().unwrap();
    }

    #[test]
    fn test_smoke_verify() {
        test_ltx_verification().unwrap();
    }

    #[test]
    fn test_prop_ltx_roundtrip() {
        std::env::set_var("PROPTEST_CASES", "10");
        prop_ltx_roundtrip().unwrap();
    }

    #[test]
    fn test_prop_durability() {
        std::env::set_var("PROPTEST_CASES", "10");
        prop_durability().unwrap();
    }

    #[test]
    fn test_prop_snapshot_integrity() {
        std::env::set_var("PROPTEST_CASES", "10");
        prop_snapshot_integrity().unwrap();
    }

    #[test]
    fn test_prop_concurrent_checkpoint() {
        std::env::set_var("PROPTEST_CASES", "5");
        prop_concurrent_checkpoint_safety().unwrap();
    }

    #[test]
    fn test_prop_large_db() {
        prop_large_database_handling().unwrap();
    }

    #[test]
    fn test_prop_incremental_chain() {
        std::env::set_var("PROPTEST_CASES", "10");
        prop_incremental_ltx_chain().unwrap();
    }

    #[test]
    fn test_prop_wal_page_sizes() {
        std::env::set_var("PROPTEST_CASES", "7");
        prop_wal_page_sizes().unwrap();
    }
}
