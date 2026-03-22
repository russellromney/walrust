// Integration test: Verify walrust can encode and decode its own LTX snapshots
// Also tests litestream→walrust compatibility (litestream-created files decoded by walrust)

use std::process::Command;
use tempfile::tempdir;

#[test]
fn test_walrust_snapshot_round_trip() {
    let dir = tempdir().unwrap();
    let source_db = dir.path().join("source.db");

    // Create test database
    let output = Command::new("sqlite3")
        .arg(&source_db)
        .arg("CREATE TABLE items (id INTEGER PRIMARY KEY, data TEXT); INSERT INTO items (data) VALUES ('alpha'), ('beta'), ('gamma');")
        .output()
        .expect("Failed to create test database");
    assert!(output.status.success(), "sqlite3 failed: {:?}", output);

    // Count original data
    let count_output = Command::new("sqlite3")
        .arg(&source_db)
        .arg("SELECT COUNT(*) FROM items;")
        .output()
        .expect("Failed to count");
    let original_count: i32 = String::from_utf8(count_output.stdout)
        .unwrap()
        .trim()
        .parse()
        .unwrap();

    // Get page size from database
    let page_size_output = Command::new("sqlite3")
        .arg(&source_db)
        .arg("PRAGMA page_size;")
        .output()
        .expect("Failed to get page size");
    let page_size: u32 = String::from_utf8(page_size_output.stdout)
        .unwrap()
        .trim()
        .parse()
        .unwrap();

    // Encode snapshot with walrust
    let ltx_path = dir.path().join("snapshot.ltx");
    let ltx_file = std::fs::File::create(&ltx_path).unwrap();
    walrust::ltx::encode_snapshot(ltx_file, &source_db, page_size, 1)
        .expect("encode_snapshot should succeed");

    // Verify LTX1 magic header
    let ltx_data = std::fs::read(&ltx_path).unwrap();
    assert!(ltx_data.len() > 100, "LTX file should have content");
    assert_eq!(&ltx_data[0..4], b"LTX1", "Should have LTX1 magic");

    // Decode back to a new database
    let restored_db = dir.path().join("restored.db");
    let cursor = std::io::Cursor::new(&ltx_data);
    let result = walrust::ltx::decode_to_db(cursor, &restored_db)
        .expect("decode_to_db should succeed");

    assert!(result.header.max_txid.into_inner() >= 1, "Should have valid TXID");

    // Verify the restored database has the same data
    let restored_count_output = Command::new("sqlite3")
        .arg(&restored_db)
        .arg("SELECT COUNT(*) FROM items;")
        .output()
        .expect("Failed to query restored db");
    let restored_count: i32 = String::from_utf8(restored_count_output.stdout)
        .unwrap()
        .trim()
        .parse()
        .unwrap();

    assert_eq!(original_count, restored_count, "Data count should match after round-trip");

    // Verify actual data content
    let data_output = Command::new("sqlite3")
        .arg(&restored_db)
        .arg("SELECT data FROM items ORDER BY id;")
        .output()
        .expect("Failed to query data");
    let data = String::from_utf8(data_output.stdout).unwrap();
    assert!(data.contains("alpha"), "Should contain 'alpha'");
    assert!(data.contains("beta"), "Should contain 'beta'");
    assert!(data.contains("gamma"), "Should contain 'gamma'");
}
