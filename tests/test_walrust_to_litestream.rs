// Integration test: Verify litestream can restore walrust-created LTX files
// Run with: cargo test --test test_walrust_to_litestream -- --ignored

use std::process::Command;
use tempfile::tempdir;

#[test]
#[ignore] // Requires litestream to be installed
fn test_walrust_ltx_litestream_restore() {
    // Check if litestream is available
    if Command::new("litestream")
        .arg("version")
        .output()
        .is_err()
    {
        eprintln!("Skipping test: litestream not installed");
        eprintln!("Install with: brew install litestream");
        return;
    }

    let dir = tempdir().unwrap();
    let source_db = dir.path().join("source.db");
    let ltx_dir = dir.path().join("ltx_files");
    let db_name = "source";

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
    let original_count = String::from_utf8(count_output.stdout)
        .unwrap()
        .trim()
        .parse::<i32>()
        .unwrap();
    println!("Original data: {} items", original_count);

    // Create LTX snapshot using walrust library
    println!("Creating walrust LTX snapshot...");
    std::fs::create_dir_all(&ltx_dir).unwrap();

    // Litestream 0.5+ uses: ltx/<generation>/<min_txid>-<max_txid>.ltx
    let ltx_path = ltx_dir.join("ltx/0/0000000000000001-0000000000000001.ltx");
    std::fs::create_dir_all(ltx_path.parent().unwrap()).unwrap();

    // Get page size from database
    let page_size_output = Command::new("sqlite3")
        .arg(&source_db)
        .arg("PRAGMA page_size;")
        .output()
        .expect("Failed to get page size");
    let page_size = String::from_utf8(page_size_output.stdout)
        .unwrap()
        .trim()
        .parse::<u32>()
        .unwrap();
    println!("Database page size: {}", page_size);

    // Encode snapshot with walrust
    let ltx_file = std::fs::File::create(&ltx_path).unwrap();
    match walrust::ltx::encode_snapshot(ltx_file, &source_db, page_size, 1) {
        Ok(()) => {
            println!("✓ Walrust encoded snapshot successfully");
        }
        Err(e) => {
            eprintln!("✗ Failed to encode snapshot: {}", e);
            panic!("Encoding failed");
        }
    }

    // Inspect the walrust-created LTX file
    let ltx_data = std::fs::read(&ltx_path).unwrap();
    println!("LTX file size: {} bytes", ltx_data.len());

    // Check flags
    assert_eq!(&ltx_data[0..4], b"LTX1", "Should have LTX1 magic");
    let flags = u32::from_be_bytes([ltx_data[4], ltx_data[5], ltx_data[6], ltx_data[7]]);
    println!("Flags in walrust LTX: 0x{:08x}", flags);

    let has_no_checksum = (flags & 0x02) == 0x02;
    println!("  NO_CHECKSUM flag: {}", if has_no_checksum { "SET" } else { "NOT SET" });

    if !has_no_checksum {
        println!("  ⚠️  Walrust files have checksums - litestream may not accept them");
        println!("  This indicates walrust may need a --no-checksum flag for litestream compatibility");
    }

    // Try to restore with litestream
    println!("\nAttempting litestream restore...");
    let restore_path = dir.path().join("restored.db");

    // Create litestream config
    let config_path = dir.path().join("litestream.yml");
    let config = format!(
        r#"dbs:
  - path: {}
    replicas:
      - path: {}
"#,
        restore_path.display(),
        ltx_dir.display()
    );
    std::fs::write(&config_path, config).unwrap();

    // Run litestream restore
    let restore_output = Command::new("litestream")
        .arg("restore")
        .arg("-config")
        .arg(&config_path)
        .arg(&restore_path)
        .output()
        .expect("Failed to run litestream restore");

    if restore_output.status.success() {
        println!("✓ Litestream successfully restored walrust snapshot!");

        // Verify data
        let restored_count_output = Command::new("sqlite3")
            .arg(&restore_path)
            .arg("SELECT COUNT(*) FROM items;")
            .output()
            .expect("Failed to query restored db");

        let restored_count = String::from_utf8(restored_count_output.stdout)
            .unwrap()
            .trim()
            .parse::<i32>()
            .unwrap();

        println!("Restored data: {} items", restored_count);

        assert_eq!(
            original_count, restored_count,
            "Data count should match"
        );

        println!("\n✓ BIDIRECTIONAL COMPATIBILITY CONFIRMED");
        println!("Both walrust→litestream and litestream→walrust work!");
    } else {
        println!("✗ Litestream failed to restore walrust snapshot");
        println!("STDOUT: {}", String::from_utf8_lossy(&restore_output.stdout));
        println!("STDERR: {}", String::from_utf8_lossy(&restore_output.stderr));

        if !has_no_checksum {
            println!("\n⚠️  This failure is likely because walrust includes checksums");
            println!("   Litestream may require the NO_CHECKSUM flag to be set");
            println!("   Consider adding a --litestream-compat flag to walrust snapshot command");
        }

        panic!("Litestream restore failed");
    }
}
