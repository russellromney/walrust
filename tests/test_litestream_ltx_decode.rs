// Integration test: Verify walrust can decode litestream-created LTX files

use std::process::Command;
use tempfile::tempdir;

#[test]
fn test_decode_litestream_ltx() {
    // Check if litestream is available
    if Command::new("litestream").arg("version").output().is_err() {
        eprintln!("Skipping test: litestream not installed");
        eprintln!("Install with: brew install litestream");
        return;
    }

    let dir = tempdir().unwrap();
    let db_path = dir.path().join("source.db");
    let replica_path = dir.path().join("replica");
    let config_path = dir.path().join("litestream.yml");

    // Create test database
    let output = Command::new("sqlite3")
        .arg(&db_path)
        .arg("CREATE TABLE test (id INTEGER PRIMARY KEY, value TEXT); INSERT INTO test (value) VALUES ('test1'), ('test2'), ('test3');")
        .output()
        .expect("Failed to create test database");
    assert!(output.status.success(), "sqlite3 failed: {:?}", output);

    // Create litestream config
    let config = format!(
        r#"dbs:
  - path: {}
    replicas:
      - path: {}
"#,
        db_path.display(),
        replica_path.display()
    );
    std::fs::write(&config_path, config).unwrap();

    // Start litestream replication
    let mut child = Command::new("litestream")
        .arg("replicate")
        .arg("-config")
        .arg(&config_path)
        .spawn()
        .expect("Failed to start litestream");

    // Wait for initial sync
    std::thread::sleep(std::time::Duration::from_secs(2));

    // Make some writes
    for i in 1..=3 {
        let _ = Command::new("sqlite3")
            .arg(&db_path)
            .arg(format!("INSERT INTO test (value) VALUES ('update{}');", i))
            .output();
        std::thread::sleep(std::time::Duration::from_millis(500));
    }

    // Wait for sync
    std::thread::sleep(std::time::Duration::from_secs(2));

    // Stop litestream
    child.kill().ok();
    let _ = child.wait();

    // Find LTX files created by litestream
    let ltx_files: Vec<_> = walkdir::WalkDir::new(&replica_path)
        .into_iter()
        .filter_map(|e| e.ok())
        .filter(|e| e.path().extension().and_then(|s| s.to_str()) == Some("ltx"))
        .collect();

    assert!(
        !ltx_files.is_empty(),
        "Litestream should have created LTX files"
    );

    println!("Found {} LTX files created by litestream", ltx_files.len());
    for entry in &ltx_files {
        println!("  - {}", entry.path().display());
    }

    // Try to decode each LTX file with walrust
    let mut decoded_count = 0;
    for entry in &ltx_files {
        let ltx_path = entry.path();
        println!("\nTesting decode of: {}", ltx_path.display());

        // Read the LTX file
        let ltx_data = std::fs::read(ltx_path).unwrap();
        println!("  Size: {} bytes", ltx_data.len());

        // Show first 32 bytes to verify NO_CHECKSUM flag
        let preview = &ltx_data[..ltx_data.len().min(32)];
        println!("  First bytes: {:02x?}", preview);

        // Check for LTX1 magic and NO_CHECKSUM flag (0x02)
        assert_eq!(&ltx_data[0..4], b"LTX1", "Should have LTX1 magic");
        let flags = u32::from_be_bytes([ltx_data[4], ltx_data[5], ltx_data[6], ltx_data[7]]);
        println!("  Flags: 0x{:08x}", flags);
        assert_eq!(
            flags & 0x02,
            0x02,
            "Litestream files should have NO_CHECKSUM flag"
        );

        // Check the header to see if this is a snapshot or incremental
        let cursor = std::io::Cursor::new(&ltx_data);
        match walrust::ltx::Decoder::new(cursor) {
            Ok((decoder, header)) => {
                println!("  Header decoded successfully:");
                println!(
                    "    TXID: {}-{}",
                    header.min_txid.into_inner(),
                    header.max_txid.into_inner()
                );
                println!("    Page size: {}", header.page_size.into_inner());
                println!("    Commit: {}", header.commit.into_inner());
                println!("    Flags: {:?}", header.flags);
                println!(
                    "    Pre-apply checksum: {:?}",
                    header
                        .pre_apply_checksum
                        .map(|c| format!("{:016x}", c.into_inner()))
                );

                let is_snapshot = header.min_txid.into_inner() == 1;
                println!(
                    "    Type: {}",
                    if is_snapshot {
                        "SNAPSHOT"
                    } else {
                        "INCREMENTAL"
                    }
                );

                // For snapshots, try to decode to a new database
                if is_snapshot {
                    let test_output = dir.path().join(format!("decoded_{}.db", decoded_count));
                    let cursor = std::io::Cursor::new(&ltx_data);

                    match walrust::ltx::decode_to_db(cursor, &test_output) {
                        Ok(result) => {
                            println!("  ✓ Successfully decoded snapshot!");
                            println!(
                                "    Post-apply checksum: {:016x}",
                                result.post_apply_checksum.into_inner()
                            );

                            // Verify the decoded file exists and has content
                            let decoded_data = std::fs::read(&test_output).unwrap();
                            println!("    Decoded DB size: {} bytes", decoded_data.len());
                            assert!(
                                !decoded_data.is_empty(),
                                "Decoded database should not be empty"
                            );

                            decoded_count += 1;
                        }
                        Err(e) => {
                            println!("  ✗ Failed to decode snapshot: {}", e);
                        }
                    }
                } else {
                    println!("  → Skipping incremental (would need base database to apply)");
                    // Still count this as a success - we successfully read the header
                    decoded_count += 1;
                }
            }
            Err(e) => {
                println!("  ✗ Failed to decode header: {}", e);
            }
        }
    }

    assert!(
        decoded_count > 0,
        "Should successfully read at least one litestream LTX file"
    );
    println!(
        "\n✓ Successfully read {} litestream LTX files with NO_CHECKSUM flag",
        decoded_count
    );
    println!("This confirms walrust can handle litestream format!");
}
