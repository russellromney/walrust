//! RSS profiling: realistic sync simulation + allocator behavior test.
//! Run: cargo run --example profile_rss --release

use std::process;
use std::collections::HashMap;

fn rss_mb() -> f64 {
    let pid = process::id();
    let output = process::Command::new("ps")
        .args(["-o", "rss=", "-p", &pid.to_string()])
        .output()
        .unwrap();
    let rss_kb: f64 = String::from_utf8_lossy(&output.stdout)
        .trim()
        .parse()
        .unwrap_or(0.0);
    rss_kb / 1024.0
}

fn stage(label: &str, baseline: f64) {
    let now = rss_mb();
    println!("{:>55}: {:6.1} MB  (+{:.1})", label, now, now - baseline);
}

#[tokio::main]
async fn main() {
    println!("=== Component initialization ===\n");

    let base = rss_mb();
    println!("{:>55}: {:6.1} MB", "tokio runtime", base);

    let config = aws_config::from_env().load().await;
    stage("aws_config load", base);

    use aws_smithy_runtime::client::http::hyper_014::HyperClientBuilder;
    let _http = HyperClientBuilder::new().build_https();
    let _s3 = aws_sdk_s3::Client::new(&config);
    stage("S3 Client", base);

    let tmp = std::env::temp_dir().join("profile_rss_test.db");
    let _ = std::fs::remove_file(&tmp);
    let _ = std::fs::remove_file(tmp.with_extension("db-wal"));
    let _ = std::fs::remove_file(tmp.with_extension("db-shm"));

    let db = rusqlite::Connection::open(&tmp).unwrap();
    db.execute_batch(
        "PRAGMA journal_mode=WAL; PRAGMA wal_autocheckpoint=0;
         CREATE TABLE IF NOT EXISTS t(id INTEGER PRIMARY KEY, v BLOB);",
    ).unwrap();

    let blocker = rusqlite::Connection::open(&tmp).unwrap();
    blocker.execute_batch(
        "PRAGMA journal_mode=WAL; PRAGMA wal_autocheckpoint=0;
         BEGIN DEFERRED; SELECT COUNT(*) FROM sqlite_master;",
    ).unwrap();
    stage("SQLite DB + checkpoint blocker", base);

    let _pool = rayon::ThreadPoolBuilder::new().num_threads(4).build().unwrap();
    let _reg = prometheus::Registry::new();
    let _req = reqwest::Client::new();
    stage("rayon + prometheus + reqwest", base);

    let init = rss_mb();
    println!("\n{:>55}: {:6.1} MB\n", "INIT COMPLETE", init);

    // ================================================================
    println!("=== Simulate initial snapshot (reads entire DB) ===\n");

    // Pre-populate DB with 10K rows (simulating a real database)
    for i in 0..10_000 {
        db.execute("INSERT INTO t VALUES (?1, randomblob(1024))", [i as i64]).unwrap();
    }

    // Checkpoint to flush WAL into DB (simulating existing DB before walrust starts)
    blocker.execute_batch("ROLLBACK;").unwrap();
    db.execute_batch("PRAGMA wal_checkpoint(TRUNCATE);").unwrap();
    blocker.execute_batch("BEGIN DEFERRED; SELECT COUNT(*) FROM sqlite_master;").unwrap();

    let db_size = std::fs::metadata(&tmp).unwrap().len();
    println!("{:>55}: {:.1} MB", "DB file size", db_size as f64 / 1024.0 / 1024.0);

    // Simulate initial snapshot: read entire DB + encode
    let db_data = std::fs::read(&tmp).unwrap();
    stage(&format!("Read entire DB ({:.1} MB)", db_data.len() as f64 / 1024.0 / 1024.0), base);

    // Simulate LTX encoding buffer (roughly same size as DB)
    let ltx_buf: Vec<u8> = vec![0u8; db_data.len()];
    stage("Alloc LTX buffer (DB-sized)", base);

    // Both held simultaneously during encode (this is the peak!)
    let peak_snapshot = rss_mb();
    println!("\n{:>55}: {:6.1} MB  ← THIS IS THE PEAK", "Peak during snapshot", peak_snapshot);

    // Drop both
    drop(ltx_buf);
    drop(db_data);
    stage("After dropping snapshot buffers", base);

    // ================================================================
    println!("\n=== Realistic sync cycles (offset-based, only new frames) ===\n");
    println!("  Real walrust reads only NEW frames since last sync offset\n");

    let wal_path = tmp.with_extension("db-wal");
    let page_size: usize = 4096;
    let frame_size: usize = 24 + page_size;
    let mut wal_offset: u64 = 0;

    for cycle in 1..=20 {
        // 400 writes
        for i in 0..400 {
            db.execute("INSERT OR REPLACE INTO t VALUES (?1, randomblob(1024))", [i as i64]).unwrap();
        }

        // Read only NEW frames from WAL (using offset, like real walrust)
        let wal_size = std::fs::metadata(&wal_path).map(|m| m.len()).unwrap_or(0);
        let read_start = if wal_offset == 0 { 32 } else { wal_offset }; // skip header on first read

        if wal_size > read_start {
            let mut file = std::fs::File::open(&wal_path).unwrap();
            use std::io::{Read, Seek, SeekFrom};
            file.seek(SeekFrom::Start(read_start)).unwrap();

            let bytes_to_read = (wal_size - read_start) as usize;
            let mut buf = vec![0u8; bytes_to_read];
            file.read_exact(&mut buf).unwrap();

            // Dedup into page_map
            let mut page_map: HashMap<u32, Vec<u8>> = HashMap::new();
            let num_frames = bytes_to_read / frame_size;
            for i in 0..num_frames {
                let off = i * frame_size;
                if off + frame_size > buf.len() { break; }
                let pn = u32::from_be_bytes(buf[off..off+4].try_into().unwrap());
                page_map.insert(pn, buf[off+24..off+24+page_size].to_vec());
            }
            let unique = page_map.len();

            // Simulate LTX encode buffer
            let ltx: Vec<u8> = vec![0u8; unique * page_size];

            // Drop all
            drop(ltx);
            drop(page_map);
            drop(buf);

            wal_offset = wal_size;

            if cycle <= 3 || cycle % 5 == 0 {
                let now = rss_mb();
                println!(
                    "  cycle {:>2}: {:5.1} MB  (read {:.1} MB new, {} frames → {} unique)",
                    cycle, now,
                    bytes_to_read as f64 / 1024.0 / 1024.0,
                    num_frames, unique
                );
            }
        }

        // Periodic checkpoint (every 5 cycles, like real walrust)
        if cycle % 5 == 0 {
            blocker.execute_batch("ROLLBACK;").unwrap();
            db.execute_batch("PRAGMA wal_checkpoint(PASSIVE);").unwrap();
            blocker.execute_batch("BEGIN DEFERRED; SELECT COUNT(*) FROM sqlite_master;").unwrap();
            wal_offset = 0; // reset after checkpoint
            if cycle % 5 == 0 {
                let wal_after = std::fs::metadata(&wal_path).map(|m| m.len()).unwrap_or(0);
                println!("  checkpoint: WAL {:.1} MB → {:.1} MB",
                    wal_size as f64 / 1024.0 / 1024.0,
                    wal_after as f64 / 1024.0 / 1024.0);
            }
        }
    }

    // ================================================================
    println!("\n=== Summary ===\n");
    println!("{:>55}: {:6.1} MB", "Init RSS", init);
    println!("{:>55}: {:6.1} MB", "Peak (during initial snapshot)", peak_snapshot);
    println!("{:>55}: {:6.1} MB", "Final RSS after 20 sync cycles", rss_mb());
    println!("{:>55}: {:6.1} MB", "Allocator retained (final - init)", rss_mb() - init);
    println!();
    println!("  Conclusion: RSS ≈ init + peak_alloc because macOS allocator");
    println!("  never returns freed memory. The peak is set by the initial");
    println!("  snapshot which reads the entire DB ({:.1} MB) + encodes it.", db_size as f64 / 1024.0 / 1024.0);

    // Cleanup
    drop(blocker);
    drop(db);
    let _ = std::fs::remove_file(&tmp);
    let _ = std::fs::remove_file(tmp.with_extension("db-wal"));
    let _ = std::fs::remove_file(tmp.with_extension("db-shm"));
}
