"""Measure real walrust RSS with actual S3 uploads."""
import subprocess
import tempfile
import sqlite3
import time
import os
import signal

WALRUST = os.path.expanduser("~/Documents/Github/personal-website/walrust/target/release/walrust")
BUCKET = os.environ.get("WALSYNC_TEST_BUCKET", "walrust-test-rr-2026")

def get_rss_mb(pid):
    try:
        out = subprocess.check_output(["ps", "-o", "rss=", "-p", str(pid)], text=True)
        return int(out.strip()) / 1024.0
    except Exception:
        return 0.0

def main():
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = os.path.join(tmpdir, "rss_test.db")

        # Pre-populate 10K rows (~13 MB DB)
        conn = sqlite3.connect(db_path)
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA wal_autocheckpoint=0")
        conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, v BLOB)")
        for i in range(10000):
            conn.execute("INSERT INTO t VALUES (?, randomblob(1024))", (i,))
        conn.commit()
        conn.close()

        db_size = os.path.getsize(db_path)
        print(f"DB size: {db_size / 1024 / 1024:.1f} MB")
        print(f"Bucket: {BUCKET}")

        # Start walrust with real S3
        proc = subprocess.Popen(
            [WALRUST, "watch", db_path,
             "--bucket", f"{BUCKET}/rss_test",
             "--no-metrics",
             "--wal-sync-interval", "1",
             "--snapshot-interval", "3600",
             "--checkpoint-interval", "30"],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )

        time.sleep(3)  # Init + snapshot upload
        print(f"After init + S3 snapshot: {get_rss_mb(proc.pid):.1f} MB")

        # Write 4000 rows in 10 batches, measuring RSS
        conn = sqlite3.connect(db_path)
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA wal_autocheckpoint=0")

        for batch in range(10):
            for i in range(400):
                conn.execute("INSERT OR REPLACE INTO t VALUES (?, randomblob(1024))", (i,))
            conn.commit()
            time.sleep(1.5)  # Sync cycle + S3 upload
            print(f"Batch {batch+1}: {get_rss_mb(proc.pid):.1f} MB")

        time.sleep(3)
        print(f"Final idle: {get_rss_mb(proc.pid):.1f} MB")

        conn.close()
        proc.send_signal(signal.SIGTERM)
        proc.wait(timeout=5)

if __name__ == "__main__":
    main()
