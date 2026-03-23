"""Measure real walrust RSS during watch + write load."""
import subprocess
import tempfile
import sqlite3
import time
import os
import signal

WALRUST = os.path.expanduser("~/Documents/Github/personal-website/walrust/target/release/walrust")

def get_rss_mb(pid):
    """Get RSS in MB for a process."""
    try:
        out = subprocess.check_output(["ps", "-o", "rss=", "-p", str(pid)], text=True)
        return int(out.strip()) / 1024.0
    except Exception:
        return 0.0

def main():
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = os.path.join(tmpdir, "test.db")

        # Pre-populate database (simulate existing DB before walrust starts)
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

        # Start walrust watch (uses dummy bucket — uploads will fail, but we measure RSS)
        proc = subprocess.Popen(
            [WALRUST, "watch", db_path,
             "--bucket", "dummy-bucket",
             "--endpoint", "https://fly.storage.tigris.dev",
             "--no-metrics",
             "--wal-sync-interval", "1",
             "--snapshot-interval", "3600",
             "--checkpoint-interval", "60"],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )

        time.sleep(2)  # Let walrust initialize + take initial snapshot
        print(f"After init + snapshot: {get_rss_mb(proc.pid):.1f} MB")

        # Write load
        conn = sqlite3.connect(db_path)
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA wal_autocheckpoint=0")

        for batch in range(10):
            for i in range(400):
                conn.execute("INSERT OR REPLACE INTO t VALUES (?, randomblob(1024))", (i,))
            conn.commit()
            time.sleep(1)  # Let sync cycle run
            rss = get_rss_mb(proc.pid)
            print(f"After batch {batch+1} (400 writes): {rss:.1f} MB")

        # Wait a bit for any pending operations
        time.sleep(3)
        print(f"Final (idle): {get_rss_mb(proc.pid):.1f} MB")

        conn.close()
        proc.send_signal(signal.SIGTERM)
        proc.wait(timeout=5)

if __name__ == "__main__":
    main()
