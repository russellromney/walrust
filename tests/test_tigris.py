#!/usr/bin/env python3
"""Quick test to verify walrust sync with Tigris (real S3)."""

import os
import sqlite3
import subprocess
import sys
import tempfile
import threading
import time
import signal

def main():
    # Load env
    with open(".env") as f:
        for line in f:
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                key, val = line.split("=", 1)
                os.environ[key] = val

    bucket = os.environ.get("S3_TEST_BUCKET", "empty-cherry-5203")

    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = os.path.join(tmpdir, "test.db")

        # Create a SQLite database with WAL mode
        print(f"Creating database at {db_path}")

        # Keep a reader connection open to prevent WAL checkpoint on close
        reader_conn = sqlite3.connect(db_path)
        reader_conn.execute("PRAGMA journal_mode=WAL")
        reader_conn.execute("PRAGMA wal_autocheckpoint=0")
        reader_conn.execute("CREATE TABLE test (id INTEGER PRIMARY KEY, data TEXT)")
        reader_conn.commit()

        # Write some data to trigger WAL
        writer_conn = sqlite3.connect(db_path)
        writer_conn.execute("PRAGMA wal_autocheckpoint=0")
        for i in range(10):
            writer_conn.execute("INSERT INTO test (data) VALUES (?)", (f"data_{i}",))
        writer_conn.commit()
        # Don't close writer_conn yet - WAL file will persist

        # Verify WAL file exists
        wal_path = db_path + "-wal"
        shm_path = db_path + "-shm"
        wal_exists = os.path.exists(wal_path)
        shm_exists = os.path.exists(shm_path)
        wal_size = os.path.getsize(wal_path) if wal_exists else 0
        print(f"Database created")
        print(f"  WAL file exists: {wal_exists}, size: {wal_size}")
        print(f"  SHM file exists: {shm_exists}")
        print(f"Bucket: {bucket}")
        print()

        # Run walrust with independent tasks mode
        env = os.environ.copy()
        env["RUST_LOG"] = "walrust=trace"

        cmd = [
            "./target/release/walrust",
            "watch",
            "--bucket", f"s3://{bucket}/tigris-test/",
            "--independent-tasks",
            "--no-metrics",
            db_path,
        ]

        print(f"Running: {' '.join(cmd)}")
        print()

        # Start walrust
        proc = subprocess.Popen(
            cmd,
            env=env,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
        )

        def read_output():
            for line in proc.stdout:
                print(line, end="")

        output_thread = threading.Thread(target=read_output, daemon=True)
        output_thread.start()

        # Wait for initial sync
        time.sleep(2)

        # Write more data in batches to trigger multiple syncs
        for batch in range(3):
            print(f"\n--- Writing batch {batch+1} ---\n")
            for i in range(100):
                writer_conn.execute("INSERT INTO test (data) VALUES (?)", (f"batch{batch}_data_{i}" * 100,))
            writer_conn.commit()
            # Force fsync to flush changes
            time.sleep(2)

        # Wait for final sync
        time.sleep(2)

        # Close connections before cleanup
        writer_conn.close()
        reader_conn.close()

        # Stop walrust
        print("\n--- Stopping walrust ---\n")
        proc.send_signal(signal.SIGINT)
        proc.wait(timeout=5)

        print("\nTest completed!")

if __name__ == "__main__":
    main()
