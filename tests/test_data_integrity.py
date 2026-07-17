#!/usr/bin/env python3
"""
Data integrity verification for walrust syncs.
Verify that S3 data actually matches what was written.
"""

import hashlib
import os
import sqlite3
import subprocess
import sys
import tempfile
import time
from pathlib import Path


def sha256_file(path: Path) -> str:
    """Compute SHA256 of file."""
    h = hashlib.sha256()
    with open(path, 'rb') as f:
        while chunk := f.read(8192):
            h.update(chunk)
    return h.hexdigest()


def get_table_checksum(db_path: Path) -> str:
    """Get checksum of table data."""
    conn = sqlite3.connect(str(db_path))
    rows = conn.execute("SELECT * FROM test ORDER BY id").fetchall()
    conn.close()

    # Checksum of all data
    h = hashlib.sha256()
    for row in rows:
        h.update(str(row).encode())
    return h.hexdigest()


def test_single_db_integrity(bucket: str, endpoint: str = None):
    """Test that a single DB's data roundtrips correctly."""
    with tempfile.TemporaryDirectory() as tmpdir:
        tmpdir = Path(tmpdir)
        original_db = tmpdir / "original.db"
        restored_db = tmpdir / "restored.db"

        print("Creating test database...")
        conn = sqlite3.connect(str(original_db))
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("CREATE TABLE test (id INTEGER PRIMARY KEY, data TEXT)")

        # Write specific data pattern
        test_data = []
        for i in range(100):
            data = f"test_data_{i}_{'x' * 100}"
            conn.execute("INSERT INTO test (data) VALUES (?)", (data,))
            test_data.append((i + 1, data))
        conn.commit()
        conn.close()

        # Get checksums before sync
        original_checksum = get_table_checksum(original_db)
        print(f"Original data checksum: {original_checksum}")

        # Sync with walrust
        print("Syncing with walrust...")
        env = os.environ.copy()
        env["RUST_LOG"] = "walrust=info"

        cmd = [
            "./target/release/walrust", "watch",
            "--bucket", f"s3://{bucket}/integrity-test/",
            "--no-metrics",
            str(original_db)
        ]
        if endpoint:
            cmd.extend(["--endpoint", endpoint])

        proc = subprocess.Popen(cmd, env=env, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)

        # Wait for initial sync
        print("Waiting for sync...")
        sync_complete = False
        for line in proc.stdout:
            print(f"  {line.strip()}")
            if "Created initial snapshot" in line or "Synced" in line:
                sync_complete = True
                time.sleep(2)  # Extra time for upload
                break

        if not sync_complete:
            print("❌ FAILED: Sync didn't complete")
            proc.kill()
            return False

        proc.terminate()
        proc.wait(timeout=5)

        # Restore from S3
        print("\nRestoring from S3...")
        restore_cmd = [
            "./target/release/walrust", "restore",
            "original", "-o", str(restored_db),
            "--bucket", f"s3://{bucket}/integrity-test/"
        ]
        if endpoint:
            restore_cmd.extend(["--endpoint", endpoint])

        result = subprocess.run(restore_cmd, capture_output=True, text=True)
        if result.returncode != 0:
            print(f"❌ FAILED: Restore failed\n{result.stderr}")
            return False

        # Verify data
        print("Verifying restored data...")
        restored_checksum = get_table_checksum(restored_db)
        print(f"Restored data checksum: {restored_checksum}")

        if original_checksum != restored_checksum:
            print("❌ FAILED: Data checksums don't match!")

            # Debug: show differences
            orig_conn = sqlite3.connect(str(original_db))
            rest_conn = sqlite3.connect(str(restored_db))

            orig_rows = orig_conn.execute("SELECT * FROM test ORDER BY id").fetchall()
            rest_rows = rest_conn.execute("SELECT * FROM test ORDER BY id").fetchall()

            print(f"  Original rows: {len(orig_rows)}")
            print(f"  Restored rows: {len(rest_rows)}")

            if len(orig_rows) != len(rest_rows):
                print(f"  Row count mismatch!")
            else:
                for i, (o, r) in enumerate(zip(orig_rows, rest_rows)):
                    if o != r:
                        print(f"  Row {i} differs:")
                        print(f"    Original: {o}")
                        print(f"    Restored: {r}")

            orig_conn.close()
            rest_conn.close()
            return False

        print("✅ SUCCESS: Data integrity verified!")
        return True


def test_multi_db_integrity(num_dbs: int, bucket: str, endpoint: str = None):
    """Test that multiple DBs all have correct data."""
    with tempfile.TemporaryDirectory() as tmpdir:
        tmpdir = Path(tmpdir)

        print(f"\nTesting {num_dbs} databases...")

        # Create DBs with unique data
        db_checksums = {}
        db_paths = []

        for i in range(num_dbs):
            db_path = tmpdir / f"db{i:03d}.db"
            db_paths.append(db_path)

            conn = sqlite3.connect(str(db_path))
            conn.execute("PRAGMA journal_mode=WAL")
            conn.execute("CREATE TABLE test (id INTEGER PRIMARY KEY, data TEXT)")

            # Each DB has unique data pattern
            for j in range(50):
                data = f"db{i}_row{j}_{'y' * (i + 1)}"
                conn.execute("INSERT INTO test (data) VALUES (?)", (data,))
            conn.commit()
            conn.close()

            db_checksums[f"db{i:03d}"] = get_table_checksum(db_path)

        # Sync all with walrust
        print("Syncing all databases...")
        env = os.environ.copy()
        env["RUST_LOG"] = "walrust=warn"

        cmd = [
            "./target/release/walrust", "watch",
            "--bucket", f"s3://{bucket}/multi-integrity-test/",
            "--no-metrics"
        ] + [str(p) for p in db_paths]

        if endpoint:
            cmd.extend(["--endpoint", endpoint])

        proc = subprocess.Popen(cmd, env=env, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)

        # Wait for all initial syncs
        time.sleep(5)
        proc.terminate()
        proc.wait(timeout=10)

        # Restore and verify each DB
        print("Restoring and verifying each database...")
        all_valid = True

        for i in range(num_dbs):
            db_name = f"db{i:03d}"
            restored_path = tmpdir / f"restored_{db_name}.db"

            restore_cmd = [
                "./target/release/walrust", "restore",
                db_name, "-o", str(restored_path),
                "--bucket", f"s3://{bucket}/multi-integrity-test/"
            ]
            if endpoint:
                restore_cmd.extend(["--endpoint", endpoint])

            result = subprocess.run(restore_cmd, capture_output=True, text=True)
            if result.returncode != 0:
                print(f"  ❌ {db_name}: Restore failed")
                all_valid = False
                continue

            restored_checksum = get_table_checksum(restored_path)
            original_checksum = db_checksums[db_name]

            if restored_checksum != original_checksum:
                print(f"  ❌ {db_name}: Checksum mismatch!")
                print(f"     Original: {original_checksum}")
                print(f"     Restored: {restored_checksum}")
                all_valid = False
            else:
                print(f"  ✅ {db_name}: Verified")

        return all_valid


def main():
    if os.path.exists(".env"):
        with open(".env") as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith("#") and "=" in line:
                    key, val = line.split("=", 1)
                    os.environ[key] = val

    bucket = os.environ.get("WALSYNC_TEST_BUCKET", "empty-cherry-5203")
    endpoint = os.environ.get("AWS_ENDPOINT_URL_S3")

    print("\n" + "="*80)
    print("DATA INTEGRITY VERIFICATION")
    print("="*80)

    # Test 1: Single DB
    print("\n[Test 1] Single database integrity...")
    if not test_single_db_integrity(bucket, endpoint):
        print("\n❌ SINGLE DB TEST FAILED - DATA MAY BE CORRUPTED!")
        sys.exit(1)

    # Test 2: Multiple DBs
    print("\n[Test 2] Multiple database integrity...")
    if not test_multi_db_integrity(10, bucket, endpoint):
        print("\n❌ MULTI-DB TEST FAILED - DATA MAY BE CORRUPTED!")
        sys.exit(1)

    print("\n" + "="*80)
    print("✅ ALL INTEGRITY TESTS PASSED")
    print("="*80)


if __name__ == "__main__":
    main()
