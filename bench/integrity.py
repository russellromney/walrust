#!/usr/bin/env python3
"""
Data integrity verification for multiple databases.
Ensures every database roundtrips correctly through S3.
"""

import argparse
import hashlib
import os
import sqlite3
import subprocess
import sys
import tempfile
import time
from dataclasses import dataclass
from pathlib import Path

# Add parent dir to path for lib imports
sys.path.insert(0, str(Path(__file__).parent))
from lib.config import get_config


@dataclass
class IntegrityResult:
    db_name: str
    original_checksum: str
    restored_checksum: str
    match: bool
    error: str = ""


def create_test_db(path: Path, db_id: int) -> str:
    """Create a test database with unique data and return checksum."""
    conn = sqlite3.connect(str(path))
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA wal_autocheckpoint=0")
    conn.execute("CREATE TABLE test (id INTEGER PRIMARY KEY, data TEXT, timestamp INTEGER)")

    # Insert unique data for this database
    for i in range(100):
        conn.execute(
            "INSERT INTO test (data, timestamp) VALUES (?, ?)",
            (f"db{db_id}_unique_row_{i}_{'x' * 100}", 1700000000 + db_id * 1000 + i)
        )
    conn.commit()

    # Calculate checksum
    checksum = compute_db_checksum(conn)
    conn.close()

    return checksum


def compute_db_checksum(conn) -> str:
    """Compute SHA256 checksum of all data in database."""
    cursor = conn.execute("SELECT * FROM test ORDER BY id")
    h = hashlib.sha256()

    for row in cursor:
        # Hash all columns
        for col in row:
            h.update(str(col).encode('utf-8'))

    return h.hexdigest()


def verify_database(path: Path, expected_checksum: str) -> tuple[bool, str]:
    """Verify a restored database matches expected checksum."""
    try:
        conn = sqlite3.connect(str(path))
        actual_checksum = compute_db_checksum(conn)
        conn.close()
        return (actual_checksum == expected_checksum, actual_checksum)
    except Exception as e:
        return (False, f"Error: {e}")


def run_integrity_test(num_dbs: int, config: dict) -> list[IntegrityResult]:
    """Test data integrity for N databases."""

    print(f"\n{'='*80}")
    print(f"🔐 Data Integrity Test: {num_dbs} databases")
    print(f"{'='*80}")

    # Find walrust binary (in parent directory's target/release/)
    script_dir = Path(__file__).parent
    walrust_bin = script_dir.parent / "target" / "release" / "walrust"
    if not walrust_bin.exists():
        raise FileNotFoundError(f"walrust binary not found at {walrust_bin}. Run: make release")

    results = []

    with tempfile.TemporaryDirectory() as tmpdir:
        tmpdir = Path(tmpdir)

        # 1. Create test databases
        print(f"\n1. Creating {num_dbs} databases with unique data...")
        db_paths = []
        checksums = {}

        for i in range(num_dbs):
            db_path = tmpdir / f"db{i:04d}.db"
            checksum = create_test_db(db_path, i)
            db_paths.append(db_path)
            checksums[f"db{i:04d}"] = checksum
            if i % 10 == 0 or i < 5:
                print(f"   Created db{i:04d}.db (checksum: {checksum[:16]}...)")

        print(f"   ✅ Created {num_dbs} databases")

        # 2. Sync to S3
        print(f"\n2. Syncing to S3...")

        # Load .env into environment
        from lib.config import load_dotenv
        load_dotenv()

        env = os.environ.copy()
        env["RUST_LOG"] = "walrust=info"  # Show what's happening

        # First, validate S3 access with a quick test
        print("   Validating S3 credentials...")
        test_cmd = [str(walrust_bin), "list", "--bucket", f"s3://{config['bucket']}/"]
        if config.get('endpoint'):
            test_cmd.extend(["--endpoint", config['endpoint']])

        test_result = subprocess.run(test_cmd, env=env, capture_output=True, text=True)
        if test_result.returncode != 0:
            raise RuntimeError(f"S3 credentials invalid or bucket not accessible:\n{test_result.stderr}")
        print("   ✅ S3 credentials valid")

        cmd = [
            str(walrust_bin), "watch",
            "--bucket", f"s3://{config['bucket']}/integrity-test/",
            "--independent-tasks", "--no-metrics"
        ] + [str(p) for p in db_paths]

        if config.get('endpoint'):
            cmd.extend(["--endpoint", config['endpoint']])

        print(f"   Running: {' '.join(cmd[:5])}...")
        proc = subprocess.Popen(cmd, env=env, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)

        # Wait for initial sync and capture output
        print("   Waiting for sync...")
        time.sleep(5)

        # Stop walrust
        proc.terminate()
        try:
            stdout, stderr = proc.communicate(timeout=5)
        except:
            proc.kill()
            stdout, stderr = proc.communicate()

        # Check for errors in output
        if "InvalidAccessKeyId" in stderr or "NoSuchBucket" in stderr or "AccessDenied" in stderr:
            raise RuntimeError(f"S3 sync failed with credentials error:\n{stderr}")

        print("   ✅ Sync complete")

        # 3. Restore each database
        print(f"\n3. Restoring {num_dbs} databases from S3...")

        restore_dir = tmpdir / "restored"
        restore_dir.mkdir()

        for i in range(num_dbs):
            db_name = f"db{i:04d}"
            restore_path = restore_dir / f"{db_name}.db"

            cmd = [
                str(walrust_bin), "restore",
                db_name, "-o", str(restore_path),
                "--bucket", f"s3://{config['bucket']}/integrity-test/"
            ]

            if config.get('endpoint'):
                cmd.extend(["--endpoint", config['endpoint']])

            result = subprocess.run(cmd, env=env, capture_output=True, text=True)

            if result.returncode != 0:
                results.append(IntegrityResult(
                    db_name=db_name,
                    original_checksum=checksums[db_name],
                    restored_checksum="",
                    match=False,
                    error=f"Restore failed: {result.stderr}"
                ))
            else:
                # Verify checksum
                match, actual_checksum = verify_database(restore_path, checksums[db_name])
                results.append(IntegrityResult(
                    db_name=db_name,
                    original_checksum=checksums[db_name],
                    restored_checksum=actual_checksum if isinstance(actual_checksum, str) and len(actual_checksum) == 64 else "",
                    match=match,
                    error="" if match else f"Checksum mismatch"
                ))

            if i % 10 == 0 or i < 5:
                status = "✅" if results[-1].match else "❌"
                print(f"   {status} Restored {db_name}")

        print(f"   ✅ Restored {num_dbs} databases")

    return results


def main():
    parser = argparse.ArgumentParser(description="Multi-database integrity verification")
    parser.add_argument('--dbs', type=str, default='10,50,100',
                        help='Comma-separated list of database counts (default: 10,50,100)')
    parser.add_argument('--bucket', type=str, help='S3 bucket for testing')
    parser.add_argument('--endpoint', type=str, help='S3 endpoint URL')
    parser.add_argument('--json', action='store_true', help='Output results as JSON')

    args = parser.parse_args()

    # Load config
    bench_config = get_config()
    config = {
        'bucket': args.bucket or bench_config.bucket,
        'endpoint': args.endpoint or bench_config.endpoint
    }

    db_counts = [int(x.strip()) for x in args.dbs.split(',')]

    print("\n" + "="*80)
    print("🔐 MULTI-DATABASE INTEGRITY VERIFICATION")
    print("Testing data roundtrip: Create → Sync → Restore → Verify")
    print("="*80)

    all_results = {}

    for num_dbs in db_counts:
        results = run_integrity_test(num_dbs, config)
        all_results[num_dbs] = results

        # Summary for this count
        passed = sum(1 for r in results if r.match)
        failed = sum(1 for r in results if not r.match)

        print(f"\n   Results: {passed}/{num_dbs} passed, {failed}/{num_dbs} failed")

        if failed > 0:
            print(f"\n   ❌ FAILED databases:")
            for r in results:
                if not r.match:
                    print(f"      {r.db_name}: {r.error}")

    # Final summary
    print("\n" + "="*80)
    print("📊 INTEGRITY TEST RESULTS")
    print("="*80)
    print(f"{'DBs':>5} {'Passed':>10} {'Failed':>10} {'Success Rate':>15}")
    print("-" * 80)

    for num_dbs, results in all_results.items():
        passed = sum(1 for r in results if r.match)
        failed = sum(1 for r in results if not r.match)
        rate = (passed / num_dbs * 100) if num_dbs > 0 else 0
        print(f"{num_dbs:5d} {passed:10d} {failed:10d} {rate:14.1f}%")

    print("="*80)

    # Overall result
    total_tested = sum(len(results) for results in all_results.values())
    total_passed = sum(sum(1 for r in results if r.match) for results in all_results.values())

    if total_passed == total_tested:
        print(f"\n✅ SUCCESS: All {total_tested} databases maintained perfect data integrity!")
        return 0
    else:
        print(f"\n❌ FAILURE: {total_tested - total_passed}/{total_tested} databases failed integrity check")
        return 1


if __name__ == "__main__":
    sys.exit(main())
