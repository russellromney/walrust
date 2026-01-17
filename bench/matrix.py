#!/usr/bin/env python3
"""
Matrix benchmark: DBs x Write Rate for walrust vs litestream.

Tests combinations of:
- Database counts: 1, 10, 50, 100
- Writes per second per database: 10, 50, 100

Usage:
    uv run bench/matrix.py                    # Uses .env config
    uv run bench/matrix.py --dbs 1,10,100     # Custom DB counts
    uv run bench/matrix.py --tool walrust     # walrust only
"""

import argparse
import os
import sys

# Add bench/lib to path
sys.path.insert(0, str(__import__("pathlib").Path(__file__).parent))
from lib.config import get_config
import time
import sqlite3
import subprocess
import tempfile
import threading
import shutil
from pathlib import Path
from dataclasses import dataclass
from typing import Optional, List

try:
    import psutil
except ImportError:
    print("Please install psutil: pip install psutil")
    sys.exit(1)

try:
    import boto3
except ImportError:
    print("Please install boto3: pip install boto3")
    sys.exit(1)


@dataclass
class MatrixResult:
    tool: str
    db_count: int
    writes_per_db: int
    total_target_rate: int
    actual_rate: float
    avg_memory_mb: float
    peak_memory_mb: float
    avg_cpu_percent: float
    sustained: bool  # Did it keep up?


def cleanup_s3_prefix(bucket: str, endpoint: Optional[str], prefix: str) -> int:
    """Delete all objects with given prefix from S3. Returns count deleted."""
    try:
        bucket_name = bucket.replace("s3://", "").split("/")[0]
        kwargs = {}
        if endpoint:
            kwargs["endpoint_url"] = endpoint

        s3 = boto3.client("s3", **kwargs)
        deleted = 0

        paginator = s3.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=bucket_name, Prefix=prefix):
            if "Contents" in page:
                objects = [{"Key": obj["Key"]} for obj in page["Contents"]]
                if objects:
                    s3.delete_objects(Bucket=bucket_name, Delete={"Objects": objects})
                    deleted += len(objects)
        return deleted
    except Exception as e:
        print(f"  Warning: Cleanup failed: {e}")
        return 0


def create_test_databases(tmpdir: Path, count: int) -> List[Path]:
    """Create test SQLite databases."""
    databases = []
    for i in range(count):
        db_path = tmpdir / f"db_{i}.db"
        conn = sqlite3.connect(str(db_path))
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("CREATE TABLE data (id INTEGER PRIMARY KEY, ts REAL, value BLOB)")
        conn.commit()
        conn.close()
        databases.append(db_path)
    return databases


def run_matrix_test(
    databases: List[Path],
    bucket: str,
    endpoint: Optional[str],
    writes_per_db: int,
    duration_secs: float,
    tool: str,
    wal_sync_interval: int = 1,
) -> MatrixResult:
    """Run a single matrix test."""
    db_count = len(databases)
    total_rate = writes_per_db * db_count

    # Start the tool
    if tool == "walrust":
        walrust_bin = Path(__file__).parent.parent / "target" / "release" / "walrust"
        cmd = [str(walrust_bin), "watch"] + [str(db) for db in databases] + ["-b", bucket]
        if endpoint:
            cmd += ["--endpoint", endpoint]
        cmd += ["--wal-sync-interval", str(wal_sync_interval)]
    else:
        litestream_bin = shutil.which("litestream")
        if not litestream_bin:
            return MatrixResult(
                tool=tool, db_count=db_count, writes_per_db=writes_per_db,
                total_target_rate=total_rate, actual_rate=0, avg_memory_mb=0,
                peak_memory_mb=0, avg_cpu_percent=0, sustained=False,
            )

        # Create litestream config
        access_key = os.environ.get("AWS_ACCESS_KEY_ID", "")
        secret_key = os.environ.get("AWS_SECRET_ACCESS_KEY", "")

        config_lines = ["dbs:"]
        for db in databases:
            config_lines.append(f"  - path: {db}")
            config_lines.append(f"    replicas:")
            config_lines.append(f"      - url: {bucket}/{db.stem}")
            if access_key:
                config_lines.append(f"        access-key-id: {access_key}")
            if secret_key:
                config_lines.append(f"        secret-access-key: {secret_key}")
            if endpoint:
                config_lines.append(f"        endpoint: {endpoint}")

        config_file = databases[0].parent / "litestream.yml"
        config_file.write_text("\n".join(config_lines))
        cmd = [litestream_bin, "replicate", "-config", str(config_file)]

    proc = subprocess.Popen(
        cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, env=os.environ.copy()
    )

    time.sleep(1)  # Let it start

    if proc.poll() is not None:
        return MatrixResult(
            tool=tool, db_count=db_count, writes_per_db=writes_per_db,
            total_target_rate=total_rate, actual_rate=0, avg_memory_mb=0,
            peak_memory_mb=0, avg_cpu_percent=0, sustained=False,
        )

    # Start writers and monitor
    stop_flag = threading.Event()
    writes_completed = [0]
    memory_samples = []
    cpu_samples = []
    chunk = b"x" * 100

    def writer(db_path: Path, rate: float):
        conn = sqlite3.connect(str(db_path))
        interval = 1.0 / rate if rate > 0 else 1.0
        while not stop_flag.is_set():
            start = time.time()
            try:
                conn.execute("INSERT INTO data (ts, value) VALUES (?, ?)", (time.time(), chunk))
                conn.commit()
                writes_completed[0] += 1
            except:
                pass
            elapsed = time.time() - start
            if elapsed < interval:
                time.sleep(interval - elapsed)
        conn.close()

    def monitor():
        while not stop_flag.is_set():
            try:
                p = psutil.Process(proc.pid)
                memory_samples.append(p.memory_info().rss / (1024 * 1024))
                cpu_samples.append(p.cpu_percent())
            except:
                break
            time.sleep(0.1)

    # Start threads
    threads = [threading.Thread(target=writer, args=(db, writes_per_db)) for db in databases]
    threads.append(threading.Thread(target=monitor))
    for t in threads:
        t.start()

    # Run for duration
    time.sleep(duration_secs)

    # Stop
    stop_flag.set()
    for t in threads:
        t.join(timeout=2)

    proc.terminate()
    proc.wait()

    actual_rate = writes_completed[0] / duration_secs
    avg_memory = sum(memory_samples) / len(memory_samples) if memory_samples else 0
    peak_memory = max(memory_samples) if memory_samples else 0
    avg_cpu = sum(cpu_samples) / len(cpu_samples) if cpu_samples else 0
    sustained = actual_rate >= total_rate * 0.9  # Within 90% of target

    return MatrixResult(
        tool=tool,
        db_count=db_count,
        writes_per_db=writes_per_db,
        total_target_rate=total_rate,
        actual_rate=actual_rate,
        avg_memory_mb=avg_memory,
        peak_memory_mb=peak_memory,
        avg_cpu_percent=avg_cpu,
        sustained=sustained,
    )


def main():
    # Load config from .env
    config = get_config()

    parser = argparse.ArgumentParser(description="Matrix benchmark: DBs x Write Rate")
    parser.add_argument("--bucket", default=config.bucket_url, help="S3 bucket URL")
    parser.add_argument("--endpoint", default=config.endpoint, help="S3 endpoint URL")
    parser.add_argument("--dbs", default="1,10,50", help="Database counts (comma-separated)")
    parser.add_argument("--rates", default="10,50,100", help="Writes/sec/db (comma-separated)")
    parser.add_argument("--duration", type=float, default=10.0, help="Duration per test")
    parser.add_argument("--tool", choices=["walrust", "litestream", "both"], default="both")
    parser.add_argument("--wal-sync-interval", type=int, default=1, help="WAL sync interval in seconds (walrust only)")

    args = parser.parse_args()
    db_counts = [int(x) for x in args.dbs.split(",")]
    rates = [int(x) for x in args.rates.split(",")]
    tools = ["walrust", "litestream"] if args.tool == "both" else [args.tool]

    print("Matrix Benchmark: DBs x Write Rate")
    print(f"Database counts: {db_counts}")
    print(f"Writes/sec/db: {rates}")
    print(f"Duration: {args.duration}s per test")
    print()

    for tool in tools:
        print(f"{'='*60}")
        print(f" {tool.upper()}")
        print(f"{'='*60}")
        print(f"{'DBs':>5} | {'W/s/db':>7} | {'Target':>8} | {'Actual':>8} | {'Mem MB':>7} | {'CPU%':>5} | {'OK':>3}")
        print("-" * 60)

        with tempfile.TemporaryDirectory() as tmpdir:
            tmpdir = Path(tmpdir)

            for db_count in db_counts:
                databases = create_test_databases(tmpdir, db_count)

                for writes_per_db in rates:
                    total = db_count * writes_per_db
                    print(f"{db_count:>5} | {writes_per_db:>7} | {total:>8} | ", end="", flush=True)

                    result = run_matrix_test(
                        databases=databases,
                        bucket=args.bucket,
                        endpoint=args.endpoint,
                        writes_per_db=writes_per_db,
                        duration_secs=args.duration,
                        tool=tool,
                        wal_sync_interval=getattr(args, 'wal_sync_interval', 1),
                    )

                    ok = "yes" if result.sustained else "NO"
                    print(f"{result.actual_rate:>8.0f} | {result.avg_memory_mb:>7.1f} | {result.avg_cpu_percent:>5.1f} | {ok:>3}")

                    if not result.sustained:
                        print(f"      ^ Could not sustain {total} writes/sec")

            # Cleanup S3 after each tool
            print(f"Cleaning up S3...", end=" ", flush=True)
            deleted = cleanup_s3_prefix(args.bucket, args.endpoint, "db_")
            print(f"deleted {deleted} objects")

        print()


if __name__ == "__main__":
    main()
