#!/usr/bin/env python3
"""
Benchmark: walrust vs litestream

Compares memory usage and CPU for single and multiple databases.

Requirements:
- walrust binary (cargo build --release)
- litestream binary (brew install litestream or download)
- psutil (pip install psutil)

Usage:
    python bench/compare.py                     # Run all benchmarks
    python bench/compare.py --dbs 1,5,10       # Specific database counts
    python bench/compare.py --walrust-only     # Only benchmark walrust
    python bench/compare.py --litestream-only  # Only benchmark litestream
    python bench/compare.py --duration 10      # Measure for 10 seconds
    python bench/compare.py --db-size 1000     # 1MB test databases
    python bench/compare.py --json             # Output as JSON
"""

import argparse
import json
import os
import sys
import time
import sqlite3
import subprocess
import tempfile
import shutil
from pathlib import Path
from dataclasses import dataclass, asdict
from typing import Optional

try:
    import psutil
except ImportError:
    print("Please install psutil: pip install psutil")
    sys.exit(1)

try:
    import boto3
except ImportError:
    boto3 = None  # Cleanup will be skipped


@dataclass
class BenchmarkResult:
    name: str
    num_databases: int
    num_processes: int
    peak_memory_mb: float
    avg_memory_mb: float
    cpu_percent: float
    startup_time_ms: float


def create_test_database(path: Path, size_kb: int = 100) -> None:
    """Create a test SQLite database with some data."""
    conn = sqlite3.connect(str(path))
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("CREATE TABLE IF NOT EXISTS data (id INTEGER PRIMARY KEY, value BLOB)")

    # Insert data to reach target size
    chunk = b"x" * 1024  # 1KB chunks
    for i in range(size_kb):
        conn.execute("INSERT INTO data (value) VALUES (?)", (chunk,))

    conn.commit()
    conn.close()


def measure_process_stats(pids: list[int], duration_secs: float = 5.0) -> tuple[float, float, float]:
    """Measure peak memory, avg memory, and CPU for a set of PIDs."""
    memory_samples = []
    cpu_samples = []

    start = time.time()
    while time.time() - start < duration_secs:
        total_memory = 0
        total_cpu = 0

        for pid in pids:
            try:
                proc = psutil.Process(pid)
                total_memory += proc.memory_info().rss / (1024 * 1024)  # MB
                total_cpu += proc.cpu_percent()
            except (psutil.NoSuchProcess, psutil.AccessDenied):
                pass

        memory_samples.append(total_memory)
        cpu_samples.append(total_cpu)
        time.sleep(0.1)

    if not memory_samples:
        return 0, 0, 0

    peak_memory = max(memory_samples)
    avg_memory = sum(memory_samples) / len(memory_samples)
    avg_cpu = sum(cpu_samples) / len(cpu_samples)

    return peak_memory, avg_memory, avg_cpu


def benchmark_walrust(
    databases: list[Path],
    bucket: str,
    endpoint: Optional[str] = None,
    duration: float = 5.0,
) -> BenchmarkResult:
    """Benchmark walrust with multiple databases (single process)."""
    walrust_bin = Path(__file__).parent.parent / "target" / "release" / "walrust"

    if not walrust_bin.exists():
        print("Building walrust...")
        subprocess.run(["cargo", "build", "--release"], cwd=walrust_bin.parent.parent, check=True)

    cmd = [str(walrust_bin), "watch"] + [str(db) for db in databases] + ["-b", bucket]
    if endpoint:
        cmd += ["--endpoint", endpoint]

    # Pass through environment (including AWS credentials)
    env = os.environ.copy()

    start_time = time.time()
    proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, env=env)
    startup_time = (time.time() - start_time) * 1000

    # Wait for process to stabilize
    time.sleep(1)

    # Check if process is still running
    if proc.poll() is not None:
        _, stderr = proc.communicate()
        print(f"    Warning: walrust exited early: {stderr.decode()[:200]}")
        return BenchmarkResult(
            name="walrust",
            num_databases=len(databases),
            num_processes=1,
            peak_memory_mb=0,
            avg_memory_mb=0,
            cpu_percent=0,
            startup_time_ms=startup_time,
        )

    # Measure stats
    peak_mem, avg_mem, cpu = measure_process_stats([proc.pid], duration)

    proc.terminate()
    proc.wait()

    return BenchmarkResult(
        name="walrust",
        num_databases=len(databases),
        num_processes=1,
        peak_memory_mb=peak_mem,
        avg_memory_mb=avg_mem,
        cpu_percent=cpu,
        startup_time_ms=startup_time,
    )


def benchmark_litestream(
    databases: list[Path],
    bucket: str,
    duration: float = 5.0,
) -> BenchmarkResult:
    """Benchmark litestream with multiple databases (one process per db)."""
    litestream_bin = shutil.which("litestream")

    if not litestream_bin:
        print("Litestream not found. Install with: brew install litestream")
        return BenchmarkResult(
            name="litestream",
            num_databases=len(databases),
            num_processes=0,
            peak_memory_mb=0,
            avg_memory_mb=0,
            cpu_percent=0,
            startup_time_ms=0,
        )

    processes = []
    start_time = time.time()

    for db in databases:
        # Create litestream config for this database
        config = f"""
dbs:
  - path: {db}
    replicas:
      - url: {bucket}/{db.stem}
"""
        config_file = db.with_suffix(".litestream.yml")
        config_file.write_text(config)

        proc = subprocess.Popen(
            [litestream_bin, "replicate", "-config", str(config_file)],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        processes.append(proc)

    startup_time = (time.time() - start_time) * 1000

    # Wait for processes to stabilize
    time.sleep(1)

    # Measure stats
    pids = [p.pid for p in processes]
    peak_mem, avg_mem, cpu = measure_process_stats(pids, duration)

    for proc in processes:
        proc.terminate()
        proc.wait()

    return BenchmarkResult(
        name="litestream",
        num_databases=len(databases),
        num_processes=len(databases),
        peak_memory_mb=peak_mem,
        avg_memory_mb=avg_mem,
        cpu_percent=cpu,
        startup_time_ms=startup_time,
    )


def run_comparison(
    db_counts: list[int],
    bucket: str,
    endpoint: Optional[str] = None,
    db_size_kb: int = 100,
    duration: float = 5.0,
    walrust_only: bool = False,
    litestream_only: bool = False,
    quiet: bool = False,
) -> list[dict]:
    """Run comparison benchmarks for different database counts."""
    results = []

    with tempfile.TemporaryDirectory() as tmpdir:
        tmpdir = Path(tmpdir)

        for count in db_counts:
            if not quiet:
                print(f"\n--- Benchmarking with {count} database(s) ---")

            # Create test databases
            databases = []
            for i in range(count):
                db_path = tmpdir / f"test_{i}.db"
                create_test_database(db_path, size_kb=db_size_kb)
                databases.append(db_path)

            if not quiet:
                print(f"Created {count} test databases ({db_size_kb}KB each)")

            result = {"num_databases": count}

            # Benchmark walrust
            if not litestream_only:
                if not quiet:
                    print("Benchmarking walrust...")
                walrust_result = benchmark_walrust(databases, bucket, endpoint, duration)
                result["walrust"] = asdict(walrust_result)

            # Benchmark litestream
            if not walrust_only:
                if not quiet:
                    print("Benchmarking litestream...")
                litestream_result = benchmark_litestream(databases, bucket, duration)
                result["litestream"] = asdict(litestream_result)

            results.append(result)

            # Print comparison
            if not quiet and not litestream_only and not walrust_only:
                print(f"\nResults for {count} database(s):")
                print(f"  {'':20} {'walrust':>12} {'litestream':>12} {'savings':>12}")
                print(f"  {'Processes':20} {walrust_result.num_processes:>12} {litestream_result.num_processes:>12}")
                print(f"  {'Peak Memory (MB)':20} {walrust_result.peak_memory_mb:>12.1f} {litestream_result.peak_memory_mb:>12.1f} {litestream_result.peak_memory_mb - walrust_result.peak_memory_mb:>12.1f}")
                print(f"  {'Avg Memory (MB)':20} {walrust_result.avg_memory_mb:>12.1f} {litestream_result.avg_memory_mb:>12.1f} {litestream_result.avg_memory_mb - walrust_result.avg_memory_mb:>12.1f}")
                print(f"  {'CPU %':20} {walrust_result.cpu_percent:>12.1f} {litestream_result.cpu_percent:>12.1f}")

    return results


def cleanup_s3_prefix(bucket: str, prefix: str, endpoint: Optional[str] = None) -> None:
    """Clean up test objects from S3."""
    try:
        session = boto3.Session()
        config_kwargs = {}
        if endpoint:
            config_kwargs["endpoint_url"] = endpoint

        s3 = session.client("s3", **config_kwargs)
        bucket_name = bucket.replace("s3://", "").split("/")[0]

        # List and delete objects with prefix
        paginator = s3.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=bucket_name, Prefix=prefix):
            if "Contents" in page:
                objects = [{"Key": obj["Key"]} for obj in page["Contents"]]
                if objects:
                    s3.delete_objects(Bucket=bucket_name, Delete={"Objects": objects})
    except Exception as e:
        print(f"Warning: Failed to cleanup S3: {e}")


def print_summary(results: list[dict]) -> None:
    """Print a summary table of all results."""
    if not results or "walrust" not in results[0] or "litestream" not in results[0]:
        return

    print("\n" + "=" * 80)
    print("SUMMARY: walrust vs litestream")
    print("=" * 80)
    print(f"\n{'DBs':>4} | {'walrust':^30} | {'litestream':^30} | {'Memory Saved':>12}")
    print(f"{'':>4} | {'Procs':>6} {'Peak MB':>10} {'Avg MB':>10} | {'Procs':>6} {'Peak MB':>10} {'Avg MB':>10} |")
    print("-" * 80)

    for r in results:
        w = r["walrust"]
        l = r["litestream"]
        savings = l["avg_memory_mb"] - w["avg_memory_mb"]
        print(f"{r['num_databases']:>4} | {w['num_processes']:>6} {w['peak_memory_mb']:>10.1f} {w['avg_memory_mb']:>10.1f} | {l['num_processes']:>6} {l['peak_memory_mb']:>10.1f} {l['avg_memory_mb']:>10.1f} | {savings:>10.1f} MB")

    print("-" * 80)


def main():
    parser = argparse.ArgumentParser(
        description="Benchmark walrust vs litestream",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  %(prog)s                          # Run full comparison with 1,5,10,20 databases
  %(prog)s --dbs 1,5               # Only test with 1 and 5 databases
  %(prog)s --walrust-only          # Only benchmark walrust
  %(prog)s --duration 10           # Measure for 10 seconds per test
  %(prog)s --db-size 1000          # Use 1MB test databases
  %(prog)s --json                  # Output results as JSON
        """,
    )
    parser.add_argument(
        "--dbs",
        type=str,
        default="1,5,10,20",
        help="Comma-separated list of database counts to test (default: 1,5,10,20)",
    )
    parser.add_argument(
        "--walrust-only",
        action="store_true",
        help="Only benchmark walrust",
    )
    parser.add_argument(
        "--litestream-only",
        action="store_true",
        help="Only benchmark litestream",
    )
    parser.add_argument(
        "--duration",
        type=float,
        default=5.0,
        help="Duration in seconds to measure each benchmark (default: 5)",
    )
    parser.add_argument(
        "--db-size",
        type=int,
        default=100,
        help="Size of each test database in KB (default: 100)",
    )
    parser.add_argument(
        "--bucket",
        type=str,
        default=os.environ.get("WALSYNC_TEST_BUCKET", "s3://walrust-bench"),
        help="S3 bucket for testing (default: $WALSYNC_TEST_BUCKET or s3://walrust-bench)",
    )
    parser.add_argument(
        "--endpoint",
        type=str,
        default=os.environ.get("AWS_ENDPOINT_URL_S3"),
        help="S3 endpoint URL (default: $AWS_ENDPOINT_URL_S3)",
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help="Output results as JSON",
    )

    args = parser.parse_args()

    # Parse database counts
    db_counts = [int(x.strip()) for x in args.dbs.split(",")]

    if not args.json:
        print("Walrust vs Litestream Benchmark")
        print(f"Bucket: {args.bucket}")
        print(f"Endpoint: {args.endpoint or 'default'}")
        print(f"Database counts: {db_counts}")
        print(f"Database size: {args.db_size}KB")
        print(f"Measurement duration: {args.duration}s")

    # Run benchmarks
    results = run_comparison(
        db_counts=db_counts,
        bucket=args.bucket,
        endpoint=args.endpoint,
        db_size_kb=args.db_size,
        duration=args.duration,
        walrust_only=args.walrust_only,
        litestream_only=args.litestream_only,
        quiet=args.json,
    )

    if args.json:
        print(json.dumps(results, indent=2))
    else:
        print_summary(results)

    # Cleanup test objects from S3
    if boto3 and not args.json:
        print("\nCleaning up S3 test data...")
        cleanup_s3_prefix(args.bucket, "test_", args.endpoint)
        print("Done.")


if __name__ == "__main__":
    main()
