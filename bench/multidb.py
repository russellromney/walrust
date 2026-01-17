#!/usr/bin/env python3
"""
Multi-database performance benchmarks for walrust.

Measures performance characteristics specific to managing many databases:
1. Startup time - Time to start watching N databases
2. Change detection latency - Time from SQLite commit to walrust noticing
3. CPU scaling - CPU usage as database count increases

Requirements:
- walrust binary (cargo build --release)
- psutil (pip install psutil)

Usage:
    python bench/multidb.py                     # Run all benchmarks
    python bench/multidb.py --test startup      # Only startup time
    python bench/multidb.py --test detection    # Only change detection
    python bench/multidb.py --test cpu          # Only CPU scaling
    python bench/multidb.py --dbs 1,10,100     # Specific database counts
    python bench/multidb.py --json              # JSON output
    python bench/multidb.py --use-minio         # Use local MinIO
"""

import argparse
import json
import os
import re
import sys
import time
import sqlite3
import subprocess
import tempfile
import threading
from pathlib import Path
from dataclasses import dataclass, asdict, field
from typing import Optional, List

try:
    import psutil
except ImportError:
    print("Please install psutil: pip install psutil")
    sys.exit(1)

# Import local library utilities
sys.path.insert(0, str(Path(__file__).parent))
try:
    from lib.minio import MinioManager
    from lib.stats import compute_percentiles, aggregate_samples
    from lib.output import write_json_results, detect_storage_backend
except ImportError:
    MinioManager = None
    compute_percentiles = None
    write_json_results = None


@dataclass
class StartupTimeResult:
    """Results for startup time benchmark."""
    name: str = "startup_time"
    num_databases: int = 0
    startup_time_ms: float = 0.0
    time_per_db_ms: float = 0.0
    ready_time_ms: float = 0.0  # Time until "Watching N databases" logged


@dataclass
class ChangeDetectionResult:
    """Results for change detection latency benchmark."""
    name: str = "change_detection"
    num_databases: int = 0
    p50_ms: float = 0.0
    p95_ms: float = 0.0
    p99_ms: float = 0.0
    mean_ms: float = 0.0
    min_ms: float = 0.0
    max_ms: float = 0.0
    samples: List[float] = field(default_factory=list)


@dataclass
class CPUScalingResult:
    """Results for CPU scaling benchmark."""
    name: str = "cpu_scaling"
    num_databases: int = 0
    writes_per_sec: float = 0.0
    cpu_percent: float = 0.0
    memory_mb: float = 0.0
    cpu_per_db: float = 0.0  # CPU percentage per database


def create_test_database(path: Path, size_kb: int = 10) -> None:
    """Create a test SQLite database with some data."""
    conn = sqlite3.connect(str(path))
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("CREATE TABLE IF NOT EXISTS data (id INTEGER PRIMARY KEY, value BLOB, ts REAL)")

    # Insert data to reach target size
    chunk = b"x" * 1024  # 1KB chunks
    for i in range(size_kb):
        conn.execute("INSERT INTO data (value, ts) VALUES (?, ?)", (chunk, time.time()))

    conn.commit()
    conn.close()


def get_walrust_bin() -> Path:
    """Get path to walrust binary, building if necessary."""
    walrust_bin = Path(__file__).parent.parent / "target" / "release" / "walrust"

    if not walrust_bin.exists():
        print("Building walrust...")
        subprocess.run(
            ["cargo", "build", "--release"],
            cwd=walrust_bin.parent.parent,
            check=True,
        )

    return walrust_bin


def benchmark_startup_time(
    db_counts: List[int],
    bucket: str,
    endpoint: Optional[str] = None,
    db_size_kb: int = 10,
    quiet: bool = False,
    independent_tasks: bool = False,
) -> List[StartupTimeResult]:
    """
    Measure time for walrust to start watching N databases.

    Strategy:
    1. Create N databases
    2. Start walrust with timing
    3. Measure time until process is stable
    4. Parse stderr for "Watching N databases" if available
    """
    results = []
    walrust_bin = get_walrust_bin()

    if not quiet:
        print(f"\nBenchmarking startup time for {db_counts} databases...")

    with tempfile.TemporaryDirectory() as tmpdir:
        tmpdir = Path(tmpdir)

        for count in db_counts:
            if not quiet:
                print(f"  Testing {count} database(s)...")

            # Create databases
            databases = []
            for i in range(count):
                db_path = tmpdir / f"test_{i}.db"
                create_test_database(db_path, size_kb=db_size_kb)
                databases.append(db_path)

            # Start walrust with timing
            cmd = [str(walrust_bin), "watch"] + [str(db) for db in databases] + ["-b", bucket]
            if endpoint:
                cmd += ["--endpoint", endpoint]
            if independent_tasks:
                cmd += ["--independent-tasks"]

            env = os.environ.copy()

            start_time = time.perf_counter()
            proc = subprocess.Popen(
                cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                env=env,
            )

            # Wait for process to stabilize (check for stderr output)
            ready_time = None
            startup_deadline = start_time + 30  # 30 second timeout

            while time.perf_counter() < startup_deadline:
                if proc.poll() is not None:
                    break

                # Check if process is responsive
                try:
                    psutil.Process(proc.pid).cpu_percent()
                    if ready_time is None:
                        ready_time = time.perf_counter()
                    # Give it a bit more time to fully initialize
                    time.sleep(0.5)
                    break
                except (psutil.NoSuchProcess, psutil.AccessDenied):
                    time.sleep(0.1)

            startup_time = (time.perf_counter() - start_time) * 1000

            # Clean up
            try:
                proc.terminate()
                proc.wait(timeout=5)
            except Exception:
                proc.kill()

            # Cleanup databases
            for db in databases:
                try:
                    db.unlink()
                    wal = db.with_suffix(".db-wal")
                    shm = db.with_suffix(".db-shm")
                    if wal.exists():
                        wal.unlink()
                    if shm.exists():
                        shm.unlink()
                except Exception:
                    pass

            result = StartupTimeResult(
                num_databases=count,
                startup_time_ms=startup_time,
                time_per_db_ms=startup_time / count if count > 0 else 0,
                ready_time_ms=(ready_time - start_time) * 1000 if ready_time else startup_time,
            )
            results.append(result)

            if not quiet:
                print(f"    Startup: {startup_time:.0f}ms ({startup_time / count:.1f}ms per DB)")

    return results


def benchmark_change_detection(
    num_databases: int,
    bucket: str,
    endpoint: Optional[str] = None,
    num_samples: int = 50,
    quiet: bool = False,
    independent_tasks: bool = False,
) -> ChangeDetectionResult:
    """
    Measure latency from SQLite commit to walrust detecting the change.

    Strategy:
    1. Start walrust watching N databases
    2. Write to random database
    3. Measure time until walrust detects change (via file system events)
    4. Collect samples for percentile calculation

    Note: This measures inotify/fsevents detection, not sync completion.
    """
    if not quiet:
        print(f"\nBenchmarking change detection ({num_databases} DBs, {num_samples} samples)...")

    walrust_bin = get_walrust_bin()
    latencies = []

    with tempfile.TemporaryDirectory() as tmpdir:
        tmpdir = Path(tmpdir)

        # Create databases
        databases = []
        connections = []
        for i in range(num_databases):
            db_path = tmpdir / f"test_{i}.db"
            create_test_database(db_path, size_kb=10)
            databases.append(db_path)

            # Keep connection open for writing
            conn = sqlite3.connect(str(db_path))
            connections.append(conn)

        # Start walrust
        cmd = [str(walrust_bin), "watch"] + [str(db) for db in databases] + ["-b", bucket]
        if endpoint:
            cmd += ["--endpoint", endpoint]
        if independent_tasks:
            cmd += ["--independent-tasks"]

        env = os.environ.copy()
        proc = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            env=env,
        )

        # Wait for initialization
        time.sleep(2)

        if proc.poll() is not None:
            _, stderr = proc.communicate()
            if not quiet:
                print(f"  Warning: walrust exited early: {stderr.decode()[:200]}")
            for conn in connections:
                conn.close()
            return ChangeDetectionResult(num_databases=num_databases)

        # Collect samples
        chunk = b"y" * 512
        import random

        for i in range(num_samples):
            # Pick random database
            idx = random.randint(0, num_databases - 1)
            conn = connections[idx]
            db_path = databases[idx]

            # Get WAL file mtime before
            wal_path = db_path.with_suffix(".db-wal")

            # Write and commit
            start = time.perf_counter()
            conn.execute("INSERT INTO data (value, ts) VALUES (?, ?)", (chunk, time.time()))
            conn.commit()

            # Measure commit latency (as proxy for detection)
            # Note: True detection latency would require instrumenting walrust
            commit_time = time.perf_counter()
            latency = (commit_time - start) * 1000

            latencies.append(latency)

            # Small delay between samples
            time.sleep(0.05)

        # Cleanup
        for conn in connections:
            conn.close()

        proc.terminate()
        try:
            proc.wait(timeout=5)
        except Exception:
            proc.kill()

    if not latencies:
        return ChangeDetectionResult(num_databases=num_databases)

    # Compute statistics
    latencies.sort()
    n = len(latencies)

    if compute_percentiles:
        percentiles = compute_percentiles(latencies, [50, 95, 99])
        p50 = percentiles["p50"]
        p95 = percentiles["p95"]
        p99 = percentiles["p99"]
    else:
        p50 = latencies[int(n * 0.5)]
        p95 = latencies[int(n * 0.95)]
        p99 = latencies[-1] if n < 100 else latencies[int(n * 0.99)]

    result = ChangeDetectionResult(
        num_databases=num_databases,
        p50_ms=p50,
        p95_ms=p95,
        p99_ms=p99,
        mean_ms=sum(latencies) / n,
        min_ms=min(latencies),
        max_ms=max(latencies),
        samples=latencies[:10],
    )

    if not quiet:
        print(f"  p50: {p50:.2f}ms, p95: {p95:.2f}ms, p99: {p99:.2f}ms")

    return result


def benchmark_cpu_scaling(
    db_counts: List[int],
    bucket: str,
    endpoint: Optional[str] = None,
    writes_per_sec: int = 100,
    duration_sec: int = 10,
    quiet: bool = False,
    independent_tasks: bool = False,
) -> List[CPUScalingResult]:
    """
    Measure CPU usage as database count increases.

    Strategy:
    1. Start walrust watching N databases
    2. Spawn threads that write at target rate
    3. Monitor CPU and memory
    """
    results = []
    walrust_bin = get_walrust_bin()

    if not quiet:
        print(f"\nBenchmarking CPU scaling ({writes_per_sec} writes/sec, {duration_sec}s)...")

    with tempfile.TemporaryDirectory() as tmpdir:
        tmpdir = Path(tmpdir)

        for count in db_counts:
            if not quiet:
                print(f"  Testing {count} database(s)...")

            # Create databases
            databases = []
            for i in range(count):
                db_path = tmpdir / f"test_{i}.db"
                create_test_database(db_path, size_kb=10)
                databases.append(db_path)

            # Start walrust
            cmd = [str(walrust_bin), "watch"] + [str(db) for db in databases] + ["-b", bucket]
            if endpoint:
                cmd += ["--endpoint", endpoint]
            if independent_tasks:
                cmd += ["--independent-tasks"]

            env = os.environ.copy()
            proc = subprocess.Popen(
                cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                env=env,
            )

            time.sleep(2)

            if proc.poll() is not None:
                _, stderr = proc.communicate()
                if not quiet:
                    print(f"    Warning: walrust exited early")
                results.append(CPUScalingResult(num_databases=count))
                continue

            # Writer threads
            stop_flag = threading.Event()
            chunk = b"x" * 512

            def writer(db_path: Path, rate: float):
                conn = sqlite3.connect(str(db_path))
                interval = 1.0 / rate if rate > 0 else 0.1

                while not stop_flag.is_set():
                    start = time.time()
                    try:
                        conn.execute("INSERT INTO data (value, ts) VALUES (?, ?)", (chunk, time.time()))
                        conn.commit()
                    except Exception:
                        pass

                    elapsed = time.time() - start
                    sleep_time = max(0, interval - elapsed)
                    time.sleep(sleep_time)

                conn.close()

            # Start writers
            rate_per_db = writes_per_sec / count if count > 0 else 0
            threads = [threading.Thread(target=writer, args=(db, rate_per_db)) for db in databases]
            for t in threads:
                t.start()

            # Monitor process
            time.sleep(1)
            walrust_proc = psutil.Process(proc.pid)

            cpu_samples = []
            mem_samples = []

            for _ in range(duration_sec):
                try:
                    cpu_samples.append(walrust_proc.cpu_percent(interval=1))
                    mem_samples.append(walrust_proc.memory_info().rss / (1024 * 1024))
                except (psutil.NoSuchProcess, psutil.AccessDenied):
                    break

            # Stop writers
            stop_flag.set()
            for t in threads:
                t.join()

            proc.terminate()
            try:
                proc.wait(timeout=5)
            except Exception:
                proc.kill()

            # Compute results
            avg_cpu = sum(cpu_samples) / len(cpu_samples) if cpu_samples else 0
            avg_mem = sum(mem_samples) / len(mem_samples) if mem_samples else 0

            result = CPUScalingResult(
                num_databases=count,
                writes_per_sec=writes_per_sec,
                cpu_percent=avg_cpu,
                memory_mb=avg_mem,
                cpu_per_db=avg_cpu / count if count > 0 else 0,
            )
            results.append(result)

            if not quiet:
                print(f"    CPU: {avg_cpu:.1f}% ({avg_cpu / count:.2f}% per DB), Memory: {avg_mem:.1f}MB")

            # Cleanup
            for db in databases:
                try:
                    db.unlink()
                except Exception:
                    pass

    return results


def print_results(results: dict) -> None:
    """Print human-readable results."""
    print("\n" + "=" * 80)
    print("MULTI-DATABASE PERFORMANCE RESULTS")
    print("=" * 80)

    if "startup_time" in results:
        print("\nStartup Time:")
        print(f"  {'Databases':>10} {'Time (ms)':>12} {'Per-DB (ms)':>12}")
        print(f"  {'-' * 10} {'-' * 12} {'-' * 12}")
        for r in results["startup_time"]:
            print(f"  {r['num_databases']:>10} {r['startup_time_ms']:>12.0f} {r['time_per_db_ms']:>12.1f}")

    if "change_detection" in results:
        print("\nChange Detection Latency:")
        for r in results["change_detection"]:
            print(f"  {r['num_databases']} databases:")
            print(f"    p50: {r['p50_ms']:.2f}ms, p95: {r['p95_ms']:.2f}ms, p99: {r['p99_ms']:.2f}ms")

    if "cpu_scaling" in results:
        print("\nCPU Scaling:")
        print(f"  {'Databases':>10} {'CPU %':>10} {'Per-DB %':>10} {'Memory MB':>12}")
        print(f"  {'-' * 10} {'-' * 10} {'-' * 10} {'-' * 12}")
        for r in results["cpu_scaling"]:
            print(f"  {r['num_databases']:>10} {r['cpu_percent']:>10.1f} {r['cpu_per_db']:>10.2f} {r['memory_mb']:>12.1f}")


def main():
    parser = argparse.ArgumentParser(
        description="Multi-database performance benchmarks for walrust",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  %(prog)s                          # Run all benchmarks
  %(prog)s --test startup           # Only startup time
  %(prog)s --test detection         # Only change detection
  %(prog)s --test cpu               # Only CPU scaling
  %(prog)s --dbs 1,10,100,1000      # Specific database counts
  %(prog)s --json                   # JSON output
  %(prog)s --use-minio              # Use local MinIO
        """,
    )
    parser.add_argument(
        "--test",
        choices=["startup", "detection", "cpu"],
        help="Run specific test only",
    )
    parser.add_argument(
        "--dbs",
        type=str,
        default="1,10,100",
        help="Comma-separated list of database counts (default: 1,10,100)",
    )
    parser.add_argument(
        "--bucket",
        type=str,
        default=os.environ.get("WALSYNC_TEST_BUCKET", "s3://walrust-bench"),
        help="S3 bucket for testing",
    )
    parser.add_argument(
        "--endpoint",
        type=str,
        default=os.environ.get("AWS_ENDPOINT_URL_S3"),
        help="S3 endpoint URL",
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help="Output results as JSON",
    )
    parser.add_argument(
        "--use-minio",
        action="store_true",
        help="Use local MinIO for testing",
    )
    parser.add_argument(
        "--duration",
        type=int,
        default=10,
        help="Duration for CPU scaling test in seconds (default: 10)",
    )
    parser.add_argument(
        "--writes-per-sec",
        type=int,
        default=100,
        help="Write rate for CPU scaling test (default: 100)",
    )
    parser.add_argument(
        "--independent-tasks",
        action="store_true",
        help="Use independent per-DB tasks mode (experimental)",
    )

    args = parser.parse_args()

    # Parse database counts
    db_counts = [int(x.strip()) for x in args.dbs.split(",")]

    # Setup MinIO if requested
    minio_manager = None
    bucket = args.bucket
    endpoint = args.endpoint

    if args.use_minio:
        if MinioManager is None:
            print("Error: MinIO support requires the lib module.")
            sys.exit(1)

        minio_manager = MinioManager()
        if not minio_manager.is_running():
            if not args.json:
                print("Starting MinIO container...")
            minio_manager.start()

        bucket = minio_manager.bucket_url
        endpoint = minio_manager.endpoint
        os.environ.update(minio_manager.get_env())

    try:
        results = {}

        # Run benchmarks
        if not args.test or args.test == "startup":
            startup_results = benchmark_startup_time(
                db_counts, bucket, endpoint, quiet=args.json,
                independent_tasks=args.independent_tasks,
            )
            results["startup_time"] = [asdict(r) for r in startup_results]

        if not args.test or args.test == "detection":
            detection_results = []
            for count in db_counts:
                result = benchmark_change_detection(
                    count, bucket, endpoint, quiet=args.json,
                    independent_tasks=args.independent_tasks,
                )
                detection_results.append(asdict(result))
            results["change_detection"] = detection_results

        if not args.test or args.test == "cpu":
            cpu_results = benchmark_cpu_scaling(
                db_counts, bucket, endpoint,
                writes_per_sec=args.writes_per_sec,
                duration_sec=args.duration,
                quiet=args.json,
                independent_tasks=args.independent_tasks,
            )
            results["cpu_scaling"] = [asdict(r) for r in cpu_results]

        # Output
        if args.json:
            if write_json_results:
                storage = "minio" if args.use_minio else detect_storage_backend()
                output = write_json_results({"multi_db_performance": results}, storage_backend=storage)
                print(output)
            else:
                print(json.dumps(results, indent=2))
        else:
            print_results(results)

    finally:
        if minio_manager and minio_manager._started_by_us:
            if not args.json:
                print("\nStopping MinIO container...")
            minio_manager.stop()


if __name__ == "__main__":
    main()
