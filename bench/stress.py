#!/usr/bin/env python3
"""
Stress test to find walrust's throughput limits.

Increases write rate until we find the bottleneck:
- Network-bound: sync lag grows, memory/CPU flat
- Memory-bound: memory grows unbounded
- CPU-bound: CPU maxes out

Usage:
    uv run bench/stress.py                    # Uses .env config
    uv run bench/stress.py --rates 10,50,100  # Custom rates
    uv run bench/stress.py --cpu-limit 50     # Limit CPU (requires cpulimit)
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
import queue
from pathlib import Path
from dataclasses import dataclass
from typing import Optional

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
class StressResult:
    write_rate: int  # target writes/sec
    actual_rate: float  # achieved writes/sec
    avg_memory_mb: float
    peak_memory_mb: float
    avg_cpu_percent: float
    sync_lag_ms: float  # estimated lag behind
    writes_completed: int
    syncs_observed: int
    bottleneck: str  # "none", "sqlite", "cpu", "memory", "network"
    write_latency_p50_ms: float = 0.0
    write_latency_p99_ms: float = 0.0
    db_size_mb: float = 0.0
    wal_size_mb: float = 0.0


def create_test_database(path: Path, size_mb: int = 0) -> None:
    """Create a test SQLite database, optionally pre-populated to a target size.

    Args:
        size_mb: Target database size in MB. 0 = empty (just schema).
                 Pre-populating makes the benchmark realistic for large DBs
                 where checksum cost dominates.
    """
    conn = sqlite3.connect(str(path))
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA page_size=4096")
    conn.execute("CREATE TABLE IF NOT EXISTS data (id INTEGER PRIMARY KEY, ts REAL, value BLOB)")
    conn.commit()

    if size_mb > 0:
        # Each row is ~1KB (100 bytes value + overhead), so ~1000 rows/MB
        rows_needed = size_mb * 1000
        chunk = b"x" * 900  # ~1KB per row with overhead
        batch_size = 1000
        inserted = 0
        while inserted < rows_needed:
            batch = min(batch_size, rows_needed - inserted)
            conn.executemany(
                "INSERT INTO data (ts, value) VALUES (?, ?)",
                [(time.time(), chunk) for _ in range(batch)],
            )
            conn.commit()
            inserted += batch
        # Checkpoint to fold WAL into main DB
        conn.execute("PRAGMA wal_checkpoint(TRUNCATE)")
        print(f"  Pre-populated {size_mb}MB database ({inserted} rows)")

    conn.close()


def run_stress_test(
    db_path: Path,
    bucket: str,
    endpoint: Optional[str],
    write_rate: int,
    duration_secs: float = 10.0,
    cpu_limit: Optional[int] = None,
    tool: str = "walrust",
) -> StressResult:
    """
    Run a stress test at a specific write rate.

    Returns metrics about memory, CPU, and sync lag.

    Args:
        cpu_limit: If set, limit process to this % of CPU (requires cpulimit)
        tool: "walrust" or "litestream"
    """
    import shutil

    if tool == "walrust":
        walrust_bin = Path(__file__).parent.parent / "target" / "release" / "walrust"
        tool_cmd = [str(walrust_bin), "watch", str(db_path), "-b", bucket]
        if endpoint:
            tool_cmd += ["--endpoint", endpoint]
    else:
        # litestream
        litestream_bin = shutil.which("litestream")
        if not litestream_bin:
            print("  Error: litestream not found")
            return StressResult(
                write_rate=write_rate, actual_rate=0, avg_memory_mb=0,
                peak_memory_mb=0, avg_cpu_percent=0, sync_lag_ms=0,
                writes_completed=0, syncs_observed=0, bottleneck="not_found",
            )

        # Create litestream config
        access_key = os.environ.get("AWS_ACCESS_KEY_ID", "")
        secret_key = os.environ.get("AWS_SECRET_ACCESS_KEY", "")

        config_lines = [
            "dbs:",
            f"  - path: {db_path}",
            "    replicas:",
            f"      - url: {bucket}/{db_path.stem}",
        ]
        if access_key:
            config_lines.append(f"        access-key-id: {access_key}")
        if secret_key:
            config_lines.append(f"        secret-access-key: {secret_key}")
        if endpoint:
            config_lines.append(f"        endpoint: {endpoint}")

        config_file = db_path.parent / "litestream.yml"
        config_file.write_text("\n".join(config_lines))

        tool_cmd = [litestream_bin, "replicate", "-config", str(config_file)]

    if cpu_limit:
        cpulimit_bin = shutil.which("cpulimit")
        if not cpulimit_bin:
            print(f"  Warning: cpulimit not found, running without CPU limit")
            cmd = tool_cmd
        else:
            cmd = [cpulimit_bin, "-l", str(cpu_limit), "--"] + tool_cmd
    else:
        cmd = tool_cmd

    proc = subprocess.Popen(
        cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        env=os.environ.copy(),
    )

    # Wait for startup
    time.sleep(1)

    if proc.poll() is not None:
        _, stderr = proc.communicate()
        print(f"  walrust exited: {stderr.decode()[:100]}")
        return StressResult(
            write_rate=write_rate,
            actual_rate=0,
            avg_memory_mb=0,
            peak_memory_mb=0,
            avg_cpu_percent=0,
            sync_lag_ms=0,
            writes_completed=0,
            syncs_observed=0,
            bottleneck="startup_failed",
        )

    # Metrics collection
    memory_samples = []
    cpu_samples = []
    write_latencies = []  # per-write latency in seconds
    wal_sizes_mb = []  # WAL file size over time
    writes_completed = [0]
    stop_flag = threading.Event()

    # Get initial DB size
    db_size_mb = db_path.stat().st_size / (1024 * 1024) if db_path.exists() else 0
    wal_path = Path(str(db_path) + "-wal")

    def writer():
        """Write to database at target rate, tracking per-write latency.

        Uses the same PRAGMA settings as walrust production:
        - synchronous=NORMAL (not FULL — WAL+walrust provides durability)
        - wal_autocheckpoint=0 (walrust owns checkpointing)
        - cache_size=-64000 (64MB)
        - mmap_size=268435456 (256MB)
        """
        conn = sqlite3.connect(str(db_path))
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA synchronous=NORMAL")
        conn.execute("PRAGMA wal_autocheckpoint=0")
        conn.execute("PRAGMA busy_timeout=5000")
        conn.execute("PRAGMA cache_size=-64000")
        conn.execute("PRAGMA mmap_size=268435456")
        interval = 1.0 / write_rate if write_rate > 0 else 1.0
        chunk = b"x" * 100  # Small payload

        while not stop_flag.is_set():
            cycle_start = time.time()
            try:
                w_start = time.monotonic()
                conn.execute("INSERT INTO data (ts, value) VALUES (?, ?)", (time.time(), chunk))
                conn.commit()
                w_end = time.monotonic()
                write_latencies.append(w_end - w_start)
                writes_completed[0] += 1
            except Exception as e:
                print(f"  Write error: {e}")

            elapsed = time.time() - cycle_start
            sleep_time = max(0, interval - elapsed)
            if sleep_time > 0:
                time.sleep(sleep_time)

        conn.close()

    def monitor():
        """Monitor walrust process memory and CPU (with children)."""
        try:
            p = psutil.Process(proc.pid)
            # Prime cpu_percent (first call always returns 0)
            p.cpu_percent()
        except (psutil.NoSuchProcess, psutil.AccessDenied):
            return

        time.sleep(0.2)  # Let baseline settle

        while not stop_flag.is_set():
            try:
                p = psutil.Process(proc.pid)
                # Include child processes
                mem = p.memory_info().rss
                cpu = p.cpu_percent(interval=0.1)
                for child in p.children(recursive=True):
                    try:
                        mem += child.memory_info().rss
                        cpu += child.cpu_percent(interval=0)
                    except (psutil.NoSuchProcess, psutil.AccessDenied):
                        pass
                memory_samples.append(mem / (1024 * 1024))
                cpu_samples.append(cpu)
                # Track WAL size
                if wal_path.exists():
                    wal_sizes_mb.append(wal_path.stat().st_size / (1024 * 1024))
            except (psutil.NoSuchProcess, psutil.AccessDenied):
                break
            time.sleep(0.2)

    # Start threads
    writer_thread = threading.Thread(target=writer)
    monitor_thread = threading.Thread(target=monitor)

    writer_thread.start()
    monitor_thread.start()

    # Run for duration
    time.sleep(duration_secs)

    # Stop
    stop_flag.set()
    writer_thread.join()
    monitor_thread.join()

    # Get final DB + WAL size
    db_size_end_mb = db_path.stat().st_size / (1024 * 1024) if db_path.exists() else 0
    wal_size_end_mb = wal_path.stat().st_size / (1024 * 1024) if wal_path.exists() else 0

    # Calculate metrics
    actual_rate = writes_completed[0] / duration_secs if duration_secs > 0 else 0
    avg_memory = sum(memory_samples) / len(memory_samples) if memory_samples else 0
    peak_memory = max(memory_samples) if memory_samples else 0
    # Drop first few CPU samples (warmup)
    cpu_warm = cpu_samples[3:] if len(cpu_samples) > 5 else cpu_samples
    avg_cpu = sum(cpu_warm) / len(cpu_warm) if cpu_warm else 0

    # Write latency percentiles
    write_latencies.sort()
    p50 = write_latencies[len(write_latencies) // 2] * 1000 if write_latencies else 0
    p99_idx = min(int(len(write_latencies) * 0.99), len(write_latencies) - 1)
    p99 = write_latencies[p99_idx] * 1000 if write_latencies else 0

    # Estimate sync lag by checking S3
    sync_lag_ms = estimate_sync_lag(bucket, endpoint, db_path.stem)

    # Determine bottleneck based on actual data
    bottleneck = "none"
    throughput_ratio = actual_rate / write_rate if write_rate > 0 else 1.0

    if throughput_ratio < 0.7:
        # We're clearly not keeping up — figure out why
        if avg_cpu > 80:
            bottleneck = "cpu"
        elif peak_memory > 500:
            bottleneck = "memory"
        elif sync_lag_ms > 5000:
            bottleneck = "network"
        else:
            # Throughput plateaued with healthy metrics = SQLite commit ceiling.
            # Per-write latency stays low because individual writes are fast,
            # but total throughput is bounded by commit overhead (fsync, WAL frames).
            bottleneck = "sqlite"

    proc.terminate()
    proc.wait()

    return StressResult(
        write_rate=write_rate,
        actual_rate=actual_rate,
        avg_memory_mb=avg_memory,
        peak_memory_mb=peak_memory,
        avg_cpu_percent=avg_cpu,
        sync_lag_ms=sync_lag_ms,
        writes_completed=writes_completed[0],
        syncs_observed=0,
        bottleneck=bottleneck,
        write_latency_p50_ms=p50,
        write_latency_p99_ms=p99,
        db_size_mb=db_size_end_mb,
        wal_size_mb=wal_size_end_mb,
    )


def cleanup_s3_prefix(bucket: str, endpoint: Optional[str], prefix: str) -> None:
    """Delete all objects with given prefix from S3."""
    try:
        bucket_name = bucket.replace("s3://", "").split("/")[0]
        kwargs = {}
        if endpoint:
            kwargs["endpoint_url"] = endpoint

        s3 = boto3.client("s3", **kwargs)

        # List and delete objects with prefix
        paginator = s3.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=bucket_name, Prefix=prefix):
            if "Contents" in page:
                objects = [{"Key": obj["Key"]} for obj in page["Contents"]]
                if objects:
                    s3.delete_objects(Bucket=bucket_name, Delete={"Objects": objects})
                    print(f"  Deleted {len(objects)} objects with prefix '{prefix}'")
    except Exception as e:
        print(f"  Warning: Cleanup failed: {e}")


def estimate_sync_lag(bucket: str, endpoint: Optional[str], db_name: str) -> float:
    """
    Estimate sync lag by checking when the last S3 object was created.
    Returns lag in milliseconds.
    """
    try:
        bucket_name = bucket.replace("s3://", "").split("/")[0]

        kwargs = {}
        if endpoint:
            kwargs["endpoint_url"] = endpoint

        s3 = boto3.client("s3", **kwargs)

        # List recent objects for this database
        prefix = f"{db_name}/"
        response = s3.list_objects_v2(
            Bucket=bucket_name,
            Prefix=prefix,
            MaxKeys=10,
        )

        if "Contents" not in response or not response["Contents"]:
            return 0  # No objects yet

        # Get the most recent object's timestamp
        latest = max(response["Contents"], key=lambda x: x["LastModified"])
        last_sync = latest["LastModified"].timestamp()

        # Lag is time since last sync
        lag_secs = time.time() - last_sync
        return lag_secs * 1000  # Convert to ms

    except Exception as e:
        print(f"  Warning: Could not estimate sync lag: {e}")
        return -1


def main():
    # Load config from .env
    config = get_config()

    parser = argparse.ArgumentParser(description="Stress test walrust throughput")
    parser.add_argument("--bucket", default=config.bucket_url, help="S3 bucket URL")
    parser.add_argument("--endpoint", default=config.endpoint, help="S3 endpoint URL")
    parser.add_argument(
        "--rates",
        default="10,50,100,200,500,1000",
        help="Comma-separated write rates to test (writes/sec)",
    )
    parser.add_argument(
        "--duration",
        type=float,
        default=10.0,
        help="Duration per rate test in seconds",
    )
    parser.add_argument(
        "--dbs",
        type=int,
        default=1,
        help="Number of databases to write to concurrently",
    )
    parser.add_argument(
        "--cpu-limit",
        type=int,
        default=None,
        help="Limit CPU usage to this %% (requires: brew install cpulimit)",
    )
    parser.add_argument(
        "--tool",
        choices=["walrust", "litestream"],
        default="walrust",
        help="Tool to benchmark (default: walrust)",
    )
    parser.add_argument(
        "--db-size",
        type=int,
        default=0,
        help="Pre-populate database to this size in MB (default: 0 = empty)",
    )

    args = parser.parse_args()
    rates = [int(r) for r in args.rates.split(",")]

    print(f"{args.tool.upper()} Stress Test")
    print(f"Bucket: {args.bucket}")
    print(f"Endpoint: {args.endpoint or 'default'}")
    print(f"Write rates: {rates} writes/sec")
    print(f"Duration per test: {args.duration}s")
    if args.db_size:
        print(f"Initial DB size: {args.db_size}MB")
    if args.cpu_limit:
        print(f"CPU limit: {args.cpu_limit}%")
    print()

    results = []

    with tempfile.TemporaryDirectory() as tmpdir:
        tmpdir = Path(tmpdir)
        db_path = tmpdir / "stress_test.db"
        create_test_database(db_path, size_mb=args.db_size)

        print(f"{'Rate':>8} | {'Actual':>8} | {'p50':>7} | {'p99':>7} | {'Mem avg':>8} | {'Mem peak':>9} | {'CPU':>6} | {'DB':>7} | {'WAL':>7} | {'Bottleneck':>10}")
        print("-" * 115)

        for rate in rates:
            print(f"Testing {rate} writes/sec...", end="", flush=True)

            result = run_stress_test(
                db_path=db_path,
                bucket=args.bucket,
                endpoint=args.endpoint,
                write_rate=rate,
                duration_secs=args.duration,
                cpu_limit=args.cpu_limit,
                tool=args.tool,
            )
            results.append(result)

            # Clear the "Testing..." line
            print(f"\r{rate:>8} | {result.actual_rate:>8.1f} | {result.write_latency_p50_ms:>5.1f}ms | {result.write_latency_p99_ms:>5.1f}ms | {result.avg_memory_mb:>6.1f}MB | {result.peak_memory_mb:>7.1f}MB | {result.avg_cpu_percent:>5.1f}% | {result.db_size_mb:>5.1f}MB | {result.wal_size_mb:>5.1f}MB | {result.bottleneck:>10}")

            # Stop if actual rate is way below target
            if result.actual_rate < rate * 0.35:
                print(f"\nCannot sustain target rate (achieved {result.actual_rate:.1f} of {rate})")
                if result.bottleneck != "none":
                    print(f"Bottleneck: {result.bottleneck}")
                break

    print()
    print("=" * 90)
    print("SUMMARY")
    print("=" * 90)

    # Find max sustainable rate (highest rate where actual >= 70% of target)
    max_sustainable = 0
    peak_actual = 0
    for r in results:
        peak_actual = max(peak_actual, r.actual_rate)
        if r.actual_rate >= r.write_rate * 0.7:
            max_sustainable = r.write_rate

    print(f"Max sustainable write rate: {max_sustainable} writes/sec")
    print(f"Peak achieved throughput: {peak_actual:.0f} writes/sec")

    if results:
        final = results[-1]
        print(f"Final bottleneck: {final.bottleneck}")
        print(f"Peak memory: {max(r.peak_memory_mb for r in results):.1f} MB")
        print(f"Peak CPU: {max(r.avg_cpu_percent for r in results):.1f}%")
        print(f"Final DB size: {final.db_size_mb:.1f} MB")
        print(f"Final WAL size: {final.wal_size_mb:.1f} MB")
        print(f"Write latency at max rate: p50={final.write_latency_p50_ms:.1f}ms, p99={final.write_latency_p99_ms:.1f}ms")
        if final.bottleneck == "sqlite":
            print(f"  -> SQLite commit throughput ceiling (~{peak_actual:.0f} w/s)")
            print(f"     Per-write latency is low — throughput limited by fsync/WAL overhead, not walrust")
        elif final.bottleneck == "cpu":
            print(f"  -> CPU-bound (walrust checksum/encode overhead)")
        elif final.bottleneck == "network":
            print(f"  -> Network-bound (S3 upload can't keep up)")

    # Cleanup S3
    print()
    print("Cleaning up S3 test data...")
    cleanup_s3_prefix(args.bucket, args.endpoint, "stress_test")
    print("Done.")


if __name__ == "__main__":
    main()
