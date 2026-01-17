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
    bottleneck: str  # "none", "cpu", "memory", "network"


def create_test_database(path: Path) -> None:
    """Create a test SQLite database."""
    conn = sqlite3.connect(str(path))
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("CREATE TABLE IF NOT EXISTS data (id INTEGER PRIMARY KEY, ts REAL, value BLOB)")
    conn.commit()
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
    write_timestamps = queue.Queue()
    writes_completed = [0]
    stop_flag = threading.Event()

    def writer():
        """Write to database at target rate."""
        conn = sqlite3.connect(str(db_path))
        interval = 1.0 / write_rate if write_rate > 0 else 1.0
        chunk = b"x" * 100  # Small payload

        while not stop_flag.is_set():
            start = time.time()
            try:
                ts = time.time()
                conn.execute("INSERT INTO data (ts, value) VALUES (?, ?)", (ts, chunk))
                conn.commit()
                write_timestamps.put(ts)
                writes_completed[0] += 1
            except Exception as e:
                print(f"  Write error: {e}")

            elapsed = time.time() - start
            sleep_time = max(0, interval - elapsed)
            if sleep_time > 0:
                time.sleep(sleep_time)

        conn.close()

    def monitor():
        """Monitor memory and CPU."""
        while not stop_flag.is_set():
            try:
                p = psutil.Process(proc.pid)
                memory_samples.append(p.memory_info().rss / (1024 * 1024))
                cpu_samples.append(p.cpu_percent())
            except (psutil.NoSuchProcess, psutil.AccessDenied):
                break
            time.sleep(0.1)

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

    # Calculate metrics
    actual_rate = writes_completed[0] / duration_secs if duration_secs > 0 else 0
    avg_memory = sum(memory_samples) / len(memory_samples) if memory_samples else 0
    peak_memory = max(memory_samples) if memory_samples else 0
    avg_cpu = sum(cpu_samples) / len(cpu_samples) if cpu_samples else 0

    # Estimate sync lag by checking S3
    # (This is approximate - we check if recent writes are in S3)
    sync_lag_ms = estimate_sync_lag(bucket, endpoint, db_path.stem)

    # Determine bottleneck
    bottleneck = "none"
    if avg_cpu > 90:
        bottleneck = "cpu"
    elif peak_memory > 500:  # Arbitrary threshold
        bottleneck = "memory"
    elif sync_lag_ms > 5000:  # 5 second lag
        bottleneck = "network"

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
        syncs_observed=0,  # TODO: count S3 objects
        bottleneck=bottleneck,
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

    args = parser.parse_args()
    rates = [int(r) for r in args.rates.split(",")]

    print(f"{args.tool.upper()} Stress Test")
    print(f"Bucket: {args.bucket}")
    print(f"Endpoint: {args.endpoint or 'default'}")
    print(f"Write rates: {rates} writes/sec")
    print(f"Duration per test: {args.duration}s")
    if args.cpu_limit:
        print(f"CPU limit: {args.cpu_limit}%")
    print()

    results = []

    with tempfile.TemporaryDirectory() as tmpdir:
        tmpdir = Path(tmpdir)
        db_path = tmpdir / "stress_test.db"
        create_test_database(db_path)

        print(f"{'Rate':>8} | {'Actual':>8} | {'Memory':>10} | {'CPU':>6} | {'Lag':>10} | {'Bottleneck':>12}")
        print("-" * 75)

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
            print(f"\r{rate:>8} | {result.actual_rate:>8.1f} | {result.avg_memory_mb:>7.1f} MB | {result.avg_cpu_percent:>5.1f}% | {result.sync_lag_ms:>7.0f} ms | {result.bottleneck:>12}")

            # Stop if we hit a bottleneck
            if result.bottleneck != "none":
                print(f"\nBottleneck detected: {result.bottleneck}")
                break

            # Also stop if actual rate is way below target
            if result.actual_rate < rate * 0.5:
                print(f"\nCannot sustain target rate (achieved {result.actual_rate:.1f} of {rate})")
                break

    print()
    print("=" * 75)
    print("SUMMARY")
    print("=" * 75)

    # Find max sustainable rate
    max_sustainable = 0
    for r in results:
        if r.actual_rate >= r.write_rate * 0.9:  # Within 90% of target
            max_sustainable = r.write_rate

    print(f"Max sustainable write rate: {max_sustainable} writes/sec")

    if results:
        final = results[-1]
        print(f"Final bottleneck: {final.bottleneck}")
        print(f"Peak memory: {max(r.peak_memory_mb for r in results):.1f} MB")
        print(f"Peak CPU: {max(r.avg_cpu_percent for r in results):.1f}%")

    # Cleanup S3
    print()
    print("Cleaning up S3 test data...")
    cleanup_s3_prefix(args.bucket, args.endpoint, "stress_test")
    print("Done.")


if __name__ == "__main__":
    main()
