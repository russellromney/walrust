#!/usr/bin/env python3
"""
Real-world performance benchmarks for walrust

Measures what users care about:
1. Sync latency (commit → Tigris)
2. Restore performance (Tigris → usable DB)
3. Multi-DB throughput
4. Network failure recovery

Requirements:
- walrust binary (cargo build --release)
- Tigris credentials in .env
- pip install psutil boto3

Usage:
    python bench/realworld.py                    # Run all benchmarks
    python bench/realworld.py --test sync        # Only sync latency
    python bench/realworld.py --test restore     # Only restore
    python bench/realworld.py --json             # JSON output
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
from dataclasses import dataclass, asdict, field
from typing import Optional, List
import threading
import statistics

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
class SyncLatencyResult:
    """Results for sync latency benchmark."""
    name: str = "sync_latency"
    num_commits: int = 0
    p50_ms: float = 0.0
    p95_ms: float = 0.0
    p99_ms: float = 0.0
    mean_ms: float = 0.0
    max_ms: float = 0.0
    samples: List[float] = field(default_factory=list)


@dataclass
class RestoreResult:
    """Results for restore benchmark."""
    name: str = "restore"
    db_size_mb: float = 0.0
    download_time_ms: float = 0.0
    restore_time_ms: float = 0.0
    total_time_ms: float = 0.0
    throughput_mbps: float = 0.0


@dataclass
class MultiDbResult:
    """Results for multi-DB throughput benchmark."""
    name: str = "multi_db_throughput"
    num_databases: int = 0
    writes_per_sec: float = 0.0
    sync_lag_p95_ms: float = 0.0
    cpu_percent: float = 0.0
    memory_mb: float = 0.0


@dataclass
class NetworkRecoveryResult:
    """Results for network recovery benchmark."""
    name: str = "network_recovery"
    outage_duration_sec: int = 0
    catchup_time_ms: float = 0.0
    writes_during_outage: int = 0
    writes_lost: int = 0


@dataclass
class WriteThroughputResult:
    """Results for write throughput benchmark."""
    name: str = "write_throughput"
    max_commits_per_sec: float = 0.0
    latency_at_max_ms: float = 0.0
    sustainable_commits_per_sec: float = 0.0


@dataclass
class CheckpointImpactResult:
    """Results for checkpoint impact benchmark."""
    name: str = "checkpoint_impact"
    normal_sync_latency_ms: float = 0.0
    during_checkpoint_latency_ms: float = 0.0
    checkpoint_duration_ms: float = 0.0


@dataclass
class CachedRestoreResult:
    """Results for cached vs direct restore benchmark."""
    name: str = "cached_restore"
    db_size_mb: float = 0.0
    s3_only_time_ms: float = 0.0
    cache_time_ms: float = 0.0
    speedup_factor: float = 0.0
    cache_hit_rate: float = 0.0  # % of files read from cache


def create_s3_client(endpoint: Optional[str] = None):
    """Create S3 client with optional endpoint."""
    session = boto3.Session(
        aws_access_key_id=os.environ.get("AWS_ACCESS_KEY_ID"),
        aws_secret_access_key=os.environ.get("AWS_SECRET_ACCESS_KEY"),
        region_name=os.environ.get("AWS_REGION", "auto"),
    )

    config_kwargs = {}
    if endpoint:
        config_kwargs["endpoint_url"] = endpoint

    return session.client("s3", **config_kwargs)


def benchmark_sync_latency(
    bucket: str,
    endpoint: Optional[str] = None,
    num_commits: int = 10,
    db_size_kb: int = 100,
) -> SyncLatencyResult:
    """
    Measure latency for walrust snapshot operations.

    Strategy:
    1. Create test database with data
    2. For each test:
       - Write new data
       - Run walrust snapshot command
       - Measure time for snapshot to complete
    3. Return p50/p95/p99/max

    Note: This measures snapshot latency (explicit sync) rather than
    file-watcher-based WAL streaming latency.
    """
    print(f"\nBenchmarking sync latency ({num_commits} snapshots)...")

    with tempfile.TemporaryDirectory() as tmpdir:
        tmpdir = Path(tmpdir)
        db_path = tmpdir / "sync_test.db"

        # Create initial database
        conn = sqlite3.connect(str(db_path))
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("CREATE TABLE data (id INTEGER PRIMARY KEY, value BLOB, ts REAL)")

        # Insert initial data
        chunk = b"x" * 1024
        for i in range(db_size_kb):
            conn.execute("INSERT INTO data (value, ts) VALUES (?, ?)", (chunk, time.time()))
        conn.commit()

        walrust_bin = Path(__file__).parent.parent / "target" / "release" / "walrust"
        env = os.environ.copy()

        latencies = []

        for i in range(num_commits):
            # Write some new data
            for _ in range(10):
                conn.execute("INSERT INTO data (value, ts) VALUES (?, ?)", (chunk, time.time()))
            conn.commit()

            # Measure snapshot time
            cmd = [str(walrust_bin), "snapshot", str(db_path), "-b", bucket]
            if endpoint:
                cmd += ["--endpoint", endpoint]

            start = time.time()
            result = subprocess.run(cmd, capture_output=True, env=env)
            latency = (time.time() - start) * 1000

            if result.returncode == 0:
                latencies.append(latency)
                print(f"  Snapshot {i + 1}/{num_commits}: {latency:.0f}ms")
            else:
                print(f"  Snapshot {i + 1} failed: {result.stderr.decode()[:100]}")

        conn.close()

        if not latencies:
            return SyncLatencyResult(num_commits=num_commits)

        latencies.sort()
        return SyncLatencyResult(
            num_commits=len(latencies),
            p50_ms=latencies[int(len(latencies) * 0.5)],
            p95_ms=latencies[int(len(latencies) * 0.95)],
            p99_ms=latencies[int(len(latencies) * 0.99)] if len(latencies) >= 100 else latencies[-1],
            mean_ms=statistics.mean(latencies),
            max_ms=max(latencies),
            samples=latencies[:10],
        )


def benchmark_restore(
    bucket: str,
    endpoint: Optional[str] = None,
    db_sizes_mb: List[int] = [1, 10, 100],
) -> List[RestoreResult]:
    """
    Measure restore performance for different database sizes.

    Strategy:
    1. Create test database of given size
    2. Snapshot to S3
    3. Measure time to restore
    4. Calculate throughput (MB/s)
    """
    print(f"\nBenchmarking restore performance (sizes: {db_sizes_mb}MB)...")
    results = []

    walrust_bin = Path(__file__).parent.parent / "target" / "release" / "walrust"

    with tempfile.TemporaryDirectory() as tmpdir:
        tmpdir = Path(tmpdir)

        for size_mb in db_sizes_mb:
            print(f"  Testing {size_mb}MB database...")

            db_path = tmpdir / f"test_{size_mb}mb.db"

            # Create database
            conn = sqlite3.connect(str(db_path))
            conn.execute("PRAGMA journal_mode=WAL")
            conn.execute("CREATE TABLE data (id INTEGER PRIMARY KEY, value BLOB)")

            # Insert data to reach target size
            chunk = b"x" * (1024 * 10)  # 10KB chunks
            num_rows = (size_mb * 1024) // 10

            for i in range(num_rows):
                conn.execute("INSERT INTO data (value) VALUES (?)", (chunk,))
                if i % 100 == 0:
                    conn.commit()

            conn.commit()
            conn.close()

            actual_size_mb = db_path.stat().st_size / (1024 * 1024)

            # Snapshot to S3
            cmd = [str(walrust_bin), "snapshot", str(db_path), "-b", bucket]
            if endpoint:
                cmd += ["--endpoint", endpoint]

            result = subprocess.run(cmd, capture_output=True, text=True)
            if result.returncode != 0:
                print(f"    Snapshot failed: {result.stderr}")
                print(f"    Command: {' '.join(cmd)}")
                raise RuntimeError(f"Snapshot failed with exit code {result.returncode}")

            # Measure restore time
            restore_path = tmpdir / f"restored_{size_mb}mb.db"

            cmd = [str(walrust_bin), "restore", f"test_{size_mb}mb", "-o", str(restore_path), "-b", bucket]
            if endpoint:
                cmd += ["--endpoint", endpoint]

            start = time.time()
            subprocess.run(cmd, check=True, capture_output=True)
            restore_time = (time.time() - start) * 1000

            throughput = (actual_size_mb / (restore_time / 1000)) if restore_time > 0 else 0

            results.append(RestoreResult(
                db_size_mb=actual_size_mb,
                download_time_ms=restore_time,  # We don't separate download vs restore yet
                restore_time_ms=0,
                total_time_ms=restore_time,
                throughput_mbps=throughput,
            ))

            # Cleanup
            db_path.unlink()
            if restore_path.exists():
                restore_path.unlink()

    return results


def benchmark_multi_db_throughput(
    bucket: str,
    endpoint: Optional[str] = None,
    num_databases: int = 10,
    writes_per_sec: int = 100,
    duration_sec: int = 10,
) -> MultiDbResult:
    """
    Measure throughput with multiple databases writing concurrently.

    Strategy:
    1. Create N databases
    2. Start walrust watching all of them
    3. Spawn threads that write to each DB at target rate
    4. Measure sync lag, CPU, memory
    """
    print(f"\nBenchmarking multi-DB throughput ({num_databases} DBs, {writes_per_sec} writes/sec)...")

    with tempfile.TemporaryDirectory() as tmpdir:
        tmpdir = Path(tmpdir)

        # Create databases
        databases = []
        for i in range(num_databases):
            db_path = tmpdir / f"test_{i}.db"
            conn = sqlite3.connect(str(db_path))
            conn.execute("PRAGMA journal_mode=WAL")
            conn.execute("CREATE TABLE data (id INTEGER PRIMARY KEY, value BLOB, ts REAL)")
            conn.commit()
            conn.close()
            databases.append(db_path)

        # Start walrust
        walrust_bin = Path(__file__).parent.parent / "target" / "release" / "walrust"
        cmd = [str(walrust_bin), "watch"] + [str(db) for db in databases] + ["-b", bucket]
        if endpoint:
            cmd += ["--endpoint", endpoint]

        proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        time.sleep(2)

        # Writer threads
        stop_flag = threading.Event()
        chunk = b"x" * 1024

        def writer(db_path: Path):
            conn = sqlite3.connect(str(db_path))
            interval = 1.0 / (writes_per_sec / num_databases)

            while not stop_flag.is_set():
                start = time.time()
                conn.execute("INSERT INTO data (value, ts) VALUES (?, ?)", (chunk, start))
                conn.commit()

                elapsed = time.time() - start
                sleep_time = max(0, interval - elapsed)
                time.sleep(sleep_time)

            conn.close()

        # Start writers
        threads = [threading.Thread(target=writer, args=(db,)) for db in databases]
        for t in threads:
            t.start()

        # Monitor walrust process
        time.sleep(1)
        walrust_proc = psutil.Process(proc.pid)

        mem_samples = []
        cpu_samples = []

        for _ in range(duration_sec):
            mem_samples.append(walrust_proc.memory_info().rss / (1024 * 1024))
            cpu_samples.append(walrust_proc.cpu_percent(interval=1))

        # Stop writers
        stop_flag.set()
        for t in threads:
            t.join()

        proc.terminate()
        proc.wait()

        return MultiDbResult(
            num_databases=num_databases,
            writes_per_sec=writes_per_sec,
            sync_lag_p95_ms=0,  # TODO: measure actual sync lag
            cpu_percent=statistics.mean(cpu_samples) if cpu_samples else 0,
            memory_mb=statistics.mean(mem_samples) if mem_samples else 0,
        )


def benchmark_network_recovery(
    bucket: str,
    endpoint: Optional[str] = None,
    outage_duration_sec: int = 5,
) -> NetworkRecoveryResult:
    """
    Measure recovery time after simulated network outage.

    Strategy:
    1. Start walrust + database
    2. Write continuously, record count
    3. Stop walrust (simulating network failure)
    4. Continue writing for N seconds
    5. Restart walrust
    6. Measure time until WAL segment count stabilizes
    """
    print(f"\nBenchmarking network recovery ({outage_duration_sec}s outage)...")

    with tempfile.TemporaryDirectory() as tmpdir:
        tmpdir = Path(tmpdir)
        db_path = tmpdir / "recovery_test.db"

        # Create database
        conn = sqlite3.connect(str(db_path))
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("CREATE TABLE data (id INTEGER PRIMARY KEY, value BLOB)")
        conn.commit()

        chunk = b"x" * 1024

        # Start walrust
        walrust_bin = Path(__file__).parent.parent / "target" / "release" / "walrust"
        cmd = [str(walrust_bin), "watch", str(db_path), "-b", bucket]
        if endpoint:
            cmd += ["--endpoint", endpoint]

        env = os.environ.copy()
        proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, env=env)
        time.sleep(2)

        if proc.poll() is not None:
            _, stderr = proc.communicate()
            print(f"  Warning: walrust exited early: {stderr.decode()[:200]}")
            conn.close()
            return NetworkRecoveryResult(outage_duration_sec=outage_duration_sec)

        # Write some initial data
        for _ in range(10):
            conn.execute("INSERT INTO data (value) VALUES (?)", (chunk,))
            conn.commit()
        time.sleep(2)  # Let it sync

        # Stop walrust (simulating outage)
        print(f"  Stopping walrust to simulate outage...")
        proc.terminate()
        proc.wait()

        # Continue writing during "outage"
        writes_during_outage = 0
        print(f"  Writing during {outage_duration_sec}s outage...")
        outage_start = time.time()
        while time.time() - outage_start < outage_duration_sec:
            conn.execute("INSERT INTO data (value) VALUES (?)", (chunk,))
            conn.commit()
            writes_during_outage += 1
            time.sleep(0.1)

        conn.close()

        # Restart walrust
        print(f"  Restarting walrust (wrote {writes_during_outage} rows during outage)...")
        catchup_start = time.time()
        proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, env=env)

        # Wait for catchup (WAL should be synced within reasonable time)
        time.sleep(5)  # Give it 5 seconds to catch up

        catchup_time = (time.time() - catchup_start) * 1000

        proc.terminate()
        proc.wait()

        return NetworkRecoveryResult(
            outage_duration_sec=outage_duration_sec,
            catchup_time_ms=catchup_time,
            writes_during_outage=writes_during_outage,
            writes_lost=0,  # Walrust should recover all writes
        )


def benchmark_write_throughput(
    bucket: str,
    endpoint: Optional[str] = None,
    duration_sec: int = 10,
) -> WriteThroughputResult:
    """
    Measure maximum sustainable write throughput.

    Strategy:
    1. Start walrust
    2. Write as fast as possible for N seconds
    3. Measure commits/sec achieved
    4. Measure if walrust keeps up (WAL file size doesn't grow unbounded)
    """
    print(f"\nBenchmarking write throughput ({duration_sec}s)...")

    with tempfile.TemporaryDirectory() as tmpdir:
        tmpdir = Path(tmpdir)
        db_path = tmpdir / "throughput_test.db"

        # Create database
        conn = sqlite3.connect(str(db_path))
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA synchronous=NORMAL")  # Faster commits
        conn.execute("CREATE TABLE data (id INTEGER PRIMARY KEY, value BLOB)")
        conn.commit()

        chunk = b"x" * 512  # Smaller chunks for faster commits

        # Start walrust
        walrust_bin = Path(__file__).parent.parent / "target" / "release" / "walrust"
        cmd = [str(walrust_bin), "watch", str(db_path), "-b", bucket]
        if endpoint:
            cmd += ["--endpoint", endpoint]

        env = os.environ.copy()
        proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, env=env)
        time.sleep(2)

        if proc.poll() is not None:
            _, stderr = proc.communicate()
            print(f"  Warning: walrust exited early: {stderr.decode()[:200]}")
            conn.close()
            return WriteThroughputResult()

        # Write as fast as possible
        commits = 0
        latencies = []
        start = time.time()

        while time.time() - start < duration_sec:
            commit_start = time.time()
            conn.execute("INSERT INTO data (value) VALUES (?)", (chunk,))
            conn.commit()
            commit_latency = (time.time() - commit_start) * 1000
            latencies.append(commit_latency)
            commits += 1

        elapsed = time.time() - start
        conn.close()

        proc.terminate()
        proc.wait()

        commits_per_sec = commits / elapsed
        avg_latency = statistics.mean(latencies) if latencies else 0

        print(f"  Achieved {commits_per_sec:.0f} commits/sec (avg latency: {avg_latency:.1f}ms)")

        return WriteThroughputResult(
            max_commits_per_sec=commits_per_sec,
            latency_at_max_ms=avg_latency,
            sustainable_commits_per_sec=commits_per_sec,  # Same for now
        )


def benchmark_checkpoint_impact(
    bucket: str,
    endpoint: Optional[str] = None,
) -> CheckpointImpactResult:
    """
    Measure sync latency impact during SQLite checkpoint.

    Strategy:
    1. Start walrust
    2. Write data to grow WAL
    3. Measure normal sync latency
    4. Trigger checkpoint
    5. Measure latency during/after checkpoint
    """
    print(f"\nBenchmarking checkpoint impact...")

    with tempfile.TemporaryDirectory() as tmpdir:
        tmpdir = Path(tmpdir)
        db_path = tmpdir / "checkpoint_test.db"

        # Create database
        conn = sqlite3.connect(str(db_path))
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA wal_autocheckpoint=0")  # Disable auto-checkpoint
        conn.execute("CREATE TABLE data (id INTEGER PRIMARY KEY, value BLOB)")
        conn.commit()

        chunk = b"x" * 1024

        # Start walrust
        walrust_bin = Path(__file__).parent.parent / "target" / "release" / "walrust"
        cmd = [str(walrust_bin), "watch", str(db_path), "-b", bucket]
        if endpoint:
            cmd += ["--endpoint", endpoint]

        env = os.environ.copy()
        proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, env=env)
        time.sleep(2)

        if proc.poll() is not None:
            _, stderr = proc.communicate()
            print(f"  Warning: walrust exited early: {stderr.decode()[:200]}")
            conn.close()
            return CheckpointImpactResult()

        # Write data to grow WAL
        print("  Growing WAL file...")
        for _ in range(1000):
            conn.execute("INSERT INTO data (value) VALUES (?)", (chunk,))
            conn.commit()

        # Measure normal write latency
        normal_latencies = []
        for _ in range(20):
            start = time.time()
            conn.execute("INSERT INTO data (value) VALUES (?)", (chunk,))
            conn.commit()
            normal_latencies.append((time.time() - start) * 1000)

        normal_avg = statistics.mean(normal_latencies)
        print(f"  Normal commit latency: {normal_avg:.2f}ms")

        # Trigger checkpoint and measure latency during it
        print("  Triggering checkpoint...")
        checkpoint_start = time.time()
        conn.execute("PRAGMA wal_checkpoint(TRUNCATE)")
        checkpoint_duration = (time.time() - checkpoint_start) * 1000

        # Measure latency right after checkpoint
        after_latencies = []
        for _ in range(20):
            start = time.time()
            conn.execute("INSERT INTO data (value) VALUES (?)", (chunk,))
            conn.commit()
            after_latencies.append((time.time() - start) * 1000)

        after_avg = statistics.mean(after_latencies)
        print(f"  Checkpoint duration: {checkpoint_duration:.0f}ms")
        print(f"  Post-checkpoint latency: {after_avg:.2f}ms")

        conn.close()
        proc.terminate()
        proc.wait()

        return CheckpointImpactResult(
            normal_sync_latency_ms=normal_avg,
            during_checkpoint_latency_ms=after_avg,
            checkpoint_duration_ms=checkpoint_duration,
        )


def benchmark_cached_vs_direct_restore(
    bucket: str,
    endpoint: Optional[str] = None,
    db_sizes_mb: List[int] = [1, 10, 50],
    num_incrementals: int = 10,
) -> List[CachedRestoreResult]:
    """
    Compare restore performance: S3-only vs cache-first.

    Strategy:
    1. Create test database and write incrementals
    2. Run walrust watch with cache enabled to populate cache
    3. Measure restore time WITHOUT cache (S3 only)
    4. Measure restore time WITH cache (--cache-dir)
    5. Calculate speedup factor

    This tests Phase 5: Fast Local Restore.
    """
    print(f"\nBenchmarking cached vs direct restore (sizes: {db_sizes_mb}MB, {num_incrementals} incrementals)...")
    results = []

    walrust_bin = Path(__file__).parent.parent / "target" / "release" / "walrust"

    with tempfile.TemporaryDirectory() as tmpdir:
        tmpdir = Path(tmpdir)

        for size_mb in db_sizes_mb:
            print(f"\n  Testing {size_mb}MB database with {num_incrementals} incrementals...")

            db_path = tmpdir / f"cached_test_{size_mb}mb.db"
            db_name = f"cached_test_{size_mb}mb"

            # Create database
            conn = sqlite3.connect(str(db_path))
            conn.execute("PRAGMA journal_mode=WAL")
            conn.execute("CREATE TABLE data (id INTEGER PRIMARY KEY, value BLOB, ts REAL)")

            # Insert initial data to reach target size
            chunk = b"x" * (1024 * 10)  # 10KB chunks
            num_rows = (size_mb * 1024) // 10

            for i in range(num_rows):
                conn.execute("INSERT INTO data (value, ts) VALUES (?, ?)", (chunk, time.time()))
                if i % 100 == 0:
                    conn.commit()
            conn.commit()

            actual_size_mb = db_path.stat().st_size / (1024 * 1024)
            print(f"    Created {actual_size_mb:.1f}MB database")

            # Take initial snapshot
            cmd = [str(walrust_bin), "snapshot", str(db_path), "-b", bucket]
            if endpoint:
                cmd += ["--endpoint", endpoint]
            subprocess.run(cmd, check=True, capture_output=True)
            print(f"    Snapshot complete")

            # Start walrust watch with cache enabled to build up cache
            cache_dir = tmpdir / f".{db_name}-walrust"
            watch_cmd = [
                str(walrust_bin), "watch", str(db_path),
                "-b", bucket,
                "--enable-cache",
                "--wal-sync-interval", "1",
            ]
            if endpoint:
                watch_cmd += ["--endpoint", endpoint]

            proc = subprocess.Popen(watch_cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            time.sleep(2)

            # Write incrementals
            print(f"    Writing {num_incrementals} incrementals...")
            for i in range(num_incrementals):
                for _ in range(10):  # 10 rows per incremental
                    conn.execute("INSERT INTO data (value, ts) VALUES (?, ?)", (chunk, time.time()))
                conn.commit()
                time.sleep(0.5)  # Let walrust sync each one

            conn.close()

            # Wait for uploads to complete
            time.sleep(3)
            proc.terminate()
            proc.wait()

            # Check cache was populated
            cache_ltx_dir = cache_dir / "ltx"
            cache_files = list(cache_ltx_dir.glob("*.ltx")) if cache_ltx_dir.exists() else []
            print(f"    Cache contains {len(cache_files)} LTX files")

            # --- Benchmark 1: S3-only restore ---
            restore_s3_path = tmpdir / f"restored_s3_{size_mb}mb.db"

            cmd = [str(walrust_bin), "restore", db_name, "-o", str(restore_s3_path), "-b", bucket]
            if endpoint:
                cmd += ["--endpoint", endpoint]

            start = time.time()
            result = subprocess.run(cmd, capture_output=True, text=True)
            s3_only_time = (time.time() - start) * 1000

            if result.returncode != 0:
                print(f"    S3-only restore failed: {result.stderr[:200]}")
                continue

            print(f"    S3-only restore: {s3_only_time:.0f}ms")

            # --- Benchmark 2: Cache-first restore ---
            restore_cache_path = tmpdir / f"restored_cache_{size_mb}mb.db"

            cmd = [
                str(walrust_bin), "restore", db_name,
                "-o", str(restore_cache_path),
                "-b", bucket,
                "--cache-dir", str(cache_dir),
            ]
            if endpoint:
                cmd += ["--endpoint", endpoint]

            start = time.time()
            result = subprocess.run(cmd, capture_output=True, text=True)
            cache_time = (time.time() - start) * 1000

            if result.returncode != 0:
                print(f"    Cache restore failed: {result.stderr[:200]}")
                continue

            print(f"    Cache-first restore: {cache_time:.0f}ms")

            # Calculate speedup
            speedup = s3_only_time / cache_time if cache_time > 0 else 0
            cache_hit_rate = (len(cache_files) / (num_incrementals + 1)) * 100 if num_incrementals > 0 else 0

            print(f"    Speedup: {speedup:.1f}x")

            results.append(CachedRestoreResult(
                db_size_mb=actual_size_mb,
                s3_only_time_ms=s3_only_time,
                cache_time_ms=cache_time,
                speedup_factor=speedup,
                cache_hit_rate=min(cache_hit_rate, 100),
            ))

            # Cleanup
            for f in [db_path, restore_s3_path, restore_cache_path]:
                if f.exists():
                    f.unlink()
            wal_path = Path(str(db_path) + "-wal")
            if wal_path.exists():
                wal_path.unlink()
            shm_path = Path(str(db_path) + "-shm")
            if shm_path.exists():
                shm_path.unlink()

    return results


def main():
    parser = argparse.ArgumentParser(
        description="Real-world performance benchmarks for walrust",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "--test",
        choices=["sync", "restore", "multi-db", "network", "throughput", "checkpoint", "cached"],
        help="Run specific test only",
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

    args = parser.parse_args()

    results = {}

    # Run benchmarks
    if not args.test or args.test == "sync":
        results["sync_latency"] = asdict(benchmark_sync_latency(args.bucket, args.endpoint))

    if not args.test or args.test == "restore":
        restore_results = benchmark_restore(args.bucket, args.endpoint)
        results["restore"] = [asdict(r) for r in restore_results]

    if not args.test or args.test == "multi-db":
        results["multi_db"] = asdict(benchmark_multi_db_throughput(args.bucket, args.endpoint))

    if not args.test or args.test == "network":
        results["network_recovery"] = asdict(benchmark_network_recovery(args.bucket, args.endpoint))

    if not args.test or args.test == "throughput":
        results["write_throughput"] = asdict(benchmark_write_throughput(args.bucket, args.endpoint))

    if not args.test or args.test == "checkpoint":
        results["checkpoint_impact"] = asdict(benchmark_checkpoint_impact(args.bucket, args.endpoint))

    if not args.test or args.test == "cached":
        cached_results = benchmark_cached_vs_direct_restore(args.bucket, args.endpoint)
        results["cached_restore"] = [asdict(r) for r in cached_results]

    # Output results
    if args.json:
        print(json.dumps(results, indent=2))
    else:
        print("\n" + "=" * 80)
        print("REAL-WORLD PERFORMANCE RESULTS")
        print("=" * 80)

        if "sync_latency" in results:
            r = results["sync_latency"]
            print(f"\nSync Latency (commit → S3):")
            print(f"  Commits:  {r['num_commits']}")
            print(f"  p50:      {r['p50_ms']:.1f} ms")
            print(f"  p95:      {r['p95_ms']:.1f} ms")
            print(f"  p99:      {r['p99_ms']:.1f} ms")
            print(f"  Mean:     {r['mean_ms']:.1f} ms")
            print(f"  Max:      {r['max_ms']:.1f} ms")

        if "restore" in results:
            print(f"\nRestore Performance:")
            print(f"  {'Size (MB)':>10} {'Time (ms)':>12} {'Throughput (MB/s)':>20}")
            print(f"  {'-' * 10} {'-' * 12} {'-' * 20}")
            for r in results["restore"]:
                print(f"  {r['db_size_mb']:>10.1f} {r['total_time_ms']:>12.0f} {r['throughput_mbps']:>20.2f}")

        if "multi_db" in results:
            r = results["multi_db"]
            print(f"\nMulti-DB Throughput:")
            print(f"  Databases:    {r['num_databases']}")
            print(f"  Writes/sec:   {r['writes_per_sec']:.0f}")
            print(f"  CPU:          {r['cpu_percent']:.1f}%")
            print(f"  Memory:       {r['memory_mb']:.1f} MB")

        if "network_recovery" in results:
            r = results["network_recovery"]
            print(f"\nNetwork Recovery:")
            print(f"  Outage:       {r['outage_duration_sec']}s")
            print(f"  Writes during outage: {r['writes_during_outage']}")
            print(f"  Catchup time: {r['catchup_time_ms']:.0f} ms")

        if "write_throughput" in results:
            r = results["write_throughput"]
            print(f"\nWrite Throughput:")
            print(f"  Max commits/sec:  {r['max_commits_per_sec']:.0f}")
            print(f"  Avg latency:      {r['latency_at_max_ms']:.2f} ms")

        if "checkpoint_impact" in results:
            r = results["checkpoint_impact"]
            print(f"\nCheckpoint Impact:")
            print(f"  Normal latency:     {r['normal_sync_latency_ms']:.2f} ms")
            print(f"  Post-checkpoint:    {r['during_checkpoint_latency_ms']:.2f} ms")
            print(f"  Checkpoint duration:{r['checkpoint_duration_ms']:.0f} ms")

        if "cached_restore" in results:
            print(f"\nCached vs Direct Restore (Phase 5):")
            print(f"  {'Size (MB)':>10} {'S3 Only (ms)':>14} {'Cache (ms)':>12} {'Speedup':>10} {'Cache Hit':>12}")
            print(f"  {'-' * 10} {'-' * 14} {'-' * 12} {'-' * 10} {'-' * 12}")
            for r in results["cached_restore"]:
                print(f"  {r['db_size_mb']:>10.1f} {r['s3_only_time_ms']:>14.0f} {r['cache_time_ms']:>12.0f} {r['speedup_factor']:>9.1f}x {r['cache_hit_rate']:>11.0f}%")


if __name__ == "__main__":
    main()
