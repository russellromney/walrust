#!/usr/bin/env python3
"""
REAL throughput measurement - no faking!
Measures ACTUAL writes completed, not target writes.
"""

import os
import sqlite3
import subprocess
import sys
import tempfile
import time
from dataclasses import dataclass
from pathlib import Path
import multiprocessing as mp

try:
    import psutil
except ImportError:
    print("Install: uv add psutil")
    sys.exit(1)


@dataclass
class RealThroughputResult:
    num_dbs: int
    target_writes_per_db: int
    actual_writes_completed: int
    duration_sec: float
    actual_throughput: float  # writes/sec achieved
    avg_cpu: float
    peak_cpu: float
    avg_mem_mb: float
    peak_mem_mb: float


def create_db(path: Path):
    """Create test DB."""
    conn = sqlite3.connect(str(path))
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA wal_autocheckpoint=0")
    conn.execute("CREATE TABLE test (id INTEGER PRIMARY KEY, data TEXT)")
    for i in range(5):
        conn.execute("INSERT INTO test (data) VALUES (?)", (f"init_{i}",))
    conn.commit()
    conn.close()


def write_worker(db_path: str, num_writes: int, result_queue: mp.Queue):
    """Worker process that writes and reports actual count."""
    try:
        conn = sqlite3.connect(db_path)
        conn.execute("PRAGMA wal_autocheckpoint=0")

        start = time.time()
        for i in range(num_writes):
            conn.execute("INSERT INTO test (data) VALUES (?)", (f"data_{i}" * 50,))
            if i % 10 == 0:
                conn.commit()
        conn.commit()
        duration = time.time() - start

        conn.close()
        result_queue.put({'writes': num_writes, 'duration': duration})
    except Exception as e:
        result_queue.put({'error': str(e)})


def run_real_throughput_test(
    num_dbs: int,
    writes_per_db: int,
    bucket: str,
    endpoint: str
) -> RealThroughputResult:
    """Run test and measure ACTUAL throughput."""

    print(f"\n{'='*80}")
    print(f"🎯 REAL Throughput Test: {num_dbs} DBs × {writes_per_db} writes")
    print(f"{'='*80}\n")

    with tempfile.TemporaryDirectory() as tmpdir:
        tmpdir = Path(tmpdir)

        # Create DBs
        print(f"📦 Creating {num_dbs} databases...")
        db_paths = []
        for i in range(num_dbs):
            db_path = tmpdir / f"db{i:04d}.db"
            create_db(db_path)
            db_paths.append(db_path)

        # Start walrust
        env = os.environ.copy()
        env["RUST_LOG"] = "walrust=warn"

        cmd = [
            "./target/release/walrust", "watch",
            "--bucket", f"s3://{bucket}/real-throughput/",
            "--independent-tasks", "--no-metrics"
        ] + [str(p) for p in db_paths]

        if endpoint:
            cmd.extend(["--endpoint", endpoint])

        print("🚀 Starting walrust...")
        proc = subprocess.Popen(cmd, env=env, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        time.sleep(2)

        # Start monitoring
        walrust_proc = psutil.Process(proc.pid)
        cpu_samples = []
        mem_samples = []

        # Create result queue
        result_queue = mp.Queue()

        # Start writers
        print(f"💥 Starting {num_dbs} writer processes...")
        workers = []
        overall_start = time.time()

        for db_path in db_paths:
            p = mp.Process(target=write_worker, args=(str(db_path), writes_per_db, result_queue))
            p.start()
            workers.append(p)

        # Monitor while writers run
        print("⚡ Monitoring performance...\n")
        while any(w.is_alive() for w in workers):
            try:
                cpu = walrust_proc.cpu_percent(interval=0.5)
                mem = walrust_proc.memory_info().rss / (1024 * 1024)
                cpu_samples.append(cpu)
                mem_samples.append(mem)

                elapsed = time.time() - overall_start
                active = sum(1 for w in workers if w.is_alive())
                print(f"  {elapsed:5.1f}s: CPU={cpu:5.1f}%, MEM={mem:6.1f}MB, Active writers={active}")
            except:
                break

        overall_duration = time.time() - overall_start

        # Wait for all workers
        for w in workers:
            w.join(timeout=5)

        # Collect results
        total_writes = 0
        errors = 0
        while not result_queue.empty():
            result = result_queue.get()
            if 'error' in result:
                errors += 1
            else:
                total_writes += result['writes']

        # Give walrust time to sync
        print("\n🔄 Waiting for final syncs...")
        time.sleep(3)

        # Final sample
        try:
            cpu_samples.append(walrust_proc.cpu_percent(interval=0.1))
            mem_samples.append(walrust_proc.memory_info().rss / (1024 * 1024))
        except:
            pass

        # Stop walrust
        proc.terminate()
        try:
            proc.wait(timeout=5)
        except:
            proc.kill()

        # Calculate stats
        avg_cpu = sum(cpu_samples) / len(cpu_samples) if cpu_samples else 0
        peak_cpu = max(cpu_samples) if cpu_samples else 0
        avg_mem = sum(mem_samples) / len(mem_samples) if mem_samples else 0
        peak_mem = max(mem_samples) if mem_samples else 0

        actual_throughput = total_writes / overall_duration if overall_duration > 0 else 0

        return RealThroughputResult(
            num_dbs=num_dbs,
            target_writes_per_db=writes_per_db,
            actual_writes_completed=total_writes,
            duration_sec=overall_duration,
            actual_throughput=actual_throughput,
            avg_cpu=avg_cpu,
            peak_cpu=peak_cpu,
            avg_mem_mb=avg_mem,
            peak_mem_mb=peak_mem
        )


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

    tests = [
        # (num_dbs, writes_per_db)
        (10, 1000),    # 10 DBs, 1K writes each
        (50, 1000),    # 50 DBs, 1K writes each
        (100, 500),    # 100 DBs, 500 writes each
        (250, 200),    # 250 DBs, 200 writes each
        (500, 100),    # 500 DBs, 100 writes each
    ]

    print("\n" + "="*80)
    print("🔥 REAL THROUGHPUT MEASUREMENT")
    print("No targets, no faking - actual writes/sec achieved!")
    print("="*80)

    results = []
    for num_dbs, writes in tests:
        result = run_real_throughput_test(num_dbs, writes, bucket, endpoint)
        results.append(result)

        print(f"\n📊 Results:")
        print(f"   Target writes: {num_dbs * writes:,}")
        print(f"   Actual writes: {result.actual_writes_completed:,}")
        print(f"   Duration: {result.duration_sec:.1f}s")
        print(f"   ACTUAL throughput: {result.actual_throughput:,.0f} writes/sec")
        print(f"   Avg CPU: {result.avg_cpu:.1f}%")
        print(f"   Avg Memory: {result.avg_mem_mb:.1f} MB")

    # Summary
    print("\n" + "="*80)
    print("🏆 REAL THROUGHPUT RESULTS")
    print("="*80)
    print(f"{'DBs':>5} {'Writes':>8} {'Duration':>10} {'Actual Throughput':>20} {'CPU':>8} {'Mem':>8}")
    print("-" * 80)

    for r in results:
        print(f"{r.num_dbs:5d} {r.actual_writes_completed:>8,} "
              f"{r.duration_sec:>9.1f}s {r.actual_throughput:>18,.0f}/s "
              f"{r.avg_cpu:>7.1f}% {r.avg_mem_mb:>7.1f}M")

    print("="*80)

    if results:
        max_throughput = max(r.actual_throughput for r in results)
        print(f"\n🚀 Maximum ACTUAL throughput: {max_throughput:,.0f} writes/sec")
        print(f"   (This is what we REALLY achieved, not a target!)")


if __name__ == "__main__":
    main()
