#!/usr/bin/env python3
"""
Fair comparison: Walrust vs Litestream
SAME workload, SAME measurement methodology
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
class BenchResult:
    tool: str
    num_dbs: int
    target_writes: int
    actual_writes: int
    duration_sec: float
    throughput: float
    avg_cpu: float
    peak_cpu: float
    avg_mem_mb: float
    peak_mem_mb: float
    success: bool
    error: str = ""


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
    """Worker that writes and reports actual count."""
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


def test_walrust(num_dbs: int, writes_per_db: int, bucket: str, endpoint: str) -> BenchResult:
    """Test walrust."""
    print(f"\n{'='*80}")
    print(f"🦀 Testing WALRUST: {num_dbs} DBs × {writes_per_db} writes")
    print(f"{'='*80}")

    try:
        with tempfile.TemporaryDirectory() as tmpdir:
            tmpdir = Path(tmpdir)

            # Create DBs
            print(f"  📦 Creating {num_dbs} databases...")
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
                "--bucket", f"s3://{bucket}/bench-walrust/",
                "--independent-tasks", "--no-metrics"
            ] + [str(p) for p in db_paths]

            if endpoint:
                cmd.extend(["--endpoint", endpoint])

            print("  🚀 Starting walrust...")
            proc = subprocess.Popen(cmd, env=env, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
            time.sleep(2)

            # Monitor
            walrust_proc = psutil.Process(proc.pid)
            cpu_samples = []
            mem_samples = []

            # Start writers
            result_queue = mp.Queue()
            print(f"  💥 Starting writers...")
            workers = []
            overall_start = time.time()

            for db_path in db_paths:
                p = mp.Process(target=write_worker, args=(str(db_path), writes_per_db, result_queue))
                p.start()
                workers.append(p)

            # Monitor
            while any(w.is_alive() for w in workers):
                try:
                    cpu = walrust_proc.cpu_percent(interval=0.5)
                    mem = walrust_proc.memory_info().rss / (1024 * 1024)
                    cpu_samples.append(cpu)
                    mem_samples.append(mem)
                except:
                    break

            overall_duration = time.time() - overall_start

            # Wait for all
            for w in workers:
                w.join(timeout=5)

            # Collect results
            total_writes = 0
            while not result_queue.empty():
                result = result_queue.get()
                if 'writes' in result:
                    total_writes += result['writes']

            # Wait for sync
            print("  🔄 Waiting for sync...")
            time.sleep(3)

            # Final sample
            try:
                cpu_samples.append(walrust_proc.cpu_percent(interval=0.1))
                mem_samples.append(walrust_proc.memory_info().rss / (1024 * 1024))
            except:
                pass

            # Stop
            proc.terminate()
            try:
                proc.wait(timeout=5)
            except:
                proc.kill()

            # Stats
            avg_cpu = sum(cpu_samples) / len(cpu_samples) if cpu_samples else 0
            peak_cpu = max(cpu_samples) if cpu_samples else 0
            avg_mem = sum(mem_samples) / len(mem_samples) if mem_samples else 0
            peak_mem = max(mem_samples) if mem_samples else 0
            throughput = total_writes / overall_duration if overall_duration > 0 else 0

            return BenchResult(
                tool="Walrust",
                num_dbs=num_dbs,
                target_writes=num_dbs * writes_per_db,
                actual_writes=total_writes,
                duration_sec=overall_duration,
                throughput=throughput,
                avg_cpu=avg_cpu,
                peak_cpu=peak_cpu,
                avg_mem_mb=avg_mem,
                peak_mem_mb=peak_mem,
                success=True
            )

    except Exception as e:
        return BenchResult(
            tool="Walrust",
            num_dbs=num_dbs,
            target_writes=num_dbs * writes_per_db,
            actual_writes=0,
            duration_sec=0,
            throughput=0,
            avg_cpu=0,
            peak_cpu=0,
            avg_mem_mb=0,
            peak_mem_mb=0,
            success=False,
            error=str(e)
        )


def test_litestream(num_dbs: int, writes_per_db: int, bucket: str, endpoint: str) -> BenchResult:
    """Test litestream."""
    print(f"\n{'='*80}")
    print(f"📺 Testing LITESTREAM: {num_dbs} DBs × {writes_per_db} writes")
    print(f"{'='*80}")

    try:
        with tempfile.TemporaryDirectory() as tmpdir:
            tmpdir = Path(tmpdir)

            # Create DBs
            print(f"  📦 Creating {num_dbs} databases...")
            db_paths = []
            for i in range(num_dbs):
                db_path = tmpdir / f"db{i:04d}.db"
                create_db(db_path)
                db_paths.append(db_path)

            # Create litestream config
            config = tmpdir / "litestream.yml"
            config_content = "dbs:\n"
            for db_path in db_paths:
                config_content += f"""  - path: {db_path}
    replicas:
      - url: s3://{bucket}/bench-litestream/{db_path.name}
"""
                if endpoint:
                    config_content += f"        endpoint: {endpoint}\n"

            config.write_text(config_content)

            # Start litestream
            print("  🚀 Starting litestream...")
            env = os.environ.copy()
            proc = subprocess.Popen(
                ["litestream", "replicate", "-config", str(config)],
                env=env,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL
            )
            time.sleep(2)

            # Monitor
            litestream_proc = psutil.Process(proc.pid)
            children = litestream_proc.children(recursive=True)
            all_procs = [litestream_proc] + children

            cpu_samples = []
            mem_samples = []

            # Start writers
            result_queue = mp.Queue()
            print(f"  💥 Starting writers...")
            workers = []
            overall_start = time.time()

            for db_path in db_paths:
                p = mp.Process(target=write_worker, args=(str(db_path), writes_per_db, result_queue))
                p.start()
                workers.append(p)

            # Monitor
            while any(w.is_alive() for w in workers):
                try:
                    children = litestream_proc.children(recursive=True)
                    all_procs = [litestream_proc] + children
                    cpu = sum(p.cpu_percent(interval=0.5) for p in all_procs if p.is_running())
                    mem = sum(p.memory_info().rss for p in all_procs if p.is_running()) / (1024 * 1024)
                    cpu_samples.append(cpu)
                    mem_samples.append(mem)
                except:
                    break

            overall_duration = time.time() - overall_start

            # Wait for all
            for w in workers:
                w.join(timeout=5)

            # Collect results
            total_writes = 0
            while not result_queue.empty():
                result = result_queue.get()
                if 'writes' in result:
                    total_writes += result['writes']

            # Wait for sync
            print("  🔄 Waiting for sync...")
            time.sleep(3)

            # Final sample
            try:
                children = litestream_proc.children(recursive=True)
                all_procs = [litestream_proc] + children
                cpu_samples.append(sum(p.cpu_percent(interval=0.1) for p in all_procs if p.is_running()))
                mem_samples.append(sum(p.memory_info().rss for p in all_procs if p.is_running()) / (1024 * 1024))
            except:
                pass

            # Stop
            proc.terminate()
            try:
                proc.wait(timeout=5)
            except:
                proc.kill()

            # Stats
            avg_cpu = sum(cpu_samples) / len(cpu_samples) if cpu_samples else 0
            peak_cpu = max(cpu_samples) if cpu_samples else 0
            avg_mem = sum(mem_samples) / len(mem_samples) if mem_samples else 0
            peak_mem = max(mem_samples) if mem_samples else 0
            throughput = total_writes / overall_duration if overall_duration > 0 else 0

            return BenchResult(
                tool="Litestream",
                num_dbs=num_dbs,
                target_writes=num_dbs * writes_per_db,
                actual_writes=total_writes,
                duration_sec=overall_duration,
                throughput=throughput,
                avg_cpu=avg_cpu,
                peak_cpu=peak_cpu,
                avg_mem_mb=avg_mem,
                peak_mem_mb=peak_mem,
                success=True
            )

    except Exception as e:
        return BenchResult(
            tool="Litestream",
            num_dbs=num_dbs,
            target_writes=num_dbs * writes_per_db,
            actual_writes=0,
            duration_sec=0,
            throughput=0,
            avg_cpu=0,
            peak_cpu=0,
            avg_mem_mb=0,
            peak_mem_mb=0,
            success=False,
            error=str(e)
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
        (10, 1000),
        (50, 1000),
        (100, 500),
    ]

    print("\n" + "="*80)
    print("⚔️  FAIR COMPARISON: Walrust vs Litestream")
    print("Same workload, same measurement, same S3 bucket")
    print("="*80)

    results = []

    for num_dbs, writes in tests:
        # Test walrust
        w_result = test_walrust(num_dbs, writes, bucket, endpoint)
        results.append(w_result)

        print(f"\n  ✅ Walrust: {w_result.throughput:,.0f} writes/sec "
              f"(CPU: {w_result.avg_cpu:.1f}%, Mem: {w_result.avg_mem_mb:.1f}MB)")

        # Test litestream
        l_result = test_litestream(num_dbs, writes, bucket, endpoint)
        results.append(l_result)

        if l_result.success:
            print(f"  ✅ Litestream: {l_result.throughput:,.0f} writes/sec "
                  f"(CPU: {l_result.avg_cpu:.1f}%, Mem: {l_result.avg_mem_mb:.1f}MB)")
        else:
            print(f"  ❌ Litestream FAILED: {l_result.error}")

        # Comparison
        if w_result.success and l_result.success:
            speedup = w_result.throughput / l_result.throughput if l_result.throughput > 0 else 0
            mem_ratio = l_result.avg_mem_mb / w_result.avg_mem_mb if w_result.avg_mem_mb > 0 else 0
            print(f"\n  📊 Walrust is {speedup:.2f}x faster, uses {mem_ratio:.2f}x less memory")

    # Summary
    print("\n" + "="*80)
    print("📊 COMPARISON RESULTS")
    print("="*80)
    print(f"{'DBs':>5} {'Tool':>12} {'Writes/sec':>15} {'CPU':>8} {'Memory':>10}")
    print("-" * 80)

    for r in results:
        if r.success:
            print(f"{r.num_dbs:5d} {r.tool:>12} {r.throughput:>15,.0f} "
                  f"{r.avg_cpu:>7.1f}% {r.avg_mem_mb:>9.1f}M")
        else:
            print(f"{r.num_dbs:5d} {r.tool:>12} {'FAILED':>15} {'-':>8} {'-':>10}")

    print("="*80)


if __name__ == "__main__":
    main()
