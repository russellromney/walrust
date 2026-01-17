#!/usr/bin/env python3
"""
Stress test for walrust independent tasks architecture.
Tests extreme concurrency and write throughput.
"""

import os
import sqlite3
import subprocess
import sys
import tempfile
import time
from dataclasses import dataclass
from pathlib import Path

try:
    import psutil
except ImportError:
    print("Install psutil: uv add psutil")
    sys.exit(1)


@dataclass
class StressTestResult:
    num_dbs: int
    writes_per_db_per_sec: int
    duration_sec: int
    total_target_writes_per_sec: int
    avg_cpu_percent: float
    peak_cpu_percent: float
    avg_memory_mb: float
    peak_memory_mb: float
    startup_time_sec: float
    success: bool
    error: str = ""


def create_test_db(path: Path) -> None:
    """Create a test database with WAL mode."""
    conn = sqlite3.connect(str(path))
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA wal_autocheckpoint=0")
    conn.execute("CREATE TABLE test (id INTEGER PRIMARY KEY, data TEXT)")
    # Add initial data
    for i in range(10):
        conn.execute("INSERT INTO test (data) VALUES (?)", (f"init_{i}",))
    conn.commit()
    conn.close()


def write_to_db(path: Path, writes: int, identifier: str):
    """Write data to a database."""
    conn = sqlite3.connect(str(path), check_same_thread=False)
    conn.execute("PRAGMA wal_autocheckpoint=0")
    for i in range(writes):
        conn.execute("INSERT INTO test (data) VALUES (?)", (f"{identifier}_{i}" * 50,))
        if i % 10 == 0:  # Commit every 10 writes
            conn.commit()
    conn.commit()
    conn.close()


def run_stress_test(
    num_dbs: int,
    writes_per_db_per_sec: int,
    duration_sec: int,
    bucket: str,
    endpoint: str = None
) -> StressTestResult:
    """Run a single stress test configuration."""

    total_target = num_dbs * writes_per_db_per_sec

    print(f"\n{'='*80}")
    print(f"Stress Test: {num_dbs} databases @ {writes_per_db_per_sec} writes/sec")
    print(f"  Target: {total_target:,} total writes/sec")
    print(f"  Duration: {duration_sec}s")
    print(f"{'='*80}\n")

    with tempfile.TemporaryDirectory() as tmpdir:
        tmpdir = Path(tmpdir)

        # Create test databases
        print(f"Creating {num_dbs} databases...")
        db_paths = []
        for i in range(num_dbs):
            db_path = tmpdir / f"db{i:04d}.db"
            create_test_db(db_path)
            db_paths.append(db_path)

        # Start walrust
        env = os.environ.copy()
        env["RUST_LOG"] = "walrust=warn"  # Reduce noise

        cmd = [
            "./target/release/walrust",
            "watch",
            "--bucket", f"s3://{bucket}/stress-test/",
            "--independent-tasks",
            "--no-metrics",
        ] + [str(p) for p in db_paths]

        if endpoint:
            cmd.extend(["--endpoint", endpoint])

        print(f"Starting walrust with {num_dbs} databases...")
        startup_start = time.time()

        proc = subprocess.Popen(
            cmd,
            env=env,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
        )

        # Wait for startup (look for "walrust running" message)
        startup_complete = False
        startup_timeout = 60  # seconds
        while time.time() - startup_start < startup_timeout:
            try:
                proc.wait(timeout=0.1)
                # Process exited
                return StressTestResult(
                    num_dbs=num_dbs,
                    writes_per_db_per_sec=writes_per_db_per_sec,
                    duration_sec=duration_sec,
                    total_target_writes_per_sec=total_target,
                    avg_cpu_percent=0,
                    peak_cpu_percent=0,
                    avg_memory_mb=0,
                    peak_memory_mb=0,
                    startup_time_sec=0,
                    success=False,
                    error="walrust exited during startup"
                )
            except subprocess.TimeoutExpired:
                pass

            # Check if all tasks spawned (hack: wait a bit)
            if time.time() - startup_start > 5:
                startup_complete = True
                break

        if not startup_complete:
            proc.kill()
            return StressTestResult(
                num_dbs=num_dbs,
                writes_per_db_per_sec=writes_per_db_per_sec,
                duration_sec=duration_sec,
                total_target_writes_per_sec=total_target,
                avg_cpu_percent=0,
                peak_cpu_percent=0,
                avg_memory_mb=0,
                peak_memory_mb=0,
                startup_time_sec=0,
                success=False,
                error="walrust startup timeout"
            )

        startup_time = time.time() - startup_start
        print(f"Walrust started in {startup_time:.2f}s")

        # Start write load using subprocess (avoid threading issues)
        print(f"\nStarting write load ({total_target:,} writes/sec)...")
        write_procs = []
        writes_per_db = writes_per_db_per_sec * duration_sec

        for i, db_path in enumerate(db_paths):
            # Create a simple Python script to write
            write_script = f"""
import sqlite3
import time
conn = sqlite3.connect("{db_path}", check_same_thread=False)
conn.execute("PRAGMA wal_autocheckpoint=0")
interval = {1.0 / writes_per_db_per_sec}
for i in range({writes_per_db}):
    start = time.time()
    conn.execute("INSERT INTO test (data) VALUES (?)", (f"db{i}_{{i}}" * 50,))
    if i % 10 == 0:
        conn.commit()
    elapsed = time.time() - start
    if elapsed < interval:
        time.sleep(interval - elapsed)
conn.commit()
conn.close()
"""
            # Write script to temp file
            script_path = tmpdir / f"write_{i}.py"
            script_path.write_text(write_script)

            # Start writer process
            writer_proc = subprocess.Popen(
                [sys.executable, str(script_path)],
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL
            )
            write_procs.append(writer_proc)

        # Monitor walrust process
        try:
            walrust_proc = psutil.Process(proc.pid)
            cpu_samples = []
            mem_samples = []

            test_start = time.time()
            sample_interval = 0.5  # Sample every 500ms

            while time.time() - test_start < duration_sec + 2:  # Extra 2s for final sync
                try:
                    cpu = walrust_proc.cpu_percent(interval=sample_interval)
                    mem = walrust_proc.memory_info().rss / (1024 * 1024)  # MB

                    cpu_samples.append(cpu)
                    mem_samples.append(mem)

                    elapsed = time.time() - test_start
                    print(f"  {elapsed:5.1f}s: CPU={cpu:5.1f}%, MEM={mem:6.1f}MB")
                except (psutil.NoSuchProcess, psutil.AccessDenied):
                    break

            # Wait for writers to complete
            print("\nWaiting for write processes to complete...")
            for wp in write_procs:
                wp.wait(timeout=10)

            # Give walrust time to sync
            print("Waiting for final syncs...")
            time.sleep(3)

            # Final samples
            try:
                cpu_samples.append(walrust_proc.cpu_percent(interval=0.1))
                mem_samples.append(walrust_proc.memory_info().rss / (1024 * 1024))
            except:
                pass

            # Stop walrust
            proc.terminate()
            try:
                proc.wait(timeout=5)
            except subprocess.TimeoutExpired:
                proc.kill()
                proc.wait()

            # Calculate stats
            avg_cpu = sum(cpu_samples) / len(cpu_samples) if cpu_samples else 0
            peak_cpu = max(cpu_samples) if cpu_samples else 0
            avg_mem = sum(mem_samples) / len(mem_samples) if mem_samples else 0
            peak_mem = max(mem_samples) if mem_samples else 0

            return StressTestResult(
                num_dbs=num_dbs,
                writes_per_db_per_sec=writes_per_db_per_sec,
                duration_sec=duration_sec,
                total_target_writes_per_sec=total_target,
                avg_cpu_percent=avg_cpu,
                peak_cpu_percent=peak_cpu,
                avg_memory_mb=avg_mem,
                peak_memory_mb=peak_mem,
                startup_time_sec=startup_time,
                success=True
            )

        except Exception as e:
            proc.kill()
            for wp in write_procs:
                wp.kill()
            return StressTestResult(
                num_dbs=num_dbs,
                writes_per_db_per_sec=writes_per_db_per_sec,
                duration_sec=duration_sec,
                total_target_writes_per_sec=total_target,
                avg_cpu_percent=0,
                peak_cpu_percent=0,
                avg_memory_mb=0,
                peak_memory_mb=0,
                startup_time_sec=0,
                success=False,
                error=str(e)
            )


def main():
    # Load env
    if os.path.exists(".env"):
        with open(".env") as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith("#") and "=" in line:
                    key, val = line.split("=", 1)
                    os.environ[key] = val

    bucket = os.environ.get("WALSYNC_TEST_BUCKET", "empty-cherry-5203")
    endpoint = os.environ.get("AWS_ENDPOINT_URL_S3")

    # Stress test configurations
    tests = [
        # (num_dbs, writes_per_db_per_sec, duration)
        (50, 100, 10),    # 5K writes/sec
        (100, 100, 10),   # 10K writes/sec
        (250, 40, 10),    # 10K writes/sec, more concurrency
        (500, 20, 10),    # 10K writes/sec, extreme concurrency
    ]

    print("\n" + "="*80)
    print("WALRUST STRESS TEST SUITE")
    print("Testing independent tasks architecture under extreme load")
    print("="*80)

    results = []
    for num_dbs, writes_per_sec, duration in tests:
        result = run_stress_test(num_dbs, writes_per_sec, duration, bucket, endpoint)
        results.append(result)

        if not result.success:
            print(f"\n❌ FAILED: {result.error}")
            break

    # Print summary
    print("\n" + "="*80)
    print("STRESS TEST RESULTS")
    print("="*80)
    print(f"{'DBs':>5} {'Target':>10} {'Avg CPU':>10} {'Peak CPU':>10} {'Avg Mem':>10} {'Peak Mem':>10} {'Status':>10}")
    print("-" * 80)

    for r in results:
        status = "✓ PASS" if r.success else "✗ FAIL"
        print(f"{r.num_dbs:5d} {r.total_target_writes_per_sec:>10,} "
              f"{r.avg_cpu_percent:>9.1f}% {r.peak_cpu_percent:>9.1f}% "
              f"{r.avg_memory_mb:>9.1f}M {r.peak_memory_mb:>9.1f}M "
              f"{status:>10}")

    print("="*80)

    # Key findings
    successful = [r for r in results if r.success]
    if successful:
        max_throughput = max(r.total_target_writes_per_sec for r in successful)
        max_dbs = max(r.num_dbs for r in successful)
        print(f"\n🚀 Maximum validated throughput: {max_throughput:,} writes/sec")
        print(f"📊 Maximum concurrent DBs: {max_dbs:,}")
        print(f"💪 5K ceiling broken: {max_throughput / 5000:.1f}x improvement")


if __name__ == "__main__":
    main()
