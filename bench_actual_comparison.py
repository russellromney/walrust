#!/usr/bin/env python3
"""
Actual head-to-head comparison: walrust vs litestream
Tests same workload, measures real performance.
"""

import os
import sqlite3
import subprocess
import sys
import tempfile
import time
from pathlib import Path

try:
    import psutil
except ImportError:
    print("Install: uv add psutil")
    sys.exit(1)


def create_db(path: Path) -> None:
    """Create test database."""
    conn = sqlite3.connect(str(path))
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("CREATE TABLE test (id INTEGER PRIMARY KEY, data TEXT)")
    for i in range(10):
        conn.execute("INSERT INTO test (data) VALUES (?)", (f"init_{i}",))
    conn.commit()
    conn.close()


def write_load(path: Path, writes: int, identifier: str):
    """Write data."""
    conn = sqlite3.connect(str(path))
    for i in range(writes):
        conn.execute("INSERT INTO test (data) VALUES (?)", (f"{identifier}_{i}" * 50,))
        if i % 10 == 0:
            conn.commit()
    conn.commit()
    conn.close()


def test_walrust(num_dbs: int, writes_per_db: int, bucket: str, endpoint: str):
    """Test walrust."""
    with tempfile.TemporaryDirectory() as tmpdir:
        tmpdir = Path(tmpdir)

        # Create DBs
        db_paths = []
        for i in range(num_dbs):
            db_path = tmpdir / f"db{i:03d}.db"
            create_db(db_path)
            db_paths.append(db_path)

        # Start walrust
        env = os.environ.copy()
        env["RUST_LOG"] = "walrust=warn"

        cmd = [
            "./target/release/walrust", "watch",
            "--bucket", f"s3://{bucket}/test-walrust/",
            "--independent-tasks", "--no-metrics"
        ] + [str(p) for p in db_paths]

        if endpoint:
            cmd.extend(["--endpoint", endpoint])

        proc = subprocess.Popen(cmd, env=env, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        time.sleep(3)  # Startup

        # Monitor
        walrust_proc = psutil.Process(proc.pid)

        # Get all child processes too
        children = walrust_proc.children(recursive=True)
        all_procs = [walrust_proc] + children

        # Measure initial state
        mem_before = sum(p.memory_info().rss for p in all_procs) / (1024 * 1024)

        # Write load
        start = time.time()
        for i, db_path in enumerate(db_paths):
            write_load(db_path, writes_per_db, f"db{i}")

        # Monitor during writes
        time.sleep(5)

        children = walrust_proc.children(recursive=True)
        all_procs = [walrust_proc] + children
        mem_during = sum(p.memory_info().rss for p in all_procs) / (1024 * 1024)
        cpu = sum(p.cpu_percent(interval=1) for p in all_procs)

        duration = time.time() - start

        proc.terminate()
        proc.wait(timeout=5)

        return {
            "procs": len(all_procs),
            "mem_before_mb": mem_before,
            "mem_during_mb": mem_during,
            "cpu_percent": cpu,
            "duration_sec": duration
        }


def test_litestream(num_dbs: int, writes_per_db: int, bucket: str, endpoint: str):
    """Test litestream."""
    with tempfile.TemporaryDirectory() as tmpdir:
        tmpdir = Path(tmpdir)

        # Create DBs
        db_paths = []
        for i in range(num_dbs):
            db_path = tmpdir / f"db{i:03d}.db"
            create_db(db_path)
            db_paths.append(db_path)

        # Create litestream config
        config = tmpdir / "litestream.yml"
        config_content = f"dbs:\n"
        for db_path in db_paths:
            config_content += f"""  - path: {db_path}
    replicas:
      - url: s3://{bucket}/test-litestream/{db_path.name}
"""
            if endpoint:
                config_content += f"        endpoint: {endpoint}\n"

        config.write_text(config_content)

        # Start litestream
        env = os.environ.copy()
        proc = subprocess.Popen(
            ["litestream", "replicate", "-config", str(config)],
            env=env,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL
        )
        time.sleep(3)  # Startup

        # Monitor
        litestream_proc = psutil.Process(proc.pid)
        children = litestream_proc.children(recursive=True)
        all_procs = [litestream_proc] + children

        mem_before = sum(p.memory_info().rss for p in all_procs) / (1024 * 1024)

        # Write load
        start = time.time()
        for i, db_path in enumerate(db_paths):
            write_load(db_path, writes_per_db, f"db{i}")

        time.sleep(5)

        children = litestream_proc.children(recursive=True)
        all_procs = [litestream_proc] + children
        mem_during = sum(p.memory_info().rss for p in all_procs) / (1024 * 1024)
        cpu = sum(p.cpu_percent(interval=1) for p in all_procs)

        duration = time.time() - start

        proc.terminate()
        proc.wait(timeout=5)

        return {
            "procs": len(all_procs),
            "mem_before_mb": mem_before,
            "mem_during_mb": mem_during,
            "cpu_percent": cpu,
            "duration_sec": duration
        }


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
        (1, 100),
        (10, 100),
        (50, 100),
    ]

    print("\n" + "="*80)
    print("ACTUAL COMPARISON: Walrust vs Litestream")
    print("="*80 + "\n")

    for num_dbs, writes in tests:
        print(f"\nTesting {num_dbs} databases, {writes} writes each...")

        print("  Running walrust...")
        w_result = test_walrust(num_dbs, writes, bucket, endpoint)

        print("  Running litestream...")
        l_result = test_litestream(num_dbs, writes, bucket, endpoint)

        print(f"\n  Results for {num_dbs} DBs:")
        print(f"    {'':20} {'Walrust':>15} {'Litestream':>15}")
        print(f"    {'Processes':20} {w_result['procs']:>15} {l_result['procs']:>15}")
        print(f"    {'Mem Before (MB)':20} {w_result['mem_before_mb']:>15.1f} {l_result['mem_before_mb']:>15.1f}")
        print(f"    {'Mem During (MB)':20} {w_result['mem_during_mb']:>15.1f} {l_result['mem_during_mb']:>15.1f}")
        print(f"    {'CPU %':20} {w_result['cpu_percent']:>15.1f} {l_result['cpu_percent']:>15.1f}")


if __name__ == "__main__":
    main()
