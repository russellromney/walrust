#!/usr/bin/env python3
"""
Deep investigation of walrust memory usage.
Is 18MB for 500 DBs really possible?
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
    conn = sqlite3.connect(str(path))
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("CREATE TABLE test (id INTEGER PRIMARY KEY, data TEXT)")
    for i in range(10):
        conn.execute("INSERT INTO test (data) VALUES (?)", (f"init_{i}",))
    conn.commit()
    conn.close()


def investigate_memory(num_dbs: int, bucket: str, endpoint: str = None):
    """Deep dive into memory usage."""

    with tempfile.TemporaryDirectory() as tmpdir:
        tmpdir = Path(tmpdir)

        # Create databases
        print(f"Creating {num_dbs} databases...")
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
            "--bucket", f"s3://{bucket}/mem-test/",
            "--independent-tasks", "--no-metrics"
        ] + [str(p) for p in db_paths]

        if endpoint:
            cmd.extend(["--endpoint", endpoint])

        print(f"Starting walrust with {num_dbs} databases...")
        proc = subprocess.Popen(cmd, env=env, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)

        # Monitor startup
        time.sleep(5)

        try:
            walrust_proc = psutil.Process(proc.pid)

            print("\n" + "="*80)
            print("MEMORY ANALYSIS")
            print("="*80)

            # Get detailed memory info
            mem_info = walrust_proc.memory_info()

            print(f"\nProcess PID: {proc.pid}")
            print(f"Status: {walrust_proc.status()}")
            print(f"Number of threads: {walrust_proc.num_threads()}")

            print(f"\n--- Memory Info (memory_info) ---")
            print(f"RSS (Resident Set Size):     {mem_info.rss / (1024*1024):>10.2f} MB")
            print(f"VMS (Virtual Memory Size):   {mem_info.vms / (1024*1024):>10.2f} MB")

            # Try to get full memory info (may require sudo on macOS)
            try:
                mem_full = walrust_proc.memory_full_info()
                print(f"\n--- Full Memory Info (memory_full_info) ---")
                print(f"RSS:                         {mem_full.rss / (1024*1024):>10.2f} MB")
                print(f"VMS:                         {mem_full.vms / (1024*1024):>10.2f} MB")
                if hasattr(mem_full, 'uss'):
                    print(f"USS (Unique Set Size):       {mem_full.uss / (1024*1024):>10.2f} MB")
                if hasattr(mem_full, 'pss'):
                    print(f"PSS (Proportional Set Size): {mem_full.pss / (1024*1024):>10.2f} MB")
            except psutil.AccessDenied:
                print(f"\n--- Full Memory Info (requires sudo) ---")
                print(f"(Run with sudo for USS/PSS metrics)")

            # Check for child processes
            children = walrust_proc.children(recursive=True)
            if children:
                print(f"\n--- Child Processes ({len(children)}) ---")
                for child in children:
                    child_mem = child.memory_info()
                    print(f"  PID {child.pid}: {child_mem.rss / (1024*1024):.2f} MB")

            # Memory maps (requires permissions)
            try:
                print(f"\n--- Memory Maps (top 10 by size) ---")
                maps = walrust_proc.memory_maps()
                sorted_maps = sorted(maps, key=lambda m: m.rss, reverse=True)[:10]
                for m in sorted_maps:
                    print(f"  {m.path:50} {m.rss / (1024*1024):>8.2f} MB")
            except (psutil.AccessDenied, AttributeError):
                print(f"\n--- Memory Maps (requires sudo) ---")
                print(f"(Run with sudo for detailed memory maps)")

            # File descriptors
            open_files = walrust_proc.open_files()
            print(f"\n--- Open Files ({len(open_files)}) ---")
            if len(open_files) > 20:
                print(f"  (showing first 20 of {len(open_files)})")
                for f in open_files[:20]:
                    print(f"  {f.path}")
            else:
                for f in open_files:
                    print(f"  {f.path}")

            # Connections
            conns = walrust_proc.connections()
            print(f"\n--- Network Connections ({len(conns)}) ---")
            for conn in conns[:10]:
                print(f"  {conn.laddr} -> {conn.raddr} ({conn.status})")

            # Summary
            total_rss = mem_info.rss
            for child in children:
                total_rss += child.memory_info().rss

            print(f"\n" + "="*80)
            print(f"SUMMARY FOR {num_dbs} DATABASES")
            print(f"="*80)
            print(f"Total RSS (including children): {total_rss / (1024*1024):.2f} MB")
            print(f"Per-DB overhead (RSS):          {total_rss / num_dbs / 1024:.2f} KB")
            print(f"Threads:                        {walrust_proc.num_threads()}")
            print(f"Open files:                     {len(open_files)}")
            print(f"Expected file descriptors:      ~{num_dbs * 3}")  # .db, .db-wal, .db-shm
            print(f"\nMemory seems {'SUSPICIOUSLY LOW' if total_rss / (1024*1024) < 30 else 'REASONABLE'}")

        finally:
            proc.terminate()
            try:
                proc.wait(timeout=5)
            except subprocess.TimeoutExpired:
                proc.kill()


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

    # Test with increasing DB counts
    for num_dbs in [10, 50, 100]:
        investigate_memory(num_dbs, bucket, endpoint)
        print("\n" + "="*80 + "\n")
        time.sleep(2)


if __name__ == "__main__":
    main()
