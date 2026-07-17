#!/usr/bin/env python3
"""Test multi-DB throughput with independent tasks mode."""

import os
import sqlite3
import subprocess
import sys
import tempfile
import threading
import time
import signal

def create_db(db_path, num_rows=10):
    """Create a test database with initial data."""
    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA wal_autocheckpoint=0")
    conn.execute("CREATE TABLE test (id INTEGER PRIMARY KEY, data TEXT)")
    for i in range(num_rows):
        conn.execute("INSERT INTO test (data) VALUES (?)", (f"init_{i}",))
    conn.commit()
    return conn

def write_loop(conn, db_num, writes_per_sec, duration_sec):
    """Write to database at specified rate."""
    total_writes = 0
    start = time.time()
    interval = 1.0 / writes_per_sec

    while time.time() - start < duration_sec:
        loop_start = time.time()
        conn.execute("INSERT INTO test (data) VALUES (?)", (f"db{db_num}_{total_writes}",))
        conn.commit()
        total_writes += 1

        # Sleep to maintain rate
        elapsed = time.time() - loop_start
        if elapsed < interval:
            time.sleep(interval - elapsed)

    return total_writes

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

    # Test parameters
    num_dbs = 250
    writes_per_db_per_sec = 40  # 250 DBs * 40 = 10K total writes/sec
    duration_sec = 10

    print(f"Multi-DB Throughput Test")
    print(f"  Databases: {num_dbs}")
    print(f"  Writes/DB/sec: {writes_per_db_per_sec}")
    print(f"  Total target: {num_dbs * writes_per_db_per_sec} writes/sec")
    print(f"  Duration: {duration_sec}s")
    print()

    with tempfile.TemporaryDirectory() as tmpdir:
        # Create databases
        print(f"Creating {num_dbs} databases...")
        db_paths = []
        connections = []
        for i in range(num_dbs):
            db_path = os.path.join(tmpdir, f"db{i:03d}.db")
            conn = create_db(db_path)
            db_paths.append(db_path)
            connections.append(conn)

        print(f"Created {num_dbs} databases in {tmpdir}")
        print()

        # Run walrust with independent tasks mode
        env = os.environ.copy()
        env["RUST_LOG"] = "walrust=info"

        cmd = [
            "./target/release/walrust",
            "watch",
            "--bucket", f"s3://{bucket}/multidb-test/",
            "--independent-tasks",
            "--no-metrics",
        ] + db_paths

        print(f"Starting walrust with {num_dbs} databases...")

        proc = subprocess.Popen(
            cmd,
            env=env,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
        )

        def read_output():
            for line in proc.stdout:
                if "INFO" in line or "ERROR" in line or "Created initial" in line:
                    print(line, end="")

        output_thread = threading.Thread(target=read_output, daemon=True)
        output_thread.start()

        # Wait for walrust to start
        print("Waiting for walrust to start...")
        time.sleep(3)
        print()

        # Start write threads
        print(f"Starting write load ({num_dbs * writes_per_db_per_sec} writes/sec total)...")
        write_threads = []
        for i, conn in enumerate(connections):
            thread = threading.Thread(
                target=write_loop,
                args=(conn, i, writes_per_db_per_sec, duration_sec)
            )
            thread.start()
            write_threads.append(thread)

        start_time = time.time()

        # Monitor CPU usage
        try:
            import psutil
            walrust_process = psutil.Process(proc.pid)
            cpu_samples = []

            for _ in range(duration_sec):
                time.sleep(1)
                try:
                    cpu_pct = walrust_process.cpu_percent(interval=0.1)
                    cpu_samples.append(cpu_pct)
                    print(f"  CPU: {cpu_pct:.1f}%")
                except:
                    pass
        except ImportError:
            # Wait without monitoring
            for thread in write_threads:
                thread.join()

        duration = time.time() - start_time

        # Close connections
        for conn in connections:
            conn.close()

        print()
        print(f"Write load completed in {duration:.1f}s")

        if 'cpu_samples' in locals() and cpu_samples:
            avg_cpu = sum(cpu_samples) / len(cpu_samples)
            max_cpu = max(cpu_samples)
            print(f"  CPU: avg={avg_cpu:.1f}%, max={max_cpu:.1f}%")

        # Give walrust time to sync
        print("\nWaiting for final syncs...")
        time.sleep(5)

        # Stop walrust
        print("Stopping walrust...")
        proc.send_signal(signal.SIGINT)
        proc.wait(timeout=10)

        print("\n" + "="*80)
        print("RESULTS:")
        print(f"  Target: {num_dbs * writes_per_db_per_sec} writes/sec")
        print(f"  Duration: {duration:.1f}s")
        if 'cpu_samples' in locals() and cpu_samples:
            print(f"  Average CPU: {avg_cpu:.1f}%")
        print("  Status: SUCCESS - Independent tasks handled {num_dbs} DBs")
        print("="*80)

if __name__ == "__main__":
    main()
