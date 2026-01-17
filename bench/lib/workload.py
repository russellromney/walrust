#!/usr/bin/env python3
"""
Workload Generator for Walrust Benchmark Framework

Writes data to SQLite databases at controlled rates for replication testing.
Focus: Data loss verification, not latency tracking.
"""

import os
import time
import sqlite3
import threading
from uuid import uuid4
from pathlib import Path
from typing import List, Tuple


class DatabaseWriter:
    """Write data to SQLite with known timestamps for replication verification.

    This class creates a write workload that can be verified after replication.
    It tracks what was written and when, allowing us to verify that all committed
    writes made it to S3 via walrust/litestream.

    Attributes:
        db_path: Path to SQLite database
        writes_per_second: Target write rate
        writes: List of (write_id, commit_timestamp) tuples for verification
        running: Control flag for write loop
        thread: Background thread running the write loop
    """

    def __init__(self, db_path: Path, writes_per_second: int):
        """Initialize database writer.

        Args:
            db_path: Path to SQLite database file
            writes_per_second: Target write rate (0 = unlimited)
        """
        self.db_path = db_path
        self.writes_per_second = writes_per_second
        self.writes: List[Tuple[str, float]] = []  # [(write_id, commit_timestamp), ...]
        self.running = False
        self.thread = None
        self._lock = threading.Lock()

    def start(self):
        """Start writing in background thread."""
        if self.running:
            return

        self.running = True
        self.thread = threading.Thread(target=self._write_loop, daemon=True)
        self.thread.start()

    def stop(self):
        """Stop writing and wait for thread to finish."""
        self.running = False
        if self.thread:
            self.thread.join(timeout=5.0)

    def get_writes(self) -> List[Tuple[str, float]]:
        """Get list of all writes performed.

        Returns:
            List of (write_id, commit_timestamp) tuples
        """
        with self._lock:
            return self.writes.copy()

    def _write_loop(self):
        """Write loop with rate limiting.

        Creates table if needed, then writes rows with:
        - id: UUID (primary key)
        - created_at: Commit timestamp (float, seconds since epoch)
        - data: Random BLOB (1024 bytes)
        """
        # Open connection
        conn = sqlite3.connect(str(self.db_path))

        # Create table if it doesn't exist
        conn.execute("""
            CREATE TABLE IF NOT EXISTS benchmark_data (
                id TEXT PRIMARY KEY,
                created_at REAL,
                data BLOB
            )
        """)
        conn.commit()

        # Calculate sleep time for rate limiting
        sleep_time = 1.0 / self.writes_per_second if self.writes_per_second > 0 else 0

        # Write loop
        while self.running:
            try:
                # Generate write data
                write_id = str(uuid4())
                commit_ts = time.time()
                data = os.urandom(1024)

                # Write to database
                conn.execute(
                    "INSERT INTO benchmark_data (id, created_at, data) VALUES (?, ?, ?)",
                    (write_id, commit_ts, data)
                )
                conn.commit()

                # Track the write
                with self._lock:
                    self.writes.append((write_id, commit_ts))

                # Rate limiting
                if sleep_time > 0:
                    time.sleep(sleep_time)

            except Exception as e:
                # Log error but continue (don't crash the benchmark)
                print(f"Write error on {self.db_path}: {e}")
                time.sleep(0.1)  # Brief pause before retrying

        # Cleanup
        conn.close()


def create_benchmark_databases(
    work_dir: Path,
    count: int,
    size_kb: int = 100,
    prefix: str = "bench"
) -> List[Path]:
    """Create empty SQLite databases for benchmarking.

    Args:
        work_dir: Directory to create databases in
        count: Number of databases to create
        size_kb: Approximate initial size (via padding)
        prefix: Filename prefix

    Returns:
        List of database file paths
    """
    work_dir.mkdir(parents=True, exist_ok=True)

    databases = []
    for i in range(count):
        db_path = work_dir / f"{prefix}_db_{i}.db"

        # Create database with WAL mode
        conn = sqlite3.connect(str(db_path))
        conn.execute("PRAGMA journal_mode=WAL")

        # Add initial data if size_kb specified
        if size_kb > 0:
            # Create padding table
            conn.execute("""
                CREATE TABLE IF NOT EXISTS _padding (
                    id INTEGER PRIMARY KEY,
                    data BLOB
                )
            """)

            # Estimate rows needed (each row ~1KB with BLOB)
            rows_needed = max(1, size_kb // 1)
            for j in range(rows_needed):
                conn.execute(
                    "INSERT INTO _padding (data) VALUES (?)",
                    (os.urandom(1024),)
                )

            conn.commit()

        conn.close()
        databases.append(db_path)

    return databases


if __name__ == "__main__":
    # Simple test
    import tempfile
    import shutil

    print("Testing DatabaseWriter...")

    # Create temp directory
    temp_dir = Path(tempfile.mkdtemp())

    try:
        # Create test database
        db_path = temp_dir / "test.db"

        # Test writer
        writer = DatabaseWriter(db_path, writes_per_second=10)
        writer.start()

        print("Writing for 5 seconds at 10 writes/sec...")
        time.sleep(5)

        writer.stop()

        # Check results
        writes = writer.get_writes()
        print(f"Completed {len(writes)} writes")

        # Verify in database
        conn = sqlite3.connect(str(db_path))
        cursor = conn.execute("SELECT COUNT(*) FROM benchmark_data")
        count = cursor.fetchone()[0]
        conn.close()

        print(f"Database contains {count} rows")
        assert len(writes) == count, "Mismatch between tracked writes and database rows"

        print("✓ Test passed!")

    finally:
        # Cleanup
        shutil.rmtree(temp_dir)
