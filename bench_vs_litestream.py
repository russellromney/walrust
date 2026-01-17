#!/usr/bin/env python3
"""
Benchmark: Walrust (disk queue) vs Litestream
Tests scalability from 1 to 500 databases with various write rates.

Requirements:
- walrust built with disk queue (v0.1.9+)
- litestream installed
- Valid S3 credentials in ../.env
"""

import os
import sys
import time
import sqlite3
import subprocess
import threading
import json
import psutil
from pathlib import Path
from dataclasses import dataclass, asdict
from typing import List, Dict
import statistics

@dataclass
class BenchmarkConfig:
    num_dbs: int
    writes_per_second_per_db: int
    duration_seconds: int

@dataclass
class BenchmarkResult:
    config: BenchmarkConfig
    tool: str  # "walrust" or "litestream"

    # Performance metrics
    total_writes: int
    successful_writes: int
    failed_writes: int
    duration_seconds: float
    writes_per_second: float

    # Latency metrics (milliseconds)
    avg_write_latency_ms: float
    p50_write_latency_ms: float
    p95_write_latency_ms: float
    p99_write_latency_ms: float
    max_write_latency_ms: float

    # Resource metrics
    peak_memory_mb: float
    avg_cpu_percent: float
    peak_cpu_percent: float

    # Upload metrics
    total_uploads: int
    successful_uploads: int
    failed_uploads: int
    avg_upload_latency_ms: float

    # Success rate
    write_success_rate: float
    upload_success_rate: float


class DatabaseWriter:
    """Writes to a SQLite database at a target rate."""

    def __init__(self, db_path: Path, writes_per_second: int):
        self.db_path = db_path
        self.writes_per_second = writes_per_second
        self.latencies = []
        self.successful = 0
        self.failed = 0
        self.running = False
        self.thread = None

    def start(self):
        """Start writing in background thread."""
        self.running = True
        self.thread = threading.Thread(target=self._write_loop)
        self.thread.start()

    def stop(self):
        """Stop writing and wait for thread."""
        self.running = False
        if self.thread:
            self.thread.join()

    def _write_loop(self):
        """Write loop with rate limiting."""
        conn = sqlite3.connect(str(self.db_path))
        conn.execute("CREATE TABLE IF NOT EXISTS metrics (id INTEGER PRIMARY KEY, value REAL, timestamp INTEGER)")
        conn.commit()

        sleep_time = 1.0 / self.writes_per_second if self.writes_per_second > 0 else 1.0

        while self.running:
            start = time.time()
            try:
                conn.execute("INSERT INTO metrics (value, timestamp) VALUES (?, ?)",
                           (time.time(), int(time.time() * 1000)))
                conn.commit()

                elapsed = (time.time() - start) * 1000  # ms
                self.latencies.append(elapsed)
                self.successful += 1
            except Exception as e:
                print(f"Write failed: {e}")
                self.failed += 1

            # Rate limiting
            if self.writes_per_second > 0:
                time.sleep(sleep_time)

        conn.close()


class ResourceMonitor:
    """Monitors CPU and memory usage of a process."""

    def __init__(self, process_name: str):
        self.process_name = process_name
        self.samples = []
        self.running = False
        self.thread = None

    def start(self):
        """Start monitoring in background thread."""
        self.running = True
        self.thread = threading.Thread(target=self._monitor_loop)
        self.thread.start()

    def stop(self):
        """Stop monitoring."""
        self.running = False
        if self.thread:
            self.thread.join()

    def _monitor_loop(self):
        """Sample resource usage every 100ms."""
        while self.running:
            try:
                for proc in psutil.process_iter(['name', 'cpu_percent', 'memory_info']):
                    if self.process_name in proc.info['name']:
                        cpu = proc.info['cpu_percent']
                        mem_mb = proc.info['memory_info'].rss / (1024 * 1024)
                        self.samples.append({'cpu': cpu, 'mem_mb': mem_mb})
                        break
            except (psutil.NoSuchProcess, psutil.AccessDenied):
                pass

            time.sleep(0.1)

    def get_stats(self):
        """Return aggregated stats."""
        if not self.samples:
            return {'peak_mem_mb': 0, 'avg_cpu': 0, 'peak_cpu': 0}

        return {
            'peak_mem_mb': max(s['mem_mb'] for s in self.samples),
            'avg_cpu': statistics.mean(s['cpu'] for s in self.samples),
            'peak_cpu': max(s['cpu'] for s in self.samples),
        }


def setup_walrust(work_dir: Path, num_dbs: int) -> List[Path]:
    """Setup walrust databases with disk queue."""
    print(f"Setting up {num_dbs} walrust databases...")

    dbs = []
    for i in range(num_dbs):
        db_path = work_dir / f"walrust_db_{i}.db"
        db_path.parent.mkdir(parents=True, exist_ok=True)

        # Create empty database
        conn = sqlite3.connect(str(db_path))
        conn.execute("PRAGMA journal_mode=WAL")
        conn.close()

        dbs.append(db_path)

    return dbs


def setup_litestream(work_dir: Path, num_dbs: int) -> List[Path]:
    """Setup Litestream databases."""
    print(f"Setting up {num_dbs} Litestream databases...")

    dbs = []
    for i in range(num_dbs):
        db_path = work_dir / f"litestream_db_{i}.db"
        db_path.parent.mkdir(parents=True, exist_ok=True)

        # Create empty database
        conn = sqlite3.connect(str(db_path))
        conn.execute("PRAGMA journal_mode=WAL")
        conn.close()

        dbs.append(db_path)

    return dbs


def run_walrust_benchmark(config: BenchmarkConfig, work_dir: Path) -> BenchmarkResult:
    """Run benchmark with walrust."""
    print(f"\n{'='*60}")
    print(f"WALRUST: {config.num_dbs} DBs @ {config.writes_per_second_per_db} writes/s/DB")
    print(f"{'='*60}")

    # Setup databases
    dbs = setup_walrust(work_dir, config.num_dbs)

    # Start walrust sync processes (one per DB)
    processes = []
    for db_path in dbs:
        cmd = [
            "./target/release/walrust", "sync",
            "--database", str(db_path),
            "--bucket", "walsync-test",
            "--endpoint", "https://t3.storage.dev",
        ]
        proc = subprocess.Popen(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        processes.append(proc)

    # Wait for startup
    time.sleep(2)

    # Start resource monitoring
    monitor = ResourceMonitor("walrust")
    monitor.start()

    # Start writers
    writers = [DatabaseWriter(db, config.writes_per_second_per_db) for db in dbs]
    for writer in writers:
        writer.start()

    print(f"Running for {config.duration_seconds} seconds...")
    start_time = time.time()

    # Run benchmark
    time.sleep(config.duration_seconds)

    # Stop writers
    for writer in writers:
        writer.stop()

    duration = time.time() - start_time

    # Stop monitoring
    monitor.stop()
    resource_stats = monitor.get_stats()

    # Stop walrust processes
    for proc in processes:
        proc.terminate()
        proc.wait()

    # Collect results
    all_latencies = []
    total_successful = 0
    total_failed = 0

    for writer in writers:
        all_latencies.extend(writer.latencies)
        total_successful += writer.successful
        total_failed += writer.failed

    all_latencies.sort()

    if all_latencies:
        avg_latency = statistics.mean(all_latencies)
        p50_latency = all_latencies[int(len(all_latencies) * 0.50)]
        p95_latency = all_latencies[int(len(all_latencies) * 0.95)]
        p99_latency = all_latencies[int(len(all_latencies) * 0.99)]
        max_latency = max(all_latencies)
    else:
        avg_latency = p50_latency = p95_latency = p99_latency = max_latency = 0

    total_writes = total_successful + total_failed
    writes_per_sec = total_successful / duration if duration > 0 else 0
    write_success_rate = (total_successful / total_writes * 100) if total_writes > 0 else 0

    result = BenchmarkResult(
        config=config,
        tool="walrust",
        total_writes=total_writes,
        successful_writes=total_successful,
        failed_writes=total_failed,
        duration_seconds=duration,
        writes_per_second=writes_per_sec,
        avg_write_latency_ms=avg_latency,
        p50_write_latency_ms=p50_latency,
        p95_write_latency_ms=p95_latency,
        p99_write_latency_ms=p99_latency,
        max_write_latency_ms=max_latency,
        peak_memory_mb=resource_stats['peak_mem_mb'],
        avg_cpu_percent=resource_stats['avg_cpu'],
        peak_cpu_percent=resource_stats['peak_cpu'],
        total_uploads=0,  # TODO: get from walrust stats
        successful_uploads=0,
        failed_uploads=0,
        avg_upload_latency_ms=0,
        write_success_rate=write_success_rate,
        upload_success_rate=100.0,
    )

    print_result_summary(result)
    return result


def run_litestream_benchmark(config: BenchmarkConfig, work_dir: Path) -> BenchmarkResult:
    """Run benchmark with Litestream."""
    print(f"\n{'='*60}")
    print(f"LITESTREAM: {config.num_dbs} DBs @ {config.writes_per_second_per_db} writes/s/DB")
    print(f"{'='*60}")

    # Setup databases
    dbs = setup_litestream(work_dir, config.num_dbs)

    # Create Litestream config
    config_path = work_dir / "litestream.yml"
    litestream_config = {
        "dbs": [
            {
                "path": str(db),
                "replicas": [{
                    "url": "s3://walsync-test",
                    "endpoint": "https://t3.storage.dev",
                }]
            }
            for db in dbs
        ]
    }

    with open(config_path, 'w') as f:
        import yaml
        yaml.dump(litestream_config, f)

    # Start Litestream
    litestream_proc = subprocess.Popen(
        ["litestream", "replicate", "-config", str(config_path)],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL
    )

    # Wait for startup
    time.sleep(2)

    # Start resource monitoring
    monitor = ResourceMonitor("litestream")
    monitor.start()

    # Start writers
    writers = [DatabaseWriter(db, config.writes_per_second_per_db) for db in dbs]
    for writer in writers:
        writer.start()

    print(f"Running for {config.duration_seconds} seconds...")
    start_time = time.time()

    # Run benchmark
    time.sleep(config.duration_seconds)

    # Stop writers
    for writer in writers:
        writer.stop()

    duration = time.time() - start_time

    # Stop monitoring
    monitor.stop()
    resource_stats = monitor.get_stats()

    # Stop Litestream
    litestream_proc.terminate()
    litestream_proc.wait()

    # Collect results
    all_latencies = []
    total_successful = 0
    total_failed = 0

    for writer in writers:
        all_latencies.extend(writer.latencies)
        total_successful += writer.successful
        total_failed += writer.failed

    all_latencies.sort()

    if all_latencies:
        avg_latency = statistics.mean(all_latencies)
        p50_latency = all_latencies[int(len(all_latencies) * 0.50)]
        p95_latency = all_latencies[int(len(all_latencies) * 0.95)]
        p99_latency = all_latencies[int(len(all_latencies) * 0.99)]
        max_latency = max(all_latencies)
    else:
        avg_latency = p50_latency = p95_latency = p99_latency = max_latency = 0

    total_writes = total_successful + total_failed
    writes_per_sec = total_successful / duration if duration > 0 else 0
    write_success_rate = (total_successful / total_writes * 100) if total_writes > 0 else 0

    result = BenchmarkResult(
        config=config,
        tool="litestream",
        total_writes=total_writes,
        successful_writes=total_successful,
        failed_writes=total_failed,
        duration_seconds=duration,
        writes_per_second=writes_per_sec,
        avg_write_latency_ms=avg_latency,
        p50_write_latency_ms=p50_latency,
        p95_write_latency_ms=p95_latency,
        p99_write_latency_ms=p99_latency,
        max_write_latency_ms=max_latency,
        peak_memory_mb=resource_stats['peak_mem_mb'],
        avg_cpu_percent=resource_stats['avg_cpu'],
        peak_cpu_percent=resource_stats['peak_cpu'],
        total_uploads=0,
        successful_uploads=0,
        failed_uploads=0,
        avg_upload_latency_ms=0,
        write_success_rate=write_success_rate,
        upload_success_rate=100.0,
    )

    print_result_summary(result)
    return result


def print_result_summary(result: BenchmarkResult):
    """Print summary of benchmark result."""
    print(f"\nResults:")
    print(f"  Writes: {result.successful_writes}/{result.total_writes} ({result.write_success_rate:.1f}%)")
    print(f"  Throughput: {result.writes_per_second:.1f} writes/s")
    print(f"  Latency: avg={result.avg_write_latency_ms:.2f}ms p50={result.p50_write_latency_ms:.2f}ms " +
          f"p95={result.p95_write_latency_ms:.2f}ms p99={result.p99_write_latency_ms:.2f}ms")
    print(f"  Resources: {result.peak_memory_mb:.1f}MB RAM, {result.avg_cpu_percent:.1f}% CPU (peak {result.peak_cpu_percent:.1f}%)")


def save_results(results: List[BenchmarkResult], output_file: Path):
    """Save results to JSON."""
    data = [asdict(r) for r in results]
    with open(output_file, 'w') as f:
        json.dump(data, f, indent=2)
    print(f"\nResults saved to {output_file}")


def generate_comparison_report(results: List[BenchmarkResult], output_file: Path):
    """Generate markdown comparison report."""

    # Group by config
    by_config = {}
    for r in results:
        key = (r.config.num_dbs, r.config.writes_per_second_per_db)
        if key not in by_config:
            by_config[key] = {}
        by_config[key][r.tool] = r

    lines = [
        "# Walrust vs Litestream Benchmark Results",
        "",
        f"**Date**: {time.strftime('%Y-%m-%d %H:%M:%S')}",
        "",
        "## Summary",
        "",
        "| DBs | Writes/s/DB | Tool | Throughput | P50 Latency | P95 Latency | P99 Latency | Peak Memory | Avg CPU |",
        "|-----|-------------|------|------------|-------------|-------------|-------------|-------------|---------|",
    ]

    for (num_dbs, writes_per_sec), tools in sorted(by_config.items()):
        for tool in ['walrust', 'litestream']:
            if tool in tools:
                r = tools[tool]
                lines.append(
                    f"| {num_dbs} | {writes_per_sec} | **{tool}** | "
                    f"{r.writes_per_second:.1f} w/s | "
                    f"{r.p50_write_latency_ms:.2f}ms | "
                    f"{r.p95_write_latency_ms:.2f}ms | "
                    f"{r.p99_write_latency_ms:.2f}ms | "
                    f"{r.peak_memory_mb:.1f}MB | "
                    f"{r.avg_cpu_percent:.1f}% |"
                )

    lines.extend([
        "",
        "## Performance Comparison",
        "",
    ])

    for (num_dbs, writes_per_sec), tools in sorted(by_config.items()):
        if 'walrust' in tools and 'litestream' in tools:
            wr = tools['walrust']
            ls = tools['litestream']

            throughput_diff = ((wr.writes_per_second - ls.writes_per_second) / ls.writes_per_second * 100) if ls.writes_per_second > 0 else 0
            latency_diff = ((wr.p95_write_latency_ms - ls.p95_write_latency_ms) / ls.p95_write_latency_ms * 100) if ls.p95_write_latency_ms > 0 else 0
            mem_diff = ((wr.peak_memory_mb - ls.peak_memory_mb) / ls.peak_memory_mb * 100) if ls.peak_memory_mb > 0 else 0

            lines.extend([
                f"### {num_dbs} DBs @ {writes_per_sec} writes/s/DB",
                "",
                f"- **Throughput**: Walrust {throughput_diff:+.1f}% vs Litestream",
                f"- **P95 Latency**: Walrust {latency_diff:+.1f}% vs Litestream",
                f"- **Memory**: Walrust {mem_diff:+.1f}% vs Litestream",
                "",
            ])

    with open(output_file, 'w') as f:
        f.write('\n'.join(lines))

    print(f"Comparison report saved to {output_file}")


def main():
    # Build walrust first
    print("Building walrust...")
    subprocess.run(["cargo", "build", "--release"], check=True)

    # Load S3 credentials
    env_file = Path("../.env")
    if env_file.exists():
        from dotenv import load_dotenv
        load_dotenv(env_file)

    # Test configurations
    configs = [
        # Baseline
        BenchmarkConfig(num_dbs=1, writes_per_second_per_db=10, duration_seconds=30),
        BenchmarkConfig(num_dbs=1, writes_per_second_per_db=100, duration_seconds=30),

        # Light load
        BenchmarkConfig(num_dbs=10, writes_per_second_per_db=10, duration_seconds=30),
        BenchmarkConfig(num_dbs=10, writes_per_second_per_db=50, duration_seconds=30),

        # Medium load
        BenchmarkConfig(num_dbs=50, writes_per_second_per_db=10, duration_seconds=30),
        BenchmarkConfig(num_dbs=50, writes_per_second_per_db=50, duration_seconds=30),

        # Heavy load
        BenchmarkConfig(num_dbs=100, writes_per_second_per_db=10, duration_seconds=30),
        BenchmarkConfig(num_dbs=100, writes_per_second_per_db=50, duration_seconds=30),

        # Extreme load
        BenchmarkConfig(num_dbs=250, writes_per_second_per_db=10, duration_seconds=30),
        BenchmarkConfig(num_dbs=500, writes_per_second_per_db=10, duration_seconds=30),
    ]

    work_dir = Path("bench_work")
    work_dir.mkdir(exist_ok=True)

    results = []

    for config in configs:
        # Run walrust
        walrust_work = work_dir / f"walrust_{config.num_dbs}dbs_{config.writes_per_second_per_db}wps"
        walrust_work.mkdir(parents=True, exist_ok=True)

        try:
            result = run_walrust_benchmark(config, walrust_work)
            results.append(result)
        except Exception as e:
            print(f"Walrust benchmark failed: {e}")

        # Run litestream
        litestream_work = work_dir / f"litestream_{config.num_dbs}dbs_{config.writes_per_second_per_db}wps"
        litestream_work.mkdir(parents=True, exist_ok=True)

        try:
            result = run_litestream_benchmark(config, litestream_work)
            results.append(result)
        except Exception as e:
            print(f"Litestream benchmark failed: {e}")

        # Cleanup between runs
        subprocess.run(["rm", "-rf", str(walrust_work), str(litestream_work)])
        time.sleep(5)

    # Save results
    save_results(results, Path("BENCHMARK_VS_LITESTREAM.json"))
    generate_comparison_report(results, Path("BENCHMARK_VS_LITESTREAM.md"))

    print("\n" + "="*60)
    print("BENCHMARK COMPLETE")
    print("="*60)


if __name__ == "__main__":
    main()
