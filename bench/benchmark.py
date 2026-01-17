#!/usr/bin/env python3
"""
Walrust Benchmark Framework

Main CLI for running walrust vs litestream benchmarks.
Measures replication completeness (data loss) and sync latency.
"""

import os
import sys
import time
import json
import argparse
import shutil
from pathlib import Path
from typing import List, Dict
from dataclasses import dataclass, asdict

# Add bench/lib to path
sys.path.insert(0, str(Path(__file__).parent / 'lib'))

from workload import DatabaseWriter, create_benchmark_databases
from runners import WalrustRunner, LitestreamRunner
from monitor import ResourceMonitor
from verify import ReplicationVerifier
from config import BenchmarkConfig


@dataclass
class BenchmarkResult:
    """Results from a single benchmark run."""
    # Config
    config_name: str
    tool: str
    databases: int
    writes_per_second_per_db: int
    duration_seconds: int

    # Workload metrics
    total_writes: int
    actual_duration_seconds: float
    actual_writes_per_second: float

    # Replication metrics
    replicated_writes: int
    missing_writes: int
    data_loss: bool
    sync_latency_p50_ms: float
    sync_latency_p95_ms: float
    sync_latency_p99_ms: float
    sync_latency_max_ms: float

    # Resource metrics
    peak_memory_mb: float
    avg_memory_mb: float
    peak_cpu_percent: float
    avg_cpu_percent: float


def run_benchmark(
    config: BenchmarkConfig,
    tool: str,
    work_dir: Path,
    cleanup: bool = True
) -> BenchmarkResult:
    """Run a single benchmark.

    Args:
        config: Benchmark configuration
        tool: "walrust" or "litestream"
        work_dir: Working directory for databases
        cleanup: Clean up databases after benchmark

    Returns:
        BenchmarkResult with all metrics
    """
    print(f"\n{'='*60}")
    print(f"Running {tool.upper()} benchmark")
    print(f"  Databases: {config.databases['count']}")
    print(f"  Write rate: {config.workload['writes_per_second']} w/s/db")
    print(f"  Duration: {config.workload['duration_seconds']}s")
    print(f"{'='*60}")

    # Create databases
    print(f"\nCreating {config.databases['count']} databases...")
    db_paths = create_benchmark_databases(
        work_dir=work_dir,
        count=config.databases['count'],
        size_kb=config.databases.get('size_kb', 100),
        prefix=tool
    )
    print(f"Created {len(db_paths)} databases in {work_dir}")

    # Start replication tool
    print(f"\nStarting {tool}...")
    if tool == "walrust":
        runner = WalrustRunner()
    else:
        runner = LitestreamRunner()

    pid = runner.start(
        databases=db_paths,
        bucket=config.storage['bucket'],
        endpoint=config.storage['endpoint']
    )
    print(f"Started {tool} (PID {pid})")

    # Start resource monitoring
    print(f"Starting resource monitor...")
    monitor = ResourceMonitor(
        pid=pid,
        include_children=(tool == "litestream"),
        sample_interval_ms=config.metrics.get('resource_sample_interval_ms', 100)
    )
    monitor.start()

    # Start workload writers
    print(f"\nStarting workload writers...")
    writers = [
        DatabaseWriter(db, config.workload['writes_per_second'])
        for db in db_paths
    ]

    for writer in writers:
        writer.start()

    print(f"Writing at {config.workload['writes_per_second']} w/s/db "
          f"for {config.workload['duration_seconds']} seconds...")

    # Run benchmark
    start_time = time.time()
    time.sleep(config.workload['duration_seconds'])
    actual_duration = time.time() - start_time

    # Stop writers
    print("\nStopping writers...")
    for writer in writers:
        writer.stop()

    # Collect write data
    all_writes = []
    for writer in writers:
        all_writes.extend(writer.get_writes())

    total_writes = len(all_writes)
    actual_wps = total_writes / actual_duration if actual_duration > 0 else 0

    print(f"Completed {total_writes} writes in {actual_duration:.1f}s "
          f"({actual_wps:.1f} w/s)")

    # Stop monitoring
    monitor.stop()
    resource_stats = monitor.get_stats()

    print(f"Resource usage: {resource_stats['avg_memory_mb']:.1f} MB RAM, "
          f"{resource_stats['avg_cpu_percent']:.1f}% CPU")

    # Stop replication tool
    print(f"\nStopping {tool}...")
    runner.stop()

    # Wait for final sync
    print("Waiting 5s for final sync...")
    time.sleep(5)

    # Verify replication
    print("\nVerifying replication...")
    verifier = ReplicationVerifier(s3_endpoint=config.storage['endpoint'])

    # Verify first database as a sample
    # (In production, we'd verify all databases)
    db_name = db_paths[0].stem

    # Get expected writes for first database
    first_writer = writers[0]
    expected_writes = first_writer.get_writes()

    try:
        replication_metrics = verifier.verify(
            tool=tool,
            db_name=db_name,
            bucket=config.storage['bucket'],
            expected_writes=expected_writes
        )

        print(f"Replication verification:")
        print(f"  Total writes: {replication_metrics['total_writes']}")
        print(f"  Replicated: {replication_metrics['replicated_writes']}")
        print(f"  Missing: {replication_metrics['missing_writes']}")
        print(f"  Data loss: {replication_metrics['data_loss']}")
        if not replication_metrics['data_loss']:
            print(f"  Sync latency P95: {replication_metrics['sync_latency_p95_ms']:.1f}ms")

    except Exception as e:
        print(f"Replication verification failed: {e}")
        # Use default values
        replication_metrics = {
            'total_writes': len(expected_writes),
            'replicated_writes': 0,
            'missing_writes': len(expected_writes),
            'data_loss': True,
            'sync_latency_p50_ms': 0,
            'sync_latency_p95_ms': 0,
            'sync_latency_p99_ms': 0,
            'sync_latency_max_ms': 0,
        }

    # Cleanup databases
    if cleanup:
        print(f"\nCleaning up databases...")
        shutil.rmtree(work_dir, ignore_errors=True)

    # Build result
    result = BenchmarkResult(
        config_name=config.name,
        tool=tool,
        databases=config.databases['count'],
        writes_per_second_per_db=config.workload['writes_per_second'],
        duration_seconds=config.workload['duration_seconds'],
        total_writes=total_writes,
        actual_duration_seconds=actual_duration,
        actual_writes_per_second=actual_wps,
        replicated_writes=replication_metrics['replicated_writes'],
        missing_writes=replication_metrics['missing_writes'],
        data_loss=replication_metrics['data_loss'],
        sync_latency_p50_ms=replication_metrics['sync_latency_p50_ms'],
        sync_latency_p95_ms=replication_metrics['sync_latency_p95_ms'],
        sync_latency_p99_ms=replication_metrics['sync_latency_p99_ms'],
        sync_latency_max_ms=replication_metrics['sync_latency_max_ms'],
        peak_memory_mb=resource_stats['peak_memory_mb'],
        avg_memory_mb=resource_stats['avg_memory_mb'],
        peak_cpu_percent=resource_stats['peak_cpu_percent'],
        avg_cpu_percent=resource_stats['avg_cpu_percent'],
    )

    print(f"\n{'='*60}")
    print(f"Benchmark complete!")
    print(f"{'='*60}\n")

    return result


def save_results(results: List[BenchmarkResult], output_file: Path):
    """Save results to JSON file."""
    data = [asdict(r) for r in results]
    with open(output_file, 'w') as f:
        json.dump(data, f, indent=2)
    print(f"Results saved to {output_file}")


def main():
    parser = argparse.ArgumentParser(
        description="Run walrust vs litestream benchmarks"
    )
    parser.add_argument(
        '--config',
        type=Path,
        required=True,
        help='Path to benchmark config YAML file'
    )
    parser.add_argument(
        '--output',
        type=Path,
        default=Path('benchmark_results.json'),
        help='Output JSON file (default: benchmark_results.json)'
    )
    parser.add_argument(
        '--work-dir',
        type=Path,
        default=Path('benchmark_work'),
        help='Working directory for databases (default: benchmark_work)'
    )
    parser.add_argument(
        '--no-cleanup',
        action='store_true',
        help='Do not clean up databases after benchmark'
    )
    parser.add_argument(
        '--tools',
        nargs='+',
        choices=['walrust', 'litestream'],
        default=['walrust', 'litestream'],
        help='Tools to benchmark (default: both)'
    )

    args = parser.parse_args()

    # Load config
    print(f"Loading config from {args.config}...")
    config = BenchmarkConfig.from_yaml(args.config)
    print(f"Loaded config: {config.name}")

    # Check for matrix config
    if hasattr(config, 'matrix') and config.matrix:
        print("\nMatrix config detected - expanding configurations...")
        configs = config.expand_matrix()
        print(f"Generated {len(configs)} configurations from matrix")
    else:
        configs = [config]

    # Run benchmarks
    results = []

    for i, cfg in enumerate(configs, 1):
        print(f"\n\n{'#'*60}")
        print(f"# Configuration {i}/{len(configs)}")
        print(f"{'#'*60}")

        for tool in args.tools:
            # Create work directory for this run
            work_dir = args.work_dir / f"run_{i}_{tool}"
            work_dir.mkdir(parents=True, exist_ok=True)

            try:
                result = run_benchmark(
                    config=cfg,
                    tool=tool,
                    work_dir=work_dir,
                    cleanup=not args.no_cleanup
                )
                results.append(result)

            except Exception as e:
                print(f"\nERROR: Benchmark failed: {e}")
                import traceback
                traceback.print_exc()

            # Brief pause between runs
            time.sleep(2)

    # Save results
    save_results(results, args.output)

    # Print summary
    print(f"\n\n{'='*60}")
    print(f"BENCHMARK SUMMARY")
    print(f"{'='*60}")
    print(f"Total runs: {len(results)}")
    print(f"Results saved to: {args.output}")

    # Check for data loss
    data_loss_count = sum(1 for r in results if r.data_loss)
    if data_loss_count > 0:
        print(f"\n⚠️  WARNING: {data_loss_count} run(s) had data loss!")
    else:
        print(f"\n✓ No data loss detected in any run")

    print(f"{'='*60}\n")


if __name__ == "__main__":
    main()
