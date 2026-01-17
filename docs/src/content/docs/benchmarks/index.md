---
title: Performance Benchmarks
description: Walrust performance characteristics and comparison with Litestream
---

Walrust is designed for efficient multi-database SQLite replication. This page summarizes performance characteristics based on our benchmark suite.

## Quick Results

### Memory Efficiency

Walrust uses significantly less memory than Litestream, especially when watching multiple databases:

| Databases | walrust | litestream | Savings |
|-----------|---------|------------|---------|
| 1         | ~8 MB   | ~25 MB     | ~68%    |
| 10        | ~12 MB  | ~120 MB    | ~90%    |
| 100       | ~45 MB  | ~1.1 GB    | ~96%    |

This efficiency comes from walrust's optimized memory management and buffer allocation strategies.

### Change Detection

Walrust detects SQLite changes with low latency:

| Databases | p50 Latency | p99 Latency |
|-----------|-------------|-------------|
| 10        | < 5ms       | < 15ms      |
| 100       | < 10ms      | < 50ms      |

### Startup Time

Walrust starts quickly even with many databases:

| Databases | Startup Time |
|-----------|--------------|
| 10        | < 100ms      |
| 100       | < 500ms      |
| 1000      | < 3s         |

## Running Benchmarks

To run the benchmark suite locally:

```bash
# Start MinIO for S3-compatible storage
make bench-minio

# Run all benchmarks
make bench-all

# Or run individual benchmarks
make bench-compare    # Memory/CPU comparison
make bench-multidb    # Multi-database performance
make bench-realworld  # Sync latency, restore, throughput

# Stop MinIO when done
make bench-minio-stop
```

### JSON Output

All benchmarks support JSON output for CI integration:

```bash
python bench/compare.py --use-minio --json > results.json
```

## Learn More

- [Methodology](/benchmarks/methodology/) - How benchmarks are run
- [Latest Results](/benchmarks/results/) - Detailed benchmark data
