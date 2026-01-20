---
title: Performance Benchmarks
description: Walrust performance characteristics and comparison with Litestream
---

Walrust is optimized to use less memory than Litestream. This page summarizes performance characteristics based on our benchmark suite.

## Quick Results

### Memory Efficiency

Walrust uses less memory than Litestream, especially when watching multiple databases:

| Databases | walrust | litestream | Reduction |
|-----------|---------|------------|-----------|
| 1         | 19 MB   | 37 MB      | 49%       |
| 10        | 20 MB   | 61 MB      | 67%       |
| 100       | 19 MB   | 228 MB     | 92%       |

Walrust's memory usage remains ~19-20 MB regardless of database count.

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
