---
title: Latest Benchmark Results
description: Detailed benchmark data from the latest walrust release
---

This page shows detailed benchmark results. Results are updated with each release.

## Memory Scaling

Memory usage (RSS) when watching multiple SQLite databases:

### Idle Memory (No Active Writes)

| Databases | walrust (MB) | litestream (MB) | Reduction |
|-----------|--------------|-----------------|-----------|
| 1         | 8.2          | 24.8            | 67%       |
| 10        | 11.5         | 118.2           | 90%       |
| 100       | 43.7         | 1,124.5         | 96%       |

### Active Memory (Under Write Load)

| Databases | walrust (MB) | litestream (MB) | Reduction |
|-----------|--------------|-----------------|-----------|
| 1         | 9.1          | 28.3            | 68%       |
| 10        | 14.2         | 142.1           | 90%       |
| 100       | 52.3         | 1,298.7         | 96%       |

## Startup Time

Time to start watching N databases (mean of 3 runs):

| Databases | Startup Time (ms) |
|-----------|-------------------|
| 1         | 45                |
| 10        | 82                |
| 100       | 423               |

## Change Detection Latency

Time from SQLite write to walrust detection:

| Databases | p50 (ms) | p95 (ms) | p99 (ms) |
|-----------|----------|----------|----------|
| 1         | 2.1      | 4.8      | 8.2      |
| 10        | 4.3      | 12.1     | 18.7     |
| 100       | 8.7      | 28.4     | 52.3     |

## CPU Usage Under Load

Average CPU usage during concurrent writes:

| Databases | CPU % |
|-----------|-------|
| 1         | 2.1   |
| 10        | 8.4   |
| 100       | 24.7  |

## Sync Latency (End-to-End)

Time from SQLite write to data available in S3:

| Operation      | p50 (ms) | p95 (ms) | p99 (ms) |
|----------------|----------|----------|----------|
| Single write   | 45       | 82       | 124      |
| Batch (100)    | 52       | 98       | 156      |

*Note: Using MinIO local storage. Real S3 latency will be higher due to network.*

## Restore Performance

Time to restore database from S3:

| Database Size | Restore Time (ms) |
|---------------|-------------------|
| 100 KB        | 234               |
| 1 MB          | 412               |
| 10 MB         | 1,847             |

## Test Environment

These results were collected on:

- **Platform**: GitHub Actions `ubuntu-latest`
- **CPU**: 2 cores
- **RAM**: 7 GB
- **Storage**: MinIO (local S3-compatible)
- **walrust**: v0.3.0
- **litestream**: v0.3.13

## Running Your Own Benchmarks

```bash
# Start MinIO
make bench-minio

# Run and get JSON output
python bench/compare.py --use-minio --json
python bench/multidb.py --use-minio --json
python bench/realworld.py --use-minio --json
```

## CI Artifacts

Benchmark JSON files are available as artifacts on each release:
- [Latest release artifacts](https://github.com/russellromney/walrust/releases/latest)
