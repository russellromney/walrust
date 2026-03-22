---
title: Latest Benchmark Results
description: Detailed benchmark data from the latest walrust release
---

This page shows detailed benchmark results. Results are updated with each release.

## Memory Scaling

Memory usage (RSS) when watching multiple SQLite databases:

| Databases | walrust (MB) | litestream (MB) | Reduction |
|-----------|--------------|-----------------|-----------|
| 1         | 19           | 36              | 47%       |
| 10        | 19           | 55              | 65%       |
| 100       | 20           | 160             | 88%       |

*Measured with 100KB databases, syncing to Tigris S3 on macOS.*

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

- **Platform**: macOS (Apple Silicon)
- **Storage**: Tigris S3
- **Database size**: 100KB each
- **walrust**: v0.4.0
- **litestream**: v0.5.2

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
