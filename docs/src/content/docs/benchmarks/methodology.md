---
title: Benchmark Methodology
description: How walrust benchmarks are conducted for transparency and reproducibility
---

This page describes how walrust benchmarks are conducted, ensuring transparency and reproducibility.

## Environment

### Local Development

Benchmarks can be run locally using MinIO as an S3-compatible storage backend:

```bash
make bench-minio   # Start MinIO container
make bench-all     # Run full suite
```

This ensures consistent results regardless of network conditions.

### CI Environment

GitHub Actions runs benchmarks on each release with:
- **Runner**: `ubuntu-latest` (2-core CPU, 7 GB RAM)
- **Storage**: MinIO service container
- **Litestream**: v0.3.13 for comparison

## Benchmark Categories

### Memory Comparison (`bench/compare.py`)

Measures RSS memory usage of walrust vs litestream when watching N databases:

1. Create N SQLite databases with sample data
2. Start walrust/litestream watching all databases
3. Wait for initial sync to complete
4. Measure RSS memory via `psutil`
5. (Optional) Generate write load and measure "active" memory

**Parameters:**
- Database counts: 1, 10, 100 (configurable)
- Sample size: 100 rows per database
- Measurement delay: 5 seconds after startup

### Multi-Database Performance (`bench/multidb.py`)

#### Startup Time
Time from process start to ready state:
1. Create N pre-synced databases
2. Start walrust, measure time until watching
3. Repeat 3 times, report mean

#### Change Detection Latency
Time from SQLite commit to walrust detection:
1. Start walrust watching N databases
2. Insert row, record timestamp
3. Monitor walrust output for detection
4. Calculate latency, collect 100 samples per database count
5. Report p50, p95, p99 percentiles

#### CPU Scaling
CPU usage under load as database count increases:
1. Start walrust watching N databases
2. Generate concurrent write load
3. Sample CPU usage over 10 seconds
4. Report average CPU percentage

### Real-World Benchmarks (`bench/realworld.py`)

#### Sync Latency
End-to-end time from write to S3:
1. Insert row into database
2. Wait for S3 object to appear
3. Measure total latency
4. Collect 50 samples, report percentiles

#### Restore Performance
Time to restore database from S3:
1. Sync database with sample data
2. Delete local database
3. Run `walrust restore`
4. Measure total time

#### LTX Throughput
Speed of LTX file generation:
1. Generate WAL frames programmatically
2. Convert to LTX format
3. Measure throughput in MB/s

## Statistical Methods

### Percentile Calculation

We use linear interpolation for percentiles (same as NumPy):

```python
def percentile(data, p):
    sorted_data = sorted(data)
    k = (len(sorted_data) - 1) * p / 100
    f = int(k)
    c = f + 1
    if c >= len(sorted_data):
        return sorted_data[-1]
    return sorted_data[f] + (k - f) * (sorted_data[c] - sorted_data[f])
```

### Outlier Handling

- No outliers are removed from latency measurements
- Results report both mean and percentiles for transparency
- Standard deviation is included where applicable

## Reproducibility

### Running Locally

```bash
# Clone and build
git clone https://github.com/russellromney/walrust.git
cd walrust
cargo build --release

# Run with MinIO
make bench-minio
make bench-all
```

### Verification

Compare your results with CI results:
- CI results are uploaded as artifacts on each release
- JSON output allows programmatic comparison

### Known Limitations

1. **CI variability**: GitHub Actions runners have variable performance
2. **Memory measurement**: RSS includes shared libraries
3. **Network latency**: MinIO eliminates S3 latency, real S3 will be slower
4. **Warm cache**: Multiple runs may benefit from OS file cache

## Output Format

All benchmarks output JSON with this schema:

```json
{
  "version": "1.0",
  "environment": {
    "platform": "linux",
    "python_version": "3.12.0",
    "walrust_version": "0.3.0",
    "litestream_version": "0.3.13",
    "storage_backend": "minio",
    "timestamp": "2024-01-15T12:00:00Z"
  },
  "benchmarks": {
    "memory_scaling": [...],
    "startup_time": [...],
    ...
  }
}
```
