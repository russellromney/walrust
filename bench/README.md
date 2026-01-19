# Walrust Benchmarks

Performance benchmarks for walrust covering micro-benchmarks, comparison with litestream, and real-world scenarios.

## Quick Start

```bash
# Micro-benchmarks (WAL parsing, SHA256)
make bench

# Compare walrust vs litestream (memory/CPU)
make bench-compare

# Real-world benchmarks (sync latency, restore performance)
make bench-realworld
```

## Benchmark Types

### 1. Micro-benchmarks (`cargo bench`)

CPU-bound operations measured with [brunch](https://docs.rs/brunch):

- **WAL header parsing**: ~47ns per header
- **WAL frame parsing**: ~32ns per frame
- **SHA256 checksums**: 6μs (1KB) → 5ms (1MB)

Run with: `make bench` or `cargo bench`

### 2. Comparison Benchmarks (`compare.py`)

Memory usage comparison between walrust and litestream under different workloads.

#### Idle Databases (no active writes)

| DBs | Litestream | Walrust | Savings |
|-----|------------|---------|---------|
| 5 | 40 MB | 13 MB | **27 MB** |
| 10 | 50 MB | 14 MB | **36 MB** |
| 20 | 62 MB | 17 MB | **45 MB** |
| 50 | 71 MB | 17 MB | **54 MB** |

#### Under Write Load

Results with active writes (10-100 writes/sec per database):

| DBs | Writes/s/db | Litestream | Walrust | Savings |
|-----|-------------|------------|---------|---------|
| 10 | 10 | 42 MB | 23 MB | **19 MB** |
| 10 | 50 | 38 MB | 20 MB | **18 MB** |
| 10 | 100 | 68 MB | 22 MB | **46 MB** |
| 20 | 10 | 80 MB | 19 MB | **61 MB** |
| 20 | 50 | 95 MB | 23 MB | **71 MB** |
| 20 | 100 | 103 MB | 24 MB | **78 MB** |
| 50 | 10 | 266 MB | 21 MB | **245 MB** |
| 50 | 50 | 227 MB | 29 MB | **198 MB** |
| 50 | 100 | 285 MB | 45 MB | **240 MB** |

#### Key Findings

- Both tools run as a single process watching multiple databases
- **Walrust scales flat**: 17-45 MB for 5-50 databases regardless of write load
- **Litestream scales linearly**: Memory grows with both DB count and write rate
- **At 50 DBs + 100 writes/sec/db**: walrust uses **6x less memory** (45 MB vs 285 MB)
- Memory savings increase dramatically under load

**Usage:**
```bash
# Full comparison (idle)
python bench/compare.py

# Specific database counts
python bench/compare.py --dbs 1,5,10,20,50

# With active write load
python bench/compare.py --dbs 10,20,50 --writes-per-sec 10
python bench/compare.py --dbs 10,20,50 --writes-per-sec 50
python bench/compare.py --dbs 10,20,50 --writes-per-sec 100

# Only walrust (skip litestream)
python bench/compare.py --walrust-only

# Longer measurement (default: 5s)
python bench/compare.py --duration 10

# JSON output
python bench/compare.py --json
```

**Requirements:**
- `uv pip install psutil`
- `brew install litestream` (optional, for comparison)

### 3. Real-World Benchmarks (`realworld.py`)

End-to-end performance metrics that users care about:

#### a) Sync Latency
Time from SQLite commit to S3 object appearing.

Measures: p50, p95, p99, mean, max latency over N commits.

#### b) Restore Performance
Time to restore databases of various sizes from S3.

Measures: download time, restore time, throughput (MB/s).

#### c) Multi-DB Throughput
Concurrent writes to N databases, measuring walrust's ability to keep up.

Measures: writes/sec sustained, sync lag, CPU%, memory.

#### d) Network Recovery
Recovery time after simulated network outage.

Measures: catchup time, writes lost (should be 0).

#### e) Write Throughput
Maximum sustainable commits per second with walrust watching.

Measures: max commits/sec, average latency.

#### f) Checkpoint Impact
Impact of SQLite checkpoints on sync latency.

Measures: normal latency, post-checkpoint latency, checkpoint duration.

**Usage:**
```bash
# Run all real-world benchmarks
python bench/realworld.py

# Run specific test
python bench/realworld.py --test sync
python bench/realworld.py --test restore
python bench/realworld.py --test multi-db
python bench/realworld.py --test network
python bench/realworld.py --test throughput
python bench/realworld.py --test checkpoint

# JSON output
python bench/realworld.py --json
```

**Requirements:**
- `uv pip install psutil boto3`
- Tigris/S3 credentials in environment
- `WALSYNC_TEST_BUCKET` env var

## Environment Setup

```bash
# Tigris credentials (from ourfam/.env or similar)
export AWS_ACCESS_KEY_ID=...
export AWS_SECRET_ACCESS_KEY=...
export AWS_ENDPOINT_URL_S3=https://fly.storage.tigris.dev
export WALSYNC_TEST_BUCKET=s3://walrust-bench

# Or use direnv/.envrc
```

## CI Integration

Add to GitHub Actions:

```yaml
- name: Build release binary
  run: cargo build --release

- name: Run micro-benchmarks
  run: cargo bench

- name: Run real-world benchmarks
  env:
    AWS_ACCESS_KEY_ID: ${{ secrets.AWS_ACCESS_KEY_ID }}
    AWS_SECRET_ACCESS_KEY: ${{ secrets.AWS_SECRET_ACCESS_KEY }}
    AWS_ENDPOINT_URL_S3: ${{ secrets.AWS_ENDPOINT_URL_S3 }}
    WALSYNC_TEST_BUCKET: s3://walrust-bench
  run: python bench/realworld.py --json > bench-results.json

- name: Upload benchmark results
  uses: actions/upload-artifact@v3
  with:
    name: benchmark-results
    path: bench-results.json
```

## Interpreting Results

**Sync latency:**
- p50 < 100ms: Good
- p95 < 500ms: Acceptable
- p99 < 1s: Watch for network issues
- Max > 5s: Check S3 endpoint

**Restore:**
- 10MB/s+: Good for Tigris
- 50MB/s+: Good for S3 same-region
- <5MB/s: Check network

**Multi-DB:**
- Memory ~10MB regardless of DB count: Good
- CPU <20% for 10 DBs @ 100 writes/sec: Good
- Sync lag p95 <1s: Good

## Adding New Benchmarks

1. Add to `realworld.py` as a new function
2. Return a dataclass with results
3. Add CLI flag if needed
4. Update this README

## Notes

- Benchmarks require real S3/Tigris (no mocking)
- Use dedicated test bucket to avoid prod data
- Some benchmarks may incur S3 costs (minimal)
- Network-dependent results may vary
