# Walrust Benchmark Results

Run date: 2026-01-13

## Real-World Performance

### Restore Performance (Tigris)

| DB Size | Time (ms) | Throughput (MB/s) |
|---------|-----------|-------------------|
| 1.2 MB  | 971       | 1.24              |
| 12 MB   | 1,502     | 8.00              |
| 120 MB  | 6,954     | 17.27             |

**Key findings:**
- Restore throughput scales with database size
- 17 MB/s for large databases is good for Tigris
- Small databases dominated by connection overhead

### Multi-Database Throughput

**Configuration:** 10 databases, 100 writes/sec

| Metric | Value |
|--------|-------|
| CPU Usage | 0.9% |
| Memory | 17.6 MB |
| Total Processes | 1 |

**Key findings:**
- Single process handles 10 databases efficiently
- ~18 MB total memory (vs ~500 MB for litestream with 10 processes)
- Very low CPU overhead

### Sync Latency (snapshot to Tigris)

| Metric | Value |
|--------|-------|
| p50 | 445 ms |
| p95 | 539 ms |
| Mean | 445 ms |
| Max | 539 ms |

*Tested with ~100KB database snapshots to Tigris*

**Key findings:**
- Snapshot latency dominated by S3 upload time
- Sub-500ms p50 is good for Tigris over residential broadband
- Consistent performance with low variance

### Write Throughput

| Metric | Value |
|--------|-------|
| Max commits/sec | 25,874 |
| Avg commit latency | 0.04 ms |

**Key findings:**
- Walrust imposes virtually no overhead on SQLite commit performance
- Can sustain >25,000 commits/sec with walrust watching

### Checkpoint Impact

| Metric | Value |
|--------|-------|
| Normal commit latency | 0.07 ms |
| Post-checkpoint latency | 0.08 ms |
| Checkpoint duration | 7 ms |

**Key findings:**
- Checkpoints have negligible impact on write performance
- Post-checkpoint latency within 15% of normal

### Network Recovery

| Metric | Value |
|--------|-------|
| Simulated outage | 5 seconds |
| Writes during outage | 49 rows |
| Catchup time | ~5 seconds |
| Data loss | 0 rows |

**Key findings:**
- All writes made during outage are preserved in WAL
- Walrust syncs pending WAL immediately on restart
- No data loss during network interruptions

## Micro-Benchmarks (CPU)

From `cargo bench`:

| Operation | Time |
|-----------|------|
| WAL header parse | 47 ns |
| WAL frame header parse | 32 ns |
| SHA256 (1KB) | 6 μs |
| SHA256 (100KB) | 512 μs |
| SHA256 (1MB) | 5.23 ms |

**Key findings:**
- WAL parsing is extremely fast (< 50ns)
- SHA256 is the bottleneck for checksums
- For 100MB database, checksum takes ~500ms

## Comparison vs Litestream

### Memory Usage (Measured)

| DBs | Litestream | Walrust | Savings |
|-----|-----------|---------|---------|
| 1   | 33 MB     | 12 MB   | **21 MB** |
| 5   | 152 MB    | 14 MB   | **138 MB** |
| 10  | 286 MB    | 12 MB   | **274 MB** |
| 20  | 600 MB    | 12 MB   | **588 MB** |

*Measured on macOS with 100KB test databases, 5 second measurement window, valid S3 credentials*

**Key observation:** Walrust memory stays constant (~12 MB) regardless of database count. Litestream scales linearly (~30 MB per process).

### Process Model

- **Litestream**: 1 process per database
- **Walrust**: 1 process for N databases

### When to Use Each

**Use Walrust when:**
- Multiple databases (5+)
- Resource-constrained environments
- Memory is a concern
- Need explicit SHA256 verification

**Use Litestream when:**
- Single database
- Battle-tested production stability is critical
- Team familiar with Go ecosystem

## Test Environment

- **Platform:** macOS (aarch64)
- **Rust:** 1.x (release build)
- **S3 Provider:** Tigris (Fly.io)
- **Network:** Residential broadband

## Running Benchmarks

```bash
# All real-world benchmarks
make bench-realworld

# Specific test
python bench/realworld.py --test restore

# Compare with litestream (requires litestream installed)
make bench-compare

# Micro-benchmarks
make bench
```

## Future Improvements

1. **WAL replay benchmark** - Once implemented, measure replay speed
2. **Larger database tests** - Test with 1GB+ databases
