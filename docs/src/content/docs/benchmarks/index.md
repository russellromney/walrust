---
title: Performance Benchmarks
description: Walrust performance characteristics and comparison with Litestream
---

Measurements and comparisons live in the repository's `bench/` directory.
They reuse the exact same plumbing as the correctness drills (`drills/lib.sh`:
write driver, process management, restore assertions), and every benchmark
run ends with a restore + integrity + row-count validity check — a benchmark
of a broken sync produces no numbers.

## Latest verified numbers

From `bench/results-20260710T065609Z` (`bench/compare-litestream.sh`, macOS,
local MinIO, Litestream 0.5.2, both tools at a 1s sync interval, 20 rows/s,
3-minute window):

| Metric | walrust | litestream |
|--------|---------|------------|
| S3 PUT requests | 182 | 187 |
| S3 LIST requests | 2 | 41 |
| S3 DELETE requests | 0 | 5 |
| Replication lag (median) | 0.57s | 0.68s |
| Replication lag (p95) | 1.33s | 1.10s |
| RSS median | 15.1 MB | 58.4 MB |
| RSS max | 21.8 MB | 70.0 MB |

Numbers vary with workload, tool version, allocator, and sync cadence.
Run the benchmarks against your own workload rather than quoting these.

## Running benchmarks

```bash
# Head-to-head vs litestream: true request counts (server-side MinIO
# trace), replication lag sentinels, RSS sampling.
make bench-compare

# Multi-database RSS scaling: one process, N databases, three load shapes.
make bench-multidb

# Micro-benchmarks (WAL parsing, checksums)
make bench
```

Both comparison scripts are self-contained: they start their own MinIO
container via docker, print the matched knobs and known asymmetries for
both tools in the output header, and write raw samples plus `results.json`
to a gitignored `bench/results-<timestamp>/` directory.

## Learn More

- [Methodology](/benchmarks/methodology/) - how the benchmarks measure honestly
- `bench/README.md` in the repository - knob-matching policy and results schema
