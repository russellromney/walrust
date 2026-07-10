---
title: Latest Benchmark Results
description: Detailed benchmark data from the latest verified bench run
---

Numbers below come from local runs of the `bench/` scripts (see
[Methodology](/benchmarks/methodology/)); each cites its results directory.
Every number passed the end-of-run validity check (restore +
`integrity_check` + exact row-count match against the write driver's ground
truth) for both tools.

## Head-to-head vs Litestream

`bench/results-20260710T065609Z` — `bench/compare-litestream.sh` on macOS
(Apple Silicon), local MinIO, walrust 0.5.2 vs Litestream 0.5.2, **both at a
1s sync interval**, identical 20 rows/s write drivers, 3-minute window:

| Metric | walrust | litestream |
|--------|---------|------------|
| Rows written (ground truth) | 3385 | 3390 |
| S3 PUT requests | 182 | 187 |
| S3 GET requests | 1 | 0 |
| S3 LIST requests | 2 | 41 |
| S3 DELETE requests | 0 | 5 |
| S3 total requests | 185 | 233 |
| Replication lag median | 0.57s | 0.68s |
| Replication lag p95 | 1.33s | 1.10s |
| RSS min / median / max (MB) | 14.4 / 15.1 / 21.8 | 7.6 / 58.4 / 70.0 |

Request counts are server-side (MinIO trace), not object listings.
Litestream's extra LIST/DELETE traffic comes from its always-on compaction
monitor — an asymmetry the run header prints. At matched sync intervals PUT
volume is equivalent; earlier claims of a large PUT-count gap did not
reproduce against Litestream 0.5.x.

## Multi-database RSS scaling

`bench/results-20260710T070916Z` — `bench/multidb-rss.sh`, steady shape
(5 rows/s per database), one process per tool watching all N databases,
120s per cell, same 1s sync interval on both tools ("end" = median of the
last 3 samples; min/max over the cell):

| N databases | walrust RSS end (min/max) MB | litestream RSS end (min/max) MB |
|-------------|------------------------------|---------------------------------|
| 1 | 14.7 (12.0/21.7) | 52.5 (0.3/60.8) |
| 10 | 19.6 (18.9/27.1) | 142.9 (0.4/151.8) |

walrust stayed roughly flat as database count grew; litestream grew ~10 MB
per additional database under this workload. Re-run with
`BENCH_DB_COUNTS="1 10 50"` for the full curve.

## Test environment

- **Platform**: macOS (Apple Silicon), local MinIO via docker
- **walrust**: 0.5.2 (release build) — **litestream**: 0.5.2
- Local MinIO removes real-network latency; replication lag against real
  S3/Tigris will be higher.

## Running your own

```bash
make bench-compare
make bench-multidb
```

CI runs the same scripts on demand (`bench.yml` workflow dispatch) and
uploads `bench/results-*` artifacts.
