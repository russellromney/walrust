# Walrust benchmarks

`bench/` is the single home for measurements and comparisons. It shares one
library with the correctness drills — `drills/lib.sh` provides the write
driver, walrust process management, restore assertions, and the local
MinIO/trace helpers — so bench numbers are produced by the exact same
plumbing the correctness gates use.

## The contract

- **drills/ = correctness.** Assertions, exit codes, gates PRs.
- **bench/ = measurement.** Numbers, tables, comparisons. Bench NEVER gates
  a PR and never runs on a schedule; it runs on demand
  (`workflow_dispatch` in `.github/workflows/bench.yml`) or locally.
- **Bench still verifies correctness at the end.** Every bench run finishes
  with the drill restore + `PRAGMA integrity_check` + exact row-count check
  against the driver's ground truth, for every tool measured. If that check
  fails, the run exits nonzero and produces no results — a benchmark of a
  broken sync must not produce numbers.
- Micro-benchmarks (`benches/benchmarks.rs`, `cargo bench`) are separate and
  unchanged.

## Knob-matching policy

A comparison is only honest if the tools run with the same knobs. Every
comparison script MUST print, in its output header:

1. the exact matched knobs and the config key used to set them on each tool
   (e.g. walrust `--wal-sync-interval 1` vs litestream
   `replica.sync-interval: 1s`), and
2. the known asymmetries — anything one tool does that the other cannot be
   configured to match (e.g. litestream v0.5's always-on compaction
   monitor). Asymmetries are printed, never silently absorbed.

If a knob genuinely cannot be matched, say so in the header and count its
effects toward the tool that has it.

## Scripts

### `bench/compare-litestream.sh` — head-to-head vs litestream

One database per tool, identical write drivers, same sync interval, same
local MinIO (started by the script via docker). Measures:

- **True S3 request counts** from the server side: an `mc admin trace
  --json -v` sidecar records every request; requests are attributed to a
  tool by object-path prefix (falling back to User-Agent for requests
  without a distinguishing path, e.g. bulk deletes) and the harness's own
  probe traffic (minio-py User-Agent) is excluded. This is *not* an
  end-state object listing — deleted/compacted objects are still counted.
- **Replication lag**: every ~10s a sentinel row is inserted; the probe
  polls the bucket for the first new object that *contains* the sentinel
  bytes (raw search, LZ4-frame fallback) and records commit-to-visible lag.
  Reported as median/p95 per tool.
- **RSS** sampled every 5s via the shared `drills/lib.sh` sampler
  (one process per tool: walrust's single `watch`, litestream's single
  `replicate`); reported min/median/max. When reading an RSS gap, remember
  the known asymmetry: litestream 0.5's `replicate` runs an always-on in-band
  compaction/snapshot monitor with no off switch, so it is doing compaction
  work in the same process being measured, while walrust's `watch` does not
  compact in-band (`walrust compact` is a separate invocation). Part of any
  RSS gap is that background work; re-measure once walrust compacts in-band.

Ends with the validity check for both tools. Knobs are env vars documented
at the top of the script; defaults: 300s duration, 1s sync interval,
20 rows/s.

```bash
WALRUST_BIN=target/release/walrust bench/compare-litestream.sh
```

### `bench/multidb-rss.sh` — RSS scaling in database count

The question: is RSS constant or linear in the number of watched databases?
For each N (default 1, 10, 50) and each load shape, runs **one** walrust
`watch` process with N databases in a generated `walrust.toml`, then **one**
litestream `replicate` process with the same N databases in its yaml, and
samples RSS over time. Load shapes (`BENCH_SHAPES`, any of `idle steady
bursty`): idle = write once before start, no live writers; steady = a modest
per-db driver (default 5 rows/s); bursty = 10s on / 50s off.

Ends with the validity check on a sample (first/middle/last) of each cell's
databases.

```bash
WALRUST_BIN=target/release/walrust BENCH_DB_COUNTS="1 10 50" BENCH_SHAPES="steady" bench/multidb-rss.sh
```

## Results

Each run writes `bench/results-<utc-timestamp>/` (gitignored) containing the
raw inputs (`rss-*.tsv`, `lag-*.tsv`, `trace.jsonl`), `run-meta.json`,
`report.txt` (the plain-text table, also printed to stdout), and
`results.json`.

### results.json schema (compare-litestream, `schema_version: 1`)

```jsonc
{
  "benchmark": "compare-litestream",
  "schema_version": 1,
  "meta": {                      // knobs as run: duration_seconds,
    "sync_interval_seconds": 1,  // sync/driver/sentinel/rss cadences,
    "versions": {...},           // tool versions, bucket/prefixes,
    "rows": {...},               // rows written per tool (ground truth),
    "validity": "..."            // what the end check asserted
  },
  "requests": {
    "walrust":    { "classes": {"PUT": n, "GET": n, "LIST": n,
                                "DELETE": n, "HEAD": n, "OTHER": n},
                    "apis": {"s3.PutObject": n, ...}, "total": n },
    "litestream": { ... },
    "excluded_probe_requests": n,   // harness traffic, not counted
    "unattributed_requests": n      // neither tool; should be ~0
  },
  "rss": { "walrust":    {"samples": n, "min_mb": x, "median_mb": x, "max_mb": x},
           "litestream": { ... } },
  "replication_lag": {
    "walrust":    {"samples": n, "timeouts": n, "median_s": x, "p95_s": x,
                   "max_s": x, "methods": {"raw": n, "lz4": n}},
    "litestream": { ... }
  }
}
```

### results.json schema (multidb-rss, `schema_version: 1`)

```jsonc
{
  "benchmark": "multidb-rss",
  "schema_version": 1,
  "meta": { "db_counts": [1,10,50], "shapes": ["steady"],
            "cell_duration_seconds": n, "sync_interval_seconds": n,
            "versions": {...}, "cells": [...], "validity": "..." },
  "rss": {
    "<tool>-n<N>-<shape>": {"samples": n, "min_mb": x, "median_mb": x,
                            "max_mb": x, "end_mb": x},  // end = median of
    ...                                                 // last 3 samples
  }
}
```

## Citing bench numbers

Any performance claim in the top-level README (or docs) MUST cite a bench
results file by date — e.g. "~1s median replication lag
(bench/results-20260710T…, compare-litestream)". A claim without a results
file behind it is a rumor; re-run the bench instead of restating it. When a
new run contradicts a published claim, update the claim.

## CI

`.github/workflows/bench.yml` runs both scripts against MinIO on ubuntu via
`workflow_dispatch` only (inputs: duration, db counts, shapes) and uploads
`bench/results-*` as an artifact. No cron, no PR gating.
