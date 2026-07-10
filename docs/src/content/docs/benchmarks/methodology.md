---
title: Benchmark Methodology
description: How walrust benchmarks are conducted for transparency and reproducibility
---

The benchmark scripts in `bench/` are built for honesty first. The rules
below are enforced by the scripts themselves; the full policy lives in
`bench/README.md`.

## Shared plumbing with the correctness drills

Bench scripts source `drills/lib.sh` — the same write driver, PID-verified
process management, and restore assertions that gate PRs. There is no
separate "benchmark harness" that could drift from what correctness testing
exercises.

## Matched knobs, printed asymmetries

A comparison only runs with the same sync interval explicitly set on both
tools (walrust `--wal-sync-interval` / litestream `replica.sync-interval`),
the same write workload, and the same S3 target. Every run prints the
matched knobs *and* the known asymmetries (e.g. Litestream v0.5's always-on
compaction monitor, which has no off switch) in its output header. If a knob
cannot be matched, it is stated, never silently absorbed.

## True request counts

S3 request counts come from a server-side `mc admin trace --json -v`
sidecar attached to the MinIO container — every PUT/GET/LIST/DELETE that
actually hit the server, attributed per tool by object path (with a
User-Agent fallback for path-ambiguous requests such as bulk deletes).
End-state object listings are **not** used for request counts: they miss
deleted and compacted objects. The harness's own probe traffic is excluded
by its User-Agent.

## Replication lag

Every ~10 seconds a sentinel row with a unique value is committed to each
tool's database. The probe then polls the bucket and records the time until
a new object appears that *contains* the sentinel bytes (raw byte search,
LZ4-frame fallback for compressed snapshot objects). Reported as median and
p95 per tool.

## Memory

RSS is sampled every 5 seconds via `ps` from the shared library sampler.
The multi-database benchmark (`bench/multidb-rss.sh`) runs one process per
tool watching N databases (walrust via `walrust.toml`, litestream via its
yaml) across idle, steady, and bursty load shapes to test constant-vs-linear
scaling in database count.

## Validity check: broken syncs produce no numbers

Every run ends by restoring from the bucket and asserting `PRAGMA
integrity_check` plus an exact row-count match against the driver's ground
truth for every tool measured. A failed check aborts the run with a nonzero
exit before results are written.

## Environment and reproducibility

Scripts start their own MinIO container (docker) and clean up all processes,
containers, and bucket objects on exit:

```bash
cargo build --release
make bench-compare
make bench-multidb
```

CI runs the same scripts via `.github/workflows/bench.yml`
(`workflow_dispatch` only — benchmarks never gate PRs and never run on a
schedule) and uploads `bench/results-*` as artifacts. Each run's
`results.json` schema is documented in `bench/README.md`. Performance claims
in the README must cite a `bench/results-<timestamp>/` file by date.

Known limitations: CI runner performance varies; RSS includes shared
libraries; a local MinIO removes real-network latency, so real S3/Tigris
replication lag will be higher than the local numbers.
