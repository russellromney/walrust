<p align="center">
  <img src="logo.svg" alt="Walrust" width="200">
</p>

# walrust

> **Experimental.** walrust is under active development and contains bugs. Be careful.

**Lightweight SQLite replication to S3 in Rust as a CLI or an embedded library.**

Walrust continuously replicates SQLite databases to any S3-compatible storage
(AWS S3, Tigris, R2, MinIO, etc.). You get durability and read replicas without
running an HA cluster, and keep SQLite's fast local reads and writes.

walrust's specific goals are to be performant and memory efficient.

Part of the [hadb](https://github.com/russellromney/hadb) ecosystem. Shared
infrastructure (S3, retry, webhooks, retention) provided by
[hadb-io](https://github.com/russellromney/hadb/tree/main/hadb-io).

## How it works

**Embedded** — your app uses walrust as a library:

```
┌───────────────────┐
│     Your App      │
│ ┌───────────────┐ │     ┌──────┐
│ │    SQLite     │─┼────>│  S3  │
│ │   + walrust   │ │     └──────┘
│ └───────────────┘ │
└───────────────────┘
```

**Sidecar** — walrust runs as a separate process, with optional read replicas:

```
  Primary machine                                    Replica machine
┌─────────────────────────────────────┐            ┌──────────────────┐
│ ┌──────────┐  ┌──────────┐         │            │ ┌──────────────┐ │
│ │ Your App │─>│  app.db  │         │            │ │walrust       │ │
│ └──────────┘  └────┬─────┘         │  ┌──────┐  │ │  replicate   │ │
│               ┌────┴─────┐         │  │      │  │ └──────┬───────┘ │
│               │ walrust  │────────────>│  S3  │────────>─┤         │
│               │  watch   │         │  │      │  │ ┌──────┴───────┐ │
│               └──────────┘         │  └──────┘  │ │  replica.db  │ │
│                                    │            │ │  (read-only) │ │
│                                    │            │ └──────────────┘ │
└─────────────────────────────────────┘            └──────────────────┘
```

walrust polls the WAL, uploads new frames as
[HADBP](https://github.com/russellromney/hadb/tree/main/hadb-changeset)
changesets, and takes periodic snapshots. See
[Safety and design](#safety-and-design) for how this avoids corrupting data.

## Quick start (CLI)

```bash
cargo install walrust
```

```bash
walrust watch app.db -b s3://my-bucket --endpoint https://fly.storage.tigris.dev
```

More commands:

```bash
walrust restore mydb -o restored.db -b s3://my-bucket                        # restore from S3
walrust restore mydb -o restored.db -b s3://my-bucket --point-in-time 42     # restore through TXID/sequence 42
walrust snapshot app.db -b s3://my-bucket                  # immediate snapshot (errors if a watcher owns the DB)
walrust verify mydb -b s3://my-bucket                      # check backup integrity
walrust list -b s3://my-bucket                             # list backups
walrust prune -b s3://my-bucket                            # GFS retention cleanup
walrust explain                                            # preview resolved config
```

## Use as a library

The `walrust` crate re-exports the engine as `walrust::walrust_core`, plus an
S3 convenience constructor.

```rust
use std::path::Path;
use walrust::walrust_core::{Replicator, sync::ReplicationConfig};

// Reads AWS_* env vars. Any hadb_storage::StorageBackend works;
// build with `default-features = false` to skip the aws-sdk dependency.
let storage = walrust::s3_backend_from_env("my-bucket", Some("https://fly.storage.tigris.dev")).await?;

// Starts the background sync loop. Databases live under "{prefix}{name}/".
let replicator = Replicator::new(storage, "backups/", ReplicationConfig::default());

// Snapshots the database and begins continuous WAL replication.
replicator.add("app", Path::new("app.db")).await?;

// ... your app writes to app.db as normal ...

replicator.flush("app").await?;                              // block until synced to S3
replicator.restore("app", Path::new("restored.db")).await?; // verified restore
```

Notes for embedders:

- Open your database in WAL mode. `walrust pragma` prints the recommended
  settings.
- walrust-core pins `rusqlite` (currently 0.35). Cargo allows one
  `libsqlite3-sys` per build, so your app's rusqlite version must match.
- `Replicator::add()` creates a small `_walrust_seq` table in the database —
  see [Safety and design](#safety-and-design).

Bindings for other languages are planned; Python bindings exist today behind
the `python` feature.

## Safety and design

How replication works: walrust reads committed frames from the SQLite WAL,
checking each frame's checksum and salt and stopping at the first torn or stale
frame — a partial transaction never leaves the machine. Committed pages are
packaged as an HADBP changeset whose checksum chains from the previous state of
the database. Periodic snapshots start fresh bases. A restore takes the newest
snapshot at or before the target and applies changesets in order, verifying the
chain against the actual restored bytes — a missing, corrupt, or out-of-order
changeset breaks the chain and fails the restore instead of producing a wrong
database.

What walrust promises. Every claim here is backed by a test that fails when the
behavior breaks:

- **Restores are verified or they fail loudly.** A restore is staged to a temp
  file, chain-verified, and must pass `PRAGMA integrity_check` before it
  replaces the output path. A gap, a missing object, or a point-in-time target
  beyond the newest backup is a hard error.
- **Recovery point is bounded by the sync interval** (default ~1s). Rows
  committed inside the final un-synced window can be lost in a hard crash;
  everything synced is restorable. A clean `flush()` before shutdown means zero
  loss.
- **Checkpoints can't destroy unshipped data.** walrust pins the WAL with a
  read transaction (via the small `_walrust_seq` table it creates in each
  watched database — the same technique Litestream uses), so an external
  `wal_checkpoint` cannot reset the WAL mid-backup. A checkpoint while walrust
  is stopped is detected on restart and triggers an immediate re-snapshot.
- **Single writer, enforced.** A lock file (`.walrust-<db>.lock`) makes a
  second watcher on the same host fail fast instead of corrupting the backup.
- **Retention never orphans a restore point.** `prune` keeps every object a
  retained point-in-time restore still needs.

## Configuration

`walrust.toml`:

```toml
[s3]
bucket = "s3://my-bucket/backups"
endpoint = "https://fly.storage.tigris.dev"

[[databases]]
path = "/data/app.db"
```

```bash
walrust watch  # auto-discovers walrust.toml
```

Everything else (sync intervals, retention, retry, webhooks) has sensible
defaults. See `walrust explain` for the full resolved config.

A glob (`path = "/data/*.db"`) that matches nothing is a startup error, so a
typo can't silently back up nothing. Set `allow_empty_globs = true` for
genuinely optional patterns; if every glob is empty, `watch` starts and idles
with a warning so a supervisor can boot walrust before its databases exist.

## Compaction (off by default)

Long-history databases accumulate tens of thousands of tiny per-second sync
objects, which makes restore slow and buckets large. Leveled compaction folds
old incrementals into a few coarser merged objects (minutes-grain L1, then
hours-grain L2), so a restore is snapshot + a handful of merged objects + a fine
seconds tail — measurably faster and far fewer objects fetched (see Performance
and cost). It is **off by default**:

```toml
[compaction]
enabled = false          # default; see the version-skew warning below
keep_fine_window = "1h"  # never merge L0 objects younger than this
l1_batch = 60            # L0 objects folded per L1 merge
l2_batch = 24            # L1 objects folded per L2 merge
```

Compaction works in **both** the `walrust` CLI (`walrust watch
--independent-tasks` compacts, `walrust restore` / `verify` read the leveled
bucket across the LTX→HADBP seam) and **library / owned mode** via the
`Replicator`. On the CLI, compaction ticks **only in independent-tasks mode**;
the default shadow watch loop does not compact, so starting it with `[compaction]
enabled = true` fails loudly and points you at `--independent-tasks` (it will not
silently ignore the knob and let the bucket grow).

Two honest caveats, one sentence each:

- **Version skew (empirically confirmed, not theoretical):** a leveled bucket
  is **not restorable by walrust binaries older than this release** — they
  don't know the `levels/` layout exists — so compaction ships dark; only
  enable it once every binary that might restore the bucket understands
  levels. `drills/version-skew.sh` (manual/`make drill-version-skew` only)
  builds a real leveled bucket and runs a real pre-compaction `walrust
  restore` (crates.io `0.5.1`) against it. Observed outcome: **exit 0** — no
  error reported to the operator — producing a **corrupt database**
  (`PRAGMA integrity_check` fails with `btreeInitPage() returns error code
  11` on the pages that existed only inside the merged-and-deleted range).
  That is worse than a short restore: silent corruption with a success exit
  code. This is the confirmed hazard the `enabled = false` default exists to
  prevent.
- **PITR granularity decays with age:** point-in-time restore stays second-exact
  inside `keep_fine_window`, but a target that falls *strictly inside* an older
  merged window fails loudly, naming the nearest restorable points on both sides,
  rather than silently returning the wrong state.

Embedders set the same knobs on `ReplicationConfig::compaction`
(`walrust_core::compaction::CompactionSettings`); `enabled` is the single
control (there is no separate internal gate). Run `walrust explain` to see the
resolved values.

## Read replica

```bash
walrust replicate s3://my-bucket/app --local replica.db --interval 5s
```

This polls S3 for new changesets and applies them to a local database. The
replica is a normal SQLite file — any application can open it read-only.
Combine with `walrust watch` on the primary for a live read replica on another
machine.

`replicate` does not read `levels/` — it only tails the flat incremental pool.
If compaction prunes a tail the replica hasn't applied yet, the replica
re-bootstraps from the newest snapshot (same handler as any other chain gap),
converging automatically at the cost of a full snapshot download; a shorter
`keep_fine_window` gives the replica more slack before that happens.

## Monitoring

`walrust verify` exits nonzero on real chain problems only (holes superseded by
a later snapshot are not alarms), so it can run in cron. Webhooks cover upload
failures, detected external checkpoints, and corruption — configure in
`walrust.toml`.

## Performance and cost

**Memory.** A watcher holds a bounded working set, so RSS stays roughly
constant as database count grows. Measured with matched knobs (1s sync on
both tools, local MinIO; `bench/results-20260710T*`): walrust ~15 MB median
RSS vs Litestream 0.5.2 ~58 MB on a single database; scaling 1→10 databases,
walrust ~15→~20 MB vs Litestream ~53→~143 MB. Part of the gap has an honest
explanation: Litestream's `replicate` does its compaction and snapshot work
in-process, and walrust does not compact incrementals yet — we will
re-measure when it does. Numbers move with workload, tool version, and
knobs: run `bench/compare-litestream.sh` and `bench/multidb-rss.sh` (see
`bench/README.md`) against your own workload.

**S3 requests and objects.** At the same 1s sync interval, PUT volume is
equivalent: 182 vs 187 over a 3-minute server-side traced window. (An
earlier claim here of "~9x more PUTs" counted objects retained, not requests
made, and did not reproduce.) Litestream issues ~10x more LIST calls and
periodic DELETEs from its always-on compaction; walrust keeps more, smaller
objects between snapshots when compaction is off (its default). If
request cost matters more than a tight recovery point, raise
`wal-sync-interval` and lean on the snapshot triggers.

**Restore speed with compaction.** Leveled compaction folds a long incremental
history into a handful of merged objects, so cold restore-to-latest fetches far
fewer objects. Measured on a ~10,000-row history built at 1s sync to local MinIO
(release binary, 3-run median, fresh output path each; `bench/restore-speed.sh`,
`bench/results-20260710T141118Z`): walrust **with** compaction restored in
**0.29 s fetching 5 objects**, versus **1.98 s fetching 242 objects** without —
so compaction makes walrust restore **~7x faster and fetch ~48x fewer objects**
on this history, and the gap widens as the history grows. Honest caveat: against
litestream's own compaction, walrust-compacted fetches fewer objects (5 vs 25)
but does **not** win wall-clock at this scale — litestream restored in 0.09 s vs
walrust's 0.29 s, because litestream's per-object apply path is more optimized
and walrust does more per object (LTX→HADBP decode + chain verify +
`integrity_check`). Re-run `bench/restore-speed.sh` against your own workload.

## vs Litestream

- **Library embedding is the point of walrust** — replication inside your Rust
  process instead of a sidecar.
- **The formats are not compatible.** walrust writes HADBP changesets, not
  Litestream's LTX. Neither tool can restore the other's backups.
- **Requests are equivalent; retention shape differs.** Same PUT volume at
  matched sync intervals; with compaction off (walrust's default) walrust keeps
  more, smaller objects while Litestream spends LISTs and DELETEs compacting.
  Median replication lag measured slightly lower for walrust (0.57s vs 0.68s).
- **Leveled compaction is available (off by default).** It cuts restore-object
  count sharply (~48x fewer on a 10k-row history) and makes walrust's own restore
  ~7x faster; it does not yet beat Litestream's wall-clock restore at small
  scale. See Performance and cost for the measured table.
- **Memory measured lower and flat** (~15 MB vs ~58 MB single-db; flat vs
  ~10 MB per added database) — see Performance and cost for conditions and
  the fairness caveat.
- **Litestream is older and more battle-tested.**

## Acknowledgments

walrust is transparently inspired by and built on the ideas from
[Litestream](https://litestream.io) by
[Ben Johnson](https://github.com/benbjohnson), including its core safety
technique (the pinned WAL read lock). The replication format is
[HADBP](https://github.com/russellromney/hadb/tree/main/hadb-changeset), a
shared changeset format used across the
[hadb](https://github.com/russellromney/hadb) ecosystem.

## Testing

Three instruments run continuously: the unit/integration suite and a fast
`basic_e2e` drill tier (real binary, kill/restart, restore row-diff, prune +
PITR) gate every PR; the full drill suite runs nightly and files an issue on
failure. Run them locally with `make basic-e2e` and `make drill` against any S3
endpoint via `AWS_*` env vars (or Tigris via Soup). `ADVERSARIAL_REVIEW_2.md`
is the findings ledger.

## License

Apache 2.0
