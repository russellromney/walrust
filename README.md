<p align="center">
  <img src="logo.svg" alt="Walrust" width="200">
</p>

# walrust

> **Experimental, tested hard.** walrust is young and its format may still change.
> Every known correctness finding is fixed with a test proven to fail without the
> fix; every PR is gated by unit tests plus user-shaped end-to-end drills, and a
> full drill suite (crash/restart soaks, hostile checkpoints, retention drills)
> runs nightly. See [Testing](#testing) and `ADVERSARIAL_REVIEW_2.md` for the ledger.

**Lightweight SQLite replication to S3 in Rust — as a CLI or an embedded library.**

Walrust continuously replicates SQLite databases to any S3-compatible storage
(AWS S3, Tigris, R2, MinIO, etc.). You get durability and read replicas without
running an HA cluster, and keep SQLite's fast local reads and writes.

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

walrust polls the WAL, uploads new WAL frames as HADBP changesets to S3, and
takes periodic snapshots. Every changeset carries a checksum chain that is
verified against the actual database bytes on restore. The format is provided by
[hadb-changeset](https://github.com/russellromney/hadb/tree/main/hadb-changeset).

## Quick start (CLI)

Until the next crates.io release, install from git — the published 0.5.2 predates
a large round of correctness fixes:

```bash
cargo install --git https://github.com/russellromney/walrust walrust
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
walrust compact -b s3://my-bucket                          # GFS retention cleanup
walrust explain                                            # preview resolved config
```

## Use as a library

The `walrust` crate re-exports the engine as `walrust::walrust_core`, plus an
S3 convenience constructor. Point-in-time restore is by TXID/sequence.

```rust
use std::path::Path;
use walrust::walrust_core::{Replicator, sync::ReplicationConfig};

// Reads AWS_* env vars. Any hadb_storage::StorageBackend works here;
// build with `default-features = false` to drop the aws-sdk dependency
// and supply your own backend.
let storage = walrust::s3_backend_from_env("my-bucket", Some("https://fly.storage.tigris.dev")).await?;

// Starts the background sync loop. Databases are stored under "{prefix}{name}/".
let replicator = Replicator::new(storage, "backups/", ReplicationConfig::default());

// Snapshots the database and begins continuous WAL replication.
replicator.add("app", Path::new("app.db")).await?;

// ... your app writes to app.db as normal ...

replicator.flush("app").await?;                              // block until synced to S3
replicator.restore("app", Path::new("restored.db")).await?; // verified restore
```

Notes for embedders:

- Open your database in WAL mode. `walrust pragma` prints the recommended
  settings (`journal_mode=WAL`, `wal_autocheckpoint=0`, `synchronous=NORMAL`).
- walrust-core pins `rusqlite` (currently 0.35). Cargo allows only one
  `libsqlite3-sys` in a build, so your app's rusqlite version must match.
- `Replicator::add()` creates a small `_walrust_seq` table in the database —
  see [Guarantees](#guarantees).

## Guarantees

What walrust promises, and what it doesn't. Each claim below is backed by a test
that fails when the behavior it describes is broken.

- **Restores are verified or they fail loudly.** A restore is staged to a temp
  file, its checksum chain is verified against the actual database bytes, it
  must pass `PRAGMA integrity_check`, and only then does it replace the output
  path. A gap in the backup chain, a missing object, or a point-in-time target
  beyond the newest backup is a hard error — never a silently wrong database.
- **Recovery point is bounded by the sync interval** (default ~1s). Committed
  rows inside the final un-synced window can be absent from the backup after a
  hard crash (`kill -9`); everything synced or flushed is restorable. A clean
  `flush()` before shutdown means zero loss.
- **Checkpoints can't destroy unshipped data.** walrust holds a read transaction
  pinned to a live WAL frame (via a small `_walrust_seq` bookkeeping table it
  creates in each watched database — the same technique Litestream uses), so an
  external `wal_checkpoint` cannot reset the WAL under an in-flight backup. A
  checkpoint that happens while walrust is *stopped* is detected on restart and
  triggers an immediate re-snapshot.
- **Single writer, enforced.** walrust is not a multi-writer system by design.
  A lock file (`.walrust-<db>.lock`, same host) makes a second watcher fail fast
  with a clear error instead of corrupting the backup stream.
- **Retention never orphans a restore point.** `compact` keeps every object a
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

A glob (`path = "/data/*.db"`) that matches no databases is a startup error by
default, so a typo does not silently back up nothing. Set
`allow_empty_globs = true` to permit genuinely optional patterns; when every
configured glob is empty, `watch` starts and idles with a warning so a
supervisor can boot walrust before its databases exist.

## Read replica

```bash
walrust replicate s3://my-bucket/app --local replica.db --interval 5s
```

This polls S3 for new changesets and applies them to a local database. The
replica is a normal SQLite file — any application can open it read-only.
Combine with `walrust watch` on the primary for a continuously updated read
replica on another machine.

## Monitoring

`walrust verify` exits nonzero on real chain problems (and only real ones —
holes that a later snapshot supersedes are not alarms), so it can run in cron.
Webhook notifications cover upload failures, detected external checkpoints, and
corruption; configure them in `walrust.toml` (see `walrust explain`).

## Performance and cost

**Memory.** A watcher holds a bounded working set, so RSS stays roughly constant
regardless of database count. In side-by-side drills on macOS syncing to Tigris,
walrust and Litestream both measured ~7–10 MB RSS — statistically
indistinguishable. Measure your own workload; absolute numbers depend on
database size, allocator, and sync cadence.

**S3 requests.** walrust favors freshness: the default ~1s `wal-sync-interval`
issues roughly one PUT per interval per busy database — about 9x more PUTs than
Litestream's coarser batching in the same drills. That is a recovery-point vs
cost tradeoff, not a defect. If request cost matters more than a tight recovery
point, raise `wal-sync-interval` and lean on the snapshot triggers
(`max-changes`, `max-interval`, `on-idle`).

## vs Litestream

walrust is transparently inspired by [Litestream](https://litestream.io) and
uses the same core safety technique (a pinned WAL read lock). Differences that
matter when choosing:

- **Library embedding is the point of walrust.** If you want replication inside
  your Rust process instead of a sidecar, that's the differentiator.
- **The formats are not compatible.** walrust writes HADBP changesets, not
  Litestream's LTX. Neither tool can restore the other's backups.
- **Freshness vs request cost** and **memory** are covered above: walrust ships
  changes faster at higher S3 request volume; memory use is a wash.
- **Litestream is older and more battle-tested.** walrust is young; its testing
  is aggressive (see below), but Litestream has years of production mileage.

## Testing

Three instruments run continuously: the unit/integration suite and a fast
`basic_e2e` drill tier (real binary, kill/restart, restore row-diff, compact +
PITR) gate every PR; the full drill suite (hostile checkpoints, soaks, replica
convergence) runs nightly and files an issue on failure. `make basic-e2e` and
`make drill` run them locally against any S3 endpoint via `AWS_*` env vars (or
Tigris via Soup). `ADVERSARIAL_REVIEW_2.md` is the findings ledger: every fixed
finding names the test that proves it, and the deferred-risk register at the
bottom lists what's consciously not done.

## Acknowledgments

walrust is transparently inspired by and built on the ideas from
[Litestream](https://litestream.io) by
[Ben Johnson](https://github.com/benbjohnson). The replication format has moved
from Litestream's LTX to
[HADBP](https://github.com/russellromney/hadb/tree/main/hadb-changeset), a
shared changeset format used across the [hadb](https://github.com/russellromney/hadb)
ecosystem.

## License

Apache 2.0
