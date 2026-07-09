<p align="center">
  <img src="logo.svg" alt="Walrust" width="200">
</p>

# walrust

> **Experimental.** walrust is under active development and contains bugs. Be careful.

**Lightweight SQLite replication to S3 in Rust.**

Walrust continuously replicates SQLite databases to any S3-compatible storage (AWS S3, Tigris, R2, MinIO, etc.), ensuring minimal data loss on server crashes, power failures, or disk corruption.

This design means durability and availability without running a HA cluster, plus fast local reads and writes.

walrust's specific goal is to be embeddable and memory efficient.

Part of the [hadb](https://github.com/russellromney/hadb) ecosystem. Shared infrastructure (S3, retry, webhooks, retention) provided by [hadb-io](https://github.com/russellromney/hadb/tree/main/hadb-io).

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

walrust polls the WAL, uploads new WAL frames as HADBP changesets to S3, and takes periodic snapshots. Every changeset carries a SHA-256 checksum chain, verified automatically on restore. The format is provided by [hadb-changeset](https://github.com/russellromney/hadb-changeset).

## Quick start

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
walrust snapshot app.db -b s3://my-bucket                  # immediate snapshot
walrust verify mydb -b s3://my-bucket                      # check backup integrity
walrust list -b s3://my-bucket                             # list backups
walrust compact -b s3://my-bucket                          # GFS retention cleanup
walrust explain                                            # preview resolved config
```

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

Everything else (sync intervals, retention, retry, webhooks) has sensible defaults. See `walrust explain` for the full resolved config.

A glob (`path = "/data/*.db"`) that matches no databases is a startup error by default, so a typo does not silently back up nothing. Set `allow_empty_globs = true` at the top level to permit genuinely optional patterns; when it is set and *every* configured glob is empty, `watch` starts and idles (logging a warning) instead of exiting, so a supervisor can boot walrust before the databases it will back up exist.

**Reserved table.** In walrust-owned mode (the library `Replicator`), walrust creates a small `_walrust_seq` table in each watched database and holds a read transaction that pins a live WAL frame. This is how walrust stops an external process from checkpointing the WAL out from under an in-flight backup — the same technique Litestream uses with its `_litestream_seq` table. The table holds a single counter row and is safe to ignore.

## Read replica

```bash
walrust replicate s3://my-bucket/app --local replica.db --interval 5s
```

This polls S3 for new changesets and applies them to a local database. The replica is a normal SQLite file — any application can open it read-only. Combine with `walrust watch` on the primary to get a continuously updated read replica on another machine.

## Memory usage

walrust aims to be embeddable and memory-efficient: a single watcher holds a
bounded working set (shadow WAL frames + the changeset being encoded), so RSS is
roughly constant regardless of database count rather than growing with it.

In recent side-by-side drills on macOS syncing small databases to Tigris S3,
walrust and Litestream both measured **~7–10 MB RSS** and were statistically
indistinguishable. Earlier versions of this README published a table claiming a
large advantage (e.g. 23–31 MB vs 36 MB); that result did not reproduce and has
been removed. Absolute numbers depend heavily on database size, allocator, and
sync cadence — measure your own workload rather than relying on a headline
figure.

## S3 request volume

walrust favors freshness over batching: the default `wal-sync-interval` is ~1s,
so under sustained writes it issues roughly one PUT per interval per database.
In the drills this produced on the order of **~9x more PUTs** than Litestream's
coarser batching over the same window — a recovery-point-vs-cost tradeoff, not a
defect. If S3 request cost matters more than a tight recovery point, raise
`wal-sync-interval` (fewer, larger uploads) and/or lean on the snapshot
triggers (`max-changes`, `max-interval`, `on-idle`) to control cadence.

## Acknowledgments

walrust is transparently inspired by and built on the ideas from [Litestream](https://litestream.io) by [Ben Johnson](https://github.com/benbjohnson). The replication format has moved from Litestream's LTX to [HADBP](https://github.com/russellromney/hadb-changeset), a shared changeset format used across the [hadb](https://github.com/russellromney/hadb) ecosystem.

## License

Apache 2.0
