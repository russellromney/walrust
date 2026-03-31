<p align="center">
  <img src="logo.svg" alt="Walrust" width="200">
</p>

# walrust

> **Pre-1.0.** APIs may change between minor versions. Published to [crates.io](https://crates.io/crates/walrust).

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

walrust polls the WAL, uploads new WAL frames as LTX files to S3, and takes periodic snapshots. Every LTX file carries an SHA256 checksum in S3 metadata, verified automatically on restore.

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
walrust restore mydb -o restored.db -b s3://my-bucket --point-in-time 2026-03-01T12:00:00Z  # PITR
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

## Read replica

```bash
walrust replicate s3://my-bucket/app --local replica.db --interval 5s
```

This polls S3 for new LTX files and applies them to a local database. The replica is a normal SQLite file — any application can open it read-only. Combine with `walrust watch` on the primary to get a continuously updated read replica on another machine.

## Memory usage

| Databases | Litestream | Walrust | Reduction |
|-----------|------------|---------|-----------|
| 1 | 36 MB | 23 MB | 36% |
| 10 | 55 MB | 30 MB | 45% |
| 100 | 160 MB | 31 MB | 81% |

*Measured with 100KB databases on macOS, syncing to Tigris S3. Walrust memory is roughly constant regardless of database count.*

## Acknowledgments

walrust is transparently inspired by and built on the ideas from [Litestream](https://litestream.io) by [Ben Johnson](https://github.com/benbjohnson), using a variation of the same [LTX file format](https://github.com/superfly/ltx). The [hadb](https://github.com/russellromney/hadb) project aims to generalize this pattern to any embedded database.

## License

Apache 2.0
