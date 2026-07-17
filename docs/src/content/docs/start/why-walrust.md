---
title: Why Walrust?
description: When walrust makes sense over alternatives
---

:::caution[Alpha Software]
Walrust is alpha. It works, but APIs may change.
:::

Walrust replicates SQLite databases to S3-compatible storage. It does the same thing as [Litestream](https://litestream.io) with a different set of tradeoffs.

## Memory

Walrust uses less memory than Litestream when watching multiple databases:

| Databases | Litestream | Walrust |
|-----------|------------|---------|
| 1 | 36 MB | 19 MB |
| 10 | 55 MB | 19 MB |
| 100 | 160 MB | 20 MB |

*Measured with 100KB databases, syncing to Tigris S3 on macOS.*

Memory stays flat because walrust shares one S3 client (with connection pooling) across all databases and uses WAL polling instead of per-database file watchers.

## When to use walrust

- You're watching many SQLite databases (10+) and memory matters
- You want TOML config instead of YAML
- You want a Python API for scripting backups

## When to use Litestream

- You need SFTP or Azure Blob storage backends
- You want a mature tool with community support
- Walrust's alpha status is a problem for your use case

## Implementation

- Written in Rust (async/await on tokio)
- Uses the [LTX file format](https://github.com/superfly/ltx) (derived from Litestream, but not compatible with Litestream restores)
- One shared S3 client with connection pooling; each database gets its own upload task for concurrency
- ~8 MB binary
