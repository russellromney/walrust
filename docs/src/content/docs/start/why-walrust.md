---
title: Why Walrust?
description: Walrust uses less memory than Litestream
---

Walrust is optimized to use less memory than Litestream for SQLite replication to S3.

## Memory Efficiency

Walrust uses significantly less memory than Litestream, especially when watching multiple databases:

### Memory Usage

| Databases | Litestream | Walrust | Reduction |
|-----------|------------|---------|-----------|
| 1 | 37 MB | 19 MB | 49% |
| 10 | 61 MB | 20 MB | 67% |
| 100 | 228 MB | 19 MB | 92% |

*Measured with 100KB databases on macOS, syncing to Tigris S3.*

Walrust's memory usage remains relatively constant (~19-20 MB) as database count increases, while Litestream's memory grows with each database.

## Usage

One walrust process watches all databases:

```bash
walrust watch \
  /var/lib/data/tenant-*.db \
  -b s3://backups
```

Or with a config file:

```toml
[s3]
bucket = "backups"
endpoint = "https://fly.storage.tigris.dev"

[[databases]]
path = "/var/lib/data/*.db"
prefix = "tenants"
```

## Litestream

[Litestream](https://litestream.io) is the original SQLite replication tool and the inspiration for walrust. Walrust uses the same [LTX file format](https://github.com/superfly/ltx), which provides compatibility between the tools.

**When to use Litestream:**
- Mature ecosystem and community support
- Litestream Cloud integration
- SFTP/Azure Blob storage backends

**When to use walrust:**
- Multi-database deployments (100+ databases)
- Memory-constrained environments
- Rust-native integration