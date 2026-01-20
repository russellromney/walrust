---
title: Quick Start
description: Get started with walrust in under 5 minutes
---

Walrust continuously replicates SQLite databases to S3-compatible storage, capturing WAL changes and uploading them as LTX files.

## Installation

### CLI (Rust)

```bash
cargo install walrust
```

### Python Package

```bash
pip install walrust
```

## Basic Usage

### Watch a Database

```bash
# Set S3 credentials
export AWS_ACCESS_KEY_ID=your-key
export AWS_SECRET_ACCESS_KEY=your-secret
export AWS_ENDPOINT_URL_S3=https://fly.storage.tigris.dev  # for Tigris

# Start watching
walrust watch app.db --bucket my-backups
```

Walrust monitors the WAL file, uploads changes to S3, and takes periodic snapshots.

### Restore a Database

```bash
walrust restore app --output restored.db --bucket my-backups
```

### Take an Immediate Snapshot

```bash
walrust snapshot app.db --bucket my-backups
```

### List Backed-Up Databases

```bash
walrust list --bucket my-backups
```

## Configuration File

For multi-database setups, create `walrust.toml`:

```toml
[s3]
bucket = "s3://my-backups"
endpoint = "https://fly.storage.tigris.dev"

[sync]
snapshot_interval = 3600
compact_after_snapshot = true

[[databases]]
path = "/data/app.db"

[[databases]]
path = "/data/users.db"
```

Then run:

```bash
walrust watch --config walrust.toml
```

## Python Usage

```python
from walrust import Walrust

ws = Walrust("s3://my-bucket", endpoint="https://fly.storage.tigris.dev")

# Snapshot
ws.snapshot("/path/to/app.db")

# List databases
dbs = ws.list()

# Restore
ws.restore("app", "/path/to/restored.db")
```

## Next Steps

- [Why Walrust?](/start/why-walrust/) - Design goals and comparison with Litestream
- [CLI Reference](/guide/cli/) - All commands and options
- [Configuration Reference](/config/configuration-reference/) - Complete `walrust.toml` reference
- [How It Works](/how-it-works/) - WAL monitoring, LTX format, and retention
