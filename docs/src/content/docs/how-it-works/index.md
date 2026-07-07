---
title: How It Works
description: SQLite pages, WAL mode, LTX files, and retention
---

A quick tour of what happens when you run `walrust watch`.

## SQLite Pages

SQLite stores everything in fixed-size pages (usually 4KB). Your database is just a file full of these pages - some hold table data, some hold indexes, some hold metadata. When you write data, SQLite modifies pages.

## WAL Mode

In WAL (Write-Ahead Logging) mode, SQLite doesn't modify the main database file directly. Instead, it appends changes to a separate `-wal` file. This enables replication: walrust watches the WAL file and captures changes before they get folded back into the main database.

```
app.db      ← main database (pages)
app.db-wal  ← changes queue (walrust watches this)
app.db-shm  ← shared memory (ignore this)
```

## The Sync Flow

```
┌─────────────┐    ┌─────────────┐    ┌─────────────┐    ┌─────────────┐
│  Your App   │───▶│   WAL File  │───▶│ Shadow WAL  │───▶│   Walrust   │───▶ S3
│  (writes)   │    │  (changes)  │    │   (copy)    │    │  (uploads)  │
└─────────────┘    └─────────────┘    └─────────────┘    └─────────────┘
```

1. Your app writes to SQLite
2. SQLite appends to the WAL file
3. Walrust detects the change (via polling at `wal_sync_interval`)
4. Walrust copies frames to a shadow WAL file (decouples from SQLite)
5. Walrust packages changed pages into an LTX file
6. LTX file uploads to S3

The shadow WAL ensures that S3 upload latency doesn't block SQLite checkpoints or writes.

## LTX Format

LTX (Lite Transaction) is a compact format for storing SQLite page changes. Each LTX file contains:

- **Header** - page size, page count, transaction IDs
- **Page data** - the actual 4KB pages that changed
- **Checksum** - SHA256-based integrity check

Files are named by transaction ID range:

```
s3://bucket/mydb/
├── 00000001-00000001.ltx   ← snapshot (TXID 1)
├── 00000002-00000010.ltx   ← incremental (TXID 2-10)
├── 00000011-00000050.ltx   ← incremental (TXID 11-50)
└── manifest.json           ← index of all LTX files
```

The format originates from [Litestream](https://litestream.io) via the [litepages crate](https://github.com/superfly/ltx). Walrust's LTX files are not compatible with Litestream (walrust enables checksums that Litestream doesn't expect).

## Checksums

Every LTX file includes a checksum computed from SHA256, truncated to 64 bits. On restore, walrust verifies checksums. If verification fails, the restore aborts with an integrity error (exit code 5).

## GFS Retention

Left unchecked, you'd accumulate LTX files forever. Walrust uses Grandfather-Father-Son (GFS) rotation to keep storage bounded:

| Tier | Default | Keeps |
|------|---------|-------|
| Hourly | 24 | Last 24 hours of snapshots |
| Daily | 7 | One per day for a week |
| Weekly | 12 | One per week for 12 weeks |
| Monthly | 12 | One per month beyond that |

Run `walrust compact` to clean up old files, or use `--compact-after-snapshot` in watch mode.

## Restore

Restoring is the reverse:

1. Download the latest snapshot LTX
2. Apply incremental LTX files in order
3. Verify checksums along the way
4. Output a complete, consistent database

Point-in-time restore works by stopping at a specific transaction ID/sequence number. Timestamp-based PITR is not currently implemented.

---

**Summary:** Walrust monitors SQLite WAL files, encodes changes as LTX files, uploads them to S3, and manages storage with GFS retention policies.
