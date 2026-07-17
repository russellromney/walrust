---
title: How It Works
description: SQLite WAL capture, the local HADBP spool, publication, and restore
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
│  Your App   │───▶│   WAL File  │───▶│ Shadow WAL  │───▶│ HADBP Spool │───▶ S3
│  (writes)   │    │  (changes)  │    │   (fsync)   │    │ (journaled) │
└─────────────┘    └─────────────┘    └─────────────┘    └─────────────┘
```

1. Your app writes to SQLite
2. SQLite appends to the WAL file
3. Walrust detects the change (via polling at `wal_sync_interval`)
4. Walrust copies frames to a shadow WAL file (decouples from SQLite)
5. Walrust fsyncs a native HADBP snapshot or delta and its local journal
6. SQLite may checkpoint; the exact immutable HADBP bytes upload asynchronously

The durable local spool keeps S3 latency out of SQLite checkpointing by default.

## Native HADBP format and publication

Walrust encodes snapshots and deltas as native HADBP changesets. Every object
binds its sequence, predecessor checksum, ending checksum, declared database
size, source cursor, destination, payload length, and SHA-256 in the durable
local journal. The remote namespace is versioned and does not use Litestream
LTX keys:

```
s3://bucket/mydb/native/v1/
├── stream.json
└── lineages/<lineage>/
    ├── 0001/<sequence>.hadbp       # immutable snapshots
    ├── 0000/<sequence>.hadbp       # immutable deltas
    └── published/<sequence>.json   # contiguous visibility records
```

An uploaded object is not visible to restore until its publish record is part
of the verified contiguous chain and a snapshot base exists.

## Checksums

Walrust verifies the HADBP header/content checksum, the page-chain checksum,
the journal SHA-256, and the remote publication chain. Restore aborts on any
mismatch; it never skips a corrupt or divergent object.

## GFS Retention

Walrust uses Grandfather-Father-Son (GFS) snapshot retention:

| Tier | Default | Keeps |
|------|---------|-------|
| Hourly | 24 | Last 24 hours of snapshots |
| Daily | 7 | One per day for a week |
| Weekly | 12 | One per week for 12 weeks |
| Monthly | 12 | One per month beyond that |

Run `walrust prune` for a dry run and add `--force` to publish a new native
retention floor and remove only objects no longer needed by that floor. Watch
can invoke the same policy with `--prune-after-snapshot` or `--prune-interval`.

## Restore

Restoring is the reverse:

1. Discover the versioned stream descriptor and contiguous publish records
2. Download the selected snapshot HADBP object
3. Apply subsequent native deltas in sequence while verifying every link
4. Output a complete, consistent database

Point-in-time restore works by stopping at a specific transaction ID/sequence number. Timestamp-based PITR is not currently implemented.

---

**Summary:** Walrust fsyncs WAL-derived native HADBP objects locally before it
allows a controlled SQLite checkpoint, then publishes those exact bytes to S3
asynchronously and exposes only a contiguous verified recovery chain.
