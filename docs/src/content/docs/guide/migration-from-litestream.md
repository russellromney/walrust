---
title: Migration from Litestream
description: Start a distinct native HADBP stream safely
---

Walrust and Litestream both replicate SQLite WAL changes, but their storage
formats and recovery protocols are not interchangeable. Walrust does not import
or extend an LTX stream. Preserve the old bucket/prefix for rollback and start a
new native HADBP stream in a distinct destination.

## 1. Verify the source database

```bash
sqlite3 /data/app.db 'PRAGMA integrity_check;'
```

The result must be `ok`.

## 2. Stop Litestream cleanly

Stop its service and confirm no Litestream process still owns the database or
writes the old prefix. Do not point walrust at a prefix that contains an LTX
history and expect it to continue that chain.

## 3. Configure a new prefix

```toml
[s3]
bucket = "s3://my-backups/walrust-native"

[sync]
checkpoint_release = "local"
on_startup = true

[spool]
path = "/var/lib/walrust/spool"
warning_size = 4294967296
max_size = 5368709120
min_free_space = 1073741824

[[databases]]
path = "/data/app.db"
prefix = "app"
```

Use the normal AWS credential provider environment. Add `s3.endpoint` for
Tigris, MinIO, R2, or another compatible service.

## 4. Start watch and wait for publication

```bash
walrust watch --config /etc/walrust.toml
```

The initial recovery base is first admitted durably to the local native spool,
then uploaded and published. Monitor `remote_lag` until it reaches zero before
treating the remote as current.

## 5. Prove restore before retiring the old service

```bash
walrust verify app --bucket s3://my-backups/walrust-native
walrust restore app --bucket s3://my-backups/walrust-native \
  --output /tmp/app-restored.db
sqlite3 /tmp/app-restored.db 'PRAGMA integrity_check;'
```

Compare application row counts or domain checks as well as SQLite integrity.

## Rollback

Stop walrust before restarting Litestream. Litestream cannot apply writes that
exist only in walrust's native stream, so rollback requires an explicit choice
of source-of-truth database and may discard changes made after the migration
boundary. Keeping the old objects is useful history, but it does not merge the
two lineages.
