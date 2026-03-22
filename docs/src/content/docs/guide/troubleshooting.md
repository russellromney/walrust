---
title: Troubleshooting
description: Common issues and how to fix them
---

Quick guide to diagnosing and fixing common walrust issues.

## Exit Codes

Walrust uses structured exit codes for scripting and automation:

| Code | Name | Meaning |
|------|------|---------|
| 0 | Success | Operation completed successfully |
| 1 | General | Unknown or uncategorized error |
| 2 | Config | Configuration error (invalid config file, missing CLI args) |
| 3 | Database | Database error (file not found, WAL corruption, SQLite issues) |
| 4 | S3 | S3 error (network, authentication, bucket access) |
| 5 | Integrity | Integrity error (checksum mismatch, LTX verification failed) |
| 6 | Restore | Restore error (no snapshot found, PITR unavailable) |

**Use in scripts:**
```bash
walrust verify mydb -b s3://bucket
case $? in
  0) echo "Verification passed" ;;
  5) echo "Integrity error - backup may be corrupted" ;;
  4) echo "S3 error - check credentials/connectivity" ;;
  *) echo "Other error: $?" ;;
esac
```

## Configuration Errors (Exit Code 2)

### Missing --bucket Argument

**Error:**
```
Error: --bucket is required when no config file is present
```

**Solution:**
Provide the bucket via CLI or config file:

```bash
# CLI option
walrust watch app.db --bucket my-backups

# Or create walrust.toml
cat > walrust.toml <<EOF
[s3]
bucket = "s3://my-backups"
EOF
walrust watch app.db
```

### Invalid TOML Syntax

**Error:**
```
Failed to parse walrust.toml: invalid key at line 5
```

**Solution:**
Check for syntax errors in your config file:

```toml
# Bad - quotes missing
bucket = s3://my-bucket

# Good
bucket = "s3://my-bucket"
```

### Invalid Duration Format

**Error:**
```
Invalid duration '5x'. Use format like '5s', '5m', '5h', '5d'
```

**Solution:**
Use valid duration suffixes:

```toml
[cache]
retention = "24h"  # hours
# retention = "7d"   # days
# retention = "30m"  # minutes
# retention = "60s"  # seconds
```

## Database Errors (Exit Code 3)

### Database File Not Found

**Error:**
```
Database not found: /path/to/app.db
```

**Solution:**
Verify the database path exists:

```bash
ls -la /path/to/app.db
```

If using a config file with wildcards, check the pattern:

```toml
[[databases]]
path = "/data/*.db"  # Make sure this matches actual files
```

### WAL Not Enabled

**Error:**
```
WAL mode not enabled for database
```

**Solution:**
Enable WAL mode on your database:

```sql
PRAGMA journal_mode=WAL;
```

Or from the command line:

```bash
sqlite3 app.db "PRAGMA journal_mode=WAL;"
```

### Invalid Page Size

**Error:**
```
Invalid page size: 512
```

**Solution:**
SQLite databases must use supported page sizes (512, 1024, 2048, 4096, 8192, 16384, 32768, or 65536 bytes). Most databases use 4096 bytes by default. This error usually indicates a corrupted database.

Verify your database:

```bash
sqlite3 app.db "PRAGMA integrity_check;"
```

## S3 Errors (Exit Code 4)

### Access Denied

**Error:**
```
AccessDenied: Access to bucket denied
```

**Solution:**
1. Check your credentials:

```bash
echo $AWS_ACCESS_KEY_ID
echo $AWS_SECRET_ACCESS_KEY
```

2. Verify bucket permissions (AWS IAM policy needs `s3:PutObject`, `s3:GetObject`, `s3:ListBucket`)

3. For Tigris, ensure you're using the correct access key format:

```bash
export AWS_ACCESS_KEY_ID=tid_xxxxx
export AWS_SECRET_ACCESS_KEY=tsec_xxxxx
export AWS_ENDPOINT_URL_S3=https://fly.storage.tigris.dev
```

### No Such Bucket

**Error:**
```
NoSuchBucket: The specified bucket does not exist
```

**Solution:**
1. Create the bucket first:

```bash
# AWS
aws s3 mb s3://my-bucket

# Tigris (via Fly.io)
fly storage create
```

2. Check bucket name spelling in your config

### Connection Timeout

**Error:**
```
Failed to connect to S3: connection timeout
```

**Solution:**
1. Check network connectivity:

```bash
ping fly.storage.tigris.dev
# or
ping s3.amazonaws.com
```

2. Verify endpoint URL is correct:

```bash
# For Tigris
export AWS_ENDPOINT_URL_S3=https://fly.storage.tigris.dev

# For AWS (usually not needed)
unset AWS_ENDPOINT_URL_S3
```

3. Check firewall rules allow outbound HTTPS (port 443)

## Integrity Errors (Exit Code 5)

### Checksum Mismatch

**Error:**
```
Checksum mismatch: expected 0x123abc, got 0x456def
```

**Solution:**
1. Run verification:

```bash
walrust verify mydb --bucket my-backups
```

2. If specific files are corrupted, restore from an earlier snapshot:

```bash
# List snapshots
walrust list --bucket my-backups

# Restore to specific point in time
walrust restore mydb -o restored.db \
  --bucket my-backups \
  --point-in-time "2024-01-15T10:00:00Z"
```

3. Check S3 storage for corruption (rare but possible)

### TXID Continuity Broken

**Error:**
```
TXID continuity broken: gap between file 5 (TXID 100) and file 6 (TXID 110)
```

**Solution:**
This indicates missing LTX files. Possible causes:

1. Manual file deletion from S3
2. Failed uploads that weren't retried
3. Compaction bug (unlikely)

**Recovery:**
Use point-in-time restore to the last valid TXID:

```bash
walrust restore mydb -o restored.db \
  --bucket my-backups \
  --point-in-time "2024-01-15T09:00:00Z"
```

## Restore Errors (Exit Code 6)

### No Snapshot Found

**Error:**
```
No snapshot found for database 'mydb'
```

**Solution:**
1. Check database name:

```bash
walrust list --bucket my-backups
```

2. Verify S3 prefix:

```bash
# If you used a custom prefix in config
[[databases]]
path = "/data/app.db"
prefix = "production"  # Use this name for restore

# Restore
walrust restore production -o app.db --bucket my-backups
```

### PITR Unavailable

**Error:**
```
Point-in-time restore unavailable: no LTX files before 2024-01-15T10:00:00Z
```

**Solution:**
The requested timestamp is before your first snapshot. List available snapshots:

```bash
walrust list --bucket my-backups
```

Restore to the earliest available snapshot instead:

```bash
walrust restore mydb -o restored.db --bucket my-backups
```

## Performance Issues

### High CPU Usage

**Symptoms:**
- walrust consuming 30%+ CPU continuously

**Solutions:**
1. Increase `wal_sync_interval` to batch WAL syncs:

```toml
[sync]
wal_sync_interval = 5  # Sync every 5 seconds
```

2. Check for extremely high write rates (10K+ writes/sec per DB)

### High Memory Usage

**Symptoms:**
- walrust using more than 50 MB for small databases

**Solutions:**
1. Check for WAL file growth:

```bash
ls -lh /data/*.db-wal
```

If WAL files are huge (>100 MB), enable checkpointing:

```toml
[sync]
checkpoint_interval = 60
min_checkpoint_page_count = 1000
wal_truncate_threshold_pages = 121359
```

2. Reduce snapshot interval for memory relief:

```toml
[sync]
snapshot_interval = 1800  # 30 minutes instead of 1 hour
```

### Slow Uploads

**Symptoms:**
- S3 uploads taking several seconds

**Solutions:**
1. Enable local cache for faster encoding:

```toml
[cache]
enabled = true
retention = "24h"
max_size = 5368709120  # 5GB
```

2. Check network bandwidth to S3

3. Consider using a CDN or S3 in a closer region

## Getting Help

If you're stuck:

1. **Enable debug logging:**

```bash
export RUST_LOG=walrust=debug
walrust watch app.db -b my-bucket 2>&1 | tee walrust.log
```

2. **Run verify to check backup integrity:**

```bash
walrust verify mydb --bucket my-backups
```

3. **Check recent issues:** [GitHub Issues](https://github.com/russellromney/walrust/issues)

4. **Ask for help:** Open a new issue with:
   - walrust version (`walrust --version`)
   - Operating system
   - Full error message
   - Relevant config (redact credentials!)
   - Debug logs
