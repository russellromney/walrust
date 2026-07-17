---
title: Logging
description: Configure walrust logging and log levels
---

Walrust uses the standard Rust `RUST_LOG` environment variable for log configuration.

## Log Levels

| Level | Description |
|-------|-------------|
| `error` | Only errors |
| `warn` | Warnings and errors |
| `info` | Normal operation (default) |
| `debug` | Detailed debugging |
| `trace` | Very verbose, includes all internal details |

## Basic Configuration

```bash
# Default: info level
export RUST_LOG=walrust=info

# Debug logging
export RUST_LOG=walrust=debug

# Trace logging (very verbose)
export RUST_LOG=walrust=trace

# Errors only
export RUST_LOG=walrust=error
```

## Advanced Filtering

Include AWS SDK logs for debugging S3 issues:

```bash
# Debug walrust + AWS config
export RUST_LOG=walrust=debug,aws_config=debug

# Debug all AWS SDK components
export RUST_LOG=walrust=debug,aws_sdk_s3=debug,aws_config=debug

# Trace everything (very verbose)
export RUST_LOG=trace
```

## Log Output Examples

### Info Level (Default)

```
2024-01-15T10:30:00Z INFO walrust: Watching 3 database(s)
2024-01-15T10:30:05Z INFO walrust: app.db: WAL sync (4 frames, 16KB)
2024-01-15T11:30:00Z INFO walrust: app.db: Scheduled snapshot complete
```

### Debug Level

```
2024-01-15T10:30:00Z INFO walrust: Watching 3 database(s)
2024-01-15T10:30:00Z DEBUG walrust: Connecting to S3 endpoint: https://fly.storage.tigris.dev
2024-01-15T10:30:01Z DEBUG walrust: S3 connection established
2024-01-15T10:30:05Z DEBUG walrust: app.db: Detected WAL change, reading frames
2024-01-15T10:30:05Z DEBUG walrust: app.db: Read 4 frames (16384 bytes)
2024-01-15T10:30:05Z DEBUG walrust: app.db: Computing SHA256 checksum
2024-01-15T10:30:05Z INFO walrust: app.db: WAL sync (4 frames, 16KB)
```

## Production Logging

### systemd

```ini
[Service]
Environment=RUST_LOG=walrust=info
```

View logs:
```bash
sudo journalctl -u walrust -f
```

### Docker

```yaml
environment:
  RUST_LOG: walrust=info
```

View logs:
```bash
docker logs -f walrust
```

## Troubleshooting with Logs

**S3 connection issues:**
```bash
RUST_LOG=walrust=debug,aws_config=debug walrust list --bucket my-bucket
```

**Permission errors:**
```bash
RUST_LOG=walrust=debug,aws_sdk_s3=debug walrust watch mydb.db --bucket my-bucket
```

**WAL sync issues:**
```bash
RUST_LOG=walrust=trace walrust watch mydb.db --bucket my-bucket
```
