# walrust

SQLite WAL sync to S3-compatible storage. Continuous replication, point-in-time restore.

## What it does

walrust reads SQLite WAL frames, encodes them as LTX (Litestream Transaction) files, and uploads them to S3. Followers pull and apply incrementals to stay in sync. Full snapshots for restore.

## Usage

```rust
use walrust::{S3Backend, StorageBackend, sync};

// Continuous replication
let storage = S3Backend::new(s3_client, "my-bucket", Some("prefix/"));
let config = sync::ReplicationConfig::default();
sync::run_replication(&storage, "prefix/", &db_path, config, cancel_rx).await?;

// Restore
sync::restore(&storage, "prefix/", "my-db", &output_path, None).await?;
```

## Features

- WAL frame extraction and deduplication
- LTX encoding with checksum chaining (litestream-compatible)
- Concurrent S3 downloads for fast follower catch-up
- Shadow WAL for decoupled uploads
- Retry with exponential backoff and circuit breaker
- Works with AWS S3, Tigris, MinIO, R2, and any S3-compatible service

## License

Apache-2.0
