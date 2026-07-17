# walrust

SQLite WAL sync to S3-compatible storage. Continuous replication, point-in-time restore.

## What it does

walrust reads SQLite WAL frames, encodes native HADBP snapshots and deltas, and uploads those immutable bytes to S3-compatible storage. Followers restore only a contiguous, checksum-verified published chain.

## Usage

```rust
use hadb_storage::StorageBackend;
use hadb_storage_s3::S3Storage;
use std::sync::Arc;
use walrust::sync;

// Continuous replication
let storage: Arc<dyn StorageBackend> =
    Arc::new(S3Storage::from_env("my-bucket", None).await?);
let config = sync::ReplicationConfig::default();
sync::run_replication(&*storage, "prefix/", &db_path, config, cancel_rx).await?;

// Restore
sync::restore(&*storage, "prefix/", "my-db", &output_path, None).await?;
```

## Features

- WAL frame extraction and deduplication
- Native HADBP changeset encoding with checksum chaining
- Fsynced local spool and versioned remote publication records
- Concurrent S3 downloads for fast follower catch-up
- Shadow WAL for lossless, checkpoint-safe local admission
- Retry with exponential backoff and circuit breaker
- Works with AWS S3, Tigris, MinIO, R2, and any S3-compatible service

## License

Apache-2.0
