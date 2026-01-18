# Litestream Compatibility

Walrust uses the same LTX (Litestream Transaction) format as litestream, enabling interoperability between the two tools.

## Format Overview

### S3 Storage Layout

Both walrust and litestream use the same S3 structure:

```
s3://bucket/prefix/
└── db_name/
    ├── 0000/                              # Generation 0 (live incrementals)
    │   ├── 0000000000000001-0000000000000001.ltx  # Snapshot (TXID 1)
    │   ├── 0000000000000002-0000000000000010.ltx  # Incremental (TXID 2-10)
    │   └── 0000000000000011-0000000000000050.ltx  # Incremental (TXID 11-50)
    └── 0001/                              # Generation 1+ (compacted snapshots)
        └── 0000000000000001-0000000000000100.ltx  # Compacted snapshot
```

### Filename Format

- **Generation folder**: 4-character lowercase hex (`0000`, `0001`, `0002`, ...)
- **LTX filename**: `{min_txid}-{max_txid}.ltx`
- **TXID format**: 16-character lowercase hex (e.g., `0000000000000001`)

### LTX File Format

Walrust uses the `litetx` Rust crate, which implements the same binary format as litestream:

| Field | Description |
|-------|-------------|
| Header | Magic bytes, version, flags |
| Flags | Compression (LZ4), etc. |
| Page Size | SQLite page size (512-65536) |
| Commit | Number of pages in database |
| Min TXID | First transaction in this file |
| Max TXID | Last transaction in this file |
| Timestamp | When file was created |
| Pre-Apply Checksum | Database checksum before applying (incrementals only) |
| Pages | Compressed page data |
| Trailer | Post-apply checksum, validation |

### Snapshot vs Incremental

| Type | min_txid | pre_apply_checksum | Description |
|------|----------|-------------------|-------------|
| Snapshot | 1 | None | Full database (all pages) |
| Incremental | > 1 | Required | Only changed pages |

## Compatibility Testing

### Prerequisites

```bash
# Install litestream
brew install litestream

# Build walrust
cd walrust
cargo build --release

# Configure S3 credentials
export AWS_ACCESS_KEY_ID=your_key
export AWS_SECRET_ACCESS_KEY=your_secret
export AWS_ENDPOINT_URL_S3=https://fly.storage.tigris.dev
export BUCKET_NAME=your-bucket
```

### Run Tests

```bash
# Run full compatibility test suite
./tests/litestream_compat.sh
```

### Manual Testing

**Walrust backup -> Litestream restore:**

```bash
# Create test database
sqlite3 test.db "CREATE TABLE t(id INTEGER PRIMARY KEY, data TEXT);"
sqlite3 test.db "INSERT INTO t VALUES(1,'hello'),(2,'world');"

# Backup with walrust
walrust snapshot test.db -b s3://bucket/test --endpoint $AWS_ENDPOINT_URL_S3

# Restore with litestream
cat > litestream.yml << EOF
dbs:
  - path: restored.db
    replicas:
      - type: s3
        bucket: your-bucket
        path: test
        endpoint: $AWS_ENDPOINT_URL_S3
        force-path-style: true
EOF
litestream restore -config litestream.yml restored.db

# Verify
sqlite3 restored.db "SELECT * FROM t;"
```

**Litestream backup -> Walrust restore:**

```bash
# Create and replicate with litestream
sqlite3 source.db "CREATE TABLE t(id INTEGER PRIMARY KEY, data TEXT);"

cat > litestream.yml << EOF
dbs:
  - path: source.db
    replicas:
      - type: s3
        bucket: your-bucket
        path: litestream-test
        endpoint: $AWS_ENDPOINT_URL_S3
        force-path-style: true
        sync-interval: 1s
EOF

litestream replicate -config litestream.yml &
sleep 3
sqlite3 source.db "INSERT INTO t VALUES(1,'test');"
sleep 2
kill %1

# Restore with walrust
walrust restore litestream-test -o restored.db -b s3://your-bucket --endpoint $AWS_ENDPOINT_URL_S3

# Verify
sqlite3 restored.db "SELECT * FROM t;"
```

## Known Differences

### Supported

| Feature | Walrust | Litestream | Notes |
|---------|---------|------------|-------|
| LTX format | Yes | Yes | Same binary format |
| LZ4 compression | Yes | Yes | Default compression |
| Generation folders | Yes | Yes | 0000, 0001, etc. |
| 16-char hex TXIDs | Yes | Yes | Same filename format |
| Checksum chaining | Yes | Yes | pre/post apply checksums |
| Point-in-time restore | Yes | Yes | By TXID |

### Not Yet Implemented

| Feature | Walrust | Litestream | Notes |
|---------|---------|------------|-------|
| WAL segment files | No | Yes | Walrust only uses LTX |
| .pos files | No | Yes | Position tracking files |
| Real-time replication | Partial | Yes | Walrust uses polling |
| Retention levels | Planned | Yes | Multi-level compaction |

### Behavior Differences

1. **State Discovery**: Walrust discovers state from S3 file listings, not manifest files
2. **Snapshot Timing**: Walrust takes snapshots to generation 1+, incrementals to generation 0
3. **Compaction**: Walrust's compaction implementation differs (see ROADMAP.md)

## Troubleshooting

### "Invalid LTX file" Error

The LTX file may be corrupted or incompatible. Verify with:

```bash
walrust verify db-name -b s3://bucket
```

### "TXID gap detected" Error

Missing incremental files. Check S3 for continuity:

```bash
aws s3 ls s3://bucket/db-name/0000/ --endpoint-url $AWS_ENDPOINT_URL_S3
```

### Restore Fails with Checksum Error

Pre-apply checksum mismatch indicates the incremental was created for a different database state. This can happen if:

1. Files were manually modified
2. Concurrent backups created inconsistent state
3. Compaction was interrupted

Solution: Restore from the latest snapshot only, or find a consistent set of incrementals.

## References

- [Litestream Documentation](https://litestream.io/how-it-works/)
- [litetx Rust Crate](https://docs.rs/litetx/)
- [Litestream Revamped Blog Post](https://fly.io/blog/litestream-revamped/)
