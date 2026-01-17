# Critical Bug Fix: Initial Sync Not Working

## Summary

**FIXED**: Initial sync was not triggering when WAL file exists but is empty (0 bytes). Data was not being backed up to S3 on first run.

## Root Cause

When SQLite creates a database in WAL mode, it creates the `-wal` file immediately but keeps data in shared memory (`.shm` file) until:
- A checkpoint is triggered
- The connection is closed
- Data is explicitly flushed

The bug occurred in `sync_wal_concurrent()` at [src/sync.rs:2379-2392](src/sync.rs#L2379-L2392):

```rust
// OLD CODE - BUGGY
let header = match wal::read_header(&input.wal_path).await? {
    Some(h) => h,
    None => {
        // No WAL file - return no-op output
        return Ok(SyncOutput {
            frame_count: 0,  // ❌ Returns early without creating snapshot!
            ...
        });
    }
};
```

**Problem Flow:**
1. WAL file exists but is 0 bytes
2. `read_header()` returns `None` (file < 32 bytes)
3. Function returns early with `frame_count: 0`
4. **Initial snapshot is never created from database file!**

## The Fix

When `current_txid == 0` (initial sync), walrust should **ALWAYS** create a snapshot from the database file itself, regardless of WAL file state.

### Changes Made

**File: [src/sync.rs](src/sync.rs)**

Added early check at the beginning of `sync_wal_concurrent()` (line 2377):

```rust
// NEW CODE - FIXED
if input.current_txid == 0 {
    tracing::debug!("{}: Initial sync - creating snapshot from database file", input.name);

    // Get page size from WAL header if available, otherwise use default
    let page_size = match wal::read_header(&input.wal_path).await? {
        Some(h) => h.page_size,
        None => 4096, // SQLite default page size
    };

    // Create snapshot from database file (not from WAL!)
    let (ltx_buffer, db_checksum_new) = tokio::task::spawn_blocking(move || {
        let mut ltx_buffer = Vec::with_capacity(estimated_size);
        ltx::encode_snapshot(&mut ltx_buffer, &db_path_for_encode, page_size, 1)
            .map_err(|e| anyhow::anyhow!("{}: Initial snapshot encode failed: {}", db_name_for_error, e))?;
        let db_checksum = ltx::compute_checksum_from_file(&db_path_for_encode)?;
        Ok::<_, anyhow::Error>((ltx_buffer, db_checksum))
    }).await??;

    // Upload snapshot and update manifest
    // ... (rest of snapshot creation logic)

    return Ok(SyncOutput {
        frame_count: 1, // ✅ Snapshot created!
        new_current_txid: 1,
        ...
    });
}
```

**File: [src/main.rs](src/main.rs)**

Fixed test compilation error (line 864):
```rust
shadow_wal: false,
independent_tasks: false,  // Added missing field
```

**File: [src/wal.rs](src/wal.rs)**

Added debug logging to help diagnose similar issues in the future (line 151):
```rust
tracing::debug!("read_frames_as_pages: path={:?}, file_size={}, page_size={}, ...",
    path, file_size, page_size, ...);
```

## Verification

### Before Fix
```bash
$ ./simple_integrity_test.sh
# Output:
✗ test: WAL exists = true
✗ test: Initial sync returned 0 frames  # BUG!
✗ ERROR: No snapshots found for database: test
```

### After Fix
```bash
$ ./simple_integrity_test.sh
# Output:
✅ test: Created initial snapshot LTX 00000001-00000001.ltx (389 bytes, TXID 1-1)
✅ test: Initial sync captured 1 frames
✅ Restored test from LTX (page_size: 4096, pages: 2, TXID: 1-1)
✅ SUCCESS: Data integrity verified!
```

### Tests
All unit tests pass:
```bash
$ make test
test result: ok. 142 passed; 0 failed; 20 ignored
```

## Impact

This was a **critical data loss bug**. Without this fix:
- First-time database backups would silently fail
- No snapshots would be created to S3
- Restore operations would fail with "No snapshots found"
- Users would lose all their data

## Files Modified

1. `src/sync.rs` - Main fix (initial sync logic)
2. `src/main.rs` - Test compilation fix
3. `src/wal.rs` - Additional debug logging

## Next Steps

Based on the [SESSION_HANDOFF_CRITICAL_BUGS.md](SESSION_HANDOFF_CRITICAL_BUGS.md) document, now that initial sync is working:

- [x] Fix initial sync bug (THIS FIX)
- [ ] Verify data integrity with comprehensive tests
- [ ] Investigate memory usage with sudo metrics
- [ ] Run actual litestream comparison
- [ ] Document real performance claims

## Related Issues

This fix addresses the primary concern in the session handoff:

> **CRITICAL BUG DISCOVERED**: Initial sync is NOT triggering in independent tasks mode. WAL files exist but no snapshots are being created. **Data is not being backed up to S3!**
