# Walrust Configuration Gaps Analysis

**Date**: 2026-01-14
**Issue**: Missing critical configuration options compared to Litestream

## Executive Summary

We discovered `wal_sync_interval` was completely missing - an EXCEEDINGLY CORE FEATURE. This analysis identifies ALL missing configuration options by comparing walrust to Litestream's mature configuration surface.

---

## Litestream Configuration Reference

Based on [Litestream Configuration Docs](https://litestream.io/reference/config/), here are ALL interval/threshold options:

| Category | Option | Default | Purpose |
|----------|--------|---------|---------|
| **WAL Syncing** | `sync-interval` | `1s` | Frequency frames pushed to replica |
| **Monitoring** | `monitor-interval` | `1s` | How often to check for changes |
| **Checkpointing** | `checkpoint-interval` | `1m` | PASSIVE checkpoint frequency |
| **Checkpointing** | `min-checkpoint-page-count` | `1000` (~4MB) | Min pages before PASSIVE checkpoint |
| **Checkpointing** | `truncate-page-n` | `121359` (~500MB) | Emergency TRUNCATE checkpoint threshold |
| **Snapshots** | `snapshot.interval` | varies | Full snapshot frequency |
| **Snapshots** | `snapshot.retention` | varies | How long to keep snapshots |
| **Validation** | `validation-interval` | N/A | Periodic replica validation (non-functional v0.5.x) |
| **L0 Compaction** | `l0-retention` | `5m` | Min time to retain L0 files |
| **L0 Compaction** | `l0-retention-check-interval` | `15s` | Check frequency for expired L0 |

---

## Walrust Current Configuration

```rust
pub struct SyncConfig {
    pub snapshot_interval: u64,           // ✅ HAVE (3600s)
    pub wal_sync_interval: u64,           // ✅ ADDED TODAY (1s)
    pub max_changes: u64,                 // ✅ HAVE (0 = disabled)
    pub max_interval: u64,                // ✅ HAVE (0 = disabled)
    pub on_idle: u64,                     // ✅ HAVE (0 = disabled)
    pub on_startup: bool,                 // ✅ HAVE (true)
    pub compact_after_snapshot: bool,     // ✅ HAVE (false)
    pub compact_interval: u64,            // ✅ HAVE (0 = disabled)
}
```

---

## CRITICAL GAPS (Missing Core Features)

### 1. ✅ **Monitor Interval** - RESOLVED (Removed)

**Original**: File watcher check rate
**Resolution**: File watcher removed entirely. Walrust now uses pure polling.

**New Approach** (as of 2026-01-20):
- `wal_sync_interval` serves as the polling interval
- No file watcher (FSEvents/inotify were unreliable for mmap writes anyway)
- Check WAL size every `wal_sync_interval` seconds and sync if changed
- Simple, predictable, works on all platforms

**Config**: Use `wal_sync_interval` to control poll frequency (default: 1 second)

---

### 2. ✅ **Checkpoint Interval** (WAL Auto-Checkpointing)

**What**: How often to trigger PASSIVE checkpoint on SQLite WAL
**Litestream**: `checkpoint-interval: 1m` (default, non-blocking)
**Walrust**: **MISSING** - relies entirely on SQLite's auto-checkpointing

**Impact**:
- WAL can grow unbounded if SQLite doesn't auto-checkpoint
- No control over when WAL gets reset
- Large WAL files = slower crash recovery

**Should Add**:
```rust
pub checkpoint_interval: u64,         // Default: 60 (seconds)
pub min_checkpoint_page_count: u64,   // Default: 1000 pages (~4MB)
```

**Implementation**: Use `PRAGMA wal_checkpoint(PASSIVE)` on interval

---

### 3. ❌ **Truncate Page Threshold** (Emergency WAL Size Limit)

**What**: Emergency threshold triggering TRUNCATE checkpoint (blocks writers!)
**Litestream**: `truncate-page-n: 121359` (~500MB, can disable with 0)
**Walrust**: **MISSING** - no emergency brake for runaway WAL

**Impact**:
- WAL can grow to gigabytes if checkpointing fails
- Disk space exhaustion possible
- No emergency recovery mechanism

**Should Add**:
```rust
pub wal_truncate_threshold_pages: u64,  // Default: 121359 (0 = disabled)
```

**Implementation**: Check WAL size, run `PRAGMA wal_checkpoint(TRUNCATE)` if exceeded

**Warning**: This BLOCKS all readers and writers - use sparingly!

---

### 4. ❌ **Validation Interval** (Replica Integrity Checks)

**What**: Periodically restore replica and compare checksums
**Litestream**: `validation-interval` (non-functional in v0.5.x, future feature)
**Walrust**: **MISSING**

**Impact**:
- Silent data corruption in S3 won't be detected
- Cannot verify backups are actually restorable

**Should Add** (future):
```rust
pub validation_interval: u64,  // Default: 0 (disabled), future: 86400 (24h)
```

**Implementation**: Daily cron job that:
1. Downloads latest snapshot + incrementals
2. Restores to temp location
3. Computes checksum and compares to source DB
4. Alerts if mismatch

**Cost Warning**: Each validation = full restore download from S3

---

### 5. ⚠️ **L0 Retention** (LTX Compaction Cleanup)

**What**: How long to keep L0 files after compacting to L1
**Litestream**: `l0-retention: 5m`, `l0-retention-check-interval: 15s`
**Walrust**: **PARTIAL** - has GFS retention but no L0/L1 tiering

**Status**: Not applicable unless we implement tiered compaction (like Litestream v0.5+)

**Should Consider**: If implementing multi-level compaction in future

---

## SEMANTIC GAPS (Different Approach)

### Snapshot Triggers vs Intervals

**Litestream**:
- Single `snapshot.interval` setting
- Simple: snapshot every N hours

**Walrust**:
- Multiple trigger options:
  - `snapshot_interval` - time-based (like Litestream)
  - `max_changes` - frame count threshold
  - `max_interval` - max time between snapshots when active
  - `on_idle` - snapshot after inactivity

**Analysis**: Walrust is MORE flexible, not a gap. Keep current approach.

---

## IMPLEMENTATION PRIORITY

### Priority 1: MUST HAVE (v0.4) - ✅ COMPLETE

1. **Checkpoint Interval** - ✅ DONE
   ```rust
   pub checkpoint_interval: u64,         // Default: 60
   pub min_checkpoint_page_count: u64,   // Default: 1000
   ```

2. **Truncate Threshold** - ✅ DONE
   ```rust
   pub wal_truncate_threshold_pages: u64,  // Default: 121359 (0 = disabled)
   ```

### Priority 2: SHOULD HAVE (v0.5) - ✅ RESOLVED

3. **Monitor Interval** - ✅ RESOLVED (removed)
   - File watcher removed entirely
   - Pure polling now controlled by `wal_sync_interval`
   - Simpler and more reliable

### Priority 3: NICE TO HAVE (v1.0+) - ❌ TODO

4. **Validation Interval** - Verify backup integrity
   ```rust
   pub validation_interval: u64,  // Default: 0 (disabled)
   ```

---

## Proposed Config Changes

### Updated `SyncConfig` Struct

```rust
pub struct SyncConfig {
    // Snapshot triggers (existing, keep)
    pub snapshot_interval: u64,           // ✅ Have
    pub max_changes: u64,                 // ✅ Have
    pub max_interval: u64,                // ✅ Have
    pub on_idle: u64,                     // ✅ Have
    pub on_startup: bool,                 // ✅ Have

    // WAL syncing (NEW + existing)
    pub wal_sync_interval: u64,           // ✅ Added today
    pub monitor_interval: u64,            // ❌ ADD - file watch check rate

    // Checkpointing (NEW)
    pub checkpoint_interval: u64,         // ❌ ADD - PASSIVE checkpoint freq
    pub min_checkpoint_page_count: u64,   // ❌ ADD - min pages before checkpoint
    pub wal_truncate_threshold_pages: u64, // ❌ ADD - emergency TRUNCATE threshold

    // Compaction (existing, keep)
    pub compact_after_snapshot: bool,     // ✅ Have
    pub compact_interval: u64,            // ✅ Have

    // Validation (future)
    pub validation_interval: u64,         // ❌ ADD - backup integrity checks
}
```

### Updated Defaults

```rust
impl Default for SyncConfig {
    fn default() -> Self {
        Self {
            // Snapshots
            snapshot_interval: 3600,      // 1 hour
            max_changes: 0,               // disabled
            max_interval: 0,              // disabled
            on_idle: 0,                   // disabled
            on_startup: true,             // enabled

            // WAL syncing
            wal_sync_interval: 1,         // 1 second (batching)
            monitor_interval: 1,          // 1 second (file watch check)

            // Checkpointing
            checkpoint_interval: 60,      // 1 minute (PASSIVE)
            min_checkpoint_page_count: 1000,  // ~4MB
            wal_truncate_threshold_pages: 121359,  // ~500MB (emergency)

            // Compaction
            compact_after_snapshot: false,
            compact_interval: 0,          // disabled

            // Validation
            validation_interval: 0,       // disabled (future feature)
        }
    }
}
```

---

## CLI Flags (Current)

```bash
# Checkpointing (✅ implemented)
--checkpoint-interval <SECS>           # Default: 60
--min-checkpoint-pages <N>             # Default: 1000
--wal-truncate-threshold <PAGES>       # Default: 121359 (0 = disabled)

# Polling (✅ implemented - replaces file watcher)
--wal-sync-interval <SECS>             # Default: 1 (poll and sync frequency)

# Validation (future)
--validation-interval <SECS>           # Default: 0 (disabled)
```

---

## Config File Example

```toml
[sync]
# Snapshots
snapshot_interval = 3600      # 1 hour
max_changes = 0               # disabled (or set to 1000 for "snapshot every 1000 frames")
max_interval = 0              # disabled
on_idle = 0                   # disabled
on_startup = true

# WAL syncing (pure polling - no file watcher)
wal_sync_interval = 1         # Poll and sync every 1 second

# Checkpointing
checkpoint_interval = 60      # Run PASSIVE checkpoint every 60 seconds
min_checkpoint_page_count = 1000  # Only checkpoint if 1000+ pages (~4MB)
wal_truncate_threshold_pages = 121359  # Emergency TRUNCATE at ~500MB

# Compaction
compact_after_snapshot = false
compact_interval = 0          # disabled (or set to 7200 for every 2 hours)

# Validation (future)
validation_interval = 0       # disabled (or 86400 for daily validation)

[retention]
hourly = 24
daily = 7
weekly = 12
monthly = 12
```

---

## Testing Requirements

### For Checkpoint Interval

```rust
#[test]
fn checkpoint_prevents_unbounded_wal() {
    // Write 10,000 transactions
    // With checkpoint_interval=10s, min_checkpoint_page_count=100
    // Verify WAL size never exceeds ~1MB
    // Verify checkpoint happens every 10 seconds
}
```

### For Truncate Threshold

```rust
#[test]
fn truncate_threshold_emergency_brake() {
    // Disable auto-checkpointing
    // Set wal_truncate_threshold_pages=1000 (~4MB for testing)
    // Write until WAL > 4MB
    // Verify TRUNCATE checkpoint triggered
    // Verify writers were blocked temporarily
}
```

### For Monitor Interval

```rust
#[test]
fn monitor_interval_controls_cpu() {
    // Set monitor_interval=5 (slow)
    // Make 100 rapid writes
    // Verify file watcher doesn't check between every write
    // Verify syncs still happen (batched)
}
```

---

## Migration Path

### v0.4 (Immediate)
- Add checkpoint configuration
- Add truncate threshold
- Backward compatible (all defaults safe)

### v0.5 (Next release)
- Add monitor interval
- Optimize file watching

### v1.0 (Future)
- Add validation interval
- Implement daily backup verification

---

## References

- [Litestream Configuration](https://litestream.io/reference/config/)
- [Litestream WAL Truncate Guide](https://litestream.io/guides/wal-truncate-threshold/)
- [SQLite WAL Checkpointing](https://www.sqlite.org/wal.html#checkpointing)
- [Litestream Issue #189](https://github.com/benbjohnson/litestream/issues/189) - Sync interval discussion

---

## Conclusion

Status as of 2026-01-20:

| Config | Status | Notes |
|--------|--------|-------|
| `wal_sync_interval` | ✅ DONE | Added, serves as polling interval |
| `checkpoint_interval` | ✅ DONE | PASSIVE checkpoint every 60s |
| `min_checkpoint_page_count` | ✅ DONE | Min 1000 pages before checkpoint |
| `wal_truncate_threshold_pages` | ✅ DONE | Emergency TRUNCATE at ~500MB |
| `monitor_interval` | ✅ RESOLVED | **Removed** - file watcher dropped, pure polling now |
| `validation_interval` | ❌ TODO | Nice to have for backup integrity verification |

**Architecture Change** (2026-01-20):
- Removed file watcher (notify crate) entirely
- Now uses pure interval-based polling controlled by `wal_sync_interval`
- Simpler, more reliable, works on all platforms (FSEvents was missing mmap writes)

**Remaining Work**:
1. ❌ `validation_interval` - periodic backup integrity checks (nice to have)
