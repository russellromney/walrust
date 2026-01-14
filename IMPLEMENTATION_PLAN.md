# Implementation Plan: Missing Configuration Options

**Date**: 2026-01-14
**Goal**: Implement `monitor_interval` and `validation_interval` for production completeness

---

## Overview

Two critical configuration options are currently missing from walrust:

1. **monitor_interval** - Control file watcher check frequency
2. **validation_interval** - Automated backup integrity verification

These are NOT "nice-to-have" features. They are production requirements that need implementation.

---

## 1. Monitor Interval Implementation

### Problem Statement

**Current Behavior:**
- walrust uses OS file watcher events (via `notify` crate)
- Events trigger immediately on every WAL write
- No debouncing or rate limiting
- High-write workloads can overwhelm the system with events

**Real-World Impact:**
- Database with 1000 writes/sec = 1000 file watcher events/sec
- Each event triggers processing overhead
- CPU usage spikes on high-write workloads
- Multi-database deployments (100+ DBs) amplify the problem

### Solution Design

**Configuration:**
```rust
pub struct SyncConfig {
    // ... existing fields ...

    /// How often to check for WAL changes (seconds)
    /// Default: 1 (check every 1 second)
    /// Set higher (e.g., 5) for low-priority DBs to reduce CPU
    #[serde(default = "default_monitor_interval")]
    pub monitor_interval: u64,
}

fn default_monitor_interval() -> u64 {
    1
}
```

**Implementation Strategy:**

Instead of processing every file watcher event immediately, debounce events:

```rust
// In watch() function
let monitor_interval = Duration::from_secs(config.monitor_interval);

// Replace immediate event processing with debounced checks
let mut watcher = notify::recommended_watcher(move |res: Result<Event, notify::Error>| {
    if let Ok(event) = res {
        // Don't process immediately - just flag that changes exist
        changed_flag.store(true, Ordering::Relaxed);
    }
})?;

// Periodic check loop
loop {
    tokio::time::sleep(monitor_interval).await;

    if changed_flag.swap(false, Ordering::Relaxed) {
        // Changes detected - process WAL sync
        sync_wal().await?;
    }

    // Other periodic checks (checkpoint, snapshot, etc.)
}
```

**Key Benefits:**
- Batches multiple writes into single sync operation
- Reduces CPU usage on high-write workloads
- Configurable per-database (low-priority DBs can use higher intervals)
- Maintains responsiveness (default 1s is still fast)

### Implementation Steps

1. **Update Config Struct** - Add `monitor_interval` to `SyncConfig`
   - File: `src/config.rs`
   - Add field with default function
   - Add CLI flag: `--monitor-interval <SECS>`

2. **Add Debouncing Logic** - Replace immediate event processing
   - File: `src/sync.rs`
   - Add `AtomicBool` changed flag shared with watcher
   - Replace event processing with periodic check loop
   - Use `tokio::time::sleep(monitor_interval)`

3. **Update Watch Loop** - Integrate monitor interval with existing timers
   - Combine with checkpoint interval, snapshot interval
   - Use `tokio::select!` to handle multiple timers efficiently

4. **Add Tests**
   - Test that high write rate (100 writes/sec) with `monitor_interval=5` only syncs every 5 seconds
   - Test that all writes are still captured (no data loss)
   - Test per-database override in config file

5. **Update Documentation**
   - README.md - Add monitor_interval to config example
   - CLI help text - Document new flag

### Files to Modify

- `src/config.rs` - Add configuration field
- `src/sync.rs` - Implement debouncing logic
- `src/main.rs` - Add CLI flag
- `tests/integration_tests.rs` - Add test cases
- `README.md` - Document usage

### Estimated Time

**2-3 hours** (straightforward refactoring)

---

## 2. Validation Interval Implementation

### Problem Statement

**Current Behavior:**
- `walrust verify` exists for manual integrity checks
- No automated periodic validation
- Silent data corruption in S3 won't be detected until restore fails
- Production deployments need proactive verification

**Real-World Impact:**
- S3 silent bit flips can corrupt backups
- LTX checksum mismatches may go unnoticed
- Users discover corruption during emergency restore (worst time)
- Compliance requirements often mandate periodic backup verification

### Solution Design

**Configuration:**
```rust
pub struct SyncConfig {
    // ... existing fields ...

    /// Automated validation interval in seconds
    /// Default: 0 (disabled)
    /// Recommended: 86400 (daily validation)
    /// Warning: Each validation downloads full backup from S3
    #[serde(default)]
    pub validation_interval: u64,
}
```

**Implementation Strategy:**

Reuse existing `walrust verify` logic in a periodic task:

```rust
// In watch() function
if config.validation_interval > 0 {
    let validation_interval = Duration::from_secs(config.validation_interval);

    tokio::spawn(async move {
        let mut interval = tokio::time::interval(validation_interval);
        interval.tick().await; // Skip first immediate tick

        loop {
            interval.tick().await;

            match run_validation(&db_name, &bucket, &client).await {
                Ok(valid) => {
                    if valid {
                        log::info!("Validation passed for {}", db_name);
                        metrics.validation_success.inc();
                    } else {
                        log::error!("Validation FAILED for {}", db_name);
                        metrics.validation_failure.inc();
                        // Optional: Send alert (email, PagerDuty, etc.)
                    }
                }
                Err(e) => {
                    log::error!("Validation error for {}: {}", db_name, e);
                    metrics.validation_error.inc();
                }
            }
        }
    });
}
```

**Validation Steps:**
1. Download latest snapshot + all incremental LTX files
2. Restore to temporary location
3. Verify all LTX checksums (CRC64 + SHA256)
4. Verify TXID continuity (no gaps)
5. Verify manifest consistency
6. Optional: Compute full DB checksum and compare
7. Clean up temp files

**Cost Optimization:**
- Don't download full DB for checksum comparison (expensive!)
- Only verify LTX file integrity and metadata consistency
- Full restore test can be separate (weekly, not daily)

### Implementation Steps

1. **Update Config Struct** - Add `validation_interval` to `SyncConfig`
   - File: `src/config.rs`
   - Add field (default: 0 = disabled)
   - Add CLI flag: `--validation-interval <SECS>`

2. **Extract Validation Logic** - Refactor `walrust verify` into reusable function
   - File: `src/verify.rs` (new or rename existing)
   - Create `pub async fn validate_backup()` function
   - Return `Result<ValidationReport>`
   - Include metrics: files checked, bytes verified, errors found

3. **Add Periodic Validation Task** - Spawn background task in watch()
   - File: `src/sync.rs`
   - Use `tokio::spawn` for independent task
   - Use `tokio::time::interval` for periodic execution
   - Log results and update metrics

4. **Add Metrics** - Track validation results
   - File: `src/dashboard.rs`
   - Add counters: `validation_success`, `validation_failure`, `validation_error`
   - Add gauge: `last_validation_timestamp`

5. **Add Tests**
   - Test validation detects corrupted LTX files
   - Test validation detects TXID gaps
   - Test validation interval triggers correctly
   - Test validation with `interval=0` (disabled) doesn't run

6. **Update Documentation**
   - README.md - Add validation_interval to config example
   - Add "Backup Validation" section explaining automated checks
   - Document S3 bandwidth costs (validation downloads full backup)

### Files to Modify

- `src/config.rs` - Add configuration field
- `src/verify.rs` - Extract/refactor validation logic
- `src/sync.rs` - Add periodic validation task
- `src/dashboard.rs` - Add validation metrics
- `src/main.rs` - Add CLI flag
- `tests/integration_tests.rs` - Add test cases
- `README.md` - Document usage and costs

### Estimated Time

**4-6 hours** (more complex, involves refactoring verify command)

---

## Implementation Priority

### Week 1: Monitor Interval (Critical for Performance)
- **Day 1**: Implement config + debouncing (2-3 hours)
- **Day 1**: Add tests (1 hour)
- **Day 1**: Update docs (30 min)

### Week 1: Validation Interval (Critical for Reliability)
- **Day 2**: Refactor verify logic (2 hours)
- **Day 2**: Add periodic task (1 hour)
- **Day 2**: Add metrics (1 hour)
- **Day 3**: Add tests (1 hour)
- **Day 3**: Update docs (30 min)

### Total Time Estimate

**~10 hours** (1.5 days of focused work)

---

## Testing Strategy

### Monitor Interval Tests

```rust
#[tokio::test]
async fn test_monitor_interval_debounces_writes() {
    // Create test DB
    let db = create_test_db();

    // Configure walrust with monitor_interval=2
    let config = SyncConfig {
        monitor_interval: 2,
        ..Default::default()
    };

    // Start watch in background
    let handle = tokio::spawn(watch(vec![db.path()], "s3://test", config));

    // Write 100 times in 1 second (fast writes)
    for i in 0..100 {
        db.execute("INSERT INTO test VALUES (?)", [i])?;
        tokio::time::sleep(Duration::from_millis(10)).await;
    }

    // Wait 2 seconds for monitor interval
    tokio::time::sleep(Duration::from_secs(2)).await;

    // Verify: Only 1 sync happened (not 100)
    let sync_count = get_s3_put_count();
    assert!(sync_count < 10, "Should batch writes, got {} syncs", sync_count);

    // Verify: All 100 writes are in S3
    let restored = restore_from_s3("s3://test", "test");
    let count: i64 = restored.query_row("SELECT COUNT(*) FROM test", [], |r| r.get(0))?;
    assert_eq!(count, 100, "No data loss from debouncing");
}
```

### Validation Interval Tests

```rust
#[tokio::test]
async fn test_validation_detects_corruption() {
    // Create backup
    backup_db("test.db", "s3://test").await?;

    // Corrupt an LTX file in S3
    corrupt_s3_file("s3://test/test/00000001-00000010.ltx").await?;

    // Run validation
    let result = validate_backup("test", "s3://test").await?;

    // Should detect corruption
    assert!(!result.valid, "Should detect corrupted file");
    assert!(result.errors.iter().any(|e| e.contains("checksum")));
}

#[tokio::test]
async fn test_validation_interval_periodic() {
    // Start watch with validation_interval=2
    let config = SyncConfig {
        validation_interval: 2,
        ..Default::default()
    };

    let handle = tokio::spawn(watch(vec![db.path()], "s3://test", config));

    // Wait 5 seconds (should run 2 validations)
    tokio::time::sleep(Duration::from_secs(5)).await;

    // Check metrics
    let metrics = get_metrics();
    assert!(metrics.validation_success >= 2, "Should run periodic validation");
}
```

---

## Rollout Plan

### Phase 1: Implementation (Week 1)
1. Implement both features
2. Add comprehensive tests
3. Update documentation

### Phase 2: Testing (Week 1-2)
1. Run on test databases with high write rate
2. Verify monitor_interval reduces CPU usage
3. Run validation_interval on production backups (read-only)
4. Collect metrics on validation overhead

### Phase 3: Release (Week 2)
1. Merge to main branch
2. Tag v0.4.0 release
3. Update CHANGELOG.md
4. Announce in README

---

## Success Criteria

**Monitor Interval:**
- [ ] Config field added and tested
- [ ] Debouncing reduces CPU usage by >50% on high-write workloads
- [ ] Zero data loss (all writes still captured)
- [ ] Per-database override works in config file

**Validation Interval:**
- [ ] Config field added and tested
- [ ] Periodic validation task runs correctly
- [ ] Detects corrupted LTX files
- [ ] Detects TXID gaps
- [ ] Metrics track validation results
- [ ] Documentation warns about S3 bandwidth costs

---

## Documentation Updates

### README.md Changes

Add to configuration example:
```toml
[sync]
snapshot_interval = 3600
wal_sync_interval = 1
monitor_interval = 1           # NEW: File watcher check frequency
validation_interval = 86400    # NEW: Daily backup validation (0 = disabled)

# ... existing fields ...
```

Add new section:
```markdown
## Backup Validation

walrust can automatically validate backup integrity on a schedule:

```bash
# Enable daily validation
walrust watch mydb.db --validation-interval 86400
```

**What validation checks:**
- LTX file checksums (CRC64 + SHA256)
- TXID continuity (no gaps)
- Manifest consistency

**Cost warning:** Each validation downloads metadata from S3. For large
databases, this can consume significant bandwidth. Recommended interval:
daily (86400 seconds) or weekly (604800 seconds).

**Metrics:** Check `/metrics` endpoint for validation results:
- `walrust_validation_success_total` - Successful validations
- `walrust_validation_failure_total` - Failed validations
- `walrust_last_validation_timestamp` - Last validation time
```

---

## Next Steps

1. Review this plan with stakeholders
2. Begin implementation (Week 1, Day 1)
3. Open PR with monitor_interval implementation
4. Open PR with validation_interval implementation
5. Update ROADMAP.md after completion
