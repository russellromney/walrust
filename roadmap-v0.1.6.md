# walrust v0.1.6 Roadmap: PITR Bug Fix & Production Hardening

## Overview

Phase 4 is complete with 173 tests passing. This release fixes the known PITR bug and adds production hardening features.

**Known Bug**: `testable::restore` ignores `point_in_time` parameter - always restores to latest TXID.

---

## Part 1: PITR Bug Fix

### Problem

In `sync.rs:2996-3001`, the restore function ignores the `point_in_time` parameter:

```rust
let target_txid = if let Some(pit) = point_in_time {
    // Parse point-in-time (implement as needed)
    manifest.current_txid // For now, restore to latest  <-- BUG
} else {
    manifest.current_txid
};
```

### Solution

1. **Parse PITR formats**:
   - `txid:N` - Restore to specific transaction ID (e.g., `txid:12345`)
   - ISO8601 timestamp - Find nearest TXID before timestamp (e.g., `2024-01-15T10:30:00Z`)

2. **Select correct files for restore**:
   - Find the most recent snapshot with `max_txid <= target_txid`
   - Apply only incrementals where `min_txid > snapshot.max_txid AND max_txid <= target_txid`

3. **Validate target TXID exists**:
   - Return error if target TXID is not reachable from available files

### Implementation Steps

- [ ] Add `parse_point_in_time()` function in `sync.rs`
- [ ] Add `find_files_for_txid()` function to select snapshot + incrementals
- [ ] Update `testable::restore()` to use proper PITR logic
- [ ] Un-ignore `test_prop_point_in_time_restore` in `invariants.rs`
- [ ] Add unit tests for PITR parsing edge cases

---

## Part 2: Production Hardening

### 2.1 Stress Test Command

```bash
walrust-dst stress --databases 10 --writes-per-sec 100 --duration 60s
```

**What it tests**:
- Multiple databases under high write load
- Concurrent snapshot/sync operations
- Memory usage under pressure
- Error recovery with fault injection

**Metrics collected**:
- Operations/sec achieved
- Memory high-water mark
- Error rate
- Latency percentiles (p50, p95, p99)

### 2.2 Soak Test Command

```bash
walrust-dst soak --duration 24h --checkpoint-interval 1h
```

**What it tests**:
- Long-running stability
- Memory leak detection over time
- File descriptor leaks
- WAL growth management

**Checkpoints**:
- Periodic memory snapshots
- Trend analysis for leak detection
- Automatic failure on >10% memory growth

### 2.3 Resource Leak Detection

**Metrics to monitor**:
- RSS memory (via `/proc/self/status` on Linux, `mach_task_info` on macOS)
- Open file descriptors (`/proc/self/fd` count)
- Thread count
- Temp file cleanup

**Implementation**:
- `ResourceMonitor` struct with `check()` method
- Baseline at start, compare after each operation batch
- Alert if growth exceeds threshold

---

## Part 3: Test Matrix

| Test Type | Cases | Duration | Fault Injection |
|-----------|-------|----------|-----------------|
| Unit tests | 173 | ~30s | No |
| Property tests | 100/property | ~2min | Yes (10% error) |
| Stress test | 10 DBs | 60s | Yes (20% error) |
| Soak test | 1 DB | 24h | Low (1% error) |

---

## Success Criteria

- [ ] `test_prop_point_in_time_restore` passes with 100+ cases
- [ ] PITR works for both `txid:N` and ISO8601 formats
- [ ] Stress test: 10 DBs, 100 writes/sec, <5% error rate
- [ ] Soak test: 24h run with no memory leaks (< 10% growth)
- [ ] All 173+ tests continue passing

---

## Files Modified

- `walrust/src/sync.rs` - PITR parsing and file selection
- `walrust-dst/src/invariants.rs` - Un-ignore PITR test
- `walrust-dst/src/main.rs` - Add stress/soak CLI commands
- `walrust-dst/src/stress.rs` - NEW: Stress test implementation
- `walrust-dst/src/soak.rs` - NEW: Soak test implementation
- `walrust-dst/src/resources.rs` - NEW: Resource monitoring

---

## Next Session Prompt

After completing v0.1.6:

```
walrust dev: Implement real chaos testing with simulated clock and deterministic scheduling.
Add virtual time support to MockStorageBackend for reproducible failure sequences.
Implement checkpoint/restart for long-running soak tests.
```
