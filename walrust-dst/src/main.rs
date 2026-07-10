//! Deterministic Simulation Testing CLI for Walrust
//!
//! Run property-based tests, chaos tests, and stress tests to verify
//! walrust's data safety guarantees.

mod chaos;
mod disk_queue_tests;
mod invariants;
pub mod metrics;
pub mod mock_storage;
mod properties;
// Entry points are exercised via `cargo test -p walrust-dst state_machine`
// (and the nightly deep sweep); the CLI binary itself does not call them.
#[cfg_attr(not(test), allow(dead_code))]
mod state_machine;

use anyhow::Context;
use clap::{Parser, Subcommand};
use metrics::{format_report, get_memory_usage, get_open_fds, IterationResult, MetricsCollector};
use rand::Rng;
use std::path::PathBuf;
use std::time::{Duration, Instant};

#[derive(Parser)]
#[command(name = "walrust-dst")]
#[command(about = "Deterministic Simulation Testing for Walrust")]
#[command(version)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Quick sanity check (runs fast property tests)
    Smoke,

    /// Property-based tests with configurable iterations
    Properties {
        /// Number of test cases per property
        #[arg(long, default_value = "100")]
        cases: u64,

        /// Specific property to test (default: all)
        #[arg(long)]
        property: Option<String>,
    },

    /// Fault injection / chaos tests
    Chaos {
        /// Comma-separated fault types: s3_errors,crashes,eventual_consistency,corruption,stress,all
        #[arg(long, default_value = "all")]
        faults: String,

        /// Seed for deterministic fault injection (default: random)
        #[arg(long)]
        seed: Option<u64>,

        /// Number of iterations per test
        #[arg(long, default_value = "10")]
        iterations: u32,
    },

    /// Stress test with multiple databases
    Stress {
        /// Number of databases to simulate
        #[arg(long, default_value = "10")]
        databases: usize,

        /// Writes per second per database
        #[arg(long, default_value = "100")]
        writes_per_sec: usize,

        /// Duration in seconds
        #[arg(long, default_value = "60")]
        duration_secs: u64,
    },

    /// Long-running soak test
    Soak {
        /// Duration (e.g., "1h", "24h")
        #[arg(long, default_value = "1h")]
        duration: String,
    },

    /// Reproduce a failure from a specific seed
    Replay {
        /// The seed to replay
        #[arg(long)]
        seed: u64,
    },

    /// Run continuous chaos testing with MTBF tracking
    Continuous {
        /// Number of iterations to run
        #[arg(long, default_value = "10000")]
        iterations: u64,

        /// Starting seed (default: random)
        #[arg(long)]
        seed: Option<u64>,

        /// Error injection rate (0.0-1.0)
        #[arg(long, default_value = "0.1")]
        error_rate: f64,

        /// Output JSON metrics to file
        #[arg(long)]
        output: Option<String>,

        /// Run invariant tests (not just chaos)
        #[arg(long)]
        invariants: bool,
    },

    /// Run core invariant property tests
    Invariants {
        /// Number of test cases per property
        #[arg(long, default_value = "100")]
        cases: u64,

        /// Specific invariant to test
        #[arg(long)]
        invariant: Option<String>,
    },
}

fn main() -> anyhow::Result<()> {
    // Crash-test child mode: when spawned by chaos_process_crash_recovery, this
    // process is a durable-writer that gets SIGKILLed mid-write. Dispatch before
    // clap so the child never touches the CLI surface.
    if std::env::var(chaos::CRASH_CHILD_ENV).is_ok() {
        return chaos::run_crash_child();
    }

    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::from_default_env()
                .add_directive("walrust_dst=info".parse().unwrap()),
        )
        .init();

    let cli = Cli::parse();

    match cli.command {
        Commands::Smoke => {
            println!("Running smoke tests...\n");
            run_smoke_tests()?;
            println!("\nSmoke tests passed!");
        }

        Commands::Properties { cases, property } => {
            println!("Running property tests ({} cases per property)...\n", cases);
            std::env::set_var("PROPTEST_CASES", cases.to_string());

            if let Some(prop) = property {
                println!("Running property: {}", prop);
                run_single_property(&prop)?;
            } else {
                run_all_properties()?;
            }
            println!("\nProperty tests passed!");
        }

        Commands::Chaos {
            faults,
            seed,
            iterations,
        } => {
            let seed = seed.unwrap_or_else(|| rand::thread_rng().gen());
            println!(
                "Running chaos tests with faults: {} (seed: {:#x})\n",
                faults, seed
            );
            let fault_types: Vec<&str> = faults.split(',').collect();
            run_chaos_tests(&fault_types, seed, iterations)?;
            println!("\nChaos tests completed!");
        }

        Commands::Stress {
            databases,
            writes_per_sec,
            duration_secs,
        } => {
            println!(
                "Running stress test: {} databases, {} writes/sec, {}s\n",
                databases, writes_per_sec, duration_secs
            );
            run_stress_test(
                databases,
                writes_per_sec,
                Duration::from_secs(duration_secs),
            )?;
            println!("\nStress test passed!");
        }

        Commands::Soak { duration } => {
            let dur = parse_duration(&duration)?;
            println!("Running soak test for {:?}...\n", dur);
            run_soak_test(dur)?;
            println!("\nSoak test passed!");
        }

        Commands::Replay { seed } => {
            println!("Replaying with seed: {}\n", seed);
            std::env::set_var("PROPTEST_SEED", format!("{:#x}", seed));
            run_all_properties()?;
            println!("\nReplay completed!");
        }

        Commands::Continuous {
            iterations,
            seed,
            error_rate,
            output,
            invariants: run_invariants,
        } => {
            let seed = seed.unwrap_or_else(|| rand::thread_rng().gen());
            println!(
                "Running continuous chaos testing ({} iterations, seed: {:#x}, error_rate: {:.1}%)\n",
                iterations,
                seed,
                error_rate * 100.0
            );
            let metrics = run_continuous_chaos(iterations, seed, error_rate, run_invariants)?;
            println!("{}", format_report(&metrics));

            if let Some(output_path) = output {
                let json = serde_json::to_string_pretty(&metrics)?;
                std::fs::write(&output_path, json)?;
                println!("Metrics written to: {}", output_path);
            }

            if metrics.failed > 0 {
                println!(
                    "\nWARNING: {} failures detected. Use --seed to replay failure seeds.",
                    metrics.failed
                );
            }
        }

        Commands::Invariants { cases, invariant } => {
            println!(
                "Running invariant tests ({} cases per property)...\n",
                cases
            );
            std::env::set_var("PROPTEST_CASES", cases.to_string());

            if let Some(inv) = invariant {
                println!("Running invariant: {}", inv);
                run_single_invariant(&inv)?;
            } else {
                run_all_invariants()?;
            }
            println!("\nInvariant tests passed!");
        }
    }

    Ok(())
}

fn run_smoke_tests() -> anyhow::Result<()> {
    // Quick sanity tests - no S3 required
    properties::test_sqlite_wal_basics()?;
    println!("  [PASS] SQLite WAL basics");

    properties::test_ltx_roundtrip()?;
    println!("  [PASS] LTX roundtrip (using walrust::ltx)");

    properties::test_wal_frame_reading()?;
    println!("  [PASS] WAL frame reading (using walrust::wal)");

    properties::test_ltx_verification()?;
    println!("  [PASS] LTX verification (corrupt detection)");

    Ok(())
}

fn run_single_property(name: &str) -> anyhow::Result<()> {
    match name {
        "ltx_roundtrip" => properties::prop_ltx_roundtrip()?,
        "durability" => properties::prop_durability()?,
        "snapshot_integrity" => properties::prop_snapshot_integrity()?,
        "concurrent_checkpoint" => properties::prop_concurrent_checkpoint_safety()?,
        "large_database" => properties::prop_large_database_handling()?,
        "incremental_chain" => properties::prop_incremental_ltx_chain()?,
        "wal_page_sizes" => properties::prop_wal_page_sizes()?,
        _ => anyhow::bail!(
            "Unknown property: {}. Available: ltx_roundtrip, durability, snapshot_integrity, \
             concurrent_checkpoint, large_database, incremental_chain, wal_page_sizes",
            name
        ),
    }
    Ok(())
}

fn run_all_properties() -> anyhow::Result<()> {
    let properties = [
        ("ltx_roundtrip", "LTX encode/decode byte-for-byte"),
        ("durability", "Snapshot/restore preserves all data"),
        ("snapshot_integrity", "Snapshots are valid SQLite DBs"),
        ("concurrent_checkpoint", "Concurrent writes don't corrupt"),
        ("large_database", "Large DBs don't cause OOM"),
        ("incremental_chain", "Incremental LTX chain integrity"),
        ("wal_page_sizes", "WAL parsing across page sizes"),
    ];

    for (name, desc) in &properties {
        println!("  Running: {} - {}", name, desc);
        run_single_property(name)?;
        println!("  [PASS] {}", name);
    }

    Ok(())
}

fn run_chaos_tests(fault_types: &[&str], seed: u64, iterations: u32) -> anyhow::Result<()> {
    let rt = tokio::runtime::Runtime::new()?;

    let run_all = fault_types.iter().any(|f| *f == "all");
    let mut all_passed = true;
    let mut results = Vec::new();

    // Note: Most chaos tests require MadSim integration to properly test walrust.
    // Currently only corruption detection is implemented (tests ltx::verify_ltx).
    // See BATTLE_TESTING.md "Chaos Test Roadmap" for the full plan.

    for fault in fault_types {
        match *fault {
            "all" => {
                println!("  Running implemented chaos tests...\n");
                println!("  NOTE: Full chaos testing requires MadSim integration.");
                println!("        See BATTLE_TESTING.md for roadmap.\n");
                let all_results = rt.block_on(chaos::run_all_chaos_tests(seed));
                for result in &all_results {
                    print_chaos_result(result);
                    if !result.passed {
                        all_passed = false;
                    }
                }
                results.extend(all_results);

                // Real child-process SIGKILL crash recovery (not MadSim-gated).
                let binary =
                    std::env::current_exe().context("locate walrust-dst binary for crash child")?;
                let crash = chaos::chaos_process_crash_recovery(&binary, seed, iterations.max(5))?;
                print_chaos_result(&crash);
                if !crash.passed {
                    all_passed = false;
                }
                results.push(crash);
            }
            "corruption" if !run_all => {
                println!("  Testing fault: corruption");
                let result = rt.block_on(chaos::chaos_silent_corruption(seed, iterations))?;
                print_chaos_result(&result);
                if !result.passed {
                    all_passed = false;
                }
                results.push(result);
            }
            "crashes" if !run_all => {
                println!("  Testing fault: crashes (real child-process SIGKILL)");
                let binary =
                    std::env::current_exe().context("locate walrust-dst binary for crash child")?;
                let result = chaos::chaos_process_crash_recovery(&binary, seed, iterations.max(5))?;
                print_chaos_result(&result);
                if !result.passed {
                    all_passed = false;
                }
                results.push(result);
            }
            "s3_errors" | "eventual_consistency" | "stress" if !run_all => {
                println!("  [TODO] {} - requires MadSim integration", fault);
                println!("         See BATTLE_TESTING.md for roadmap");
            }
            _ if !run_all => {
                println!("  [WARN] Unknown fault type: {}", fault);
            }
            _ => {}
        }
    }

    // Summary
    if !results.is_empty() {
        println!(
            "\n  Summary: {}/{} tests passed",
            results.iter().filter(|r| r.passed).count(),
            results.len()
        );
    }

    if !all_passed {
        anyhow::bail!("Some chaos tests failed");
    }

    Ok(())
}

fn print_chaos_result(result: &chaos::ChaosTestResult) {
    let status = if result.passed { "[PASS]" } else { "[FAIL]" };
    println!(
        "    {} {} ({} iterations, {} errors injected, {} recovered)",
        status, result.name, result.iterations, result.errors_injected, result.errors_recovered
    );
    println!("         {}", result.message);
}

/// Threshold decision for a stress run. A breach is a hard failure so CI and
/// automation see a non-zero exit instead of a buried `[WARN]` line.
pub fn evaluate_stress_result(error_rate_pct: f64, max_error_rate_pct: f64) -> anyhow::Result<()> {
    if error_rate_pct >= max_error_rate_pct {
        anyhow::bail!(
            "stress error rate {error_rate_pct:.1}% breached the {max_error_rate_pct:.1}% threshold"
        );
    }
    Ok(())
}

/// Threshold decision for a soak run. Any breach — sustained memory growth, an
/// upward memory trend, or file-descriptor growth — is a hard failure so a leak
/// surfaces as a non-zero exit rather than a `[WARN]` nobody reads.
pub fn evaluate_soak_result(
    memory_growth_pct: f64,
    memory_trend_pct: Option<f64>,
    fd_growth: i32,
    max_memory_growth_pct: f64,
    max_memory_trend_pct: f64,
    max_fd_growth: i32,
) -> anyhow::Result<()> {
    let mut breaches = Vec::new();
    if memory_growth_pct.abs() >= max_memory_growth_pct {
        breaches.push(format!(
            "memory growth {memory_growth_pct:.1}% >= {max_memory_growth_pct:.1}%"
        ));
    }
    if let Some(trend) = memory_trend_pct {
        if trend >= max_memory_trend_pct {
            breaches.push(format!(
                "memory trend {trend:.1}% >= {max_memory_trend_pct:.1}%"
            ));
        }
    }
    if fd_growth >= max_fd_growth {
        breaches.push(format!("fd growth {fd_growth} >= {max_fd_growth}"));
    }
    if !breaches.is_empty() {
        anyhow::bail!("soak thresholds breached: {}", breaches.join("; "));
    }
    Ok(())
}

fn run_stress_test(
    databases: usize,
    writes_per_sec: usize,
    duration: Duration,
) -> anyhow::Result<()> {
    use mock_storage::{MockStorageBackend, MockStorageConfig, StorageFault};
    use rusqlite::Connection;
    use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
    use std::sync::Arc;
    use tempfile::TempDir;
    use walrust::retry::{RetryConfig, RetryPolicy};
    use walrust::testable::{self, SyncState};

    let rt = tokio::runtime::Runtime::new()?;

    // Setup
    let tmpdir = TempDir::new()?;
    let stop_flag = Arc::new(AtomicBool::new(false));
    let total_writes = Arc::new(AtomicU64::new(0));
    let total_syncs = Arc::new(AtomicU64::new(0));
    let total_errors = Arc::new(AtomicU64::new(0));

    let start = Instant::now();
    let baseline_memory = get_memory_usage().unwrap_or(0);
    let baseline_fds = get_open_fds().unwrap_or(0);

    println!(
        "  Baseline: Memory={:.2}MB, FDs={}",
        baseline_memory as f64 / 1024.0 / 1024.0,
        baseline_fds
    );
    println!("  Creating {} databases...", databases);

    // Create databases and mock storage
    let mut db_infos: Vec<(PathBuf, MockStorageBackend, SyncState)> = Vec::new();

    for i in 0..databases {
        let db_path = tmpdir.path().join(format!("stress_db_{}.db", i));

        // Create and initialize database
        let conn = Connection::open(&db_path)?;
        conn.execute_batch(
            "PRAGMA journal_mode=WAL; PRAGMA page_size=4096;
             CREATE TABLE stress_data (id INTEGER PRIMARY KEY, data TEXT, ts TEXT);",
        )?;
        conn.execute_batch("PRAGMA wal_checkpoint(TRUNCATE);")?;
        drop(conn);

        // Create storage with 20% error rate for stress
        let config = MockStorageConfig::new(&format!("stress-bucket-{}", i))
            .with_seed(i as u64)
            .with_fault(StorageFault::RandomError { rate: 0.2 });
        let storage = MockStorageBackend::new(config);

        let state = SyncState::new(db_path.clone())?;
        db_infos.push((db_path.clone(), storage, state));
    }

    println!("  Starting stress test for {:?}...", duration);

    // Run stress test
    let stop = stop_flag.clone();
    let writes = total_writes.clone();
    let syncs = total_syncs.clone();
    let errors = total_errors.clone();

    rt.block_on(async {
        // Spawn timer to stop test
        let stop_timer = stop_flag.clone();
        tokio::spawn(async move {
            tokio::time::sleep(duration).await;
            stop_timer.store(true, Ordering::SeqCst);
        });

        let retry_config = RetryConfig {
            max_retries: 3,
            base_delay_ms: 10,
            max_delay_ms: 100,
            circuit_breaker_enabled: false,
            ..Default::default()
        };
        let retry_policy = RetryPolicy::new(retry_config);

        // Write loop
        let write_interval = Duration::from_micros(1_000_000 / writes_per_sec as u64);
        let mut write_count = 0u64;

        while !stop.load(Ordering::SeqCst) {
            // Pick a random database
            let db_idx = (write_count as usize) % databases;
            let (ref db_path, ref storage, ref mut state) = db_infos[db_idx];

            // Write a row
            {
                let conn = Connection::open(db_path)?;
                let data = format!("stress_write_{}", write_count);
                conn.execute(
                    "INSERT INTO stress_data (data, ts) VALUES (?, datetime('now'))",
                    [&data],
                )?;
            }

            writes.fetch_add(1, Ordering::SeqCst);
            write_count += 1;

            // Sync every 10 writes
            if write_count % 10 == 0 {
                match testable::sync_wal_with_retry(storage, "", state, &retry_policy).await {
                    Ok(_) => {
                        syncs.fetch_add(1, Ordering::SeqCst);
                    }
                    Err(_) => {
                        errors.fetch_add(1, Ordering::SeqCst);
                    }
                }
            }

            // Snapshot every 100 writes
            if write_count % 100 == 0 {
                // Checkpoint first
                {
                    let conn = Connection::open(db_path)?;
                    conn.execute_batch("PRAGMA wal_checkpoint(PASSIVE);")?;
                }

                match testable::take_snapshot_with_retry(storage, "", state, &retry_policy).await {
                    Ok(_) => {
                        syncs.fetch_add(1, Ordering::SeqCst);
                    }
                    Err(_) => {
                        errors.fetch_add(1, Ordering::SeqCst);
                    }
                }
            }

            // Rate limiting
            tokio::time::sleep(write_interval).await;
        }

        Ok::<_, anyhow::Error>(())
    })?;

    let elapsed = start.elapsed();
    let final_memory = get_memory_usage().unwrap_or(0);
    let final_fds = get_open_fds().unwrap_or(0);

    // Report
    let writes_done = total_writes.load(Ordering::SeqCst);
    let syncs_done = total_syncs.load(Ordering::SeqCst);
    let errors_count = total_errors.load(Ordering::SeqCst);

    let writes_per_sec_actual = writes_done as f64 / elapsed.as_secs_f64();
    let error_rate = if syncs_done > 0 {
        errors_count as f64 / (syncs_done + errors_count) as f64 * 100.0
    } else {
        0.0
    };

    println!("\n  Results:");
    println!("    Duration:      {:?}", elapsed);
    println!(
        "    Writes:        {} ({:.1}/sec)",
        writes_done, writes_per_sec_actual
    );
    println!("    Syncs:         {}", syncs_done);
    println!("    Errors:        {} ({:.1}%)", errors_count, error_rate);
    println!(
        "    Memory:        {:.2}MB -> {:.2}MB (delta: {:+.2}MB)",
        baseline_memory as f64 / 1024.0 / 1024.0,
        final_memory as f64 / 1024.0 / 1024.0,
        (final_memory as i64 - baseline_memory as i64) as f64 / 1024.0 / 1024.0
    );
    println!(
        "    FDs:           {} -> {} (delta: {:+})",
        baseline_fds,
        final_fds,
        final_fds as i32 - baseline_fds as i32
    );

    // Success criteria: <10% error rate under 20% fault injection. A breach is
    // a hard failure (non-zero exit), not a buried [WARN].
    const STRESS_MAX_ERROR_RATE_PCT: f64 = 10.0;
    if error_rate < STRESS_MAX_ERROR_RATE_PCT {
        println!(
            "\n  [PASS] Error rate {:.1}% < {:.1}% threshold",
            error_rate, STRESS_MAX_ERROR_RATE_PCT
        );
    } else {
        println!(
            "\n  [FAIL] Error rate {:.1}% exceeds {:.1}% threshold",
            error_rate, STRESS_MAX_ERROR_RATE_PCT
        );
    }

    evaluate_stress_result(error_rate, STRESS_MAX_ERROR_RATE_PCT)
}

fn run_soak_test(duration: Duration) -> anyhow::Result<()> {
    use mock_storage::{MockStorageBackend, MockStorageConfig, StorageFault};
    use rusqlite::Connection;
    use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
    use std::sync::Arc;
    use tempfile::TempDir;
    use walrust::retry::{RetryConfig, RetryPolicy};
    use walrust::testable::{self, SyncState};

    let rt = tokio::runtime::Runtime::new()?;

    // Setup
    let tmpdir = TempDir::new()?;
    let stop_flag = Arc::new(AtomicBool::new(false));

    let start = Instant::now();
    let baseline_memory = get_memory_usage().unwrap_or(0);
    let baseline_fds = get_open_fds().unwrap_or(0);

    // Checkpoint interval for memory tracking
    let checkpoint_interval = Duration::from_secs(60);
    let mut last_checkpoint = Instant::now();
    let mut memory_samples: Vec<(Duration, u64)> = Vec::new();
    let mut fd_samples: Vec<(Duration, u32)> = Vec::new();

    memory_samples.push((Duration::ZERO, baseline_memory));
    fd_samples.push((Duration::ZERO, baseline_fds));

    println!(
        "  Baseline: Memory={:.2}MB, FDs={}",
        baseline_memory as f64 / 1024.0 / 1024.0,
        baseline_fds
    );
    println!("  Starting soak test for {:?}...", duration);
    println!("  (Checkpoints every {:?})", checkpoint_interval);

    // Create database
    let db_path = tmpdir.path().join("soak_test.db");
    let conn = Connection::open(&db_path)?;
    conn.execute_batch(
        "PRAGMA journal_mode=WAL; PRAGMA page_size=4096;
         CREATE TABLE soak_data (id INTEGER PRIMARY KEY, data TEXT, ts TEXT);",
    )?;
    conn.execute_batch("PRAGMA wal_checkpoint(TRUNCATE);")?;
    drop(conn);

    // Storage with low error rate for stability
    let config = MockStorageConfig::new("soak-bucket")
        .with_seed(42)
        .with_fault(StorageFault::RandomError { rate: 0.01 });
    let storage = MockStorageBackend::new(config);
    let mut state = SyncState::new(db_path.clone())?;

    let retry_config = RetryConfig {
        max_retries: 5,
        base_delay_ms: 50,
        max_delay_ms: 1000,
        circuit_breaker_enabled: false,
        ..Default::default()
    };
    let retry_policy = RetryPolicy::new(retry_config);

    let total_operations = Arc::new(AtomicU64::new(0));
    let total_errors = Arc::new(AtomicU64::new(0));

    // Run soak test
    let stop = stop_flag.clone();
    let ops = total_operations.clone();
    let errs = total_errors.clone();

    rt.block_on(async {
        // Spawn timer
        let stop_timer = stop_flag.clone();
        tokio::spawn(async move {
            tokio::time::sleep(duration).await;
            stop_timer.store(true, Ordering::SeqCst);
        });

        let mut op_count = 0u64;

        while !stop.load(Ordering::SeqCst) {
            // Write some data
            {
                let conn = Connection::open(&db_path)?;
                for i in 0..10 {
                    let data = format!("soak_write_{}_{}", op_count, i);
                    conn.execute(
                        "INSERT INTO soak_data (data, ts) VALUES (?, datetime('now'))",
                        [&data],
                    )?;
                }
                drop(conn);
            }

            // Sync WAL
            if let Err(_) =
                testable::sync_wal_with_retry(&storage, "", &mut state, &retry_policy).await
            {
                errs.fetch_add(1, Ordering::SeqCst);
            }

            // Checkpoint and snapshot periodically
            if op_count % 100 == 0 {
                let conn = Connection::open(&db_path)?;
                conn.execute_batch("PRAGMA wal_checkpoint(PASSIVE);")?;
                drop(conn);

                if let Err(_) =
                    testable::take_snapshot_with_retry(&storage, "", &mut state, &retry_policy)
                        .await
                {
                    errs.fetch_add(1, Ordering::SeqCst);
                }
            }

            ops.fetch_add(1, Ordering::SeqCst);
            op_count += 1;

            // Memory checkpoint
            if last_checkpoint.elapsed() >= checkpoint_interval {
                let elapsed = start.elapsed();
                let mem = get_memory_usage().unwrap_or(0);
                let fds = get_open_fds().unwrap_or(0);
                memory_samples.push((elapsed, mem));
                fd_samples.push((elapsed, fds));

                let mem_delta =
                    (mem as i64 - baseline_memory as i64) as f64 / baseline_memory as f64 * 100.0;
                println!(
                    "  [{:?}] Ops: {}, Memory: {:.2}MB ({:+.1}%), FDs: {}",
                    elapsed,
                    op_count,
                    mem as f64 / 1024.0 / 1024.0,
                    mem_delta,
                    fds
                );

                // Check for memory leak (>50% growth is concerning)
                if mem_delta > 50.0 {
                    println!(
                        "  [WARN] Possible memory leak detected: {:+.1}% growth",
                        mem_delta
                    );
                }

                last_checkpoint = Instant::now();
            }

            // Slow down to avoid overwhelming the system
            tokio::time::sleep(Duration::from_millis(100)).await;
        }

        Ok::<_, anyhow::Error>(())
    })?;

    // Final report
    let elapsed = start.elapsed();
    let final_memory = get_memory_usage().unwrap_or(0);
    let final_fds = get_open_fds().unwrap_or(0);
    let ops_done = total_operations.load(Ordering::SeqCst);
    let errors_count = total_errors.load(Ordering::SeqCst);

    let memory_growth =
        (final_memory as i64 - baseline_memory as i64) as f64 / baseline_memory as f64 * 100.0;
    let fd_growth = final_fds as i32 - baseline_fds as i32;

    println!("\n  Soak Test Results:");
    println!("    Duration:      {:?}", elapsed);
    println!(
        "    Operations:    {} ({:.1}/sec)",
        ops_done,
        ops_done as f64 / elapsed.as_secs_f64()
    );
    println!("    Errors:        {}", errors_count);
    println!(
        "    Memory:        {:.2}MB -> {:.2}MB ({:+.1}%)",
        baseline_memory as f64 / 1024.0 / 1024.0,
        final_memory as f64 / 1024.0 / 1024.0,
        memory_growth
    );
    println!(
        "    FDs:           {} -> {} ({:+})",
        baseline_fds, final_fds, fd_growth
    );

    // Memory trend analysis (only meaningful with enough samples).
    let memory_trend_pct: Option<f64> = if memory_samples.len() >= 3 {
        let first_half_avg: f64 = memory_samples[..memory_samples.len() / 2]
            .iter()
            .map(|(_, m)| *m as f64)
            .sum::<f64>()
            / (memory_samples.len() / 2) as f64;
        let second_half_avg: f64 = memory_samples[memory_samples.len() / 2..]
            .iter()
            .map(|(_, m)| *m as f64)
            .sum::<f64>()
            / (memory_samples.len() - memory_samples.len() / 2) as f64;
        let trend = (second_half_avg - first_half_avg) / first_half_avg * 100.0;

        println!(
            "    Memory Trend:  {:+.1}% (first half avg vs second half avg)",
            trend
        );
        Some(trend)
    } else {
        None
    };

    // Hard thresholds — a breach exits non-zero so a leak surfaces in CI. The
    // thresholds are set well above healthy warmup noise (which the [WARN] lines
    // above still surface) so only a genuine leak / fd exhaustion fails the run.
    const SOAK_MAX_MEMORY_GROWTH_PCT: f64 = 50.0;
    const SOAK_MAX_MEMORY_TREND_PCT: f64 = 25.0;
    const SOAK_MAX_FD_GROWTH: i32 = 64;

    if memory_growth.abs() >= 10.0 {
        println!(
            "  [WARN] Memory growth {:.1}% exceeds soft 10% threshold",
            memory_growth
        );
    }

    evaluate_soak_result(
        memory_growth,
        memory_trend_pct,
        fd_growth,
        SOAK_MAX_MEMORY_GROWTH_PCT,
        SOAK_MAX_MEMORY_TREND_PCT,
        SOAK_MAX_FD_GROWTH,
    )
}

fn parse_duration(s: &str) -> anyhow::Result<Duration> {
    let s = s.trim().to_lowercase();
    if s.ends_with('h') {
        let hours: u64 = s.trim_end_matches('h').parse()?;
        Ok(Duration::from_secs(hours * 3600))
    } else if s.ends_with('m') {
        let mins: u64 = s.trim_end_matches('m').parse()?;
        Ok(Duration::from_secs(mins * 60))
    } else if s.ends_with('s') {
        let secs: u64 = s.trim_end_matches('s').parse()?;
        Ok(Duration::from_secs(secs))
    } else {
        let secs: u64 = s.parse()?;
        Ok(Duration::from_secs(secs))
    }
}

// ============================================================================
// Continuous Chaos Testing
// ============================================================================

fn run_continuous_chaos(
    iterations: u64,
    seed: u64,
    error_rate: f64,
    run_invariants: bool,
) -> anyhow::Result<metrics::ChaosMetrics> {
    let rt = tokio::runtime::Runtime::new()?;
    let mut collector = MetricsCollector::new("continuous_chaos");

    let progress_interval = std::cmp::max(iterations / 20, 1);

    for i in 0..iterations {
        let iter_seed = seed.wrapping_add(i);
        let start = Instant::now();

        let result = rt.block_on(run_single_chaos_iteration(
            iter_seed,
            error_rate,
            run_invariants,
        ));

        let duration_ms = start.elapsed().as_millis() as u64;
        let (passed, error) = match result {
            Ok(_) => (true, None),
            Err(e) => (false, Some(e.to_string())),
        };

        collector.record_iteration(IterationResult {
            iteration: i,
            seed: iter_seed,
            passed,
            duration_ms,
            error,
            memory_bytes: get_memory_usage(),
            open_fds: get_open_fds(),
        });

        // Progress reporting
        if (i + 1) % progress_interval == 0 || i == iterations - 1 {
            let pct = ((i + 1) as f64 / iterations as f64) * 100.0;
            println!(
                "  Progress: {:.0}% ({}/{}) - Pass rate: {:.1}%",
                pct,
                i + 1,
                iterations,
                collector.pass_rate()
            );
        }
    }

    Ok(collector.finalize())
}

async fn run_single_chaos_iteration(
    seed: u64,
    error_rate: f64,
    run_invariants: bool,
) -> anyhow::Result<()> {
    use mock_storage::{MockStorageBackend, MockStorageConfig, StorageFault};
    use rusqlite::Connection;
    use tempfile::TempDir;
    use walrust::testable::{self, SyncState};

    let tmpdir = TempDir::new()?;
    let db_path = tmpdir.path().join("test.db");
    let restored_path = tmpdir.path().join("restored.db");

    // Create test database
    let conn = Connection::open(&db_path)?;
    conn.execute_batch(
        "PRAGMA journal_mode=WAL; PRAGMA page_size=4096;
         CREATE TABLE chaos_test (id INTEGER PRIMARY KEY, data TEXT, checksum TEXT);",
    )?;

    // Insert random amount of data
    let num_rows = (seed % 50) as usize + 5;
    for i in 0..num_rows {
        let data = format!("data_{}_{}", seed, i);
        let checksum = format!("{:x}", seed.wrapping_mul(i as u64 + 1));
        conn.execute(
            "INSERT INTO chaos_test (data, checksum) VALUES (?, ?)",
            [&data, &checksum],
        )?;
    }
    conn.execute_batch("PRAGMA wal_checkpoint(TRUNCATE);")?;
    drop(conn);

    // Create storage with error injection
    let config = MockStorageConfig::new("test-bucket")
        .with_seed(seed)
        .with_fault(StorageFault::RandomError { rate: error_rate });
    let storage = MockStorageBackend::new(config);

    // Use retry policy for reliability
    let retry_config = walrust::retry::RetryConfig {
        max_retries: 5,
        base_delay_ms: 10,
        max_delay_ms: 100,
        circuit_breaker_enabled: false,
        ..Default::default()
    };
    let retry_policy = walrust::retry::RetryPolicy::new(retry_config);

    let mut state = SyncState::new(db_path.clone())?;

    // Take snapshot with retry
    testable::take_snapshot_with_retry(&storage, "", &mut state, &retry_policy).await?;

    // Restore
    testable::restore(&storage, "", &state.name, &restored_path, None::<&str>).await?;

    // Verify data integrity
    let restored_conn = Connection::open(&restored_path)?;
    let integrity: String = restored_conn.query_row("PRAGMA integrity_check", [], |r| r.get(0))?;
    if integrity != "ok" {
        anyhow::bail!("Integrity check failed: {}", integrity);
    }

    let count: i64 =
        restored_conn.query_row("SELECT COUNT(*) FROM chaos_test", [], |r| r.get(0))?;
    if count as usize != num_rows {
        anyhow::bail!("Data loss: expected {} rows, got {}", num_rows, count);
    }

    // Run invariant checks if requested
    if run_invariants {
        // Check TXID is valid
        if state.current_txid == 0 {
            anyhow::bail!("TXID should not be 0 after snapshot");
        }

        // Check data matches
        for i in 0..num_rows {
            let expected_data = format!("data_{}_{}", seed, i);
            let actual: String = restored_conn.query_row(
                "SELECT data FROM chaos_test WHERE id = ?",
                [i as i64 + 1],
                |r| r.get(0),
            )?;
            if actual != expected_data {
                anyhow::bail!(
                    "Data mismatch at row {}: expected '{}', got '{}'",
                    i,
                    expected_data,
                    actual
                );
            }
        }
    }

    Ok(())
}

// ============================================================================
// Invariant Testing
// ============================================================================

fn run_single_invariant(name: &str) -> anyhow::Result<()> {
    match name {
        "transaction_recovery" => invariants::prop_transaction_recovery()?,
        "point_in_time" | "pitr" => invariants::prop_point_in_time_restore()?,
        "wal_batching" => invariants::prop_wal_batching_no_loss()?,
        "production_published_deltas" | "production_deltas" => {
            invariants::prop_production_published_delta_restore()?
        }
        "fenced_delta_restore" | "fenced_deltas" => invariants::prop_fenced_delta_restore()?,
        "snapshot_atomicity" => invariants::prop_snapshot_atomicity()?,
        "txid_monotonicity" => invariants::prop_txid_monotonicity()?,
        "binary_preservation" => invariants::prop_binary_preservation()?,
        "recovery_under_failure" => invariants::prop_recovery_under_failure()?,
        _ => anyhow::bail!(
            "Unknown invariant: {}. Available: transaction_recovery, point_in_time, \
             wal_batching, production_published_deltas, fenced_delta_restore, \
             snapshot_atomicity, txid_monotonicity, binary_preservation, \
             recovery_under_failure",
            name
        ),
    }
    Ok(())
}

fn run_all_invariants() -> anyhow::Result<()> {
    let invariants = [
        (
            "transaction_recovery",
            "Every transaction recoverable from S3",
        ),
        ("point_in_time", "Point-in-time restore gives exact state"),
        ("wal_batching", "WAL batching never loses frames"),
        (
            "production_published_deltas",
            "Production core published deltas restore cleanly",
        ),
        (
            "fenced_delta_restore",
            "Fenced TLM_DELTA followers reconstruct the exact DB",
        ),
        ("snapshot_atomicity", "Snapshots are atomic"),
        ("txid_monotonicity", "TXIDs are monotonic"),
        ("binary_preservation", "Restored DB is byte-identical"),
        ("recovery_under_failure", "Recovery succeeds under failure"),
    ];

    for (name, desc) in &invariants {
        println!("  Running: {} - {}", name, desc);
        match run_single_invariant(name) {
            Ok(_) => println!("  [PASS] {}", name),
            Err(e) => {
                println!("  [FAIL] {}: {}", name, e);
                return Err(e);
            }
        }
    }

    Ok(())
}

#[cfg(test)]
mod threshold_tests {
    use super::{evaluate_soak_result, evaluate_stress_result};

    #[test]
    fn stress_breach_is_hard_failure() {
        // Under threshold: pass.
        assert!(evaluate_stress_result(5.0, 10.0).is_ok());
        // At/over threshold: non-zero exit.
        assert!(evaluate_stress_result(10.0, 10.0).is_err());
        assert!(evaluate_stress_result(42.0, 10.0).is_err());
    }

    #[test]
    fn soak_breach_is_hard_failure() {
        // Healthy run: pass.
        assert!(evaluate_soak_result(3.0, Some(1.0), 4, 50.0, 25.0, 64).is_ok());
        // Sustained memory growth: fail.
        assert!(evaluate_soak_result(80.0, Some(1.0), 4, 50.0, 25.0, 64).is_err());
        // Upward memory trend: fail.
        assert!(evaluate_soak_result(3.0, Some(40.0), 4, 50.0, 25.0, 64).is_err());
        // File-descriptor leak: fail.
        assert!(evaluate_soak_result(3.0, Some(1.0), 128, 50.0, 25.0, 64).is_err());
        // No trend sample must not spuriously fail on the trend axis.
        assert!(evaluate_soak_result(3.0, None, 4, 50.0, 25.0, 64).is_ok());
    }
}
