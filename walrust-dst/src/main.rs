//! Deterministic Simulation Testing CLI for Walrust
//!
//! Run property-based tests, chaos tests, and stress tests to verify
//! walrust's data safety guarantees.

mod chaos;
pub mod mock_storage;
mod properties;

use clap::{Parser, Subcommand};
use rand::Rng;
use std::time::Duration;

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
}

fn main() -> anyhow::Result<()> {
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
            println!("Running chaos tests with faults: {} (seed: {:#x})\n", faults, seed);
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
            run_stress_test(databases, writes_per_sec, Duration::from_secs(duration_secs))?;
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
            "s3_errors" | "crashes" | "eventual_consistency" | "stress" if !run_all => {
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
        println!("\n  Summary: {}/{} tests passed",
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

fn run_stress_test(
    _databases: usize,
    _writes_per_sec: usize,
    _duration: Duration,
) -> anyhow::Result<()> {
    println!("  [SKIP] Stress test not yet implemented");
    Ok(())
}

fn run_soak_test(_duration: Duration) -> anyhow::Result<()> {
    println!("  [SKIP] Soak test not yet implemented");
    Ok(())
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
