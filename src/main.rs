mod config;
mod dashboard;
mod errors;
mod ltx;
mod retention;
mod retry;
mod s3;
mod shadow;
mod storage;
mod sync;
mod wal;
mod webhook;

use anyhow::{anyhow, Result};
use errors::{classify_error, ExitStatus};
use std::process::ExitCode;
use clap::{Parser, Subcommand};
use config::{Config, ResolvedDbConfig, RetentionConfig, SyncConfig};
use std::path::PathBuf;
use std::time::Duration;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

#[derive(Parser)]
#[command(name = "walrust")]
#[command(version)]
#[command(about = "Lightweight SQLite WAL sync to S3/Tigris with data integrity verification")]
#[command(long_about = "Walrust provides production-grade SQLite database backup and replication \
to S3-compatible storage. Features include point-in-time recovery, GFS retention policies, \
Litestream-compatible LTX format, and multi-database support in a single process.")]
struct Cli {
    /// Config file path (checks ./walrust.toml if not specified)
    #[arg(long, global = true)]
    config: Option<PathBuf>,

    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Watch SQLite databases and sync WAL changes to S3
    Watch {
        /// Database files to watch (can be omitted if config file specifies databases)
        databases: Vec<PathBuf>,

        /// S3 bucket (e.g., "s3://my-bucket/prefix")
        #[arg(short, long)]
        bucket: Option<String>,

        /// Snapshot interval in seconds
        #[arg(long)]
        snapshot_interval: Option<u64>,

        /// WAL sync interval in seconds (default: 1)
        /// Batches WAL changes instead of syncing immediately on each write
        #[arg(long)]
        wal_sync_interval: Option<u64>,

        /// S3 endpoint URL (for Tigris/MinIO/etc)
        #[arg(long, env = "AWS_ENDPOINT_URL_S3")]
        endpoint: Option<String>,

        /// Take snapshot after N WAL frames (0 = disabled)
        #[arg(long)]
        max_changes: Option<u64>,

        /// Maximum seconds between snapshots when changes detected
        #[arg(long)]
        max_interval: Option<u64>,

        /// Take snapshot after N seconds of no WAL activity (0 = disabled)
        #[arg(long)]
        on_idle: Option<u64>,

        /// Take snapshot immediately on watch start
        #[arg(long)]
        on_startup: Option<bool>,

        /// Run compaction after each snapshot
        #[arg(long)]
        compact_after_snapshot: bool,

        /// Compaction interval in seconds (0 = disabled)
        #[arg(long)]
        compact_interval: Option<u64>,

        /// Checkpoint interval in seconds (default: 60)
        /// Runs PASSIVE checkpoint periodically to prevent unbounded WAL growth
        #[arg(long)]
        checkpoint_interval: Option<u64>,

        /// Minimum pages before checkpoint (default: 1000, ~4MB)
        #[arg(long)]
        min_checkpoint_pages: Option<u64>,

        /// Emergency WAL truncate threshold in pages (default: 121359, ~500MB)
        /// Set to 0 to disable emergency checkpoints
        #[arg(long)]
        wal_truncate_threshold: Option<u64>,

        /// File watcher check interval in seconds (default: 1)
        /// Debounces trigger checks to reduce CPU on high-write workloads
        #[arg(long)]
        monitor_interval: Option<u64>,

        /// Backup validation interval in seconds (0 = disabled)
        /// Periodically verifies backup integrity by checking LTX checksums
        #[arg(long)]
        validation_interval: Option<u64>,

        /// Number of hourly snapshots to retain
        #[arg(long)]
        retain_hourly: Option<usize>,

        /// Number of daily snapshots to retain
        #[arg(long)]
        retain_daily: Option<usize>,

        /// Number of weekly snapshots to retain
        #[arg(long)]
        retain_weekly: Option<usize>,

        /// Number of monthly snapshots to retain
        #[arg(long)]
        retain_monthly: Option<usize>,

        /// Metrics server port (default: 16767, disable with --no-metrics)
        #[arg(long, default_value = "16767")]
        metrics_port: u16,

        /// Disable metrics server
        #[arg(long)]
        no_metrics: bool,

        // Retry configuration
        /// Maximum retry attempts for S3 operations (default: 5)
        #[arg(long)]
        max_retries: Option<u32>,

        /// Initial backoff delay in milliseconds (default: 100)
        #[arg(long)]
        base_delay_ms: Option<u64>,

        /// Maximum backoff delay in milliseconds (default: 30000)
        #[arg(long)]
        max_delay_ms: Option<u64>,

        /// Disable circuit breaker (default: enabled)
        #[arg(long)]
        no_circuit_breaker: bool,

        /// Failures before circuit breaker opens (default: 10)
        #[arg(long)]
        circuit_breaker_threshold: Option<u32>,

        /// Enable shadow WAL mode (experimental)
        ///
        /// Shadow WAL decouples S3 uploads from SQLite's active WAL file,
        /// providing better performance at high write rates. This matches
        /// Litestream's architecture.
        #[arg(long)]
        shadow_wal: bool,

        /// Enable independent per-DB tasks (experimental)
        ///
        /// Each database gets its own task that independently watches
        /// for WAL changes and syncs to S3. This allows maximum concurrency
        /// across all databases with CPU-bound encoding distributed across
        /// the thread pool.
        #[arg(long)]
        independent_tasks: bool,
    },

    /// Restore a database from S3
    Restore {
        /// Database name (as registered in S3)
        name: String,

        /// Output path for restored database
        #[arg(short, long)]
        output: PathBuf,

        /// S3 bucket
        #[arg(short, long)]
        bucket: String,

        /// S3 endpoint URL
        #[arg(long, env = "AWS_ENDPOINT_URL_S3")]
        endpoint: Option<String>,

        /// Restore to specific point in time (ISO 8601)
        #[arg(long)]
        point_in_time: Option<String>,
    },

    /// List databases in S3 bucket
    List {
        /// S3 bucket
        #[arg(short, long)]
        bucket: String,

        /// S3 endpoint URL
        #[arg(long, env = "AWS_ENDPOINT_URL_S3")]
        endpoint: Option<String>,
    },

    /// Take an immediate snapshot
    Snapshot {
        /// Database file
        database: PathBuf,

        /// S3 bucket
        #[arg(short, long)]
        bucket: String,

        /// S3 endpoint URL
        #[arg(long, env = "AWS_ENDPOINT_URL_S3")]
        endpoint: Option<String>,
    },

    /// Compact old snapshots using retention policy (GFS rotation)
    Compact {
        /// Database name (as registered in S3)
        name: String,

        /// S3 bucket
        #[arg(short, long)]
        bucket: String,

        /// S3 endpoint URL
        #[arg(long, env = "AWS_ENDPOINT_URL_S3")]
        endpoint: Option<String>,

        /// Number of hourly snapshots to keep (default: 24)
        #[arg(long, default_value = "24")]
        hourly: usize,

        /// Number of daily snapshots to keep (default: 7)
        #[arg(long, default_value = "7")]
        daily: usize,

        /// Number of weekly snapshots to keep (default: 12)
        #[arg(long, default_value = "12")]
        weekly: usize,

        /// Number of monthly snapshots to keep (default: 12)
        #[arg(long, default_value = "12")]
        monthly: usize,

        /// Actually delete files (default: dry-run only)
        #[arg(long)]
        force: bool,
    },

    /// Run as a read replica, polling S3 for changes
    Replicate {
        /// Source S3 location (e.g., "s3://bucket/mydb")
        source: String,

        /// Local database path for the replica
        #[arg(long)]
        local: PathBuf,

        /// Poll interval (e.g., "5s", "1m", "30s")
        #[arg(long, default_value = "5s")]
        interval: String,

        /// S3 endpoint URL
        #[arg(long, env = "AWS_ENDPOINT_URL_S3")]
        endpoint: Option<String>,
    },

    /// Show what the current configuration will do without executing
    ///
    /// Displays a summary of: S3 storage settings, snapshot triggers, compaction policy,
    /// retention tiers, and resolved database paths with any per-database overrides.
    Explain,

    /// Verify integrity of backed-up LTX files in S3
    ///
    /// Checks that each LTX file exists, has valid checksums, and maintains
    /// TXID continuity. Use --fix to remove orphaned manifest entries.
    Verify {
        /// Database name (as registered in S3)
        name: String,

        /// S3 bucket
        #[arg(short, long)]
        bucket: String,

        /// S3 endpoint URL
        #[arg(long, env = "AWS_ENDPOINT_URL_S3")]
        endpoint: Option<String>,

        /// Fix issues by removing orphaned manifest entries
        #[arg(long)]
        fix: bool,
    },

    /// Output recommended SQLite PRAGMA settings for optimal walrust performance
    ///
    /// These settings disable auto-checkpointing (walrust manages checkpoints),
    /// enable WAL mode, and optimize for replication workloads.
    Pragma {
        /// Output as SQL file instead of printing to stdout
        #[arg(short, long)]
        output: Option<PathBuf>,

        /// Include comments explaining each setting
        #[arg(long, default_value = "true")]
        comments: bool,
    },
}

/// CLI arguments for Watch command
struct WatchArgs {
    databases: Vec<PathBuf>,
    bucket: Option<String>,
    snapshot_interval: Option<u64>,
    wal_sync_interval: Option<u64>,
    endpoint: Option<String>,
    max_changes: Option<u64>,
    max_interval: Option<u64>,
    on_idle: Option<u64>,
    on_startup: Option<bool>,
    compact_after_snapshot: bool,
    compact_interval: Option<u64>,
    checkpoint_interval: Option<u64>,
    min_checkpoint_pages: Option<u64>,
    wal_truncate_threshold: Option<u64>,
    monitor_interval: Option<u64>,
    validation_interval: Option<u64>,
    retain_hourly: Option<usize>,
    retain_daily: Option<usize>,
    retain_weekly: Option<usize>,
    retain_monthly: Option<usize>,
    metrics_port: u16,
    no_metrics: bool,
    // Retry configuration
    max_retries: Option<u32>,
    base_delay_ms: Option<u64>,
    max_delay_ms: Option<u64>,
    no_circuit_breaker: bool,
    circuit_breaker_threshold: Option<u32>,
    // Shadow WAL mode
    shadow_wal: bool,
    // Independent per-DB tasks mode
    independent_tasks: bool,
}

/// Resolve watch configuration by merging config file with CLI args
fn resolve_watch_config(
    config: &Option<Config>,
    cli: &WatchArgs,
) -> Result<(Vec<ResolvedDbConfig>, String, Option<String>, SyncConfig, RetentionConfig, retry::RetryConfig, Vec<config::WebhookConfig>)> {
    match config {
        Some(cfg) => {
            // Start with config file values, CLI overrides
            let bucket = cli
                .bucket
                .clone()
                .or(cfg.s3.bucket.clone())
                .ok_or_else(|| anyhow!("bucket required (via --bucket or config file)"))?;

            let endpoint = cli.endpoint.clone().or(cfg.s3.endpoint.clone());

            // If CLI specifies databases, use those; otherwise use config
            let resolved_dbs = if !cli.databases.is_empty() {
                // CLI databases - use global config settings with CLI overrides
                let sync = merge_cli_sync_overrides(&cfg.sync, cli);
                let retention = merge_cli_retention_overrides(&cfg.retention, cli);

                cli.databases
                    .iter()
                    .map(|p| ResolvedDbConfig {
                        path: p.clone(),
                        prefix: p
                            .file_stem()
                            .and_then(|s| s.to_str())
                            .unwrap_or("db")
                            .to_string(),
                        sync: sync.clone(),
                        retention: retention.clone(),
                    })
                    .collect()
            } else {
                // Use databases from config file
                let mut dbs = cfg.resolve_databases()?;
                if dbs.is_empty() {
                    return Err(anyhow!(
                        "No databases specified (provide paths or configure in config file)"
                    ));
                }

                // Apply CLI overrides to each database's config
                for db in &mut dbs {
                    db.sync = merge_cli_sync_overrides(&db.sync, cli);
                    db.retention = merge_cli_retention_overrides(&db.retention, cli);
                }
                dbs
            };

            // For global sync/retention, merge CLI overrides with config
            let sync = merge_cli_sync_overrides(&cfg.sync, cli);
            let retention = merge_cli_retention_overrides(&cfg.retention, cli);
            let retry_config = merge_cli_retry_overrides(&cfg.retry, cli);
            let webhooks = cfg.webhooks.clone();

            Ok((resolved_dbs, bucket, endpoint, sync, retention, retry_config, webhooks))
        }
        None => {
            // No config file - require CLI args
            let bucket = cli
                .bucket
                .clone()
                .ok_or_else(|| anyhow!("--bucket is required when no config file is present"))?;

            if cli.databases.is_empty() {
                return Err(anyhow!(
                    "At least one database path required when no config file is present"
                ));
            }

            // Build config from CLI with defaults
            let sync = SyncConfig {
                snapshot_interval: cli.snapshot_interval.unwrap_or(3600),
                wal_sync_interval: cli.wal_sync_interval.unwrap_or(1),
                max_changes: cli.max_changes.unwrap_or(0),
                max_interval: cli.max_interval.unwrap_or(0),
                on_idle: cli.on_idle.unwrap_or(0),
                on_startup: cli.on_startup.unwrap_or(true),
                compact_after_snapshot: cli.compact_after_snapshot,
                compact_interval: cli.compact_interval.unwrap_or(0),
                checkpoint_interval: cli.checkpoint_interval.unwrap_or(60),
                min_checkpoint_page_count: cli.min_checkpoint_pages.unwrap_or(1000),
                wal_truncate_threshold_pages: cli.wal_truncate_threshold.unwrap_or(121359),
                monitor_interval: cli.monitor_interval.unwrap_or(1),
                validation_interval: cli.validation_interval.unwrap_or(0),
            };

            let retention = RetentionConfig {
                hourly: cli.retain_hourly.unwrap_or(24),
                daily: cli.retain_daily.unwrap_or(7),
                weekly: cli.retain_weekly.unwrap_or(12),
                monthly: cli.retain_monthly.unwrap_or(12),
            };

            let retry_config = merge_cli_retry_overrides(&retry::RetryConfig::default(), cli);

            let resolved_dbs = cli
                .databases
                .iter()
                .map(|p| ResolvedDbConfig {
                    path: p.clone(),
                    prefix: p
                        .file_stem()
                        .and_then(|s| s.to_str())
                        .unwrap_or("db")
                        .to_string(),
                    sync: sync.clone(),
                    retention: retention.clone(),
                })
                .collect();

            Ok((
                resolved_dbs,
                bucket,
                cli.endpoint.clone(),
                sync,
                retention,
                retry_config,
                vec![], // No webhooks from CLI-only mode
            ))
        }
    }
}

/// Merge CLI sync overrides with base config
fn merge_cli_sync_overrides(base: &SyncConfig, cli: &WatchArgs) -> SyncConfig {
    SyncConfig {
        snapshot_interval: cli.snapshot_interval.unwrap_or(base.snapshot_interval),
        wal_sync_interval: cli.wal_sync_interval.unwrap_or(base.wal_sync_interval),
        max_changes: cli.max_changes.unwrap_or(base.max_changes),
        max_interval: cli.max_interval.unwrap_or(base.max_interval),
        on_idle: cli.on_idle.unwrap_or(base.on_idle),
        on_startup: cli.on_startup.unwrap_or(base.on_startup),
        compact_after_snapshot: cli.compact_after_snapshot || base.compact_after_snapshot,
        compact_interval: cli.compact_interval.unwrap_or(base.compact_interval),
        checkpoint_interval: cli.checkpoint_interval.unwrap_or(base.checkpoint_interval),
        min_checkpoint_page_count: cli.min_checkpoint_pages.unwrap_or(base.min_checkpoint_page_count),
        wal_truncate_threshold_pages: cli.wal_truncate_threshold.unwrap_or(base.wal_truncate_threshold_pages),
        monitor_interval: cli.monitor_interval.unwrap_or(base.monitor_interval),
        validation_interval: cli.validation_interval.unwrap_or(base.validation_interval),
    }
}

/// Merge CLI retention overrides with base config
fn merge_cli_retention_overrides(base: &RetentionConfig, cli: &WatchArgs) -> RetentionConfig {
    RetentionConfig {
        hourly: cli.retain_hourly.unwrap_or(base.hourly),
        daily: cli.retain_daily.unwrap_or(base.daily),
        weekly: cli.retain_weekly.unwrap_or(base.weekly),
        monthly: cli.retain_monthly.unwrap_or(base.monthly),
    }
}

/// Merge CLI retry overrides with base config
fn merge_cli_retry_overrides(base: &retry::RetryConfig, cli: &WatchArgs) -> retry::RetryConfig {
    retry::RetryConfig {
        max_retries: cli.max_retries.unwrap_or(base.max_retries),
        base_delay_ms: cli.base_delay_ms.unwrap_or(base.base_delay_ms),
        max_delay_ms: cli.max_delay_ms.unwrap_or(base.max_delay_ms),
        circuit_breaker_enabled: !cli.no_circuit_breaker && base.circuit_breaker_enabled,
        circuit_breaker_threshold: cli.circuit_breaker_threshold.unwrap_or(base.circuit_breaker_threshold),
        circuit_breaker_cooldown_ms: base.circuit_breaker_cooldown_ms,
    }
}

/// Parse duration string like "5s", "1m", "30s", "2h"
fn parse_duration(s: &str) -> Result<Duration> {
    let s = s.trim();
    if s.is_empty() {
        return Err(anyhow!("Empty duration string"));
    }

    let (num_str, unit) = if s.ends_with("ms") {
        (&s[..s.len() - 2], "ms")
    } else if s.ends_with('s') {
        (&s[..s.len() - 1], "s")
    } else if s.ends_with('m') {
        (&s[..s.len() - 1], "m")
    } else if s.ends_with('h') {
        (&s[..s.len() - 1], "h")
    } else {
        return Err(anyhow!(
            "Invalid duration '{}'. Use format like '5s', '1m', '2h'",
            s
        ));
    };

    let num: u64 = num_str
        .parse()
        .map_err(|_| anyhow!("Invalid number in duration: {}", num_str))?;

    match unit {
        "ms" => Ok(Duration::from_millis(num)),
        "s" => Ok(Duration::from_secs(num)),
        "m" => Ok(Duration::from_secs(num * 60)),
        "h" => Ok(Duration::from_secs(num * 3600)),
        _ => unreachable!(),
    }
}

/// Main entry point with structured exit codes
///
/// Exit codes:
/// - 0: Success
/// - 1: General/unknown error
/// - 2: Configuration error (invalid config file, missing CLI args)
/// - 3: Database error (file not found, WAL corruption, SQLite issues)
/// - 4: S3 error (network, authentication, bucket access)
/// - 5: Integrity error (checksum mismatch, LTX verification failed)
/// - 6: Restore error (no snapshot found, PITR unavailable)
#[tokio::main]
async fn main() -> ExitCode {
    tracing_subscriber::registry()
        .with(tracing_subscriber::EnvFilter::new(
            std::env::var("RUST_LOG").unwrap_or_else(|_| "walrust=info".into()),
        ))
        .with(tracing_subscriber::fmt::layer())
        .init();

    match run().await {
        Ok(()) => ExitCode::from(ExitStatus::Success),
        Err(err) => {
            let status = classify_error(&err);
            tracing::error!("{:#}", err);
            ExitCode::from(status)
        }
    }
}

/// Run the CLI commands
async fn run() -> Result<()> {
    let cli = Cli::parse();

    // Load config file (optional)
    let config = Config::load(cli.config.as_deref())?;

    match cli.command {
        Commands::Watch {
            databases,
            bucket,
            snapshot_interval,
            wal_sync_interval,
            endpoint,
            max_changes,
            max_interval,
            on_idle,
            on_startup,
            compact_after_snapshot,
            compact_interval,
            checkpoint_interval,
            min_checkpoint_pages,
            wal_truncate_threshold,
            monitor_interval,
            validation_interval,
            retain_hourly,
            retain_daily,
            retain_weekly,
            retain_monthly,
            metrics_port,
            no_metrics,
            max_retries,
            base_delay_ms,
            max_delay_ms,
            no_circuit_breaker,
            circuit_breaker_threshold,
            shadow_wal,
            independent_tasks,
        } => {
            let watch_args = WatchArgs {
                databases,
                bucket,
                snapshot_interval,
                wal_sync_interval,
                endpoint,
                max_changes,
                max_interval,
                on_idle,
                on_startup,
                compact_after_snapshot,
                compact_interval,
                checkpoint_interval,
                min_checkpoint_pages,
                wal_truncate_threshold,
                monitor_interval,
                validation_interval,
                retain_hourly,
                retain_daily,
                retain_weekly,
                retain_monthly,
                metrics_port,
                no_metrics,
                max_retries,
                base_delay_ms,
                max_delay_ms,
                no_circuit_breaker,
                circuit_breaker_threshold,
                shadow_wal,
                independent_tasks,
            };

            let (resolved_dbs, bucket, endpoint, sync_config, retention_config, retry_config, webhooks) =
                resolve_watch_config(&config, &watch_args)?;

            if shadow_wal {
                tracing::info!("Shadow WAL mode enabled (experimental)");
                tracing::warn!("Shadow WAL integration is in progress - using standard mode for now");
                // TODO: Call watch_with_shadow when fully implemented
                // For now, initialize shadow directories for each database
                for db in &resolved_dbs {
                    let shadow_dir = walrust::shadow::ShadowWal::shadow_dir_for(&db.path);
                    if let Err(e) = std::fs::create_dir_all(&shadow_dir) {
                        tracing::warn!("Could not create shadow dir {}: {}", shadow_dir.display(), e);
                    } else {
                        tracing::debug!("Shadow WAL directory: {}", shadow_dir.display());
                    }
                }
            }

            let compact_policy =
                if sync_config.compact_after_snapshot || sync_config.compact_interval > 0 {
                    Some(retention::RetentionPolicy::new(
                        retention_config.hourly,
                        retention_config.daily,
                        retention_config.weekly,
                        retention_config.monthly,
                    ))
                } else {
                    None
                };

            // Choose sync mode based on flags
            if independent_tasks {
                tracing::info!("Independent tasks mode enabled (experimental)");
                sync::watch_with_independent_tasks(
                    resolved_dbs,
                    &bucket,
                    endpoint.as_deref(),
                    sync_config,
                    compact_policy,
                    watch_args.metrics_port,
                    watch_args.no_metrics,
                    retry_config,
                    webhooks,
                )
                .await?;
            } else {
                sync::watch_with_config(
                    resolved_dbs,
                    &bucket,
                    endpoint.as_deref(),
                    sync_config,
                    compact_policy,
                    watch_args.metrics_port,
                    watch_args.no_metrics,
                    retry_config,
                    webhooks,
                )
                .await?;
            }
        }
        Commands::Restore {
            name,
            output,
            bucket,
            endpoint,
            point_in_time,
        } => {
            sync::restore(&name, &output, &bucket, endpoint.as_deref(), point_in_time.as_deref()).await?;
        }
        Commands::List { bucket, endpoint } => {
            sync::list(&bucket, endpoint.as_deref()).await?;
        }
        Commands::Snapshot {
            database,
            bucket,
            endpoint,
        } => {
            sync::snapshot(&database, &bucket, endpoint.as_deref()).await?;
        }
        Commands::Compact {
            name,
            bucket,
            endpoint,
            hourly,
            daily,
            weekly,
            monthly,
            force,
        } => {
            let policy = retention::RetentionPolicy::new(hourly, daily, weekly, monthly);
            sync::compact(&name, &bucket, endpoint.as_deref(), &policy, force).await?;
        }

        Commands::Replicate {
            source,
            local,
            interval,
            endpoint,
        } => {
            let duration = parse_duration(&interval)?;
            sync::replicate(&source, &local, duration, endpoint.as_deref()).await?;
        }

        Commands::Explain => {
            sync::explain(&config)?;
        }

        Commands::Verify {
            name,
            bucket,
            endpoint,
            fix,
        } => {
            sync::verify(&name, &bucket, endpoint.as_deref(), fix).await?;
        }

        Commands::Pragma { output, comments } => {
            let pragma_sql = generate_pragma_sql(comments);

            if let Some(path) = output {
                std::fs::write(&path, &pragma_sql)?;
                println!("Wrote PRAGMA settings to {}", path.display());
            } else {
                println!("{}", pragma_sql);
            }
        }
    }

    Ok(())
}

/// Generate recommended PRAGMA SQL for walrust
fn generate_pragma_sql(with_comments: bool) -> String {
    let mut sql = String::new();

    if with_comments {
        sql.push_str("-- Recommended SQLite PRAGMA settings for walrust\n");
        sql.push_str("-- Run these once when creating your database, or on every connection\n\n");

        sql.push_str("-- Enable WAL mode (required for walrust)\n");
    }
    sql.push_str("PRAGMA journal_mode=WAL;\n");

    if with_comments {
        sql.push_str("\n-- Disable auto-checkpointing (walrust manages checkpoints)\n");
        sql.push_str("-- This prevents checkpoint contention and ensures walrust captures all WAL frames\n");
    }
    sql.push_str("PRAGMA wal_autocheckpoint=0;\n");

    if with_comments {
        sql.push_str("\n-- Use NORMAL synchronous mode for better performance\n");
        sql.push_str("-- WAL mode + walrust provides durability guarantees\n");
    }
    sql.push_str("PRAGMA synchronous=NORMAL;\n");

    if with_comments {
        sql.push_str("\n-- Optional: Set page size (must be done before any tables are created)\n");
        sql.push_str("-- 4096 is a good default, 8192 or 16384 can improve large row performance\n");
    }
    sql.push_str("PRAGMA page_size=4096;\n");

    if with_comments {
        sql.push_str("\n-- Optional: Increase cache size for better read performance\n");
        sql.push_str("-- Negative value = KB, so -64000 = ~64MB cache\n");
    }
    sql.push_str("PRAGMA cache_size=-64000;\n");

    if with_comments {
        sql.push_str("\n-- Optional: Enable memory-mapped I/O for better read performance\n");
        sql.push_str("-- Set to desired size in bytes (256MB shown)\n");
    }
    sql.push_str("PRAGMA mmap_size=268435456;\n");

    sql
}

#[cfg(test)]
mod tests {
    use super::*;

    fn default_watch_args() -> WatchArgs {
        WatchArgs {
            databases: vec![],
            bucket: None,
            snapshot_interval: None,
            wal_sync_interval: None,
            endpoint: None,
            max_changes: None,
            max_interval: None,
            on_idle: None,
            on_startup: None,
            compact_after_snapshot: false,
            compact_interval: None,
            checkpoint_interval: None,
            min_checkpoint_pages: None,
            wal_truncate_threshold: None,
            monitor_interval: None,
            validation_interval: None,
            retain_hourly: None,
            retain_daily: None,
            retain_weekly: None,
            retain_monthly: None,
            metrics_port: 16767,
            no_metrics: false,
            max_retries: None,
            base_delay_ms: None,
            max_delay_ms: None,
            no_circuit_breaker: false,
            circuit_breaker_threshold: None,
            shadow_wal: false,
            independent_tasks: false,
        }
    }

    #[test]
    fn test_merge_cli_retry_overrides_defaults() {
        let base = retry::RetryConfig::default();
        let cli = default_watch_args();

        let result = merge_cli_retry_overrides(&base, &cli);

        assert_eq!(result.max_retries, 5);
        assert_eq!(result.base_delay_ms, 100);
        assert_eq!(result.max_delay_ms, 30000);
        assert!(result.circuit_breaker_enabled);
        assert_eq!(result.circuit_breaker_threshold, 10);
    }

    #[test]
    fn test_merge_cli_retry_overrides_cli_values() {
        let base = retry::RetryConfig::default();
        let mut cli = default_watch_args();
        cli.max_retries = Some(10);
        cli.base_delay_ms = Some(200);
        cli.max_delay_ms = Some(60000);
        cli.circuit_breaker_threshold = Some(20);

        let result = merge_cli_retry_overrides(&base, &cli);

        assert_eq!(result.max_retries, 10);
        assert_eq!(result.base_delay_ms, 200);
        assert_eq!(result.max_delay_ms, 60000);
        assert!(result.circuit_breaker_enabled);
        assert_eq!(result.circuit_breaker_threshold, 20);
    }

    #[test]
    fn test_merge_cli_retry_overrides_disable_circuit_breaker() {
        let base = retry::RetryConfig::default();
        let mut cli = default_watch_args();
        cli.no_circuit_breaker = true;

        let result = merge_cli_retry_overrides(&base, &cli);

        assert!(!result.circuit_breaker_enabled);
    }

    #[test]
    fn test_merge_cli_retry_overrides_partial() {
        let base = retry::RetryConfig::default();
        let mut cli = default_watch_args();
        cli.max_retries = Some(3); // Only override max_retries

        let result = merge_cli_retry_overrides(&base, &cli);

        assert_eq!(result.max_retries, 3);
        assert_eq!(result.base_delay_ms, 100); // Default
        assert_eq!(result.max_delay_ms, 30000); // Default
    }
}
