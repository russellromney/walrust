//! Configuration file support for walrust.
//!
//! Loads walrust.toml from current directory with CLI override support.

use crate::errors::WalrustError;
use anyhow::Result;
use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};

// Re-export shared config types from hadb-io
pub use hadb_io::config::{S3Config, WebhookConfig};

/// Root configuration structure
#[derive(Debug, Clone, Deserialize, Serialize, Default)]
#[serde(deny_unknown_fields)]
pub struct Config {
    /// S3/storage configuration
    #[serde(default)]
    pub s3: S3Config,

    /// Global sync trigger settings
    #[serde(default)]
    pub sync: SyncConfig,

    /// Global retention policy
    #[serde(default)]
    pub retention: RetentionConfig,

    /// Mandatory native-HADBP local spool used by default shadow watch.
    #[serde(default)]
    pub spool: SpoolConfig,

    /// Retry configuration for transient failures
    #[serde(default)]
    pub retry: crate::retry::RetryConfig,

    /// Webhook configuration for failure notifications
    #[serde(default)]
    pub webhooks: Vec<WebhookConfig>,

    /// Leveled compaction (experimental, off by default)
    #[serde(default)]
    pub compaction: CompactionConfig,

    /// Database-specific configurations
    #[serde(default)]
    pub databases: Vec<DatabaseConfig>,

    /// Allow glob patterns that match no databases to silently skip instead of
    /// failing startup. Default false: a matched-nothing glob is a startup error.
    #[serde(default)]
    pub allow_empty_globs: bool,
}

/// Sync trigger configuration
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct SyncConfig {
    /// Snapshot interval in seconds (default: 3600)
    #[serde(default = "default_snapshot_interval")]
    pub snapshot_interval: u64,

    /// WAL sync interval in seconds (default: 1)
    /// Batches WAL changes instead of syncing immediately on each write
    #[serde(default = "default_wal_sync_interval")]
    pub wal_sync_interval: u64,

    /// Take snapshot after N WAL frames (0 = disabled)
    #[serde(default)]
    pub max_changes: u64,

    /// Maximum seconds between snapshots when changes detected
    #[serde(default)]
    pub max_interval: u64,

    /// Take snapshot after N seconds of no WAL activity (0 = disabled)
    #[serde(default)]
    pub on_idle: u64,

    /// Take snapshot immediately on watch start
    #[serde(default = "default_on_startup")]
    pub on_startup: bool,

    /// Run retention pruning after each snapshot.
    #[serde(default)]
    pub prune_after_snapshot: bool,

    /// Retention pruning interval in seconds (0 = disabled).
    #[serde(default)]
    pub prune_interval: u64,

    /// Checkpoint interval in seconds (default: 60)
    /// Runs PRAGMA wal_checkpoint(PASSIVE) on interval
    #[serde(default = "default_checkpoint_interval")]
    pub checkpoint_interval: u64,

    /// Minimum page count before running checkpoint (default: 1000)
    /// Only checkpoint if WAL has this many pages (~4MB)
    #[serde(default = "default_min_checkpoint_pages")]
    pub min_checkpoint_page_count: u64,

    /// Emergency WAL size threshold in pages (default: 121359 = ~500MB)
    /// Triggers blocking TRUNCATE checkpoint. Set to 0 to disable.
    #[serde(default = "default_truncate_threshold")]
    pub wal_truncate_threshold_pages: u64,

    /// Automated validation interval in seconds (default: 0 = disabled)
    /// Periodically verifies native HADBP checksums and published-chain continuity.
    /// Recommended: 86400 (daily) for production. Warning: downloads metadata from S3.
    #[serde(default)]
    pub validation_interval: u64,

    /// Boundary required before walrust releases its checkpoint blocker.
    #[serde(default)]
    pub checkpoint_release: CheckpointRelease,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize, clap::ValueEnum, Default)]
#[serde(rename_all = "lowercase")]
pub enum CheckpointRelease {
    /// Fsynced native HADBP payload plus matching local journal record.
    #[default]
    Local,
    /// Local admission plus contiguous remote publish confirmation.
    Remote,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct SpoolConfig {
    /// Root directory. A collision-safe per-stream directory is created below it.
    pub path: Option<PathBuf>,
    /// Warning watermark in bytes.
    #[serde(default = "default_spool_warning_size")]
    pub warning_size: u64,
    /// Hard spool capacity in bytes. Pending objects are never evicted.
    #[serde(default = "default_spool_max_size")]
    pub max_size: u64,
    /// Required free bytes on the actual spool filesystem.
    #[serde(default = "default_spool_min_free_space")]
    pub min_free_space: u64,
    /// Optional bounded cloud drain on graceful shutdown.
    #[serde(default = "default_spool_shutdown_drain")]
    pub shutdown_drain_seconds: u64,
}

impl Default for SpoolConfig {
    fn default() -> Self {
        Self {
            path: None,
            warning_size: default_spool_warning_size(),
            max_size: default_spool_max_size(),
            min_free_space: default_spool_min_free_space(),
            shutdown_drain_seconds: default_spool_shutdown_drain(),
        }
    }
}

fn default_spool_warning_size() -> u64 {
    4 * 1024 * 1024 * 1024
}
fn default_spool_max_size() -> u64 {
    5 * 1024 * 1024 * 1024
}
fn default_spool_min_free_space() -> u64 {
    1024 * 1024 * 1024
}
fn default_spool_shutdown_drain() -> u64 {
    10
}

impl Default for SyncConfig {
    fn default() -> Self {
        Self {
            snapshot_interval: 3600,
            wal_sync_interval: 1, // PERF: Set to 0.5 for aggressive sync (500ms batching)
            max_changes: 0,
            max_interval: 0,
            on_idle: 0,
            on_startup: true,
            prune_after_snapshot: false,
            prune_interval: 0,
            checkpoint_interval: 60,
            min_checkpoint_page_count: 1000,
            wal_truncate_threshold_pages: 121359,
            validation_interval: 0,
            checkpoint_release: CheckpointRelease::Local,
        }
    }
}

fn default_snapshot_interval() -> u64 {
    3600
}
fn default_wal_sync_interval() -> u64 {
    1 // PERF: Default 1s, use walrust.toml wal_sync_interval=0.5 for aggressive batching
}
fn default_on_startup() -> bool {
    true
}
fn default_checkpoint_interval() -> u64 {
    60
}
fn default_min_checkpoint_pages() -> u64 {
    1000
}
fn default_truncate_threshold() -> u64 {
    121359
}

/// Retention policy configuration
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct RetentionConfig {
    #[serde(default = "default_hourly")]
    pub hourly: usize,
    #[serde(default = "default_daily")]
    pub daily: usize,
    #[serde(default = "default_weekly")]
    pub weekly: usize,
    #[serde(default = "default_monthly")]
    pub monthly: usize,
}

impl Default for RetentionConfig {
    fn default() -> Self {
        Self {
            hourly: 24,
            daily: 7,
            weekly: 12,
            monthly: 12,
        }
    }
}

/// Leveled-compaction configuration (`[compaction]` in `walrust.toml`).
///
/// **Experimental, off by default.** Old walrust binaries cannot restore a
/// leveled bucket, so this ships dark (`enabled = false`); flip it a release
/// after every reader understands levels. See the README's Compaction section.
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct CompactionConfig {
    /// Master switch. Default false (ship-dark / version-skew safety).
    #[serde(default)]
    pub enabled: bool,
    /// L0 objects younger than this are never merged ("restore to the second
    /// within this window"). Duration string, e.g. "1h", "30m". Default "1h".
    #[serde(default = "default_keep_fine_window")]
    pub keep_fine_window: String,
    /// L0 objects folded per L0→L1 merge. Default 60.
    #[serde(default = "default_l1_batch")]
    pub l1_batch: usize,
    /// L1 objects folded per L1→L2 merge. Default 24.
    #[serde(default = "default_l2_batch")]
    pub l2_batch: usize,
}

impl Default for CompactionConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            keep_fine_window: default_keep_fine_window(),
            l1_batch: default_l1_batch(),
            l2_batch: default_l2_batch(),
        }
    }
}

fn default_keep_fine_window() -> String {
    "1h".to_string()
}
fn default_l1_batch() -> usize {
    60
}
fn default_l2_batch() -> usize {
    24
}

impl CompactionConfig {
    /// Resolve into the core [`CompactionSettings`]. A malformed
    /// `keep_fine_window` falls back to one hour.
    pub fn to_settings(&self) -> walrust_core::compaction::CompactionSettings {
        let keep_fine_window = parse_compaction_duration(&self.keep_fine_window)
            .unwrap_or_else(|| std::time::Duration::from_secs(3600));
        walrust_core::compaction::CompactionSettings {
            enabled: self.enabled,
            keep_fine_window,
            l1_batch: self.l1_batch,
            l2_batch: self.l2_batch,
        }
    }
}

/// Parse a compact duration string like "1h", "30m", "3600s", "500ms".
fn parse_compaction_duration(s: &str) -> Option<std::time::Duration> {
    let s = s.trim();
    let (num, unit) = if let Some(v) = s.strip_suffix("ms") {
        (v, "ms")
    } else if let Some(v) = s.strip_suffix('s') {
        (v, "s")
    } else if let Some(v) = s.strip_suffix('m') {
        (v, "m")
    } else if let Some(v) = s.strip_suffix('h') {
        (v, "h")
    } else {
        (s, "s")
    };
    let n: u64 = num.trim().parse().ok()?;
    Some(match unit {
        "ms" => std::time::Duration::from_millis(n),
        "s" => std::time::Duration::from_secs(n),
        "m" => std::time::Duration::from_secs(n * 60),
        "h" => std::time::Duration::from_secs(n * 3600),
        _ => return None,
    })
}

fn default_hourly() -> usize {
    24
}
fn default_daily() -> usize {
    7
}
fn default_weekly() -> usize {
    12
}
fn default_monthly() -> usize {
    12
}

/// Per-database configuration
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct DatabaseConfig {
    /// Path to database file (supports wildcards: /data/*.db)
    pub path: String,

    /// S3 prefix for this database (default: filename stem)
    pub prefix: Option<String>,

    /// Override snapshot interval for this database
    pub snapshot_interval: Option<u64>,

    /// Override WAL sync interval for this database
    pub wal_sync_interval: Option<u64>,

    /// Override max_changes for this database
    pub max_changes: Option<u64>,

    /// Override max_interval for this database
    pub max_interval: Option<u64>,

    /// Override on_idle for this database
    pub on_idle: Option<u64>,

    /// Override checkpoint_interval for this database
    pub checkpoint_interval: Option<u64>,

    /// Override min_checkpoint_page_count for this database
    pub min_checkpoint_page_count: Option<u64>,

    /// Override wal_truncate_threshold_pages for this database
    pub wal_truncate_threshold_pages: Option<u64>,

    /// Override validation_interval for this database
    pub validation_interval: Option<u64>,

    /// Override retention policy for this database
    pub retention: Option<RetentionConfig>,
}

/// Resolved configuration for a single database (after merging)
#[derive(Debug, Clone)]
pub struct ResolvedDbConfig {
    pub path: PathBuf,
    pub prefix: String,
    pub sync: SyncConfig,
    pub retention: RetentionConfig,
    /// Resolved leveled-compaction settings (global; experimental, off by default).
    pub compaction: walrust_core::compaction::CompactionSettings,
}

impl Config {
    /// Load config from file, or return None if not found.
    ///
    /// If `path` is Some, load from that path.
    /// If `path` is None, check for ./walrust.toml in current directory.
    /// Returns Ok(None) if no config file exists (backward compat).
    pub fn load(path: Option<&Path>) -> Result<Option<Self>> {
        let config_path = match path {
            Some(p) => {
                // Explicit path provided - must exist
                if !p.exists() {
                    return Err(WalrustError::config(format!(
                        "Config file not found: {}",
                        p.display()
                    ))
                    .into());
                }
                p.to_path_buf()
            }
            None => {
                // Check for ./walrust.toml in current directory
                let default_path = PathBuf::from("./walrust.toml");
                if !default_path.exists() {
                    return Ok(None);
                }
                default_path
            }
        };

        let content = std::fs::read_to_string(&config_path).map_err(|e| {
            WalrustError::config(format!("Failed to read {}: {}", config_path.display(), e))
        })?;

        let config: Config = toml::from_str(&content).map_err(|e| {
            WalrustError::config(format!("Failed to parse {}: {}", config_path.display(), e))
        })?;

        config.validate()?;

        tracing::info!("Loaded config from {}", config_path.display());
        Ok(Some(config))
    }

    /// Validate configuration
    fn validate(&self) -> Result<()> {
        // Validate global retention policy
        if self.retention.hourly == 0
            && self.retention.daily == 0
            && self.retention.weekly == 0
            && self.retention.monthly == 0
        {
            return Err(WalrustError::config("[retention]: at least one tier must be > 0").into());
        }

        // Validate S3 bucket format if specified
        if let Some(ref bucket) = self.s3.bucket {
            if bucket.is_empty() {
                return Err(WalrustError::config("[s3].bucket cannot be empty string").into());
            }
            // Bucket should not contain spaces or invalid characters
            if bucket.contains(' ') {
                return Err(WalrustError::config(format!(
                    "[s3].bucket cannot contain spaces: '{}'",
                    bucket
                ))
                .into());
            }
        }

        for (i, db) in self.databases.iter().enumerate() {
            if db.path.is_empty() {
                return Err(
                    WalrustError::config(format!("databases[{}].path cannot be empty", i)).into(),
                );
            }

            // Validate retention values if overridden
            if let Some(ref ret) = db.retention {
                if ret.hourly == 0 && ret.daily == 0 && ret.weekly == 0 && ret.monthly == 0 {
                    return Err(WalrustError::config(format!(
                        "databases[{}].retention: at least one tier must be > 0",
                        i
                    ))
                    .into());
                }
            }
        }
        Ok(())
    }

    /// Expand wildcards and resolve all database configurations
    pub fn resolve_databases(&self) -> Result<Vec<ResolvedDbConfig>> {
        let mut resolved = Vec::new();

        for db_config in &self.databases {
            let paths = expand_glob(&db_config.path)?;

            if paths.is_empty() {
                if self.allow_empty_globs {
                    tracing::warn!("No databases found matching: {}", db_config.path);
                    continue;
                }
                return Err(WalrustError::config(format!(
                    "No databases found matching: {} (set allow_empty_globs=true to permit optional globs)",
                    db_config.path
                ))
                .into());
            }

            for path in paths {
                let prefix = db_config.prefix.clone().unwrap_or_else(|| {
                    path.file_stem()
                        .and_then(|s| s.to_str())
                        .unwrap_or("unknown")
                        .to_string()
                });

                // Merge per-db settings with global defaults
                let sync = self.merge_sync_config(db_config);
                let retention = db_config
                    .retention
                    .clone()
                    .unwrap_or_else(|| self.retention.clone());

                resolved.push(ResolvedDbConfig {
                    path,
                    prefix,
                    sync,
                    retention,
                    compaction: self.compaction.to_settings(),
                });
            }
        }

        Ok(resolved)
    }

    /// Merge per-database sync overrides with global sync config
    fn merge_sync_config(&self, db: &DatabaseConfig) -> SyncConfig {
        let mut sync = self.sync.clone();

        // Apply per-database overrides
        if let Some(interval) = db.snapshot_interval {
            sync.snapshot_interval = interval;
        }
        if let Some(interval) = db.wal_sync_interval {
            sync.wal_sync_interval = interval;
        }
        if let Some(v) = db.max_changes {
            sync.max_changes = v;
        }
        if let Some(v) = db.max_interval {
            sync.max_interval = v;
        }
        if let Some(v) = db.on_idle {
            sync.on_idle = v;
        }
        if let Some(v) = db.checkpoint_interval {
            sync.checkpoint_interval = v;
        }
        if let Some(v) = db.min_checkpoint_page_count {
            sync.min_checkpoint_page_count = v;
        }
        if let Some(v) = db.wal_truncate_threshold_pages {
            sync.wal_truncate_threshold_pages = v;
        }
        if let Some(v) = db.validation_interval {
            sync.validation_interval = v;
        }

        sync
    }
}

/// Expand glob patterns to actual file paths
fn expand_glob(pattern: &str) -> Result<Vec<PathBuf>> {
    // Check if pattern contains wildcards
    if !pattern.contains('*') && !pattern.contains('?') && !pattern.contains('[') {
        // No wildcards - treat as literal path
        let path = PathBuf::from(pattern);
        if path.exists() {
            return Ok(vec![path]);
        } else {
            return Err(WalrustError::database(format!("Database not found: {}", pattern)).into());
        }
    }

    // Expand glob pattern
    let paths: Vec<PathBuf> = glob::glob(pattern)
        .map_err(|e| WalrustError::config(format!("Invalid glob pattern '{}': {}", pattern, e)))?
        .filter_map(|entry| entry.ok())
        .filter(|p| p.is_file())
        .collect();

    Ok(paths)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_minimal_config() {
        let toml = r#"
            [s3]
            bucket = "s3://test-bucket"

            [[databases]]
            path = "/data/test.db"
        "#;
        let config: Config = toml::from_str(toml).unwrap();
        assert_eq!(config.s3.bucket, Some("s3://test-bucket".to_string()));
        assert_eq!(config.databases.len(), 1);
        assert_eq!(config.databases[0].path, "/data/test.db");
    }

    #[test]
    fn test_parse_full_config() {
        let toml = r#"
            [s3]
            bucket = "s3://backups/prod"
            endpoint = "https://fly.storage.tigris.dev"

            [sync]
            snapshot_interval = 1800
            max_changes = 100
            max_interval = 300
            on_idle = 60
            on_startup = false
            prune_after_snapshot = true
            prune_interval = 7200

            [retention]
            hourly = 12
            daily = 5
            weekly = 8
            monthly = 6

            [[databases]]
            path = "/var/lib/app.db"
            prefix = "main"

            [[databases]]
            path = "/data/tenant-*.db"
            prefix = "tenants"
            snapshot_interval = 900
            max_changes = 50

            [[databases]]
            path = "/data/analytics.db"
            prefix = "analytics"
            retention = { hourly = 6, daily = 3, weekly = 4, monthly = 3 }
        "#;

        let config: Config = toml::from_str(toml).unwrap();

        // Check S3 config
        assert_eq!(config.s3.bucket, Some("s3://backups/prod".to_string()));
        assert_eq!(
            config.s3.endpoint,
            Some("https://fly.storage.tigris.dev".to_string())
        );

        // Check sync config
        assert_eq!(config.sync.snapshot_interval, 1800);
        assert_eq!(config.sync.max_changes, 100);
        assert_eq!(config.sync.max_interval, 300);
        assert_eq!(config.sync.on_idle, 60);
        assert!(!config.sync.on_startup);
        assert!(config.sync.prune_after_snapshot);
        assert_eq!(config.sync.prune_interval, 7200);

        // Check retention config
        assert_eq!(config.retention.hourly, 12);
        assert_eq!(config.retention.daily, 5);
        assert_eq!(config.retention.weekly, 8);
        assert_eq!(config.retention.monthly, 6);

        // Check databases
        assert_eq!(config.databases.len(), 3);
        assert_eq!(config.databases[0].prefix, Some("main".to_string()));
        assert_eq!(config.databases[1].snapshot_interval, Some(900));
        assert_eq!(config.databases[1].max_changes, Some(50));
        assert_eq!(config.databases[2].retention.as_ref().unwrap().hourly, 6);
    }

    #[test]
    fn test_defaults_applied() {
        let toml = r#"
            [[databases]]
            path = "/data/test.db"
        "#;
        let config: Config = toml::from_str(toml).unwrap();

        // Check sync defaults
        assert_eq!(config.sync.snapshot_interval, 3600);
        assert_eq!(config.sync.max_changes, 0);
        assert_eq!(config.sync.max_interval, 0);
        assert_eq!(config.sync.on_idle, 0);
        assert!(config.sync.on_startup);
        assert!(!config.sync.prune_after_snapshot);
        assert_eq!(config.sync.prune_interval, 0);
        assert_eq!(config.sync.validation_interval, 0);

        // Check retention defaults
        assert_eq!(config.retention.hourly, 24);
        assert_eq!(config.retention.daily, 7);
        assert_eq!(config.retention.weekly, 12);
        assert_eq!(config.retention.monthly, 12);
    }

    #[test]
    fn test_prune_keys_parse() {
        let prune_toml = r#"
            [sync]
            prune_after_snapshot = true
            prune_interval = 4242

            [[databases]]
            path = "/data/test.db"
        "#;
        let config: Config = toml::from_str(prune_toml).unwrap();
        assert!(config.sync.prune_after_snapshot);
        assert_eq!(config.sync.prune_interval, 4242);
    }

    #[test]
    fn test_validation_empty_path() {
        let toml = r#"
            [[databases]]
            path = ""
        "#;
        let config: Config = toml::from_str(toml).unwrap();
        assert!(config.validate().is_err());
    }

    #[test]
    fn test_validation_zero_retention() {
        let toml = r#"
            [[databases]]
            path = "/data/test.db"
            retention = { hourly = 0, daily = 0, weekly = 0, monthly = 0 }
        "#;
        let config: Config = toml::from_str(toml).unwrap();
        assert!(config.validate().is_err());
    }

    #[test]
    fn test_merge_sync_config() {
        let toml = r#"
            [sync]
            snapshot_interval = 3600
            max_changes = 100
            max_interval = 300
            on_idle = 60

            [[databases]]
            path = "/data/test.db"
            snapshot_interval = 1800
            max_changes = 50
        "#;
        let config: Config = toml::from_str(toml).unwrap();

        let merged = config.merge_sync_config(&config.databases[0]);

        // Overridden values
        assert_eq!(merged.snapshot_interval, 1800);
        assert_eq!(merged.max_changes, 50);

        // Inherited values
        assert_eq!(merged.max_interval, 300);
        assert_eq!(merged.on_idle, 60);
    }

    #[test]
    fn test_deny_unknown_fields_top_level() {
        // Root Config has deny_unknown_fields — top-level unknowns rejected
        let toml = r#"
            unknown_section = "should fail"

            [[databases]]
            path = "/data/test.db"
        "#;
        let result: Result<Config, _> = toml::from_str(toml);
        assert!(result.is_err());
    }

    #[test]
    fn test_expand_glob_literal_nonexistent() {
        let result = expand_glob("/nonexistent/path/to/database.db");
        assert!(result.is_err());
    }

    #[test]
    fn test_empty_glob_is_error_by_default() {
        let toml = r#"
            [[databases]]
            path = "/nonexistent/*.db"
        "#;
        let config: Config = toml::from_str(toml).unwrap();
        let err = config.resolve_databases().unwrap_err().to_string();
        assert!(
            err.contains("No databases found matching"),
            "expected empty-glob error, got: {err}"
        );
        assert!(
            err.contains("allow_empty_globs"),
            "error should mention opt-out: {err}"
        );
    }

    #[test]
    fn test_empty_glob_allowed_with_opt_in() {
        let toml = r#"
            allow_empty_globs = true

            [[databases]]
            path = "/nonexistent/*.db"
        "#;
        let config: Config = toml::from_str(toml).unwrap();
        let resolved = config.resolve_databases().unwrap();
        assert!(
            resolved.is_empty(),
            "optional glob should resolve to no databases"
        );
    }

    #[test]
    fn test_validation_global_zero_retention() {
        // Global retention with all zeros should fail validation
        let toml = r#"
            [retention]
            hourly = 0
            daily = 0
            weekly = 0
            monthly = 0

            [[databases]]
            path = "/data/test.db"
        "#;
        let config: Config = toml::from_str(toml).unwrap();
        let result = config.validate();
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("[retention]"));
    }

    #[test]
    fn test_validation_empty_bucket_string() {
        let toml = r#"
            [s3]
            bucket = ""

            [[databases]]
            path = "/data/test.db"
        "#;
        let config: Config = toml::from_str(toml).unwrap();
        let result = config.validate();
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("bucket"));
    }

    #[test]
    fn test_validation_bucket_with_spaces() {
        let toml = r#"
            [s3]
            bucket = "s3://my bucket/prefix"

            [[databases]]
            path = "/data/test.db"
        "#;
        let config: Config = toml::from_str(toml).unwrap();
        let result = config.validate();
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("spaces"));
    }

    #[test]
    fn test_validation_valid_config() {
        let toml = r#"
            [s3]
            bucket = "s3://valid-bucket/prefix"
            endpoint = "https://fly.storage.tigris.dev"

            [retention]
            hourly = 24
            daily = 7

            [[databases]]
            path = "/data/test.db"
        "#;
        let config: Config = toml::from_str(toml).unwrap();
        let result = config.validate();
        assert!(result.is_ok());
    }

    #[test]
    fn test_validation_partial_retention_ok() {
        // Having just one retention tier > 0 should be valid
        let toml = r#"
            [retention]
            hourly = 1
            daily = 0
            weekly = 0
            monthly = 0

            [[databases]]
            path = "/data/test.db"
        "#;
        let config: Config = toml::from_str(toml).unwrap();
        let result = config.validate();
        assert!(result.is_ok());
    }

    #[test]
    fn test_validation_interval_default() {
        let toml = r#"
            [[databases]]
            path = "/data/test.db"
        "#;
        let config: Config = toml::from_str(toml).unwrap();
        assert_eq!(config.sync.validation_interval, 0);
    }

    #[test]
    fn test_validation_interval_override() {
        let toml = r#"
            [sync]
            validation_interval = 86400

            [[databases]]
            path = "/data/test.db"
        "#;
        let config: Config = toml::from_str(toml).unwrap();
        assert_eq!(config.sync.validation_interval, 86400);
    }

    #[test]
    fn test_per_db_validation_interval() {
        let toml = r#"
            [sync]
            validation_interval = 0

            [[databases]]
            path = "/data/test.db"
            validation_interval = 3600
        "#;
        let config: Config = toml::from_str(toml).unwrap();
        let merged = config.merge_sync_config(&config.databases[0]);
        assert_eq!(merged.validation_interval, 3600);
    }

    #[test]
    fn test_compaction_defaults_off() {
        let toml = r#"
            [[databases]]
            path = "/data/test.db"
        "#;
        let config: Config = toml::from_str(toml).unwrap();
        assert!(
            !config.compaction.enabled,
            "compaction ships off by default"
        );
        let settings = config.compaction.to_settings();
        assert!(!settings.enabled);
        assert_eq!(
            settings.keep_fine_window,
            std::time::Duration::from_secs(3600)
        );
        assert_eq!(settings.l1_batch, 60);
        assert_eq!(settings.l2_batch, 24);
    }

    #[test]
    fn test_compaction_section_parses_and_resolves() {
        let toml = r#"
            [compaction]
            enabled = true
            keep_fine_window = "30m"
            l1_batch = 12
            l2_batch = 6

            [[databases]]
            path = "/data/test.db"
        "#;
        let config: Config = toml::from_str(toml).unwrap();
        assert!(config.compaction.enabled);
        let settings = config.compaction.to_settings();
        assert!(settings.enabled);
        assert_eq!(
            settings.keep_fine_window,
            std::time::Duration::from_secs(1800)
        );
        assert_eq!(settings.l1_batch, 12);
        assert_eq!(settings.l2_batch, 6);
    }
}
