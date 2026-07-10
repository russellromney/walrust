use anyhow::Result;

use crate::config::Config;

/// Explain what the current configuration will do without running
///
/// Loads the config file and prints a human-readable summary of:
/// - Databases being watched (resolved from config/globs)
/// - Snapshot triggers (interval, max_changes, on_idle, on_startup)
/// - Pruning settings if enabled
/// - Retention policy tiers
/// - S3 bucket and endpoint
pub fn explain(config: &Option<Config>) -> Result<()> {
    match config {
        None => {
            println!("No configuration file found.");
            println!();
            println!("walrust looks for ./walrust.toml in the current directory,");
            println!("or you can specify a config file with --config <path>.");
            println!();
            println!("Without a config file, you must provide all options via CLI:");
            println!("  walrust watch <database> --bucket <bucket> [options]");
            return Ok(());
        }
        Some(cfg) => {
            println!("Configuration Summary");
            println!("=====================");
            println!();

            // S3 Settings
            println!("S3 Storage:");
            if let Some(bucket) = &cfg.s3.bucket {
                println!("  Bucket:   {}", bucket);
            } else {
                println!("  Bucket:   (not configured - must specify via --bucket)");
            }
            if let Some(endpoint) = &cfg.s3.endpoint {
                println!("  Endpoint: {}", endpoint);
            } else {
                println!("  Endpoint: (default AWS S3)");
            }
            println!();

            // Snapshot Triggers
            println!("Snapshot Triggers (global defaults):");
            println!(
                "  Interval:    {} seconds ({} minutes)",
                cfg.sync.snapshot_interval,
                cfg.sync.snapshot_interval / 60
            );
            if cfg.sync.max_changes > 0 {
                println!("  Max changes: {} WAL frames", cfg.sync.max_changes);
            } else {
                println!("  Max changes: disabled");
            }
            if cfg.sync.max_interval > 0 {
                println!("  Max interval: {} seconds", cfg.sync.max_interval);
            }
            if cfg.sync.on_idle > 0 {
                println!("  On idle:     {} seconds", cfg.sync.on_idle);
            } else {
                println!("  On idle:     disabled");
            }
            println!(
                "  On startup:  {}",
                if cfg.sync.on_startup { "yes" } else { "no" }
            );
            println!();

            // Pruning Settings
            println!("Pruning:");
            if cfg.sync.compact_after_snapshot {
                println!("  After snapshot: enabled");
            } else {
                println!("  After snapshot: disabled");
            }
            if cfg.sync.compact_interval > 0 {
                println!(
                    "  Interval:       {} seconds ({} minutes)",
                    cfg.sync.compact_interval,
                    cfg.sync.compact_interval / 60
                );
            } else {
                println!("  Interval:       disabled");
            }
            println!();

            // Compaction (experimental, off by default)
            println!("Compaction (experimental):");
            if cfg.compaction.enabled {
                println!("  Enabled:         yes (config)");
                println!("  keep_fine_window: {}", cfg.compaction.keep_fine_window);
                println!("  l1_batch:        {}", cfg.compaction.l1_batch);
                println!("  l2_batch:        {}", cfg.compaction.l2_batch);
                println!("  WARNING: leveled compaction is NOT yet supported by the CLI watch.");
                println!("           The CLI restore path cannot read leveled buckets, so the");
                println!("           watch will REFUSE to start with this enabled. Compaction is");
                println!("           available only in library (owned) mode via the Replicator.");
                println!("  WARNING: leveled buckets are NOT restorable by walrust binaries");
                println!("           older than this release (version skew).");
            } else {
                println!("  Enabled:         no (default; ship-dark for version skew)");
            }
            println!();

            // Retention Policy
            println!("Retention Policy (GFS rotation):");
            println!(
                "  Hourly:  {} snapshots (last {} hours)",
                cfg.retention.hourly, cfg.retention.hourly
            );
            println!(
                "  Daily:   {} snapshots (last {} days)",
                cfg.retention.daily, cfg.retention.daily
            );
            println!(
                "  Weekly:  {} snapshots (last {} weeks)",
                cfg.retention.weekly, cfg.retention.weekly
            );
            println!(
                "  Monthly: {} snapshots (last {} months)",
                cfg.retention.monthly, cfg.retention.monthly
            );
            println!();

            // Databases
            println!("Databases:");
            if cfg.databases.is_empty() {
                println!("  (none configured - must specify via CLI)");
            } else {
                // Resolve databases to show actual paths
                match cfg.resolve_databases() {
                    Ok(resolved) => {
                        if resolved.is_empty() {
                            println!("  (no matching files found for configured patterns)");
                        } else {
                            for db in &resolved {
                                println!("  - {} -> s3://.../{}/*", db.path.display(), db.prefix);

                                // Show per-database overrides if different from global
                                let mut overrides = Vec::new();
                                if db.sync.snapshot_interval != cfg.sync.snapshot_interval {
                                    overrides
                                        .push(format!("interval={}s", db.sync.snapshot_interval));
                                }
                                if db.sync.max_changes != cfg.sync.max_changes {
                                    overrides.push(format!("max_changes={}", db.sync.max_changes));
                                }
                                if db.retention.hourly != cfg.retention.hourly
                                    || db.retention.daily != cfg.retention.daily
                                    || db.retention.weekly != cfg.retention.weekly
                                    || db.retention.monthly != cfg.retention.monthly
                                {
                                    overrides.push(format!(
                                        "retention={}/{}/{}/{}",
                                        db.retention.hourly,
                                        db.retention.daily,
                                        db.retention.weekly,
                                        db.retention.monthly
                                    ));
                                }
                                if !overrides.is_empty() {
                                    println!("    Overrides: {}", overrides.join(", "));
                                }
                            }
                        }
                    }
                    Err(e) => {
                        println!("  (error resolving databases: {})", e);
                        for db in &cfg.databases {
                            println!("  - {} (pattern)", db.path);
                        }
                    }
                }
            }
            println!();

            // Validation
            println!("Validation:");
            if cfg.sync.validation_interval > 0 {
                println!(
                    "  Interval: {} seconds ({} hours)",
                    cfg.sync.validation_interval,
                    cfg.sync.validation_interval / 3600
                );
                println!("  Checks: File existence, header validity, checksums, TXID continuity");
            } else {
                println!("  Disabled (recommended: enable with --validation-interval 86400 for daily checks)");
            }
            println!();

            // Webhooks
            println!("Webhook Notifications:");
            if cfg.webhooks.is_empty() {
                println!("  None configured");
            } else {
                for (i, webhook) in cfg.webhooks.iter().enumerate() {
                    println!("  {}. {}", i + 1, webhook.url);
                    println!("     Events: {}", webhook.events.join(", "));
                    if webhook.secret.is_some() {
                        println!("     HMAC:   enabled (X-Hadb-Signature header)");
                    }
                }
            }
            println!();

            // Summary with cost estimation
            let total_snapshots = cfg.retention.hourly
                + cfg.retention.daily
                + cfg.retention.weekly
                + cfg.retention.monthly;
            println!("Summary:");
            println!(
                "  Max snapshots retained per database: ~{}",
                total_snapshots
            );
            if cfg.sync.compact_after_snapshot || cfg.sync.compact_interval > 0 {
                println!("  Automatic pruning: enabled");
            } else {
                println!("  Automatic pruning: disabled (run 'walrust prune' manually)");
            }

            // Cost estimation
            match cfg.resolve_databases() {
                Ok(resolved) if !resolved.is_empty() => {
                    println!();
                    println!("Estimated Storage Costs:");
                    println!("  Note: Assumes average database size of 1GB per database");
                    println!();

                    let db_count = resolved.len();
                    let avg_db_size_gb = 1.0; // Conservative estimate
                    let snapshots_per_db = total_snapshots as f64;

                    // Tigris/S3 pricing (Tigris: ~$0.02/GB/month)
                    let storage_gb = db_count as f64 * avg_db_size_gb * snapshots_per_db;
                    let cost_tigris = storage_gb * 0.02;
                    let cost_s3 = storage_gb * 0.023; // S3 Standard pricing

                    println!(
                        "  Total snapshots: {} databases x {} snapshots = {} snapshots",
                        db_count,
                        snapshots_per_db,
                        db_count as f64 * snapshots_per_db
                    );
                    println!("  Estimated storage: {:.1} GB", storage_gb);
                    println!("  Monthly cost (Tigris): ~${:.2}", cost_tigris);
                    println!("  Monthly cost (S3 Standard): ~${:.2}", cost_s3);
                    println!();
                    println!("  Actual costs depend on:");
                    println!(
                        "  - Real database sizes (current estimate: {}GB per DB)",
                        avg_db_size_gb
                    );
                    println!("  - Compression ratio (LTX typically compresses well)");
                    println!("  - Incremental file sizes between snapshots");
                }
                _ => {}
            }
        }
    }

    Ok(())
}
