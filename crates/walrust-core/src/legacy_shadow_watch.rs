//! Legacy shadow-watch lifecycle helpers shared by the root CLI wrapper.

use crate::legacy_cache::LocalCache;
use crate::shadow::ShadowWal;
use anyhow::{Context, Result};
use chrono::Utc;
use serde::{Deserialize, Serialize};
use std::fs::{self, File};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::time::Duration;

const SHADOW_PROGRESS_FILE: &str = "progress.json";

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct ShadowProgress {
    pub version: u32,
    pub current_txid: u64,
    pub last_snapshot: Option<chrono::DateTime<Utc>>,
    pub db_checksum: Option<u64>,
    pub shadow_sync_generation: u64,
    pub shadow_sync_offset: u64,
}

pub fn shadow_progress_path(shadow_dir: &Path) -> PathBuf {
    shadow_dir.join(SHADOW_PROGRESS_FILE)
}

fn fsync_dir(path: &Path) -> Result<()> {
    File::open(path)
        .with_context(|| format!("failed to open directory {} for fsync", path.display()))?
        .sync_all()
        .with_context(|| format!("failed to fsync directory {}", path.display()))
}

pub fn save_shadow_progress(
    shadow_dir: &Path,
    db_name: &str,
    progress: &ShadowProgress,
) -> Result<()> {
    fs::create_dir_all(shadow_dir).with_context(|| {
        format!(
            "{}: failed to create shadow progress directory {}",
            db_name,
            shadow_dir.display()
        )
    })?;

    let progress_path = shadow_progress_path(shadow_dir);
    let tmp_path = progress_path.with_extension("json.tmp");
    let json = serde_json::to_vec_pretty(progress)
        .with_context(|| format!("{}: failed to serialize shadow progress", db_name))?;

    {
        let mut file = File::create(&tmp_path).with_context(|| {
            format!(
                "{}: failed to create temporary shadow progress file {}",
                db_name,
                tmp_path.display()
            )
        })?;
        file.write_all(&json).with_context(|| {
            format!(
                "{}: failed to write temporary shadow progress file {}",
                db_name,
                tmp_path.display()
            )
        })?;
        file.sync_all().with_context(|| {
            format!(
                "{}: failed to fsync temporary shadow progress file {}",
                db_name,
                tmp_path.display()
            )
        })?;
    }

    fs::rename(&tmp_path, &progress_path).with_context(|| {
        format!(
            "{}: failed to install shadow progress file {}",
            db_name,
            progress_path.display()
        )
    })?;
    fsync_dir(shadow_dir).with_context(|| {
        format!(
            "{}: failed to durably commit shadow progress file {}",
            db_name,
            progress_path.display()
        )
    })?;

    Ok(())
}

pub fn load_shadow_progress(shadow: &ShadowWal, db_name: &str) -> Result<Option<ShadowProgress>> {
    let progress_path = shadow_progress_path(shadow.shadow_dir());
    let data = match fs::read(&progress_path) {
        Ok(data) => data,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(e) => {
            return Err(e).with_context(|| {
                format!(
                    "{}: failed to read shadow progress file {}",
                    db_name,
                    progress_path.display()
                )
            });
        }
    };

    let progress: ShadowProgress = serde_json::from_slice(&data).with_context(|| {
        format!(
            "{}: failed to parse shadow progress file {}",
            db_name,
            progress_path.display()
        )
    })?;

    if progress.version != 1 {
        anyhow::bail!(
            "{}: unsupported shadow progress version {} in {}",
            db_name,
            progress.version,
            progress_path.display()
        );
    }
    if progress.shadow_sync_generation > shadow.generation() {
        anyhow::bail!(
            "{}: shadow progress generation {} is ahead of live generation {} in {}",
            db_name,
            progress.shadow_sync_generation,
            shadow.generation(),
            progress_path.display()
        );
    }

    Ok(Some(progress))
}

pub async fn wait_for_cache_checkpoint_durability(
    cache: &LocalCache,
    db_name: &str,
    required_txid: u64,
    timeout: Duration,
) -> Result<()> {
    let deadline = tokio::time::Instant::now() + timeout;

    loop {
        let failed = cache.failed_uploads();
        if !failed.is_empty() {
            anyhow::bail!(
                "{}: cannot checkpoint shadow WAL; cache upload failures remain: {:?}",
                db_name,
                failed
            );
        }

        let pending = cache.pending_uploads();
        let uploaded_txid = cache.last_uploaded_txid();
        if pending.is_empty() && uploaded_txid >= required_txid {
            return Ok(());
        }

        if tokio::time::Instant::now() >= deadline {
            anyhow::bail!(
                "{}: cannot checkpoint shadow WAL; durable upload confirmation timed out after {:?} (required_txid={}, uploaded_txid={}, pending={:?})",
                db_name,
                timeout,
                required_txid,
                uploaded_txid,
                pending
            );
        }

        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}
