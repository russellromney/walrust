use crate::cache::LocalCache;
use crate::errors::{classify_or_else, WalrustError};
use crate::s3::{self, create_client, parse_bucket};
use anyhow::Result;
use async_trait::async_trait;
use hadb_storage::{CasResult, StorageBackend};
use hadb_storage_s3::S3Storage;
use std::path::Path;
use walrust_core::legacy_restore;

use super::manifest::{
    discover_state_from_s3, find_latest_snapshot, list_generation_files, GENERATION_LIVE,
};
use walrust_core::legacy_manifest::{parse_legacy_flat_ltx_filename, parse_ltx_filename};

struct CachedLegacyStorage {
    s3: S3Storage,
    cache: Option<LocalCache>,
}

fn key_max_txid(key: &str) -> Option<u64> {
    let filename = key.rsplit('/').next().unwrap_or(key);
    parse_ltx_filename(filename)
        .map(|(_, max_txid)| max_txid)
        .or_else(|| parse_legacy_flat_ltx_filename(filename))
}

#[async_trait]
impl StorageBackend for CachedLegacyStorage {
    async fn get(&self, key: &str) -> Result<Option<Vec<u8>>> {
        if let (Some(cache), Some(txid)) = (&self.cache, key_max_txid(key)) {
            if cache.has_txid(txid) {
                tracing::debug!("Reading TXID {} from cache", txid);
                return Ok(Some(cache.read_ltx(txid)?));
            }
        }
        self.s3
            .get(key)
            .await
            .map_err(|e| classify_or_else(e, WalrustError::s3))
    }

    async fn put(&self, key: &str, data: &[u8]) -> Result<()> {
        self.s3
            .put(key, data)
            .await
            .map_err(|e| classify_or_else(e, WalrustError::s3))
    }

    async fn delete(&self, key: &str) -> Result<()> {
        self.s3
            .delete(key)
            .await
            .map_err(|e| classify_or_else(e, WalrustError::s3))
    }

    async fn list(&self, prefix: &str, after: Option<&str>) -> Result<Vec<String>> {
        self.s3
            .list(prefix, after)
            .await
            .map_err(|e| classify_or_else(e, WalrustError::s3))
    }

    async fn exists(&self, key: &str) -> Result<bool> {
        self.s3
            .exists(key)
            .await
            .map_err(|e| classify_or_else(e, WalrustError::s3))
    }

    async fn put_if_absent(&self, key: &str, data: &[u8]) -> Result<CasResult> {
        self.s3
            .put_if_absent(key, data)
            .await
            .map_err(|e| classify_or_else(e, WalrustError::s3))
    }

    async fn put_if_match(&self, key: &str, data: &[u8], etag: &str) -> Result<CasResult> {
        self.s3
            .put_if_match(key, data, etag)
            .await
            .map_err(|e| classify_or_else(e, WalrustError::s3))
    }
}

pub async fn restore(
    name: &str,
    output: &Path,
    bucket: &str,
    endpoint: Option<&str>,
    point_in_time: Option<&str>,
    cache_dir: Option<&Path>,
    webhook: Option<std::sync::Arc<crate::webhook::WebhookSender>>,
) -> Result<()> {
    let (bucket_name, prefix) = parse_bucket(bucket);
    let client = create_client(endpoint)
        .await
        .map_err(|e| classify_or_else(e, WalrustError::s3))?;

    // Try to open local cache if provided
    let cache = if let Some(dir) = cache_dir {
        match LocalCache::open(dir)? {
            Some(c) => {
                tracing::info!("Opened local cache at {}", dir.display());
                Some(c)
            }
            None => {
                tracing::warn!(
                    "Cache directory {} has no manifest, falling back to S3",
                    dir.display()
                );
                None
            }
        }
    } else {
        None
    };

    // Parse point in time if provided (TXID only for litestream format).
    let parsed_point_in_time = if let Some(pit) = point_in_time {
        Some(pit.parse::<u64>().map_err(|_| {
            WalrustError::restore("Invalid point_in_time format. Use TXID (number)")
        })?)
    } else {
        None
    };

    let storage = CachedLegacyStorage {
        s3: S3Storage::new(client.clone(), bucket_name.clone()),
        cache,
    };
    let result =
        legacy_restore::restore_legacy_ltx(&storage, &prefix, name, output, parsed_point_in_time)
            .await;
    let final_txid = match result {
        Ok(txid) => txid,
        Err(e) => {
            if let Some(webhook) = webhook {
                let error_msg = format!("legacy LTX restore failed: {e}");
                let webhook = webhook.clone();
                let name = name.to_string();
                tokio::spawn(async move {
                    webhook.notify_corruption(&name, &error_msg).await;
                });
            }
            return Err(classify_or_else(e, WalrustError::restore));
        }
    };

    println!(
        "Restored {} to {} (TXID: {})",
        name,
        output.display(),
        final_txid
    );
    Ok(())
}

/// List databases in bucket
pub async fn list(bucket: &str, endpoint: Option<&str>) -> Result<()> {
    let (bucket_name, prefix) = parse_bucket(bucket);
    let client = create_client(endpoint)
        .await
        .map_err(|e| classify_or_else(e, WalrustError::s3))?;

    let objects = s3::list_objects(&client, &bucket_name, &prefix)
        .await
        .map_err(|e| classify_or_else(e, WalrustError::s3))?;

    // Extract unique database names (litestream format: db_name/GGGG/file.ltx)
    let mut dbs: std::collections::HashSet<String> = std::collections::HashSet::new();

    for key in &objects {
        if let Some(rest) = key.strip_prefix(&prefix) {
            if let Some(name) = rest.split('/').next() {
                if !name.is_empty() {
                    dbs.insert(name.to_string());
                }
            }
        }
    }

    if dbs.is_empty() {
        println!("No databases found in s3://{}/{}", bucket_name, prefix);
    } else {
        println!("Databases in s3://{}/{}:", bucket_name, prefix);
        for db in &dbs {
            // Discover state from S3 (litestream format)
            let (current_txid, _max_gen, _) =
                discover_state_from_s3(&client, &bucket_name, &prefix, db)
                    .await
                    .map_err(|e| classify_or_else(e, WalrustError::s3))?;

            // Count files in generation 0 (live incrementals)
            let live_files =
                list_generation_files(&client, &bucket_name, &prefix, db, GENERATION_LIVE)
                    .await
                    .map_err(|e| classify_or_else(e, WalrustError::s3))?;

            // Find snapshots (generation 1+)
            let snapshot = find_latest_snapshot(&client, &bucket_name, &prefix, db)
                .await
                .map_err(|e| classify_or_else(e, WalrustError::s3))?;
            let snapshot_info = match snapshot {
                Some((gen, _, _, max_txid)) => format!("snapshot gen {} (TXID {})", gen, max_txid),
                None => "no snapshot".to_string(),
            };

            println!(
                "  {} (TXID: {}, {} incrementals, {})",
                db,
                current_txid,
                live_files.len(),
                snapshot_info
            );
        }
    }

    Ok(())
}
