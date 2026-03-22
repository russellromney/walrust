use anyhow::{anyhow, Result};
use std::path::Path;
use crate::cache::LocalCache;
use crate::ltx;
use crate::s3::{self, create_client, parse_bucket};

use super::manifest::{discover_state_from_s3, find_latest_snapshot, list_generation_files, GENERATION_LIVE};

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
    let client = create_client(endpoint).await?;

    // Try to open local cache if provided
    let cache = if let Some(dir) = cache_dir {
        match LocalCache::open(dir)? {
            Some(c) => {
                tracing::info!("Opened local cache at {}", dir.display());
                Some(c)
            }
            None => {
                tracing::warn!("Cache directory {} has no manifest, falling back to S3", dir.display());
                None
            }
        }
    } else {
        None
    };

    // Discover state from S3 file listings (litestream format - no manifest)
    let (current_txid, _max_gen, _) =
        discover_state_from_s3(&client, &bucket_name, &prefix, name).await?;

    if current_txid == 0 {
        return Err(anyhow!("No LTX files found for database: {}", name));
    }

    // Find the latest snapshot (min_txid=1 in generation 1+)
    let snapshot = find_latest_snapshot(&client, &bucket_name, &prefix, name)
        .await?
        .ok_or_else(|| anyhow!("No snapshot found for database: {}", name))?;

    let (snapshot_gen, snapshot_key, snapshot_min_txid, snapshot_max_txid) = snapshot;

    // Parse point in time if provided (TXID only for litestream format)
    let target_txid = if let Some(pit) = point_in_time {
        pit.parse::<u64>().map_err(|_| {
            anyhow!("Invalid point_in_time format. Use TXID (number)")
        })?
    } else {
        current_txid
    };

    tracing::info!(
        "Restoring from LTX snapshot: {} (TXID: {}-{}, generation: {})",
        snapshot_key,
        snapshot_min_txid,
        snapshot_max_txid,
        snapshot_gen
    );

    // Download and decode LTX snapshot
    // Try cache first, fall back to S3
    let ltx_data = if let Some(ref cache) = cache {
        if cache.has_txid(snapshot_max_txid) {
            tracing::info!("Reading snapshot TXID {} from cache", snapshot_max_txid);
            cache.read_ltx(snapshot_max_txid)?
        } else {
            tracing::debug!("Snapshot TXID {} not in cache, fetching from S3", snapshot_max_txid);
            s3::download_bytes(&client, &bucket_name, &snapshot_key).await?
        }
    } else {
        s3::download_bytes(&client, &bucket_name, &snapshot_key).await?
    };

    let cursor = std::io::Cursor::new(ltx_data);
    let decode_result = ltx::decode_to_db(cursor, output).map_err(|e| {
        if let Some(webhook) = webhook {
            let error_msg = format!("LTX decode failed for snapshot: {}", e);
            let webhook = webhook.clone();
            let name = name.to_string();
            tokio::spawn(async move {
                webhook.notify_corruption(&name, &error_msg).await;
            });
        }
        e
    })?;

    tracing::info!(
        "Restored {} from LTX (page_size: {}, pages: {}, TXID: {}-{}, checksum: {:016x})",
        name,
        decode_result.header.page_size.into_inner(),
        decode_result.header.commit.into_inner(),
        decode_result.header.min_txid.into_inner(),
        decode_result.header.max_txid.into_inner(),
        decode_result.post_apply_checksum.into_inner()
    );

    let mut final_txid = snapshot_max_txid;

    // Get incrementals from generation 0 (live folder)
    let incrementals = list_generation_files(&client, &bucket_name, &prefix, name, GENERATION_LIVE).await?;

    // Filter to files we need: min_txid > snapshot_max_txid and max_txid <= target_txid
    let applicable: Vec<_> = incrementals
        .iter()
        .filter(|(_, min, max)| *min > snapshot_max_txid && *max <= target_txid)
        .collect();

    if !applicable.is_empty() {
        tracing::info!("Applying {} incremental LTX files", applicable.len());

        // Track cache hits for logging
        let mut cache_hits = 0;
        let mut s3_fetches = 0;

        for (key, _min_txid, max_txid) in &applicable {
            // Try to read from cache first using max_txid as the key
            let ltx_data = if let Some(ref cache) = cache {
                if cache.has_txid(*max_txid) {
                    cache_hits += 1;
                    tracing::debug!("Reading TXID {} from cache", max_txid);
                    cache.read_ltx(*max_txid)?
                } else {
                    s3_fetches += 1;
                    tracing::debug!("TXID {} not in cache, fetching from S3", max_txid);
                    s3::download_bytes(&client, &bucket_name, key).await?
                }
            } else {
                s3_fetches += 1;
                s3::download_bytes(&client, &bucket_name, key).await?
            };

            let cursor = std::io::Cursor::new(ltx_data);
            // apply_ltx_to_db now verifies pre_apply and post_apply checksums
            let apply_result = ltx::apply_ltx_to_db(cursor, output)?;

            tracing::debug!(
                "Applied {} (TXID: {}-{}, checksum: {:016x})",
                key,
                apply_result.header.min_txid.into_inner(),
                apply_result.header.max_txid.into_inner(),
                apply_result.post_apply_checksum.into_inner()
            );

            final_txid = *max_txid;
        }

        if cache.is_some() {
            tracing::info!(
                "Applied {} incremental LTX files (cache: {}, S3: {}, final TXID: {})",
                applicable.len(),
                cache_hits,
                s3_fetches,
                final_txid
            );
        } else {
            tracing::info!(
                "Applied {} incremental LTX files (final TXID: {})",
                applicable.len(),
                final_txid
            );
        }
    }

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
    let client = create_client(endpoint).await?;

    let objects = s3::list_objects(&client, &bucket_name, &prefix).await?;

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
                discover_state_from_s3(&client, &bucket_name, &prefix, db).await?;

            // Count files in generation 0 (live incrementals)
            let live_files = list_generation_files(&client, &bucket_name, &prefix, db, GENERATION_LIVE).await?;

            // Find snapshots (generation 1+)
            let snapshot = find_latest_snapshot(&client, &bucket_name, &prefix, db).await?;
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
