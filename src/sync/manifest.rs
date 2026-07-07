use crate::ltx::Decoder;
use anyhow::Result;
use std::io::Cursor;
pub(crate) use walrust_core::legacy_manifest::{
    build_ltx_key, database_prefix, format_generation, is_snapshot, parse_generation,
    parse_legacy_flat_ltx_filename, parse_ltx_filename, DiscoveredLtx, GENERATION_LIVE,
};

use super::types::Manifest;
use crate::s3;

// ============================================
// Litestream-compatible format helpers
// ============================================
// Litestream format:
//   db_name/0000/{min_txid}-{max_txid}.ltx  <- live incrementals
//   db_name/0001/{min_txid}-{max_txid}.ltx  <- generation 1 (snapshot + compacted)
//   db_name/0002/...                         <- generation 2, etc.
// TXIDs are 16-char lowercase hex (e.g., 0000000000000001)

async fn legacy_flat_ltx_range(
    client: &aws_sdk_s3::Client,
    bucket: &str,
    key: &str,
    txid: u64,
) -> (u64, u64) {
    let Ok(bytes) = s3::download_bytes(client, bucket, key).await else {
        return (txid, txid);
    };
    let Ok((_, header)) = Decoder::new(Cursor::new(bytes)) else {
        return (txid, txid);
    };
    (header.min_txid.into_inner(), header.max_txid.into_inner())
}

/// Discover all snapshot LTX files from S3 by listing (no manifest needed).
///
/// Returns `(key, generation, min_txid, max_txid)` for every file that
/// [`is_snapshot`] classifies as a snapshot, across the live generation and all
/// snapshot generations up to the highest present. Mirrors how `verify` and
/// `restore` discover state so `compact` no longer depends on a `manifest.json`
/// the production watch path never writes (F6).
pub(crate) async fn discover_snapshots_from_s3(
    client: &aws_sdk_s3::Client,
    bucket: &str,
    prefix: &str,
    db_name: &str,
) -> Result<Vec<(String, u64, u64, u64)>> {
    let (_current_txid, max_gen, _) =
        discover_state_from_s3(client, bucket, prefix, db_name).await?;

    let mut snapshots = Vec::new();
    // Live generation may hold the initial base (min==max==1).
    for (key, min, max) in
        list_generation_files(client, bucket, prefix, db_name, GENERATION_LIVE).await?
    {
        if is_snapshot(GENERATION_LIVE, min, max) {
            snapshots.push((key, GENERATION_LIVE, min, max));
        }
    }
    // Snapshot generations 1..=max_gen are snapshots by definition.
    for gen in 1..=max_gen {
        for (key, min, max) in list_generation_files(client, bucket, prefix, db_name, gen).await? {
            snapshots.push((key, gen, min, max));
        }
    }
    snapshots.sort_by_key(|(_, gen, _, max)| (*gen, *max));
    Ok(snapshots)
}

/// Discover current state from S3 by listing files (no manifest needed)
/// Returns (current_txid, latest_generation, last_checksum)
pub(crate) async fn discover_state_from_s3(
    client: &aws_sdk_s3::Client,
    bucket: &str,
    prefix: &str,
    db_name: &str,
) -> Result<(u64, u64, Option<u64>)> {
    // List all objects under db_name/
    let db_prefix = database_prefix(prefix, db_name);
    let objects = s3::list_objects(client, bucket, &db_prefix).await?;

    if objects.is_empty() {
        return Ok((0, 0, None));
    }

    let mut max_txid: u64 = 0;
    let mut max_generation: u64 = 0;

    for key in &objects {
        // Extract generation and filename from key
        // Key format: prefix/db_name/GGGG/min-max.ltx
        let relative = key.strip_prefix(&db_prefix).unwrap_or(key);
        let parts: Vec<&str> = relative.split('/').collect();

        if parts.len() == 2 {
            if let Some(gen) = parse_generation(parts[0]) {
                if gen > max_generation && gen > 0 {
                    max_generation = gen;
                }
                if let Some((_, file_max_txid)) = parse_ltx_filename(parts[1]) {
                    if file_max_txid > max_txid {
                        max_txid = file_max_txid;
                    }
                }
            }
        } else if parts.len() == 1 {
            if let Some(file_max_txid) = parse_legacy_flat_ltx_filename(parts[0]) {
                if file_max_txid > max_txid {
                    max_txid = file_max_txid;
                }
            }
        }
    }

    // For checksum, we'd need to read the latest LTX header
    // For now, return None - we'll compute from local DB on startup
    Ok((max_txid, max_generation, None))
}

/// Find the latest snapshot in S3 (file with min_txid = 1 in highest generation)
pub(crate) async fn find_latest_snapshot(
    client: &aws_sdk_s3::Client,
    bucket: &str,
    prefix: &str,
    db_name: &str,
) -> Result<Option<(u64, String, u64, u64)>> {
    find_latest_snapshot_matching(client, bucket, prefix, db_name, |_| true).await
}

/// Find the latest snapshot whose max TXID is not after `target_txid`.
pub(crate) async fn find_latest_snapshot_at_or_before(
    client: &aws_sdk_s3::Client,
    bucket: &str,
    prefix: &str,
    db_name: &str,
    target_txid: u64,
) -> Result<Option<(u64, String, u64, u64)>> {
    find_latest_snapshot_matching(client, bucket, prefix, db_name, |max_txid| {
        max_txid <= target_txid
    })
    .await
}

async fn find_latest_snapshot_matching(
    client: &aws_sdk_s3::Client,
    bucket: &str,
    prefix: &str,
    db_name: &str,
    include_max_txid: impl Fn(u64) -> bool,
) -> Result<Option<(u64, String, u64, u64)>> {
    // Returns: (generation, key, min_txid, max_txid)
    let db_prefix = database_prefix(prefix, db_name);
    let objects = s3::list_objects(client, bucket, &db_prefix).await?;

    let mut best_snapshot: Option<(u64, String, u64, u64)> = None;

    for key in &objects {
        let relative = key.strip_prefix(&db_prefix).unwrap_or(key);
        let parts: Vec<&str> = relative.split('/').collect();

        if parts.len() == 2 {
            if let Some(gen) = parse_generation(parts[0]) {
                if let Some((min_txid, max_txid)) = parse_ltx_filename(parts[1]) {
                    // A snapshot has min_txid = 1
                    // Look in all generations (litestream puts initial snapshot in gen 0)
                    if min_txid == 1 && include_max_txid(max_txid) {
                        match &best_snapshot {
                            None => {
                                best_snapshot = Some((gen, key.clone(), min_txid, max_txid));
                            }
                            Some((best_gen, _, _, best_max)) => {
                                // Prefer higher generation, or higher max_txid in same generation
                                if gen > *best_gen || (gen == *best_gen && max_txid > *best_max) {
                                    best_snapshot = Some((gen, key.clone(), min_txid, max_txid));
                                }
                            }
                        }
                    }
                }
            }
        } else if parts.len() == 1 {
            if let Some(txid) = parse_legacy_flat_ltx_filename(parts[0]) {
                let (min_txid, max_txid) = legacy_flat_ltx_range(client, bucket, key, txid).await;
                if min_txid == 1 && include_max_txid(max_txid) {
                    match &best_snapshot {
                        None => {
                            best_snapshot =
                                Some((GENERATION_LIVE, key.clone(), min_txid, max_txid));
                        }
                        Some((best_gen, _, _, best_max)) => {
                            if *best_gen == GENERATION_LIVE && max_txid > *best_max {
                                best_snapshot =
                                    Some((GENERATION_LIVE, key.clone(), min_txid, max_txid));
                            }
                        }
                    }
                }
            }
        }
    }

    Ok(best_snapshot)
}

/// List all LTX files in a generation folder
pub(crate) async fn list_generation_files(
    client: &aws_sdk_s3::Client,
    bucket: &str,
    prefix: &str,
    db_name: &str,
    generation: u64,
) -> Result<Vec<(String, u64, u64)>> {
    // Returns: Vec<(key, min_txid, max_txid)>
    let gen_prefix = format!(
        "{}{}/",
        database_prefix(prefix, db_name),
        format_generation(generation)
    );
    let objects = s3::list_objects(client, bucket, &gen_prefix).await?;

    let mut files = Vec::new();
    for key in objects {
        let filename = key.rsplit('/').next().unwrap_or(&key);
        if let Some((min_txid, max_txid)) = parse_ltx_filename(filename) {
            files.push((key, min_txid, max_txid));
        }
    }

    if generation == GENERATION_LIVE {
        let db_prefix = database_prefix(prefix, db_name);
        let legacy_objects = s3::list_objects(client, bucket, &db_prefix).await?;
        for key in legacy_objects {
            let relative = key.strip_prefix(&db_prefix).unwrap_or(&key);
            let parts: Vec<&str> = relative.split('/').collect();
            if parts.len() != 1 {
                continue;
            }
            let Some(txid) = parse_legacy_flat_ltx_filename(parts[0]) else {
                continue;
            };
            let (min_txid, max_txid) = legacy_flat_ltx_range(client, bucket, &key, txid).await;
            files.push((key, min_txid, max_txid));
        }
    }

    // Sort by min_txid
    files.sort_by_key(|(_, min, _)| *min);
    Ok(files)
}

/// Discover every LTX file (snapshots + incrementals) for a database from the
/// S3 listing, across the live generation and all snapshot generations.
///
/// This is the manifest-free discovery used by `replicate` so it works against
/// the litestream-format layout the production watch path actually writes (F6).
/// Returned entries carry the full S3 key and are sorted by `(min_txid, gen)`.
pub(crate) async fn discover_all_ltx_from_s3(
    client: &aws_sdk_s3::Client,
    bucket: &str,
    prefix: &str,
    db_name: &str,
) -> Result<Vec<DiscoveredLtx>> {
    let (_current_txid, max_gen, _) =
        discover_state_from_s3(client, bucket, prefix, db_name).await?;

    let mut files = Vec::new();
    for (key, min, max) in
        list_generation_files(client, bucket, prefix, db_name, GENERATION_LIVE).await?
    {
        files.push(DiscoveredLtx {
            key,
            generation: GENERATION_LIVE,
            min_txid: min,
            max_txid: max,
            is_snapshot: is_snapshot(GENERATION_LIVE, min, max),
        });
    }
    for gen in 1..=max_gen {
        for (key, min, max) in list_generation_files(client, bucket, prefix, db_name, gen).await? {
            files.push(DiscoveredLtx {
                key,
                generation: gen,
                min_txid: min,
                max_txid: max,
                is_snapshot: is_snapshot(gen, min, max),
            });
        }
    }
    files.sort_by_key(|f| (f.min_txid, f.generation));
    Ok(files)
}

/// Load manifest from S3
pub(crate) async fn load_manifest(
    client: &aws_sdk_s3::Client,
    bucket: &str,
    prefix: &str,
    db_name: &str,
) -> Result<Manifest> {
    let manifest_key = format!("{}manifest.json", database_prefix(prefix, db_name));
    match s3::download_bytes(client, bucket, &manifest_key).await {
        Ok(data) => Ok(serde_json::from_slice(&data)?),
        Err(_) => Ok(Manifest {
            name: db_name.to_string(),
            ..Default::default()
        }),
    }
}

/// Save manifest to S3
pub(crate) async fn save_manifest(
    client: &aws_sdk_s3::Client,
    bucket: &str,
    prefix: &str,
    manifest: &Manifest,
) -> Result<()> {
    let manifest_key = format!("{}manifest.json", database_prefix(prefix, &manifest.name));
    s3::upload_bytes(
        client,
        bucket,
        &manifest_key,
        serde_json::to_vec_pretty(manifest)?,
    )
    .await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn build_ltx_key_normalizes_prefix_separator() {
        assert_eq!(
            build_ltx_key("base", "db", 0, 2, 3),
            "base/db/0000/0000000000000002-0000000000000003.ltx"
        );
        assert_eq!(
            build_ltx_key("base/", "db", 0, 2, 3),
            "base/db/0000/0000000000000002-0000000000000003.ltx"
        );
        assert_eq!(
            build_ltx_key("", "db", 1, 1, 1),
            "db/0001/0000000000000001-0000000000000001.ltx"
        );
    }

    #[test]
    fn parse_legacy_flat_ltx_filename_accepts_only_old_cache_shape() {
        assert_eq!(parse_legacy_flat_ltx_filename("00000003.ltx"), Some(3));
        assert_eq!(parse_legacy_flat_ltx_filename("0000000000000003.ltx"), None);
        assert_eq!(
            parse_legacy_flat_ltx_filename("0000000000000002-0000000000000003.ltx"),
            None
        );
    }
}
