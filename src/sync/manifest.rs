use anyhow::Result;

use crate::s3;
use super::types::{DbState, Manifest};

// ============================================
// Litestream-compatible format helpers
// ============================================
// Litestream format:
//   db_name/0000/{min_txid}-{max_txid}.ltx  <- live incrementals
//   db_name/0001/{min_txid}-{max_txid}.ltx  <- generation 1 (snapshot + compacted)
//   db_name/0002/...                         <- generation 2, etc.
// TXIDs are 16-char lowercase hex (e.g., 0000000000000001)

/// Format a TXID as 16-char lowercase hex (litestream format)
pub(crate) fn format_txid_hex(txid: u64) -> String {
    format!("{:016x}", txid)
}

/// Parse a TXID from 16-char hex string
pub(crate) fn parse_txid_hex(s: &str) -> Option<u64> {
    u64::from_str_radix(s, 16).ok()
}

/// Format an LTX filename in litestream format
pub(crate) fn format_ltx_filename(min_txid: u64, max_txid: u64) -> String {
    format!("{}-{}.ltx", format_txid_hex(min_txid), format_txid_hex(max_txid))
}

/// Parse min/max TXID from litestream-format filename
/// e.g., "0000000000000001-0000000000000010.ltx" -> Some((1, 16))
pub(crate) fn parse_ltx_filename(filename: &str) -> Option<(u64, u64)> {
    let name = filename.strip_suffix(".ltx")?;
    let parts: Vec<&str> = name.split('-').collect();
    if parts.len() != 2 {
        return None;
    }
    let min_txid = parse_txid_hex(parts[0])?;
    let max_txid = parse_txid_hex(parts[1])?;
    Some((min_txid, max_txid))
}

/// Format generation folder name (4-char hex)
pub(crate) fn format_generation(gen: u64) -> String {
    format!("{:04x}", gen)
}

/// Parse generation from folder name
pub(crate) fn parse_generation(s: &str) -> Option<u64> {
    u64::from_str_radix(s, 16).ok()
}

/// Build S3 key for an LTX file in litestream format
/// - generation 0 = live incrementals (0000/)
/// - generation 1+ = snapshots and compacted files
pub(crate) fn build_ltx_key(prefix: &str, db_name: &str, generation: u64, min_txid: u64, max_txid: u64) -> String {
    format!(
        "{}{}/{}/{}",
        prefix,
        db_name,
        format_generation(generation),
        format_ltx_filename(min_txid, max_txid)
    )
}

/// Live incrementals go to generation 0 (0000/)
pub(crate) const GENERATION_LIVE: u64 = 0;

/// Discover current state from S3 by listing files (no manifest needed)
/// Returns (current_txid, latest_generation, last_checksum)
pub(crate) async fn discover_state_from_s3(
    client: &aws_sdk_s3::Client,
    bucket: &str,
    prefix: &str,
    db_name: &str,
) -> Result<(u64, u64, Option<u64>)> {
    // List all objects under db_name/
    let db_prefix = format!("{}{}/", prefix, db_name);
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
    // Returns: (generation, key, min_txid, max_txid)
    let db_prefix = format!("{}{}/", prefix, db_name);
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
                    if min_txid == 1 {
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
    let gen_prefix = format!("{}{}/{}/", prefix, db_name, format_generation(generation));
    let objects = s3::list_objects(client, bucket, &gen_prefix).await?;

    let mut files = Vec::new();
    for key in objects {
        let filename = key.rsplit('/').next().unwrap_or(&key);
        if let Some((min_txid, max_txid)) = parse_ltx_filename(filename) {
            files.push((key, min_txid, max_txid));
        }
    }

    // Sort by min_txid
    files.sort_by_key(|(_, min, _)| *min);
    Ok(files)
}

/// Save legacy state.json file to S3
pub(crate) async fn save_state(
    client: &aws_sdk_s3::Client,
    bucket: &str,
    prefix: &str,
    state: &DbState,
) -> Result<()> {
    let state_key = format!("{}{}/state.json", prefix, state.name);
    let state_json = serde_json::json!({
        "wal_offset": state.wal_offset,
        "wal_generation": state.wal_generation,
        "current_txid": state.current_txid,
        "last_snapshot": state.last_snapshot,
    });

    s3::upload_bytes(
        client,
        bucket,
        &state_key,
        serde_json::to_vec_pretty(&state_json)?,
    )
    .await?;

    Ok(())
}

/// Load manifest from S3
pub(crate) async fn load_manifest(
    client: &aws_sdk_s3::Client,
    bucket: &str,
    prefix: &str,
    db_name: &str,
) -> Result<Manifest> {
    let manifest_key = format!("{}{}/manifest.json", prefix, db_name);
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
    let manifest_key = format!("{}{}/manifest.json", prefix, manifest.name);
    s3::upload_bytes(
        client,
        bucket,
        &manifest_key,
        serde_json::to_vec_pretty(manifest)?,
    )
    .await?;
    Ok(())
}
