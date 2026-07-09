use anyhow::Result;
use hadb_storage_s3::S3Storage;
#[cfg(test)]
pub(crate) use walrust_core::legacy_manifest::build_ltx_key;
use walrust_core::legacy_manifest::{
    discover_all_legacy_ltx, discover_legacy_snapshots, discover_legacy_state,
    find_latest_legacy_snapshot, list_legacy_generation_files,
};
pub(crate) use walrust_core::legacy_manifest::{is_snapshot, DiscoveredLtx, GENERATION_LIVE};

// ============================================
// Litestream-compatible format helpers
// ============================================
// Litestream format:
//   db_name/0000/{min_txid}-{max_txid}.ltx  <- live incrementals
//   db_name/0001/{min_txid}-{max_txid}.ltx  <- generation 1 (snapshot + compacted)
//   db_name/0002/...                         <- generation 2, etc.
// TXIDs are 16-char lowercase hex (e.g., 0000000000000001)

fn s3_storage(client: &aws_sdk_s3::Client, bucket: &str) -> S3Storage {
    S3Storage::new(client.clone(), bucket.to_string())
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
    let storage = s3_storage(client, bucket);
    Ok(discover_legacy_snapshots(&storage, prefix, db_name)
        .await?
        .into_iter()
        .map(|file| (file.key, file.generation, file.min_txid, file.max_txid))
        .collect())
}

/// Discover current state from S3 by listing files (no manifest needed)
/// Returns (current_txid, latest_generation, last_checksum)
pub(crate) async fn discover_state_from_s3(
    client: &aws_sdk_s3::Client,
    bucket: &str,
    prefix: &str,
    db_name: &str,
) -> Result<(u64, u64, Option<u64>)> {
    let storage = s3_storage(client, bucket);
    let (current_txid, max_generation) = discover_legacy_state(&storage, prefix, db_name).await?;
    Ok((current_txid, max_generation, None))
}

/// Find the latest snapshot in S3 (file with min_txid = 1 in highest generation)
pub(crate) async fn find_latest_snapshot(
    client: &aws_sdk_s3::Client,
    bucket: &str,
    prefix: &str,
    db_name: &str,
) -> Result<Option<(u64, String, u64, u64)>> {
    let storage = s3_storage(client, bucket);
    Ok(find_latest_legacy_snapshot(&storage, prefix, db_name)
        .await?
        .map(|file| (file.generation, file.key, file.min_txid, file.max_txid)))
}

/// List all LTX files in a generation folder
pub(crate) async fn list_generation_files(
    client: &aws_sdk_s3::Client,
    bucket: &str,
    prefix: &str,
    db_name: &str,
    generation: u64,
) -> Result<Vec<(String, u64, u64)>> {
    let storage = s3_storage(client, bucket);
    list_legacy_generation_files(&storage, prefix, db_name, generation).await
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
    let storage = s3_storage(client, bucket);
    discover_all_legacy_ltx(&storage, prefix, db_name).await
}

#[cfg(test)]
mod tests {
    use super::*;
    use walrust_core::legacy_manifest::parse_legacy_flat_ltx_filename;

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
