//! Legacy Litestream-derived LTX object layout helpers.
//!
//! The root CLI still reads and writes this object layout while Phase 4 moves
//! the implementation into `walrust-core`.

use anyhow::Result;
use hadb_storage::StorageBackend;
use std::io::Cursor;

use crate::legacy_ltx::Decoder;

/// Live incrementals go to generation 0 (`0000/`).
pub const GENERATION_LIVE: u64 = 0;

/// Format a TXID as 16-char lowercase hex.
pub fn format_txid_hex(txid: u64) -> String {
    format!("{txid:016x}")
}

/// Parse a TXID from a 16-char hex string.
pub fn parse_txid_hex(s: &str) -> Option<u64> {
    u64::from_str_radix(s, 16).ok()
}

/// Format an LTX filename as `{min_txid:016x}-{max_txid:016x}.ltx`.
pub fn format_ltx_filename(min_txid: u64, max_txid: u64) -> String {
    format!(
        "{}-{}.ltx",
        format_txid_hex(min_txid),
        format_txid_hex(max_txid)
    )
}

/// Parse min/max TXID from a legacy LTX filename.
pub fn parse_ltx_filename(filename: &str) -> Option<(u64, u64)> {
    let name = filename.strip_suffix(".ltx")?;
    let parts: Vec<&str> = name.split('-').collect();
    if parts.len() != 2 {
        return None;
    }
    let min_txid = parse_txid_hex(parts[0])?;
    let max_txid = parse_txid_hex(parts[1])?;
    Some((min_txid, max_txid))
}

/// Parse the old flat cache/S3 shape: `00000003.ltx`.
pub fn parse_legacy_flat_ltx_filename(filename: &str) -> Option<u64> {
    let name = filename.strip_suffix(".ltx")?;
    if name.contains('-') || name.len() != 8 {
        return None;
    }
    name.parse::<u64>().ok()
}

/// Ensure a non-empty prefix ends with `/`.
pub fn prefix_with_separator(prefix: &str) -> String {
    if prefix.is_empty() || prefix.ends_with('/') {
        prefix.to_string()
    } else {
        format!("{prefix}/")
    }
}

/// Build the per-database object prefix.
pub fn database_prefix(prefix: &str, db_name: &str) -> String {
    format!("{}{}/", prefix_with_separator(prefix), db_name)
}

/// Format generation folder name as 4-char lowercase hex.
pub fn format_generation(generation: u64) -> String {
    format!("{generation:04x}")
}

/// Parse generation from a folder name.
pub fn parse_generation(s: &str) -> Option<u64> {
    u64::from_str_radix(s, 16).ok()
}

/// Build an S3/object key for an LTX file in legacy layout.
pub fn build_ltx_key(
    prefix: &str,
    db_name: &str,
    generation: u64,
    min_txid: u64,
    max_txid: u64,
) -> String {
    format!(
        "{}{}/{}/{}",
        prefix_with_separator(prefix),
        db_name,
        format_generation(generation),
        format_ltx_filename(min_txid, max_txid)
    )
}

/// Single definition of "is this LTX file a snapshot (full DB base)".
pub fn is_snapshot(generation: u64, min_txid: u64, max_txid: u64) -> bool {
    generation > 0 || (min_txid == 1 && max_txid == 1)
}

/// A discovered legacy LTX file from object listing.
#[derive(Debug, Clone)]
pub struct DiscoveredLtx {
    /// Full object key.
    pub key: String,
    pub generation: u64,
    pub min_txid: u64,
    pub max_txid: u64,
    pub is_snapshot: bool,
}

async fn legacy_flat_ltx_range(storage: &dyn StorageBackend, key: &str, txid: u64) -> (u64, u64) {
    let Ok(Some(bytes)) = storage.get(key).await else {
        return (txid, txid);
    };
    let Ok((_, header)) = Decoder::new(Cursor::new(bytes)) else {
        return (txid, txid);
    };
    (header.min_txid.into_inner(), header.max_txid.into_inner())
}

/// Discover current state from legacy LTX object listings.
///
/// Returns `(current_txid, latest_generation)`.
pub async fn discover_legacy_state(
    storage: &dyn StorageBackend,
    prefix: &str,
    db_name: &str,
) -> Result<(u64, u64)> {
    let db_prefix = database_prefix(prefix, db_name);
    let objects = storage.list(&db_prefix, None).await?;

    let mut max_txid = 0;
    let mut max_generation = 0;

    for key in &objects {
        let relative = key.strip_prefix(&db_prefix).unwrap_or(key);
        let parts: Vec<&str> = relative.split('/').collect();

        if parts.len() == 2 {
            if let Some(generation) = parse_generation(parts[0]) {
                if generation > max_generation && generation > GENERATION_LIVE {
                    max_generation = generation;
                }
                if let Some((_, file_max_txid)) = parse_ltx_filename(parts[1]) {
                    max_txid = max_txid.max(file_max_txid);
                }
            }
        } else if parts.len() == 1 {
            if let Some(file_max_txid) = parse_legacy_flat_ltx_filename(parts[0]) {
                max_txid = max_txid.max(file_max_txid);
            }
        }
    }

    Ok((max_txid, max_generation))
}

/// List all legacy LTX files in a generation folder.
pub async fn list_legacy_generation_files(
    storage: &dyn StorageBackend,
    prefix: &str,
    db_name: &str,
    generation: u64,
) -> Result<Vec<(String, u64, u64)>> {
    let gen_prefix = format!(
        "{}{}/",
        database_prefix(prefix, db_name),
        format_generation(generation)
    );
    let objects = storage.list(&gen_prefix, None).await?;

    let mut files = Vec::new();
    for key in objects {
        let filename = key.rsplit('/').next().unwrap_or(&key);
        if let Some((min_txid, max_txid)) = parse_ltx_filename(filename) {
            files.push((key, min_txid, max_txid));
        }
    }

    if generation == GENERATION_LIVE {
        let db_prefix = database_prefix(prefix, db_name);
        let legacy_objects = storage.list(&db_prefix, None).await?;
        for key in legacy_objects {
            let relative = key.strip_prefix(&db_prefix).unwrap_or(&key);
            let parts: Vec<&str> = relative.split('/').collect();
            if parts.len() != 1 {
                continue;
            }
            let Some(txid) = parse_legacy_flat_ltx_filename(parts[0]) else {
                continue;
            };
            let (min_txid, max_txid) = legacy_flat_ltx_range(storage, &key, txid).await;
            files.push((key, min_txid, max_txid));
        }
    }

    files.sort_by_key(|(_, min, _)| *min);
    Ok(files)
}

/// Discover all snapshot LTX files from legacy object listings.
pub async fn discover_legacy_snapshots(
    storage: &dyn StorageBackend,
    prefix: &str,
    db_name: &str,
) -> Result<Vec<DiscoveredLtx>> {
    let (_current_txid, max_generation) = discover_legacy_state(storage, prefix, db_name).await?;

    let mut snapshots = Vec::new();
    for (key, min_txid, max_txid) in
        list_legacy_generation_files(storage, prefix, db_name, GENERATION_LIVE).await?
    {
        if is_snapshot(GENERATION_LIVE, min_txid, max_txid) {
            snapshots.push(DiscoveredLtx {
                key,
                generation: GENERATION_LIVE,
                min_txid,
                max_txid,
                is_snapshot: true,
            });
        }
    }

    for generation in 1..=max_generation {
        for (key, min_txid, max_txid) in
            list_legacy_generation_files(storage, prefix, db_name, generation).await?
        {
            snapshots.push(DiscoveredLtx {
                key,
                generation,
                min_txid,
                max_txid,
                is_snapshot: true,
            });
        }
    }

    snapshots.sort_by_key(|file| (file.generation, file.max_txid));
    Ok(snapshots)
}

/// Find the latest legacy LTX snapshot.
pub async fn find_latest_legacy_snapshot(
    storage: &dyn StorageBackend,
    prefix: &str,
    db_name: &str,
) -> Result<Option<DiscoveredLtx>> {
    find_latest_legacy_snapshot_matching(storage, prefix, db_name, |_| true).await
}

/// Find the latest legacy LTX snapshot whose max TXID is not after `target_txid`.
pub async fn find_latest_legacy_snapshot_at_or_before(
    storage: &dyn StorageBackend,
    prefix: &str,
    db_name: &str,
    target_txid: u64,
) -> Result<Option<DiscoveredLtx>> {
    find_latest_legacy_snapshot_matching(storage, prefix, db_name, |max_txid| {
        max_txid <= target_txid
    })
    .await
}

async fn find_latest_legacy_snapshot_matching(
    storage: &dyn StorageBackend,
    prefix: &str,
    db_name: &str,
    include_max_txid: impl Fn(u64) -> bool,
) -> Result<Option<DiscoveredLtx>> {
    let snapshots = discover_legacy_snapshots(storage, prefix, db_name).await?;
    let mut best: Option<DiscoveredLtx> = None;

    for snapshot in snapshots {
        if !include_max_txid(snapshot.max_txid) {
            continue;
        }
        match &best {
            None => best = Some(snapshot),
            Some(current) => {
                if snapshot.generation > current.generation
                    || (snapshot.generation == current.generation
                        && snapshot.max_txid > current.max_txid)
                {
                    best = Some(snapshot);
                }
            }
        }
    }

    Ok(best)
}

/// Discover every legacy LTX file for a database.
pub async fn discover_all_legacy_ltx(
    storage: &dyn StorageBackend,
    prefix: &str,
    db_name: &str,
) -> Result<Vec<DiscoveredLtx>> {
    let (_current_txid, max_generation) = discover_legacy_state(storage, prefix, db_name).await?;

    let mut files = Vec::new();
    for (key, min_txid, max_txid) in
        list_legacy_generation_files(storage, prefix, db_name, GENERATION_LIVE).await?
    {
        files.push(DiscoveredLtx {
            key,
            generation: GENERATION_LIVE,
            min_txid,
            max_txid,
            is_snapshot: is_snapshot(GENERATION_LIVE, min_txid, max_txid),
        });
    }
    for generation in 1..=max_generation {
        for (key, min_txid, max_txid) in
            list_legacy_generation_files(storage, prefix, db_name, generation).await?
        {
            files.push(DiscoveredLtx {
                key,
                generation,
                min_txid,
                max_txid,
                is_snapshot: is_snapshot(generation, min_txid, max_txid),
            });
        }
    }

    files.sort_by_key(|file| (file.min_txid, file.generation));
    Ok(files)
}

#[cfg(test)]
mod tests {
    use super::*;
    use async_trait::async_trait;
    use hadb_storage::{CasResult, StorageBackend};
    use std::collections::HashMap;

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

    struct TestStorage {
        objects: HashMap<String, Vec<u8>>,
    }

    #[async_trait]
    impl StorageBackend for TestStorage {
        async fn get(&self, key: &str) -> Result<Option<Vec<u8>>> {
            Ok(self.objects.get(key).cloned())
        }

        async fn put(&self, _key: &str, _data: &[u8]) -> Result<()> {
            Ok(())
        }

        async fn delete(&self, _key: &str) -> Result<()> {
            Ok(())
        }

        async fn list(&self, prefix: &str, after: Option<&str>) -> Result<Vec<String>> {
            let mut keys: Vec<String> = self
                .objects
                .keys()
                .filter(|key| key.starts_with(prefix))
                .filter(|key| after.map(|marker| key.as_str() > marker).unwrap_or(true))
                .cloned()
                .collect();
            keys.sort();
            Ok(keys)
        }

        async fn exists(&self, key: &str) -> Result<bool> {
            Ok(self.objects.contains_key(key))
        }

        async fn put_if_absent(&self, _key: &str, _data: &[u8]) -> Result<CasResult> {
            Ok(CasResult {
                success: true,
                etag: Some("test".into()),
            })
        }

        async fn put_if_match(&self, _key: &str, _data: &[u8], _etag: &str) -> Result<CasResult> {
            Ok(CasResult {
                success: true,
                etag: Some("test".into()),
            })
        }
    }

    #[tokio::test]
    async fn legacy_ltx_discovery_is_owned_by_core_storage_backend() {
        let storage = TestStorage {
            objects: HashMap::from([
                (
                    "backups/app/0000/0000000000000001-0000000000000001.ltx".to_string(),
                    Vec::new(),
                ),
                (
                    "backups/app/0000/0000000000000002-0000000000000003.ltx".to_string(),
                    Vec::new(),
                ),
                (
                    "backups/app/0001/0000000000000001-0000000000000003.ltx".to_string(),
                    Vec::new(),
                ),
                ("backups/app/00000004.ltx".to_string(), Vec::new()),
            ]),
        };

        let (current_txid, max_generation) = discover_legacy_state(&storage, "backups", "app")
            .await
            .unwrap();
        assert_eq!((current_txid, max_generation), (4, 1));

        let snapshot = find_latest_legacy_snapshot_at_or_before(&storage, "backups", "app", 2)
            .await
            .unwrap()
            .expect("snapshot at or before target");
        assert_eq!(
            snapshot.key,
            "backups/app/0000/0000000000000001-0000000000000001.ltx"
        );

        let all = discover_all_legacy_ltx(&storage, "backups", "app")
            .await
            .unwrap();
        assert_eq!(all.len(), 4);
        assert_eq!(all.last().unwrap().max_txid, 4);
    }
}
