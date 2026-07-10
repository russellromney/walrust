//! Legacy Litestream-derived LTX object layout helpers.
//!
//! The root CLI still reads and writes this object layout while Phase 4 moves
//! the implementation into `walrust-core`.

use anyhow::Result;
use chrono::{DateTime, Utc};
use hadb_io::{analyze_retention, RetentionPlan, RetentionPolicy, SnapshotEntry};
use hadb_storage::StorageBackend;
use std::collections::HashSet;
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

/// Build the retention/deletion plan for legacy LTX snapshots.
///
/// The retention policy is time/size-oriented, but a legacy restore needs a
/// snapshot base for retained live incrementals. This planner keeps the latest
/// snapshot overall and also rescues the latest snapshot before the earliest
/// live incremental from deletion.
pub async fn plan_legacy_prune(
    storage: &dyn StorageBackend,
    prefix: &str,
    db_name: &str,
    snapshot_entries: &[SnapshotEntry],
    policy: &RetentionPolicy,
    now: DateTime<Utc>,
) -> Result<RetentionPlan> {
    let mut plan = analyze_retention(snapshot_entries, policy, now);

    let live_incrementals =
        list_legacy_generation_files(storage, prefix, db_name, GENERATION_LIVE).await?;
    let earliest_live_min = live_incrementals
        .iter()
        .filter(|(_, min, max)| !(*min == 1 && *max == 1))
        .map(|(_, min, _)| *min)
        .min();

    let max_snapshot_txid = snapshot_entries.iter().map(|entry| entry.sequence).max();
    let base_for_live_chain = earliest_live_min.and_then(|min| {
        snapshot_entries
            .iter()
            .filter(|entry| entry.sequence < min)
            .map(|entry| entry.sequence)
            .max()
            .or_else(|| snapshot_entries.iter().map(|entry| entry.sequence).min())
    });

    // Rescue every snapshot that bridges a hole in the surviving live
    // incremental chain (E2). Taking a full snapshot at TXID H consumes that
    // TXID: the gen-0 incremental stream jumps from ..H-1 straight to H+1..,
    // leaving a single-TXID hole at H that ONLY the snapshot at H can fill.
    // Deleting that snapshot strands every incremental after the hole from any
    // earlier base, so PITR into that range fails with "restore incremental
    // gap". Protecting the bridge snapshots keeps the whole retained range
    // restorable. (Snapshots older than the live chain's base are still pruned,
    // which shrinks the window honestly rather than corrupting it.)
    let mut live_sorted: Vec<(u64, u64)> = live_incrementals
        .iter()
        .filter(|(_, min, max)| !(*min == 1 && *max == 1))
        .map(|(_, min, max)| (*min, *max))
        .collect();
    live_sorted.sort_unstable();
    let snapshot_txids: std::collections::BTreeSet<u64> = snapshot_entries
        .iter()
        .map(|entry| entry.sequence)
        .collect();
    let mut bridge_snapshots: HashSet<u64> = HashSet::new();
    let mut prev_max: Option<u64> = None;
    for (min, max) in &live_sorted {
        if let Some(pmax) = prev_max {
            if *min > pmax + 1 {
                // Hole in [pmax+1 .. min-1]; the base that reconnects the chain
                // at `min` is the highest snapshot strictly inside the hole
                // (normally exactly at min-1).
                if let Some(&bridge) = snapshot_txids.range(pmax + 1..*min).next_back() {
                    bridge_snapshots.insert(bridge);
                }
            }
        }
        prev_max = Some(prev_max.map_or(*max, |p| p.max(*max)));
    }

    let protected: HashSet<u64> = max_snapshot_txid
        .into_iter()
        .chain(base_for_live_chain)
        .chain(bridge_snapshots)
        .collect();

    let rescued: Vec<_> = plan
        .delete
        .iter()
        .filter(|entry| protected.contains(&entry.sequence))
        .cloned()
        .collect();
    if !rescued.is_empty() {
        plan.delete
            .retain(|entry| !protected.contains(&entry.sequence));
        for entry in &rescued {
            if !plan.keep.iter().any(|kept| kept.sequence == entry.sequence) {
                plan.keep.push(entry.clone());
            }
            plan.bytes_freed = plan.bytes_freed.saturating_sub(entry.size);
        }
    }

    Ok(plan)
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

    #[tokio::test]
    async fn legacy_ltx_compaction_plan_is_owned_by_core_and_rescues_chain_base() {
        let storage = TestStorage {
            objects: HashMap::from([(
                "backups/app/0000/0000000000000004-0000000000000005.ltx".to_string(),
                Vec::new(),
            )]),
        };
        let now = Utc::now();
        let snapshots = vec![
            SnapshotEntry {
                key: "backups/app/0001/0000000000000001-0000000000000003.ltx".to_string(),
                created_at: now - chrono::Duration::days(40),
                sequence: 3,
                size: 10,
            },
            SnapshotEntry {
                key: "backups/app/0002/0000000000000001-0000000000000008.ltx".to_string(),
                created_at: now - chrono::Duration::days(1),
                sequence: 8,
                size: 10,
            },
        ];
        let policy = RetentionPolicy {
            hourly: 0,
            daily: 0,
            weekly: 0,
            monthly: 0,
            minimum: 1,
        };

        let plan = plan_legacy_prune(&storage, "backups", "app", &snapshots, &policy, now)
            .await
            .unwrap();

        let kept: Vec<_> = plan.keep.iter().map(|entry| entry.sequence).collect();
        assert!(
            kept.contains(&3),
            "core compaction must protect the snapshot base for live incrementals"
        );
        assert!(
            kept.contains(&8),
            "core compaction must keep the latest snapshot"
        );
        assert!(
            plan.delete.is_empty(),
            "the policy wanted to delete the old snapshot, but reachability must rescue it"
        );
    }

    #[tokio::test]
    async fn e2_compaction_rescues_bridge_snapshots_for_retained_pitr_points() {
        // Layout mirrors the drill: three snapshot generations at TXIDs 10, 20,
        // 30, each of which punched a single-TXID hole in the gen-0 live chain
        // (2-9, 11-19, 21-29, 31-40). The snapshots at 10, 20, 30 are the ONLY
        // objects supplying TXIDs 10, 20, 30, so each is a load-bearing bridge.
        let storage = TestStorage {
            objects: HashMap::from([
                (
                    "backups/app/0000/0000000000000002-0000000000000009.ltx".to_string(),
                    Vec::new(),
                ),
                (
                    "backups/app/0000/000000000000000b-0000000000000013.ltx".to_string(),
                    Vec::new(),
                ),
                (
                    "backups/app/0000/0000000000000015-000000000000001d.ltx".to_string(),
                    Vec::new(),
                ),
                (
                    "backups/app/0000/000000000000001f-0000000000000028.ltx".to_string(),
                    Vec::new(),
                ),
            ]),
        };
        let now = Utc::now();
        // All three snapshots land in the same hour bucket, so a `hourly=1`
        // policy wants to keep only the latest (30) plus the minimum floor,
        // deleting the intermediate bridges 10 and 20.
        let snapshots = vec![
            SnapshotEntry {
                key: "backups/app/0001/0000000000000001-000000000000000a.ltx".to_string(),
                created_at: now - chrono::Duration::minutes(3),
                sequence: 10,
                size: 10,
            },
            SnapshotEntry {
                key: "backups/app/0002/0000000000000001-0000000000000014.ltx".to_string(),
                created_at: now - chrono::Duration::minutes(2),
                sequence: 20,
                size: 10,
            },
            SnapshotEntry {
                key: "backups/app/0003/0000000000000001-000000000000001e.ltx".to_string(),
                created_at: now - chrono::Duration::minutes(1),
                sequence: 30,
                size: 10,
            },
        ];
        let policy = RetentionPolicy {
            hourly: 1,
            daily: 0,
            weekly: 0,
            monthly: 0,
            minimum: 1,
        };

        let plan = plan_legacy_prune(&storage, "backups", "app", &snapshots, &policy, now)
            .await
            .unwrap();

        let kept: Vec<_> = plan.keep.iter().map(|entry| entry.sequence).collect();
        for txid in [10u64, 20, 30] {
            assert!(
                kept.contains(&txid),
                "snapshot bridging the live-chain hole at {txid} must be retained (kept={kept:?})"
            );
        }
        assert!(
            !plan
                .delete
                .iter()
                .any(|e| [10, 20, 30].contains(&e.sequence)),
            "no bridge snapshot may be deleted: {:?}",
            plan.delete.iter().map(|e| e.sequence).collect::<Vec<_>>()
        );
    }
}
