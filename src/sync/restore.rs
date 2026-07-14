use crate::cache::LocalCache;
use crate::errors::{classify_or_else, WalrustError};
use crate::s3::{self, create_client, parse_bucket};
use anyhow::{bail, Result};
use async_trait::async_trait;
use hadb_storage::{CasResult, StorageBackend};
use hadb_storage_s3::S3Storage;
use std::path::{Path, PathBuf};
use walrust_core::legacy_restore;

use super::manifest::{
    discover_state_from_s3, find_latest_snapshot, list_generation_files, GENERATION_LIVE,
};
use walrust_core::legacy_manifest::{parse_legacy_flat_ltx_filename, parse_ltx_filename};

struct CachedLegacyStorage {
    s3: S3Storage,
    cache: Option<LocalCache>,
}

fn unique_local_native_spool(
    dir: &Path,
    bucket: &str,
    prefix: &str,
    database: &str,
) -> Result<Option<(PathBuf, walrust_core::native_spool::SpoolIdentity)>> {
    let mut roots = Vec::new();
    if dir.join("journal.json").exists() {
        roots.push(dir.to_path_buf());
    }
    let native_root = dir.join("native-v1");
    if let Ok(entries) = std::fs::read_dir(&native_root) {
        roots.extend(entries.filter_map(|entry| entry.ok().map(|entry| entry.path())));
    }
    roots.sort();
    roots.dedup();
    let mut matches = Vec::new();
    for root in roots {
        let Some(identity) = walrust_core::native_spool::NativeSpool::read_identity(&root)? else {
            continue;
        };
        if identity.bucket == bucket && identity.prefix == prefix && identity.database == database {
            matches.push((root, identity));
        }
    }
    if matches.len() > 1 {
        let candidates = matches
            .iter()
            .map(|(root, identity)| format!("{} (lineage {})", root.display(), identity.lineage_id))
            .collect::<Vec<_>>()
            .join(", ");
        bail!(
            "multiple local native spools match s3://{}/{}{}; refusing ambiguous restore: {}",
            bucket,
            prefix,
            database,
            candidates
        );
    }
    Ok(matches.pop())
}

/// True if `key` addresses a compaction **level** object (`.../levels/L{n}/…`).
///
/// Identified **structurally** by path segments — a segment exactly `levels`
/// immediately followed by a segment `L{digits}` — not by a bare substring. A
/// real level object's key is always built as `{db}/levels/L{level}/{file}`
/// (see `compaction::layout::level_subpath`), so this can never false-negative
/// one (which would let a cached LTX be substituted for an HADBP payload — the
/// corruption the bypass exists to prevent). It also cannot be tripped by a
/// database name or prefix that merely contains the text "levels".
fn is_compaction_level_key(key: &str) -> bool {
    let segs: Vec<&str> = key.split('/').collect();
    segs.windows(2).any(|w| {
        w[0] == "levels"
            && w[1].len() >= 2
            && w[1].as_bytes()[0] == b'L'
            && w[1][1..].bytes().all(|b| b.is_ascii_digit())
    })
}

/// Parse the (min_txid, max_txid) interval an object KEY names.
///
/// Canonical keys are `db/GEN/min-max.ltx`; the legacy flat shape `00000003.ltx`
/// covers a single TXID (min == max).
fn key_txid_range(key: &str) -> Option<(u64, u64)> {
    let filename = key.rsplit('/').next().unwrap_or(key);
    parse_ltx_filename(filename)
        .or_else(|| parse_legacy_flat_ltx_filename(filename).map(|txid| (txid, txid)))
}

/// Decide whether the local cache can satisfy a request for `key`.
///
/// Returns `Some(bytes)` ONLY when the cache holds an LTX that (a) decodes and
/// verifies internally (`verify_ltx`, which runs even for litestream NO_CHECKSUM
/// files) and (b) whose encoded `(min, max)` TXID range matches the range the KEY
/// names. A bare max-TXID match is NOT sufficient (B13): two objects from
/// different generations / lineages can share a max TXID, and substituting one
/// for the other silently corrupts a restore. Any mismatch or verification
/// failure returns `None` so the caller falls back to authoritative remote
/// storage instead of trusting an ambiguous local copy.
fn cache_substitute_for_key(cache: &LocalCache, key: &str) -> Option<Vec<u8>> {
    // Cache substitution is bypassed for compaction **level** objects (C3a
    // decision). The restore cache (B13) is an LTX-by-TXID store keyed on the
    // `(min, max)` TXID range; a merged level object lives at `{db}/levels/L*/`
    // with a `{min}-{max}.ltx` name that `key_txid_range` would happily parse,
    // but its payload is **HADBP**, not LTX — substituting a cached LTX for it
    // by matching TXID range would corrupt a leveled restore. Merged objects are
    // coarse and read once, so bypassing the cache for them costs nothing;
    // always fetch them from authoritative storage. The match is structural (path
    // segments, not a bare substring) so it can never false-negative a real level
    // object nor be tripped by a db name/prefix that merely contains "levels".
    if is_compaction_level_key(key) {
        return None;
    }
    let (min_txid, max_txid) = key_txid_range(key)?;
    if !cache.has_txid(max_txid) {
        return None;
    }
    let bytes = match cache.read_ltx(max_txid) {
        Ok(bytes) => bytes,
        Err(e) => {
            tracing::warn!(
                "Cache read for TXID {} failed ({e}); falling back to S3",
                max_txid
            );
            return None;
        }
    };
    match walrust_core::legacy_ltx::verify_ltx(std::io::Cursor::new(&bytes)) {
        Ok(header) => {
            let cached = (header.min_txid.into_inner(), header.max_txid.into_inner());
            if cached == (min_txid, max_txid) {
                tracing::debug!(
                    "Reading TXID {}-{} from cache for {}",
                    min_txid,
                    max_txid,
                    key
                );
                Some(bytes)
            } else {
                tracing::warn!(
                    "Cache entry for max TXID {} spans {:?} but key {} names {:?}; \
                     not substituting (lineage/generation mismatch), falling back to S3",
                    max_txid,
                    cached,
                    key,
                    (min_txid, max_txid)
                );
                None
            }
        }
        Err(e) => {
            tracing::warn!(
                "Cached LTX for TXID {} failed verification ({e}); falling back to S3",
                max_txid
            );
            None
        }
    }
}

#[async_trait]
impl StorageBackend for CachedLegacyStorage {
    async fn get(&self, key: &str) -> Result<Option<Vec<u8>>> {
        if let Some(cache) = &self.cache {
            if let Some(bytes) = cache_substitute_for_key(cache, key) {
                return Ok(Some(bytes));
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

    if let Some(dir) = cache_dir {
        if let Some((root, identity)) = unique_local_native_spool(dir, &bucket_name, &prefix, name)?
        {
            let spool = walrust_core::native_spool::NativeSpool::create_or_open(
                &root,
                identity,
                walrust_core::native_spool::CapacityPolicy {
                    warning_bytes: u64::MAX - 1,
                    hard_bytes: u64::MAX,
                    minimum_free_bytes: 0,
                },
            )?;
            if let Some(seq) = walrust_core::native_restore::restore_local_spool(
                &spool,
                output,
                parsed_point_in_time,
            )? {
                println!(
                    "Restored {} to {} from local native HADBP spool (sequence: {})",
                    name,
                    output.display(),
                    seq
                );
                return Ok(());
            }
        }
    }

    // A complete local native spool is a self-contained recovery source. Do
    // not initialize an S3 client until local restore has been exhausted.
    let client = create_client(endpoint)
        .await
        .map_err(|e| classify_or_else(e, WalrustError::s3))?;

    let storage = CachedLegacyStorage {
        s3: S3Storage::new(client.clone(), bucket_name.clone()),
        cache,
    };
    match walrust_core::native_restore::restore_native_v1(
        &storage,
        &bucket_name,
        &prefix,
        name,
        output,
        parsed_point_in_time,
    )
    .await
    {
        Ok(walrust_core::native_restore::NativeRestoreAvailability::Restored { seq }) => {
            println!(
                "Restored {} to {} (native HADBP sequence: {})",
                name,
                output.display(),
                seq
            );
            return Ok(());
        }
        Ok(
            walrust_core::native_restore::NativeRestoreAvailability::LegacyOnly
            | walrust_core::native_restore::NativeRestoreAvailability::LegacyPoint { .. },
        ) => {}
        Err(error) => {
            if let Some(webhook) = webhook {
                let error_msg = format!("native HADBP restore failed: {error:#}");
                webhook.notify_corruption(name, &error_msg).await;
            }
            return Err(classify_or_else(error, WalrustError::restore));
        }
    }
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
                let send = async move {
                    webhook.notify_corruption(&name, &error_msg).await;
                };
                if tokio::time::timeout(std::time::Duration::from_secs(5), send)
                    .await
                    .is_err()
                {
                    tracing::warn!("corruption webhook timed out after 5s");
                }
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
            let storage = S3Storage::new(client.clone(), bucket_name.clone());
            if let Some(native) =
                walrust_core::native_restore::inspect_native_v1(&storage, &bucket_name, &prefix, db)
                    .await
                    .map_err(|e| classify_or_else(e, WalrustError::s3))?
            {
                println!(
                    "  {} (native HADBP TXID: {}, {} objects, snapshot TXID {}{})",
                    db,
                    native.head_seq,
                    native.object_count,
                    native.latest_snapshot_seq,
                    native
                        .legacy_boundary_txid
                        .map(|seq| format!(", legacy PITR through TXID {seq}"))
                        .unwrap_or_default()
                );
                continue;
            }
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::WebhookConfig;
    use crate::webhook::WebhookSender;
    use std::sync::Arc;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use walrust_core::legacy_manifest::build_ltx_key;

    async fn capture_one_webhook() -> (String, tokio::task::JoinHandle<String>) {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());

        let handle = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut buffer = Vec::new();
            let mut chunk = [0u8; 1024];

            loop {
                let n = stream.read(&mut chunk).await.unwrap();
                assert!(n > 0, "webhook connection closed before request body");
                buffer.extend_from_slice(&chunk[..n]);

                if let Some(header_end) = buffer.windows(4).position(|window| window == b"\r\n\r\n")
                {
                    let headers = String::from_utf8_lossy(&buffer[..header_end]);
                    let content_length = headers
                        .lines()
                        .find_map(|line| {
                            line.strip_prefix("Content-Length:")
                                .or_else(|| line.strip_prefix("content-length:"))
                                .and_then(|value| value.trim().parse::<usize>().ok())
                        })
                        .unwrap_or(0);
                    let body_start = header_end + 4;
                    if buffer.len() >= body_start + content_length {
                        let body = String::from_utf8(
                            buffer[body_start..body_start + content_length].to_vec(),
                        )
                        .unwrap();
                        stream
                            .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 0\r\n\r\n")
                            .await
                            .unwrap();
                        return body;
                    }
                }
            }
        });

        (url, handle)
    }

    /// Start a minimal TCP listener that returns HTTP 500 for every request.
    /// Used as a fake S3 endpoint so `restore()` fails and reaches the corruption
    /// webhook path without needing real credentials or buckets.
    async fn failing_s3_endpoint() -> String {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            loop {
                let (mut stream, _) = listener.accept().await.unwrap();
                let mut buf = [0u8; 512];
                let _ = stream.read(&mut buf).await;
                let _ = stream
                    .write_all(b"HTTP/1.1 500 Internal Server Error\r\nContent-Length: 0\r\n\r\n")
                    .await;
            }
        });
        format!("http://{addr}")
    }

    fn make_snapshot_ltx(txid: u64) -> Vec<u8> {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("src.db");
        let conn = rusqlite::Connection::open(&db_path).unwrap();
        conn.execute_batch(
            "CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT);
             INSERT INTO t (v) VALUES ('a'), ('b'), ('c');",
        )
        .unwrap();
        drop(conn);
        let (bytes, _cs) =
            walrust_core::legacy_ltx::encode_sqlite_snapshot_to_vec(&db_path, 4096, txid).unwrap();
        bytes
    }

    #[test]
    fn cache_substitution_binds_to_key_txid_range_not_bare_txid() {
        // A snapshot LTX always spans (1, txid).
        let bytes = make_snapshot_ltx(5);
        let cache_dir = tempfile::tempdir().unwrap();
        let cache = LocalCache::new_at(cache_dir.path()).unwrap();
        cache.write_snapshot_ltx(5, &bytes).unwrap();

        // Matching key db/GEN/1-5.ltx: range (1,5) matches the cached bytes.
        let matching = build_ltx_key("prefix", "db", 1, 1, 5);
        assert_eq!(
            cache_substitute_for_key(&cache, &matching).as_deref(),
            Some(bytes.as_slice()),
            "matching range must substitute from cache"
        );

        // Divergent key with the SAME max TXID but a different min names a
        // DIFFERENT object (e.g. an incremental 3-5). Bare-TXID substitution (the
        // B13 bug) returns the wrong cached bytes; the fix must refuse it.
        let divergent = build_ltx_key("prefix", "db", 0, 3, 5);
        assert!(
            cache_substitute_for_key(&cache, &divergent).is_none(),
            "same max TXID but different range must NOT be substituted from cache (B13)"
        );

        // A key whose filename does not parse into a TXID interval must not
        // panic and must fall back to authoritative S3 (returns None).
        assert!(
            cache_substitute_for_key(&cache, "db/GEN/not-an-ltx-name").is_none(),
            "unparseable key must fall back to S3, not substitute or crash"
        );
        assert!(
            cache_substitute_for_key(&cache, "").is_none(),
            "empty key must fall back to S3"
        );
    }

    #[test]
    fn local_native_restore_rejects_multiple_matching_lineages() {
        let dir = tempfile::tempdir().unwrap();
        let db_a = dir.path().join("a.sqlite");
        let db_b = dir.path().join("b.sqlite");
        std::fs::File::create(&db_a).unwrap();
        std::fs::File::create(&db_b).unwrap();
        let capacity = walrust_core::native_spool::CapacityPolicy {
            warning_bytes: u64::MAX - 1,
            hard_bytes: u64::MAX,
            minimum_free_bytes: 0,
        };
        for (db, lineage) in [(&db_a, "lineage-a"), (&db_b, "lineage-b")] {
            let identity = walrust_core::native_spool::SpoolIdentity::new(
                db, "bucket", "p/", "db", lineage, 1, None, true,
            )
            .unwrap();
            let root = walrust_core::native_spool::NativeSpool::path_for(dir.path(), &identity);
            walrust_core::native_spool::NativeSpool::create_or_open(&root, identity, capacity)
                .unwrap();
        }

        let error = unique_local_native_spool(dir.path(), "bucket", "p/", "db").unwrap_err();
        assert!(error.to_string().contains("refusing ambiguous restore"));
        assert!(error.to_string().contains("lineage-a"));
        assert!(error.to_string().contains("lineage-b"));
    }

    #[tokio::test]
    async fn local_native_restore_refuses_active_spool_owner_without_cloud_fallback() {
        let dir = tempfile::tempdir().unwrap();
        let db = dir.path().join("active.sqlite");
        std::fs::File::create(&db).unwrap();
        let identity = walrust_core::native_spool::SpoolIdentity::new(
            &db,
            "bucket",
            "p/",
            "db",
            "lineage-active",
            1,
            None,
            true,
        )
        .unwrap();
        let root = walrust_core::native_spool::NativeSpool::path_for(dir.path(), &identity);
        let _owner = walrust_core::native_spool::NativeSpool::create_or_open(
            &root,
            identity,
            walrust_core::native_spool::CapacityPolicy {
                warning_bytes: u64::MAX - 1,
                hard_bytes: u64::MAX,
                minimum_free_bytes: 0,
            },
        )
        .unwrap();
        let output = dir.path().join("restore.sqlite");
        let error = restore(
            "db",
            &output,
            "bucket/p/",
            None,
            None,
            Some(dir.path()),
            None,
        )
        .await
        .unwrap_err();
        let message = format!("{error:#}");
        assert!(message.contains("active watcher"), "{message}");
        assert!(
            message.contains("stop watch before local restore"),
            "{message}"
        );
        assert!(!output.exists());
    }

    #[test]
    fn level_key_detection_is_structural() {
        // Real merged level objects (built as `{db}/levels/L{n}/{file}`) are always
        // detected, so a cached LTX can never be substituted for their HADBP bytes.
        assert!(is_compaction_level_key("p/db/levels/L1/0002-0011.ltx"));
        assert!(is_compaction_level_key("db/levels/L2/00-ff.ltx"));
        assert!(is_compaction_level_key("a/b/c/db/levels/L16/x.ltx"));
        // Normal L0 / snapshot objects are NOT level keys.
        assert!(!is_compaction_level_key("p/db/0000/0002-0002.ltx"));
        assert!(!is_compaction_level_key("p/db/0001/0001-0001.ltx"));
        // Not fooled by a segment that merely contains "levels" or a bad L-suffix.
        assert!(!is_compaction_level_key("p/mylevels/0000/x.ltx"));
        assert!(!is_compaction_level_key("p/db/levels/Lx/x.ltx"));
        assert!(!is_compaction_level_key("p/db/levels/L/x.ltx"));
        assert!(!is_compaction_level_key("p/levelsL1/x.ltx"));
        // A db name literally "levels" does not create a false level key: the
        // following segment ("0000") is not `L{digits}`.
        assert!(!is_compaction_level_key("p/levels/0000/0002-0002.ltx"));
    }

    #[tokio::test]
    async fn test_restore_failure_awaits_corruption_webhook_before_returning() {
        // D7.3: the corruption webhook on the restore exit path must be awaited
        // (with a short timeout) before the function returns. A fire-and-forget
        // spawn would return before the POST is delivered.
        let received = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let received_check = Arc::clone(&received);

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        let webhook_handle = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut buffer = Vec::new();
            let mut chunk = [0u8; 1024];

            loop {
                let n = stream.read(&mut chunk).await.unwrap();
                assert!(n > 0, "webhook connection closed before request body");
                buffer.extend_from_slice(&chunk[..n]);

                if let Some(header_end) = buffer.windows(4).position(|window| window == b"\r\n\r\n")
                {
                    let headers = String::from_utf8_lossy(&buffer[..header_end]);
                    let content_length = headers
                        .lines()
                        .find_map(|line| {
                            line.strip_prefix("Content-Length:")
                                .or_else(|| line.strip_prefix("content-length:"))
                                .and_then(|value| value.trim().parse::<usize>().ok())
                        })
                        .unwrap_or(0);
                    let body_start = header_end + 4;
                    if buffer.len() >= body_start + content_length {
                        // Delay before recording receipt so a fire-and-forget
                        // caller would observe the flag still false on return.
                        tokio::time::sleep(std::time::Duration::from_millis(200)).await;
                        received.store(true, std::sync::atomic::Ordering::SeqCst);
                        stream
                            .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 0\r\n\r\n")
                            .await
                            .unwrap();
                        return;
                    }
                }
            }
        });

        let webhook_sender = Arc::new(WebhookSender::new(vec![WebhookConfig {
            url,
            events: vec!["corruption_detected".to_string()],
            secret: None,
        }]));

        let output_dir = tempfile::tempdir().unwrap();
        let output = output_dir.path().join("restored.db");
        let endpoint = failing_s3_endpoint().await;

        let _err = restore(
            "missing-db",
            &output,
            "s3://bucket/prefix",
            Some(&endpoint),
            None,
            None,
            Some(webhook_sender),
        )
        .await
        .expect_err("restore must fail against the failing S3 endpoint");

        // With the fix, restore awaited the webhook, so the delayed server has
        // already set the flag. With tokio::spawn, it would still be false here.
        assert!(
            received_check.load(std::sync::atomic::Ordering::SeqCst),
            "restore must await the corruption webhook before returning"
        );

        // Clean up the server task.
        let _ = webhook_handle.await;
    }
}
