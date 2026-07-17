//! Test-only sync/snapshot/restore primitives wired straight onto the
//! `hadb_storage::StorageBackend` trait.
//!
//! The production watch path threads WAL frames through a shadow WAL, a disk
//! cache, and a concurrent uploader that all assume a live S3 client. That is
//! the right shape for a long-running daemon but the wrong shape for a
//! deterministic harness that wants to inject storage faults and assert on the
//! outcome of a single snapshot/restore. This module exposes the same data
//! path — encode the DB to LTX, PUT every object through a backend, GET them
//! back and decode with checksum verification — but against a `&dyn
//! StorageBackend` so the DST mock can sit underneath it.
//!
//! It is not a second implementation of the watch loop; it reuses the real
//! [`crate::ltx`] codec (the same encoder/decoder/checksum chain production
//! uses) and the real litestream key layout. A fault injected by the mock
//! (corruption, partial write, random error, eventual consistency) therefore
//! flows through the real encode → PUT → GET → decode → checksum path, and a
//! corrupt object is caught by the same `apply_ltx_to_db` / `decode_to_db`
//! verification the daemon relies on.
//!
//! On-storage layout (litestream-compatible):
//!
//! ```text
//! {prefix}{name}/0000/{min:016x}-{max:016x}.ltx   live generation
//! ```
//!
//! The base snapshot is `0000000000000001-0000000000000001.ltx` (min==max==1).
//! Incrementals carry a pre/post checksum chain so a missing or torn link is
//! detected on restore.

use crate::ltx;
use crate::ltx::Checksum;
use crate::retry::RetryPolicy;
use anyhow::{anyhow, Context, Result};
use hadb_storage::StorageBackend;
use std::path::{Path, PathBuf};

/// Live generation folder (litestream `0000`).
const GENERATION_LIVE: u64 = 0;

/// Default page size for harness databases.
const PAGE_SIZE: u32 = 4096;

fn db_name_from_path(db_path: &Path) -> String {
    db_path
        .file_stem()
        .map(|s| s.to_string_lossy().into_owned())
        .unwrap_or_else(|| "db".to_string())
}

/// Build the live-generation key for an LTX object.
fn ltx_key(prefix: &str, name: &str, min_txid: u64, max_txid: u64) -> String {
    format!(
        "{}{}/{:04x}/{:016x}-{:016x}.ltx",
        prefix, name, GENERATION_LIVE, min_txid, max_txid
    )
}

/// Parse `min`/`max` TXIDs out of a live-generation key.
fn parse_ltx_key(prefix: &str, name: &str, key: &str) -> Option<(u64, u64)> {
    let folder = format!("{}{}/{:04x}/", prefix, name, GENERATION_LIVE);
    let file = key.strip_prefix(&folder)?;
    let stem = file.strip_suffix(".ltx")?;
    let (min_s, max_s) = stem.split_once('-')?;
    let min = u64::from_str_radix(min_s, 16).ok()?;
    let max = u64::from_str_radix(max_s, 16).ok()?;
    Some((min, max))
}

/// Per-database sync state carried across snapshot/sync calls.
///
/// Mirrors the fields the production `DbState` tracks that matter for a
/// fault-injection harness: the database name, the current TXID, and the
/// checksum chain head used to link the next incremental.
pub struct SyncState {
    /// Database name (file stem) — also the storage key namespace.
    pub name: String,
    /// Path to the database file on disk.
    pub db_path: PathBuf,
    /// Highest committed TXID written to storage so far.
    pub current_txid: u64,
    /// Post-apply checksum of the most recent object — the pre-apply checksum
    /// of the next incremental. `None` until the base snapshot is written.
    pub last_checksum: Option<Checksum>,
    /// Page snapshot of the DB as it was at `current_txid`, used to compute the
    /// changed-page delta for the next [`sync_wal`].
    last_pages: Vec<Vec<u8>>,
}

impl SyncState {
    /// Create fresh sync state for a database path.
    pub fn new(db_path: PathBuf) -> Result<Self> {
        let name = db_name_from_path(&db_path);
        Ok(Self {
            name,
            db_path,
            current_txid: 0,
            last_checksum: None,
            last_pages: Vec::new(),
        })
    }
}

/// Read a database file into page-sized chunks (zero-padding a short tail).
fn read_db_pages(db_path: &Path) -> Result<Vec<Vec<u8>>> {
    let data = std::fs::read(db_path)
        .with_context(|| format!("reading db for snapshot: {}", db_path.display()))?;
    let ps = PAGE_SIZE as usize;
    let mut pages = Vec::new();
    let mut off = 0;
    while off < data.len() {
        let end = (off + ps).min(data.len());
        let mut page = data[off..end].to_vec();
        if page.len() < ps {
            page.resize(ps, 0);
        }
        pages.push(page);
        off += ps;
    }
    Ok(pages)
}

/// Take a full-DB snapshot and PUT it through the backend.
///
/// Every snapshot is encoded with the real LTX encoder and stored as a fresh
/// object keyed by its TXID. The first snapshot is the litestream base
/// (`min==max==1`); later snapshots advance the TXID and reset the checksum
/// chain head (a snapshot is a self-contained full-DB base).
pub async fn take_snapshot(
    storage: &dyn StorageBackend,
    prefix: &str,
    state: &mut SyncState,
) -> Result<()> {
    let next_txid = if state.current_txid == 0 {
        1
    } else {
        state.current_txid + 1
    };

    let mut buf = Vec::new();
    ltx::encode_snapshot(&mut buf, &state.db_path, PAGE_SIZE, next_txid)
        .context("encode_snapshot")?;

    // For the base, key it as min==max==1 so it is recognized as a snapshot.
    // Later snapshots are full bases too but keyed at their own TXID range.
    let (min_txid, max_txid) = if state.current_txid == 0 {
        (1, 1)
    } else {
        (next_txid, next_txid)
    };
    let key = ltx_key(prefix, &state.name, min_txid, max_txid);

    storage
        .put(&key, &buf)
        .await
        .with_context(|| format!("put snapshot {}", key))?;

    // The snapshot's post-apply checksum is the chain head for the next
    // incremental. Compute it from the on-disk DB pages.
    let pages = read_db_pages(&state.db_path)?;
    state.last_checksum = Some(full_db_checksum(&pages));
    state.last_pages = pages;
    state.current_txid = max_txid;
    Ok(())
}

/// Compute the full-DB chain checksum (chain over every page from a zero base).
fn full_db_checksum(pages: &[Vec<u8>]) -> Checksum {
    let indexed: Vec<(u32, Vec<u8>)> = pages
        .iter()
        .enumerate()
        .map(|(i, p)| ((i + 1) as u32, p.clone()))
        .collect();
    ltx::chain_checksum(Checksum::new(0), &indexed)
}

/// Encode and PUT an incremental covering the pages that changed since the
/// last snapshot/sync.
///
/// The incremental carries `pre_apply_checksum = state.last_checksum`, so a
/// gap in the chain (a lost prior object, or a torn write to this one) is
/// caught by `apply_ltx_to_db` on restore. If nothing changed, this is a
/// no-op (no empty object is written).
pub async fn sync_wal(
    storage: &dyn StorageBackend,
    prefix: &str,
    state: &mut SyncState,
) -> Result<()> {
    if state.current_txid == 0 {
        // No base yet — a sync before a snapshot is meaningless; take a base.
        return take_snapshot(storage, prefix, state).await;
    }

    let pages = read_db_pages(&state.db_path)?;
    // Compute the changed pages relative to the last known state.
    let mut changed: Vec<(u32, Vec<u8>)> = Vec::new();
    for (i, page) in pages.iter().enumerate() {
        let pn = (i + 1) as u32;
        let prior = state.last_pages.get(i);
        if prior.map(|p| p != page).unwrap_or(true) {
            changed.push((pn, page.clone()));
        }
    }

    if changed.is_empty() {
        return Ok(());
    }

    let min_txid = state.current_txid + 1;
    let max_txid = min_txid;
    let pre = state.last_checksum;
    // The post-apply checksum after these pages is the full-DB chain checksum
    // of the new page set (so the next incremental's pre matches a restore that
    // applied this one).
    let post_full = full_db_checksum(&pages);

    // Encode an incremental whose chain links pre -> the chained delta. The
    // restore path verifies post_apply against chain_checksum(pre, decoded
    // pages), so we must store that exact value as the trailer.
    let post_incremental = ltx::chain_checksum(pre.unwrap_or(Checksum::new(0)), &changed);

    let mut buf = Vec::new();
    let commit_page = pages.len() as u32;
    ltx::encode_wal_changes(
        &mut buf,
        &changed,
        PAGE_SIZE,
        min_txid,
        max_txid,
        commit_page,
        pre,
        post_incremental,
    )
    .context("encode_wal_changes")?;

    let key = ltx_key(prefix, &state.name, min_txid, max_txid);
    storage
        .put(&key, &buf)
        .await
        .with_context(|| format!("put incremental {}", key))?;

    state.current_txid = max_txid;
    state.last_checksum = Some(post_full);
    state.last_pages = pages;
    Ok(())
}

/// Snapshot with retry, used by stress/soak loops under fault injection.
pub async fn take_snapshot_with_retry(
    storage: &dyn StorageBackend,
    prefix: &str,
    state: &mut SyncState,
    retry: &RetryPolicy,
) -> Result<()> {
    // Encode once; retry only the PUT (the expensive, fault-prone step).
    let next_txid = if state.current_txid == 0 {
        1
    } else {
        state.current_txid + 1
    };
    let mut buf = Vec::new();
    ltx::encode_snapshot(&mut buf, &state.db_path, PAGE_SIZE, next_txid)
        .context("encode_snapshot")?;
    let (min_txid, max_txid) = if state.current_txid == 0 {
        (1, 1)
    } else {
        (next_txid, next_txid)
    };
    let key = ltx_key(prefix, &state.name, min_txid, max_txid);

    retry
        .execute(|| async {
            storage.put(&key, &buf).await?;
            Ok(())
        })
        .await
        .with_context(|| format!("put snapshot {} (with retry)", key))?;

    let pages = read_db_pages(&state.db_path)?;
    state.last_checksum = Some(full_db_checksum(&pages));
    state.last_pages = pages;
    state.current_txid = max_txid;
    Ok(())
}

/// `sync_wal` with retry on the PUT.
pub async fn sync_wal_with_retry(
    storage: &dyn StorageBackend,
    prefix: &str,
    state: &mut SyncState,
    retry: &RetryPolicy,
) -> Result<()> {
    if state.current_txid == 0 {
        return take_snapshot_with_retry(storage, prefix, state, retry).await;
    }

    let pages = read_db_pages(&state.db_path)?;
    let mut changed: Vec<(u32, Vec<u8>)> = Vec::new();
    for (i, page) in pages.iter().enumerate() {
        let pn = (i + 1) as u32;
        if state.last_pages.get(i).map(|p| p != page).unwrap_or(true) {
            changed.push((pn, page.clone()));
        }
    }
    if changed.is_empty() {
        return Ok(());
    }

    let min_txid = state.current_txid + 1;
    let max_txid = min_txid;
    let pre = state.last_checksum;
    let post_full = full_db_checksum(&pages);
    let post_incremental = ltx::chain_checksum(pre.unwrap_or(Checksum::new(0)), &changed);

    let mut buf = Vec::new();
    let commit_page = pages.len() as u32;
    ltx::encode_wal_changes(
        &mut buf,
        &changed,
        PAGE_SIZE,
        min_txid,
        max_txid,
        commit_page,
        pre,
        post_incremental,
    )
    .context("encode_wal_changes")?;

    let key = ltx_key(prefix, &state.name, min_txid, max_txid);
    retry
        .execute(|| async {
            storage.put(&key, &buf).await?;
            Ok(())
        })
        .await
        .with_context(|| format!("put incremental {} (with retry)", key))?;

    state.current_txid = max_txid;
    state.last_checksum = Some(post_full);
    state.last_pages = pages;
    Ok(())
}

/// A manifest entry, mirroring the production `LtxEntry` surface tests touch.
#[derive(Debug, Clone)]
pub struct ManifestEntry {
    pub min_txid: u64,
    pub max_txid: u64,
    pub is_snapshot: bool,
}

/// A reconstructed manifest, discovered by listing the live generation.
#[derive(Debug, Clone, Default)]
pub struct Manifest {
    pub name: String,
    pub files: Vec<ManifestEntry>,
}

/// Discover the manifest for a database by listing its objects.
///
/// There is no `manifest.json` here (the production watch path doesn't write
/// one either — state is discovered from the listing). Faults on `list` (a
/// random error, or an object still invisible under eventual consistency)
/// surface as a short/empty manifest, exactly as a stale LIST would.
pub async fn load_manifest(
    storage: &dyn StorageBackend,
    prefix: &str,
    name: &str,
) -> Result<Manifest> {
    let folder = format!("{}{}/{:04x}/", prefix, name, GENERATION_LIVE);
    let keys = storage.list(&folder, None).await.context("list manifest")?;

    let mut files: Vec<ManifestEntry> = Vec::new();
    for key in &keys {
        if let Some((min, max)) = parse_ltx_key(prefix, name, key) {
            files.push(ManifestEntry {
                min_txid: min,
                max_txid: max,
                is_snapshot: min == 1 && max == 1,
            });
        }
    }
    // Snapshot (base) first, then incrementals in TXID order.
    files.sort_by_key(|e| (!e.is_snapshot, e.max_txid));
    Ok(Manifest {
        name: name.to_string(),
        files,
    })
}

/// Restore a database from storage to `output`.
///
/// Walks the same path as production restore: find the base snapshot, decode
/// it (full DB), then apply each incremental in TXID order with checksum-chain
/// verification. `point_in_time` is `Some("txid:N")` or a bare `Some("N")` to
/// stop at the latest object with `max_txid <= N`; `None` restores to latest.
///
/// Faults injected by the backend are caught here, not swallowed:
/// - a corrupt object fails `decode_to_db` / `apply_ltx_to_db` (checksum or
///   bounds), so restore returns `Err` rather than producing a bad DB;
/// - a missing base or a chain gap returns `Err`;
/// - an object still invisible under eventual consistency surfaces as a GET
///   error and is propagated.
pub async fn restore(
    storage: &dyn StorageBackend,
    prefix: &str,
    name: &str,
    output: &Path,
    point_in_time: Option<&str>,
) -> Result<()> {
    let manifest = load_manifest(storage, prefix, name).await?;
    if manifest.files.is_empty() {
        return Err(anyhow!("no LTX objects found for database {}", name));
    }

    let target_txid = match point_in_time {
        None => u64::MAX,
        Some(pit) => {
            let raw = pit.strip_prefix("txid:").unwrap_or(pit);
            raw.parse::<u64>()
                .map_err(|_| anyhow!("invalid point_in_time {pit:?}; expected txid:N or N"))?
        }
    };

    // The base snapshot is the chain root.
    let base = manifest
        .files
        .iter()
        .find(|e| e.is_snapshot)
        .ok_or_else(|| anyhow!("no base snapshot for database {}", name))?;

    let base_key = ltx_key(prefix, name, base.min_txid, base.max_txid);
    let base_bytes = storage
        .get(&base_key)
        .await
        .with_context(|| format!("get base {}", base_key))?
        .ok_or_else(|| anyhow!("base snapshot object missing: {}", base_key))?;

    let decode = ltx::decode_to_db(std::io::Cursor::new(base_bytes), output)
        .with_context(|| format!("decode base {}", base_key))?;
    let mut final_txid = base.max_txid;
    let _ = decode; // post_apply_checksum verified internally by the codec

    // Apply incrementals in order, stopping at the requested point.
    let mut incrementals: Vec<&ManifestEntry> = manifest
        .files
        .iter()
        .filter(|e| !e.is_snapshot && e.max_txid <= target_txid)
        .collect();
    incrementals.sort_by_key(|e| e.max_txid);

    for inc in incrementals {
        let key = ltx_key(prefix, name, inc.min_txid, inc.max_txid);
        let bytes = storage
            .get(&key)
            .await
            .with_context(|| format!("get incremental {}", key))?
            .ok_or_else(|| anyhow!("incremental object missing: {}", key))?;
        ltx::apply_ltx_to_db(std::io::Cursor::new(bytes), output)
            .with_context(|| format!("apply incremental {}", key))?;
        final_txid = inc.max_txid;
    }

    tracing::debug!("restored {} to TXID {}", name, final_txid);
    Ok(())
}
