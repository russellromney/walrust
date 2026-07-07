//! Legacy WAL-to-LTX sync engine.

use crate::legacy_cache::LocalCache;
use crate::legacy_ltx::{self as ltx, Checksum};
use crate::legacy_manifest::{build_ltx_key, discover_legacy_state, GENERATION_LIVE};
use crate::legacy_uploader::UploadMessage;
use crate::shadow::ShadowWal;
use crate::wal;
use anyhow::{anyhow, Result};
use hadb_storage::StorageBackend;
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tokio::sync::mpsc;

#[derive(Clone)]
pub struct SyncInput {
    pub db_path: PathBuf,
    pub name: String,
    pub wal_path: PathBuf,
    pub wal_offset: u64,
    pub wal_generation: u64,
    pub current_txid: u64,
    pub db_checksum: Option<u64>,
    pub wal_salt: Option<(u32, u32)>,
    pub wal_checksum_chain: Option<(u32, u32)>,
}

pub struct SyncOutput {
    pub db_path: PathBuf,
    pub frame_count: u64,
    pub new_wal_offset: u64,
    pub new_current_txid: u64,
    pub new_db_checksum: Option<u64>,
    pub checkpoint_detected: bool,
    pub new_wal_generation: u64,
    pub new_wal_salt: Option<(u32, u32)>,
    pub new_wal_checksum_chain: Option<(u32, u32)>,
}

pub struct SnapshotUploadOutput {
    pub key: String,
    pub generation: u64,
    pub min_txid: u64,
    pub max_txid: u64,
    pub size_bytes: u64,
    pub checksum: u64,
}

pub async fn sync_wal_to_storage(
    storage: &dyn StorageBackend,
    prefix: &str,
    input: SyncInput,
) -> Result<SyncOutput> {
    if input.current_txid == 0 {
        tracing::debug!(
            "{}: Initial sync - creating snapshot from database file",
            input.name
        );

        let page_size = match wal::read_header(&input.wal_path).await? {
            Some(header) => header.page_size,
            None => {
                ensure_database_in_wal_mode(&input.db_path, &input.name).await?;
                4096
            }
        };

        let db_path_for_encode = input.db_path.clone();
        let db_name_for_error = input.name.clone();
        let new_txid = 1u64;

        let (ltx_buffer, db_checksum_new) = tokio::task::spawn_blocking(move || {
            ltx::encode_sqlite_snapshot_to_vec(&db_path_for_encode, page_size, new_txid).map_err(
                |e| {
                    anyhow::anyhow!(
                        "{}: Initial snapshot encode failed: {}",
                        db_name_for_error,
                        e
                    )
                },
            )
        })
        .await??;

        let ltx_size = ltx_buffer.len() as u64;
        let ltx_key = build_ltx_key(prefix, &input.name, 1, 1, new_txid);

        storage.put(&ltx_key, &ltx_buffer).await?;

        tracing::info!(
            "{}: Created initial snapshot LTX ({} bytes, TXID 1-{}) -> {}",
            input.name,
            ltx_size,
            new_txid,
            ltx_key
        );

        return Ok(SyncOutput {
            db_path: input.db_path,
            frame_count: 1,
            new_wal_offset: 0,
            new_current_txid: new_txid,
            new_db_checksum: Some(db_checksum_new.into_inner()),
            checkpoint_detected: false,
            new_wal_generation: input.wal_generation,
            new_wal_salt: None,
            new_wal_checksum_chain: None,
        });
    }

    let header = match wal::read_header(&input.wal_path).await? {
        Some(header) => header,
        None => {
            ensure_database_in_wal_mode(&input.db_path, &input.name).await?;
            return Ok(SyncOutput {
                db_path: input.db_path,
                frame_count: 0,
                new_wal_offset: input.wal_offset,
                new_current_txid: input.current_txid,
                new_db_checksum: input.db_checksum,
                checkpoint_detected: false,
                new_wal_generation: input.wal_generation,
                new_wal_salt: input.wal_salt,
                new_wal_checksum_chain: input.wal_checksum_chain,
            });
        }
    };

    let wal_offset = input.wal_offset;
    let wal_generation = input.wal_generation;
    let db_checksum = input.db_checksum;
    let checkpoint_detected = false;
    let mut wal_salt = input.wal_salt;
    let mut wal_checksum_chain = input.wal_checksum_chain;

    let current_size = wal::get_wal_size(&input.wal_path).await?;
    let current_salt = header.salt();
    let size_rollover = current_size < wal_offset;
    let salt_rollover = matches!(wal_salt, Some(prev) if prev != current_salt);
    if size_rollover || salt_rollover {
        tracing::warn!(
            "{}: WAL rollover detected (size_rollover={}, salt_rollover={}); publishing a new snapshot instead of an incremental across the gap",
            input.name,
            size_rollover,
            salt_rollover
        );
        return upload_rollover_snapshot(storage, prefix, input, wal_generation + 1).await;
    }
    wal_salt = Some(current_salt);

    let chain_seed = if wal_offset == 0 || wal_offset == wal::WAL_HEADER_SIZE {
        None
    } else {
        wal_checksum_chain
    };

    let (page_map, frame_count, new_offset, final_db_size, _commit_count, new_chain) =
        wal::read_frames_as_page_map_checked(
            &input.wal_path,
            header.page_size,
            wal_offset,
            chain_seed,
        )
        .await?;
    if let Some(chain) = new_chain {
        wal_checksum_chain = Some(chain);
    }

    if page_map.is_empty() {
        return Ok(SyncOutput {
            db_path: input.db_path,
            frame_count: 0,
            new_wal_offset: wal_offset,
            new_current_txid: input.current_txid,
            new_db_checksum: db_checksum,
            checkpoint_detected,
            new_wal_generation: wal_generation,
            new_wal_salt: wal_salt,
            new_wal_checksum_chain: wal_checksum_chain,
        });
    }

    let page_size = header.page_size;
    let pages: Vec<(u32, Vec<u8>)> = page_map.into_iter().collect();
    let pre_checksum = match db_checksum {
        Some(checksum) => Checksum::new(checksum),
        None => {
            tracing::debug!(
                "{}: Computing checksum from database (no cached value)",
                input.name
            );
            ltx::compute_checksum_from_file(&input.db_path)?
        }
    };

    let min_txid = input.current_txid + 1;
    let max_txid = min_txid + pages.len() as u64 - 1;
    let commit_page = if final_db_size > 0 {
        final_db_size
    } else {
        let db_size = std::fs::metadata(&input.db_path)?.len();
        (db_size / page_size as u64) as u32
    };
    let expected_post = ltx::chain_checksum(pre_checksum, &pages);

    let unique_pages = pages.len();
    let estimated_size = unique_pages
        .saturating_mul(page_size as usize)
        .saturating_mul(2);
    let db_name_for_error = input.name.clone();
    let page_nums: Vec<u32> = pages.iter().map(|(number, _)| *number).collect();
    let (ltx_buffer, post_checksum) = tokio::task::spawn_blocking(move || {
        let mut ltx_buffer = Vec::with_capacity(estimated_size);
        let post_checksum = ltx::encode_wal_changes(
            &mut ltx_buffer,
            &pages,
            page_size,
            min_txid,
            max_txid,
            commit_page,
            Some(pre_checksum),
            expected_post,
        )
        .map_err(|e| {
            anyhow::anyhow!(
                "{}: LTX encode failed (pages={:?}, page_size={}, txid={}-{}, commit={}): {}",
                db_name_for_error,
                page_nums,
                page_size,
                min_txid,
                max_txid,
                commit_page,
                e
            )
        })?;
        Ok::<_, anyhow::Error>((ltx_buffer, post_checksum))
    })
    .await??;

    let ltx_size = ltx_buffer.len() as u64;
    let ltx_key = build_ltx_key(prefix, &input.name, GENERATION_LIVE, min_txid, max_txid);

    storage.put(&ltx_key, &ltx_buffer).await?;

    tracing::info!(
        "{}: Synced {} WAL frames as incremental LTX ({} bytes, {} unique pages, TXID {}-{}) -> {}",
        input.name,
        frame_count,
        ltx_size,
        unique_pages,
        min_txid,
        max_txid,
        ltx_key
    );

    Ok(SyncOutput {
        db_path: input.db_path,
        frame_count: frame_count as u64,
        new_wal_offset: new_offset,
        new_current_txid: max_txid,
        new_db_checksum: Some(post_checksum.into_inner()),
        checkpoint_detected,
        new_wal_generation: wal_generation,
        new_wal_salt: wal_salt,
        new_wal_checksum_chain: wal_checksum_chain,
    })
}

pub async fn sync_wal_to_cache(
    input: &SyncInput,
    cache: &Arc<LocalCache>,
    shadow: &Arc<tokio::sync::Mutex<ShadowWal>>,
    upload_tx: &mpsc::Sender<UploadMessage>,
) -> Result<SyncOutput> {
    if input.current_txid == 0 {
        tracing::debug!(
            "{}: Initial sync - creating snapshot from database file",
            input.name
        );

        let page_size = {
            let shadow_guard = shadow.lock().await;
            shadow_guard.page_size()
        };

        let db_path_for_encode = input.db_path.clone();
        let db_name_for_error = input.name.clone();
        let new_txid = 1u64;

        let (ltx_buffer, db_checksum_new) = tokio::task::spawn_blocking(move || {
            ltx::encode_sqlite_snapshot_to_vec(&db_path_for_encode, page_size, new_txid).map_err(
                |e| {
                    anyhow::anyhow!(
                        "{}: Initial snapshot encode failed: {}",
                        db_name_for_error,
                        e
                    )
                },
            )
        })
        .await??;

        let ltx_size = ltx_buffer.len();
        cache.write_snapshot_ltx(new_txid, &ltx_buffer)?;

        if let Err(e) = upload_tx.send(UploadMessage::Upload(new_txid)).await {
            tracing::warn!(
                "{}: Failed to notify uploader for TXID {}: {}",
                input.name,
                new_txid,
                e
            );
        }

        tracing::info!(
            "{}: Created initial snapshot LTX ({} bytes, TXID 1) -> cache",
            input.name,
            ltx_size
        );

        return Ok(SyncOutput {
            db_path: input.db_path.clone(),
            frame_count: 1,
            new_wal_offset: 0,
            new_current_txid: new_txid,
            new_db_checksum: Some(db_checksum_new.into_inner()),
            checkpoint_detected: false,
            new_wal_generation: input.wal_generation,
            new_wal_salt: None,
            new_wal_checksum_chain: None,
        });
    }

    let mut shadow_guard = shadow.lock().await;
    let page_size = shadow_guard.page_size();
    let (frames, new_offset) = shadow_guard.copy_frames(input.wal_offset).await?;
    let shadow_gen = shadow_guard.generation();
    let checkpoint_detected = shadow_gen > input.wal_generation;
    let wal_generation = shadow_gen;
    let db_checksum = input.db_checksum;
    drop(shadow_guard);

    if frames.is_empty() {
        return Ok(SyncOutput {
            db_path: input.db_path.clone(),
            frame_count: 0,
            new_wal_offset: new_offset,
            new_current_txid: input.current_txid,
            new_db_checksum: db_checksum,
            checkpoint_detected,
            new_wal_generation: wal_generation,
            new_wal_salt: input.wal_salt,
            new_wal_checksum_chain: input.wal_checksum_chain,
        });
    }

    let frame_count = frames.len();
    let mut final_db_size = 0u32;
    let mut page_map: HashMap<u32, Vec<u8>> = HashMap::new();
    for frame in frames {
        if frame.db_size > 0 {
            final_db_size = frame.db_size;
        }
        page_map.insert(frame.page_number, frame.data);
    }

    let pages: Vec<(u32, Vec<u8>)> = page_map.into_iter().collect();
    let pre_checksum = match db_checksum {
        Some(cs) => Checksum::new(cs),
        None => {
            tracing::debug!(
                "{}: Computing checksum from database (no cached value)",
                input.name
            );
            ltx::compute_checksum_from_file(&input.db_path)?
        }
    };

    let min_txid = input.current_txid + 1;
    let max_txid = min_txid + pages.len() as u64 - 1;
    let commit_page = if final_db_size > 0 {
        final_db_size
    } else {
        let db_size = std::fs::metadata(&input.db_path)?.len();
        (db_size / page_size as u64) as u32
    };

    let expected_post = ltx::chain_checksum(pre_checksum, &pages);
    let unique_pages = pages.len();
    let estimated_size = unique_pages
        .saturating_mul(page_size as usize)
        .saturating_mul(2);
    let db_name_for_error = input.name.clone();
    let page_nums: Vec<u32> = pages.iter().map(|(n, _)| *n).collect();

    let (ltx_buffer, post_checksum) = tokio::task::spawn_blocking(move || {
        let mut ltx_buffer = Vec::with_capacity(estimated_size);
        let post_checksum = ltx::encode_wal_changes(
            &mut ltx_buffer,
            &pages,
            page_size,
            min_txid,
            max_txid,
            commit_page,
            Some(pre_checksum),
            expected_post,
        )
        .map_err(|e| {
            anyhow::anyhow!(
                "{}: LTX encode failed (pages={:?}, page_size={}, txid={}-{}, commit={}): {}",
                db_name_for_error,
                page_nums,
                page_size,
                min_txid,
                max_txid,
                commit_page,
                e
            )
        })?;
        Ok::<_, anyhow::Error>((ltx_buffer, post_checksum))
    })
    .await??;

    let ltx_size = ltx_buffer.len();
    cache.write_ltx(max_txid, &ltx_buffer)?;

    if let Err(e) = upload_tx.send(UploadMessage::Upload(max_txid)).await {
        tracing::warn!(
            "{}: Failed to notify uploader for TXID {}: {}",
            input.name,
            max_txid,
            e
        );
    }

    tracing::info!(
        "{}: Synced {} WAL frames to cache ({} bytes, {} unique pages, TXID {}-{})",
        input.name,
        frame_count,
        ltx_size,
        unique_pages,
        min_txid,
        max_txid
    );

    Ok(SyncOutput {
        db_path: input.db_path.clone(),
        frame_count: frame_count as u64,
        new_wal_offset: new_offset,
        new_current_txid: max_txid,
        new_db_checksum: Some(post_checksum.into_inner()),
        checkpoint_detected,
        new_wal_generation: wal_generation,
        new_wal_salt: input.wal_salt,
        new_wal_checksum_chain: input.wal_checksum_chain,
    })
}

pub async fn take_snapshot_to_storage(
    storage: &dyn StorageBackend,
    prefix: &str,
    input: SyncInput,
) -> Result<SyncOutput> {
    let snapshot =
        snapshot_database_to_storage(storage, prefix, &input.name, &input.db_path).await?;

    let wal_salt = wal::read_header(&input.wal_path)
        .await
        .ok()
        .flatten()
        .map(|h| h.salt());

    Ok(SyncOutput {
        db_path: input.db_path,
        frame_count: 1,
        new_wal_offset: 0,
        new_current_txid: snapshot.max_txid,
        new_db_checksum: Some(snapshot.checksum),
        checkpoint_detected: true,
        new_wal_generation: input.wal_generation + 1,
        new_wal_salt: wal_salt,
        new_wal_checksum_chain: None,
    })
}

pub async fn snapshot_database_to_storage(
    storage: &dyn StorageBackend,
    prefix: &str,
    name: &str,
    database: &Path,
) -> Result<SnapshotUploadOutput> {
    checkpoint_wal_passive(database).await?;

    let page_size = get_page_size(database).await?;
    let (current_txid, current_gen) = discover_legacy_state(storage, prefix, name).await?;
    let new_txid = current_txid + 1;
    let snapshot_gen = current_gen + 1;
    let ltx_key = build_ltx_key(prefix, name, snapshot_gen, 1, new_txid);

    let db_path_for_encode = database.to_path_buf();
    let db_name_for_error = name.to_string();
    let (ltx_buffer, db_checksum_new) = tokio::task::spawn_blocking(move || {
        ltx::encode_sqlite_snapshot_to_vec(&db_path_for_encode, page_size, new_txid).map_err(|e| {
            anyhow::anyhow!(
                "{}: Snapshot encode failed for {}: {}",
                db_name_for_error,
                db_path_for_encode.display(),
                e
            )
        })
    })
    .await??;

    let ltx_size = ltx_buffer.len() as u64;
    storage.put(&ltx_key, &ltx_buffer).await?;
    let checksum = db_checksum_new.into_inner();

    tracing::info!(
        "{}: LTX snapshot uploaded (gen {}, TXID 1-{}, {} bytes, checksum {:#x}) -> {}",
        name,
        snapshot_gen,
        new_txid,
        ltx_size,
        checksum,
        ltx_key
    );

    Ok(SnapshotUploadOutput {
        key: ltx_key,
        generation: snapshot_gen,
        min_txid: 1,
        max_txid: new_txid,
        size_bytes: ltx_size,
        checksum,
    })
}

async fn upload_rollover_snapshot(
    storage: &dyn StorageBackend,
    prefix: &str,
    input: SyncInput,
    new_wal_generation: u64,
) -> Result<SyncOutput> {
    checkpoint_wal_truncate(&input.db_path).await?;

    let page_size = get_page_size(&input.db_path).await?;
    let db_path_for_encode = input.db_path.clone();
    let new_txid = input.current_txid + 1;
    let db_name_for_error = input.name.clone();

    let (ltx_buffer, db_checksum_new) = tokio::task::spawn_blocking(move || {
        ltx::encode_sqlite_snapshot_to_vec(&db_path_for_encode, page_size, new_txid).map_err(|e| {
            anyhow::anyhow!(
                "{}: Rollover snapshot encode failed: {}",
                db_name_for_error,
                e
            )
        })
    })
    .await??;

    let ltx_size = ltx_buffer.len() as u64;
    let (_current_txid, current_gen) = discover_legacy_state(storage, prefix, &input.name).await?;
    let snapshot_gen = (current_gen + 1).max(1);
    let ltx_key = build_ltx_key(prefix, &input.name, snapshot_gen, 1, new_txid);

    storage.put(&ltx_key, &ltx_buffer).await?;

    tracing::info!(
        "{}: Rollover snapshot uploaded (gen {}, TXID 1-{}, {} bytes, checksum {:#x}) -> {}",
        input.name,
        snapshot_gen,
        new_txid,
        ltx_size,
        db_checksum_new.into_inner(),
        ltx_key
    );

    let new_wal_salt = wal::read_header(&input.wal_path)
        .await
        .ok()
        .flatten()
        .map(|header| header.salt());

    Ok(SyncOutput {
        db_path: input.db_path,
        frame_count: 1,
        new_wal_offset: 0,
        new_current_txid: new_txid,
        new_db_checksum: Some(db_checksum_new.into_inner()),
        checkpoint_detected: true,
        new_wal_generation,
        new_wal_salt,
        new_wal_checksum_chain: None,
    })
}

async fn checkpoint_wal_passive(db_path: &Path) -> Result<()> {
    let db_path = db_path.to_path_buf();
    tokio::task::spawn_blocking(move || {
        let conn = rusqlite::Connection::open_with_flags(
            &db_path,
            rusqlite::OpenFlags::SQLITE_OPEN_READ_WRITE,
        )?;
        conn.busy_timeout(std::time::Duration::from_secs(5))?;
        conn.execute_batch("PRAGMA wal_checkpoint(PASSIVE)")?;
        Ok::<_, anyhow::Error>(())
    })
    .await??;
    Ok(())
}

pub async fn checkpoint_wal_truncate(db_path: &Path) -> Result<()> {
    let db_path = db_path.to_path_buf();
    tokio::task::spawn_blocking(move || {
        let conn = rusqlite::Connection::open_with_flags(
            &db_path,
            rusqlite::OpenFlags::SQLITE_OPEN_READ_WRITE,
        )?;
        conn.busy_timeout(std::time::Duration::from_secs(5))?;
        let (busy, log_frames, checkpointed_frames): (i64, i64, i64) =
            conn.query_row("PRAGMA wal_checkpoint(TRUNCATE);", [], |row| {
                Ok((row.get(0)?, row.get(1)?, row.get(2)?))
            })?;
        if busy != 0 || checkpointed_frames < log_frames {
            anyhow::bail!(
                "{}: snapshot checkpoint incomplete (busy={}, log_frames={}, checkpointed_frames={})",
                db_path.display(),
                busy,
                log_frames,
                checkpointed_frames
            );
        }
        Ok::<_, anyhow::Error>(())
    })
    .await??;
    Ok(())
}

pub async fn get_page_size(db_path: &Path) -> Result<u32> {
    use tokio::io::AsyncReadExt;
    let mut file = tokio::fs::File::open(db_path).await?;
    let mut header = [0u8; 100];
    file.read_exact(&mut header).await?;

    let page_size = u16::from_be_bytes([header[16], header[17]]) as u32;
    Ok(if page_size == 1 { 65536 } else { page_size })
}

async fn ensure_database_in_wal_mode(db_path: &Path, db_name: &str) -> Result<()> {
    let db_path = db_path.to_path_buf();
    let mode = tokio::task::spawn_blocking(move || -> Result<String> {
        let conn = rusqlite::Connection::open(&db_path)?;
        let mode: String = conn.query_row("PRAGMA journal_mode", [], |row| row.get(0))?;
        Ok(mode)
    })
    .await??;

    if mode.eq_ignore_ascii_case("wal") {
        Ok(())
    } else {
        Err(anyhow!(
            "{}: SQLite journal_mode is '{}', expected WAL; replication cannot continue",
            db_name,
            mode
        ))
    }
}
