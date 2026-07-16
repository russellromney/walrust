use anyhow::{anyhow, bail, Context, Result};
use futures::future::join_all;
use std::collections::{HashMap, HashSet};
use std::future::Future;
use std::path::PathBuf;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::signal;
use tokio::sync::mpsc;

use crate::cache::LocalCache;
use crate::config::{CacheConfig, ResolvedDbConfig, SpoolConfig, SyncConfig, WebhookConfig};
use crate::dashboard::{self, DbStatus, MetricsState};
use crate::errors::WalrustError;
use crate::ltx;
use crate::retention::RetentionPolicy;
use crate::retry::{RetryConfig, RetryPolicy};
use crate::s3::{self, create_client, parse_bucket};
use crate::shadow::ShadowWal;
use crate::uploader::{UploadMessage, UploaderStats};
use crate::webhook::{WebhookPayload, WebhookSender};
use hadb_storage::StorageBackend;
use hadb_storage_s3::S3Storage;
use rusqlite::Connection;
use walrust_core::legacy_shadow_watch::{
    apply_shadow_sync_result_to_state, apply_shadow_sync_results_strict,
    checkpoint_blocker_heartbeat_is_live, checkpoint_data_version, load_shadow_progress,
    rearm_checkpoint_blocker, save_shadow_watch_progress as save_shadow_progress,
    shadow_sync_input, wait_for_cache_checkpoint_durability, ShadowProgress,
};
use walrust_core::native_publish::{object_key as native_object_key, NativeUploader, UploadWake};
use walrust_core::native_shadow::{
    encode_shadow_to_hadbp, write_snapshot_from_shadow_file, NativeShadowInput, NativeSnapshotInput,
};
use walrust_core::native_spool::{
    durability_failpoint, filesystem_available_bytes, CapacityPolicy, CapacityState, NativeSpool,
    ObjectKind, RecoveryHead, RemoteUploadState, SourceCursor, SpoolIdentity, StageObject,
};

use super::manifest::discover_state_from_s3;
use super::prune::prune_with_client;
use super::shadow::{sync_shadow_concurrent_with_retry, sync_shadow_to_cache_with_retry};
#[cfg(test)]
use super::types::Manifest;
use super::types::{DbState, ShadowDbState, ShadowSyncInput, TriggerState};
use super::verify::validate_backup_integrity;
use super::wal_sync::take_snapshot_with_retry_and_rearm;

type ShadowSyncFuture =
    Pin<Box<dyn Future<Output = Result<super::types::ShadowSyncOutput>> + Send>>;

const CHECKPOINT_UPLOAD_DRAIN_TIMEOUT: Duration = Duration::from_secs(30);
const WAL_SIZE_EXCEEDED_EVENT: &str = "wal_size_exceeded";

type NativeSpoolState = (Arc<Mutex<NativeSpool>>, UploadWake);

fn restore_wal_copy_progress(shadow: &mut ShadowWal, progress: &ShadowProgress) -> u64 {
    if shadow.discarded_unproven_tail() {
        // The progress cursor described bytes that recovery just rejected for
        // lack of a durable-tail marker. Reusing its live WAL offset would skip
        // those source frames and leave a direct snapshot trying to read
        // WAL-only pages from the shorter main DB. ShadowWal already carries
        // the current live header salt, so checked recopy starts at zero.
        0
    } else {
        shadow.restore_read_cursor(progress.wal_salt, progress.wal_checksum_chain);
        progress.wal_copy_offset
    }
}

fn spawn_native_uploader_supervisor(
    initial: NativeUploader,
    storage: Arc<dyn StorageBackend>,
    spool: Arc<Mutex<NativeSpool>>,
    wake: UploadWake,
    lag: Arc<Mutex<walrust_core::native_publish::RemoteLagState>>,
    mut shutdown: tokio::sync::watch::Receiver<bool>,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let mut next = Some(initial);
        loop {
            if *shutdown.borrow() {
                return;
            }
            let uploader = match next.take() {
                Some(uploader) => uploader,
                None => match NativeUploader::with_runtime(
                    Arc::clone(&storage),
                    Arc::clone(&spool),
                    wake.clone(),
                    Arc::clone(&lag),
                ) {
                    Ok(uploader) => uploader,
                    Err(error) => {
                        tracing::error!(
                            error = %error,
                            "remote_lag: failed to reconstruct native uploader; retrying from disk"
                        );
                        if let Ok(mut state) = lag.lock() {
                            state.last_error = Some(format!("{error:#}"));
                        }
                        tokio::time::sleep(Duration::from_millis(250)).await;
                        continue;
                    }
                },
            };
            let mut child = tokio::spawn(uploader.run(shutdown.clone()));
            tokio::select! {
                result = &mut child => {
                    if *shutdown.borrow() {
                        return;
                    }
                    let detail = match result {
                        Ok(()) => "native uploader exited unexpectedly".to_string(),
                        Err(error) => format!("native uploader task failed: {error}"),
                    };
                    tracing::error!(
                        error = %detail,
                        "remote_lag: native uploader died; restarting from durable spool"
                    );
                    if let Ok(mut state) = lag.lock() {
                        state.last_error = Some(detail);
                    }
                    tokio::time::sleep(Duration::from_millis(250)).await;
                }
                changed = shutdown.changed() => {
                    if changed.is_err() || *shutdown.borrow() {
                        let _ = child.await;
                        return;
                    }
                }
            }
        }
    })
}

fn spool_lock(spool: &Arc<Mutex<NativeSpool>>) -> Result<std::sync::MutexGuard<'_, NativeSpool>> {
    spool
        .lock()
        .map_err(|_| anyhow!("native spool lock poisoned"))
}

fn watcher_retention_has_published_native_base(spool_state: &NativeSpoolState) -> Result<bool> {
    let spool = spool_lock(&spool_state.0)?;
    let identity = spool.identity();
    if identity.legacy_boundary_txid.is_none() {
        return Ok(true);
    }
    let Some(remote_seq) = spool.remote_published_seq() else {
        return Ok(false);
    };
    if remote_seq < identity.first_native_seq {
        return Ok(false);
    }
    let has_retained_base = spool.objects().any(|object| {
        object.seq <= remote_seq
            && object.kind == ObjectKind::Snapshot
            && object.remote_upload_state == RemoteUploadState::Published
    });
    Ok(has_retained_base)
}

async fn prune_watcher_database(
    client: &aws_sdk_s3::Client,
    bucket: &str,
    prefix: &str,
    state: &ShadowDbState,
    spool_state: &NativeSpoolState,
    policy: &RetentionPolicy,
) -> Result<()> {
    if !watcher_retention_has_published_native_base(spool_state)? {
        tracing::warn!(
            database = %state.name,
            "native migration snapshot is not yet contiguously published; preserving legacy recovery history"
        );
        return Ok(());
    }
    prune_with_client(client, bucket, prefix, &state.name, policy, true).await
}

fn shadow_storage_bytes(state: &ShadowDbState) -> u64 {
    walkdir::WalkDir::new(state.shadow.shadow_dir())
        .into_iter()
        .filter_map(|entry| entry.ok())
        .filter_map(|entry| entry.metadata().ok())
        .map(|metadata| metadata.len())
        .sum()
}

fn source_footprint_on_spool_filesystem(state: &ShadowDbState, spool: &NativeSpool) -> Result<u64> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        let source = state
            .db_path
            .parent()
            .unwrap_or_else(|| std::path::Path::new("."));
        if std::fs::metadata(source)?.dev() != std::fs::metadata(spool.root())?.dev() {
            return Ok(0);
        }
    }
    Ok(std::fs::metadata(&state.wal_path)
        .map(|metadata| metadata.len())
        .unwrap_or(0)
        .saturating_add(shadow_storage_bytes(state)))
}

async fn verify_legacy_migration_head(
    storage: &dyn StorageBackend,
    prefix: &str,
    name: &str,
    verify_path: &std::path::Path,
    legacy_txid: u64,
) -> Result<()> {
    let verification = async {
        let restored_txid = walrust_core::legacy_restore::restore_legacy_ltx(
            storage,
            prefix,
            name,
            verify_path,
            Some(legacy_txid),
        )
        .await
        .with_context(|| format!("{}: verify legacy migration head", name))?;
        if restored_txid != legacy_txid {
            bail!(
                "{}: legacy migration verification restored TXID {}, expected {}",
                name,
                restored_txid,
                legacy_txid
            );
        }
        let verify_connection = Connection::open(verify_path)?;
        let integrity: String =
            verify_connection.query_row("PRAGMA integrity_check", [], |row| row.get(0))?;
        if integrity != "ok" {
            bail!(
                "{}: legacy migration head failed SQLite integrity_check: {}",
                name,
                integrity
            );
        }
        Ok::<(), anyhow::Error>(())
    }
    .await;

    let cleanup: Result<()> = match std::fs::remove_file(verify_path) {
        Ok(()) => match verify_path.parent() {
            Some(parent) => std::fs::File::open(parent)
                .and_then(|file| file.sync_all())
                .map_err(Into::into),
            None => Err(anyhow!("legacy migration scratch path has no parent")),
        },
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(error.into()),
    };
    match (verification, cleanup) {
        (Err(verification), Err(cleanup)) => Err(anyhow!(
            "{verification:#}; additionally failed to remove legacy migration scratch {}: {cleanup}",
            verify_path.display()
        )),
        (Err(verification), Ok(())) => Err(verification),
        (Ok(()), Err(cleanup)) => Err(cleanup.into()),
        (Ok(()), Ok(())) => Ok(()),
    }
}

async fn reconcile_shadow_progress_from_spool(
    state: &mut ShadowDbState,
    head: Option<RecoveryHead>,
) -> Result<()> {
    let Some(head) = head else {
        return Ok(());
    };
    if state.current_txid > head.seq {
        bail!(
            "{}: durable shadow progress seq {} is ahead of native spool head {}; refusing ambiguous restart",
            state.name,
            state.current_txid,
            head.seq
        );
    }
    let frame_size = 24u64 + state.shadow.page_size() as u64;
    let head_offset = head
        .source_cursor
        .shadow_frame_index
        .checked_mul(frame_size)
        .ok_or_else(|| anyhow!("{}: native spool source cursor overflows", state.name))?;
    let local_cursor = (state.shadow_sync_generation, state.shadow_sync_offset);
    let spool_cursor = (head.source_cursor.shadow_generation, head_offset);
    if head.seq > state.current_txid && spool_cursor < local_cursor {
        bail!(
            "{}: native spool head seq {} has a source cursor behind durable shadow progress",
            state.name,
            head.seq
        );
    }
    if head.seq > state.current_txid || spool_cursor > local_cursor {
        if head.source_cursor.shadow_generation > state.shadow.generation() {
            bail!(
                "{}: native spool source generation {} is ahead of durable shadow generation {}",
                state.name,
                head.source_cursor.shadow_generation,
                state.shadow.generation()
            );
        }
        let available = state
            .shadow
            .list_segments(head.source_cursor.shadow_generation)
            .await?
            .iter()
            .map(|segment| segment.size)
            .sum::<u64>();
        if head_offset > available {
            bail!(
                "{}: native spool source cursor {} exceeds durable shadow bytes {} in generation {}",
                state.name,
                head_offset,
                available,
                head.source_cursor.shadow_generation
            );
        }
        state.shadow_sync_generation = head.source_cursor.shadow_generation;
        state.shadow_sync_offset = head_offset;
        state.wal_copy_offset = head.source_cursor.wal_offset;
        state.shadow.restore_read_cursor(
            head.source_cursor.wal_salt,
            head.source_cursor.wal_checksum_chain,
        );
    }
    state.current_txid = head.seq;
    state.db_checksum = Some(head.ending_chain_checksum);
    save_shadow_progress(state)?;
    Ok(())
}

async fn stage_native_shadow(
    state: &ShadowDbState,
    spool_state: &NativeSpoolState,
) -> Result<super::types::ShadowSyncOutput> {
    let (seq, previous_chain_checksum) = {
        let spool = spool_lock(&spool_state.0)?;
        let seq = spool
            .admitted_seq()
            .map(|seq| seq + 1)
            .unwrap_or(spool.identity().first_native_seq);
        let previous = spool
            .objects()
            .last()
            .map(|object| object.ending_chain_checksum)
            .unwrap_or(0);
        (seq, previous)
    };
    let input = NativeShadowInput {
        seq,
        previous_chain_checksum,
        generation: state.shadow_sync_generation,
        shadow_sync_offset: state.shadow_sync_offset,
        page_size: state.shadow.page_size(),
        shadow_dir: state.shadow.shadow_dir().to_path_buf(),
    };
    let encoded = tokio::task::spawn_blocking(move || encode_shadow_to_hadbp(&input)).await??;
    let Some(encoded) = encoded else {
        return Ok(super::types::ShadowSyncOutput {
            db_path: state.db_path.clone(),
            frame_count: 0,
            new_shadow_sync_offset: state.shadow_sync_offset,
            new_current_txid: state.current_txid,
            new_db_checksum: state.db_checksum,
        });
    };
    let cursor = SourceCursor {
        shadow_generation: state.shadow_sync_generation,
        shadow_frame_index: encoded.new_shadow_sync_offset / (24 + state.shadow.page_size() as u64),
        wal_offset: state.wal_copy_offset,
        wal_salt: state.shadow.wal_read_salt(),
        wal_checksum_chain: state.shadow.wal_read_chain(),
    };
    let intended_remote_key = {
        let spool = spool_lock(&spool_state.0)?;
        native_object_key(spool.identity(), ObjectKind::Delta, encoded.seq)
    };
    let stage_started = std::time::Instant::now();
    {
        let mut spool = spool_lock(&spool_state.0)?;
        let peak = (encoded.payload.len() as u64)
            .saturating_mul(2)
            .saturating_add(source_footprint_on_spool_filesystem(state, &spool)?);
        match spool.capacity_state(peak)? {
            CapacityState::High => tracing::error!(
                database = %state.name,
                event = "local_spool_high",
                spool_bytes = spool.used_bytes()?,
                additional_peak_bytes = peak,
                filesystem_free_bytes = spool.free_bytes()?,
                "local native spool crossed its warning watermark"
            ),
            CapacityState::Full => bail!(
                "local_spool_full: {} cannot admit native seq {}; blocker remains held",
                state.name,
                encoded.seq
            ),
            CapacityState::Healthy => {}
        }
        spool.stage(StageObject {
            seq: encoded.seq,
            kind: ObjectKind::Delta,
            previous_chain_checksum: encoded.previous_chain_checksum,
            ending_chain_checksum: encoded.ending_chain_checksum,
            end_page_count: encoded.end_page_count,
            intended_remote_key,
            source_cursor: cursor,
            payload: &encoded.payload,
        })?;
    }
    tracing::info!(
        database = %state.name,
        seq = encoded.seq,
        frames = encoded.frame_count,
        unique_pages = encoded.unique_pages,
        bytes = encoded.payload.len(),
        local_hadbp_stage_ms = stage_started.elapsed().as_millis() as u64,
        "native HADBP delta admitted to durable local spool"
    );
    spool_state.1.notify();
    Ok(super::types::ShadowSyncOutput {
        db_path: state.db_path.clone(),
        frame_count: encoded.frame_count,
        new_shadow_sync_offset: encoded.new_shadow_sync_offset,
        new_current_txid: encoded.seq,
        new_db_checksum: Some(encoded.ending_chain_checksum),
    })
}

async fn snapshot_frozen_cursor_pause() -> Result<()> {
    if !cfg!(debug_assertions) {
        return Ok(());
    }
    let Some(path) = std::env::var_os("WALRUST_TEST_NATIVE_SNAPSHOT_SOURCE_PAUSE_FILE") else {
        return Ok(());
    };
    let path = PathBuf::from(path);
    let used = path.with_extension("used");
    if used.exists() {
        return Ok(());
    }
    std::fs::write(&path, b"shadow-cursor-frozen")?;
    while path.exists() {
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    std::fs::write(used, b"consumed")?;
    Ok(())
}

async fn checkpoint_preflight_sample_pause() -> Result<()> {
    if !cfg!(debug_assertions) {
        return Ok(());
    }
    let Some(path) = std::env::var_os("WALRUST_TEST_NATIVE_CHECKPOINT_PREFLIGHT_PAUSE_FILE") else {
        return Ok(());
    };
    let path = PathBuf::from(path);
    let used = path.with_extension("used");
    if used.exists() {
        return Ok(());
    }
    std::fs::write(&path, b"entered")?;
    while path.exists() {
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    std::fs::write(used, b"consumed")?;
    Ok(())
}

async fn stage_native_snapshot(
    state: &mut ShadowDbState,
    spool_state: &NativeSpoolState,
) -> Result<u64> {
    let stage_started = std::time::Instant::now();
    // Freeze the source at an exact committed, checksum-validated, fsynced
    // shadow boundary. Application commits after this copy remain only in the
    // live WAL and are deliberately excluded from this snapshot.
    #[cfg(unix)]
    let db_identity_before = {
        use std::os::unix::fs::MetadataExt;
        let metadata = std::fs::metadata(&state.db_path)?;
        (metadata.dev(), metadata.ino())
    };
    let generation_before = state.shadow.generation();
    let wal_offset_before = state.wal_copy_offset;
    let (frozen_frames, new_wal_offset) = state.shadow.copy_frames(wal_offset_before).await?;
    tracing::debug!(
        database = %state.name,
        frames = frozen_frames.len(),
        wal_offset_before,
        wal_offset_after = new_wal_offset,
        shadow_generation = state.shadow.generation(),
        shadow_bytes = state.shadow.segment_offset(),
        "froze native snapshot shadow cursor"
    );
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        let metadata = std::fs::metadata(&state.db_path)?;
        anyhow::ensure!(
            (metadata.dev(), metadata.ino()) == db_identity_before,
            "{}: SQLite database path was replaced while freezing native snapshot source",
            state.name
        );
    }
    state.wal_copy_offset = new_wal_offset;
    let page_size = state.shadow.page_size();
    let frame_size = 24 + page_size as u64;
    if state.shadow.generation() != generation_before {
        state.shadow_sync_generation = state.shadow.generation();
        state.shadow_sync_offset = 0;
    }
    let snapshot_generation = state.shadow.generation();
    let shadow_end_offset = state.shadow.segment_offset();
    anyhow::ensure!(
        shadow_end_offset.is_multiple_of(frame_size),
        "{}: snapshot source cursor is not frame-aligned",
        state.name
    );
    let proposed_cursor = SourceCursor {
        shadow_generation: snapshot_generation,
        shadow_frame_index: shadow_end_offset / frame_size,
        wal_offset: state.wal_copy_offset,
        wal_salt: state.shadow.wal_read_salt(),
        wal_checksum_chain: state.shadow.wal_read_chain(),
    };
    let source_footprint = {
        let spool = spool_lock(&spool_state.0)?;
        source_footprint_on_spool_filesystem(state, &spool)?
    };
    let main_bytes = std::fs::metadata(&state.db_path)?.len();
    let shadow_frames = shadow_end_offset / frame_size;
    let logical_upper = main_bytes.saturating_add(shadow_frames.saturating_mul(page_size as u64));
    let payload_upper = logical_upper
        .saturating_add((logical_upper / page_size as u64).saturating_mul(64))
        .saturating_add(4096);
    {
        let spool = spool_lock(&spool_state.0)?;
        let journal_peak = spool.next_journal_rewrite_peak_bytes()?;
        let peak = payload_upper
            .saturating_mul(2)
            .saturating_add(journal_peak)
            .saturating_add(source_footprint);
        if spool.capacity_state(peak)? == CapacityState::Full {
            bail!(
                "local_spool_full: {} lacks peak capacity/reserve for direct native HADBP snapshot payload + journal \
                 (additional_peak={peak}, payload_upper={payload_upper}, journal_peak={journal_peak}, \
                 source_footprint={source_footprint}, main_bytes={main_bytes}, shadow_frames={shadow_frames})",
                state.name,
            );
        }
    }
    let preparation = {
        let mut spool = spool_lock(&spool_state.0)?;
        let seq = spool
            .admitted_seq()
            .map(|seq| seq + 1)
            .unwrap_or(spool.identity().first_native_seq);
        let previous = spool
            .objects()
            .last()
            .map(|object| object.ending_chain_checksum)
            .unwrap_or(0);
        let intended_remote_key = native_object_key(spool.identity(), ObjectKind::Snapshot, seq);
        spool.prepare_snapshot(
            seq,
            previous,
            intended_remote_key,
            proposed_cursor,
            page_size,
        )
    };
    let preparation = preparation?;
    let seq = preparation.seq;
    let previous_chain_checksum = preparation.previous_chain_checksum;
    let intended_remote_key = preparation.intended_remote_key.clone();
    let payload_temp =
        spool_lock(&spool_state.0)?.payload_temporary_path(ObjectKind::Snapshot, seq);
    if let Err(error) = snapshot_frozen_cursor_pause().await {
        spool_lock(&spool_state.0)?.abandon_unadmitted_snapshot(seq)?;
        return Err(error);
    }
    let snapshot_input = NativeSnapshotInput {
        db_path: state.db_path.clone(),
        seq,
        previous_chain_checksum,
        generation: preparation.source_cursor.shadow_generation,
        shadow_end_offset: preparation
            .source_cursor
            .shadow_frame_index
            .saturating_mul(frame_size),
        page_size,
        shadow_dir: state.shadow.shadow_dir().to_path_buf(),
        #[cfg(unix)]
        expected_db_file_identity: db_identity_before,
    };
    let payload_temp_for_encode = payload_temp.clone();
    let source_db_file = state.source_db_file.as_ref().cloned().ok_or_else(|| {
        anyhow!(
            "{}: native snapshot source descriptor was not retained",
            state.name
        )
    })?;
    let encoded_result = match tokio::task::spawn_blocking(move || -> Result<_> {
        let mut source = source_db_file
            .lock()
            .map_err(|_| anyhow!("native snapshot source descriptor lock poisoned"))?;
        write_snapshot_from_shadow_file(&snapshot_input, &mut source, &payload_temp_for_encode)
    })
    .await
    {
        Ok(result) => result,
        Err(error) => {
            spool_lock(&spool_state.0)?.abandon_unadmitted_snapshot(seq)?;
            return Err(error.into());
        }
    };
    let encoded = match encoded_result {
        Ok(encoded) => encoded,
        Err(error) => {
            spool_lock(&spool_state.0)?.abandon_unadmitted_snapshot(seq)?;
            return Err(error);
        }
    };
    let encoded_payload = match std::fs::read(&payload_temp) {
        Ok(payload) if payload.len() as u64 == encoded.payload_length => payload,
        Ok(_) => {
            spool_lock(&spool_state.0)?.abandon_unadmitted_snapshot(seq)?;
            bail!("native snapshot payload temporary length changed after fsync");
        }
        Err(error) => {
            spool_lock(&spool_state.0)?.abandon_unadmitted_snapshot(seq)?;
            return Err(error.into());
        }
    };
    let stage_result = (|| -> Result<()> {
        let mut spool = spool_lock(&spool_state.0)?;
        // The fsynced payload temporary is already included in used_bytes and
        // admission renames that exact inode. Only journal/intent rewrite and
        // source-filesystem reserve remain as additional peak here.
        let peak = spool
            .next_journal_rewrite_peak_bytes()?
            .saturating_add(source_footprint_on_spool_filesystem(state, &spool)?);
        match spool.capacity_state(peak)? {
            CapacityState::High => tracing::error!(
                database = %state.name,
                event = "local_spool_high",
                spool_bytes = spool.used_bytes()?,
                additional_peak_bytes = peak,
                filesystem_free_bytes = spool.free_bytes()?,
                "local native spool crossed its warning watermark while snapshotting"
            ),
            CapacityState::Full => {
                spool.abandon_unadmitted_snapshot(seq)?;
                return Err(anyhow!(
                    "local_spool_full: {} cannot admit native snapshot seq {}; blocker remains held",
                state.name, seq
            ));
            }
            CapacityState::Healthy => {}
        }
        spool.stage(StageObject {
            seq,
            kind: ObjectKind::Snapshot,
            previous_chain_checksum,
            ending_chain_checksum: encoded.ending_chain_checksum,
            end_page_count: encoded.end_page_count,
            intended_remote_key,
            source_cursor: preparation.source_cursor.clone(),
            payload: &encoded_payload,
        })?;
        durability_failpoint("snapshot_object_admitted");
        spool.finish_snapshot(seq)?;
        Ok(())
    })();
    if let Err(error) = stage_result {
        // `stage` may already have durably installed an object intent or final
        // payload before a later journal I/O error. Preserve that evidence for
        // deterministic orphan adoption; never discard it as a generic retry.
        return Err(error);
    }
    state.current_txid = seq;
    state.db_checksum = Some(encoded.ending_chain_checksum);
    state.last_snapshot = Some(chrono::Utc::now());
    state.shadow_sync_generation = preparation.source_cursor.shadow_generation;
    state.shadow_sync_offset = preparation
        .source_cursor
        .shadow_frame_index
        .saturating_mul(frame_size);
    state.wal_copy_offset = preparation.source_cursor.wal_offset;
    state.shadow.restore_read_cursor(
        preparation.source_cursor.wal_salt,
        preparation.source_cursor.wal_checksum_chain,
    );
    save_shadow_progress(state)?;
    tracing::info!(
        database = %state.name,
        seq,
        bytes = encoded_payload.len(),
        shadow_frames = encoded.frame_count,
        shadow_pages = encoded.unique_shadow_pages,
        local_hadbp_stage_ms = stage_started.elapsed().as_millis() as u64,
        "native HADBP snapshot admitted to durable local spool"
    );
    spool_state.1.notify();
    Ok(seq)
}

async fn require_native_snapshot(
    state: &mut ShadowDbState,
    spool_state: &NativeSpoolState,
    reason: &str,
) -> Result<u64> {
    loop {
        match stage_native_snapshot(state, spool_state).await {
            Ok(seq) => return Ok(seq),
            Err(error) if format!("{error:#}").contains("local_spool_full") => {
                tracing::error!(
                    database = %state.name,
                    event = "local_spool_full",
                    reason,
                    error = %error,
                    "required native snapshot cannot be admitted; retaining blocker and retrying"
                );
                tokio::time::sleep(Duration::from_secs(1)).await;
            }
            Err(error) => return Err(error),
        }
    }
}

async fn wait_for_remote_publish(
    spool: &Arc<Mutex<NativeSpool>>,
    database: &str,
    seq: u64,
    timeout: Duration,
) -> Result<()> {
    let deadline = tokio::time::Instant::now() + timeout;
    loop {
        if spool_lock(spool)?.remote_published_seq().unwrap_or(0) >= seq {
            return Ok(());
        }
        if tokio::time::Instant::now() >= deadline {
            bail!("{database}: timed out waiting for contiguous remote publish through native seq {seq}");
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
}

#[derive(Clone, Copy)]
enum ShadowCheckpointMode {
    Passive,
    Truncate,
}

#[derive(Debug)]
struct CheckpointAttempt {
    completed: bool,
    dirty: bool,
}

async fn checkpoint_with_state_blocker_attempt(
    state: &mut ShadowDbState,
    mode: ShadowCheckpointMode,
    data_version_before: i64,
) -> Result<CheckpointAttempt> {
    // End the blocker read transaction, then run the checkpoint on the
    // lifetime monitor. Its own checkpoint does not change its connection-local
    // data_version; any app commit since the caller's final copy baseline does,
    // including one in the unblocked window.
    let blocker = state
        .checkpoint_blocker
        .as_ref()
        .ok_or_else(|| anyhow!("{}: CLI checkpoint blocker was not held", state.name))?;
    blocker.execute_batch("ROLLBACK;")?;

    let monitor = state
        .data_version_monitor
        .as_ref()
        .ok_or_else(|| anyhow!("{}: CLI data_version monitor was not held", state.name))?;

    let mode_name = match mode {
        ShadowCheckpointMode::Passive => "PASSIVE",
        ShadowCheckpointMode::Truncate => "TRUNCATE",
    };
    let checkpoint_result = monitor
        .query_row(
            &format!("PRAGMA wal_checkpoint({mode_name});"),
            [],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )
        .map_err(anyhow::Error::from)
        .and_then(|(busy, log_frames, checkpointed_frames): (i64, i64, i64)| {
            let completed = busy == 0 && checkpointed_frames >= log_frames;
            if !completed {
                tracing::warn!(
                    database = %state.name,
                    checkpoint_mode = mode_name,
                    busy,
                    log_frames,
                    checkpointed_frames,
                    "SQLite checkpoint was partial/busy; blocker will be rearmed and checkpoint retried later"
                );
            }
            Ok(completed)
        });
    durability_failpoint("sqlite_checkpoint_returned");

    // Keep the monitor opened before the blocker for its whole lifetime. Its
    // data_version advances once for every commit from another connection,
    // while the checkpoint above is an operation on the monitor itself. The
    // replacement blocker's heartbeat is written on that monitor, so it does
    // not change the monitor's own value. Sampling both sides of rearm catches
    // an app commit anywhere in the
    // release/reacquire window, including the old sample-to-heartbeat gap that
    // could otherwise be checkpointed away before the new blocker pinned it.
    let data_version_before_rearm = checkpoint_data_version(state);
    if cfg!(debug_assertions) {
        if let Some(path) = std::env::var_os("WALRUST_TEST_NATIVE_CHECKPOINT_PAUSE_FILE") {
            let selected_db = std::env::var_os("WALRUST_TEST_NATIVE_CHECKPOINT_PAUSE_DB");
            if selected_db
                .as_ref()
                .is_none_or(|selected| std::path::Path::new(selected) == state.db_path)
            {
                let path = std::path::PathBuf::from(path);
                std::fs::write(&path, b"entered")?;
                while path.exists() {
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
            }
        }
    }
    let rearm_result = rearm_checkpoint_blocker(state);
    let heartbeat_live = match &rearm_result {
        Ok(()) => checkpoint_blocker_heartbeat_is_live(state),
        Err(_) => Ok(false),
    };
    if rearm_result.is_ok() {
        durability_failpoint("blocker_reacquired");
    }
    let data_version_after_rearm = checkpoint_data_version(state);
    let checkpoint_result = checkpoint_result.and_then(|completed| {
        let before_rearm = data_version_before_rearm?;
        let after_rearm = data_version_after_rearm?;
        let dirty_before_rearm = before_rearm != data_version_before;
        let dirty_during_rearm = after_rearm != before_rearm;
        Ok((completed, dirty_before_rearm || dirty_during_rearm))
    });
    match (checkpoint_result, rearm_result) {
        (Ok((completed, data_version_dirty)), Ok(())) => Ok(CheckpointAttempt {
            completed,
            dirty: data_version_dirty || !heartbeat_live?,
        }),
        (Err(checkpoint_error), Ok(())) => Err(checkpoint_error),
        (Ok(_), Err(rearm_error)) => Err(rearm_error),
        (Err(checkpoint_error), Err(rearm_error)) => Err(anyhow!(
            "{}; additionally failed to rearm CLI checkpoint blocker: {}",
            checkpoint_error,
            rearm_error
        )),
    }
}

async fn checkpoint_with_state_blocker(
    state: &mut ShadowDbState,
    mode: ShadowCheckpointMode,
    data_version_before: i64,
) -> Result<bool> {
    let attempt = checkpoint_with_state_blocker_attempt(state, mode, data_version_before).await?;
    if !attempt.completed {
        bail!("{}: shadow checkpoint incomplete", state.name);
    }
    Ok(attempt.dirty)
}

#[derive(Clone)]
struct DirectShadowSyncTarget {
    client: Arc<aws_sdk_s3::Client>,
    bucket_name: String,
    prefix: String,
}

async fn run_shadow_syncs(
    db_states: &HashMap<PathBuf, ShadowDbState>,
    cache_states: &HashMap<PathBuf, (Arc<LocalCache>, mpsc::Sender<UploadMessage>)>,
    direct_target: Option<DirectShadowSyncTarget>,
    retry_policy: &RetryPolicy,
    webhook_sender: Arc<WebhookSender>,
) -> Vec<Result<super::types::ShadowSyncOutput>> {
    let sync_inputs: Vec<ShadowSyncInput> = db_states.values().map(shadow_sync_input).collect();

    let sync_futures: Vec<ShadowSyncFuture> = sync_inputs
        .into_iter()
        .map(|input| {
            let policy = retry_policy.clone();
            let webhooks = Arc::clone(&webhook_sender);

            if let Some((cache, upload_tx)) = cache_states.get(&input.db_path) {
                let cache = Arc::clone(cache);
                let upload_tx = upload_tx.clone();
                Box::pin(sync_shadow_to_cache_with_retry(
                    cache, upload_tx, input, policy, webhooks,
                )) as ShadowSyncFuture
            } else if let Some(target) = direct_target.clone() {
                Box::pin(sync_shadow_concurrent_with_retry(
                    Arc::clone(&target.client),
                    target.bucket_name,
                    target.prefix,
                    input,
                    policy,
                    webhooks,
                )) as ShadowSyncFuture
            } else {
                let name = input.name;
                Box::pin(async move {
                    Err(anyhow!(
                        "{}: no cache uploader or direct upload target configured",
                        name
                    ))
                }) as ShadowSyncFuture
            }
        })
        .collect();

    join_all(sync_futures).await
}

async fn copy_final_shadow_frames(db_states: &mut HashMap<PathBuf, ShadowDbState>) -> Result<()> {
    for state in db_states.values_mut() {
        if !state.wal_path.exists() {
            continue;
        }

        let (frames, new_offset) = state
            .shadow
            .copy_frames(state.wal_copy_offset)
            .await
            .with_context(|| format!("{}: final shadow copy failed", state.name))?;

        if !frames.is_empty() {
            tracing::debug!("{}: Final shadow copy: {} frames", state.name, frames.len());
            state.wal_copy_offset = new_offset;
        }
    }

    Ok(())
}

async fn finish_shutdown_local_admission(
    db_states: &mut HashMap<PathBuf, ShadowDbState>,
    native_spools: &HashMap<PathBuf, NativeSpoolState>,
) {
    loop {
        let attempt = async {
            copy_final_shadow_frames(db_states).await?;
            let mut final_results = Vec::with_capacity(db_states.len());
            for (db_path, state) in db_states.iter() {
                final_results.push(match native_spools.get(db_path) {
                    Some(spool) => stage_native_shadow(state, spool).await,
                    None => Err(anyhow!("{}: native spool missing", state.name)),
                });
            }
            apply_shadow_sync_results_strict(db_states, final_results).await
        }
        .await;
        match attempt {
            Ok(()) => return,
            Err(error) => {
                tracing::error!(
                    event = "local_spool_full",
                    error = %error,
                    "shutdown local admission is not durable; retaining checkpoint blockers and retrying (SIGKILL is the explicit forced stop)"
                );
                // A stage can commit its journal before progress persistence
                // fails. Reconcile from that durable head before retrying so
                // the same shadow suffix is never admitted under a new seq.
                for (db_path, state) in db_states.iter_mut() {
                    let head = match native_spools.get(db_path) {
                        Some((spool, _)) => match spool_lock(spool) {
                            Ok(guard) => Ok(guard.recovery_head()),
                            Err(error) => Err(error),
                        },
                        None => Err(anyhow!("{}: native spool missing", state.name)),
                    };
                    let reconcile_result = match head {
                        Ok(head) => reconcile_shadow_progress_from_spool(state, head).await,
                        Err(error) => Err(error),
                    };
                    if let Err(reconcile_error) = reconcile_result {
                        tracing::error!(
                            database = %state.name,
                            error = %reconcile_error,
                            "shutdown could not reconcile durable spool head; retaining blocker and retrying"
                        );
                    }
                }
                tokio::time::sleep(Duration::from_secs(1)).await;
            }
        }
    }
}

async fn checkpoint_shadow_after_durable_sync(
    state: &mut ShadowDbState,
    cache_state: Option<&(Arc<LocalCache>, mpsc::Sender<UploadMessage>)>,
    direct_target: Option<DirectShadowSyncTarget>,
    retry_policy: &RetryPolicy,
    webhook_sender: Arc<WebhookSender>,
    drain_timeout: Duration,
    checkpoint_mode: ShadowCheckpointMode,
) -> Result<bool> {
    // `data_version` is connection-local and changes only when another
    // connection commits. Read it before the final copy so any app commit that
    // can race the drain/checkpoint boundary is detected after reactivation.
    let data_version_before = checkpoint_data_version(state)?;
    let (frames, new_offset) = state
        .shadow
        .copy_frames(state.wal_copy_offset)
        .await
        .with_context(|| format!("{}: shadow copy before checkpoint failed", state.name))?;
    if !frames.is_empty() {
        tracing::debug!(
            "{}: checkpoint copied {} frames to shadow (offset {} -> {})",
            state.name,
            frames.len(),
            state.wal_copy_offset,
            new_offset
        );
        state.wal_copy_offset = new_offset;
    }

    let input = shadow_sync_input(state);
    let output = if let Some((cache, upload_tx)) = cache_state {
        sync_shadow_to_cache_with_retry(
            Arc::clone(cache),
            upload_tx.clone(),
            input,
            retry_policy.clone(),
            Arc::clone(&webhook_sender),
        )
        .await?
    } else if let Some(target) = direct_target {
        sync_shadow_concurrent_with_retry(
            Arc::clone(&target.client),
            target.bucket_name,
            target.prefix,
            input,
            retry_policy.clone(),
            Arc::clone(&webhook_sender),
        )
        .await?
    } else {
        anyhow::bail!(
            "{}: no cache uploader or direct upload target configured for checkpoint drain",
            state.name
        );
    };
    if let Some((cache, _)) = cache_state {
        wait_for_cache_checkpoint_durability(
            cache,
            &state.name,
            output.new_current_txid,
            drain_timeout,
        )
        .await?;
    }

    apply_shadow_sync_result_to_state(state, &output).await?;

    let checkpoint_window_dirty =
        checkpoint_with_state_blocker(state, checkpoint_mode, data_version_before)
            .await
            .with_context(|| format!("{}: shadow checkpoint failed", state.name))?;

    let cleanup_before_gen = state.shadow_sync_generation;
    if cleanup_before_gen > 0 {
        state
            .shadow
            .cleanup_segments(cleanup_before_gen)
            .await
            .with_context(|| format!("{}: shadow cleanup failed", state.name))?;
    }

    Ok(checkpoint_window_dirty)
}

async fn checkpoint_shadow_after_native_admission(
    state: &mut ShadowDbState,
    spool_state: &NativeSpoolState,
    checkpoint_release: crate::config::CheckpointRelease,
    drain_timeout: Duration,
    checkpoint_mode: ShadowCheckpointMode,
) -> Result<CheckpointAttempt> {
    // A commit between the first data_version sample and a successful shadow
    // admission is already protected by that admission. Do not misclassify it
    // as a commit from the later unblocked checkpoint window. Drain until one
    // complete sample -> copy -> admission -> sample interval is stable, then
    // use that final sample as the checkpoint-window baseline. A commit after
    // the stable sample is still detected after checkpoint/rearm and forces the
    // required snapshot re-anchor.
    const MAX_PREFLIGHT_DRAINS: usize = 8;
    let mut data_version_before = checkpoint_data_version(state)?;
    checkpoint_preflight_sample_pause().await?;
    let mut preflight_drain = 0usize;
    let admitted_seq = loop {
        preflight_drain += 1;
        let shadow_generation_before = state.shadow.generation();
        let wal_offset_before = state.wal_copy_offset;
        let (frames, new_offset) = state
            .shadow
            .copy_frames(wal_offset_before)
            .await
            .with_context(|| format!("{}: shadow copy before checkpoint failed", state.name))?;
        tracing::debug!(
            database = %state.name,
            frames = frames.len(),
            wal_offset_before,
            wal_offset_after = new_offset,
            wal_bytes = std::fs::metadata(&state.wal_path).map(|metadata| metadata.len()).unwrap_or(0),
            shadow_generation = state.shadow.generation(),
            shadow_bytes = state.shadow.segment_offset(),
            "froze native checkpoint shadow cursor"
        );
        if !frames.is_empty() {
            state.wal_copy_offset = new_offset;
        }

        if state.shadow.generation() != shadow_generation_before {
            tracing::warn!(
                database = %state.name,
                shadow_generation_before,
                shadow_generation_after = state.shadow.generation(),
                "checkpoint preflight observed a reset after data_version sampling; admitting a full native re-anchor"
            );
            stage_native_snapshot(state, spool_state).await?;
        } else {
            let output = stage_native_shadow(state, spool_state).await?;
            apply_shadow_sync_result_to_state(state, &output).await?;
        }
        let admitted_seq = spool_lock(&spool_state.0)?
            .admitted_seq()
            .ok_or_else(|| anyhow!("{}: native spool has no admitted snapshot base", state.name))?;
        spool_lock(&spool_state.0)?.verify_durable_admission(admitted_seq)?;

        let data_version_after = checkpoint_data_version(state)?;
        if data_version_after == data_version_before {
            break admitted_seq;
        }
        tracing::debug!(
            database = %state.name,
            data_version_before,
            data_version_after,
            "application commit crossed native checkpoint preflight; draining the newly committed WAL frames before release"
        );
        data_version_before = data_version_after;
        if preflight_drain >= MAX_PREFLIGHT_DRAINS {
            tracing::warn!(
                database = %state.name,
                preflight_drains = preflight_drain,
                "native checkpoint preflight remained busy with application commits; blocker stays held and checkpoint will retry later"
            );
            return Ok(CheckpointAttempt {
                completed: false,
                dirty: false,
            });
        }
    };
    if checkpoint_release == crate::config::CheckpointRelease::Remote {
        wait_for_remote_publish(&spool_state.0, &state.name, admitted_seq, drain_timeout).await?;
    }
    spool_lock(&spool_state.0)?.begin_checkpoint_window(admitted_seq)?;

    let checkpoint_started = std::time::Instant::now();
    let attempt =
        checkpoint_with_state_blocker_attempt(state, checkpoint_mode, data_version_before)
            .await
            .with_context(|| format!("{}: shadow checkpoint failed", state.name))?;
    skip_rearm_heartbeat_transaction(state).await?;
    if attempt.dirty {
        spool_lock(&spool_state.0)?
            .mark_checkpoint_window_rearmed_dirty(admitted_seq, attempt.completed)?;
    } else {
        spool_lock(&spool_state.0)?.close_checkpoint_window(
            admitted_seq,
            attempt.completed,
            None,
        )?;
    }
    tracing::info!(
        database = %state.name,
        seq = admitted_seq,
        sqlite_checkpoint_ms = checkpoint_started.elapsed().as_millis() as u64,
        release = ?checkpoint_release,
        "controlled SQLite checkpoint completed after native spool admission"
    );

    if attempt.completed {
        let cleanup_before_gen = state.shadow_sync_generation;
        if cleanup_before_gen > 0 {
            state.shadow.cleanup_segments(cleanup_before_gen).await?;
        }
    }
    Ok(attempt)
}

/// The blocker heartbeat is an internal WAL transaction used only to acquire a
/// non-zero read mark. It must be the final SQLite operation, but it must not
/// cause walrust to checkpoint, rearm, and replicate another heartbeat forever.
/// Copy it through the checked/fsynced shadow path, then advance only across
/// that first committed transaction. Any app transaction racing behind it
/// remains after the shadow sync cursor for normal admission.
async fn skip_rearm_heartbeat_transaction(state: &mut ShadowDbState) -> Result<()> {
    let generation_before = state.shadow.generation();
    let (frames, new_offset) = state.shadow.copy_frames(state.wal_copy_offset).await?;
    state.wal_copy_offset = new_offset;
    if frames.is_empty() {
        return Ok(());
    }
    let heartbeat_end = frames
        .iter()
        .position(|frame| frame.db_size != 0)
        .ok_or_else(|| anyhow!("{}: blocker heartbeat has no WAL commit marker", state.name))?
        + 1;
    if state.shadow.generation() != generation_before {
        state.shadow_sync_generation = state.shadow.generation();
        state.shadow_sync_offset = 0;
    }
    let frame_size = 24 + state.shadow.page_size() as u64;
    state.shadow_sync_offset = state
        .shadow_sync_offset
        .saturating_add((heartbeat_end as u64).saturating_mul(frame_size));
    anyhow::ensure!(
        state.shadow_sync_offset <= state.shadow.segment_offset(),
        "{}: blocker heartbeat cursor exceeds fsynced shadow tail",
        state.name
    );
    save_shadow_progress(state)?;
    Ok(())
}

async fn live_wal_page_count(wal_path: &std::path::Path) -> Result<u64> {
    use tokio::io::AsyncReadExt;

    let metadata = match tokio::fs::metadata(wal_path).await {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(0),
        Err(error) => return Err(error.into()),
    };
    if metadata.len() < 32 {
        return Ok(0);
    }

    let mut file = tokio::fs::File::open(wal_path).await?;
    let mut header = [0u8; 32];
    file.read_exact(&mut header).await?;
    let page_size = u32::from_be_bytes([header[8], header[9], header[10], header[11]]) as u64;
    if page_size == 0 {
        return Ok(0);
    }

    Ok(metadata.len().saturating_sub(32) / (page_size + 24))
}

async fn reanchor_after_dirty_checkpoint(
    state: &mut ShadowDbState,
    target: DirectShadowSyncTarget,
    retry_policy: &RetryPolicy,
    webhook_sender: &Arc<WebhookSender>,
) -> Result<()> {
    tracing::error!(
        "{}: application commit crossed walrust's controlled checkpoint window; re-anchoring with a fresh snapshot",
        state.name
    );
    let mut db_state = DbState {
        name: state.name.clone(),
        db_path: state.db_path.clone(),
        wal_path: state.wal_path.clone(),
        wal_offset: 0,
        wal_generation: state.shadow.generation(),
        current_txid: state.current_txid,
        last_snapshot: state.last_snapshot,
        db_checksum: state.db_checksum,
        wal_salt: None,
        wal_checksum_chain: None,
    };
    take_snapshot_with_retry_and_rearm(
        &target.client,
        &target.bucket_name,
        &target.prefix,
        &mut db_state,
        state,
        retry_policy,
        webhook_sender,
    )
    .await
    .with_context(|| {
        format!(
            "{}: failed to protect dirty controlled-checkpoint window",
            state.name
        )
    })??;

    state.current_txid = db_state.current_txid;
    state.last_snapshot = db_state.last_snapshot;
    state.db_checksum = db_state.db_checksum;
    save_shadow_progress(state)?;
    Ok(())
}

#[allow(clippy::too_many_arguments)]
async fn enforce_wal_backpressure(
    state: &mut ShadowDbState,
    threshold_pages: u64,
    cache_state: Option<&(Arc<LocalCache>, mpsc::Sender<UploadMessage>)>,
    direct_target: Option<DirectShadowSyncTarget>,
    retry_policy: &RetryPolicy,
    webhook_sender: Arc<WebhookSender>,
    drain_timeout: Duration,
) -> Result<bool> {
    if threshold_pages == 0 {
        return Ok(false);
    }

    let wal_pages = live_wal_page_count(&state.wal_path)
        .await
        .with_context(|| format!("{}: cannot measure live WAL size", state.name))?;
    if wal_pages < threshold_pages {
        return Ok(false);
    }

    let alarm = format!(
        "{}: WAL backpressure alarm: live WAL reached {} pages (configured threshold: {}); \
         walrust may be falling behind and application writes can stall. Draining shadow data \
         durably before a controlled TRUNCATE checkpoint",
        state.name, wal_pages, threshold_pages
    );
    tracing::error!("{}", alarm);
    webhook_sender
        .send(
            WebhookPayload::custom(WAL_SIZE_EXCEEDED_EVENT, &state.name, &alarm, 1).with_context(
                serde_json::json!({
                    "wal_pages": wal_pages,
                    "threshold_pages": threshold_pages,
                    "wal_path": state.wal_path.display().to_string(),
                }),
            ),
        )
        .await;

    let checkpoint_window_dirty = checkpoint_shadow_after_durable_sync(
        state,
        cache_state,
        direct_target.clone(),
        retry_policy,
        Arc::clone(&webhook_sender),
        drain_timeout,
        ShadowCheckpointMode::Truncate,
    )
    .await
    .with_context(|| {
        format!(
            "{}: WAL backpressure checkpoint failed after threshold alarm",
            state.name
        )
    })?;

    if checkpoint_window_dirty {
        let target = direct_target.ok_or_else(|| {
            anyhow!(
                "{}: controlled checkpoint raced an app commit but no direct snapshot target is available",
                state.name
            )
        })?;
        reanchor_after_dirty_checkpoint(state, target, retry_policy, &webhook_sender).await?;
    }

    Ok(true)
}

async fn enforce_native_wal_backpressure(
    state: &mut ShadowDbState,
    threshold_pages: u64,
    spool_state: &NativeSpoolState,
    checkpoint_release: crate::config::CheckpointRelease,
    webhook_sender: Arc<WebhookSender>,
    drain_timeout: Duration,
) -> Result<bool> {
    if threshold_pages == 0 {
        return Ok(false);
    }
    let wal_pages = live_wal_page_count(&state.wal_path).await?;
    if wal_pages < threshold_pages {
        return Ok(false);
    }
    let alarm = format!(
        "{}: WAL backpressure alarm: live WAL reached {} pages (threshold {}); admitting native HADBP locally before controlled TRUNCATE",
        state.name, wal_pages, threshold_pages
    );
    tracing::error!("{}", alarm);
    webhook_sender
        .send(
            WebhookPayload::custom(WAL_SIZE_EXCEEDED_EVENT, &state.name, &alarm, 1).with_context(
                serde_json::json!({
                    "wal_pages": wal_pages,
                    "threshold_pages": threshold_pages,
                    "wal_path": state.wal_path.display().to_string(),
                }),
            ),
        )
        .await;
    let attempt = checkpoint_shadow_after_native_admission(
        state,
        spool_state,
        checkpoint_release,
        drain_timeout,
        ShadowCheckpointMode::Truncate,
    )
    .await?;
    if !attempt.completed {
        bail!(
            "{}: controlled TRUNCATE was partial/busy; blocker rearmed and retry scheduled",
            state.name
        );
    }
    if attempt.dirty {
        let reanchor_seq = stage_native_snapshot(state, spool_state).await?;
        spool_lock(&spool_state.0)?.complete_checkpoint_reanchor(reanchor_seq)?;
    }
    Ok(true)
}

async fn shutdown_shadow_uploaders(
    cache_states: &HashMap<PathBuf, (Arc<LocalCache>, mpsc::Sender<UploadMessage>)>,
    db_states: &HashMap<PathBuf, ShadowDbState>,
    uploader_handles: Vec<(PathBuf, tokio::task::JoinHandle<Result<UploaderStats>>)>,
) -> Result<()> {
    for (db_path, (_, upload_tx)) in cache_states.iter() {
        let name = db_states
            .get(db_path)
            .map(|s| s.name.as_str())
            .unwrap_or("unknown");
        tracing::debug!("{}: Sending shutdown to uploader", name);
        upload_tx
            .send(UploadMessage::Shutdown)
            .await
            .map_err(|e| anyhow!("{}: failed to send shutdown to uploader: {}", name, e))?;
    }

    let drain_timeout = Duration::from_secs(10);
    let mut first_error = None;

    for (db_path, handle) in uploader_handles {
        let name = db_states
            .get(&db_path)
            .map(|s| s.name.as_str())
            .unwrap_or("unknown");
        match tokio::time::timeout(drain_timeout, handle).await {
            Ok(Ok(Ok(stats))) => {
                tracing::debug!("{}: Uploader drained successfully: {:?}", name, stats);
            }
            Ok(Ok(Err(e))) => {
                let e = e.context(format!("{}: uploader drain failed", name));
                tracing::error!("{}", e);
                if first_error.is_none() {
                    first_error = Some(e);
                }
            }
            Ok(Err(e)) => {
                let e = anyhow!("{}: uploader task panicked: {}", name, e);
                tracing::error!("{}", e);
                if first_error.is_none() {
                    first_error = Some(e);
                }
            }
            Err(_) => {
                let e = anyhow!(
                    "{}: uploader drain timed out after {:?}",
                    name,
                    drain_timeout
                );
                tracing::error!("{}", e);
                if first_error.is_none() {
                    first_error = Some(e);
                }
            }
        }
    }

    match first_error {
        Some(e) => Err(e),
        None => Ok(()),
    }
}

/// Perform the initial shadow copy on startup and detect databases whose live
/// WAL salt changed while walrust was down.
///
/// Returns the set of database paths that should be snapshotted eagerly (D3).
async fn initial_shadow_copy(
    db_states: &mut HashMap<PathBuf, ShadowDbState>,
) -> Result<HashSet<PathBuf>> {
    let mut eager_snapshot: HashSet<PathBuf> = HashSet::new();

    for (_db_path, state) in db_states.iter_mut() {
        if state.shadow.discarded_unproven_tail() {
            tracing::error!(
                database = %state.name,
                "shadow startup discarded bytes beyond the durable fsync marker; scheduling conservative snapshot re-anchor"
            );
            state.shadow_sync_generation = state.shadow.generation();
            state.shadow_sync_offset = 0;
            eager_snapshot.insert(state.db_path.clone());
        }
        if !state.wal_path.exists() {
            continue;
        }

        let generation_before = state.shadow.generation();
        match state.shadow.copy_frames(state.wal_copy_offset).await {
            Ok((frames, new_offset)) => {
                if !frames.is_empty() {
                    tracing::debug!(
                        "{}: Initial shadow copy: {} frames",
                        state.name,
                        frames.len()
                    );
                    state.wal_copy_offset = new_offset;
                }

                if state.shadow.generation() > generation_before {
                    tracing::info!(
                        "{}: Downtime checkpoint detected (WAL salt mismatch); scheduling eager snapshot",
                        state.name
                    );
                    eager_snapshot.insert(state.db_path.clone());
                }
            }
            Err(e) => {
                tracing::error!("{}: Initial shadow copy failed: {}", state.name, e);
                return Err(e)
                    .with_context(|| format!("{}: initial shadow copy failed", state.name));
            }
        }
    }

    Ok(eager_snapshot)
}

/// Shadow WAL mode decouples S3 uploads from SQLite's active WAL file:
/// - Holds a read transaction to prevent SQLite auto-checkpoint
/// - Copies WAL frames to shadow directory on notification
/// - Uploads from shadow directory (not active WAL)
/// - Manually triggers checkpoints when ready
pub async fn watch_with_shadow(
    databases: Vec<ResolvedDbConfig>,
    bucket: &str,
    endpoint: Option<&str>,
    global_sync: SyncConfig,
    compact_policy: Option<RetentionPolicy>,
    metrics_port: u16,
    no_metrics: bool,
    retry_config: RetryConfig,
    webhooks: Vec<WebhookConfig>,
    cache_config: CacheConfig,
    spool_config: SpoolConfig,
) -> Result<()> {
    // Install process signal handlers before any startup work. Remote
    // discovery and initial native snapshot admission can both take long
    // enough for an operator to request shutdown; registering only after
    // startup left a window where SIGTERM took its default action and skipped
    // the durable local shutdown admission path entirely. Tokio's Unix signal
    // stream queues a signal until the main select loop is ready to consume it.
    #[cfg(unix)]
    let (mut sigterm, mut sigint) = {
        use signal::unix::{signal, SignalKind};
        (
            signal(SignalKind::terminate()).context("failed to install SIGTERM handler")?,
            signal(SignalKind::interrupt()).context("failed to install SIGINT handler")?,
        )
    };

    let (bucket_name, prefix) = parse_bucket(bucket);

    // Pin every source database before client construction or remote
    // discovery. Credential resolution and S3 can stall indefinitely; no app
    // checkpoint may erase unread WAL during that startup window.
    let mut db_locks: Vec<crate::lock::DbLock> = Vec::new();
    // Declared before the SQLite maps so error-path reverse local-drop order
    // closes blocker/monitor connections before these source descriptors.
    let mut startup_source_db_files: HashMap<PathBuf, Arc<Mutex<std::fs::File>>> = HashMap::new();
    let mut startup_blockers: HashMap<PathBuf, Connection> = HashMap::new();
    let mut startup_shadows: HashMap<PathBuf, ShadowWal> = HashMap::new();
    let mut startup_data_version_monitors: HashMap<PathBuf, Connection> = HashMap::new();
    let mut startup_db_checksums: HashMap<PathBuf, Result<ltx::Checksum>> = HashMap::new();
    for db_config in &databases {
        if !db_config.path.exists() {
            return Err(WalrustError::database(format!(
                "Database not found: {}",
                db_config.path.display()
            ))
            .into());
        }
        db_locks.push(crate::lock::DbLock::acquire(&db_config.path)?);
        let source_db_file = std::fs::File::open(&db_config.path).with_context(|| {
            format!(
                "{}: failed to open long-lived native snapshot source descriptor",
                db_config.prefix
            )
        })?;
        // Build every other same-database SQLite handle before the blocker.
        // Closing any handle after taking a POSIX blocker can drop the
        // process-wide inode locks, so blocker acquisition must be the final
        // SQLite operation for this database before remote startup.
        let shadow = ShadowWal::new_without_checkpoint_blocker(&db_config.path)
            .await
            .with_context(|| {
                format!(
                    "{}: failed to initialize shadow before remote startup",
                    db_config.prefix
                )
            })?;
        let data_version_monitor = Connection::open(&db_config.path).with_context(|| {
            format!(
                "{}: failed to open CLI data_version monitor",
                db_config.prefix
            )
        })?;
        data_version_monitor.busy_timeout(Duration::from_secs(5))?;
        ShadowWal::enable_persistent_wal(&data_version_monitor, &db_config.path)?;
        // This plain-file open targets the SQLite inode too. Complete it before
        // the final blocker operation so its close cannot release POSIX locks.
        startup_db_checksums.insert(
            db_config.path.clone(),
            ltx::compute_checksum_from_file(&db_config.path),
        );
        let blocker = ShadowWal::open_checkpoint_blocker(&db_config.path).with_context(|| {
            format!(
                "{}: failed to pin CLI checkpoint blocker before remote startup",
                db_config.prefix
            )
        })?;
        startup_shadows.insert(db_config.path.clone(), shadow);
        startup_data_version_monitors.insert(db_config.path.clone(), data_version_monitor);
        startup_blockers.insert(db_config.path.clone(), blocker);
        startup_source_db_files
            .insert(db_config.path.clone(), Arc::new(Mutex::new(source_db_file)));
    }
    if cfg!(debug_assertions) {
        if let Some(path) = std::env::var_os("WALRUST_TEST_STARTUP_DISCOVERY_PAUSE_FILE") {
            let path = PathBuf::from(path);
            std::fs::write(&path, b"blockers-attached")?;
            while path.exists() {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        }
    }
    let client = Arc::new(
        create_client(endpoint)
            .await
            .map_err(|e| WalrustError::s3(e.to_string()))?,
    );

    // Set up retry policy and webhook sender
    let webhook_sender = Arc::new(WebhookSender::new(webhooks));

    if retry_config.max_retries > 0 {
        tracing::info!(
            "Retry enabled: {} attempts, {}ms base delay, {}ms max delay",
            retry_config.max_retries,
            retry_config.base_delay_ms,
            retry_config.max_delay_ms
        );
    }
    if !webhook_sender.is_empty() {
        tracing::info!("Webhooks enabled for failure notifications");
    }

    // Set up metrics server (unless disabled)
    let metrics_state = Arc::new(MetricsState::new());
    if !no_metrics {
        let state_clone = Arc::clone(&metrics_state);
        tokio::spawn(async move {
            dashboard::start_server(metrics_port, state_clone).await;
        });
    }

    // Initialize cache + uploader per database (if cache enabled)
    let cache_states: HashMap<PathBuf, (Arc<LocalCache>, mpsc::Sender<UploadMessage>)> =
        HashMap::new();
    let uploader_handles: Vec<(PathBuf, tokio::task::JoinHandle<Result<UploaderStats>>)> =
        Vec::new();
    let mut native_spools: HashMap<PathBuf, NativeSpoolState> = HashMap::new();
    let mut native_uploader_shutdown: HashMap<PathBuf, tokio::sync::watch::Sender<bool>> =
        HashMap::new();
    let mut native_uploader_handles: Vec<(PathBuf, tokio::task::JoinHandle<()>)> = Vec::new();
    let mut native_lag_states: HashMap<
        PathBuf,
        Arc<Mutex<walrust_core::native_publish::RemoteLagState>>,
    > = HashMap::new();
    if cache_config.enabled {
        tracing::info!(
            "Legacy LTX cache migration compatibility enabled (concurrency={}, retention={}, max_size={})",
            cache_config.uploader_concurrency,
            cache_config.retention,
            cache_config.max_size,
        );
    }

    // Initialize shadow state for each database
    let mut db_states: HashMap<PathBuf, ShadowDbState> = HashMap::new();
    let mut trigger_states: HashMap<PathBuf, TriggerState> = HashMap::new();
    let mut sync_configs: HashMap<PathBuf, SyncConfig> = HashMap::new();
    let mut required_native_reanchors: HashSet<PathBuf> = HashSet::new();
    for db_config in &databases {
        let db_path = &db_config.path;

        let name = db_config.prefix.clone();
        let wal_path = db_path.with_extension("db-wal");

        let spool_base = spool_config.path.clone().unwrap_or_else(|| {
            db_path
                .parent()
                .unwrap_or_else(|| std::path::Path::new("."))
                .join(".walrust-spool")
        });
        let binding_identity = SpoolIdentity::new(
            db_path,
            bucket_name.clone(),
            prefix.clone(),
            name.clone(),
            "binding-only",
            1,
            None,
            false,
        )?;
        let spool_root = NativeSpool::resolve_path_for(&spool_base, &binding_identity)?;
        let existing_spool_identity = NativeSpool::read_identity(&spool_root)?;

        // A complete matching local spool is sufficient for offline restart.
        // Without it, startup must verify both the versioned native descriptor
        // and legacy manifest remotely; ambiguous cloud failure is never fresh.
        //
        // H8 cousin (PR #36 review): a TRANSIENT manifest fetch failure — or a
        // present-but-unparseable manifest — must NOT be silently read as "fresh
        // database, txid 0". That default is safe ONLY for a genuine not-found; a
        // transient (or a corrupt manifest) that defaults to fresh lets a replica
        // adopt a fresh identity over existing remote state. Durable local shadow
        // progress overrides this seed when present, and publishes are CAS-guarded
        // (so the worst case is a loud CAS failure, not a silent fork) — but on a
        // fresh host with no local progress a transient would still misclassify.
        // Only a CONFIRMED not-found starts fresh; every other fetch/parse error
        // propagates so startup fails loudly and is retried against a complete
        // view. See `seed_state_from_manifest_fetch`. Not-found is classified
        // from the TYPED SDK error (`s3::download_error_is_not_found`), never
        // by matching message strings — free text like a DNS "host not found"
        // must not read as a missing manifest and silently start fresh.
        let (mut current_txid, manifest_checksum, spool_identity) = if let Some(identity) =
            existing_spool_identity
        {
            if !identity.remote_base_verified
                || identity.canonical_db_path != binding_identity.canonical_db_path
                || identity.bucket != bucket_name
                || identity.prefix != prefix
                || identity.database != name
            {
                bail!("{}: local native spool identity/base mismatch", name);
            }
            if !NativeSpool::validate_existing_complete_base(&spool_root, &identity)? {
                let descriptor_key = format!("{}{}/native/v1/stream.json", prefix, name);
                match s3::download_bytes(&client, &bucket_name, &descriptor_key).await {
                    Ok(_) => bail!(
                        "{}: local native spool has no complete snapshot base but the remote native stream already exists",
                        name
                    ),
                    Err(error) if s3::download_error_is_not_found(&error) => {
                        let (legacy_txid, _, _) = discover_state_from_s3(
                            &client,
                            &bucket_name,
                            &prefix,
                            &name,
                        )
                        .await
                        .with_context(|| {
                            format!(
                                "{}: verify remote predecessor for incomplete local spool",
                                name
                            )
                        })?;
                        if identity.legacy_boundary_txid.unwrap_or(0) != legacy_txid {
                            bail!(
                                "{}: incomplete local spool predecessor differs from remote legacy head",
                                name
                            );
                        }
                    }
                    Err(error) => {
                        return Err(error).with_context(|| {
                            format!(
                                "{}: remote unavailable and local native spool has no complete snapshot base",
                                name
                            )
                        })
                    }
                }
            }
            (0, None, identity)
        } else {
            // One-time compatibility migration: drain any durable legacy
            // LTX cache before pinning the legacy boundary. This preserves
            // every locally pending 0.7 PITR object, then the native stream
            // re-anchors with a full snapshot. The new spool never adopts
            // or rewrites these bytes.
            let configured_legacy_cache = cache_config.path.as_ref().map(PathBuf::from);
            let legacy_cache_path = configured_legacy_cache
                .filter(|path| path.join("manifest.json").exists())
                .unwrap_or_else(|| LocalCache::cache_dir_for_db(db_path));
            if let Some(legacy_cache) = LocalCache::open(&legacy_cache_path)? {
                if !legacy_cache.pending_uploads().is_empty() {
                    tracing::info!(
                        database = %name,
                        cache = %legacy_cache_path.display(),
                        "draining verified legacy LTX cache before native HADBP migration boundary"
                    );
                    let storage: Arc<dyn StorageBackend> =
                        Arc::new(S3Storage::new((*client).clone(), bucket_name.clone()));
                    for txid in legacy_cache.pending_uploads() {
                        let bytes = legacy_cache.read_ltx(txid)?;
                        walrust_core::legacy_ltx::verify_ltx(std::io::Cursor::new(&bytes))?;
                        let (generation, min_txid, max_txid) = legacy_cache.remote_key_parts(txid);
                        let key = walrust_core::legacy_manifest::build_ltx_key(
                            &prefix, &name, generation, min_txid, max_txid,
                        );
                        let cas = storage.put_if_absent(&key, &bytes).await?;
                        if !cas.success {
                            let existing = storage.get(&key).await?.ok_or_else(|| {
                                anyhow!("legacy migration object vanished after CAS: {key}")
                            })?;
                            if existing != bytes {
                                bail!(
                                        "{}: divergent legacy LTX already exists at {}; refusing overwrite",
                                        name,
                                        key
                                    );
                            }
                        }
                        legacy_cache.mark_uploaded(txid)?;
                    }
                    if !legacy_cache.pending_uploads().is_empty() {
                        bail!("{}: legacy cache did not drain contiguously; refusing native migration", name);
                    }
                }
            }
            let descriptor_key = format!("{}{}/native/v1/stream.json", prefix, name);
            match s3::download_bytes(&client, &bucket_name, &descriptor_key).await {
                    Ok(_) => bail!(
                        "{}: remote native stream exists but no matching verified local spool/base is present",
                        name
                    ),
                    Err(error) if s3::download_error_is_not_found(&error) => {}
                    Err(error) => {
                        return Err(error).with_context(|| {
                            format!(
                                "{}: native stream discovery unavailable and no verified local spool exists",
                                name
                            )
                        })
                    }
                }
            let (legacy_txid, _generation, checksum) =
                discover_state_from_s3(&client, &bucket_name, &prefix, &name)
                    .await
                    .with_context(|| format!("{}: discover verified legacy history", name))?;
            if legacy_txid > 0 {
                let verify_path = spool_base.join(format!(
                    ".legacy-migration-verify-{}-{}.db",
                    std::process::id(),
                    uuid::Uuid::new_v4()
                ));
                std::fs::create_dir_all(&spool_base)?;
                let storage = S3Storage::new((*client).clone(), bucket_name.clone());
                verify_legacy_migration_head(&storage, &prefix, &name, &verify_path, legacy_txid)
                    .await?;
            }
            let identity = SpoolIdentity::new(
                db_path,
                bucket_name.clone(),
                prefix.clone(),
                name.clone(),
                uuid::Uuid::new_v4().to_string(),
                legacy_txid.saturating_add(1).max(1),
                (legacy_txid > 0).then_some(legacy_txid),
                true,
            )?;
            (legacy_txid, checksum, identity)
        };

        // Get initial checksum: from manifest if available, otherwise compute from db
        let mut db_checksum = match manifest_checksum {
            Some(cs) => {
                tracing::debug!("{}: Using checksum from manifest: {:#x}", name, cs);
                Some(cs)
            }
            None => match startup_db_checksums
                .remove(db_path)
                .ok_or_else(|| anyhow!("{}: startup database checksum missing", name))?
            {
                Ok(cs) => {
                    tracing::debug!(
                        "{}: Computed initial checksum: {:#x}",
                        name,
                        cs.into_inner()
                    );
                    Some(cs.into_inner())
                }
                Err(e) => {
                    tracing::warn!("{}: Could not compute initial checksum: {}", name, e);
                    None
                }
            },
        };
        let mut last_snapshot = None;
        let mut shadow_sync_generation = 0;
        let mut shadow_sync_offset = 0;

        // Create shadow WAL manager (this holds the checkpoint blocker)
        let mut shadow = startup_shadows
            .remove(db_path)
            .ok_or_else(|| anyhow!("{}: startup shadow state missing", name))?;
        let data_version_monitor = startup_data_version_monitors
            .remove(db_path)
            .ok_or_else(|| anyhow!("{}: startup data_version monitor missing", name))?;
        let checkpoint_blocker = startup_blockers
            .remove(db_path)
            .ok_or_else(|| anyhow!("{}: startup checkpoint blocker missing", name))?;
        let source_db_file = startup_source_db_files
            .remove(db_path)
            .ok_or_else(|| anyhow!("{}: startup source descriptor missing", name))?;

        let mut restored_wal_copy_offset = 0u64;
        if let Some(progress) = load_shadow_progress(&shadow, &name)? {
            tracing::info!(
                "{}: restored durable shadow progress (TXID: {}, generation: {}, offset: {}, wal_copy_offset: {})",
                name,
                progress.current_txid,
                progress.shadow_sync_generation,
                progress.shadow_sync_offset,
                progress.wal_copy_offset
            );
            current_txid = progress.current_txid;
            last_snapshot = progress.last_snapshot;
            db_checksum = progress.db_checksum;
            shadow_sync_generation = progress.shadow_sync_generation;
            shadow_sync_offset = progress.shadow_sync_offset;
            // B4 restart-window: normally resume the persisted WAL cursor and
            // checksum chain. A discarded markerless shadow must instead
            // recopy from zero because its progress bytes no longer exist.
            restored_wal_copy_offset = restore_wal_copy_progress(&mut shadow, &progress);
        }

        tracing::info!(
            "Shadow WAL: Watching {} as '{}' (TXID: {}, generation: {}, shadow dir: {})",
            db_path.display(),
            name,
            current_txid,
            shadow.generation(),
            shadow.shadow_dir().display()
        );

        let capacity = CapacityPolicy {
            warning_bytes: spool_config.warning_size,
            hard_bytes: spool_config.max_size,
            minimum_free_bytes: spool_config.min_free_space,
        };
        let spool = Arc::new(Mutex::new(NativeSpool::create_or_open(
            &spool_root,
            spool_identity,
            capacity,
        )?));
        {
            let guard = spool_lock(&spool)?;
            if let Some(last) = guard.objects().last() {
                current_txid = last.seq;
                db_checksum = Some(last.ending_chain_checksum);
                last_snapshot = guard
                    .objects()
                    .filter(|object| object.kind == ObjectKind::Snapshot)
                    .last()
                    .map(|_| chrono::Utc::now());
            }
        }
        let storage: Arc<dyn StorageBackend> =
            Arc::new(S3Storage::new((*client).clone(), bucket_name.clone()));
        let (native_uploader, wake, lag) =
            NativeUploader::new(Arc::clone(&storage), Arc::clone(&spool))?;
        let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
        let handle = spawn_native_uploader_supervisor(
            native_uploader,
            storage,
            Arc::clone(&spool),
            wake.clone(),
            Arc::clone(&lag),
            shutdown_rx,
        );
        native_spools.insert(db_path.clone(), (spool, wake));
        native_lag_states.insert(db_path.clone(), lag);
        native_uploader_shutdown.insert(db_path.clone(), shutdown_tx);
        native_uploader_handles.push((db_path.clone(), handle));

        db_states.insert(
            db_path.clone(),
            ShadowDbState {
                name,
                db_path: db_path.clone(),
                wal_path,
                current_txid,
                last_snapshot,
                db_checksum,
                shadow,
                checkpoint_blocker: Some(checkpoint_blocker),
                data_version_monitor: Some(data_version_monitor),
                source_db_file: Some(source_db_file),
                shadow_sync_generation,
                shadow_sync_offset,
                wal_copy_offset: restored_wal_copy_offset,
            },
        );
        let state = db_states
            .get_mut(db_path)
            .expect("shadow state inserted above");
        // Several startup operations open and close SQLite handles. On POSIX a
        // close can release process-scoped fcntl locks, so rearm unconditionally
        // after the final such operation. This is the final SQLite operation in
        // successful startup.
        rearm_checkpoint_blocker(state).with_context(|| {
            format!(
                "{}: failed to finalize initial CLI checkpoint blocker",
                state.name
            )
        })?;
        anyhow::ensure!(
            checkpoint_blocker_heartbeat_is_live(state)?,
            "{}: initial CLI checkpoint blocker is not live",
            state.name
        );
        let recovery_head = {
            let spool_state = native_spools
                .get(db_path)
                .ok_or_else(|| anyhow!("{}: native spool missing after startup", state.name))?;
            let guard = spool_lock(&spool_state.0)?;
            if guard.requires_checkpoint_reanchor() {
                required_native_reanchors.insert(db_path.clone());
            }
            guard.recovery_head()
        };
        reconcile_shadow_progress_from_spool(state, recovery_head).await?;

        trigger_states.insert(db_path.clone(), TriggerState::default());
        sync_configs.insert(db_path.clone(), db_config.sync.clone());

        // Update dashboard with initial state
        let wal_size = std::fs::metadata(&db_path.with_extension("db-wal"))
            .map(|m| m.len())
            .unwrap_or(0);
        metrics_state
            .update_db(DbStatus {
                name: db_config.prefix.clone(),
                path: db_path.display().to_string(),
                last_sync_timestamp: 0,
                wal_size_bytes: wal_size,
                next_snapshot_timestamp: chrono::Utc::now().timestamp()
                    + global_sync.snapshot_interval as i64,
                error_count: 0,
                snapshot_count: 0,
                current_txid,
                last_error: None,
                errors_last_hour: None,
            })
            .await;
    }

    // Initial copy of any existing WAL data to shadow. If the live WAL salt
    // changed while we were down, `copy_frames` bumps the shadow generation;
    // those databases need an eager snapshot instead of waiting for the periodic
    // timer (D3).
    let eager_snapshot_paths = initial_shadow_copy(&mut db_states).await?;

    let mut startup_reanchored = HashSet::new();
    for db_path in required_native_reanchors.clone() {
        let state = db_states
            .get_mut(&db_path)
            .ok_or_else(|| anyhow!("checkpoint recovery database disappeared"))?;
        let spool_state = native_spools
            .get(&db_path)
            .ok_or_else(|| anyhow!("{}: native spool was not initialized", state.name))?;
        let seq = require_native_snapshot(state, spool_state, "open_checkpoint_window_restart")
            .await
            .with_context(|| {
                format!("{}: checkpoint-window recovery snapshot failed", state.name)
            })?;
        spool_lock(&spool_state.0)?.complete_checkpoint_reanchor(seq)?;
        required_native_reanchors.remove(&db_path);
        startup_reanchored.insert(db_path);
    }

    // Take initial snapshots if on_startup is enabled, skipping any DB that is
    // already scheduled for an eager snapshot below.
    for (db_path, state) in db_states.iter_mut() {
        let sync_config = sync_configs.get(db_path).unwrap_or(&global_sync);
        let spool_state = native_spools
            .get(db_path)
            .ok_or_else(|| anyhow!("{}: native spool was not initialized", state.name))?;
        let needs_base = spool_lock(&spool_state.0)?.admitted_seq().is_none();
        if (needs_base || sync_config.on_startup)
            && !eager_snapshot_paths.contains(db_path)
            && !startup_reanchored.contains(db_path)
        {
            require_native_snapshot(state, spool_state, "initial_base")
                .await
                .with_context(|| {
                    format!("{}: initial native snapshot admission failed", state.name)
                })?;
            if let Some(trigger) = trigger_states.get_mut(db_path) {
                trigger.frames_since_snapshot = 0;
                trigger.first_change_time = None;
            }
        }
    }

    // D3: eager snapshot for any database whose live WAL salt changed while we
    // were down. This runs even when on_startup is disabled, so we do not wait
    // for the periodic snapshot timer after a downtime checkpoint.
    for db_path in &eager_snapshot_paths {
        if startup_reanchored.contains(db_path) {
            continue;
        }
        let state = db_states
            .get_mut(db_path)
            .expect("eager snapshot path must exist in db_states");
        let spool_state = native_spools
            .get(db_path)
            .ok_or_else(|| anyhow!("{}: native spool was not initialized", state.name))?;
        require_native_snapshot(state, spool_state, "downtime_reanchor")
            .await
            .with_context(|| format!("{}: downtime native re-anchor failed", state.name))?;
        metrics_state.record_snapshot(&state.name);
        if let Some(trigger) = trigger_states.get_mut(db_path) {
            trigger.frames_since_snapshot = 0;
            trigger.first_change_time = None;
        }
    }

    // Set up periodic timers
    let snapshot_interval = Duration::from_secs(global_sync.snapshot_interval);
    let mut snapshot_timer = tokio::time::interval(snapshot_interval);
    // Initial-base/on-startup/downtime snapshots were handled above. Consume
    // Tokio's immediate first tick so startup does not emit a second full
    // snapshot instead of the first native delta.
    snapshot_timer.tick().await;

    let wal_sync_interval = Duration::from_secs(global_sync.wal_sync_interval);
    let mut wal_sync_timer = tokio::time::interval(wal_sync_interval);
    wal_sync_timer.tick().await;

    let disabled_timer_duration = Duration::from_secs(86400 * 365);
    let compact_interval_duration = if global_sync.compact_interval > 0 {
        Duration::from_secs(global_sync.compact_interval)
    } else {
        disabled_timer_duration
    };
    let mut compact_timer = tokio::time::interval(compact_interval_duration);
    compact_timer.tick().await;

    // Shadow mode: checkpoint is manual via shadow.checkpoint()
    let checkpoint_interval_duration = if global_sync.checkpoint_interval > 0 {
        Duration::from_secs(global_sync.checkpoint_interval)
    } else {
        disabled_timer_duration
    };
    let mut checkpoint_timer = tokio::time::interval(checkpoint_interval_duration);
    checkpoint_timer.tick().await;

    let trigger_interval_duration = Duration::from_secs(global_sync.wal_sync_interval);
    let mut trigger_timer = tokio::time::interval(trigger_interval_duration);

    let validation_interval_duration = if global_sync.validation_interval > 0 {
        Duration::from_secs(global_sync.validation_interval)
    } else {
        disabled_timer_duration
    };
    let mut validation_timer = tokio::time::interval(validation_interval_duration);
    validation_timer.tick().await;

    // Cache/native-spool cleanup timer (every 5 minutes in production). The
    // debug-only override lets parent/child durability tests SIGKILL the two
    // cleanup journal boundaries without waiting five minutes.
    let cleanup_interval = if cfg!(debug_assertions) {
        std::env::var("WALRUST_TEST_NATIVE_CLEANUP_INTERVAL_MS")
            .ok()
            .and_then(|raw| raw.parse::<u64>().ok())
            .filter(|millis| *millis > 0)
            .map(Duration::from_millis)
            .unwrap_or_else(|| Duration::from_secs(300))
    } else {
        Duration::from_secs(300)
    };
    let mut cache_cleanup_timer = tokio::time::interval(cleanup_interval);
    cache_cleanup_timer.tick().await; // Skip first immediate tick

    // Parse cache retention for cleanup
    let cache_retention = if cache_config.enabled {
        crate::config::parse_duration_string(&cache_config.retention).ok()
    } else {
        None
    };

    tracing::info!(
        "walrust shadow mode running (snapshot: {}s, WAL sync: {}s, checkpoint: {}s)",
        global_sync.snapshot_interval,
        global_sync.wal_sync_interval,
        global_sync.checkpoint_interval
    );

    // Set up shutdown signal
    let shutdown_signal = async {
        #[cfg(unix)]
        {
            tokio::select! {
                _ = sigterm.recv() => "SIGTERM",
                _ = sigint.recv() => "SIGINT",
            }
        }
        #[cfg(not(unix))]
        {
            signal::ctrl_c()
                .await
                .expect("Failed to set up Ctrl+C handler");
            "Ctrl+C"
        }
    };
    tokio::pin!(shutdown_signal);

    loop {
        tokio::select! {
            // Handle shutdown signals
            signal_name = &mut shutdown_signal => {
                tracing::info!("Received {}, initiating graceful shutdown...", signal_name);
                break;
            }

            // Poll and sync at wal_sync_interval
            _ = wal_sync_timer.tick() => {
                // Copy any new WAL frames to shadow for all databases
                for state in db_states.values_mut() {
                    let source_filesystem = state
                        .db_path
                        .parent()
                        .unwrap_or_else(|| std::path::Path::new("."));
                    if filesystem_available_bytes(source_filesystem)?
                        < spool_config.min_free_space
                    {
                        tracing::error!(
                            database = %state.name,
                            event = "local_spool_full",
                            filesystem = %source_filesystem.display(),
                            filesystem_free_bytes = filesystem_available_bytes(source_filesystem)?,
                            reserve_bytes = spool_config.min_free_space,
                            "source WAL/shadow filesystem is below its free-space reserve; retaining blocker and pausing admission/checkpoints"
                        );
                        continue;
                    }
                    match state.shadow.copy_frames(state.wal_copy_offset).await {
                        Ok((frames, new_offset)) => {
                            tracing::debug!(
                                database = %state.name,
                                frames = frames.len(),
                                wal_offset_before = state.wal_copy_offset,
                                wal_offset_after = new_offset,
                                wal_bytes = std::fs::metadata(&state.wal_path).map(|metadata| metadata.len()).unwrap_or(0),
                                shadow_generation = state.shadow.generation(),
                                shadow_bytes = state.shadow.segment_offset(),
                                "polled native live WAL into shadow"
                            );
                            if !frames.is_empty() {
                                tracing::debug!(
                                    "{}: Copied {} frames to shadow (offset {} -> {})",
                                    state.name,
                                    frames.len(),
                                    state.wal_copy_offset,
                                    new_offset
                                );
                                state.wal_copy_offset = new_offset;
                            }
                        }
                        Err(e) => {
                            tracing::error!("{}: Shadow copy failed: {}", state.name, e);
                            webhook_sender
                                .notify_upload_failed(&state.name, &e.to_string(), 1)
                                .await;
                            // Retain the live-WAL blocker and retry. Exiting here
                            // would release the only protection for uncopied
                            // committed frames.
                            continue;
                        }
                    }
                }

                for (db_path, (spool, _)) in &native_spools {
                    if spool_lock(spool)?.requires_checkpoint_reanchor() {
                        required_native_reanchors.insert(db_path.clone());
                    }
                }
                for db_path in required_native_reanchors.clone() {
                    let Some(state) = db_states.get_mut(&db_path) else { continue };
                    let Some(spool) = native_spools.get(&db_path) else { continue };
                    match stage_native_snapshot(state, spool).await {
                        Ok(seq) => {
                            spool_lock(&spool.0)?.complete_checkpoint_reanchor(seq)?;
                            required_native_reanchors.remove(&db_path);
                        }
                        Err(error) => {
                            tracing::error!(
                                database = %state.name,
                                error = %error,
                                "native re-anchor still cannot be admitted; retaining blocker and pausing deltas"
                            );
                        }
                    }
                }
                let mut results = Vec::with_capacity(db_states.len());
                for (db_path, state) in &db_states {
                    if required_native_reanchors.contains(db_path) {
                        results.push(Err(anyhow!(
                            "{}: native re-anchor required before delta continuation",
                            state.name
                        )));
                        continue;
                    }
                    let result = match native_spools.get(db_path) {
                        Some(spool) => stage_native_shadow(state, spool).await,
                        None => Err(anyhow!("{}: native spool missing", state.name)),
                    };
                    results.push(result);
                }

                // Phase 3: Apply results sequentially
                for result in results {
                    match result {
                        Ok(output) => {
                            let frame_count = output.frame_count;

                            if let Some(state) = db_states.get_mut(&output.db_path) {
                                apply_shadow_sync_result_to_state(state, &output).await?;

                                if frame_count == 0 {
                                    continue;
                                }

                                // Update dashboard
                                let shadow_size = walkdir::WalkDir::new(state.shadow.shadow_dir())
                                    .into_iter()
                                    .filter_map(|e| e.ok())
                                    .filter_map(|e| e.metadata().ok())
                                    .map(|m| m.len())
                                    .sum::<u64>();

                                metrics_state.update_db(DbStatus {
                                    name: state.name.clone(),
                                    path: state.db_path.display().to_string(),
                                    last_sync_timestamp: chrono::Utc::now().timestamp(),
                                    wal_size_bytes: std::fs::metadata(&state.wal_path)
                                        .map(|metadata| metadata.len())
                                        .unwrap_or(0),
                                    next_snapshot_timestamp: state.last_snapshot.map(|t| t.timestamp() + global_sync.snapshot_interval as i64).unwrap_or(0),
                                    error_count: 0,
                                    snapshot_count: 0,
                                    current_txid: state.current_txid,
                                    last_error: None,
                                    errors_last_hour: None,
                                }).await;
                                metrics_state
                                    .shadow_bytes
                                    .with_label_values(&[&state.name])
                                    .set(shadow_size.min(i64::MAX as u64) as i64);

                                // Update trigger state
                                if let Some(trigger) = trigger_states.get_mut(&output.db_path) {
                                    trigger.frames_since_snapshot += frame_count;
                                    trigger.last_wal_activity = Some(std::time::Instant::now());
                                    if trigger.first_change_time.is_none() {
                                        trigger.first_change_time = Some(std::time::Instant::now());
                                    }

                                    // Check max_changes trigger
                                    let sync_config = sync_configs.get(&output.db_path).unwrap_or(&global_sync);
                                    if sync_config.max_changes > 0
                                        && trigger.frames_since_snapshot >= sync_config.max_changes
                                    {
                                        tracing::info!(
                                            "{}: max_changes trigger ({} frames)",
                                            state.name,
                                            trigger.frames_since_snapshot
                                        );
                                        let snapshot_result = match native_spools.get(&output.db_path) {
                                            Some(spool) => stage_native_snapshot(state, spool).await,
                                            None => Err(anyhow!("{}: native spool missing", state.name)),
                                        };
                                        if let Err(e) = snapshot_result {
                                            tracing::error!("Failed to snapshot {}: {}", state.name, e);
                                            metrics_state.record_error(&state.name);
                                        } else {
                                            metrics_state.record_snapshot(&state.name);
                                            trigger.frames_since_snapshot = 0;
                                            trigger.first_change_time = None;
                                        }
                                    }
                                }
                            }
                        }
                        Err(e) => {
                            tracing::error!("Shadow sync failed: {}", e);
                            // The checkpoint blocker remains held. Local spool
                            // capacity/disk warnings are degraded
                            // non-checkpointing states, never permission to
                            // exit and release the blocker.
                            continue;
                        }
                    }
                }

                for (db_path, (spool, _)) in &native_spools {
                    let Some(state) = db_states.get(db_path) else { continue };
                    let guard = spool_lock(spool)?;
                    metrics_state
                        .spool_bytes
                        .with_label_values(&[&state.name])
                        .set(guard.used_bytes()?.min(i64::MAX as u64) as i64);
                    metrics_state
                        .spool_free_bytes
                        .with_label_values(&[&state.name])
                        .set(guard.free_bytes()?.min(i64::MAX as u64) as i64);
                    metrics_state
                        .native_stage_duration_seconds
                        .with_label_values(&[&state.name])
                        .set(guard.last_stage_duration_ms() as f64 / 1000.0);
                    let capacity_state = guard.last_capacity_state();
                    metrics_state
                        .local_spool_high
                        .with_label_values(&[&state.name])
                        .set(i64::from(capacity_state == CapacityState::High));
                    metrics_state
                        .local_spool_full
                        .with_label_values(&[&state.name])
                        .set(i64::from(capacity_state == CapacityState::Full));
                    metrics_state
                        .shadow_bytes
                        .with_label_values(&[&state.name])
                        .set(shadow_storage_bytes(state).min(i64::MAX as u64) as i64);
                    metrics_state
                        .wal_size
                        .with_label_values(&[&state.name])
                        .set(
                            std::fs::metadata(&state.wal_path)
                                .map(|metadata| metadata.len())
                                .unwrap_or(0)
                                .min(i64::MAX as u64) as i64,
                        );
                    drop(guard);
                    if let Some(lag) = native_lag_states.get(db_path) {
                        if let Ok(lag) = lag.lock() {
                            metrics_state
                                .remote_lag_objects
                                .with_label_values(&[&state.name])
                                .set(lag.pending_objects.min(i64::MAX as u64) as i64);
                            metrics_state
                                .remote_lag_bytes
                                .with_label_values(&[&state.name])
                                .set(lag.pending_bytes.min(i64::MAX as u64) as i64);
                            metrics_state
                                .remote_lag_age_seconds
                                .with_label_values(&[&state.name])
                                .set(lag.oldest_age_ms as f64 / 1000.0);
                            metrics_state
                                .native_upload_duration_seconds
                                .with_label_values(&[&state.name])
                                .set(lag.last_upload_duration_ms as f64 / 1000.0);
                        }
                    }
                }

                // Holding the checkpoint blocker makes walrust responsible for
                // bounding the live WAL. Alarm before the configured ceiling turns
                // into application write stalls, then drain every copied frame to
                // durable storage before opening the controlled TRUNCATE window.
                for (db_path, state) in db_states.iter_mut() {
                    let sync_config = sync_configs.get(db_path).unwrap_or(&global_sync);
                    let Some(spool_state) = native_spools.get(db_path) else {
                        return Err(anyhow!("{}: native spool missing", state.name));
                    };
                    if let Err(error) = enforce_native_wal_backpressure(
                        state,
                        sync_config.wal_truncate_threshold_pages,
                        spool_state,
                        sync_config.checkpoint_release,
                        Arc::clone(&webhook_sender),
                        CHECKPOINT_UPLOAD_DRAIN_TIMEOUT,
                    ).await {
                        tracing::error!("{}: {}", state.name, error);
                        required_native_reanchors.insert(db_path.clone());
                        webhook_sender
                            .notify_upload_failed(&state.name, &error.to_string(), 1)
                            .await;
                        // Retain the blocker and local spool. A recoverable
                        // capacity/cloud/checkpoint warning must not terminate
                        // shadow watch and release the pin.
                        continue;
                    }
                }
            }

            // Trigger timer for max_interval and on_idle checks
            _ = trigger_timer.tick() => {
                let now = std::time::Instant::now();

                for (db_path, trigger) in trigger_states.iter_mut() {
                    let sync_config = sync_configs.get(db_path).unwrap_or(&global_sync);

                    if trigger.frames_since_snapshot == 0 {
                        continue;
                    }

                    let state = match db_states.get_mut(db_path) {
                        Some(s) => s,
                        None => continue,
                    };

                    let mut should_snapshot = false;
                    let mut reason = "";

                    // Check max_interval
                    if sync_config.max_interval > 0 {
                        if let Some(first_change) = trigger.first_change_time {
                            if now.duration_since(first_change).as_secs() >= sync_config.max_interval {
                                should_snapshot = true;
                                reason = "max_interval";
                            }
                        }
                    }

                    // Check on_idle
                    if !should_snapshot && sync_config.on_idle > 0 {
                        if let Some(last_activity) = trigger.last_wal_activity {
                            if now.duration_since(last_activity).as_secs() >= sync_config.on_idle {
                                should_snapshot = true;
                                reason = "on_idle";
                            }
                        }
                    }

                    if should_snapshot {
                        tracing::info!(
                            "{}: {} trigger ({} frames)",
                            state.name,
                            reason,
                            trigger.frames_since_snapshot
                        );

                        let snapshot_result = match native_spools.get(db_path) {
                            Some(spool) => stage_native_snapshot(state, spool).await,
                            None => Err(anyhow!("{}: native spool missing", state.name)),
                        };
                        if let Err(e) = snapshot_result {
                            tracing::error!("Failed to snapshot {}: {}", state.name, e);
                            metrics_state.record_error(&state.name);
                        } else {
                            metrics_state.record_snapshot(&state.name);
                            trigger.frames_since_snapshot = 0;
                            trigger.first_change_time = None;
                        }
                    }
                }

            }

            // Periodic snapshot timer
            _ = snapshot_timer.tick() => {
                for (db_path, state) in db_states.iter_mut() {
                    let snapshot_result = match native_spools.get(db_path) {
                        Some(spool) => stage_native_snapshot(state, spool).await,
                        None => Err(anyhow!("{}: native spool missing", state.name)),
                    };
                    anyhow::ensure!(
                        state
                            .checkpoint_blocker
                            .as_ref()
                            .is_some_and(|blocker| !blocker.is_autocommit()),
                        "{}: CLI checkpoint blocker transaction ended during periodic snapshot",
                        state.name
                    );
                    if let Err(e) = snapshot_result {
                        tracing::error!("Failed to snapshot {}: {}", state.name, e);
                        metrics_state.record_error(&state.name);
                    } else {
                        metrics_state.record_snapshot(&state.name);

                        if let Some(trigger) = trigger_states.get_mut(db_path) {
                            trigger.frames_since_snapshot = 0;
                            trigger.first_change_time = None;
                        }
                    }
                }

                // Run retention pruning after snapshots if enabled
                if global_sync.compact_after_snapshot {
                    if let Some(ref policy) = compact_policy {
                        for (db_path, state) in &db_states {
                            let Some(spool_state) = native_spools.get(db_path) else {
                                tracing::error!("Failed to prune {}: native spool missing", state.name);
                                continue;
                            };
                            if let Err(e) = prune_watcher_database(&client, &bucket_name, &prefix, state, spool_state, policy).await {
                                tracing::error!("Failed to prune {}: {}", state.name, e);
                            }
                        }
                    }
                }

            }

            // Pruning timer
            _ = compact_timer.tick(), if global_sync.compact_interval > 0 => {
                if let Some(ref policy) = compact_policy {
                    for (db_path, state) in &db_states {
                        let Some(spool_state) = native_spools.get(db_path) else {
                            tracing::error!("Failed to prune {}: native spool missing", state.name);
                            continue;
                        };
                        if let Err(e) = prune_watcher_database(&client, &bucket_name, &prefix, state, spool_state, policy).await {
                            tracing::error!("Failed to prune {}: {}", state.name, e);
                        }
                    }
                }
            }

            // Checkpoint timer - copy, sync, verify durability, then checkpoint.
            _ = checkpoint_timer.tick(), if global_sync.checkpoint_interval > 0 => {
                for (db_path, state) in db_states.iter_mut() {
                    let sync_config = sync_configs.get(&state.db_path).unwrap_or(&global_sync);
                    let Some(spool_state) = native_spools.get(db_path) else {
                        return Err(anyhow!("{}: native spool missing", state.name));
                    };
                    let estimated_frames =
                        spool_lock(&spool_state.0)?.admitted_frames_since_checkpoint();

                    if estimated_frames >= sync_config.min_checkpoint_page_count {
                        tracing::info!(
                            "{}: Running shadow checkpoint (~{} frames)",
                            state.name,
                            estimated_frames
                        );

                        let checkpoint_started = std::time::Instant::now();
                        let checkpoint_result = checkpoint_shadow_after_native_admission(
                            state,
                            spool_state,
                            sync_config.checkpoint_release,
                            CHECKPOINT_UPLOAD_DRAIN_TIMEOUT,
                            ShadowCheckpointMode::Passive,
                        ).await;
                        let checkpoint_window_dirty = match checkpoint_result {
                            Ok(attempt) if attempt.completed => {
                                metrics_state.record_checkpoint(
                                    &state.name,
                                    "passive",
                                    checkpoint_started.elapsed().as_secs_f64(),
                                );
                                attempt.dirty
                            },
                            Ok(_) => {
                                tracing::warn!(
                                    database = %state.name,
                                    "PASSIVE checkpoint was partial/busy; blocker is rearmed and the checkpoint will retry later"
                                );
                                continue;
                            },
                            Err(e) => {
                            tracing::error!("{}: Shadow checkpoint failed: {}", state.name, e);
                            webhook_sender
                                .notify_upload_failed(&state.name, &e.to_string(), 1)
                                .await;
                            continue;
                            }
                        };
                        if checkpoint_window_dirty {
                            let reanchor = stage_native_snapshot(state, spool_state).await;
                            if let Err(error) = reanchor.as_ref() {
                                tracing::error!(
                                    database = %state.name,
                                    error = %error,
                                    "dirty checkpoint window re-anchor is pending; pausing deltas"
                                );
                                required_native_reanchors.insert(db_path.clone());
                            } else if let Some(seq) = reanchor.ok() {
                                spool_lock(&spool_state.0)?
                                    .complete_checkpoint_reanchor(seq)?;
                            }
                        }

                        tracing::debug!("{}: Shadow checkpoint completed", state.name);
                    } else {
                        tracing::debug!(
                            "{}: Skipping checkpoint (only ~{} frames, need {})",
                            state.name,
                            estimated_frames,
                            sync_config.min_checkpoint_page_count
                        );
                    }
                }
            }

            // Cache cleanup timer
            _ = cache_cleanup_timer.tick() => {
                if let Some(ref retention) = cache_retention {
                    for (db_path, (cache, _)) in cache_states.iter() {
                        let name = db_states.get(db_path).map(|s| s.name.as_str()).unwrap_or("unknown");
                        match cache.cleanup(*retention, cache_config.max_size) {
                            Ok(stats) => {
                                if stats.deleted_count > 0 {
                                    tracing::info!(
                                        "{}: Cache cleanup: deleted {} files ({:.2} MB), {:.2} MB remaining",
                                        name,
                                        stats.deleted_count,
                                        stats.deleted_bytes as f64 / (1024.0 * 1024.0),
                                        stats.remaining_bytes as f64 / (1024.0 * 1024.0)
                                    );
                                }
                            }
                            Err(e) => {
                                tracing::error!("{}: Cache cleanup failed: {}", name, e);
                            }
                        }
                    }
                }
                for (db_path, (spool, _)) in &native_spools {
                    let name = db_states
                        .get(db_path)
                        .map(|state| state.name.as_str())
                        .unwrap_or("unknown");
                    match spool_lock(spool)?.cleanup_published_before_latest_snapshot() {
                        Ok(deleted) if deleted > 0 => tracing::info!(
                            database = name,
                            deleted,
                            "cleaned remotely published native spool history before newest local snapshot base"
                        ),
                        Ok(_) => {}
                        Err(error) => tracing::error!(
                            database = name,
                            error = %error,
                            "native spool cleanup failed; retaining recovery data"
                        ),
                    }
                }
            }

            // Validation timer
            _ = validation_timer.tick(), if global_sync.validation_interval > 0 => {
                for (_db_path, state) in db_states.iter() {
                    let db_name = &state.name;

                    tracing::debug!("{}: Running periodic backup validation", db_name);

                    match validate_backup_integrity(&client, &bucket_name, &prefix, db_name).await {
                        Ok(result) => {
                            if result.is_valid {
                                tracing::info!(
                                    "{}: Validation passed ({} files, {:.2} MB)",
                                    db_name,
                                    result.verified_count,
                                    result.verified_size_bytes as f64 / (1024.0 * 1024.0)
                                );
                                metrics_state.record_validation_success(db_name);
                            } else {
                                tracing::error!(
                                    "{}: Validation failed with {} issues",
                                    db_name,
                                    result.issues.len()
                                );
                                for issue in &result.issues {
                                    tracing::error!("  {}: {}", issue.filename, issue.issue);
                                }
                                metrics_state.record_validation_failure(db_name);
                            }
                        }
                        Err(e) => {
                            tracing::error!("{}: Validation error: {}", db_name, e);
                            metrics_state.record_validation_failure(db_name);
                        }
                    }
                }
            }
        }
    }

    // Graceful shutdown - sync remaining shadow data
    tracing::info!("Shadow mode shutdown: syncing remaining data...");

    finish_shutdown_local_admission(&mut db_states, &native_spools).await;
    durability_failpoint("shutdown_local_admission_complete");

    shutdown_shadow_uploaders(&cache_states, &db_states, uploader_handles).await?;
    let cloud_drain_deadline =
        tokio::time::Instant::now() + Duration::from_secs(spool_config.shutdown_drain_seconds);
    loop {
        let pending = native_spools
            .values()
            .try_fold(0usize, |count, (spool, wake)| {
                wake.notify();
                Ok::<_, anyhow::Error>(count + spool_lock(spool)?.pending_objects().count())
            })?;
        if pending == 0 || tokio::time::Instant::now() >= cloud_drain_deadline {
            if pending > 0 {
                tracing::warn!(
                    pending,
                    "bounded shutdown cloud drain expired; native spool remains durable"
                );
            }
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    for sender in native_uploader_shutdown.values() {
        let _ = sender.send(true);
    }
    for (db_path, mut handle) in native_uploader_handles {
        let name = db_states
            .get(&db_path)
            .map(|state| state.name.as_str())
            .unwrap_or("unknown");
        if tokio::time::timeout(Duration::from_secs(2), &mut handle)
            .await
            .is_err()
        {
            tracing::warn!(
                "{}: native uploader cloud drain timed out; pending spool remains durable",
                name
            );
            handle.abort();
        }
    }

    // On non-OFD systems, close ordering is part of blocker correctness: the
    // long-lived plain source descriptor must outlive every SQLite connection
    // that owns or observes the source database locks.
    for state in db_states.values_mut() {
        if let Some(blocker) = state.checkpoint_blocker.take() {
            if !blocker.is_autocommit() {
                if let Err(error) = blocker.execute_batch("ROLLBACK;") {
                    tracing::error!(
                        database = %state.name,
                        error = %error,
                        "failed to release checkpoint blocker during shutdown"
                    );
                }
            }
            drop(blocker);
        }
        drop(state.data_version_monitor.take());
        drop(state.source_db_file.take());
    }

    tracing::info!("walrust shadow mode shutdown complete");
    Ok(())
}

/// How startup state is seeded from the remote `manifest.json`.
#[derive(Debug, PartialEq, Eq)]
#[cfg(test)]
enum ManifestSeed {
    /// No remote manifest exists: a brand-new database starts fresh at txid 0.
    Fresh,
    /// Seeded from an existing remote manifest.
    Seeded { txid: u64, checksum: Option<u64> },
}

/// Decide how to seed startup state from a manifest FETCH result, enforcing the
/// H8-cousin policy: a transient (or otherwise non-not-found) fetch failure, and
/// a present-but-unparseable manifest, must NOT silently start fresh.
///
/// - `Ok(bytes)`: parse the manifest. A parse failure PROPAGATES (a corrupt or
///   truncated manifest over existing remote state must be loud, never fresh).
/// - `Err(e)` that `is_not_found` confirms: a genuine missing manifest → `Fresh`.
/// - any other `Err(e)`: PROPAGATE. A transient/ambiguous failure never defaults
///   to a fresh txid-0 database.
///
/// Pure and independent of S3 so both directions are unit-tested without a live
/// backend.
#[cfg(test)]
fn seed_state_from_manifest_fetch(
    fetch: Result<Vec<u8>>,
    is_not_found: impl Fn(&anyhow::Error) -> bool,
) -> Result<ManifestSeed> {
    match fetch {
        Ok(data) => {
            let manifest: Manifest = serde_json::from_slice(&data).context(
                "manifest.json present but unparseable; refusing to start fresh over existing \
                 remote state",
            )?;
            Ok(ManifestSeed::Seeded {
                txid: manifest.current_txid,
                checksum: manifest.last_checksum,
            })
        }
        Err(e) if is_not_found(&e) => Ok(ManifestSeed::Fresh),
        Err(e) => Err(e.context(
            "manifest fetch failed and is not a confirmed not-found; refusing to default to a \
             fresh txid-0 database (a transient must be retried, not adopted as fresh)",
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::WebhookConfig;
    use crate::shadow::format_segment_name;
    use async_trait::async_trait;
    use hadb_storage::CasResult;
    use rusqlite::Connection;
    use std::io::{Read, Write};
    use tempfile::TempDir;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    struct NoRemoteIo;

    #[async_trait]
    impl StorageBackend for NoRemoteIo {
        async fn get(&self, _key: &str) -> Result<Option<Vec<u8>>> {
            panic!("local checkpoint path performed remote GET")
        }
        async fn put(&self, _key: &str, _data: &[u8]) -> Result<()> {
            panic!("local checkpoint path performed remote PUT")
        }
        async fn delete(&self, _key: &str) -> Result<()> {
            panic!("local checkpoint path performed remote DELETE")
        }
        async fn list(&self, _prefix: &str, _after: Option<&str>) -> Result<Vec<String>> {
            panic!("local checkpoint path performed remote LIST")
        }
        async fn put_if_absent(&self, _key: &str, _data: &[u8]) -> Result<CasResult> {
            panic!("local checkpoint path performed remote CAS")
        }
        async fn put_if_match(&self, _key: &str, _data: &[u8], _etag: &str) -> Result<CasResult> {
            panic!("local checkpoint path performed remote CAS")
        }
    }

    struct FailingMigrationStorage;

    #[async_trait]
    impl StorageBackend for FailingMigrationStorage {
        async fn get(&self, _key: &str) -> Result<Option<Vec<u8>>> {
            panic!("legacy migration verification should fail during discovery")
        }
        async fn put(&self, _key: &str, _data: &[u8]) -> Result<()> {
            panic!("legacy migration verification performed PUT")
        }
        async fn delete(&self, _key: &str) -> Result<()> {
            panic!("legacy migration verification performed DELETE")
        }
        async fn list(&self, _prefix: &str, _after: Option<&str>) -> Result<Vec<String>> {
            Err(anyhow!("injected migration discovery failure"))
        }
        async fn put_if_absent(&self, _key: &str, _data: &[u8]) -> Result<CasResult> {
            panic!("legacy migration verification performed CAS")
        }
        async fn put_if_match(&self, _key: &str, _data: &[u8], _etag: &str) -> Result<CasResult> {
            panic!("legacy migration verification performed CAS")
        }
    }

    #[tokio::test]
    async fn failed_legacy_migration_verification_removes_scratch() {
        let temp = TempDir::new().unwrap();
        let scratch = temp.path().join(".legacy-migration-verify-test.db");
        std::fs::write(&scratch, b"partial restore").unwrap();

        let error =
            verify_legacy_migration_head(&FailingMigrationStorage, "prefix/", "db", &scratch, 7)
                .await
                .unwrap_err();

        assert!(
            format!("{error:#}").contains("injected migration discovery failure"),
            "unexpected verification error: {error:#}"
        );
        assert!(
            !scratch.exists(),
            "failed migration verification left a live scratch database"
        );
    }

    fn test_native_spool_state(
        db_path: &std::path::Path,
        root: &std::path::Path,
    ) -> NativeSpoolState {
        let identity = SpoolIdentity::new(
            db_path,
            "bucket",
            "tests/",
            "db",
            "test-lineage",
            1,
            None,
            true,
        )
        .unwrap();
        let root = NativeSpool::path_for(root, &identity);
        let spool = Arc::new(Mutex::new(
            NativeSpool::create_or_open(
                &root,
                identity,
                CapacityPolicy {
                    warning_bytes: u64::MAX - 1,
                    hard_bytes: u64::MAX,
                    minimum_free_bytes: 0,
                },
            )
            .unwrap(),
        ));
        let (uploader, wake, _lag) =
            NativeUploader::new(Arc::new(NoRemoteIo), Arc::clone(&spool)).unwrap();
        // Keep the receiver alive but deliberately paused: no remote operation
        // can acknowledge the staged object.
        drop(uploader);
        (spool, wake)
    }

    fn test_source_db_file(path: &std::path::Path) -> Option<Arc<Mutex<std::fs::File>>> {
        Some(Arc::new(Mutex::new(std::fs::File::open(path).unwrap())))
    }

    fn test_migrated_native_spool_state(
        db_path: &std::path::Path,
        root: &std::path::Path,
        bucket: &str,
        prefix: &str,
        database: &str,
        legacy_boundary_txid: u64,
    ) -> NativeSpoolState {
        let identity = SpoolIdentity::new(
            db_path,
            bucket,
            prefix,
            database,
            "pending-migration-lineage",
            legacy_boundary_txid + 1,
            Some(legacy_boundary_txid),
            true,
        )
        .unwrap();
        let root = NativeSpool::path_for(root, &identity);
        let spool = Arc::new(Mutex::new(
            NativeSpool::create_or_open(
                &root,
                identity,
                CapacityPolicy {
                    warning_bytes: u64::MAX - 1,
                    hard_bytes: u64::MAX,
                    minimum_free_bytes: 0,
                },
            )
            .unwrap(),
        ));
        let (uploader, wake, _lag) =
            NativeUploader::new(Arc::new(NoRemoteIo), Arc::clone(&spool)).unwrap();
        drop(uploader);
        (spool, wake)
    }

    #[tokio::test]
    async fn watcher_retention_preserves_legacy_base_before_descriptor_publication() {
        if std::env::var("AWS_ENDPOINT_URL_S3").is_err()
            && std::env::var("AWS_ENDPOINT_URL").is_err()
            && std::env::var("AWS_ACCESS_KEY_ID").is_err()
        {
            eprintln!("SKIP watcher_retention_preserves_legacy_base_before_descriptor_publication: no S3 endpoint/credentials configured");
            return;
        }
        let bucket_arg = std::env::var("WALRUST_TEST_BUCKET")
            .unwrap_or_else(|_| "walrust-test-rr-2026/verify-test".to_string());
        let endpoint = std::env::var("AWS_ENDPOINT_URL_S3")
            .or_else(|_| std::env::var("AWS_ENDPOINT_URL"))
            .ok();
        let (bucket, prefix) = s3::parse_bucket(&bucket_arg);
        let client = s3::create_client(endpoint.as_deref()).await.unwrap();
        let name = format!(
            "watch-pending-migration-{}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        );
        let keep_old = crate::sync::manifest::build_ltx_key(&prefix, &name, 1, 1, 1);
        let would_delete = crate::sync::manifest::build_ltx_key(&prefix, &name, 2, 1, 2);
        let keep_latest = crate::sync::manifest::build_ltx_key(&prefix, &name, 3, 1, 3);
        let keys = vec![keep_old.clone(), would_delete.clone(), keep_latest.clone()];
        for key in &keys {
            s3::upload_bytes(&client, &bucket, key, b"snapshot".to_vec())
                .await
                .unwrap();
        }

        let (_sqlite_temp, db_path, _writer) = create_real_wal_db();
        let shadow = ShadowWal::new_without_checkpoint_blocker(&db_path)
            .await
            .unwrap();
        let mut state = ShadowDbState {
            name: name.clone(),
            db_path: db_path.clone(),
            wal_path: db_path.with_extension("db-wal"),
            current_txid: 3,
            last_snapshot: None,
            db_checksum: None,
            shadow,
            checkpoint_blocker: Some(ShadowWal::open_checkpoint_blocker(&db_path).unwrap()),
            data_version_monitor: Some(Connection::open(&db_path).unwrap()),
            source_db_file: test_source_db_file(&db_path),
            shadow_sync_generation: 0,
            shadow_sync_offset: 0,
            wal_copy_offset: 0,
        };
        let spool_temp = TempDir::new().unwrap();
        let spool_state = test_migrated_native_spool_state(
            &db_path,
            spool_temp.path(),
            &bucket,
            &prefix,
            &name,
            3,
        );
        assert_eq!(
            stage_native_snapshot(&mut state, &spool_state)
                .await
                .unwrap(),
            4
        );
        assert!(
            spool_lock(&spool_state.0)
                .unwrap()
                .get(4)
                .is_some_and(|object| object.remote_upload_state == RemoteUploadState::Pending),
            "migration snapshot must be durably pending locally"
        );
        assert!(
            !watcher_retention_has_published_native_base(&spool_state).unwrap(),
            "pending first migration snapshot must keep watcher retention closed"
        );
        let descriptor_key = format!("{}{}/native/v1/stream.json", prefix, name);
        assert!(
            !s3::exists(&client, &bucket, &descriptor_key).await.unwrap(),
            "test requires the pre-descriptor publication window"
        );

        let policy = RetentionPolicy::new(0, 0, 0, 0);
        prune_watcher_database(&client, &bucket, &prefix, &state, &spool_state, &policy)
            .await
            .unwrap();
        for key in &keys {
            assert!(
                s3::exists(&client, &bucket, key).await.unwrap(),
                "watcher retention deleted legacy recovery object {key} while its native migration snapshot was only local"
            );
        }
        let _ = s3::delete_objects(&client, &bucket, &keys).await;
    }

    #[tokio::test]
    async fn watcher_retention_accepts_retained_published_snapshot_after_local_cleanup() {
        let (_sqlite_temp, db_path, _writer) = create_real_wal_db();
        let shadow = ShadowWal::new_without_checkpoint_blocker(&db_path)
            .await
            .unwrap();
        let mut state = ShadowDbState {
            name: "cleanup-migration".to_string(),
            db_path: db_path.clone(),
            wal_path: db_path.with_extension("db-wal"),
            current_txid: 3,
            last_snapshot: None,
            db_checksum: None,
            shadow,
            checkpoint_blocker: Some(ShadowWal::open_checkpoint_blocker(&db_path).unwrap()),
            data_version_monitor: Some(Connection::open(&db_path).unwrap()),
            source_db_file: test_source_db_file(&db_path),
            shadow_sync_generation: 0,
            shadow_sync_offset: 0,
            wal_copy_offset: 0,
        };
        let spool_temp = TempDir::new().unwrap();
        let spool_state = test_migrated_native_spool_state(
            &db_path,
            spool_temp.path(),
            "bucket",
            "tests/",
            "cleanup-migration",
            3,
        );

        assert_eq!(
            stage_native_snapshot(&mut state, &spool_state)
                .await
                .unwrap(),
            4
        );
        {
            let mut spool = spool_lock(&spool_state.0).unwrap();
            spool.mark_uploaded(4).unwrap();
            spool.mark_published(4, b"published-four").unwrap();
        }
        assert!(watcher_retention_has_published_native_base(&spool_state).unwrap());

        assert_eq!(
            stage_native_snapshot(&mut state, &spool_state)
                .await
                .unwrap(),
            5
        );
        {
            let mut spool = spool_lock(&spool_state.0).unwrap();
            spool.mark_uploaded(5).unwrap();
            spool.mark_published(5, b"published-five").unwrap();
            assert_eq!(spool.cleanup_published_before_latest_snapshot().unwrap(), 1);
            assert!(spool.get(4).is_none(), "first snapshot should be cleaned");
            assert!(spool.get(5).is_some(), "latest snapshot must remain");
        }
        assert!(
            watcher_retention_has_published_native_base(&spool_state).unwrap(),
            "a retained published snapshot and contiguous cursor keep watcher retention live after first-snapshot cleanup"
        );
    }

    // ── H8 cousin: manifest-fetch seeding never silently starts fresh ────────

    /// Build the anyhow error `s3::download_bytes` produces for a genuine
    /// missing object: the typed SDK `NoSuchKey` service error.
    fn typed_no_such_key() -> anyhow::Error {
        use aws_sdk_s3::error::SdkError;
        use aws_sdk_s3::operation::get_object::GetObjectError;
        use aws_smithy_runtime_api::client::orchestrator::HttpResponse;
        use aws_smithy_runtime_api::http::StatusCode;
        use aws_smithy_types::body::SdkBody;
        let err = GetObjectError::NoSuchKey(aws_sdk_s3::types::error::NoSuchKey::builder().build());
        let raw = HttpResponse::new(StatusCode::try_from(404u16).unwrap(), SdkBody::empty());
        anyhow::Error::new(SdkError::service_error(err, raw))
    }

    /// A genuine (typed) not-found seeds a fresh txid-0 database (correct:
    /// brand-new DB).
    #[test]
    fn manifest_not_found_starts_fresh() {
        let seed = seed_state_from_manifest_fetch(
            Err(typed_no_such_key()),
            s3::download_error_is_not_found,
        )
        .expect("a confirmed not-found must seed fresh, not error");
        assert_eq!(seed, ManifestSeed::Fresh);
    }

    /// A TRANSIENT fetch failure must NOT silently start fresh — it propagates.
    /// This is the swallow-shape the harden closes: reverting it (defaulting to
    /// `(0, None)` on any error) would return `Fresh` here.
    #[test]
    fn manifest_transient_fetch_does_not_start_fresh() {
        let err = seed_state_from_manifest_fetch(
            Err(anyhow!("Service unavailable (injected); dispatch failure")),
            s3::download_error_is_not_found,
        )
        .expect_err("a transient fetch failure must propagate, never seed fresh");
        assert!(
            format!("{err:#}").contains("refusing to default to a fresh"),
            "got: {err:#}"
        );
    }

    /// A present-but-unparseable manifest must be loud, never a silent fresh
    /// start over existing remote state.
    #[test]
    fn manifest_corrupt_parse_does_not_start_fresh() {
        let err = seed_state_from_manifest_fetch(
            Ok(b"{ this is not valid json".to_vec()),
            s3::download_error_is_not_found,
        )
        .expect_err("a corrupt manifest must propagate, never seed fresh");
        assert!(format!("{err:#}").contains("unparseable"), "got: {err:#}");
    }

    /// A valid remote manifest seeds the recorded txid + checksum.
    #[test]
    fn manifest_present_seeds_recorded_state() {
        let manifest = Manifest {
            name: "db".into(),
            current_txid: 42,
            page_size: 4096,
            files: Vec::new(),
            last_checksum: Some(0xDEAD_BEEF),
        };
        let bytes = serde_json::to_vec(&manifest).unwrap();
        let seed =
            seed_state_from_manifest_fetch(Ok(bytes), s3::download_error_is_not_found).unwrap();
        assert_eq!(
            seed,
            ManifestSeed::Seeded {
                txid: 42,
                checksum: Some(0xDEAD_BEEF)
            }
        );
    }

    /// The not-found classifier is TYPED, not string-matched: only the SDK's
    /// `NoSuchKey`/404 service error classifies as not-found. Free text that
    /// merely *mentions* not-found signatures (a DNS "host not found", a proxy
    /// body with "404", the SDK error message quoted in a wrapper) must NOT —
    /// otherwise a transient would misread as a missing manifest and silently
    /// start fresh, the exact bug this hardening closes.
    #[test]
    fn not_found_classifier_is_typed_not_string_matched() {
        use aws_sdk_s3::error::SdkError;
        use aws_sdk_s3::operation::get_object::GetObjectError;

        // The real typed shape → not-found (also when wrapped with context).
        assert!(s3::download_error_is_not_found(&typed_no_such_key()));
        assert!(s3::download_error_is_not_found(
            &typed_no_such_key().context("fetching manifest.json")
        ));

        // A typed TIMEOUT is not not-found even though it is an SDK error.
        let timeout: SdkError<GetObjectError> = SdkError::timeout_error("timed out");
        assert!(!s3::download_error_is_not_found(&anyhow::Error::new(
            timeout
        )));

        // Free-text errors carrying not-found-looking words are NOT not-found.
        for msg in [
            "NoSuchKey: The specified key does not exist",
            "Object not found: prefix/db/manifest.json",
            "service error, HTTP status: 404",
            "dns error: host not found",
            "Service unavailable (injected)",
            "connection reset by peer",
        ] {
            assert!(
                !s3::download_error_is_not_found(&anyhow!("{msg}")),
                "free-text {msg:?} must not classify as not-found"
            );
        }
    }

    fn create_real_wal_db() -> (TempDir, PathBuf, Connection) {
        let temp = TempDir::new().unwrap();
        let db_path = temp.path().join("shutdown-shadow.db");
        let conn = Connection::open(&db_path).unwrap();
        conn.execute_batch(
            "
            PRAGMA journal_mode=WAL;
            PRAGMA wal_autocheckpoint=0;
            CREATE TABLE items (id INTEGER PRIMARY KEY, value TEXT NOT NULL);
            INSERT INTO items (value) VALUES ('alpha'), ('beta'), ('gamma');
            ",
        )
        .unwrap();

        assert!(
            db_path.with_extension("db-wal").exists(),
            "test must exercise a live SQLite WAL"
        );

        (temp, db_path, conn)
    }

    async fn capture_one_webhook() -> (String, tokio::task::JoinHandle<String>) {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        let handle = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut buffer = Vec::new();
            let mut chunk = [0u8; 1024];

            loop {
                let count = stream.read(&mut chunk).await.unwrap();
                assert!(count > 0, "webhook closed before sending its body");
                buffer.extend_from_slice(&chunk[..count]);
                let Some(header_end) = buffer.windows(4).position(|window| window == b"\r\n\r\n")
                else {
                    continue;
                };
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
                if buffer.len() < body_start + content_length {
                    continue;
                }

                let body =
                    String::from_utf8(buffer[body_start..body_start + content_length].to_vec())
                        .unwrap();
                stream
                    .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 0\r\n\r\n")
                    .await
                    .unwrap();
                return body;
            }
        });

        (url, handle)
    }

    fn write_shadow_segment(
        shadow_dir: &std::path::Path,
        generation: u64,
        page_size: usize,
        page_data: &[u8],
    ) {
        std::fs::create_dir_all(shadow_dir).unwrap();
        let path = shadow_dir.join(format_segment_name(generation, 0));
        let mut file = std::fs::File::create(path).unwrap();
        let mut header = [0u8; 24];
        header[0..4].copy_from_slice(&1u32.to_be_bytes());
        header[4..8].copy_from_slice(&1u32.to_be_bytes());
        file.write_all(&header).unwrap();
        file.write_all(&page_data[..page_size]).unwrap();
    }

    #[tokio::test]
    async fn markerless_shadow_recovery_rewinds_live_wal_cursor_before_reanchor() {
        let (_temp, db_path, _conn) = create_real_wal_db();
        let shadow_dir = ShadowWal::shadow_dir_for(&db_path);
        let mut page = vec![0u8; 4096];
        std::fs::File::open(&db_path)
            .unwrap()
            .read_exact(&mut page)
            .unwrap();
        write_shadow_segment(&shadow_dir, 7, 4096, &page);

        let mut shadow = ShadowWal::new_without_checkpoint_blocker(&db_path)
            .await
            .unwrap();
        assert!(shadow.discarded_unproven_tail());
        let stale_progress = ShadowProgress {
            version: 2,
            current_txid: 9,
            last_snapshot: None,
            db_checksum: Some(11),
            shadow_sync_generation: 7,
            shadow_sync_offset: (24 + 4096) as u64,
            wal_copy_offset: u64::MAX / 2,
            wal_salt: Some((1, 2)),
            wal_checksum_chain: Some((3, 4)),
        };
        assert_eq!(
            restore_wal_copy_progress(&mut shadow, &stale_progress),
            0,
            "discarded shadow bytes must force checked WAL recopy from zero"
        );
        let (frames, offset) = shadow.copy_frames(0).await.unwrap();
        assert!(
            !frames.is_empty(),
            "pinned live WAL frames must be recopied"
        );
        assert!(offset > 0);
    }

    #[tokio::test]
    async fn test_shadow_shutdown_syncs_final_real_wal_frames_to_cache() {
        let (_temp, db_path, _conn) = create_real_wal_db();
        let shadow = ShadowWal::new_without_checkpoint_blocker(&db_path)
            .await
            .unwrap();
        let wal_path = db_path.with_extension("db-wal");

        let mut db_states = HashMap::new();
        db_states.insert(
            db_path.clone(),
            ShadowDbState {
                name: "shutdown_shadow".to_string(),
                db_path: db_path.clone(),
                wal_path,
                current_txid: 0,
                last_snapshot: None,
                db_checksum: None,
                shadow,
                checkpoint_blocker: None,
                data_version_monitor: None,
                source_db_file: None,
                shadow_sync_generation: 0,
                shadow_sync_offset: 0,
                wal_copy_offset: 0,
            },
        );

        copy_final_shadow_frames(&mut db_states).await.unwrap();
        assert!(
            db_states.get(&db_path).unwrap().wal_copy_offset > 0,
            "final shutdown copy must consume real WAL frames"
        );

        let cache = Arc::new(LocalCache::new(&db_path).unwrap());
        let (upload_tx, mut upload_rx) = mpsc::channel(10);
        let mut cache_states = HashMap::new();
        cache_states.insert(db_path.clone(), (Arc::clone(&cache), upload_tx));

        let results = run_shadow_syncs(
            &db_states,
            &cache_states,
            None,
            &RetryPolicy::new(RetryConfig::default()),
            Arc::new(WebhookSender::new(vec![])),
        )
        .await;
        apply_shadow_sync_results_strict(&mut db_states, results)
            .await
            .unwrap();

        let pending = cache.pending_uploads();
        assert_eq!(
            pending.len(),
            1,
            "final shutdown sync must queue an LTX upload"
        );

        let ltx = cache.read_ltx(pending[0]).unwrap();
        crate::ltx::verify_ltx(std::io::Cursor::new(ltx)).unwrap();

        let msg = upload_rx.try_recv().unwrap();
        assert!(
            matches!(msg, UploadMessage::Upload(txid) if txid == pending[0]),
            "cache sync must notify uploader for queued LTX"
        );
    }

    #[tokio::test]
    async fn test_shadow_sync_persists_restart_progress_after_durable_cache_write() {
        let (_temp, db_path, _conn) = create_real_wal_db();
        let shadow = ShadowWal::new_without_checkpoint_blocker(&db_path)
            .await
            .unwrap();
        let progress_path = shadow.shadow_dir().join("progress.json");
        let wal_path = db_path.with_extension("db-wal");

        let mut db_states = HashMap::new();
        db_states.insert(
            db_path.clone(),
            ShadowDbState {
                name: "restart_progress".to_string(),
                db_path: db_path.clone(),
                wal_path,
                current_txid: 0,
                last_snapshot: None,
                db_checksum: None,
                shadow,
                checkpoint_blocker: None,
                data_version_monitor: None,
                source_db_file: None,
                shadow_sync_generation: 0,
                shadow_sync_offset: 0,
                wal_copy_offset: 0,
            },
        );

        copy_final_shadow_frames(&mut db_states).await.unwrap();

        let cache = Arc::new(LocalCache::new(&db_path).unwrap());
        let (upload_tx, _upload_rx) = mpsc::channel(10);
        let mut cache_states = HashMap::new();
        cache_states.insert(db_path.clone(), (Arc::clone(&cache), upload_tx));

        let results = run_shadow_syncs(
            &db_states,
            &cache_states,
            None,
            &RetryPolicy::new(RetryConfig::default()),
            Arc::new(WebhookSender::new(vec![])),
        )
        .await;
        apply_shadow_sync_results_strict(&mut db_states, results)
            .await
            .unwrap();

        assert!(
            progress_path.exists(),
            "shadow sync must persist a restart progress record after durable cache write"
        );
        let state = db_states.get(&db_path).unwrap();
        let reloaded = load_shadow_progress(&state.shadow, &state.name)
            .unwrap()
            .expect("progress record must reload");
        assert_eq!(reloaded.current_txid, state.current_txid);
        assert_eq!(
            reloaded.shadow_sync_generation,
            state.shadow_sync_generation
        );
        assert_eq!(reloaded.shadow_sync_offset, state.shadow_sync_offset);
    }

    #[tokio::test]
    async fn test_shadow_sync_cursor_resets_offset_when_advancing_generation() {
        let (_temp, db_path, _conn) = create_real_wal_db();
        let shadow_dir = ShadowWal::shadow_dir_for(&db_path);
        let page_size = 4096usize;
        let mut page = vec![0u8; page_size];
        std::fs::File::open(&db_path)
            .unwrap()
            .read_exact(&mut page)
            .unwrap();

        write_shadow_segment(&shadow_dir, 0, page_size, &page);
        write_shadow_segment(&shadow_dir, 1, page_size, &page);
        let frame_len = (24 + page_size) as u64;
        std::fs::write(
            shadow_dir.join("durable-tail-v1.json"),
            serde_json::to_vec_pretty(&serde_json::json!({
                "version": 1,
                "segments": {
                    format_segment_name(0, 0): frame_len,
                    format_segment_name(1, 0): frame_len,
                }
            }))
            .unwrap(),
        )
        .unwrap();

        let shadow = ShadowWal::new_without_checkpoint_blocker(&db_path)
            .await
            .unwrap();
        assert_eq!(
            shadow.generation(),
            1,
            "test setup must start with a newer live shadow generation"
        );
        let frame_size = 24 + page_size as u64;
        let wal_path = db_path.with_extension("db-wal");
        let mut db_states = HashMap::new();
        db_states.insert(
            db_path.clone(),
            ShadowDbState {
                name: "generation_cursor".to_string(),
                db_path: db_path.clone(),
                wal_path,
                current_txid: 0,
                last_snapshot: None,
                db_checksum: None,
                shadow,
                checkpoint_blocker: None,
                data_version_monitor: None,
                source_db_file: None,
                shadow_sync_generation: 0,
                shadow_sync_offset: frame_size,
                wal_copy_offset: 0,
            },
        );

        let cache = Arc::new(LocalCache::new(&db_path).unwrap());
        let (upload_tx, _upload_rx) = mpsc::channel(10);
        let mut cache_states = HashMap::new();
        cache_states.insert(db_path.clone(), (Arc::clone(&cache), upload_tx));

        let drained_old_generation = run_shadow_syncs(
            &db_states,
            &cache_states,
            None,
            &RetryPolicy::new(RetryConfig::default()),
            Arc::new(WebhookSender::new(vec![])),
        )
        .await;
        apply_shadow_sync_results_strict(&mut db_states, drained_old_generation)
            .await
            .unwrap();
        let state = db_states.get(&db_path).unwrap();
        assert_eq!(state.shadow_sync_generation, 1);
        assert_eq!(
            state.shadow_sync_offset, 0,
            "offset must reset when advancing to the new shadow generation"
        );
        assert!(
            cache.pending_uploads().is_empty(),
            "draining the old generation at EOF should not upload anything"
        );

        let synced_new_generation = run_shadow_syncs(
            &db_states,
            &cache_states,
            None,
            &RetryPolicy::new(RetryConfig::default()),
            Arc::new(WebhookSender::new(vec![])),
        )
        .await;
        apply_shadow_sync_results_strict(&mut db_states, synced_new_generation)
            .await
            .unwrap();
        assert_eq!(
            cache.pending_uploads().len(),
            1,
            "new generation frames must be read from offset 0, not skipped by the prior generation offset"
        );
    }

    #[tokio::test]
    async fn test_shadow_checkpoint_detects_app_commit_after_durable_copy() {
        let (_temp, db_path, _conn) = create_real_wal_db();
        let shadow = ShadowWal::new_without_checkpoint_blocker(&db_path)
            .await
            .unwrap();
        let wal_path = db_path.with_extension("db-wal");
        let mut state = ShadowDbState {
            name: "checkpoint_shadow".to_string(),
            db_path: db_path.clone(),
            wal_path,
            current_txid: 0,
            last_snapshot: None,
            db_checksum: None,
            shadow,
            checkpoint_blocker: Some(ShadowWal::open_checkpoint_blocker(&db_path).unwrap()),
            data_version_monitor: Some(Connection::open(&db_path).unwrap()),
            source_db_file: test_source_db_file(&db_path),
            shadow_sync_generation: 0,
            shadow_sync_offset: 0,
            wal_copy_offset: 0,
        };

        let cache = Arc::new(LocalCache::new(&db_path).unwrap());
        let (upload_tx, mut upload_rx) = mpsc::channel(10);
        let ack_cache = Arc::clone(&cache);
        let late_write_db = db_path.clone();
        let ack_handle = tokio::spawn(async move {
            match upload_rx.recv().await {
                Some(UploadMessage::Upload(txid)) => {
                    let app = Connection::open(late_write_db).unwrap();
                    app.execute(
                        "INSERT INTO items (value) VALUES ('commit-after-durable-copy')",
                        [],
                    )
                    .unwrap();
                    drop(app);
                    ack_cache.mark_uploaded(txid).unwrap();
                }
                other => panic!("expected upload notification, got {other:?}"),
            }
        });
        let cache_state = (Arc::clone(&cache), upload_tx);

        let checkpoint_window_dirty = checkpoint_shadow_after_durable_sync(
            &mut state,
            Some(&cache_state),
            None,
            &RetryPolicy::new(RetryConfig::default()),
            Arc::new(WebhookSender::new(vec![])),
            Duration::from_secs(2),
            ShadowCheckpointMode::Passive,
        )
        .await
        .unwrap();
        ack_handle.await.unwrap();
        assert!(
            checkpoint_window_dirty,
            "an app commit after the durable shadow copy must force a snapshot re-anchor"
        );

        assert!(
            state.wal_copy_offset > 0,
            "checkpoint path must copy real active WAL frames before checkpointing"
        );
        assert!(
            state.shadow_sync_offset > 0,
            "checkpoint path must sync copied shadow frames before checkpointing"
        );
        assert!(
            cache.pending_uploads().is_empty(),
            "checkpoint must wait until cache uploads are confirmed durable"
        );
        assert!(
            cache.last_uploaded_txid() >= state.current_txid,
            "confirmed uploaded LTX must cover the checkpointed shadow state"
        );
    }

    #[tokio::test]
    async fn default_local_release_checkpoints_after_native_admission_without_remote_ack() {
        let (temp, db_path, conn) = create_real_wal_db();
        let shadow = ShadowWal::new_without_checkpoint_blocker(&db_path)
            .await
            .unwrap();
        let mut state = ShadowDbState {
            name: "native-local-release".to_string(),
            db_path: db_path.clone(),
            wal_path: db_path.with_extension("db-wal"),
            current_txid: 0,
            last_snapshot: None,
            db_checksum: None,
            shadow,
            checkpoint_blocker: Some(ShadowWal::open_checkpoint_blocker(&db_path).unwrap()),
            data_version_monitor: Some(Connection::open(&db_path).unwrap()),
            source_db_file: test_source_db_file(&db_path),
            shadow_sync_generation: 0,
            shadow_sync_offset: 0,
            wal_copy_offset: 0,
        };
        rearm_checkpoint_blocker(&mut state).unwrap();
        let spool_state = test_native_spool_state(&db_path, temp.path());
        stage_native_snapshot(&mut state, &spool_state)
            .await
            .unwrap();
        assert!(
            !spool_lock(&spool_state.0)
                .unwrap()
                .snapshot_in_progress()
                .unwrap(),
            "snapshot call site must durably admit then retire its frozen-source intent"
        );

        conn.execute(
            "INSERT INTO items(value) VALUES ('local-no-remote-ack')",
            [],
        )
        .unwrap();
        let attempt = checkpoint_shadow_after_native_admission(
            &mut state,
            &spool_state,
            crate::config::CheckpointRelease::Local,
            Duration::from_millis(50),
            ShadowCheckpointMode::Truncate,
        )
        .await
        .unwrap();
        assert!(attempt.completed);
        assert!(!attempt.dirty);
        let spool = spool_lock(&spool_state.0).unwrap();
        assert_eq!(spool.admitted_seq(), Some(2));
        assert_eq!(spool.remote_published_seq(), None);
        let object = spool.get(2).unwrap();
        assert_eq!(
            object.remote_upload_state,
            walrust_core::native_spool::RemoteUploadState::Pending
        );
        assert!(spool.read_payload(2).unwrap().starts_with(b"HADBP"));
        let durable_root = spool.root().to_path_buf();
        let durable_identity = spool.identity().clone();
        drop(spool);
        drop(spool_state);
        let reopened = NativeSpool::create_or_open(
            &durable_root,
            durable_identity,
            CapacityPolicy {
                warning_bytes: u64::MAX - 1,
                hard_bytes: u64::MAX,
                minimum_free_bytes: 0,
            },
        )
        .expect("checkpoint admission must survive a process restart from its journal");
        assert_eq!(reopened.admitted_seq(), Some(2));
        assert!(reopened.read_payload(2).unwrap().starts_with(b"HADBP"));
        assert!(
            state
                .checkpoint_blocker
                .as_ref()
                .is_some_and(|blocker| !blocker.is_autocommit()),
            "blocker must be reacquired after local-only checkpoint"
        );
        assert!(
            live_wal_page_count(&state.wal_path).await.unwrap() <= 4,
            "controlled TRUNCATE plus blocker heartbeat should leave a bounded live WAL"
        );
    }

    #[tokio::test]
    async fn direct_snapshot_is_exact_while_passive_checkpoint_contends() {
        use std::sync::atomic::{AtomicBool, Ordering};

        let (temp, db_path, conn) = create_real_wal_db();
        for id in 10..1010i64 {
            conn.execute(
                "INSERT INTO items(id, value) VALUES (?1, ?2)",
                rusqlite::params![id, format!("passive-{id}")],
            )
            .unwrap();
        }
        let expected_count: i64 = conn
            .query_row("SELECT count(*) FROM items", [], |row| row.get(0))
            .unwrap();
        let shadow = ShadowWal::new_without_checkpoint_blocker(&db_path)
            .await
            .unwrap();
        let mut state = ShadowDbState {
            name: "native-passive-snapshot".to_string(),
            db_path: db_path.clone(),
            wal_path: db_path.with_extension("db-wal"),
            current_txid: 0,
            last_snapshot: None,
            db_checksum: None,
            shadow,
            checkpoint_blocker: Some(ShadowWal::open_checkpoint_blocker(&db_path).unwrap()),
            data_version_monitor: Some(Connection::open(&db_path).unwrap()),
            source_db_file: test_source_db_file(&db_path),
            shadow_sync_generation: 0,
            shadow_sync_offset: 0,
            wal_copy_offset: 0,
        };
        let spool_state = test_native_spool_state(&db_path, temp.path());
        let running = Arc::new(AtomicBool::new(true));
        let checkpoint_running = Arc::clone(&running);
        let checkpoint_db = db_path.clone();
        let checkpointer = std::thread::spawn(move || {
            let checkpoint = Connection::open(checkpoint_db).unwrap();
            while checkpoint_running.load(Ordering::Relaxed) {
                let _: (i64, i64, i64) = checkpoint
                    .query_row("PRAGMA wal_checkpoint(PASSIVE)", [], |row| {
                        Ok((row.get(0)?, row.get(1)?, row.get(2)?))
                    })
                    .unwrap();
                std::thread::yield_now();
            }
        });
        let stage_result = stage_native_snapshot(&mut state, &spool_state).await;
        running.store(false, Ordering::Relaxed);
        checkpointer.join().unwrap();
        stage_result.unwrap();
        let payload = spool_lock(&spool_state.0).unwrap().read_payload(1).unwrap();
        let restored_path = temp.path().join("passive-restored.db");
        walrust_core::ltx::decode_to_db(&payload, &restored_path).unwrap();
        let restored = Connection::open(restored_path).unwrap();
        let restored_count: i64 = restored
            .query_row("SELECT count(*) FROM items", [], |row| row.get(0))
            .unwrap();
        assert_eq!(restored_count, expected_count);
        assert_eq!(
            restored
                .query_row("PRAGMA integrity_check", [], |row| row.get::<_, String>(0))
                .unwrap(),
            "ok"
        );
    }

    #[tokio::test]
    async fn remote_release_waits_before_opening_checkpoint_window() {
        let (temp, db_path, conn) = create_real_wal_db();
        let shadow = ShadowWal::new_without_checkpoint_blocker(&db_path)
            .await
            .unwrap();
        let mut state = ShadowDbState {
            name: "native-remote-release".to_string(),
            db_path: db_path.clone(),
            wal_path: db_path.with_extension("db-wal"),
            current_txid: 0,
            last_snapshot: None,
            db_checksum: None,
            shadow,
            checkpoint_blocker: Some(ShadowWal::open_checkpoint_blocker(&db_path).unwrap()),
            data_version_monitor: Some(Connection::open(&db_path).unwrap()),
            source_db_file: test_source_db_file(&db_path),
            shadow_sync_generation: 0,
            shadow_sync_offset: 0,
            wal_copy_offset: 0,
        };
        let spool_state = test_native_spool_state(&db_path, temp.path());
        stage_native_snapshot(&mut state, &spool_state)
            .await
            .unwrap();
        conn.execute("INSERT INTO items(value) VALUES ('remote-policy')", [])
            .unwrap();
        let error = checkpoint_shadow_after_native_admission(
            &mut state,
            &spool_state,
            crate::config::CheckpointRelease::Remote,
            Duration::from_millis(50),
            ShadowCheckpointMode::Truncate,
        )
        .await
        .unwrap_err();
        assert!(error
            .to_string()
            .contains("timed out waiting for contiguous remote publish"));
        assert!(
            state
                .checkpoint_blocker
                .as_ref()
                .is_some_and(|blocker| !blocker.is_autocommit()),
            "remote policy timeout must not release the blocker"
        );
    }

    #[tokio::test]
    async fn partial_passive_checkpoint_rearms_blocker_and_retries_without_advancing() {
        let (temp, db_path, conn) = create_real_wal_db();
        let shadow = ShadowWal::new_without_checkpoint_blocker(&db_path)
            .await
            .unwrap();
        let mut state = ShadowDbState {
            name: "native-partial-passive".to_string(),
            db_path: db_path.clone(),
            wal_path: db_path.with_extension("db-wal"),
            current_txid: 0,
            last_snapshot: None,
            db_checksum: None,
            shadow,
            checkpoint_blocker: Some(ShadowWal::open_checkpoint_blocker(&db_path).unwrap()),
            data_version_monitor: Some(Connection::open(&db_path).unwrap()),
            source_db_file: test_source_db_file(&db_path),
            shadow_sync_generation: 0,
            shadow_sync_offset: 0,
            wal_copy_offset: 0,
        };
        let spool_state = test_native_spool_state(&db_path, temp.path());
        stage_native_snapshot(&mut state, &spool_state)
            .await
            .unwrap();

        let reader = Connection::open(&db_path).unwrap();
        reader
            .execute_batch("BEGIN; SELECT count(*) FROM items;")
            .unwrap();
        conn.execute("INSERT INTO items(value) VALUES ('reader-pinned')", [])
            .unwrap();
        let attempt = checkpoint_shadow_after_native_admission(
            &mut state,
            &spool_state,
            crate::config::CheckpointRelease::Local,
            Duration::from_millis(50),
            ShadowCheckpointMode::Passive,
        )
        .await
        .unwrap();
        assert!(!attempt.completed, "reader pin must make PASSIVE partial");
        assert!(
            state
                .checkpoint_blocker
                .as_ref()
                .is_some_and(|blocker| !blocker.is_autocommit()),
            "partial PASSIVE must immediately rearm the blocker"
        );
        assert_eq!(
            spool_lock(&spool_state.0).unwrap().admitted_seq(),
            Some(2),
            "local admission remains durable for the later checkpoint retry"
        );
        reader.execute_batch("ROLLBACK").unwrap();
    }

    #[tokio::test]
    async fn checkpoint_keeps_pre_blocker_monitor_open_through_rearm() {
        let (_temp, db_path, _conn) = create_real_wal_db();
        let shadow = ShadowWal::new_without_checkpoint_blocker(&db_path)
            .await
            .unwrap();
        let mut state = ShadowDbState {
            name: "native-monitor-stable".to_string(),
            db_path: db_path.clone(),
            wal_path: db_path.with_extension("db-wal"),
            current_txid: 0,
            last_snapshot: None,
            db_checksum: None,
            shadow,
            checkpoint_blocker: Some(ShadowWal::open_checkpoint_blocker(&db_path).unwrap()),
            data_version_monitor: Some(Connection::open(&db_path).unwrap()),
            source_db_file: test_source_db_file(&db_path),
            shadow_sync_generation: 0,
            shadow_sync_offset: 0,
            wal_copy_offset: 0,
        };
        let data_version_before = checkpoint_data_version(&state).unwrap();
        let monitor_handle = unsafe {
            state
                .data_version_monitor
                .as_ref()
                .expect("monitor exists")
                .handle()
        };

        let attempt = checkpoint_with_state_blocker_attempt(
            &mut state,
            ShadowCheckpointMode::Passive,
            data_version_before,
        )
        .await
        .unwrap();
        assert!(attempt.completed);
        assert!(
            !attempt.dirty,
            "a heartbeat written on the monitor must not change its own data_version"
        );
        assert_eq!(
            unsafe {
                state
                    .data_version_monitor
                    .as_ref()
                    .expect("monitor remains open")
                    .handle()
            },
            monitor_handle,
            "closing a same-inode SQLite monitor after blocker acquisition can release process-scoped POSIX locks"
        );
        assert!(
            state
                .checkpoint_blocker
                .as_ref()
                .is_some_and(|blocker| !blocker.is_autocommit()),
            "checkpoint returned to the watch loop without a live blocker"
        );
        assert!(
            checkpoint_blocker_heartbeat_is_live(&state).unwrap(),
            "replacement blocker heartbeat must remain pinned"
        );
    }

    #[tokio::test]
    async fn app_commit_after_final_sample_marks_checkpoint_window_dirty() {
        struct RemoveEnv(&'static str);
        impl Drop for RemoveEnv {
            fn drop(&mut self) {
                unsafe { std::env::remove_var(self.0) };
            }
        }

        let (temp, db_path, _conn) = create_real_wal_db();
        let shadow = ShadowWal::new_without_checkpoint_blocker(&db_path)
            .await
            .unwrap();
        let monitor = Connection::open(&db_path).unwrap();
        let blocker = ShadowWal::open_checkpoint_blocker(&db_path).unwrap();
        let mut state = ShadowDbState {
            name: "native-dirty-rearm-gap".to_string(),
            db_path: db_path.clone(),
            wal_path: db_path.with_extension("db-wal"),
            current_txid: 0,
            last_snapshot: None,
            db_checksum: None,
            shadow,
            checkpoint_blocker: Some(blocker),
            data_version_monitor: Some(monitor),
            source_db_file: test_source_db_file(&db_path),
            shadow_sync_generation: 0,
            shadow_sync_offset: 0,
            wal_copy_offset: 0,
        };
        let data_version_before = checkpoint_data_version(&state).unwrap();
        let pause = temp.path().join("checkpoint-rearm-gap.pause");
        const PAUSE_ENV: &str = "WALRUST_TEST_NATIVE_CHECKPOINT_PAUSE_FILE";
        const PAUSE_DB_ENV: &str = "WALRUST_TEST_NATIVE_CHECKPOINT_PAUSE_DB";
        unsafe { std::env::set_var(PAUSE_ENV, &pause) };
        unsafe { std::env::set_var(PAUSE_DB_ENV, &db_path) };
        let _remove_env = RemoveEnv(PAUSE_ENV);
        let _remove_db_env = RemoveEnv(PAUSE_DB_ENV);

        let writer_db = db_path.clone();
        let writer_pause = pause.clone();
        let writer = std::thread::spawn(move || {
            let result = (|| -> Result<()> {
                let deadline = std::time::Instant::now() + Duration::from_secs(5);
                while !writer_pause.exists() {
                    anyhow::ensure!(
                        std::time::Instant::now() < deadline,
                        "checkpoint never reached the post-sample rearm gap"
                    );
                    std::thread::sleep(Duration::from_millis(5));
                }
                let app = Connection::open(writer_db)?;
                app.execute(
                    "INSERT INTO items(value) VALUES ('commit-in-rearm-gap')",
                    [],
                )?;
                let busy: i64 =
                    app.query_row("PRAGMA wal_checkpoint(TRUNCATE)", [], |row| row.get(0))?;
                anyhow::ensure!(busy == 0, "test commit must cross the unblocked gap");
                Ok(())
            })();
            let _ = std::fs::remove_file(writer_pause);
            result.unwrap();
        });

        let attempt = checkpoint_with_state_blocker_attempt(
            &mut state,
            ShadowCheckpointMode::Passive,
            data_version_before,
        )
        .await
        .unwrap();
        writer.join().unwrap();
        assert!(attempt.completed);
        assert!(
            attempt.dirty,
            "an app commit checkpointed after the final pre-rearm sample requires a full native re-anchor"
        );
        assert!(checkpoint_blocker_heartbeat_is_live(&state).unwrap());
    }

    #[tokio::test]
    async fn spool_full_refuses_checkpoint_and_keeps_blocker() {
        let (temp, db_path, conn) = create_real_wal_db();
        let shadow = ShadowWal::new_without_checkpoint_blocker(&db_path)
            .await
            .unwrap();
        let mut state = ShadowDbState {
            name: "native-spool-full".to_string(),
            db_path: db_path.clone(),
            wal_path: db_path.with_extension("db-wal"),
            current_txid: 0,
            last_snapshot: None,
            db_checksum: None,
            shadow,
            checkpoint_blocker: Some(ShadowWal::open_checkpoint_blocker(&db_path).unwrap()),
            data_version_monitor: Some(Connection::open(&db_path).unwrap()),
            source_db_file: test_source_db_file(&db_path),
            shadow_sync_generation: 0,
            shadow_sync_offset: 0,
            wal_copy_offset: 0,
        };
        let initial = test_native_spool_state(&db_path, temp.path());
        stage_native_snapshot(&mut state, &initial).await.unwrap();
        let (root, identity, hard_bytes) = {
            let spool = spool_lock(&initial.0).unwrap();
            (
                spool.root().to_path_buf(),
                spool.identity().clone(),
                spool.used_bytes().unwrap().saturating_add(1),
            )
        };
        drop(initial);
        let spool = Arc::new(Mutex::new(
            NativeSpool::create_or_open(
                &root,
                identity,
                CapacityPolicy {
                    warning_bytes: 0,
                    hard_bytes,
                    minimum_free_bytes: 0,
                },
            )
            .unwrap(),
        ));
        let (uploader, wake, _lag) =
            NativeUploader::new(Arc::new(NoRemoteIo), Arc::clone(&spool)).unwrap();
        drop(uploader);
        let spool_state = (spool, wake);

        conn.execute(
            "INSERT INTO items(value) VALUES ('must-not-checkpoint')",
            [],
        )
        .unwrap();
        let error = checkpoint_shadow_after_native_admission(
            &mut state,
            &spool_state,
            crate::config::CheckpointRelease::Local,
            Duration::from_millis(50),
            ShadowCheckpointMode::Truncate,
        )
        .await
        .unwrap_err();
        assert!(format!("{error:#}").contains("local_spool_full"));
        assert!(
            state
                .checkpoint_blocker
                .as_ref()
                .is_some_and(|blocker| !blocker.is_autocommit()),
            "capacity exhaustion must retain the checkpoint blocker"
        );
        assert_eq!(spool_lock(&spool_state.0).unwrap().admitted_seq(), Some(1));
    }

    #[tokio::test]
    async fn test_wal_backpressure_alarms_drains_truncates_and_reacquires_blocker() {
        let (_temp, db_path, conn) = create_real_wal_db();
        let shadow = ShadowWal::new_without_checkpoint_blocker(&db_path)
            .await
            .unwrap();
        for id in 0..40i64 {
            conn.execute(
                "INSERT INTO items (value) VALUES (?1)",
                [format!("backpressure-{id}-{}", "x".repeat(500))],
            )
            .unwrap();
        }

        let wal_path = db_path.with_extension("db-wal");
        let mut state = ShadowDbState {
            name: "backpressure-shadow".to_string(),
            db_path: db_path.clone(),
            wal_path: wal_path.clone(),
            current_txid: 0,
            last_snapshot: None,
            db_checksum: None,
            shadow,
            checkpoint_blocker: Some(ShadowWal::open_checkpoint_blocker(&db_path).unwrap()),
            data_version_monitor: Some(Connection::open(&db_path).unwrap()),
            source_db_file: test_source_db_file(&db_path),
            shadow_sync_generation: 0,
            shadow_sync_offset: 0,
            wal_copy_offset: 0,
        };
        let before_pages = live_wal_page_count(&wal_path).await.unwrap();
        assert!(
            before_pages > 2,
            "test must create a meaningfully large WAL"
        );

        let cache = Arc::new(LocalCache::new(&db_path).unwrap());
        let (upload_tx, mut upload_rx) = mpsc::channel(10);
        let ack_cache = Arc::clone(&cache);
        let ack_handle = tokio::spawn(async move {
            match upload_rx.recv().await {
                Some(UploadMessage::Upload(txid)) => ack_cache.mark_uploaded(txid).unwrap(),
                other => panic!("expected upload notification, got {other:?}"),
            }
        });
        let cache_state = (Arc::clone(&cache), upload_tx);
        let (webhook_url, webhook_body) = capture_one_webhook().await;
        let webhook_sender = Arc::new(WebhookSender::new(vec![WebhookConfig {
            url: webhook_url,
            events: vec![WAL_SIZE_EXCEEDED_EVENT.to_string()],
            secret: None,
        }]));

        assert!(
            enforce_wal_backpressure(
                &mut state,
                before_pages,
                Some(&cache_state),
                None,
                &RetryPolicy::new(RetryConfig::default()),
                Arc::clone(&webhook_sender),
                Duration::from_secs(2),
            )
            .await
            .unwrap(),
            "crossing the threshold must run the backpressure path"
        );
        ack_handle.await.unwrap();

        let payload: serde_json::Value = serde_json::from_str(
            &tokio::time::timeout(Duration::from_secs(2), webhook_body)
                .await
                .unwrap()
                .unwrap(),
        )
        .unwrap();
        assert_eq!(payload["event"], WAL_SIZE_EXCEEDED_EVENT);
        assert_eq!(payload["database"], state.name);
        assert_eq!(payload["context"]["wal_pages"], before_pages);
        assert_eq!(payload["context"]["threshold_pages"], before_pages);

        let after_pages = live_wal_page_count(&wal_path).await.unwrap();
        assert!(
            after_pages < before_pages,
            "controlled TRUNCATE must shrink the WAL (before={before_pages}, after={after_pages})"
        );
        conn.execute_batch("PRAGMA busy_timeout=0;").unwrap();
        let busy: i64 = conn
            .query_row("PRAGMA wal_checkpoint(TRUNCATE);", [], |row| row.get(0))
            .unwrap();
        assert_eq!(
            busy, 1,
            "backpressure checkpoint must reacquire the blocker before returning"
        );
    }

    #[tokio::test]
    async fn test_shadow_checkpoint_refuses_pending_cache_upload() {
        let (_temp, db_path, _conn) = create_real_wal_db();
        let shadow = ShadowWal::new_without_checkpoint_blocker(&db_path)
            .await
            .unwrap();
        let wal_path = db_path.with_extension("db-wal");
        let mut state = ShadowDbState {
            name: "checkpoint_shadow_pending".to_string(),
            db_path: db_path.clone(),
            wal_path,
            current_txid: 0,
            last_snapshot: None,
            db_checksum: None,
            shadow,
            checkpoint_blocker: Some(ShadowWal::open_checkpoint_blocker(&db_path).unwrap()),
            data_version_monitor: Some(Connection::open(&db_path).unwrap()),
            source_db_file: test_source_db_file(&db_path),
            shadow_sync_generation: 0,
            shadow_sync_offset: 0,
            wal_copy_offset: 0,
        };

        let cache = Arc::new(LocalCache::new(&db_path).unwrap());
        let (upload_tx, _upload_rx) = mpsc::channel(10);
        let cache_state = (Arc::clone(&cache), upload_tx);

        let err = checkpoint_shadow_after_durable_sync(
            &mut state,
            Some(&cache_state),
            None,
            &RetryPolicy::new(RetryConfig::default()),
            Arc::new(WebhookSender::new(vec![])),
            Duration::from_millis(100),
            ShadowCheckpointMode::Passive,
        )
        .await
        .expect_err("checkpoint must fail closed while cache uploads are pending")
        .to_string();

        assert!(
            err.contains("durable upload confirmation timed out"),
            "expected durability timeout, got {err}"
        );
        assert!(
            !cache.pending_uploads().is_empty(),
            "test must leave an unconfirmed cache upload pending"
        );
    }

    #[tokio::test]
    async fn test_initial_shadow_copy_detects_downtime_checkpoint() {
        // D3: if the live WAL salt changed while walrust was down, the initial
        // copy must flag the database for an eager snapshot.
        let (_temp, db_path, conn) = create_real_wal_db();

        // First "process": copy WAL frames and remember the salt/offset.
        let mut shadow = ShadowWal::new(&db_path).await.unwrap();
        let (frames, offset) = shadow.copy_frames(0).await.unwrap();
        assert!(!frames.is_empty(), "pre-restart copy must read frames");
        let saved_salt = shadow.wal_read_salt();
        let saved_chain = shadow.wal_read_chain();
        drop(shadow);

        // External checkpoint while walrust is down resets the WAL and changes salt.
        let _: (i64, i64, i64) = conn
            .query_row("PRAGMA wal_checkpoint(TRUNCATE);", [], |row| {
                Ok((row.get(0)?, row.get(1)?, row.get(2)?))
            })
            .unwrap();
        conn.execute("INSERT INTO items (value) VALUES ('post-ckpt')", [])
            .unwrap();

        // Second "process": restart with the persisted cursor.
        let restarted = ShadowWal::new(&db_path).await.unwrap();
        let mut shadow = restarted;
        shadow.restore_read_cursor(saved_salt, saved_chain);

        let mut db_states = HashMap::new();
        db_states.insert(
            db_path.clone(),
            ShadowDbState {
                name: "downtime-ckpt".to_string(),
                db_path: db_path.clone(),
                wal_path: db_path.with_extension("db-wal"),
                current_txid: 0,
                last_snapshot: None,
                db_checksum: None,
                shadow,
                checkpoint_blocker: None,
                data_version_monitor: None,
                source_db_file: None,
                shadow_sync_generation: 0,
                shadow_sync_offset: 0,
                wal_copy_offset: offset,
            },
        );

        let eager = initial_shadow_copy(&mut db_states).await.unwrap();
        assert!(
            eager.contains(&db_path),
            "initial copy must flag a downtime-checkpointed DB for eager snapshot"
        );
        assert!(
            db_states[&db_path].shadow.generation() > 0,
            "shadow generation must advance after salt mismatch"
        );
    }

    #[tokio::test]
    async fn test_initial_shadow_copy_no_eager_snapshot_without_downtime_checkpoint() {
        // D3 (negative): if the live WAL salt did NOT change while walrust was
        // down (no external checkpoint), the initial copy must NOT flag the
        // database for an eager snapshot. Eager snapshots fire ONLY on mismatch.
        let (_temp, db_path, conn) = create_real_wal_db();

        // First "process": copy WAL frames and remember the salt/offset.
        let mut shadow = ShadowWal::new(&db_path).await.unwrap();
        let (frames, offset) = shadow.copy_frames(0).await.unwrap();
        assert!(!frames.is_empty(), "pre-restart copy must read frames");
        let saved_salt = shadow.wal_read_salt();
        let saved_chain = shadow.wal_read_chain();
        drop(shadow);

        // Normal writes while walrust is "down" — appended to the SAME WAL, no
        // checkpoint, so the salt is unchanged.
        conn.execute("INSERT INTO items (value) VALUES ('delta')", [])
            .unwrap();

        // Second "process": restart with the persisted cursor.
        let mut shadow = ShadowWal::new(&db_path).await.unwrap();
        shadow.restore_read_cursor(saved_salt, saved_chain);
        let generation_before = shadow.generation();

        let mut db_states = HashMap::new();
        db_states.insert(
            db_path.clone(),
            ShadowDbState {
                name: "no-downtime-ckpt".to_string(),
                db_path: db_path.clone(),
                wal_path: db_path.with_extension("db-wal"),
                current_txid: 0,
                last_snapshot: None,
                db_checksum: None,
                shadow,
                checkpoint_blocker: None,
                data_version_monitor: None,
                source_db_file: None,
                shadow_sync_generation: 0,
                shadow_sync_offset: 0,
                wal_copy_offset: offset,
            },
        );

        let eager = initial_shadow_copy(&mut db_states).await.unwrap();
        assert!(
            !eager.contains(&db_path),
            "no salt mismatch means no eager snapshot must be scheduled"
        );
        assert_eq!(
            db_states[&db_path].shadow.generation(),
            generation_before,
            "shadow generation must not advance without a downtime checkpoint"
        );
    }
}
