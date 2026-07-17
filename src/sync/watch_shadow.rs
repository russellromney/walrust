use crate::config::{ResolvedDbConfig, SpoolConfig, SyncConfig, WebhookConfig};
use crate::dashboard::{self, DbStatus, MetricsState};
use crate::errors::WalrustError;
use crate::ltx;
use crate::retention::RetentionPolicy;
use crate::retry::RetryConfig;
use crate::s3::{self, create_client, parse_bucket};
use crate::shadow::ShadowWal;
use crate::webhook::{WebhookPayload, WebhookSender};
use anyhow::{anyhow, bail, Context, Result};
use hadb_storage::StorageBackend;
use hadb_storage_s3::S3Storage;
use rusqlite::Connection;
use std::collections::{HashMap, HashSet};
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::signal;
use walrust_core::native_publish::{object_key as native_object_key, NativeUploader, UploadWake};
use walrust_core::native_shadow::{
    committed_shadow_prefix_offset, encode_shadow_to_hadbp, snapshot_source_proof,
    write_snapshot_from_shadow_file, NativeShadowInput, NativeSnapshotInput,
};
use walrust_core::native_spool::{
    durability_failpoint, filesystem_available_bytes, CapacityPolicy, CapacityState, NativeSpool,
    ObjectKind, RecoveryHead, SourceCursor, SpoolIdentity, StageObject,
};
use walrust_core::shadow_watch::{
    apply_shadow_sync_result_to_state, checkpoint_blocker_heartbeat_is_live,
    checkpoint_data_version, load_shadow_progress, rearm_checkpoint_blocker,
    rearm_checkpoint_blocker_after_checkpoint, save_shadow_watch_progress as save_shadow_progress,
    ShadowProgress, ShadowSyncOutput,
};

use super::prune::prune_with_client;
use super::types::{ShadowDbState, TriggerState};
use super::verify::validate_backup_integrity;

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

fn classify_descriptor_absent_native_keys(database: &str, keys: &[String]) -> Result<()> {
    if keys.is_empty() {
        return Ok(());
    }
    let sample = keys.iter().take(3).cloned().collect::<Vec<_>>().join(", ");
    bail!(
        "{database}: native-v1 descriptor is absent but its remote namespace is not empty; refusing to create a new lineage over orphan/foreign keys: {sample}"
    )
}

async fn require_empty_native_remote_namespace(
    client: &aws_sdk_s3::Client,
    bucket: &str,
    prefix: &str,
    database: &str,
) -> Result<()> {
    let native_prefix = format!("{}{}/native/v1/", prefix, database);
    let keys = s3::list_objects(client, bucket, &native_prefix).await?;
    classify_descriptor_absent_native_keys(database, &keys)
}

async fn prune_watcher_database(
    client: &aws_sdk_s3::Client,
    bucket: &str,
    prefix: &str,
    state: &ShadowDbState,
    _spool_state: &NativeSpoolState,
    policy: &RetentionPolicy,
) -> Result<()> {
    prune_with_client(client, bucket, prefix, &state.name, policy, true).await
}

fn shadow_storage_bytes(state: &ShadowDbState) -> u64 {
    walkdir::WalkDir::new(state.shadow.shadow_dir())
        .into_iter()
        .filter_map(|entry| entry.ok())
        .filter(|entry| entry.file_type().is_file())
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
) -> Result<ShadowSyncOutput> {
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
        return Ok(ShadowSyncOutput {
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
        let external_used = source_footprint_on_spool_filesystem(state, &spool)?;
        let additional_peak =
            (encoded.payload.len() as u64).saturating_add(spool.next_journal_rewrite_peak_bytes()?);
        match spool.capacity_state_with_external(external_used, additional_peak)? {
            CapacityState::High => tracing::error!(
                database = %state.name,
                event = "local_spool_high",
                spool_bytes = spool.used_bytes()?,
                additional_peak_bytes = additional_peak,
                external_used_bytes = external_used,
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
    Ok(ShadowSyncOutput {
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

async fn checkpoint_quiet_interval_pause(state: &ShadowDbState) -> Result<()> {
    if !cfg!(debug_assertions) {
        return Ok(());
    }
    let Some(path) = std::env::var_os("WALRUST_TEST_NATIVE_CHECKPOINT_QUIET_PAUSE_FILE") else {
        return Ok(());
    };
    if std::env::var_os("WALRUST_TEST_NATIVE_CHECKPOINT_QUIET_PAUSE_DB")
        .is_some_and(|selected| std::path::Path::new(&selected) != state.db_path)
    {
        return Ok(());
    }
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
    let durable_shadow_end_offset = state.shadow.segment_offset();
    anyhow::ensure!(
        durable_shadow_end_offset.is_multiple_of(frame_size),
        "{}: snapshot source cursor is not frame-aligned",
        state.name
    );
    let shadow_end_offset = committed_shadow_prefix_offset(
        state.shadow.shadow_dir(),
        snapshot_generation,
        durable_shadow_end_offset,
        page_size,
    )?;
    if shadow_end_offset < durable_shadow_end_offset {
        tracing::debug!(
            database = %state.name,
            committed_shadow_end_offset = shadow_end_offset,
            durable_shadow_end_offset,
            "native snapshot excluded an in-flight transaction from its frozen boundary"
        );
    }
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
        let additional_peak = payload_upper.saturating_add(journal_peak);
        if spool.capacity_state_with_external(source_footprint, additional_peak)?
            == CapacityState::Full
        {
            bail!(
                "local_spool_full: {} lacks peak capacity/reserve for direct native HADBP snapshot payload + journal \
                 (additional_peak={additional_peak}, payload_upper={payload_upper}, journal_peak={journal_peak}, \
                 source_footprint={source_footprint}, main_bytes={main_bytes}, shadow_frames={shadow_frames})",
                state.name,
            );
        }
    }
    let (proposed_seq, proposed_previous, proposed_key) = {
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
        (
            seq,
            previous,
            native_object_key(spool.identity(), ObjectKind::Snapshot, seq),
        )
    };
    let source_db_file = state.source_db_file.as_ref().cloned().ok_or_else(|| {
        anyhow!(
            "{}: native snapshot source descriptor was not retained",
            state.name
        )
    })?;
    let proof_input = NativeSnapshotInput {
        db_path: state.db_path.clone(),
        seq: proposed_seq,
        previous_chain_checksum: proposed_previous,
        generation: proposed_cursor.shadow_generation,
        shadow_end_offset: proposed_cursor
            .shadow_frame_index
            .saturating_mul(frame_size),
        page_size,
        shadow_dir: state.shadow.shadow_dir().to_path_buf(),
        #[cfg(unix)]
        expected_db_file_identity: db_identity_before,
    };
    let source_for_proof = Arc::clone(&source_db_file);
    let source_proof = tokio::task::spawn_blocking(move || {
        let mut source = source_for_proof
            .lock()
            .map_err(|_| anyhow!("native snapshot source descriptor lock poisoned"))?;
        snapshot_source_proof(&proof_input, &mut source)
    })
    .await??;
    let preparation = {
        let mut spool = spool_lock(&spool_state.0)?;
        spool.prepare_snapshot(
            proposed_seq,
            proposed_previous,
            proposed_key,
            proposed_cursor,
            page_size,
            source_proof.end_page_count,
            source_proof.ending_chain_checksum,
            source_proof.page_image_sha256,
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
    let decoded_snapshot = ltx::decode_sqlite_changeset(&encoded_payload)?;
    let (page_digest, page_count) = ltx::snapshot_page_image_sha256(&decoded_snapshot)?;
    let page_digest = page_digest
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect::<String>();
    anyhow::ensure!(
        encoded.end_page_count == preparation.expected_end_page_count
            && encoded.ending_chain_checksum == preparation.expected_ending_chain_checksum
            && page_count == preparation.expected_end_page_count
            && page_digest == preparation.expected_page_image_sha256,
        "native snapshot payload differs from its durable source-content intent at seq {}",
        seq
    );
    let stage_result = (|| -> Result<()> {
        let mut spool = spool_lock(&spool_state.0)?;
        // The fsynced payload temporary is already included in used_bytes and
        // admission renames that exact inode. Only journal/intent rewrite and
        // source-filesystem reserve remain as additional peak here.
        let external_used = source_footprint_on_spool_filesystem(state, &spool)?;
        let additional_peak = spool.next_journal_rewrite_peak_bytes()?;
        match spool.capacity_state_with_external(external_used, additional_peak)? {
            CapacityState::High => tracing::error!(
                database = %state.name,
                event = "local_spool_high",
                spool_bytes = spool.used_bytes()?,
                additional_peak_bytes = additional_peak,
                external_used_bytes = external_used,
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

    let controller = state
        .checkpoint_controller
        .as_ref()
        .ok_or_else(|| anyhow!("{}: CLI checkpoint controller was not held", state.name))?;

    let mode_name = match mode {
        ShadowCheckpointMode::Passive => "PASSIVE",
        ShadowCheckpointMode::Truncate => "TRUNCATE",
    };
    let checkpoint_result = controller
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
    let rearm_result = rearm_checkpoint_blocker_after_checkpoint(state, data_version_before);
    let heartbeat_live = match &rearm_result {
        Ok(_) => checkpoint_blocker_heartbeat_is_live(state),
        Err(_) => Ok(false),
    };
    if rearm_result.is_ok() {
        durability_failpoint("blocker_reacquired");
    }
    let checkpoint_result = checkpoint_result.map(|completed| completed);
    match (checkpoint_result, rearm_result) {
        (Ok(completed), Ok(data_version_dirty)) => Ok(CheckpointAttempt {
            completed,
            dirty: data_version_dirty || !heartbeat_live?,
        }),
        (Err(checkpoint_error), Ok(_)) => Err(checkpoint_error),
        (Ok(_), Err(rearm_error)) => Err(rearm_error),
        (Err(checkpoint_error), Err(rearm_error)) => Err(anyhow!(
            "{}; additionally failed to rearm CLI checkpoint blocker: {}",
            checkpoint_error,
            rearm_error
        )),
    }
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
        let attempt: Result<()> = async {
            copy_final_shadow_frames(db_states).await?;
            let mut final_results = Vec::with_capacity(db_states.len());
            for (db_path, state) in db_states.iter() {
                final_results.push(match native_spools.get(db_path) {
                    Some(spool) => stage_native_shadow(state, spool).await,
                    None => Err(anyhow!("{}: native spool missing", state.name)),
                });
            }
            for result in final_results {
                let output = result?;
                if let Some(state) = db_states.get_mut(&output.db_path) {
                    apply_shadow_sync_result_to_state(state, &output).await?;
                }
            }
            Ok(())
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
    // Longer than a typical short-lived/periodic writer cadence: an
    // instantaneous lull between application sessions must not be mistaken
    // for a safe release boundary. Eight failed intervals remain bounded and
    // defer the checkpoint with the blocker held.
    const PREFLIGHT_QUIET_INTERVAL: Duration = Duration::from_millis(500);
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
            // One instantaneous equal sample is not a useful quiescence
            // boundary for periodic/ephemeral writers. Keep the blocker held
            // for a short bounded interval, then require the version to remain
            // unchanged before opening the controlled checkpoint window.
            checkpoint_quiet_interval_pause(state).await?;
            tokio::time::sleep(PREFLIGHT_QUIET_INTERVAL).await;
            let data_version_after_quiet = checkpoint_data_version(state)?;
            if data_version_after_quiet == data_version_after {
                data_version_before = data_version_after_quiet;
                break admitted_seq;
            }
            tracing::debug!(
                database = %state.name,
                data_version_before = data_version_after,
                data_version_after = data_version_after_quiet,
                quiet_ms = PREFLIGHT_QUIET_INTERVAL.as_millis() as u64,
                "application commit crossed native checkpoint quiet interval; draining before release"
            );
            data_version_before = data_version_after_quiet;
        } else {
            tracing::debug!(
                database = %state.name,
                data_version_before,
                data_version_after,
                "application commit crossed native checkpoint preflight; draining the newly committed WAL frames before release"
            );
            data_version_before = data_version_after;
        }
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
    prune_policy: Option<RetentionPolicy>,
    metrics_port: u16,
    no_metrics: bool,
    retry_config: RetryConfig,
    webhooks: Vec<WebhookConfig>,
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
    let mut startup_checkpoint_controllers: HashMap<PathBuf, Connection> = HashMap::new();
    let mut startup_db_checksums: HashMap<PathBuf, Result<u64>> = HashMap::new();
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
        let checkpoint_controller = Connection::open(&db_config.path).with_context(|| {
            format!(
                "{}: failed to open CLI checkpoint controller",
                db_config.prefix
            )
        })?;
        checkpoint_controller.busy_timeout(Duration::from_secs(5))?;
        ShadowWal::enable_persistent_wal(&checkpoint_controller, &db_config.path)?;
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
        startup_checkpoint_controllers.insert(db_config.path.clone(), checkpoint_controller);
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

    let mut native_spools: HashMap<PathBuf, NativeSpoolState> = HashMap::new();
    let mut native_uploader_shutdown: HashMap<PathBuf, tokio::sync::watch::Sender<bool>> =
        HashMap::new();
    let mut native_uploader_handles: Vec<(PathBuf, tokio::task::JoinHandle<()>)> = Vec::new();
    let mut native_lag_states: HashMap<
        PathBuf,
        Arc<Mutex<walrust_core::native_publish::RemoteLagState>>,
    > = HashMap::new();
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
        )?;
        let spool_root = NativeSpool::resolve_path_for(&spool_base, &binding_identity)?;
        let existing_spool_identity = NativeSpool::read_identity(&spool_root)?;

        // Offline restart requires a complete matching local spool whose
        // durable journal records a published snapshot base. A host without
        // that proof must contact remote storage before it can continue or
        // create a lineage. Ambiguous remote failure is never treated as a
        // fresh stream.
        let mut current_txid = 0;
        let spool_identity = if let Some(identity) = existing_spool_identity {
            if identity.canonical_db_path != binding_identity.canonical_db_path
                || identity.bucket != bucket_name
                || identity.prefix != prefix
                || identity.database != name
            {
                bail!("{}: local native spool identity/base mismatch", name);
            }
            if !NativeSpool::validate_existing_published_base(&spool_root, &identity)? {
                let descriptor_key = format!("{}{}/native/v1/stream.json", prefix, name);
                match s3::download_bytes(&client, &bucket_name, &descriptor_key).await {
                    Ok(bytes) => {
                        let descriptor: walrust_core::native_publish::StreamDescriptor =
                            serde_json::from_slice(&bytes).with_context(|| {
                                format!("{}: parse remote native stream descriptor", name)
                            })?;
                        let expected =
                            walrust_core::native_publish::StreamDescriptor::from(&identity);
                        anyhow::ensure!(
                            descriptor == expected,
                            "{}: remote native descriptor does not match the durable local lineage",
                            name
                        );
                    }
                    Err(error) if s3::download_error_is_not_found(&error) => {
                        require_empty_native_remote_namespace(
                            &client,
                            &bucket_name,
                            &prefix,
                            &name,
                        )
                        .await?;
                    }
                    Err(error) => {
                        return Err(error).with_context(|| {
                            format!(
                                "{}: remote unavailable and local native spool has no remotely published snapshot base",
                                name
                            )
                        })
                    }
                }
            }
            identity
        } else {
            let descriptor_key = format!("{}{}/native/v1/stream.json", prefix, name);
            match s3::download_bytes(&client, &bucket_name, &descriptor_key).await {
                    Ok(_) => bail!(
                        "{}: remote native stream exists but no matching verified local spool/base is present",
                        name
                    ),
                    Err(error) if s3::download_error_is_not_found(&error) => {
                        require_empty_native_remote_namespace(
                            &client,
                            &bucket_name,
                            &prefix,
                            &name,
                        )
                        .await?;
                    }
                    Err(error) => {
                        return Err(error).with_context(|| {
                            format!(
                                "{}: native stream discovery unavailable and no verified local spool exists",
                                name
                            )
                        })
                    }
            }
            SpoolIdentity::new(
                db_path,
                bucket_name.clone(),
                prefix.clone(),
                name.clone(),
                uuid::Uuid::new_v4().to_string(),
                1,
            )?
        };

        let mut db_checksum = match startup_db_checksums
            .remove(db_path)
            .ok_or_else(|| anyhow!("{}: startup database checksum missing", name))?
        {
            Ok(checksum) => Some(checksum),
            Err(error) => {
                tracing::warn!("{}: Could not compute initial checksum: {}", name, error);
                None
            }
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
        let checkpoint_controller = startup_checkpoint_controllers
            .remove(db_path)
            .ok_or_else(|| anyhow!("{}: startup checkpoint controller missing", name))?;
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
                checkpoint_controller: Some(checkpoint_controller),
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
    let prune_interval_duration = if global_sync.prune_interval > 0 {
        Duration::from_secs(global_sync.prune_interval)
    } else {
        disabled_timer_duration
    };
    let mut compact_timer = tokio::time::interval(prune_interval_duration);
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
    let mut spool_cleanup_timer = tokio::time::interval(cleanup_interval);
    spool_cleanup_timer.tick().await; // Skip first immediate tick

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
                if global_sync.prune_after_snapshot {
                    if let Some(ref policy) = prune_policy {
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
            _ = compact_timer.tick(), if global_sync.prune_interval > 0 => {
                if let Some(ref policy) = prune_policy {
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

            // Native spool cleanup timer
            _ = spool_cleanup_timer.tick() => {
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
        drop(state.checkpoint_controller.take());
        drop(state.source_db_file.take());
    }

    tracing::info!("walrust shadow mode shutdown complete");
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use async_trait::async_trait;
    use hadb_storage::CasResult;
    use rusqlite::Connection;
    use std::path::Path;
    use tempfile::TempDir;

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

    fn create_real_wal_db() -> (TempDir, PathBuf, Connection) {
        let temp = TempDir::new().unwrap();
        let db_path = temp.path().join("watch-native.db");
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
        assert!(db_path.with_extension("db-wal").exists());
        (temp, db_path, conn)
    }

    fn test_source_db_file(path: &Path) -> Option<Arc<Mutex<std::fs::File>>> {
        Some(Arc::new(Mutex::new(std::fs::File::open(path).unwrap())))
    }

    fn test_native_spool_state(db_path: &Path, base: &Path) -> NativeSpoolState {
        let identity =
            SpoolIdentity::new(db_path, "bucket", "tests/", "db", "test-lineage", 1).unwrap();
        let root = NativeSpool::path_for(base, &identity);
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

    async fn test_state(db_path: &Path) -> ShadowDbState {
        let shadow = ShadowWal::new_without_checkpoint_blocker(db_path)
            .await
            .unwrap();
        let data_version_monitor = Connection::open(db_path).unwrap();
        let checkpoint_controller = Connection::open(db_path).unwrap();
        let checkpoint_blocker = ShadowWal::open_checkpoint_blocker(db_path).unwrap();
        ShadowDbState {
            name: "native-watch-test".to_string(),
            db_path: db_path.to_path_buf(),
            wal_path: db_path.with_extension("db-wal"),
            current_txid: 0,
            last_snapshot: None,
            db_checksum: None,
            shadow,
            checkpoint_blocker: Some(checkpoint_blocker),
            data_version_monitor: Some(data_version_monitor),
            checkpoint_controller: Some(checkpoint_controller),
            source_db_file: test_source_db_file(db_path),
            shadow_sync_generation: 0,
            shadow_sync_offset: 0,
            wal_copy_offset: 0,
        }
    }

    #[tokio::test]
    async fn application_commit_after_checkpoint_is_observed_before_heartbeat_write() {
        let (_temp, db_path, app) = create_real_wal_db();
        let mut state = test_state(&db_path).await;
        rearm_checkpoint_blocker(&mut state).unwrap();
        let baseline = checkpoint_data_version(&state).unwrap();

        state
            .checkpoint_blocker
            .as_ref()
            .unwrap()
            .execute_batch("ROLLBACK;")
            .unwrap();
        let _: (i64, i64, i64) = state
            .checkpoint_controller
            .as_ref()
            .unwrap()
            .query_row("PRAGMA wal_checkpoint(PASSIVE);", [], |row| {
                Ok((row.get(0)?, row.get(1)?, row.get(2)?))
            })
            .unwrap();
        app.execute("INSERT INTO items(value) VALUES ('dirty-window')", [])
            .unwrap();

        let dirty = rearm_checkpoint_blocker_after_checkpoint(&mut state, baseline).unwrap();
        assert!(dirty, "application commit must force a snapshot re-anchor");
        assert!(checkpoint_blocker_heartbeat_is_live(&state).unwrap());
    }

    #[tokio::test]
    async fn default_local_release_checkpoints_without_any_remote_io() {
        let (temp, db_path, conn) = create_real_wal_db();
        let mut state = test_state(&db_path).await;
        rearm_checkpoint_blocker(&mut state).unwrap();
        let spool_state = test_native_spool_state(&db_path, temp.path());
        stage_native_snapshot(&mut state, &spool_state)
            .await
            .unwrap();
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
        let spool = spool_lock(&spool_state.0).unwrap();
        assert_eq!(spool.admitted_seq(), Some(2));
        assert_eq!(spool.remote_published_seq(), None);
        assert_eq!(
            spool.get(2).unwrap().remote_upload_state,
            walrust_core::native_spool::RemoteUploadState::Pending
        );
        assert!(spool.read_payload(2).unwrap().starts_with(b"HADBP"));
        assert!(checkpoint_blocker_heartbeat_is_live(&state).unwrap());
        assert!(live_wal_page_count(&state.wal_path).await.unwrap() <= 4);
    }

    #[tokio::test]
    async fn remote_release_requires_contiguous_publication_before_checkpoint() {
        let (temp, db_path, conn) = create_real_wal_db();
        let mut state = test_state(&db_path).await;
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
            Duration::from_millis(25),
            ShadowCheckpointMode::Truncate,
        )
        .await
        .unwrap_err();

        assert!(format!("{error:#}").contains("contiguous remote publish"));
        assert_eq!(
            spool_lock(&spool_state.0).unwrap().checkpoint_window(),
            &walrust_core::native_spool::CheckpointWindow::Closed
        );
        assert!(checkpoint_blocker_heartbeat_is_live(&state).unwrap());
    }

    #[tokio::test]
    async fn partial_passive_checkpoint_is_nonfatal_and_rearms_blocker() {
        let (temp, db_path, conn) = create_real_wal_db();
        let mut state = test_state(&db_path).await;
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
            Duration::from_millis(25),
            ShadowCheckpointMode::Passive,
        )
        .await
        .unwrap();

        assert!(!attempt.completed);
        assert_eq!(spool_lock(&spool_state.0).unwrap().admitted_seq(), Some(2));
        assert!(checkpoint_blocker_heartbeat_is_live(&state).unwrap());
        reader.execute_batch("ROLLBACK").unwrap();
    }

    #[tokio::test]
    async fn partial_truncate_checkpoint_is_bounded_nonfatal_and_rearms_blocker() {
        let (temp, db_path, conn) = create_real_wal_db();
        let mut state = test_state(&db_path).await;
        state
            .checkpoint_controller
            .as_ref()
            .unwrap()
            .busy_timeout(Duration::from_millis(50))
            .unwrap();
        let spool_state = test_native_spool_state(&db_path, temp.path());
        stage_native_snapshot(&mut state, &spool_state)
            .await
            .unwrap();

        let reader = Connection::open(&db_path).unwrap();
        reader
            .execute_batch("BEGIN; SELECT count(*) FROM items;")
            .unwrap();
        conn.execute("INSERT INTO items(value) VALUES ('truncate-pinned')", [])
            .unwrap();

        let started = std::time::Instant::now();
        let attempt = checkpoint_shadow_after_native_admission(
            &mut state,
            &spool_state,
            crate::config::CheckpointRelease::Local,
            Duration::from_millis(25),
            ShadowCheckpointMode::Truncate,
        )
        .await
        .unwrap();

        assert!(!attempt.completed);
        assert!(started.elapsed() < Duration::from_secs(2));
        assert_eq!(spool_lock(&spool_state.0).unwrap().admitted_seq(), Some(2));
        assert!(checkpoint_blocker_heartbeat_is_live(&state).unwrap());
        reader.execute_batch("ROLLBACK").unwrap();
    }

    #[tokio::test]
    async fn spool_full_refuses_checkpoint_and_retains_blocker() {
        let (temp, db_path, conn) = create_real_wal_db();
        let mut state = test_state(&db_path).await;
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
        conn.execute("INSERT INTO items(value) VALUES ('must-stay-local')", [])
            .unwrap();

        let error = checkpoint_shadow_after_native_admission(
            &mut state,
            &spool_state,
            crate::config::CheckpointRelease::Local,
            Duration::from_millis(25),
            ShadowCheckpointMode::Truncate,
        )
        .await
        .unwrap_err();

        assert!(format!("{error:#}").contains("local_spool_full"));
        assert_eq!(spool_lock(&spool_state.0).unwrap().admitted_seq(), Some(1));
        assert!(checkpoint_blocker_heartbeat_is_live(&state).unwrap());
    }

    #[tokio::test]
    async fn direct_native_snapshot_restores_exactly_during_passive_contention() {
        use std::sync::atomic::{AtomicBool, Ordering};

        let (temp, db_path, conn) = create_real_wal_db();
        for id in 10..1010i64 {
            conn.execute(
                "INSERT INTO items(id, value) VALUES (?1, ?2)",
                rusqlite::params![id, format!("passive-{id}")],
            )
            .unwrap();
        }
        let expected: i64 = conn
            .query_row("SELECT count(*) FROM items", [], |row| row.get(0))
            .unwrap();
        let mut state = test_state(&db_path).await;
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

        let staged = stage_native_snapshot(&mut state, &spool_state).await;
        running.store(false, Ordering::Relaxed);
        checkpointer.join().unwrap();
        staged.unwrap();

        let payload = spool_lock(&spool_state.0).unwrap().read_payload(1).unwrap();
        let restored_path = temp.path().join("restored.db");
        walrust_core::ltx::decode_to_db(&payload, &restored_path).unwrap();
        let restored = Connection::open(restored_path).unwrap();
        assert_eq!(
            restored
                .query_row("SELECT count(*) FROM items", [], |row| row.get::<_, i64>(0))
                .unwrap(),
            expected
        );
        assert_eq!(
            restored
                .query_row("PRAGMA integrity_check", [], |row| row.get::<_, String>(0))
                .unwrap(),
            "ok"
        );
    }

    #[test]
    fn descriptor_absence_rejects_orphan_native_namespace_keys() {
        classify_descriptor_absent_native_keys(
            "db",
            &["p/db/native/v1/lineages/orphan/0001/0000000000000001.hadbp".into()],
        )
        .expect_err("orphan native key must prevent fresh-lineage classification");
        classify_descriptor_absent_native_keys("db", &[]).unwrap();
    }
}
