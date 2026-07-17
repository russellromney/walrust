use anyhow::{anyhow, Result};
use chrono::Utc;
use hadb_storage_s3::S3Storage;
use std::ffi::OsString;
use std::fs::{self, File};
use std::path::{Path, PathBuf};
use std::time::Duration;

use super::types::ReplicaState;
use crate::errors::{classify_or_else, WalrustError};
use crate::s3::create_client;

fn fsync_parent_dir(path: &Path) -> Result<()> {
    let parent = path.parent().unwrap_or_else(|| Path::new("."));
    File::open(parent)
        .map_err(|error| {
            anyhow!(
                "failed to open directory {} for fsync: {error}",
                parent.display()
            )
        })?
        .sync_all()
        .map_err(|error| anyhow!("failed to fsync directory {}: {error}", parent.display()))
}

fn sqlite_sidecar_path(path: &Path, suffix: &str) -> PathBuf {
    let mut value = OsString::from(path.as_os_str());
    value.push(suffix);
    PathBuf::from(value)
}

fn remove_replica_staging(tmp: &Path) -> Result<()> {
    let mut removed = false;
    for path in [
        tmp.to_path_buf(),
        sqlite_sidecar_path(tmp, "-wal"),
        sqlite_sidecar_path(tmp, "-shm"),
    ] {
        match fs::remove_file(&path) {
            Ok(()) => removed = true,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => return Err(error.into()),
        }
    }
    if removed {
        fsync_parent_dir(tmp)?;
    }
    Ok(())
}

fn ensure_replica_replace_safe(local: &Path) -> Result<()> {
    for sidecar in [
        sqlite_sidecar_path(local, "-wal"),
        sqlite_sidecar_path(local, "-shm"),
    ] {
        match fs::symlink_metadata(&sidecar) {
            Ok(_) => {
                return Err(WalrustError::restore(format!(
                    "refusing to replace native replica {} while SQLite sidecar {} exists",
                    local.display(),
                    sidecar.display()
                ))
                .into())
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => return Err(error.into()),
        }
    }
    Ok(())
}

fn validate_replica_identity(
    state: &ReplicaState,
    source_identity: &str,
    visible: &walrust_core::native_restore::NativeVisibleState,
    local: &Path,
) -> Result<()> {
    if state.source != source_identity
        || state.stream_digest != visible.stream_digest
        || state.lineage_id != visible.lineage_id
    {
        return Err(WalrustError::restore(format!(
            "native replica source identity changed for {}; refusing to keep a database from another stream/lineage",
            local.display()
        ))
        .into());
    }
    if visible.head_seq < state.current_txid {
        return Err(WalrustError::restore(format!(
            "native replica remote head regressed from {} to {}",
            state.current_txid, visible.head_seq
        ))
        .into());
    }
    Ok(())
}

pub async fn replicate(
    source: &str,
    local: &Path,
    interval: Duration,
    endpoint: Option<&str>,
) -> Result<()> {
    let source = source.strip_prefix("s3://").unwrap_or(source);
    let (bucket, path) = source.split_once('/').ok_or_else(|| {
        WalrustError::config(
            "Invalid source format. Expected: s3://bucket/dbname or s3://bucket/prefix/dbname",
        )
    })?;
    let (prefix, database) = match path.rsplit_once('/') {
        Some((prefix, database)) => (format!("{prefix}/"), database.to_string()),
        None => (String::new(), path.to_string()),
    };
    if database.is_empty() {
        return Err(WalrustError::config("native replica source database name is empty").into());
    }

    let _db_lock = crate::lock::DbLock::acquire(local)?;
    let client = create_client(endpoint)
        .await
        .map_err(|error| classify_or_else(error, WalrustError::s3))?;
    let source_identity = format!("s3://{bucket}/{prefix}{database}");
    let mut replica_state = read_replica_state(local)?;
    match (local.exists(), replica_state.is_some()) {
        (true, false) => {
            return Err(WalrustError::restore(format!(
                "native replica database {} exists without its identity-bound state file",
                local.display()
            ))
            .into())
        }
        (false, true) => {
            return Err(WalrustError::restore(format!(
                "native replica state exists but database {} is missing",
                local.display()
            ))
            .into())
        }
        _ => {}
    }
    println!(
        "Replicating native-v1 s3://{}/{}{} -> {}",
        bucket,
        prefix,
        database,
        local.display()
    );

    loop {
        match replicate_poll(
            &client,
            bucket,
            &prefix,
            &database,
            &source_identity,
            local,
            &mut replica_state,
        )
        .await
        {
            Ok(true) => println!(
                "[{}] Published native sequence {} locally",
                chrono::Local::now().format("%H:%M:%S"),
                replica_state
                    .as_ref()
                    .map(|state| state.current_txid)
                    .unwrap_or(0)
            ),
            Ok(false) => {}
            Err(error) => {
                tracing::error!(error = %error, "native replica poll failed");
                eprintln!(
                    "[{}] Error: {error:#}",
                    chrono::Local::now().format("%H:%M:%S")
                );
            }
        }
        tokio::time::sleep(interval).await;
    }
}

async fn replicate_poll(
    client: &aws_sdk_s3::Client,
    bucket: &str,
    prefix: &str,
    database: &str,
    source_identity: &str,
    local: &Path,
    replica_state: &mut Option<ReplicaState>,
) -> Result<bool> {
    let tmp = local.with_extension("db-native-replica.tmp");
    let storage = S3Storage::new(client.clone(), bucket.to_string());
    let visible =
        walrust_core::native_restore::inspect_native_v1(&storage, bucket, prefix, database)
            .await?
            .ok_or_else(|| {
                WalrustError::restore(format!(
                    "no native-v1 HADBP recovery stream found for '{database}'"
                ))
            })?;
    if let Some(state) = replica_state.as_ref() {
        validate_replica_identity(state, source_identity, &visible, local)?;
    }
    // This path is private replica staging, never the user-visible database.
    // A crash after a successful restore but before the atomic swap may leave
    // it behind; remote immutable objects can deterministically rebuild it.
    remove_replica_staging(&tmp)?;
    let availability = walrust_core::native_restore::restore_native_v1(
        &storage, bucket, prefix, database, &tmp, None,
    )
    .await?;
    let seq = match availability {
        walrust_core::native_restore::NativeRestoreAvailability::Restored { seq } => seq,
        _ => {
            return Err(WalrustError::restore(format!(
                "no native-v1 HADBP recovery stream found for '{database}'"
            ))
            .into())
        }
    };
    if replica_state
        .as_ref()
        .is_some_and(|state| seq <= state.current_txid)
        && local.exists()
    {
        let _ = fs::remove_file(&tmp);
        return Ok(false);
    }
    ensure_replica_replace_safe(local)?;
    fs::rename(&tmp, local)?;
    fsync_parent_dir(local)?;
    let state = ReplicaState {
        source: source_identity.to_string(),
        stream_digest: visible.stream_digest,
        lineage_id: visible.lineage_id,
        current_txid: seq,
        last_updated: Utc::now().to_rfc3339(),
    };
    save_replica_state(local, &state)?;
    *replica_state = Some(state);
    Ok(true)
}

fn read_replica_state(local: &Path) -> Result<Option<ReplicaState>> {
    let path = local.with_extension("db-replica-state");
    let state = match std::fs::read_to_string(&path) {
        Ok(state) => state,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(error.into()),
    };
    serde_json::from_str::<ReplicaState>(&state)
        .map(Some)
        .map_err(|error| {
            anyhow!(
                "native replica state {} is invalid; refusing an unbound resume: {error}",
                path.display()
            )
        })
}

fn save_replica_state(local: &Path, state: &ReplicaState) -> Result<()> {
    let state_path = local.with_extension("db-replica-state");
    let tmp = state_path.with_extension("db-replica-state.tmp");
    let bytes = serde_json::to_vec_pretty(state)?;
    {
        use std::io::Write;
        let mut file = File::create(&tmp)?;
        file.write_all(&bytes)?;
        file.sync_all()?;
    }
    fs::rename(&tmp, &state_path)?;
    fsync_parent_dir(&state_path)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn visible() -> walrust_core::native_restore::NativeVisibleState {
        walrust_core::native_restore::NativeVisibleState {
            stream_digest: "digest-a".into(),
            lineage_id: "lineage-a".into(),
            head_seq: 4,
            object_count: 4,
            latest_snapshot_seq: 1,
            retention_floor_seq: 1,
            snapshot_seqs: vec![1],
        }
    }

    #[test]
    fn replica_resume_rejects_different_remote_identity_and_regressed_head() {
        let local = Path::new("replica.db");
        let state = ReplicaState {
            source: "s3://bucket/db".into(),
            stream_digest: "digest-a".into(),
            lineage_id: "lineage-a".into(),
            current_txid: 4,
            last_updated: "now".into(),
        };
        validate_replica_identity(&state, "s3://bucket/db", &visible(), local).unwrap();

        let mut changed = visible();
        changed.lineage_id = "lineage-b".into();
        assert!(
            validate_replica_identity(&state, "s3://bucket/db", &changed, local)
                .unwrap_err()
                .to_string()
                .contains("identity changed")
        );

        let mut regressed = visible();
        regressed.head_seq = 3;
        assert!(
            validate_replica_identity(&state, "s3://bucket/db", &regressed, local)
                .unwrap_err()
                .to_string()
                .contains("regressed")
        );
    }

    #[test]
    fn replica_state_missing_identity_fields_fails_loudly() {
        let dir = tempfile::tempdir().unwrap();
        let local = dir.path().join("replica.db");
        std::fs::write(
            local.with_extension("db-replica-state"),
            r#"{"current_txid":4,"last_updated":"now"}"#,
        )
        .unwrap();
        assert!(read_replica_state(&local)
            .unwrap_err()
            .to_string()
            .contains("unbound resume"));
    }

    #[test]
    fn replica_refuses_live_or_stale_sqlite_sidecars_and_cleans_only_private_staging() {
        let dir = tempfile::tempdir().unwrap();
        let local = dir.path().join("replica.db");
        fs::write(&local, b"visible replica").unwrap();
        for sidecar in [
            sqlite_sidecar_path(&local, "-wal"),
            sqlite_sidecar_path(&local, "-shm"),
        ] {
            fs::write(&sidecar, b"must survive").unwrap();
            assert!(ensure_replica_replace_safe(&local)
                .unwrap_err()
                .to_string()
                .contains("refusing to replace"));
            assert_eq!(fs::read(&sidecar).unwrap(), b"must survive");
            fs::remove_file(sidecar).unwrap();
        }

        let tmp = local.with_extension("db-native-replica.tmp");
        fs::write(&tmp, b"stale staging").unwrap();
        fs::write(sqlite_sidecar_path(&tmp, "-wal"), b"stale staging wal").unwrap();
        remove_replica_staging(&tmp).unwrap();
        assert!(!tmp.exists());
        assert!(!sqlite_sidecar_path(&tmp, "-wal").exists());
        assert_eq!(fs::read(&local).unwrap(), b"visible replica");
    }
}
