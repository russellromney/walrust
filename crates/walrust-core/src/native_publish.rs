//! Ordered, idempotent remote publication for the native CLI spool.

use crate::native_spool::{
    durability_failpoint, NativeSpool, ObjectKind, RemoteUploadState, SpoolIdentity, SpoolObject,
};
use anyhow::{anyhow, bail, Context, Result};
use hadb_storage::StorageBackend;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::sync::Notify;

pub const REMOTE_LAYOUT_VERSION: u32 = 1;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StreamDescriptor {
    pub version: u32,
    pub stream_digest: String,
    pub bucket: String,
    pub prefix: String,
    pub database: String,
    pub lineage_id: String,
    pub first_native_seq: u64,
    pub legacy_boundary_txid: Option<u64>,
}

impl From<&SpoolIdentity> for StreamDescriptor {
    fn from(value: &SpoolIdentity) -> Self {
        Self {
            version: REMOTE_LAYOUT_VERSION,
            stream_digest: value.stream_digest(),
            bucket: value.bucket.clone(),
            prefix: value.prefix.clone(),
            database: value.database.clone(),
            lineage_id: value.lineage_id.clone(),
            first_native_seq: value.first_native_seq,
            legacy_boundary_txid: value.legacy_boundary_txid,
        }
    }
}

impl StreamDescriptor {
    pub fn key(&self) -> String {
        format!("{}{}/native/v1/stream.json", self.prefix, self.database)
    }

    pub fn bytes(&self) -> Result<Vec<u8>> {
        Ok(serde_json::to_vec_pretty(self)?)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PublishRecord {
    pub version: u32,
    pub stream_digest: String,
    pub lineage_id: String,
    pub seq: u64,
    pub kind: ObjectKind,
    pub previous_publish_sha256: Option<String>,
    pub previous_chain_checksum: u64,
    pub ending_chain_checksum: u64,
    pub end_page_count: u64,
    pub object_key: String,
    pub payload_length: u64,
    pub payload_sha256: String,
}

impl PublishRecord {
    pub fn from_object(object: &SpoolObject, previous_publish_sha256: Option<String>) -> Self {
        Self {
            version: REMOTE_LAYOUT_VERSION,
            stream_digest: object.stream_digest.clone(),
            lineage_id: object.lineage_id.clone(),
            seq: object.seq,
            kind: object.kind,
            previous_publish_sha256,
            previous_chain_checksum: object.previous_chain_checksum,
            ending_chain_checksum: object.ending_chain_checksum,
            end_page_count: object.end_page_count,
            object_key: object.intended_remote_key.clone(),
            payload_length: object.payload_length,
            payload_sha256: object.payload_sha256.clone(),
        }
    }

    pub fn key(&self, prefix: &str, database: &str) -> String {
        format!(
            "{}{}/native/v1/lineages/{}/published/{:016x}.json",
            prefix, database, self.lineage_id, self.seq
        )
    }

    pub fn bytes(&self) -> Result<Vec<u8>> {
        Ok(serde_json::to_vec_pretty(self)?)
    }
}

pub fn object_key(identity: &SpoolIdentity, kind: ObjectKind, seq: u64) -> String {
    let generation = match kind {
        ObjectKind::Snapshot => 1,
        ObjectKind::Delta => 0,
    };
    format!(
        "{}{}/native/v1/lineages/{}/{generation:04x}/{seq:016x}.hadbp",
        identity.prefix, identity.database, identity.lineage_id
    )
}

#[derive(Debug, Clone, Default)]
pub struct RemoteLagState {
    pub pending_objects: u64,
    pub pending_bytes: u64,
    pub oldest_age_ms: u64,
    pub last_error: Option<String>,
    pub last_upload_duration_ms: u64,
}

#[derive(Clone)]
pub struct UploadWake {
    notify: Arc<Notify>,
}

impl UploadWake {
    /// Best effort only: durability is the spool journal, never this channel.
    pub fn notify(&self) {
        self.notify.notify_one();
    }
}

pub struct NativeUploader {
    storage: Arc<dyn StorageBackend>,
    spool: Arc<Mutex<NativeSpool>>,
    descriptor: StreamDescriptor,
    wake: UploadWake,
    lag: Arc<Mutex<RemoteLagState>>,
    scan_interval: Duration,
    max_backoff: Duration,
    test_pause_file: Option<PathBuf>,
    test_crash_once_file: Option<PathBuf>,
}

impl NativeUploader {
    pub fn new(
        storage: Arc<dyn StorageBackend>,
        spool: Arc<Mutex<NativeSpool>>,
    ) -> Result<(Self, UploadWake, Arc<Mutex<RemoteLagState>>)> {
        let descriptor = {
            let guard = spool
                .lock()
                .map_err(|_| anyhow!("native spool lock poisoned"))?;
            StreamDescriptor::from(guard.identity())
        };
        let wake = UploadWake {
            notify: Arc::new(Notify::new()),
        };
        let lag = Arc::new(Mutex::new(RemoteLagState::default()));
        Ok((
            Self {
                storage,
                spool,
                descriptor,
                wake: wake.clone(),
                lag: lag.clone(),
                scan_interval: Duration::from_secs(5),
                max_backoff: Duration::from_secs(30),
                test_pause_file: if cfg!(debug_assertions) {
                    std::env::var_os("WALRUST_TEST_NATIVE_UPLOAD_PAUSE_FILE").map(PathBuf::from)
                } else {
                    None
                },
                test_crash_once_file: if cfg!(debug_assertions) {
                    std::env::var_os("WALRUST_TEST_NATIVE_UPLOAD_CRASH_ONCE_FILE")
                        .map(PathBuf::from)
                } else {
                    None
                },
            },
            wake,
            lag,
        ))
    }

    pub fn with_runtime(
        storage: Arc<dyn StorageBackend>,
        spool: Arc<Mutex<NativeSpool>>,
        wake: UploadWake,
        lag: Arc<Mutex<RemoteLagState>>,
    ) -> Result<Self> {
        let descriptor = {
            let guard = spool
                .lock()
                .map_err(|_| anyhow!("native spool lock poisoned"))?;
            StreamDescriptor::from(guard.identity())
        };
        Ok(Self {
            storage,
            spool,
            descriptor,
            wake,
            lag,
            scan_interval: Duration::from_secs(5),
            max_backoff: Duration::from_secs(30),
            test_pause_file: if cfg!(debug_assertions) {
                std::env::var_os("WALRUST_TEST_NATIVE_UPLOAD_PAUSE_FILE").map(PathBuf::from)
            } else {
                None
            },
            test_crash_once_file: if cfg!(debug_assertions) {
                std::env::var_os("WALRUST_TEST_NATIVE_UPLOAD_CRASH_ONCE_FILE").map(PathBuf::from)
            } else {
                None
            },
        })
    }

    /// Run until cancellation. Remote errors are lag state, not watcher-fatal.
    pub async fn run(self, mut shutdown: tokio::sync::watch::Receiver<bool>) {
        let mut backoff = Duration::from_millis(100);
        loop {
            if *shutdown.borrow() {
                return;
            }
            if self
                .test_crash_once_file
                .as_ref()
                .is_some_and(|path| path.exists())
            {
                if let Some(path) = &self.test_crash_once_file {
                    let _ = std::fs::remove_file(path);
                }
                panic!("test-only native uploader crash");
            }
            if self
                .test_pause_file
                .as_ref()
                .is_some_and(|path| path.exists())
            {
                self.refresh_lag(Some("test uploader pause is active".to_string()));
                tokio::select! {
                    _ = tokio::time::sleep(Duration::from_millis(50)) => {}
                    _ = self.wake.notify.notified() => {}
                    _ = shutdown.changed() => {}
                }
                continue;
            }
            match self.publish_pending_once().await {
                Ok(progress) => {
                    backoff = Duration::from_millis(100);
                    self.refresh_lag(None);
                    if progress {
                        continue;
                    }
                }
                Err(error) => {
                    tracing::error!(error = %error, "remote_lag: native spool publication failed; retaining local objects and retrying");
                    self.refresh_lag(Some(format!("{error:#}")));
                    tokio::select! {
                        _ = tokio::time::sleep(backoff) => {}
                        _ = self.wake.notify.notified() => {}
                        _ = shutdown.changed() => {}
                    }
                    backoff = (backoff * 2).min(self.max_backoff);
                    continue;
                }
            }

            tokio::select! {
                _ = tokio::time::sleep(self.scan_interval) => {}
                _ = self.wake.notify.notified() => {}
                _ = shutdown.changed() => {}
            }
        }
    }

    /// Publish at most one contiguous object. This deliberately serializes the
    /// visible chain even if object transfer is parallelized in the future.
    pub async fn publish_pending_once(&self) -> Result<bool> {
        let (object, payload, previous_publish_sha256, previous_object, retained_base) = {
            let guard = self
                .spool
                .lock()
                .map_err(|_| anyhow!("native spool lock poisoned"))?;
            let next = match guard.remote_published_seq() {
                Some(seq) => seq
                    .checked_add(1)
                    .ok_or_else(|| anyhow!("publish seq overflow"))?,
                None => guard.identity().first_native_seq,
            };
            let Some(object) = guard.get(next).cloned() else {
                return Ok(false);
            };
            let previous_publish_sha256 = if next == guard.identity().first_native_seq {
                None
            } else {
                guard
                    .get(next - 1)
                    .and_then(|o| o.publish_record_sha256.clone())
                    .ok_or_else(|| {
                        anyhow!(
                            "local published predecessor record missing at seq {}",
                            next - 1
                        )
                    })?
                    .into()
            };
            let previous_object = if next == guard.identity().first_native_seq {
                None
            } else {
                Some(
                    guard
                        .get(next - 1)
                        .cloned()
                        .ok_or_else(|| anyhow!("local native predecessor object is missing"))?,
                )
            };
            let retained_base = if previous_object.is_some() {
                Some(
                    guard
                        .objects()
                        .next()
                        .filter(|object| object.kind == ObjectKind::Snapshot)
                        .cloned()
                        .ok_or_else(|| anyhow!("local native retained snapshot base is missing"))?,
                )
            } else {
                None
            };
            let payload = guard.read_payload(next)?;
            (
                object,
                payload,
                previous_publish_sha256,
                previous_object,
                retained_base,
            )
        };

        self.ensure_descriptor().await?;
        let upload_started = std::time::Instant::now();
        self.ensure_remote_object(&object, &payload).await?;
        durability_failpoint("remote_put_verified");
        if let Ok(mut lag) = self.lag.lock() {
            lag.last_upload_duration_ms = upload_started
                .elapsed()
                .as_millis()
                .min(u128::from(u64::MAX)) as u64;
        }
        tracing::info!(
            database = %object.database,
            seq = object.seq,
            bytes = payload.len(),
            remote_upload_ms = upload_started.elapsed().as_millis() as u64,
            "uploaded exact native HADBP spool object"
        );
        {
            let mut guard = self
                .spool
                .lock()
                .map_err(|_| anyhow!("native spool lock poisoned"))?;
            if guard
                .get(object.seq)
                .map(|o| o.remote_upload_state == RemoteUploadState::Pending)
                .unwrap_or(false)
            {
                guard.mark_uploaded(object.seq)?;
                durability_failpoint("uploaded_state_committed");
            }
        }

        let record = PublishRecord::from_object(&object, previous_publish_sha256);
        let record_bytes = record.bytes()?;
        self.verify_predecessor(&record, previous_object.as_ref(), retained_base.as_ref())
            .await?;
        put_immutable_exact(
            self.storage.as_ref(),
            &record.key(&self.descriptor.prefix, &self.descriptor.database),
            &record_bytes,
            "native publish record",
        )
        .await?;
        durability_failpoint("publish_record_committed");

        let mut guard = self
            .spool
            .lock()
            .map_err(|_| anyhow!("native spool lock poisoned"))?;
        guard.mark_published(object.seq, &record_bytes)?;
        durability_failpoint("published_state_committed");
        Ok(true)
    }

    async fn ensure_descriptor(&self) -> Result<()> {
        let bytes = self.descriptor.bytes()?;
        put_immutable_exact(
            self.storage.as_ref(),
            &self.descriptor.key(),
            &bytes,
            "native stream descriptor",
        )
        .await
    }

    async fn ensure_remote_object(&self, object: &SpoolObject, payload: &[u8]) -> Result<()> {
        if object.intended_remote_key
            != object_key(
                &self
                    .spool
                    .lock()
                    .map_err(|_| anyhow!("native spool lock poisoned"))?
                    .identity()
                    .clone(),
                object.kind,
                object.seq,
            )
        {
            bail!(
                "native object intended key is not canonical at seq {}",
                object.seq
            );
        }
        put_immutable_exact(
            self.storage.as_ref(),
            &object.intended_remote_key,
            payload,
            "native HADBP object",
        )
        .await
    }

    async fn verify_predecessor(
        &self,
        record: &PublishRecord,
        previous_object: Option<&SpoolObject>,
        retained_base: Option<&SpoolObject>,
    ) -> Result<()> {
        if record.seq == self.descriptor.first_native_seq {
            if record.kind != ObjectKind::Snapshot
                || record.previous_publish_sha256.is_some()
                || previous_object.is_some()
                || retained_base.is_some()
            {
                bail!("native remote chain must start with its declared snapshot base");
            }
            return Ok(());
        }
        let previous_seq = record.seq - 1;
        let previous_object = previous_object.ok_or_else(|| {
            anyhow!("local native predecessor object is missing at seq {previous_seq}")
        })?;
        if previous_object.seq != previous_seq {
            bail!("local native predecessor sequence differs from {previous_seq}");
        }
        let previous_key = format!(
            "{}{}/native/v1/lineages/{}/published/{previous_seq:016x}.json",
            self.descriptor.prefix, self.descriptor.database, self.descriptor.lineage_id
        );
        let bytes = self.storage.get(&previous_key).await?.ok_or_else(|| {
            anyhow!("remote native predecessor record is absent at seq {previous_seq}")
        })?;
        let digest = sha256_hex(&bytes);
        if record.previous_publish_sha256.as_deref() != Some(digest.as_str()) {
            bail!(
                "split brain: remote native predecessor digest changed at seq {}",
                previous_seq
            );
        }
        let previous: PublishRecord = serde_json::from_slice(&bytes)
            .context("decode remote native predecessor publish record")?;
        if previous.seq != previous_seq
            || previous.lineage_id != record.lineage_id
            || previous.ending_chain_checksum != record.previous_chain_checksum
        {
            bail!("split brain: incompatible remote native predecessor at seq {previous_seq}");
        }
        self.verify_remote_published_object(previous_object).await?;
        let retained_base = retained_base
            .ok_or_else(|| anyhow!("local native retained snapshot base is missing"))?;
        if retained_base.kind != ObjectKind::Snapshot {
            bail!("local native retained base is not a snapshot");
        }
        if retained_base.seq != previous_seq {
            self.verify_remote_published_object(retained_base).await?;
        }
        Ok(())
    }

    async fn verify_remote_published_object(&self, object: &SpoolObject) -> Result<()> {
        let key = format!(
            "{}{}/native/v1/lineages/{}/published/{:016x}.json",
            self.descriptor.prefix,
            self.descriptor.database,
            self.descriptor.lineage_id,
            object.seq
        );
        let bytes = self.storage.get(&key).await?.ok_or_else(|| {
            anyhow!(
                "remote native published record is absent at seq {}",
                object.seq
            )
        })?;
        let record: PublishRecord = serde_json::from_slice(&bytes).with_context(|| {
            format!("decode remote native publish record at seq {}", object.seq)
        })?;
        let expected_digest = object.publish_record_sha256.as_deref().ok_or_else(|| {
            anyhow!(
                "local native published object {} has no record digest",
                object.seq
            )
        })?;
        if sha256_hex(&bytes) != expected_digest {
            bail!(
                "split brain: remote native publish record digest changed at seq {}",
                object.seq
            );
        }
        if record != PublishRecord::from_object(object, record.previous_publish_sha256.clone()) {
            bail!(
                "split brain: remote native publish record differs from local object at seq {}",
                object.seq
            );
        }
        crate::native_restore::get_verified_object(
            self.storage.as_ref(),
            &self.descriptor,
            &record,
        )
        .await?;
        Ok(())
    }

    fn refresh_lag(&self, last_error: Option<String>) {
        let now = unix_ms();
        let snapshot = self.spool.lock().ok().map(|guard| {
            let mut count = 0u64;
            let mut bytes = 0u64;
            let mut oldest = now;
            for object in guard.pending_objects() {
                count += 1;
                bytes = bytes.saturating_add(object.payload_length);
                oldest = oldest.min(object.created_unix_ms);
            }
            (count, bytes, now.saturating_sub(oldest))
        });
        if let (Some((pending_objects, pending_bytes, oldest_age_ms)), Ok(mut lag)) =
            (snapshot, self.lag.lock())
        {
            let last_upload_duration_ms = lag.last_upload_duration_ms;
            *lag = RemoteLagState {
                pending_objects,
                pending_bytes,
                oldest_age_ms: if pending_objects == 0 {
                    0
                } else {
                    oldest_age_ms
                },
                last_error,
                last_upload_duration_ms,
            };
        }
    }
}

async fn put_immutable_exact(
    storage: &dyn StorageBackend,
    key: &str,
    bytes: &[u8],
    label: &str,
) -> Result<()> {
    let cas = storage.put_if_absent(key, bytes).await?;
    if cas.success {
        return Ok(());
    }
    let existing = storage
        .get(key)
        .await?
        .ok_or_else(|| anyhow!("{label} vanished after create-if-absent conflict at {key}"))?;
    if existing != bytes {
        bail!("split brain/equivocation: divergent {label} already exists at {key}");
    }
    Ok(())
}

fn sha256_hex(bytes: &[u8]) -> String {
    let digest = Sha256::digest(bytes);
    let mut out = String::with_capacity(64);
    for byte in digest {
        use std::fmt::Write as _;
        let _ = write!(&mut out, "{byte:02x}");
    }
    out
}

fn unix_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ltx;
    use crate::native_spool::{CapacityPolicy, SourceCursor, StageObject};
    use async_trait::async_trait;
    use hadb_storage::CasResult;
    use std::collections::BTreeMap;
    use std::path::Path;
    use tempfile::tempdir;

    #[derive(Default)]
    struct MemoryStorage(Mutex<BTreeMap<String, Vec<u8>>>);

    #[async_trait]
    impl StorageBackend for MemoryStorage {
        async fn get(&self, key: &str) -> Result<Option<Vec<u8>>> {
            Ok(self.0.lock().unwrap().get(key).cloned())
        }
        async fn put(&self, key: &str, data: &[u8]) -> Result<()> {
            self.0.lock().unwrap().insert(key.into(), data.into());
            Ok(())
        }
        async fn delete(&self, key: &str) -> Result<()> {
            self.0.lock().unwrap().remove(key);
            Ok(())
        }
        async fn list(&self, prefix: &str, _after: Option<&str>) -> Result<Vec<String>> {
            Ok(self
                .0
                .lock()
                .unwrap()
                .keys()
                .filter(|k| k.starts_with(prefix))
                .cloned()
                .collect())
        }
        async fn put_if_absent(&self, key: &str, data: &[u8]) -> Result<CasResult> {
            let mut map = self.0.lock().unwrap();
            if map.contains_key(key) {
                Ok(CasResult {
                    success: false,
                    etag: None,
                })
            } else {
                map.insert(key.into(), data.into());
                Ok(CasResult {
                    success: true,
                    etag: Some("1".into()),
                })
            }
        }
        async fn put_if_match(&self, _key: &str, _data: &[u8], _etag: &str) -> Result<CasResult> {
            bail!("unused")
        }
    }

    fn staged_spool(dir: &Path) -> Arc<Mutex<NativeSpool>> {
        let db = dir.join("db.sqlite");
        let conn = rusqlite::Connection::open(&db).unwrap();
        conn.execute_batch("CREATE TABLE t(id INTEGER PRIMARY KEY); INSERT INTO t DEFAULT VALUES;")
            .unwrap();
        let page_size = conn
            .query_row("PRAGMA page_size", [], |r| r.get::<_, u32>(0))
            .unwrap();
        drop(conn);
        let identity =
            SpoolIdentity::new(&db, "bucket", "p/", "db", "lineage", 1, None, true).unwrap();
        let encoded = ltx::encode_snapshot_with_checksum(&db, page_size, 1, 0).unwrap();
        let pages = std::fs::metadata(&db).unwrap().len() / page_size as u64;
        let root = NativeSpool::path_for(dir, &identity);
        let mut spool = NativeSpool::create_or_open(
            &root,
            identity.clone(),
            CapacityPolicy {
                warning_bytes: u64::MAX - 1,
                hard_bytes: u64::MAX,
                minimum_free_bytes: 0,
            },
        )
        .unwrap();
        spool
            .stage(StageObject {
                seq: 1,
                kind: ObjectKind::Snapshot,
                previous_chain_checksum: 0,
                ending_chain_checksum: encoded.checksum,
                end_page_count: pages,
                intended_remote_key: object_key(&identity, ObjectKind::Snapshot, 1),
                source_cursor: SourceCursor::snapshot(),
                payload: &encoded.bytes,
            })
            .unwrap();
        Arc::new(Mutex::new(spool))
    }

    #[tokio::test]
    async fn publishes_object_then_contiguous_record_and_is_idempotent() {
        let dir = tempdir().unwrap();
        let spool = staged_spool(dir.path());
        let storage = Arc::new(MemoryStorage::default());
        let (uploader, _wake, _lag) = NativeUploader::new(storage.clone(), spool.clone()).unwrap();
        assert!(uploader.publish_pending_once().await.unwrap());
        assert_eq!(spool.lock().unwrap().remote_published_seq(), Some(1));
        assert!(!uploader.publish_pending_once().await.unwrap());
        let keys = storage.list("p/db/native/v1/", None).await.unwrap();
        assert_eq!(keys.iter().filter(|k| k.ends_with(".hadbp")).count(), 1);
        assert_eq!(keys.iter().filter(|k| k.contains("/published/")).count(), 1);
    }

    #[tokio::test]
    async fn restart_adopts_exact_remote_put_and_publish_before_local_state_commit() {
        let dir = tempdir().unwrap();
        let spool = staged_spool(dir.path());
        let storage = Arc::new(MemoryStorage::default());
        let (descriptor, object, payload) = {
            let guard = spool.lock().unwrap();
            (
                StreamDescriptor::from(guard.identity()),
                guard.get(1).unwrap().clone(),
                guard.read_payload(1).unwrap(),
            )
        };
        let record = PublishRecord::from_object(&object, None);
        storage
            .put(&descriptor.key(), &descriptor.bytes().unwrap())
            .await
            .unwrap();
        storage
            .put(&object.intended_remote_key, &payload)
            .await
            .unwrap();
        storage
            .put(
                &record.key(&descriptor.prefix, &descriptor.database),
                &record.bytes().unwrap(),
            )
            .await
            .unwrap();

        // Simulates SIGKILL after both remote immutable writes but before
        // either matching local uploaded/published journal commit.
        assert_eq!(spool.lock().unwrap().remote_published_seq(), None);
        let (uploader, _wake, _lag) = NativeUploader::new(storage.clone(), spool.clone()).unwrap();
        assert!(uploader.publish_pending_once().await.unwrap());
        assert_eq!(spool.lock().unwrap().remote_published_seq(), Some(1));
        assert_eq!(
            storage.get(&object.intended_remote_key).await.unwrap(),
            Some(payload)
        );
    }

    #[tokio::test]
    async fn divergent_existing_object_is_never_overwritten() {
        let dir = tempdir().unwrap();
        let spool = staged_spool(dir.path());
        let storage = Arc::new(MemoryStorage::default());
        let key = spool
            .lock()
            .unwrap()
            .get(1)
            .unwrap()
            .intended_remote_key
            .clone();
        storage.put(&key, b"divergent").await.unwrap();
        let (uploader, _wake, _lag) = NativeUploader::new(storage.clone(), spool).unwrap();
        let err = uploader.publish_pending_once().await.unwrap_err();
        assert!(err.to_string().contains("split brain/equivocation"));
        assert_eq!(storage.get(&key).await.unwrap().unwrap(), b"divergent");
    }

    #[tokio::test]
    async fn missing_remote_snapshot_base_blocks_descendant_publication() {
        let dir = tempdir().unwrap();
        let spool = staged_spool(dir.path());
        let storage = Arc::new(MemoryStorage::default());
        let (uploader, _wake, _lag) = NativeUploader::new(storage.clone(), spool.clone()).unwrap();
        assert!(uploader.publish_pending_once().await.unwrap());

        let (identity, previous, base_key) = {
            let guard = spool.lock().unwrap();
            let base = guard.get(1).unwrap();
            (
                guard.identity().clone(),
                base.ending_chain_checksum,
                base.intended_remote_key.clone(),
            )
        };
        let (payload, ending) = ltx::encode_wal_changes_with_end_page_count(
            &[(1, vec![0u8; 4096])],
            4096,
            2,
            previous,
            1,
        )
        .unwrap();
        spool
            .lock()
            .unwrap()
            .stage(StageObject {
                seq: 2,
                kind: ObjectKind::Delta,
                previous_chain_checksum: previous,
                ending_chain_checksum: ending,
                end_page_count: 1,
                intended_remote_key: object_key(&identity, ObjectKind::Delta, 2),
                source_cursor: SourceCursor::snapshot(),
                payload: &payload,
            })
            .unwrap();

        storage.delete(&base_key).await.unwrap();
        let error = uploader.publish_pending_once().await.unwrap_err();
        assert!(
            format!("{error:#}").contains("published native object is missing"),
            "unexpected publish error: {error:#}"
        );
        assert_eq!(spool.lock().unwrap().remote_published_seq(), Some(1));
        let descendant_record = format!(
            "p/db/native/v1/lineages/{}/published/0000000000000002.json",
            identity.lineage_id
        );
        assert!(
            storage.get(&descendant_record).await.unwrap().is_none(),
            "descendant publish record must remain invisible without its snapshot base"
        );
    }

    #[tokio::test]
    async fn full_or_closed_wake_channel_never_blocks() {
        let dir = tempdir().unwrap();
        let spool = staged_spool(dir.path());
        let storage = Arc::new(MemoryStorage::default());
        let (uploader, wake, _lag) = NativeUploader::new(storage, spool).unwrap();
        wake.notify();
        wake.notify(); // full, coalesced
        drop(uploader); // receiver closed
        wake.notify();
    }

    #[tokio::test]
    async fn published_native_snapshot_restores_row_exact_and_integrity_clean() {
        let dir = tempdir().unwrap();
        let spool = staged_spool(dir.path());
        let storage = Arc::new(MemoryStorage::default());
        let (uploader, _wake, _lag) = NativeUploader::new(storage.clone(), spool).unwrap();
        uploader.publish_pending_once().await.unwrap();
        let output = dir.path().join("restored.sqlite");
        let result = crate::native_restore::restore_native_v1(
            storage.as_ref(),
            "bucket",
            "p/",
            "db",
            &output,
            None,
        )
        .await
        .unwrap();
        assert_eq!(
            result,
            crate::native_restore::NativeRestoreAvailability::Restored { seq: 1 }
        );
        let conn = rusqlite::Connection::open(output).unwrap();
        let count: i64 = conn
            .query_row("SELECT count(*) FROM t", [], |row| row.get(0))
            .unwrap();
        let integrity: String = conn
            .query_row("PRAGMA integrity_check", [], |row| row.get(0))
            .unwrap();
        assert_eq!(count, 1);
        assert_eq!(integrity, "ok");
    }

    #[tokio::test]
    async fn published_record_with_missing_or_corrupt_payload_is_not_visible() {
        let dir = tempdir().unwrap();
        let spool = staged_spool(dir.path());
        let storage = Arc::new(MemoryStorage::default());
        let (uploader, _wake, _lag) = NativeUploader::new(storage.clone(), spool.clone()).unwrap();
        uploader.publish_pending_once().await.unwrap();
        let object_key = spool
            .lock()
            .unwrap()
            .get(1)
            .unwrap()
            .intended_remote_key
            .clone();

        storage.delete(&object_key).await.unwrap();
        let missing =
            crate::native_restore::inspect_native_v1(storage.as_ref(), "bucket", "p/", "db")
                .await
                .unwrap_err();
        assert!(missing
            .to_string()
            .contains("published native object is missing"));

        storage.put(&object_key, b"divergent").await.unwrap();
        let corrupt =
            crate::native_restore::inspect_native_v1(storage.as_ref(), "bucket", "p/", "db")
                .await
                .unwrap_err();
        assert!(corrupt.to_string().contains("length/digest mismatch"));
    }

    #[tokio::test]
    async fn descriptor_bucket_must_match_the_actual_cli_destination() {
        let dir = tempdir().unwrap();
        let spool = staged_spool(dir.path());
        let storage = Arc::new(MemoryStorage::default());
        let (uploader, _wake, _lag) = NativeUploader::new(storage.clone(), spool).unwrap();
        uploader.publish_pending_once().await.unwrap();
        let error = crate::native_restore::inspect_native_v1(
            storage.as_ref(),
            "different-bucket",
            "p/",
            "db",
        )
        .await
        .unwrap_err();
        assert!(error
            .to_string()
            .contains("descriptor identity/layout mismatch"));
    }

    #[tokio::test]
    async fn published_native_snapshot_and_delta_restore_latest_and_pitr() {
        let dir = tempdir().unwrap();
        let spool = staged_spool(dir.path());
        let db = dir.path().join("db.sqlite");
        let conn = rusqlite::Connection::open(&db).unwrap();
        conn.execute("INSERT INTO t DEFAULT VALUES", []).unwrap();
        let page_size = conn
            .query_row("PRAGMA page_size", [], |row| row.get::<_, u32>(0))
            .unwrap();
        drop(conn);
        let db_bytes = std::fs::read(&db).unwrap();
        let pages = db_bytes
            .chunks_exact(page_size as usize)
            .enumerate()
            .map(|(index, page)| ((index + 1) as u32, page.to_vec()))
            .collect::<Vec<_>>();
        let (identity, previous) = {
            let guard = spool.lock().unwrap();
            (
                guard.identity().clone(),
                guard.get(1).unwrap().ending_chain_checksum,
            )
        };
        let (delta, ending) = ltx::encode_wal_changes_with_end_page_count(
            &pages,
            page_size,
            2,
            previous,
            pages.len() as u64,
        )
        .unwrap();
        spool
            .lock()
            .unwrap()
            .stage(StageObject {
                seq: 2,
                kind: ObjectKind::Delta,
                previous_chain_checksum: previous,
                ending_chain_checksum: ending,
                end_page_count: pages.len() as u64,
                intended_remote_key: object_key(&identity, ObjectKind::Delta, 2),
                source_cursor: SourceCursor::snapshot(),
                payload: &delta,
            })
            .unwrap();

        let storage = Arc::new(MemoryStorage::default());
        let (uploader, _wake, _lag) = NativeUploader::new(storage.clone(), spool.clone()).unwrap();
        assert!(uploader.publish_pending_once().await.unwrap());
        assert!(uploader.publish_pending_once().await.unwrap());
        assert_eq!(
            crate::native_restore::verify_native_v1(storage.as_ref(), "bucket", "p/", "db")
                .await
                .unwrap(),
            Some(2)
        );

        for (target, expected_count) in [(Some(1), 1i64), (None, 2i64)] {
            let output = dir.path().join(format!(
                "restore-{}.sqlite",
                target
                    .map(|seq| seq.to_string())
                    .unwrap_or_else(|| "head".into())
            ));
            let result = crate::native_restore::restore_native_v1(
                storage.as_ref(),
                "bucket",
                "p/",
                "db",
                &output,
                target,
            )
            .await
            .unwrap();
            assert_eq!(
                result,
                crate::native_restore::NativeRestoreAvailability::Restored {
                    seq: target.unwrap_or(2)
                }
            );
            let restored = rusqlite::Connection::open(output).unwrap();
            assert_eq!(
                restored
                    .query_row("SELECT count(*) FROM t", [], |row| row.get::<_, i64>(0))
                    .unwrap(),
                expected_count
            );
            assert_eq!(
                restored
                    .query_row("PRAGMA integrity_check", [], |row| row.get::<_, String>(0))
                    .unwrap(),
                "ok"
            );
        }
    }

    #[tokio::test]
    async fn native_retention_floor_prunes_only_below_verified_snapshot() {
        let dir = tempdir().unwrap();
        let spool = staged_spool(dir.path());
        let db = dir.path().join("db.sqlite");
        let conn = rusqlite::Connection::open(&db).unwrap();
        conn.execute("INSERT INTO t DEFAULT VALUES", []).unwrap();
        let page_size = conn
            .query_row("PRAGMA page_size", [], |row| row.get::<_, u32>(0))
            .unwrap();
        drop(conn);
        let (identity, previous) = {
            let guard = spool.lock().unwrap();
            (
                guard.identity().clone(),
                guard.get(1).unwrap().ending_chain_checksum,
            )
        };
        let delta_pages = std::fs::read(&db)
            .unwrap()
            .chunks_exact(page_size as usize)
            .enumerate()
            .map(|(index, page)| ((index + 1) as u32, page.to_vec()))
            .collect::<Vec<_>>();
        let (delta, delta_end) = ltx::encode_wal_changes_with_end_page_count(
            &delta_pages,
            page_size,
            2,
            previous,
            delta_pages.len() as u64,
        )
        .unwrap();
        spool
            .lock()
            .unwrap()
            .stage(StageObject {
                seq: 2,
                kind: ObjectKind::Delta,
                previous_chain_checksum: previous,
                ending_chain_checksum: delta_end,
                end_page_count: delta_pages.len() as u64,
                intended_remote_key: object_key(&identity, ObjectKind::Delta, 2),
                source_cursor: SourceCursor::snapshot(),
                payload: &delta,
            })
            .unwrap();
        let snapshot = ltx::encode_snapshot_with_checksum(&db, page_size, 3, delta_end).unwrap();
        let snapshot_pages = std::fs::metadata(&db).unwrap().len() / page_size as u64;
        spool
            .lock()
            .unwrap()
            .stage(StageObject {
                seq: 3,
                kind: ObjectKind::Snapshot,
                previous_chain_checksum: delta_end,
                ending_chain_checksum: snapshot.checksum,
                end_page_count: snapshot_pages,
                intended_remote_key: object_key(&identity, ObjectKind::Snapshot, 3),
                source_cursor: SourceCursor::snapshot(),
                payload: &snapshot.bytes,
            })
            .unwrap();
        let storage = Arc::new(MemoryStorage::default());
        let (uploader, _wake, _lag) = NativeUploader::new(storage.clone(), spool).unwrap();
        assert!(uploader.publish_pending_once().await.unwrap());
        assert!(uploader.publish_pending_once().await.unwrap());
        assert!(uploader.publish_pending_once().await.unwrap());

        let outcome = crate::native_restore::prune_native_before_snapshot(
            storage.as_ref(),
            "bucket",
            "p/",
            "db",
            3,
        )
        .await
        .unwrap();
        assert_eq!(outcome.deleted_objects, 2);
        let visible =
            crate::native_restore::inspect_native_v1(storage.as_ref(), "bucket", "p/", "db")
                .await
                .unwrap()
                .unwrap();
        assert_eq!(visible.head_seq, 3);
        assert_eq!(visible.retention_floor_seq, 3);
        assert_eq!(visible.snapshot_seqs, vec![3]);

        let expired = crate::native_restore::restore_native_v1(
            storage.as_ref(),
            "bucket",
            "p/",
            "db",
            &dir.path().join("expired.sqlite"),
            Some(2),
        )
        .await
        .unwrap_err();
        assert!(expired.to_string().contains("intentionally expired"));
    }

    #[tokio::test]
    async fn offline_reconnect_divergent_remote_head_is_rejected_and_retained() {
        let dir = tempdir().unwrap();
        let spool = staged_spool(dir.path());
        let storage = Arc::new(MemoryStorage::default());
        let (uploader, _wake, _lag) = NativeUploader::new(storage.clone(), spool.clone()).unwrap();
        uploader.publish_pending_once().await.unwrap();

        // This object is staged locally while the remote is notionally offline.
        let (identity, previous) = {
            let guard = spool.lock().unwrap();
            (
                guard.identity().clone(),
                guard.get(1).unwrap().ending_chain_checksum,
            )
        };
        let (payload, ending) = ltx::encode_wal_changes_with_end_page_count(
            &[(1, vec![0u8; 4096])],
            4096,
            2,
            previous,
            1,
        )
        .unwrap();
        spool
            .lock()
            .unwrap()
            .stage(StageObject {
                seq: 2,
                kind: ObjectKind::Delta,
                previous_chain_checksum: previous,
                ending_chain_checksum: ending,
                end_page_count: 1,
                intended_remote_key: object_key(&identity, ObjectKind::Delta, 2),
                source_cursor: SourceCursor::snapshot(),
                payload: &payload,
            })
            .unwrap();

        // A second writer advances the same remote sequence incompatibly.
        let marker_key = format!(
            "p/db/native/v1/lineages/{}/published/{:016x}.json",
            identity.lineage_id, 2
        );
        storage
            .put(&marker_key, b"divergent-writer-head")
            .await
            .unwrap();
        let error = uploader.publish_pending_once().await.unwrap_err();
        assert!(error.to_string().contains("split brain/equivocation"));
        let guard = spool.lock().unwrap();
        assert_eq!(guard.remote_published_seq(), Some(1));
        assert!(
            guard.get(2).is_some(),
            "conflicting offline descendant must be retained"
        );
        assert_ne!(
            guard.get(2).unwrap().remote_upload_state,
            RemoteUploadState::Published
        );
    }
}
