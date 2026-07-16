//! Durable local-first spool for native HADBP CLI shadow-watch streams.
//!
//! This is deliberately independent of `legacy_cache::LocalCache`: that cache
//! stores Litestream-heritage LTX bytes and keys objects by legacy TXID.  A
//! native spool entry is an immutable HADBP payload plus a journal record that
//! binds its source cursor, lineage, destination, checksums, and remote key.

use crate::ltx;
use anyhow::{anyhow, bail, Context, Result};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::cell::Cell;
use std::collections::BTreeMap;
use std::fs::{self, File, OpenOptions};
use std::io::Write;
#[cfg(unix)]
use std::os::fd::AsRawFd;
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

pub const SPOOL_VERSION: u32 = 1;
const JOURNAL_VERSION: u32 = 2;

pub fn durability_failpoint(name: &str) {
    if !cfg!(debug_assertions)
        || std::env::var("WALRUST_TEST_DURABILITY_FAILPOINT").as_deref() != Ok(name)
    {
        return;
    }
    let marker = std::env::var_os("WALRUST_TEST_DURABILITY_FAILPOINT_MARKER")
        .map(PathBuf::from)
        .expect("durability failpoint requires a marker path");
    let mut file = File::create(&marker).expect("create durability failpoint marker");
    file.write_all(name.as_bytes())
        .expect("write durability failpoint marker");
    file.sync_all().expect("fsync durability failpoint marker");
    sync_dir(marker.parent().unwrap_or_else(|| Path::new(".")))
        .expect("fsync durability failpoint marker directory");
    loop {
        std::thread::park_timeout(std::time::Duration::from_secs(1));
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ObjectKind {
    Snapshot,
    Delta,
}

impl ObjectKind {
    fn generation(self) -> u32 {
        match self {
            Self::Snapshot => 1,
            Self::Delta => 0,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SourceCursor {
    pub shadow_generation: u64,
    pub shadow_frame_index: u64,
    pub wal_offset: u64,
    pub wal_salt: Option<(u32, u32)>,
    pub wal_checksum_chain: Option<(u32, u32)>,
}

impl SourceCursor {
    pub fn snapshot() -> Self {
        Self {
            shadow_generation: 0,
            shadow_frame_index: 0,
            wal_offset: 0,
            wal_salt: None,
            wal_checksum_chain: None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SpoolIdentity {
    pub version: u32,
    pub canonical_db_path: String,
    pub bucket: String,
    pub prefix: String,
    pub database: String,
    pub lineage_id: String,
    pub first_native_seq: u64,
    pub legacy_boundary_txid: Option<u64>,
    /// True only after startup successfully verified the remote base/absence.
    pub remote_base_verified: bool,
}

impl SpoolIdentity {
    pub fn new(
        db_path: &Path,
        bucket: impl Into<String>,
        prefix: impl Into<String>,
        database: impl Into<String>,
        lineage_id: impl Into<String>,
        first_native_seq: u64,
        legacy_boundary_txid: Option<u64>,
        remote_base_verified: bool,
    ) -> Result<Self> {
        let canonical = fs::canonicalize(db_path)
            .with_context(|| format!("canonicalize spool database path {}", db_path.display()))?;
        Ok(Self {
            version: SPOOL_VERSION,
            canonical_db_path: canonical.to_string_lossy().into_owned(),
            bucket: bucket.into(),
            prefix: prefix.into(),
            database: database.into(),
            lineage_id: lineage_id.into(),
            first_native_seq,
            legacy_boundary_txid,
            remote_base_verified,
        })
    }

    pub fn stream_digest(&self) -> String {
        let mut h = Sha256::new();
        h.update(self.version.to_be_bytes());
        for value in [
            self.bucket.as_str(),
            self.prefix.as_str(),
            self.database.as_str(),
            self.lineage_id.as_str(),
        ] {
            h.update((value.len() as u64).to_be_bytes());
            h.update(value.as_bytes());
        }
        h.update(self.first_native_seq.to_be_bytes());
        h.update(self.legacy_boundary_txid.unwrap_or(0).to_be_bytes());
        hex_digest(h.finalize().as_slice())
    }

    fn legacy_local_path_digest(&self) -> String {
        let mut h = Sha256::new();
        h.update(b"walrust-native-spool-path-v1");
        h.update(self.canonical_db_path.as_bytes());
        h.update(self.bucket.as_bytes());
        h.update(self.prefix.as_bytes());
        h.update(self.database.as_bytes());
        hex_digest(h.finalize().as_slice())
    }

    fn local_path_digest(&self) -> String {
        let mut h = Sha256::new();
        h.update(b"walrust-native-spool-path-v2");
        for value in [
            self.canonical_db_path.as_str(),
            self.bucket.as_str(),
            self.prefix.as_str(),
            self.database.as_str(),
        ] {
            h.update((value.len() as u64).to_be_bytes());
            h.update(value.as_bytes());
        }
        hex_digest(h.finalize().as_slice())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum LocalCreationState {
    Installed,
    Deleting,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RemoteUploadState {
    Pending,
    Uploaded,
    Published,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SpoolObject {
    pub version: u32,
    pub stream_digest: String,
    pub lineage_id: String,
    pub bucket: String,
    pub prefix: String,
    pub database: String,
    pub seq: u64,
    pub kind: ObjectKind,
    pub previous_chain_checksum: u64,
    pub ending_chain_checksum: u64,
    pub end_page_count: u64,
    pub intended_remote_key: String,
    pub payload_length: u64,
    pub payload_sha256: String,
    pub source_cursor: SourceCursor,
    pub local_creation_state: LocalCreationState,
    pub remote_upload_state: RemoteUploadState,
    pub created_unix_ms: u64,
    pub uploaded_unix_ms: Option<u64>,
    pub published_unix_ms: Option<u64>,
    pub publish_record_sha256: Option<String>,
}

impl SpoolObject {
    pub fn payload_file_name(&self) -> String {
        format!("{:04x}-{:016x}.hadbp", self.kind.generation(), self.seq)
    }
}

#[derive(Debug, Clone)]
pub struct StageObject<'a> {
    pub seq: u64,
    pub kind: ObjectKind,
    pub previous_chain_checksum: u64,
    pub ending_chain_checksum: u64,
    pub end_page_count: u64,
    pub intended_remote_key: String,
    pub source_cursor: SourceCursor,
    pub payload: &'a [u8],
}

#[derive(Debug, Clone, Copy)]
pub struct CapacityPolicy {
    pub warning_bytes: u64,
    pub hard_bytes: u64,
    pub minimum_free_bytes: u64,
}

impl Default for CapacityPolicy {
    fn default() -> Self {
        Self {
            warning_bytes: 8 * 1024 * 1024 * 1024,
            hard_bytes: 10 * 1024 * 1024 * 1024,
            minimum_free_bytes: 1024 * 1024 * 1024,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CapacityState {
    Healthy,
    High,
    Full,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Journal {
    version: u32,
    identity: SpoolIdentity,
    objects: BTreeMap<u64, SpoolObject>,
    local_base_seq: u64,
    admitted_seq: Option<u64>,
    checkpointed_seq: Option<u64>,
    #[serde(default)]
    checkpointed_source_cursor: Option<SourceCursor>,
    remote_published_seq: Option<u64>,
    checkpoint_window: CheckpointWindow,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "state", rename_all = "snake_case")]
pub enum CheckpointWindow {
    Closed,
    Opening {
        seq: u64,
        source_cursor: SourceCursor,
    },
    RearmedDirty {
        seq: u64,
        source_cursor: SourceCursor,
        checkpoint_completed: bool,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct JournalV1 {
    version: u32,
    identity: SpoolIdentity,
    objects: BTreeMap<u64, SpoolObject>,
    local_base_seq: u64,
    admitted_seq: Option<u64>,
    checkpointed_seq: Option<u64>,
    remote_published_seq: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RecoveryHead {
    pub seq: u64,
    pub ending_chain_checksum: u64,
    pub source_cursor: SourceCursor,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct InstallIntent {
    version: u32,
    object: SpoolObject,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "state", rename_all = "snake_case")]
pub enum SnapshotIntentState {
    Creating,
    /// Compatibility-only state written by pre-direct-snapshot PR #43 builds.
    Stable {
        payload_length: u64,
        sha256: String,
    },
    Admitted,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SnapshotIntent {
    pub version: u32,
    pub stream_digest: String,
    pub seq: u64,
    pub previous_chain_checksum: u64,
    pub intended_remote_key: String,
    pub source_cursor: SourceCursor,
    pub page_size: u32,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        alias = "stable_file_name"
    )]
    pub legacy_stable_file_name: Option<String>,
    pub state: SnapshotIntentState,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SnapshotPreparation {
    pub seq: u64,
    pub previous_chain_checksum: u64,
    pub intended_remote_key: String,
    pub source_cursor: SourceCursor,
    pub page_size: u32,
}

pub struct NativeSpool {
    root: PathBuf,
    objects_dir: PathBuf,
    intents_dir: PathBuf,
    snapshots_dir: PathBuf,
    journal_path: PathBuf,
    snapshot_intent_path: PathBuf,
    journal: Journal,
    capacity: CapacityPolicy,
    last_stage_duration_ms: u64,
    last_capacity_state: Cell<CapacityState>,
    // The advisory lock is held for this instance's lifetime. It prevents a
    // restore or diagnostic opener from running create_or_open's mutating
    // recovery while the watcher/uploader owns the spool.
    _owner_lock: File,
}

impl NativeSpool {
    /// Return the collision-safe directory for this stream below a configured
    /// spool root.  The full identity is still persisted and compared on open;
    /// the digest is namespace isolation, not the identity proof by itself.
    pub fn path_for(base: &Path, identity: &SpoolIdentity) -> PathBuf {
        base.join("native-v1").join(identity.local_path_digest())
    }

    /// Resolve the collision-safe v2 path, falling back to the original v1
    /// path only when it already exists. This keeps PR-created local spools
    /// restartable while all new streams use the unambiguous encoding.
    pub fn resolve_path_for(base: &Path, identity: &SpoolIdentity) -> Result<PathBuf> {
        let current = Self::path_for(base, identity);
        let legacy = base
            .join("native-v1")
            .join(identity.legacy_local_path_digest());
        let legacy_matches = if legacy.exists() {
            Self::read_identity(&legacy)?.is_some_and(|stored| {
                stored.canonical_db_path == identity.canonical_db_path
                    && stored.bucket == identity.bucket
                    && stored.prefix == identity.prefix
                    && stored.database == identity.database
            })
        } else {
            false
        };
        if current.exists() && legacy_matches {
            bail!(
                "both v1 and v2 native spool paths exist for the same stream: {} and {}",
                legacy.display(),
                current.display()
            );
        }
        Ok(if current.exists() || !legacy_matches {
            current
        } else {
            legacy
        })
    }

    pub fn read_identity(root: &Path) -> Result<Option<SpoolIdentity>> {
        let path = root.join("journal.json");
        let bytes = match fs::read(&path) {
            Ok(bytes) => bytes,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(e) => return Err(e).with_context(|| format!("read {}", path.display())),
        };
        #[derive(Deserialize)]
        struct IdentityEnvelope {
            version: u32,
            identity: SpoolIdentity,
        }
        let journal: IdentityEnvelope =
            serde_json::from_slice(&bytes).with_context(|| format!("parse {}", path.display()))?;
        if !matches!(journal.version, SPOOL_VERSION | JOURNAL_VERSION)
            || journal.identity.version != SPOOL_VERSION
        {
            bail!(
                "unsupported native spool journal/identity version at {}",
                path.display()
            );
        }
        Ok(Some(journal.identity))
    }

    pub fn validate_existing_complete_base(
        root: &Path,
        expected_identity: &SpoolIdentity,
    ) -> Result<bool> {
        let path = root.join("journal.json");
        let bytes = match fs::read(&path) {
            Ok(bytes) => bytes,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(false),
            Err(error) => return Err(error.into()),
        };
        let (journal, _) = load_journal(&bytes, &path)?;
        if &journal.identity != expected_identity {
            bail!("native spool identity mismatch while validating local base");
        }
        let base = match journal.objects.get(&journal.local_base_seq) {
            Some(base)
                if base.kind == ObjectKind::Snapshot
                    && base.local_creation_state == LocalCreationState::Installed =>
            {
                base
            }
            _ => {
                // cleanup_published_before_latest_snapshot first commits every
                // victim as Deleting, then removes payloads, and only in its
                // final journal commit advances local_base_seq. Preflight runs
                // before create_or_open completes that transaction, so accept
                // only this exact, deterministic interrupted-cleanup shape.
                let Some(candidate) = journal
                    .objects
                    .values()
                    .find(|object| object.local_creation_state != LocalCreationState::Deleting)
                else {
                    return Ok(false);
                };
                if candidate.kind != ObjectKind::Snapshot
                    || candidate.local_creation_state != LocalCreationState::Installed
                    || candidate.remote_upload_state != RemoteUploadState::Published
                    || candidate.seq <= journal.local_base_seq
                    || journal.objects.range(..candidate.seq).any(|(_, object)| {
                        object.local_creation_state != LocalCreationState::Deleting
                            || object.remote_upload_state != RemoteUploadState::Published
                    })
                    || journal.objects.range(candidate.seq..).any(|(_, object)| {
                        object.local_creation_state == LocalCreationState::Deleting
                    })
                {
                    return Ok(false);
                }
                candidate
            }
        };
        let payload = fs::read(root.join("objects").join(base.payload_file_name()))
            .with_context(|| format!("read local native snapshot base seq {}", base.seq))?;
        validate_payload(base, &payload, &std::env::temp_dir())?;
        Ok(true)
    }

    pub fn create_or_open(
        root: &Path,
        identity: SpoolIdentity,
        capacity: CapacityPolicy,
    ) -> Result<Self> {
        if identity.version != SPOOL_VERSION {
            bail!(
                "unsupported native spool identity version {}",
                identity.version
            );
        }
        if capacity.warning_bytes > capacity.hard_bytes {
            bail!("spool warning watermark exceeds hard capacity");
        }
        fs::create_dir_all(root)
            .with_context(|| format!("create native spool root {}", root.display()))?;
        sync_dir(root.parent().unwrap_or_else(|| Path::new(".")))?;
        let owner_lock = acquire_owner_lock(root)?;
        let objects_dir = root.join("objects");
        let intents_dir = root.join("intents");
        let snapshots_dir = root.join("snapshots");
        fs::create_dir_all(&objects_dir)?;
        fs::create_dir_all(&intents_dir)?;
        fs::create_dir_all(&snapshots_dir)?;
        sync_dir(root)?;

        let journal_path = root.join("journal.json");
        let (journal, migrated) = match fs::read(&journal_path) {
            Ok(bytes) => {
                let (journal, migrated) = load_journal(&bytes, &journal_path)?;
                if journal.identity != identity {
                    bail!(
                        "native spool identity mismatch at {}; refusing cross-stream reuse",
                        root.display()
                    );
                }
                (journal, migrated)
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                let local_base_seq = identity.first_native_seq;
                let journal = Journal {
                    version: JOURNAL_VERSION,
                    identity,
                    objects: BTreeMap::new(),
                    local_base_seq,
                    admitted_seq: None,
                    checkpointed_seq: None,
                    checkpointed_source_cursor: None,
                    remote_published_seq: None,
                    checkpoint_window: CheckpointWindow::Closed,
                };
                persist_json(root, &journal_path, &journal)?;
                (journal, false)
            }
            Err(e) => return Err(e).context("read native spool journal"),
        };

        let mut spool = Self {
            root: root.to_path_buf(),
            objects_dir,
            intents_dir,
            snapshots_dir,
            journal_path,
            snapshot_intent_path: root.join("snapshot-intent.json"),
            journal,
            capacity,
            last_stage_duration_ms: 0,
            last_capacity_state: Cell::new(CapacityState::Healthy),
            _owner_lock: owner_lock,
        };
        if migrated {
            spool.persist_journal()?;
        }
        spool.complete_interrupted_cleanup()?;
        spool.verify_journal_payloads()?;
        spool.reconcile_orphans()?;
        spool.reconcile_snapshot_intent()?;
        spool.cleanup_unbound_snapshot_temporaries()?;
        Ok(spool)
    }

    pub fn root(&self) -> &Path {
        &self.root
    }

    pub fn identity(&self) -> &SpoolIdentity {
        &self.journal.identity
    }

    pub fn objects(&self) -> impl Iterator<Item = &SpoolObject> {
        self.journal.objects.values()
    }

    pub fn recovery_head(&self) -> Option<RecoveryHead> {
        self.journal
            .objects
            .last_key_value()
            .map(|(seq, object)| RecoveryHead {
                seq: *seq,
                ending_chain_checksum: object.ending_chain_checksum,
                source_cursor: object.source_cursor.clone(),
            })
    }

    pub fn has_complete_local_base(&self) -> bool {
        self.journal
            .objects
            .get(&self.journal.local_base_seq)
            .is_some_and(|object| {
                object.kind == ObjectKind::Snapshot
                    && object.local_creation_state == LocalCreationState::Installed
            })
    }

    pub fn checkpoint_window(&self) -> &CheckpointWindow {
        &self.journal.checkpoint_window
    }

    pub fn requires_checkpoint_reanchor(&self) -> bool {
        self.journal.checkpoint_window != CheckpointWindow::Closed
    }

    pub fn checkpointed_seq(&self) -> Option<u64> {
        self.journal.checkpointed_seq
    }

    /// Number of durably admitted source frames not yet covered by a completed
    /// SQLite checkpoint. The persisted cursor remains valid even after local
    /// cleanup removes the checkpointed object's payload record.
    pub fn admitted_frames_since_checkpoint(&self) -> u64 {
        let Some(head) = self
            .journal
            .admitted_seq
            .and_then(|seq| self.journal.objects.get(&seq))
        else {
            return 0;
        };
        let checkpoint_cursor = self
            .journal
            .checkpointed_source_cursor
            .as_ref()
            .or_else(|| {
                self.journal
                    .checkpointed_seq
                    .and_then(|seq| self.journal.objects.get(&seq))
                    .map(|object| &object.source_cursor)
            });
        let Some(checkpoint_cursor) = checkpoint_cursor else {
            return head.source_cursor.shadow_frame_index;
        };
        match head
            .source_cursor
            .shadow_generation
            .cmp(&checkpoint_cursor.shadow_generation)
        {
            std::cmp::Ordering::Less => u64::MAX,
            std::cmp::Ordering::Equal => head
                .source_cursor
                .shadow_frame_index
                .saturating_sub(checkpoint_cursor.shadow_frame_index),
            std::cmp::Ordering::Greater => u64::MAX,
        }
    }

    pub fn snapshot_in_progress(&self) -> Result<bool> {
        Ok(self.read_snapshot_intent()?.is_some())
    }

    pub fn prepare_snapshot(
        &mut self,
        seq: u64,
        previous_chain_checksum: u64,
        intended_remote_key: String,
        source_cursor: SourceCursor,
        page_size: u32,
    ) -> Result<SnapshotPreparation> {
        let proposed = SnapshotIntent {
            version: SPOOL_VERSION,
            stream_digest: self.journal.identity.stream_digest(),
            seq,
            previous_chain_checksum,
            intended_remote_key,
            source_cursor,
            page_size,
            legacy_stable_file_name: None,
            state: SnapshotIntentState::Creating,
        };
        let intent = match self.read_snapshot_intent()? {
            Some(existing) => {
                if !same_snapshot_identity(&existing, &proposed) {
                    bail!(
                        "divergent native snapshot intent already exists at seq {}",
                        existing.seq
                    );
                }
                existing
            }
            None => {
                let expected = self
                    .journal
                    .admitted_seq
                    .map(|value| value.saturating_add(1))
                    .unwrap_or(self.journal.identity.first_native_seq);
                if seq != expected {
                    bail!("native snapshot intent expected seq {expected}, got {seq}");
                }
                self.ensure_capacity(serialized_json_len(&proposed)?.saturating_mul(2))?;
                persist_json(&self.root, &self.snapshot_intent_path, &proposed)?;
                durability_failpoint("snapshot_intent_committed");
                proposed
            }
        };
        Ok(SnapshotPreparation {
            seq: intent.seq,
            previous_chain_checksum: intent.previous_chain_checksum,
            intended_remote_key: intent.intended_remote_key,
            source_cursor: intent.source_cursor,
            page_size: intent.page_size,
        })
    }

    pub fn finish_snapshot(&mut self, seq: u64) -> Result<()> {
        let mut intent = self
            .read_snapshot_intent()?
            .ok_or_else(|| anyhow!("no native snapshot intent exists for seq {seq}"))?;
        if intent.seq != seq {
            bail!(
                "native snapshot intent seq {} differs from {seq}",
                intent.seq
            );
        }
        let object = self
            .journal
            .objects
            .get(&seq)
            .ok_or_else(|| anyhow!("native snapshot seq {seq} was not admitted"))?;
        if object.kind != ObjectKind::Snapshot
            || object.source_cursor != intent.source_cursor
            || object.previous_chain_checksum != intent.previous_chain_checksum
            || object.intended_remote_key != intent.intended_remote_key
        {
            bail!("admitted native snapshot seq {seq} differs from its durable intent");
        }
        intent.state = SnapshotIntentState::Admitted;
        persist_json(&self.root, &self.snapshot_intent_path, &intent)?;
        if let Some(path) = self.legacy_snapshot_stable_path(&intent)? {
            remove_and_sync(&path, &self.snapshots_dir)?;
        }
        remove_and_sync(&self.snapshot_intent_path, &self.root)
    }

    /// Abandon a source boundary that never reached durable object admission.
    /// No checkpoint can have been released for it, so retry must freeze a new
    /// current boundary instead of pretending the old main-DB base is still
    /// reconstructible after arbitrary application activity.
    pub fn abandon_unadmitted_snapshot(&mut self, seq: u64) -> Result<()> {
        let Some(intent) = self.read_snapshot_intent()? else {
            return Ok(());
        };
        if intent.seq != seq {
            bail!(
                "cannot abandon native snapshot seq {seq}; intent belongs to {}",
                intent.seq
            );
        }
        if self.journal.objects.contains_key(&seq) {
            bail!("cannot abandon durably admitted native snapshot seq {seq}");
        }
        if self.intent_path(seq).exists() {
            bail!(
                "cannot abandon native snapshot seq {seq}; durable object installation is in progress"
            );
        }
        let tmp = self.payload_temporary_path(ObjectKind::Snapshot, seq);
        remove_and_sync(&tmp, &self.objects_dir)?;
        remove_and_sync(&self.snapshot_intent_path, &self.root)
    }

    pub fn pending_objects(&self) -> impl Iterator<Item = &SpoolObject> {
        self.journal
            .objects
            .values()
            .filter(|o| o.remote_upload_state != RemoteUploadState::Published)
    }

    pub fn get(&self, seq: u64) -> Option<&SpoolObject> {
        self.journal.objects.get(&seq)
    }

    pub fn payload_path(&self, object: &SpoolObject) -> PathBuf {
        self.objects_dir.join(object.payload_file_name())
    }

    /// Exact same-directory temporary consumed by [`NativeSpool::stage`].
    /// Snapshot encoders may stream directly here before admission; the
    /// durable snapshot source intent, not this temporary alone, is its proof.
    pub fn payload_temporary_path(&self, kind: ObjectKind, seq: u64) -> PathBuf {
        payload_temp_path(
            &self
                .objects_dir
                .join(format!("{:04x}-{seq:016x}.hadbp", kind.generation())),
        )
    }

    pub fn read_payload(&self, seq: u64) -> Result<Vec<u8>> {
        let object = self
            .get(seq)
            .ok_or_else(|| anyhow!("native spool has no sequence {seq}"))?;
        let bytes = fs::read(self.payload_path(object))?;
        validate_payload(object, &bytes, &self.root)?;
        Ok(bytes)
    }

    /// Re-read the on-disk journal and payload as the checkpoint-release
    /// admission proof. In-memory state or a successful channel notification
    /// is intentionally insufficient here.
    pub fn verify_durable_admission(&self, seq: u64) -> Result<()> {
        let bytes = fs::read(&self.journal_path)
            .with_context(|| format!("re-read durable journal for native seq {seq}"))?;
        let durable: Journal = serde_json::from_slice(&bytes)
            .with_context(|| format!("parse durable journal for native seq {seq}"))?;
        if durable.version != JOURNAL_VERSION || durable.identity != self.journal.identity {
            bail!("durable native spool journal identity/version mismatch");
        }
        if durable.admitted_seq.map_or(true, |admitted| admitted < seq) {
            bail!("native seq {seq} is not committed in the durable spool journal");
        }
        let durable_object = durable
            .objects
            .get(&seq)
            .ok_or_else(|| anyhow!("native seq {seq} has no durable journal object"))?;
        let memory_object = self
            .journal
            .objects
            .get(&seq)
            .ok_or_else(|| anyhow!("native seq {seq} has no in-memory journal object"))?;
        if durable_object != memory_object
            || durable_object.local_creation_state != LocalCreationState::Installed
        {
            bail!("native seq {seq} durable journal object differs from admitted state");
        }
        let payload = fs::read(self.payload_path(durable_object))?;
        validate_payload(durable_object, &payload, &self.root)
    }

    /// Install immutable HADBP bytes and atomically admit the matching object
    /// record. Returning success is the local checkpoint-release proof.
    pub fn stage(&mut self, stage: StageObject<'_>) -> Result<SpoolObject> {
        let stage_started = std::time::Instant::now();
        if stage.seq == 0 {
            bail!("native spool sequence 0 is invalid");
        }
        if stage.kind == ObjectKind::Delta && self.requires_checkpoint_reanchor() {
            bail!("native checkpoint window is not closed; a snapshot re-anchor is required before another delta");
        }
        let decoded = ltx::decode_sqlite_changeset(stage.payload)
            .context("native spool payload is not valid HADBP")?;
        if decoded.header.seq != stage.seq {
            bail!(
                "native spool seq mismatch: record {}, HADBP {}",
                stage.seq,
                decoded.header.seq
            );
        }
        if decoded.header.prev_checksum != stage.previous_chain_checksum {
            bail!("native spool predecessor checksum does not match HADBP header");
        }
        let encoded_end = ltx::changeset_end_page_count(&decoded)?;
        match stage.kind {
            ObjectKind::Snapshot if encoded_end.is_some() => {
                bail!("native spool snapshot carries a delta end-page marker")
            }
            ObjectKind::Delta if encoded_end != Some(stage.end_page_count) => bail!(
                "native spool delta end-page count mismatch: record {}, HADBP {:?}",
                stage.end_page_count,
                encoded_end
            ),
            _ => {}
        }
        let digest = sha256_hex(stage.payload);
        let object = SpoolObject {
            version: SPOOL_VERSION,
            stream_digest: self.journal.identity.stream_digest(),
            lineage_id: self.journal.identity.lineage_id.clone(),
            bucket: self.journal.identity.bucket.clone(),
            prefix: self.journal.identity.prefix.clone(),
            database: self.journal.identity.database.clone(),
            seq: stage.seq,
            kind: stage.kind,
            previous_chain_checksum: stage.previous_chain_checksum,
            ending_chain_checksum: stage.ending_chain_checksum,
            end_page_count: stage.end_page_count,
            intended_remote_key: stage.intended_remote_key,
            payload_length: stage.payload.len() as u64,
            payload_sha256: digest,
            source_cursor: stage.source_cursor,
            local_creation_state: LocalCreationState::Installed,
            remote_upload_state: RemoteUploadState::Pending,
            created_unix_ms: unix_ms(),
            uploaded_unix_ms: None,
            published_unix_ms: None,
            publish_record_sha256: None,
        };
        validate_object_identity(&self.journal.identity, &object)?;

        if let Some(existing) = self.journal.objects.get(&stage.seq) {
            if same_immutable_object(existing, &object) {
                let bytes = fs::read(self.payload_path(existing))?;
                validate_payload(existing, &bytes, &self.root)?;
                if bytes == stage.payload {
                    self.last_stage_duration_ms = stage_started
                        .elapsed()
                        .as_millis()
                        .min(u128::from(u64::MAX))
                        as u64;
                    return Ok(existing.clone());
                }
            }
            bail!(
                "native spool equivocation at sequence {}: existing object differs",
                stage.seq
            );
        }
        self.validate_next_object(&object)?;

        // Reserve the complete write-order peak before creating the intent:
        // intent JSON, payload temporary/installed bytes, and the full new
        // journal temporary while the old journal remains installed.
        let mut projected_journal = self.journal.clone();
        projected_journal.objects.insert(stage.seq, object.clone());
        projected_journal.admitted_seq = Some(stage.seq);
        let intent_bytes = serialized_json_len(&InstallIntent {
            version: SPOOL_VERSION,
            object: object.clone(),
        })?;
        let journal_bytes = serialized_json_len(&projected_journal)?;
        let payload_path = self.payload_path(&object);
        let prepared_temp_exists = payload_temp_path(&payload_path).exists();
        let additional_peak = (if prepared_temp_exists {
            0
        } else {
            (stage.payload.len() as u64).saturating_mul(2)
        })
        .saturating_add(intent_bytes)
        .saturating_add(journal_bytes);
        self.ensure_capacity(additional_peak)?;

        let intent_path = self.intent_path(stage.seq);
        persist_json(
            &self.intents_dir,
            &intent_path,
            &InstallIntent {
                version: SPOOL_VERSION,
                object: object.clone(),
            },
        )?;
        durability_failpoint("object_intent_committed");

        match fs::read(&payload_path) {
            Ok(existing) if existing == stage.payload => {
                validate_payload(&object, &existing, &self.root)?;
            }
            Ok(_) => bail!(
                "native spool equivocation: divergent installed payload at sequence {}",
                stage.seq
            ),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                let tmp = payload_temp_path(&payload_path);
                match fs::read(&tmp) {
                    Ok(existing) if existing == stage.payload => {
                        validate_payload(&object, &existing, &self.root)?;
                        File::open(&tmp)?.sync_all()?;
                        fs::rename(&tmp, &payload_path)?;
                        sync_dir(&self.objects_dir)?;
                    }
                    Ok(_) => bail!(
                        "native spool equivocation: divergent payload temporary at sequence {}",
                        stage.seq
                    ),
                    Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                        install_payload(&self.objects_dir, &payload_path, stage.payload)?;
                    }
                    Err(error) => return Err(error.into()),
                }
                durability_failpoint("payload_renamed");
            }
            Err(e) => return Err(e.into()),
        }

        let previous_admitted = self.journal.admitted_seq;
        self.journal.objects.insert(stage.seq, object.clone());
        self.journal.admitted_seq = Some(stage.seq);
        if let Err(error) = self.persist_journal() {
            self.journal.objects.remove(&stage.seq);
            self.journal.admitted_seq = previous_admitted;
            return Err(error);
        }
        durability_failpoint("object_journal_committed");

        remove_and_sync(&intent_path, &self.intents_dir)?;
        self.last_stage_duration_ms = stage_started
            .elapsed()
            .as_millis()
            .min(u128::from(u64::MAX)) as u64;
        Ok(object)
    }

    /// Persist the release boundary before ending the SQLite blocker read
    /// transaction. A crash with any non-closed window requires a conservative
    /// full native snapshot before another delta can be admitted.
    pub fn begin_checkpoint_window(&mut self, seq: u64) -> Result<()> {
        if self.journal.checkpoint_window != CheckpointWindow::Closed {
            bail!("cannot open a nested native checkpoint window");
        }
        let object = self
            .journal
            .objects
            .get(&seq)
            .ok_or_else(|| anyhow!("cannot open checkpoint window for unadmitted seq {seq}"))?;
        if self.journal.admitted_seq != Some(seq) {
            bail!("cannot checkpoint native seq {seq}; it is not the admitted head");
        }
        let old = self.journal.clone();
        self.journal.checkpoint_window = CheckpointWindow::Opening {
            seq,
            source_cursor: object.source_cursor.clone(),
        };
        if let Err(error) = self.persist_journal() {
            self.journal = old;
            return Err(error);
        }
        durability_failpoint("checkpoint_window_committed");
        Ok(())
    }

    /// Record that the blocker was reacquired but an application commit crossed
    /// the controlled window. The marker remains non-closed until a native
    /// snapshot re-anchor has been admitted.
    pub fn mark_checkpoint_window_rearmed_dirty(
        &mut self,
        seq: u64,
        checkpoint_completed: bool,
    ) -> Result<()> {
        let source_cursor = match &self.journal.checkpoint_window {
            CheckpointWindow::Opening {
                seq: open_seq,
                source_cursor,
            } if *open_seq == seq => source_cursor.clone(),
            other => {
                bail!("cannot mark native checkpoint seq {seq} dirty from window state {other:?}")
            }
        };
        let old = self.journal.clone();
        self.journal.checkpoint_window = CheckpointWindow::RearmedDirty {
            seq,
            source_cursor,
            checkpoint_completed,
        };
        if let Err(error) = self.persist_journal() {
            self.journal = old;
            return Err(error);
        }
        durability_failpoint("checkpoint_window_rearmed_dirty");
        Ok(())
    }

    /// Close a controlled checkpoint window only after the blocker is known to
    /// be rearmed. Dirty windows additionally require a later admitted snapshot.
    pub fn close_checkpoint_window(
        &mut self,
        seq: u64,
        checkpoint_completed: bool,
        reanchor_seq: Option<u64>,
    ) -> Result<()> {
        let checkpoint_cursor = match &self.journal.checkpoint_window {
            CheckpointWindow::Opening { seq: open_seq, .. } if *open_seq == seq => {
                if reanchor_seq.is_some() {
                    bail!("clean checkpoint window cannot claim a re-anchor");
                }
                self.journal
                    .objects
                    .get(&seq)
                    .expect("open checkpoint object exists")
                    .source_cursor
                    .clone()
            }
            CheckpointWindow::RearmedDirty {
                seq: open_seq,
                source_cursor,
                ..
            } if *open_seq == seq => {
                let reanchor_seq = reanchor_seq.ok_or_else(|| {
                    anyhow!(
                        "dirty checkpoint window seq {seq} requires a native snapshot re-anchor"
                    )
                })?;
                let reanchor = self.journal.objects.get(&reanchor_seq).ok_or_else(|| {
                    anyhow!("checkpoint re-anchor seq {reanchor_seq} is not admitted")
                })?;
                if reanchor_seq <= seq || reanchor.kind != ObjectKind::Snapshot {
                    bail!(
                        "checkpoint re-anchor must be a later native snapshot (window {seq}, got {reanchor_seq})"
                    );
                }
                source_cursor.clone()
            }
            other => bail!("cannot close native checkpoint seq {seq} from window state {other:?}"),
        };
        let old = self.journal.clone();
        if checkpoint_completed {
            self.journal.checkpointed_seq = Some(seq);
            self.journal.checkpointed_source_cursor = Some(checkpoint_cursor);
        }
        self.journal.checkpoint_window = CheckpointWindow::Closed;
        if let Err(error) = self.persist_journal() {
            self.journal = old;
            return Err(error);
        }
        durability_failpoint("checkpoint_window_closed");
        Ok(())
    }

    pub fn complete_checkpoint_reanchor(&mut self, reanchor_seq: u64) -> Result<()> {
        let (seq, source_cursor, checkpoint_completed) = match &self.journal.checkpoint_window {
            CheckpointWindow::Opening { seq, source_cursor } => {
                (*seq, source_cursor.clone(), false)
            }
            CheckpointWindow::RearmedDirty {
                seq,
                source_cursor,
                checkpoint_completed,
                ..
            } => (*seq, source_cursor.clone(), *checkpoint_completed),
            CheckpointWindow::Closed => return Ok(()),
        };
        let reanchor = self.journal.objects.get(&reanchor_seq).ok_or_else(|| {
            anyhow!("checkpoint recovery re-anchor seq {reanchor_seq} is not admitted")
        })?;
        if reanchor_seq <= seq || reanchor.kind != ObjectKind::Snapshot {
            bail!(
                "checkpoint recovery requires a later native snapshot (window {seq}, got {reanchor_seq})"
            );
        }
        let old = self.journal.clone();
        if checkpoint_completed {
            self.journal.checkpointed_seq = Some(seq);
            self.journal.checkpointed_source_cursor = Some(source_cursor);
        }
        self.journal.checkpoint_window = CheckpointWindow::Closed;
        if let Err(error) = self.persist_journal() {
            self.journal = old;
            return Err(error);
        }
        durability_failpoint("checkpoint_reanchor_closed");
        Ok(())
    }

    pub fn mark_uploaded(&mut self, seq: u64) -> Result<()> {
        let old = self.journal.clone();
        let object = self
            .journal
            .objects
            .get_mut(&seq)
            .ok_or_else(|| anyhow!("cannot upload unknown native spool sequence {seq}"))?;
        if object.remote_upload_state == RemoteUploadState::Published {
            return Ok(());
        }
        object.remote_upload_state = RemoteUploadState::Uploaded;
        object.uploaded_unix_ms = Some(unix_ms());
        if let Err(error) = self.persist_journal() {
            self.journal = old;
            return Err(error);
        }
        Ok(())
    }

    pub fn mark_published(&mut self, seq: u64, publish_record: &[u8]) -> Result<()> {
        let expected = match self.journal.remote_published_seq {
            None => self.journal.identity.first_native_seq,
            Some(previous) => previous
                .checked_add(1)
                .ok_or_else(|| anyhow!("native spool publish sequence overflow"))?,
        };
        if seq != expected {
            bail!("cannot publish native seq {seq}; contiguous next seq is {expected}");
        }
        let old = self.journal.clone();
        let object = self
            .journal
            .objects
            .get_mut(&seq)
            .ok_or_else(|| anyhow!("cannot publish unknown native spool sequence {seq}"))?;
        if object.remote_upload_state != RemoteUploadState::Uploaded {
            bail!("cannot publish native seq {seq} before exact object upload verification");
        }
        object.remote_upload_state = RemoteUploadState::Published;
        object.published_unix_ms = Some(unix_ms());
        object.publish_record_sha256 = Some(sha256_hex(publish_record));
        self.journal.remote_published_seq = Some(seq);
        if let Err(error) = self.persist_journal() {
            self.journal = old;
            return Err(error);
        }
        Ok(())
    }

    pub fn admitted_seq(&self) -> Option<u64> {
        self.journal.admitted_seq
    }

    pub fn remote_published_seq(&self) -> Option<u64> {
        self.journal.remote_published_seq
    }

    pub fn used_bytes(&self) -> Result<u64> {
        directory_bytes(&self.root)
    }

    pub fn free_bytes(&self) -> Result<u64> {
        filesystem_free_bytes(&self.root)
    }

    pub fn capacity_state(&self, additional_peak_bytes: u64) -> Result<CapacityState> {
        let used = self.used_bytes()?;
        let free = self.free_bytes()?;
        let state = if used.saturating_add(additional_peak_bytes) > self.capacity.hard_bytes
            || free.saturating_sub(additional_peak_bytes) < self.capacity.minimum_free_bytes
        {
            CapacityState::Full
        } else if used.saturating_add(additional_peak_bytes) >= self.capacity.warning_bytes {
            CapacityState::High
        } else {
            CapacityState::Healthy
        };
        self.last_capacity_state.set(state);
        Ok(state)
    }

    pub fn last_stage_duration_ms(&self) -> u64 {
        self.last_stage_duration_ms
    }

    pub fn last_capacity_state(&self) -> CapacityState {
        self.last_capacity_state.get()
    }

    /// Bytes needed for an atomic rewrite of the current journal plus a
    /// conservative allowance for one newly admitted object record.
    pub fn next_journal_rewrite_peak_bytes(&self) -> Result<u64> {
        Ok(serialized_json_len(&self.journal)?.saturating_add(64 * 1024))
    }

    /// Reclaim only remotely published history older than the newest published
    /// snapshot. The newest snapshot and every descendant remain a complete
    /// locally restorable chain; pending/unpublished objects are never victims.
    pub fn cleanup_published_before_latest_snapshot(&mut self) -> Result<u64> {
        let Some(base_seq) = self
            .journal
            .objects
            .values()
            .filter(|object| {
                object.kind == ObjectKind::Snapshot
                    && object.remote_upload_state == RemoteUploadState::Published
                    && object.local_creation_state == LocalCreationState::Installed
            })
            .map(|object| object.seq)
            .max()
        else {
            return Ok(0);
        };
        if base_seq <= self.journal.local_base_seq {
            return Ok(0);
        }
        let victims = self
            .journal
            .objects
            .values()
            .filter(|object| object.seq < base_seq)
            .map(|object| object.seq)
            .collect::<Vec<_>>();
        if victims.iter().any(|seq| {
            self.journal.objects[seq].remote_upload_state != RemoteUploadState::Published
        }) {
            bail!("native spool cleanup would delete pending/unpublished recovery data");
        }
        let old = self.journal.clone();
        for seq in &victims {
            self.journal
                .objects
                .get_mut(seq)
                .unwrap()
                .local_creation_state = LocalCreationState::Deleting;
        }
        if let Err(error) = self.persist_journal() {
            self.journal = old;
            return Err(error);
        }
        durability_failpoint("cleanup_marked_deleting");
        for seq in &victims {
            let path = self.payload_path(self.journal.objects.get(seq).unwrap());
            remove_and_sync(&path, &self.objects_dir)?;
        }
        durability_failpoint("cleanup_payloads_deleted");
        for seq in &victims {
            self.journal.objects.remove(seq);
        }
        self.journal.local_base_seq = base_seq;
        self.persist_journal()?;
        Ok(victims.len() as u64)
    }

    fn ensure_capacity(&self, additional_peak_bytes: u64) -> Result<()> {
        if self.capacity_state(additional_peak_bytes)? == CapacityState::Full {
            bail!(
                "local_spool_full: used={} additional_peak={} hard={} free={} reserve={}",
                self.used_bytes()?,
                additional_peak_bytes,
                self.capacity.hard_bytes,
                self.free_bytes()?,
                self.capacity.minimum_free_bytes
            );
        }
        Ok(())
    }

    fn validate_next_object(&self, object: &SpoolObject) -> Result<()> {
        let expected_seq = match self.journal.objects.last_key_value() {
            Some((seq, previous)) => {
                if object.previous_chain_checksum != previous.ending_chain_checksum {
                    bail!(
                        "native spool chain mismatch at seq {}: previous ends {:016x}, object starts {:016x}",
                        object.seq,
                        previous.ending_chain_checksum,
                        object.previous_chain_checksum
                    );
                }
                validate_source_cursor_successor(&previous.source_cursor, &object.source_cursor)?;
                seq.checked_add(1)
                    .ok_or_else(|| anyhow!("native spool sequence overflow"))?
            }
            None => self.journal.local_base_seq,
        };
        if object.seq != expected_seq {
            bail!(
                "native spool sequence gap: expected {}, got {}",
                expected_seq,
                object.seq
            );
        }
        if self.journal.objects.is_empty() && object.kind != ObjectKind::Snapshot {
            bail!("native spool must begin with a full HADBP snapshot");
        }
        Ok(())
    }

    fn verify_journal_payloads(&self) -> Result<()> {
        let mut prior: Option<&SpoolObject> = None;
        for (seq, object) in &self.journal.objects {
            if *seq != object.seq {
                bail!("native spool journal key/object sequence mismatch");
            }
            validate_object_identity(&self.journal.identity, object)?;
            if object.local_creation_state != LocalCreationState::Installed {
                bail!("native spool cleanup state was not reconciled at seq {seq}");
            }
            if let Some(previous) = prior {
                if object.seq != previous.seq + 1
                    || object.previous_chain_checksum != previous.ending_chain_checksum
                {
                    bail!("native spool journal contains a sequence/checksum gap at {seq}");
                }
                validate_source_cursor_successor(&previous.source_cursor, &object.source_cursor)
                    .with_context(|| {
                        format!("native spool source cursor regressed at seq {seq}")
                    })?;
            } else if object.seq != self.journal.local_base_seq
                || object.kind != ObjectKind::Snapshot
            {
                bail!("native spool journal does not begin at its snapshot base");
            }
            let bytes = fs::read(self.payload_path(object))
                .with_context(|| format!("read admitted native payload for sequence {seq}"))?;
            validate_payload(object, &bytes, &self.root)?;
            prior = Some(object);
        }
        if self.journal.admitted_seq != self.journal.objects.last_key_value().map(|(seq, _)| *seq) {
            bail!("native spool admitted cursor does not equal the journal object head");
        }
        if let Some(remote_seq) = self.journal.remote_published_seq {
            if remote_seq < self.journal.local_base_seq
                || self
                    .journal
                    .admitted_seq
                    .map_or(true, |admitted| remote_seq > admitted)
                || !self.journal.objects.contains_key(&remote_seq)
            {
                bail!(
                    "native spool remote publish cursor {} is outside the retained admitted chain",
                    remote_seq
                );
            }
        }
        for (seq, object) in &self.journal.objects {
            let should_be_published = self
                .journal
                .remote_published_seq
                .is_some_and(|remote_seq| *seq <= remote_seq);
            match object.remote_upload_state {
                RemoteUploadState::Pending => {
                    if should_be_published
                        || object.uploaded_unix_ms.is_some()
                        || object.published_unix_ms.is_some()
                        || object.publish_record_sha256.is_some()
                    {
                        bail!("native spool pending upload state is inconsistent at seq {seq}");
                    }
                }
                RemoteUploadState::Uploaded => {
                    if should_be_published
                        || object.uploaded_unix_ms.is_none()
                        || object.published_unix_ms.is_some()
                        || object.publish_record_sha256.is_some()
                    {
                        bail!("native spool uploaded state is inconsistent at seq {seq}");
                    }
                }
                RemoteUploadState::Published => {
                    if !should_be_published
                        || object.uploaded_unix_ms.is_none()
                        || object.published_unix_ms.is_none()
                        || object.publish_record_sha256.is_none()
                    {
                        bail!("native spool published state is inconsistent at seq {seq}");
                    }
                }
            }
        }
        match (
            self.journal.checkpointed_seq,
            self.journal.checkpointed_source_cursor.as_ref(),
        ) {
            (None, None) => {}
            (None, Some(_)) => {
                bail!("native spool has a checkpoint source cursor without a sequence")
            }
            (Some(seq), None) if !self.journal.objects.contains_key(&seq) => {
                bail!("native spool checkpoint cursor names a missing local object")
            }
            (Some(seq), Some(cursor)) => match self.journal.objects.get(&seq) {
                Some(object) if &object.source_cursor != cursor => {
                    bail!("native spool checkpoint source cursor differs from seq {seq}")
                }
                None if seq >= self.journal.local_base_seq => {
                    bail!("native spool checkpoint cursor names a missing retained object")
                }
                _ => {}
            },
            (Some(_), None) => {}
        }
        if let Some(checkpointed_seq) = self.journal.checkpointed_seq {
            let admitted_seq = self.journal.admitted_seq.ok_or_else(|| {
                anyhow!("native spool checkpoint cursor exists without an admitted head")
            })?;
            if checkpointed_seq > admitted_seq {
                bail!(
                    "native spool checkpoint cursor {} is ahead of admitted head {}",
                    checkpointed_seq,
                    admitted_seq
                );
            }
            if let Some(checkpoint_cursor) = self
                .journal
                .checkpointed_source_cursor
                .as_ref()
                .or_else(|| {
                    self.journal
                        .objects
                        .get(&checkpointed_seq)
                        .map(|object| &object.source_cursor)
                })
            {
                let admitted_cursor = &self
                    .journal
                    .objects
                    .get(&admitted_seq)
                    .expect("admitted head was validated above")
                    .source_cursor;
                validate_source_cursor_successor(checkpoint_cursor, admitted_cursor)
                    .context("native spool admitted cursor is behind its checkpoint cursor")?;
            }
        }
        match &self.journal.checkpoint_window {
            CheckpointWindow::Closed => {}
            CheckpointWindow::Opening { seq, source_cursor }
            | CheckpointWindow::RearmedDirty {
                seq, source_cursor, ..
            } => {
                let object =
                    self.journal.objects.get(seq).ok_or_else(|| {
                        anyhow!("native checkpoint window names missing seq {seq}")
                    })?;
                if &object.source_cursor != source_cursor {
                    bail!("native checkpoint window source cursor differs from seq {seq}");
                }
            }
        }
        Ok(())
    }

    fn complete_interrupted_cleanup(&mut self) -> Result<()> {
        let deleting = self
            .journal
            .objects
            .values()
            .filter(|object| object.local_creation_state == LocalCreationState::Deleting)
            .map(|object| object.seq)
            .collect::<Vec<_>>();
        if deleting.is_empty() {
            return Ok(());
        }
        let first_remaining = self
            .journal
            .objects
            .values()
            .find(|object| object.local_creation_state != LocalCreationState::Deleting)
            .ok_or_else(|| anyhow!("native spool cleanup marked every local object deleting"))?;
        if first_remaining.kind != ObjectKind::Snapshot
            || first_remaining.local_creation_state != LocalCreationState::Installed
            || first_remaining.remote_upload_state != RemoteUploadState::Published
            || self
                .journal
                .objects
                .range(..first_remaining.seq)
                .any(|(_, object)| {
                    object.local_creation_state != LocalCreationState::Deleting
                        || object.remote_upload_state != RemoteUploadState::Published
                })
            || self
                .journal
                .objects
                .range(first_remaining.seq..)
                .any(|(_, object)| object.local_creation_state == LocalCreationState::Deleting)
        {
            bail!(
                "native spool interrupted cleanup is not a published prefix below a complete snapshot base"
            );
        }
        let new_base_seq = first_remaining.seq;
        let mut prior: Option<&SpoolObject> = None;
        for (seq, object) in self.journal.objects.range(new_base_seq..) {
            validate_object_identity(&self.journal.identity, object)?;
            if object.local_creation_state != LocalCreationState::Installed {
                bail!("native spool retained cleanup chain is not installed at seq {seq}");
            }
            if let Some(previous) = prior {
                if object.seq != previous.seq + 1
                    || object.previous_chain_checksum != previous.ending_chain_checksum
                {
                    bail!("native spool retained cleanup chain has a gap at seq {seq}");
                }
                validate_source_cursor_successor(&previous.source_cursor, &object.source_cursor)
                    .with_context(|| {
                        format!(
                            "native spool retained cleanup source cursor regressed at seq {seq}"
                        )
                    })?;
            } else if object.seq != new_base_seq || object.kind != ObjectKind::Snapshot {
                bail!("native spool retained cleanup chain does not start with its snapshot base");
            }
            let payload = fs::read(self.payload_path(object)).with_context(|| {
                format!("read retained native cleanup payload for sequence {seq}")
            })?;
            validate_payload(object, &payload, &self.root).with_context(|| {
                format!("validate retained native cleanup payload for sequence {seq}")
            })?;
            prior = Some(object);
        }
        for seq in deleting {
            let object = self.journal.objects.get(&seq).unwrap().clone();
            remove_and_sync(&self.payload_path(&object), &self.objects_dir)?;
            self.journal.objects.remove(&seq);
        }
        self.journal.local_base_seq = new_base_seq;
        self.persist_journal()
    }

    fn reconcile_orphans(&mut self) -> Result<()> {
        let mut paths = fs::read_dir(&self.intents_dir)?
            .filter_map(|entry| entry.ok().map(|e| e.path()))
            .filter(|path| path.extension().and_then(|v| v.to_str()) == Some("json"))
            .collect::<Vec<_>>();
        paths.sort();
        for path in paths {
            let bytes = fs::read(&path)?;
            let intent: InstallIntent = serde_json::from_slice(&bytes)
                .with_context(|| format!("parse native spool intent {}", path.display()))?;
            if intent.version != SPOOL_VERSION {
                bail!("unsupported native spool intent version {}", intent.version);
            }
            let object = intent.object;
            validate_object_identity(&self.journal.identity, &object)?;
            let payload_path = self.payload_path(&object);
            let payload = match fs::read(&payload_path) {
                Ok(bytes) => bytes,
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                    // A crash can land after the payload temporary was fsynced
                    // but before its rename. The durable intent proves the
                    // temporary's identity. Finish an exact temporary; discard
                    // a partial one and retry encoding later.
                    let tmp = payload_temp_path(&payload_path);
                    match fs::read(&tmp) {
                        Ok(bytes) => match validate_payload(&object, &bytes, &self.root) {
                            Ok(()) => {
                                File::open(&tmp)?.sync_all()?;
                                fs::rename(&tmp, &payload_path)?;
                                sync_dir(&self.objects_dir)?;
                                bytes
                            }
                            Err(error) => {
                                tracing::error!(
                                    path = %tmp.display(),
                                    error = %error,
                                    "removing partial native HADBP payload temporary after interrupted install"
                                );
                                remove_and_sync(&tmp, &self.objects_dir)?;
                                remove_and_sync(&path, &self.intents_dir)?;
                                continue;
                            }
                        },
                        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                            // Intent was durable but payload creation never
                            // reached either a temporary or final file.
                            remove_and_sync(&path, &self.intents_dir)?;
                            continue;
                        }
                        Err(error) => return Err(error.into()),
                    }
                }
                Err(e) => return Err(e.into()),
            };
            validate_payload(&object, &payload, &self.root)?;
            if let Some(existing) = self.journal.objects.get(&object.seq) {
                if !same_immutable_object(existing, &object) {
                    bail!("divergent orphan for admitted native seq {}", object.seq);
                }
                remove_and_sync(&path, &self.intents_dir)?;
                continue;
            }
            self.validate_next_object(&object)?;
            self.journal.objects.insert(object.seq, object.clone());
            self.journal.admitted_seq = Some(object.seq);
            self.persist_journal()?;
            remove_and_sync(&path, &self.intents_dir)?;
        }

        // A final payload without either a committed record or a durable intent
        // has no source-cursor/identity proof. Retain it and fail loudly.
        for entry in fs::read_dir(&self.objects_dir)? {
            let path = entry?.path();
            if path.extension().and_then(|v| v.to_str()) != Some("hadbp") {
                continue;
            }
            let known = self
                .journal
                .objects
                .values()
                .any(|object| self.payload_path(object) == path);
            if !known {
                bail!(
                    "unproven native HADBP orphan {}; retaining for operator recovery",
                    path.display()
                );
            }
        }
        Ok(())
    }

    fn read_snapshot_intent(&self) -> Result<Option<SnapshotIntent>> {
        let bytes = match fs::read(&self.snapshot_intent_path) {
            Ok(bytes) => bytes,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(error) => return Err(error.into()),
        };
        let intent: SnapshotIntent = serde_json::from_slice(&bytes).with_context(|| {
            format!(
                "parse native snapshot intent {}",
                self.snapshot_intent_path.display()
            )
        })?;
        if intent.version != SPOOL_VERSION
            || intent.stream_digest != self.journal.identity.stream_digest()
        {
            bail!("native snapshot intent identity/version mismatch");
        }
        Ok(Some(intent))
    }

    fn reconcile_snapshot_intent(&mut self) -> Result<()> {
        let Some(mut intent) = self.read_snapshot_intent()? else {
            return Ok(());
        };
        if let Some(object) = self.journal.objects.get(&intent.seq) {
            if object.kind != ObjectKind::Snapshot
                || object.source_cursor != intent.source_cursor
                || object.previous_chain_checksum != intent.previous_chain_checksum
                || object.intended_remote_key != intent.intended_remote_key
            {
                bail!(
                    "admitted native seq {} diverges from interrupted snapshot intent",
                    intent.seq
                );
            }
            intent.state = SnapshotIntentState::Admitted;
            persist_json(&self.root, &self.snapshot_intent_path, &intent)?;
            if let Some(path) = self.legacy_snapshot_stable_path(&intent)? {
                remove_and_sync(&path, &self.snapshots_dir)?;
            }
            return remove_and_sync(&self.snapshot_intent_path, &self.root);
        }
        match &intent.state {
            SnapshotIntentState::Creating => {
                let tmp = self.payload_temporary_path(ObjectKind::Snapshot, intent.seq);
                let bytes = match fs::read(&tmp) {
                    Ok(bytes) => bytes,
                    Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                        // Encoding never made a durable payload. No checkpoint
                        // can have been released, so a new boundary is safe.
                        return remove_and_sync(&self.snapshot_intent_path, &self.root);
                    }
                    Err(error) => return Err(error.into()),
                };
                let decoded = match ltx::decode_sqlite_changeset(&bytes) {
                    Ok(decoded) => decoded,
                    Err(error) => {
                        // A create-new temporary with no install intent is an
                        // interrupted encode, not an admitted recovery point.
                        // Validate it as incomplete before removing both
                        // pieces of unadmitted state.
                        tracing::warn!(
                            seq = intent.seq,
                            path = %tmp.display(),
                            error = %error,
                            "removing incomplete unadmitted native snapshot temporary"
                        );
                        remove_and_sync(&tmp, &self.objects_dir)?;
                        return remove_and_sync(&self.snapshot_intent_path, &self.root);
                    }
                };
                if decoded.header.seq != intent.seq
                    || decoded.header.prev_checksum != intent.previous_chain_checksum
                    || decoded.header.page_size != intent.page_size
                {
                    bail!(
                        "valid native snapshot temporary diverges from durable source intent at seq {}; retaining it",
                        intent.seq
                    );
                }
                let (ending_chain_checksum, end_page_count) =
                    ltx::snapshot_checksum_and_page_count(&decoded)?;
                self.stage(StageObject {
                    seq: intent.seq,
                    kind: ObjectKind::Snapshot,
                    previous_chain_checksum: intent.previous_chain_checksum,
                    ending_chain_checksum,
                    end_page_count,
                    intended_remote_key: intent.intended_remote_key.clone(),
                    source_cursor: intent.source_cursor.clone(),
                    payload: &bytes,
                })?;
                self.finish_snapshot(intent.seq)
            }
            SnapshotIntentState::Stable {
                payload_length,
                sha256,
            } => {
                let path = self.legacy_snapshot_stable_path(&intent)?.ok_or_else(|| {
                    anyhow!("legacy stable snapshot intent is missing its filename")
                })?;
                let bytes = fs::read(&path)
                    .with_context(|| format!("read legacy stable snapshot {}", path.display()))?;
                if bytes.len() as u64 != *payload_length || sha256_hex(&bytes) != *sha256 {
                    bail!(
                        "legacy stable snapshot intent failed length/digest validation at seq {}",
                        intent.seq
                    );
                }
                let encoded = ltx::encode_snapshot_with_checksum(
                    &path,
                    intent.page_size,
                    intent.seq,
                    intent.previous_chain_checksum,
                )?;
                let pages = bytes.len() as u64 / intent.page_size as u64;
                self.stage(StageObject {
                    seq: intent.seq,
                    kind: ObjectKind::Snapshot,
                    previous_chain_checksum: intent.previous_chain_checksum,
                    ending_chain_checksum: encoded.checksum,
                    end_page_count: pages,
                    intended_remote_key: intent.intended_remote_key.clone(),
                    source_cursor: intent.source_cursor.clone(),
                    payload: &encoded.bytes,
                })?;
                self.finish_snapshot(intent.seq)
            }
            SnapshotIntentState::Admitted => {
                bail!(
                    "native snapshot intent says seq {} was admitted but the journal object is missing",
                    intent.seq
                );
            }
        }
    }

    fn legacy_snapshot_stable_path(&self, intent: &SnapshotIntent) -> Result<Option<PathBuf>> {
        let Some(file_name) = &intent.legacy_stable_file_name else {
            return Ok(None);
        };
        let name = Path::new(file_name);
        if name.components().count() != 1
            || name.file_name().and_then(|value| value.to_str()) != Some(file_name.as_str())
        {
            bail!("legacy native snapshot intent contains an unsafe stable filename");
        }
        Ok(Some(self.snapshots_dir.join(name)))
    }

    fn cleanup_unbound_snapshot_temporaries(&self) -> Result<()> {
        for entry in fs::read_dir(&self.snapshots_dir)? {
            let path = entry?.path();
            if !path.is_file() {
                continue;
            }
            tracing::error!(
                path = %path.display(),
                "removing obsolete native SQLite stable-copy artifact; snapshots encode directly to HADBP"
            );
            remove_and_sync(&path, &self.snapshots_dir)?;
        }
        // Recover pre-intent PR #43 snapshot files before capacity accounting.
        for entry in fs::read_dir(&self.root)? {
            let path = entry?.path();
            let Some(name) = path.file_name().and_then(|value| value.to_str()) else {
                continue;
            };
            if name.starts_with(".snapshot-") && name.ends_with(".db.tmp") && path.is_file() {
                tracing::error!(
                    path = %path.display(),
                    "removing legacy unbound snapshot temporary before spool capacity accounting"
                );
                remove_and_sync(&path, &self.root)?;
            }
        }
        for entry in fs::read_dir(&self.objects_dir)? {
            let path = entry?.path();
            if path
                .file_name()
                .and_then(|value| value.to_str())
                .is_some_and(|name| name.starts_with("0001-") && name.ends_with(".hadbp.tmp"))
            {
                tracing::error!(
                    path = %path.display(),
                    "removing unadmitted native snapshot payload temporary after restart"
                );
                remove_and_sync(&path, &self.objects_dir)?;
            }
        }
        Ok(())
    }

    fn intent_path(&self, seq: u64) -> PathBuf {
        self.intents_dir.join(format!("{seq:016x}.json"))
    }

    fn persist_journal(&self) -> Result<()> {
        persist_json(&self.root, &self.journal_path, &self.journal)
    }
}

fn same_snapshot_identity(a: &SnapshotIntent, b: &SnapshotIntent) -> bool {
    a.version == b.version
        && a.stream_digest == b.stream_digest
        && a.seq == b.seq
        && a.previous_chain_checksum == b.previous_chain_checksum
        && a.intended_remote_key == b.intended_remote_key
        && a.source_cursor == b.source_cursor
        && a.page_size == b.page_size
}

fn validate_source_cursor_successor(previous: &SourceCursor, next: &SourceCursor) -> Result<()> {
    if next.shadow_generation < previous.shadow_generation
        || (next.shadow_generation == previous.shadow_generation
            && next.shadow_frame_index < previous.shadow_frame_index)
    {
        bail!(
            "shadow cursor moved backward from generation/frame {}/{} to {}/{}",
            previous.shadow_generation,
            previous.shadow_frame_index,
            next.shadow_generation,
            next.shadow_frame_index
        );
    }
    Ok(())
}

fn load_journal(bytes: &[u8], path: &Path) -> Result<(Journal, bool)> {
    #[derive(Deserialize)]
    struct VersionEnvelope {
        version: u32,
    }
    let version = serde_json::from_slice::<VersionEnvelope>(bytes)
        .with_context(|| format!("parse native spool journal version at {}", path.display()))?
        .version;
    match version {
        JOURNAL_VERSION => {
            let journal: Journal = serde_json::from_slice(bytes)
                .with_context(|| format!("parse {}", path.display()))?;
            Ok((journal, false))
        }
        SPOOL_VERSION => {
            let old: JournalV1 = serde_json::from_slice(bytes)
                .with_context(|| format!("parse legacy local journal {}", path.display()))?;
            let checkpointed_source_cursor = old
                .checkpointed_seq
                .and_then(|seq| old.objects.get(&seq))
                .map(|object| object.source_cursor.clone());
            let checkpoint_window = match old.objects.last_key_value() {
                Some((seq, object)) => CheckpointWindow::Opening {
                    seq: *seq,
                    source_cursor: object.source_cursor.clone(),
                },
                None => CheckpointWindow::Closed,
            };
            tracing::error!(
                path = %path.display(),
                "migrating v1 native spool journal conservatively; a non-empty spool requires a snapshot re-anchor"
            );
            Ok((
                Journal {
                    version: JOURNAL_VERSION,
                    identity: old.identity,
                    objects: old.objects,
                    local_base_seq: old.local_base_seq,
                    admitted_seq: old.admitted_seq,
                    checkpointed_seq: old.checkpointed_seq,
                    checkpointed_source_cursor,
                    remote_published_seq: old.remote_published_seq,
                    checkpoint_window,
                },
                true,
            ))
        }
        other => bail!("unsupported native spool journal version {other}"),
    }
}

fn validate_object_identity(identity: &SpoolIdentity, object: &SpoolObject) -> Result<()> {
    if object.version != SPOOL_VERSION
        || object.stream_digest != identity.stream_digest()
        || object.lineage_id != identity.lineage_id
        || object.bucket != identity.bucket
        || object.prefix != identity.prefix
        || object.database != identity.database
    {
        bail!(
            "native spool object identity/destination mismatch at seq {}",
            object.seq
        );
    }
    Ok(())
}

fn validate_payload(object: &SpoolObject, bytes: &[u8], _scratch_root: &Path) -> Result<()> {
    if bytes.len() as u64 != object.payload_length || sha256_hex(bytes) != object.payload_sha256 {
        bail!(
            "native spool payload length/digest mismatch at seq {}",
            object.seq
        );
    }
    let decoded = ltx::decode_sqlite_changeset(bytes)
        .with_context(|| format!("decode native spool payload seq {}", object.seq))?;
    if decoded.header.seq != object.seq
        || decoded.header.prev_checksum != object.previous_chain_checksum
    {
        bail!("native spool HADBP header mismatch at seq {}", object.seq);
    }
    let end_page_count = ltx::changeset_end_page_count(&decoded)?;
    match object.kind {
        ObjectKind::Delta => {
            if end_page_count != Some(object.end_page_count)
                || decoded.checksum != object.ending_chain_checksum
            {
                bail!(
                    "native spool delta checksum/page-count mismatch at seq {}",
                    object.seq
                );
            }
        }
        ObjectKind::Snapshot => {
            let (checksum, page_count) = ltx::snapshot_checksum_and_page_count(&decoded)?;
            if checksum != object.ending_chain_checksum {
                bail!(
                    "native spool snapshot ending checksum mismatch at seq {}",
                    object.seq
                );
            }
            if page_count != object.end_page_count {
                bail!(
                    "native spool snapshot page-count mismatch at seq {}: record {}, decoded {}",
                    object.seq,
                    object.end_page_count,
                    page_count
                );
            }
        }
    }
    Ok(())
}

fn same_immutable_object(a: &SpoolObject, b: &SpoolObject) -> bool {
    a.version == b.version
        && a.stream_digest == b.stream_digest
        && a.lineage_id == b.lineage_id
        && a.bucket == b.bucket
        && a.prefix == b.prefix
        && a.database == b.database
        && a.seq == b.seq
        && a.kind == b.kind
        && a.previous_chain_checksum == b.previous_chain_checksum
        && a.ending_chain_checksum == b.ending_chain_checksum
        && a.end_page_count == b.end_page_count
        && a.intended_remote_key == b.intended_remote_key
        && a.payload_length == b.payload_length
        && a.payload_sha256 == b.payload_sha256
        && a.source_cursor == b.source_cursor
}

fn install_payload(dir: &Path, final_path: &Path, bytes: &[u8]) -> Result<()> {
    let tmp = payload_temp_path(final_path);
    let mut file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&tmp)
        .with_context(|| format!("create native payload temp {}", tmp.display()))?;
    file.write_all(bytes)?;
    file.sync_all()?;
    drop(file);
    fs::rename(&tmp, final_path)?;
    sync_dir(dir)
}

fn payload_temp_path(final_path: &Path) -> PathBuf {
    final_path.with_extension("hadbp.tmp")
}

fn serialized_json_len<T: Serialize>(value: &T) -> Result<u64> {
    Ok(serde_json::to_vec_pretty(value)?.len() as u64)
}

fn acquire_owner_lock(root: &Path) -> Result<File> {
    let path = root.join("owner.lock");
    let file = OpenOptions::new()
        .create(true)
        .read(true)
        .write(true)
        .open(&path)
        .with_context(|| format!("open native spool ownership lock {}", path.display()))?;
    #[cfg(unix)]
    {
        let rc = unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) };
        if rc != 0 {
            let error = std::io::Error::last_os_error();
            if matches!(
                error.raw_os_error(),
                Some(code) if code == libc::EWOULDBLOCK || code == libc::EAGAIN
            ) {
                bail!(
                    "native spool {} is owned by an active watcher; stop watch before local restore or recovery",
                    root.display()
                );
            }
            return Err(error).with_context(|| {
                format!("acquire native spool ownership lock {}", path.display())
            });
        }
    }
    #[cfg(not(unix))]
    {
        bail!("native spool ownership locking is unsupported on this platform");
    }
    Ok(file)
}

fn persist_json<T: Serialize>(dir: &Path, path: &Path, value: &T) -> Result<()> {
    let bytes = serde_json::to_vec_pretty(value)?;
    let tmp = path.with_extension("json.tmp");
    let mut file = File::create(&tmp)?;
    file.write_all(&bytes)?;
    file.sync_all()?;
    drop(file);
    fs::rename(&tmp, path)?;
    sync_dir(dir)
}

fn remove_and_sync(path: &Path, dir: &Path) -> Result<()> {
    match fs::remove_file(path) {
        Ok(()) => sync_dir(dir),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(e) => Err(e.into()),
    }
}

fn sync_dir(path: &Path) -> Result<()> {
    File::open(path)
        .with_context(|| format!("open directory {} for fsync", path.display()))?
        .sync_all()
        .with_context(|| format!("fsync directory {}", path.display()))
}

fn directory_bytes(root: &Path) -> Result<u64> {
    let mut total = 0u64;
    for entry in walkdir::WalkDir::new(root) {
        let entry = entry?;
        if entry.file_type().is_file() {
            total = total.saturating_add(entry.metadata()?.len());
        }
    }
    Ok(total)
}

#[cfg(unix)]
fn filesystem_free_bytes(path: &Path) -> Result<u64> {
    use std::ffi::CString;
    use std::os::unix::ffi::OsStrExt;
    let c_path = CString::new(path.as_os_str().as_bytes())?;
    let mut stat: libc::statvfs = unsafe { std::mem::zeroed() };
    let rc = unsafe { libc::statvfs(c_path.as_ptr(), &mut stat) };
    if rc != 0 {
        return Err(std::io::Error::last_os_error()).context("statvfs native spool");
    }
    Ok((stat.f_bavail as u64).saturating_mul(stat.f_frsize as u64))
}

#[cfg(unix)]
pub fn filesystem_available_bytes(path: &Path) -> Result<u64> {
    filesystem_free_bytes(path)
}

#[cfg(not(unix))]
fn filesystem_free_bytes(_path: &Path) -> Result<u64> {
    // Windows support needs a platform API before local-first watch is enabled
    // there. Failing closed prevents a false capacity claim.
    bail!("native spool filesystem free-space accounting is unsupported on this platform")
}

#[cfg(not(unix))]
pub fn filesystem_available_bytes(path: &Path) -> Result<u64> {
    filesystem_free_bytes(path)
}

fn sha256_hex(bytes: &[u8]) -> String {
    hex_digest(Sha256::digest(bytes).as_slice())
}

fn hex_digest(bytes: &[u8]) -> String {
    let mut out = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
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
    use tempfile::tempdir;

    fn identity(db: &Path) -> SpoolIdentity {
        SpoolIdentity::new(db, "bucket", "prefix/", "db", "lineage-a", 1, None, true).unwrap()
    }

    fn snapshot(db: &Path, seq: u64, prev: u64) -> (Vec<u8>, u64, u64) {
        let conn = rusqlite::Connection::open(db).unwrap();
        conn.execute_batch("CREATE TABLE IF NOT EXISTS t(id INTEGER PRIMARY KEY, v TEXT); INSERT INTO t(v) VALUES ('a');").unwrap();
        let page_size = conn
            .query_row("PRAGMA page_size", [], |r| r.get::<_, u32>(0))
            .unwrap();
        drop(conn);
        let encoded = ltx::encode_snapshot_with_checksum(db, page_size, seq, prev).unwrap();
        let pages = fs::metadata(db).unwrap().len() / page_size as u64;
        (encoded.bytes, encoded.checksum, pages)
    }

    fn generous() -> CapacityPolicy {
        CapacityPolicy {
            warning_bytes: u64::MAX - 1,
            hard_bytes: u64::MAX,
            minimum_free_bytes: 0,
        }
    }

    #[test]
    fn stages_native_snapshot_durably_and_reopens() {
        let dir = tempdir().unwrap();
        let db = dir.path().join("db.sqlite");
        let (bytes, checksum, pages) = snapshot(&db, 1, 0);
        let root = NativeSpool::path_for(dir.path(), &identity(&db));
        let mut spool = NativeSpool::create_or_open(&root, identity(&db), generous()).unwrap();
        spool
            .stage(StageObject {
                seq: 1,
                kind: ObjectKind::Snapshot,
                previous_chain_checksum: 0,
                ending_chain_checksum: checksum,
                end_page_count: pages,
                intended_remote_key:
                    "prefix/db/native/v1/lineages/lineage-a/0001/0000000000000001.hadbp".into(),
                source_cursor: SourceCursor::snapshot(),
                payload: &bytes,
            })
            .unwrap();
        assert_eq!(spool.admitted_seq(), Some(1));
        drop(spool);
        let reopened = NativeSpool::create_or_open(&root, identity(&db), generous()).unwrap();
        assert_eq!(reopened.read_payload(1).unwrap(), bytes);
        assert!(NativeSpool::validate_existing_complete_base(&root, &identity(&db)).unwrap());
    }

    #[test]
    fn stage_installs_the_exact_prewritten_snapshot_temporary() {
        let dir = tempdir().unwrap();
        let db = dir.path().join("db.sqlite");
        let (bytes, checksum, pages) = snapshot(&db, 1, 0);
        let root = NativeSpool::path_for(dir.path(), &identity(&db));
        let mut spool = NativeSpool::create_or_open(&root, identity(&db), generous()).unwrap();
        let cursor = SourceCursor::snapshot();
        spool
            .prepare_snapshot(1, 0, "one.hadbp".into(), cursor.clone(), 4096)
            .unwrap();
        let tmp = spool.payload_temporary_path(ObjectKind::Snapshot, 1);
        let mut file = OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(&tmp)
            .unwrap();
        file.write_all(&bytes).unwrap();
        file.sync_all().unwrap();
        drop(file);
        #[cfg(unix)]
        let temp_inode = {
            use std::os::unix::fs::MetadataExt;
            fs::metadata(&tmp).unwrap().ino()
        };
        let object = spool
            .stage(StageObject {
                seq: 1,
                kind: ObjectKind::Snapshot,
                previous_chain_checksum: 0,
                ending_chain_checksum: checksum,
                end_page_count: pages,
                intended_remote_key: "one.hadbp".into(),
                source_cursor: cursor,
                payload: &bytes,
            })
            .unwrap();
        assert!(!tmp.exists());
        let installed = spool.payload_path(&object);
        assert_eq!(fs::read(&installed).unwrap(), bytes);
        #[cfg(unix)]
        {
            use std::os::unix::fs::MetadataExt;
            assert_eq!(
                fs::metadata(installed).unwrap().ino(),
                temp_inode,
                "admission must rename the encoder's exact fsynced payload, not rewrite it"
            );
        }
    }

    #[test]
    fn reopen_rejects_remote_cursor_ahead_of_object_publication_state() {
        let dir = tempdir().unwrap();
        let db = dir.path().join("db.sqlite");
        let (bytes, checksum, pages) = snapshot(&db, 1, 0);
        let id = identity(&db);
        let root = NativeSpool::path_for(dir.path(), &id);
        let mut spool = NativeSpool::create_or_open(&root, id.clone(), generous()).unwrap();
        spool
            .stage(StageObject {
                seq: 1,
                kind: ObjectKind::Snapshot,
                previous_chain_checksum: 0,
                ending_chain_checksum: checksum,
                end_page_count: pages,
                intended_remote_key: "one.hadbp".into(),
                source_cursor: SourceCursor::snapshot(),
                payload: &bytes,
            })
            .unwrap();

        // Simulate a torn/corrupt journal cursor that claims remote visibility
        // while the exact object and publish record remain locally pending.
        spool.journal.remote_published_seq = Some(1);
        spool.persist_journal().unwrap();
        drop(spool);

        let error = NativeSpool::create_or_open(&root, id, generous())
            .err()
            .expect("ahead remote cursor must fail spool reopen");
        assert!(
            format!("{error:#}").contains("pending upload state is inconsistent"),
            "unexpected reopen error: {error:#}"
        );
    }

    #[test]
    fn identity_only_journal_is_not_an_offline_restart_base() {
        let dir = tempdir().unwrap();
        let db = dir.path().join("db.sqlite");
        File::create(&db).unwrap();
        let id = identity(&db);
        let root = NativeSpool::path_for(dir.path(), &id);
        NativeSpool::create_or_open(&root, id.clone(), generous()).unwrap();
        assert!(!NativeSpool::validate_existing_complete_base(&root, &id).unwrap());
    }

    #[test]
    fn open_checkpoint_window_survives_restart_until_later_snapshot_reanchor() {
        let dir = tempdir().unwrap();
        let db = dir.path().join("db.sqlite");
        let (bytes, checksum, pages) = snapshot(&db, 1, 0);
        let id = identity(&db);
        let root = NativeSpool::path_for(dir.path(), &id);
        let mut spool = NativeSpool::create_or_open(&root, id.clone(), generous()).unwrap();
        spool
            .stage(StageObject {
                seq: 1,
                kind: ObjectKind::Snapshot,
                previous_chain_checksum: 0,
                ending_chain_checksum: checksum,
                end_page_count: pages,
                intended_remote_key: "snapshot-1.hadbp".into(),
                source_cursor: SourceCursor::snapshot(),
                payload: &bytes,
            })
            .unwrap();
        spool.begin_checkpoint_window(1).unwrap();
        drop(spool);

        let mut reopened = NativeSpool::create_or_open(&root, id, generous()).unwrap();
        assert!(reopened.requires_checkpoint_reanchor());
        assert_eq!(reopened.checkpointed_seq(), None);
        let (next, next_checksum, next_pages) = snapshot(&db, 2, checksum);
        reopened
            .stage(StageObject {
                seq: 2,
                kind: ObjectKind::Snapshot,
                previous_chain_checksum: checksum,
                ending_chain_checksum: next_checksum,
                end_page_count: next_pages,
                intended_remote_key: "snapshot-2.hadbp".into(),
                source_cursor: SourceCursor::snapshot(),
                payload: &next,
            })
            .unwrap();
        reopened.complete_checkpoint_reanchor(2).unwrap();
        assert!(!reopened.requires_checkpoint_reanchor());
    }

    #[test]
    fn v1_nonempty_journal_migration_fails_closed_to_reanchor() {
        let dir = tempdir().unwrap();
        let db = dir.path().join("db.sqlite");
        let (bytes, checksum, pages) = snapshot(&db, 1, 0);
        let id = identity(&db);
        let root = NativeSpool::path_for(dir.path(), &id);
        let mut spool = NativeSpool::create_or_open(&root, id.clone(), generous()).unwrap();
        spool
            .stage(StageObject {
                seq: 1,
                kind: ObjectKind::Snapshot,
                previous_chain_checksum: 0,
                ending_chain_checksum: checksum,
                end_page_count: pages,
                intended_remote_key: "snapshot-1.hadbp".into(),
                source_cursor: SourceCursor::snapshot(),
                payload: &bytes,
            })
            .unwrap();
        let legacy = JournalV1 {
            version: SPOOL_VERSION,
            identity: spool.journal.identity.clone(),
            objects: spool.journal.objects.clone(),
            local_base_seq: spool.journal.local_base_seq,
            admitted_seq: spool.journal.admitted_seq,
            checkpointed_seq: spool.journal.checkpointed_seq,
            remote_published_seq: spool.journal.remote_published_seq,
        };
        persist_json(&root, &spool.journal_path, &legacy).unwrap();
        drop(spool);

        let reopened = NativeSpool::create_or_open(&root, id, generous()).unwrap();
        assert!(reopened.requires_checkpoint_reanchor());
        let durable: Journal =
            serde_json::from_slice(&fs::read(root.join("journal.json")).unwrap()).unwrap();
        assert_eq!(durable.version, JOURNAL_VERSION);
    }

    #[test]
    fn admitted_snapshot_intent_reconciles_after_restart() {
        let dir = tempdir().unwrap();
        let db = dir.path().join("db.sqlite");
        let (bytes, checksum, pages) = snapshot(&db, 1, 0);
        let id = identity(&db);
        let root = NativeSpool::path_for(dir.path(), &id);
        let cursor = SourceCursor {
            shadow_generation: 7,
            shadow_frame_index: 11,
            wal_offset: 1234,
            wal_salt: Some((1, 2)),
            wal_checksum_chain: Some((3, 4)),
        };
        let mut spool = NativeSpool::create_or_open(&root, id.clone(), generous()).unwrap();
        let preparing = spool
            .prepare_snapshot(1, 0, "snapshot-1.hadbp".into(), cursor.clone(), 4096)
            .unwrap();
        spool
            .stage(StageObject {
                seq: 1,
                kind: ObjectKind::Snapshot,
                previous_chain_checksum: 0,
                ending_chain_checksum: checksum,
                end_page_count: pages,
                intended_remote_key: preparing.intended_remote_key,
                source_cursor: preparing.source_cursor,
                payload: &bytes,
            })
            .unwrap();
        drop(spool);

        let reopened = NativeSpool::create_or_open(&root, id, generous()).unwrap();
        assert_eq!(reopened.get(1).unwrap().source_cursor, cursor);
        assert!(!reopened.snapshot_intent_path.exists());
    }

    #[test]
    fn pre_direct_snapshot_stable_intent_migrates_once_to_hadbp() {
        let dir = tempdir().unwrap();
        let db = dir.path().join("db.sqlite");
        let (_bytes, expected_checksum, pages) = snapshot(&db, 1, 0);
        let id = identity(&db);
        let root = NativeSpool::path_for(dir.path(), &id);
        let mut spool = NativeSpool::create_or_open(&root, id.clone(), generous()).unwrap();
        spool
            .prepare_snapshot(
                1,
                0,
                "snapshot-1.hadbp".into(),
                SourceCursor::snapshot(),
                4096,
            )
            .unwrap();
        let stable_name = "0000000000000001.db";
        let stable = spool.snapshots_dir.join(stable_name);
        fs::copy(&db, &stable).unwrap();
        File::open(&stable).unwrap().sync_all().unwrap();
        let stable_bytes = fs::read(&stable).unwrap();
        let mut intent = spool.read_snapshot_intent().unwrap().unwrap();
        intent.legacy_stable_file_name = Some(stable_name.into());
        intent.state = SnapshotIntentState::Stable {
            payload_length: stable_bytes.len() as u64,
            sha256: sha256_hex(&stable_bytes),
        };
        persist_json(&spool.root, &spool.snapshot_intent_path, &intent).unwrap();
        drop(spool);

        let reopened = NativeSpool::create_or_open(&root, id, generous()).unwrap();
        let object = reopened.get(1).unwrap();
        assert_eq!(object.ending_chain_checksum, expected_checksum);
        assert_eq!(object.end_page_count, pages);
        assert!(reopened.read_payload(1).unwrap().starts_with(b"HADBP"));
        assert!(!stable.exists());
        assert!(!reopened.snapshot_intent_path.exists());
    }

    #[test]
    fn valid_unadmitted_snapshot_payload_is_adopted_on_restart() {
        let dir = tempdir().unwrap();
        let db = dir.path().join("db.sqlite");
        let (bytes, checksum, pages) = snapshot(&db, 1, 0);
        let id = identity(&db);
        let root = NativeSpool::path_for(dir.path(), &id);
        let mut spool = NativeSpool::create_or_open(&root, id.clone(), generous()).unwrap();
        spool
            .prepare_snapshot(
                1,
                0,
                "snapshot-1.hadbp".into(),
                SourceCursor::snapshot(),
                4096,
            )
            .unwrap();
        let tmp = spool.payload_temporary_path(ObjectKind::Snapshot, 1);
        fs::write(&tmp, &bytes).unwrap();
        File::open(&tmp).unwrap().sync_all().unwrap();
        drop(spool);

        let reopened = NativeSpool::create_or_open(&root, id, generous()).unwrap();
        assert!(reopened.read_snapshot_intent().unwrap().is_none());
        assert!(!tmp.exists());
        let object = reopened.get(1).unwrap();
        assert_eq!(object.ending_chain_checksum, checksum);
        assert_eq!(object.end_page_count, pages);
        assert_eq!(reopened.read_payload(1).unwrap(), bytes);
    }

    #[test]
    fn incomplete_unadmitted_snapshot_payload_is_removed_on_restart() {
        let dir = tempdir().unwrap();
        let db = dir.path().join("db.sqlite");
        rusqlite::Connection::open(&db).unwrap();
        let id = identity(&db);
        let root = NativeSpool::path_for(dir.path(), &id);
        let mut spool = NativeSpool::create_or_open(&root, id.clone(), generous()).unwrap();
        spool
            .prepare_snapshot(
                1,
                0,
                "snapshot-1.hadbp".into(),
                SourceCursor::snapshot(),
                4096,
            )
            .unwrap();
        let tmp = spool.payload_temporary_path(ObjectKind::Snapshot, 1);
        fs::write(&tmp, b"HADBP interrupted").unwrap();
        File::open(&tmp).unwrap().sync_all().unwrap();
        sync_dir(&spool.objects_dir).unwrap();
        drop(spool);

        let reopened = NativeSpool::create_or_open(&root, id, generous()).unwrap();
        assert!(reopened.read_snapshot_intent().unwrap().is_none());
        assert!(!tmp.exists());
        assert!(reopened.get(1).is_none());
    }

    #[test]
    fn snapshot_abandonment_preserves_durable_install_intent_and_payload() {
        let dir = tempdir().unwrap();
        let db = dir.path().join("db.sqlite");
        let (bytes, checksum, pages) = snapshot(&db, 1, 0);
        let id = identity(&db);
        let root = NativeSpool::path_for(dir.path(), &id);
        let mut spool = NativeSpool::create_or_open(&root, id.clone(), generous()).unwrap();
        let source_cursor = SourceCursor::snapshot();
        spool
            .prepare_snapshot(1, 0, "snapshot-1.hadbp".into(), source_cursor.clone(), 4096)
            .unwrap();
        let tmp = spool.payload_temporary_path(ObjectKind::Snapshot, 1);
        fs::write(&tmp, &bytes).unwrap();
        File::open(&tmp).unwrap().sync_all().unwrap();
        sync_dir(&spool.objects_dir).unwrap();
        let object = SpoolObject {
            version: SPOOL_VERSION,
            stream_digest: spool.identity().stream_digest(),
            lineage_id: spool.identity().lineage_id.clone(),
            bucket: spool.identity().bucket.clone(),
            prefix: spool.identity().prefix.clone(),
            database: spool.identity().database.clone(),
            seq: 1,
            kind: ObjectKind::Snapshot,
            previous_chain_checksum: 0,
            ending_chain_checksum: checksum,
            end_page_count: pages,
            intended_remote_key: "snapshot-1.hadbp".into(),
            payload_length: bytes.len() as u64,
            payload_sha256: sha256_hex(&bytes),
            source_cursor,
            local_creation_state: LocalCreationState::Installed,
            remote_upload_state: RemoteUploadState::Pending,
            created_unix_ms: unix_ms(),
            uploaded_unix_ms: None,
            published_unix_ms: None,
            publish_record_sha256: None,
        };
        persist_json(
            &spool.intents_dir,
            &spool.intent_path(1),
            &InstallIntent {
                version: SPOOL_VERSION,
                object,
            },
        )
        .unwrap();

        assert!(spool
            .abandon_unadmitted_snapshot(1)
            .unwrap_err()
            .to_string()
            .contains("installation is in progress"));
        assert!(tmp.exists());
        drop(spool);

        let reopened = NativeSpool::create_or_open(&root, id, generous()).unwrap();
        assert_eq!(reopened.read_payload(1).unwrap(), bytes);
        assert!(!tmp.exists());
    }

    #[test]
    fn valid_snapshot_temp_divergent_from_source_intent_is_retained_and_rejected() {
        let dir = tempdir().unwrap();
        let db = dir.path().join("db.sqlite");
        let (wrong_seq_bytes, _, _) = snapshot(&db, 2, 0);
        let id = identity(&db);
        let root = NativeSpool::path_for(dir.path(), &id);
        let mut spool = NativeSpool::create_or_open(&root, id.clone(), generous()).unwrap();
        spool
            .prepare_snapshot(
                1,
                0,
                "snapshot-1.hadbp".into(),
                SourceCursor::snapshot(),
                4096,
            )
            .unwrap();
        let tmp = spool.payload_temporary_path(ObjectKind::Snapshot, 1);
        fs::write(&tmp, wrong_seq_bytes).unwrap();
        File::open(&tmp).unwrap().sync_all().unwrap();
        sync_dir(&spool.objects_dir).unwrap();
        drop(spool);

        let error = NativeSpool::create_or_open(&root, id, generous())
            .err()
            .expect("divergent valid temp must fail startup");
        assert!(error
            .to_string()
            .contains("diverges from durable source intent"));
        assert!(tmp.exists(), "divergent evidence must be retained");
    }

    #[test]
    fn divergent_existing_sequence_is_rejected() {
        let dir = tempdir().unwrap();
        let db = dir.path().join("db.sqlite");
        let (bytes, checksum, pages) = snapshot(&db, 1, 0);
        let root = NativeSpool::path_for(dir.path(), &identity(&db));
        let mut spool = NativeSpool::create_or_open(&root, identity(&db), generous()).unwrap();
        let base = StageObject {
            seq: 1,
            kind: ObjectKind::Snapshot,
            previous_chain_checksum: 0,
            ending_chain_checksum: checksum,
            end_page_count: pages,
            intended_remote_key: "key-a.hadbp".into(),
            source_cursor: SourceCursor::snapshot(),
            payload: &bytes,
        };
        spool.stage(base.clone()).unwrap();
        let mut divergent = base;
        divergent.intended_remote_key = "key-b.hadbp".into();
        assert!(spool
            .stage(divergent)
            .unwrap_err()
            .to_string()
            .contains("equivocation"));
    }

    #[test]
    fn custom_root_is_collision_safe_and_identity_bound() {
        let dir = tempdir().unwrap();
        let db1 = dir.path().join("a.sqlite");
        let db2 = dir.path().join("b.sqlite");
        File::create(&db1).unwrap();
        File::create(&db2).unwrap();
        let a = identity(&db1);
        let b = SpoolIdentity::new(&db2, "bucket", "prefix/", "db", "lineage-a", 1, None, true)
            .unwrap();
        assert_ne!(
            NativeSpool::path_for(dir.path(), &a),
            NativeSpool::path_for(dir.path(), &b)
        );
        let root = NativeSpool::path_for(dir.path(), &a);
        NativeSpool::create_or_open(&root, a, generous()).unwrap();
        assert!(NativeSpool::create_or_open(&root, b, generous()).is_err());
    }

    #[test]
    fn local_path_digest_length_prefixes_identity_components_and_finds_v1() {
        let dir = tempdir().unwrap();
        let db_a = dir.path().join("a");
        let db_ab = dir.path().join("ab");
        File::create(&db_a).unwrap();
        File::create(&db_ab).unwrap();
        let first =
            SpoolIdentity::new(&db_a, "b", "prefix/", "db", "lineage-a", 1, None, true).unwrap();
        let second =
            SpoolIdentity::new(&db_ab, "", "prefix/", "db", "lineage-b", 1, None, true).unwrap();
        assert_eq!(
            first.legacy_local_path_digest(),
            second.legacy_local_path_digest(),
            "the old concatenation must reproduce the adversarial tuple collision"
        );
        assert_ne!(
            NativeSpool::path_for(dir.path(), &first),
            NativeSpool::path_for(dir.path(), &second),
            "v2 length-prefixing must separate the tuples"
        );

        let legacy = dir
            .path()
            .join("native-v1")
            .join(first.legacy_local_path_digest());
        let spool = NativeSpool::create_or_open(&legacy, first.clone(), generous()).unwrap();
        drop(spool);
        assert_eq!(
            NativeSpool::resolve_path_for(dir.path(), &first).unwrap(),
            legacy,
            "existing v1 spools must remain discoverable"
        );
        assert_eq!(
            NativeSpool::resolve_path_for(dir.path(), &second).unwrap(),
            NativeSpool::path_for(dir.path(), &second),
            "a colliding v1 tuple owned by another identity must not strand a new v2 stream"
        );
    }

    #[test]
    fn active_spool_owner_blocks_mutating_second_open() {
        let dir = tempdir().unwrap();
        let db = dir.path().join("db.sqlite");
        File::create(&db).unwrap();
        let id = identity(&db);
        let root = NativeSpool::path_for(dir.path(), &id);
        let owner = NativeSpool::create_or_open(&root, id.clone(), generous()).unwrap();
        let error = NativeSpool::create_or_open(&root, id.clone(), generous())
            .err()
            .expect("a second mutating opener must not race the active owner");
        assert!(format!("{error:#}").contains("active watcher"));
        drop(owner);
        NativeSpool::create_or_open(&root, id, generous())
            .expect("the OS lock must release when the owner closes");
    }

    #[test]
    fn hard_capacity_fails_before_installing_payload() {
        let dir = tempdir().unwrap();
        let db = dir.path().join("db.sqlite");
        let (bytes, checksum, pages) = snapshot(&db, 1, 0);
        let id = identity(&db);
        let root = NativeSpool::path_for(dir.path(), &id);
        let mut spool = NativeSpool::create_or_open(
            &root,
            id,
            CapacityPolicy {
                warning_bytes: 0,
                hard_bytes: 1,
                minimum_free_bytes: 0,
            },
        )
        .unwrap();
        let err = spool
            .stage(StageObject {
                seq: 1,
                kind: ObjectKind::Snapshot,
                previous_chain_checksum: 0,
                ending_chain_checksum: checksum,
                end_page_count: pages,
                intended_remote_key: "key.hadbp".into(),
                source_cursor: SourceCursor::snapshot(),
                payload: &bytes,
            })
            .unwrap_err();
        assert!(err.to_string().contains("local_spool_full"));
        assert_eq!(spool.admitted_seq(), None);
    }

    #[test]
    fn admission_reserves_intent_and_full_journal_rewrite_peak() {
        let dir = tempdir().unwrap();
        let db = dir.path().join("db.sqlite");
        let (bytes, checksum, pages) = snapshot(&db, 1, 0);
        let id = identity(&db);
        let root = NativeSpool::path_for(dir.path(), &id);
        let initial = NativeSpool::create_or_open(&root, id.clone(), generous()).unwrap();
        let used = initial.used_bytes().unwrap();
        drop(initial);
        let old_payload_only_peak = (bytes.len() as u64).saturating_mul(2);
        let hard = used.saturating_add(old_payload_only_peak).saturating_add(1);
        let mut spool = NativeSpool::create_or_open(
            &root,
            id,
            CapacityPolicy {
                warning_bytes: hard,
                hard_bytes: hard,
                minimum_free_bytes: 0,
            },
        )
        .unwrap();
        let error = spool
            .stage(StageObject {
                seq: 1,
                kind: ObjectKind::Snapshot,
                previous_chain_checksum: 0,
                ending_chain_checksum: checksum,
                end_page_count: pages,
                intended_remote_key: "key.hadbp".into(),
                source_cursor: SourceCursor::snapshot(),
                payload: &bytes,
            })
            .unwrap_err();
        assert!(format!("{error:#}").contains("local_spool_full"));
        assert_eq!(spool.admitted_seq(), None);
        assert!(
            !spool.intent_path(1).exists(),
            "capacity refusal must happen before intent creation"
        );
    }

    #[test]
    fn journal_peak_dominates_after_thousand_tiny_objects() {
        let dir = tempdir().unwrap();
        let db = dir.path().join("db.sqlite");
        let (snapshot_bytes, mut checksum, pages) = snapshot(&db, 1, 0);
        let id = identity(&db);
        let root = NativeSpool::path_for(dir.path(), &id);
        let mut spool = NativeSpool::create_or_open(&root, id.clone(), generous()).unwrap();
        spool
            .stage(StageObject {
                seq: 1,
                kind: ObjectKind::Snapshot,
                previous_chain_checksum: 0,
                ending_chain_checksum: checksum,
                end_page_count: pages,
                intended_remote_key: "snapshot.hadbp".into(),
                source_cursor: SourceCursor::snapshot(),
                payload: &snapshot_bytes,
            })
            .unwrap();
        let page_size = 4096u32;
        for seq in 2..=1001u64 {
            let page = vec![(seq & 0xff) as u8; page_size as usize];
            let (payload, ending) = ltx::encode_wal_changes_with_end_page_count(
                &[(1, page)],
                page_size,
                seq,
                checksum,
                pages,
            )
            .unwrap();
            let mut cursor = SourceCursor::snapshot();
            cursor.shadow_frame_index = seq;
            spool
                .stage(StageObject {
                    seq,
                    kind: ObjectKind::Delta,
                    previous_chain_checksum: checksum,
                    ending_chain_checksum: ending,
                    end_page_count: pages,
                    intended_remote_key: format!("{seq:016x}.hadbp"),
                    source_cursor: cursor,
                    payload: &payload,
                })
                .unwrap();
            checksum = ending;
        }
        let journal_len = std::fs::metadata(root.join("journal.json")).unwrap().len();
        let page = vec![0xA5; page_size as usize];
        let (payload, ending) = ltx::encode_wal_changes_with_end_page_count(
            &[(1, page)],
            page_size,
            1002,
            checksum,
            pages,
        )
        .unwrap();
        assert!(
            journal_len > payload.len() as u64 * 10,
            "test setup must make journal rewrite, not payload, the dominant peak"
        );
        let used = spool.used_bytes().unwrap();
        drop(spool);
        let old_payload_only_hard = used
            .saturating_add(payload.len() as u64 * 2)
            .saturating_add(1);
        let mut tight = NativeSpool::create_or_open(
            &root,
            id,
            CapacityPolicy {
                warning_bytes: old_payload_only_hard,
                hard_bytes: old_payload_only_hard,
                minimum_free_bytes: 0,
            },
        )
        .unwrap();
        let mut cursor = SourceCursor::snapshot();
        cursor.shadow_frame_index = 1002;
        let error = tight
            .stage(StageObject {
                seq: 1002,
                kind: ObjectKind::Delta,
                previous_chain_checksum: checksum,
                ending_chain_checksum: ending,
                end_page_count: pages,
                intended_remote_key: "0000000000001002.hadbp".into(),
                source_cursor: cursor,
                payload: &payload,
            })
            .unwrap_err();
        assert!(format!("{error:#}").contains("local_spool_full"));
        assert_eq!(tight.admitted_seq(), Some(1001));
    }

    #[test]
    fn warning_watermark_is_distinct_from_hard_capacity() {
        let dir = tempdir().unwrap();
        let db = dir.path().join("db.sqlite");
        File::create(&db).unwrap();
        let id = identity(&db);
        let root = NativeSpool::path_for(dir.path(), &id);
        let spool = NativeSpool::create_or_open(
            &root,
            id,
            CapacityPolicy {
                warning_bytes: 1,
                hard_bytes: u64::MAX,
                minimum_free_bytes: 0,
            },
        )
        .unwrap();
        assert_eq!(spool.capacity_state(0).unwrap(), CapacityState::High);
    }

    #[test]
    fn valid_payload_orphan_with_durable_intent_is_adopted() {
        let dir = tempdir().unwrap();
        let db = dir.path().join("db.sqlite");
        let (bytes, checksum, pages) = snapshot(&db, 1, 0);
        let id = identity(&db);
        let root = NativeSpool::path_for(dir.path(), &id);
        let spool = NativeSpool::create_or_open(&root, id.clone(), generous()).unwrap();
        let payload_sha256 = sha256_hex(&bytes);
        let object = SpoolObject {
            version: SPOOL_VERSION,
            stream_digest: id.stream_digest(),
            lineage_id: id.lineage_id.clone(),
            bucket: id.bucket.clone(),
            prefix: id.prefix.clone(),
            database: id.database.clone(),
            seq: 1,
            kind: ObjectKind::Snapshot,
            previous_chain_checksum: 0,
            ending_chain_checksum: checksum,
            end_page_count: pages,
            intended_remote_key: "key.hadbp".into(),
            payload_length: bytes.len() as u64,
            payload_sha256,
            source_cursor: SourceCursor::snapshot(),
            local_creation_state: LocalCreationState::Installed,
            remote_upload_state: RemoteUploadState::Pending,
            created_unix_ms: unix_ms(),
            uploaded_unix_ms: None,
            published_unix_ms: None,
            publish_record_sha256: None,
        };
        persist_json(
            &spool.intents_dir,
            &spool.intent_path(1),
            &InstallIntent {
                version: SPOOL_VERSION,
                object: object.clone(),
            },
        )
        .unwrap();
        install_payload(&spool.objects_dir, &spool.payload_path(&object), &bytes).unwrap();
        drop(spool); // crash before journal commit

        let reopened = NativeSpool::create_or_open(&root, id, generous()).unwrap();
        assert_eq!(reopened.admitted_seq(), Some(1));
        assert_eq!(reopened.read_payload(1).unwrap(), bytes);
        assert!(!reopened.intent_path(1).exists());
    }

    #[test]
    fn fsynced_payload_temp_with_durable_intent_is_finished_and_adopted() {
        let dir = tempdir().unwrap();
        let db = dir.path().join("db.sqlite");
        let (bytes, checksum, pages) = snapshot(&db, 1, 0);
        let id = identity(&db);
        let root = NativeSpool::path_for(dir.path(), &id);
        let spool = NativeSpool::create_or_open(&root, id.clone(), generous()).unwrap();
        let object = SpoolObject {
            version: SPOOL_VERSION,
            stream_digest: id.stream_digest(),
            lineage_id: id.lineage_id.clone(),
            bucket: id.bucket.clone(),
            prefix: id.prefix.clone(),
            database: id.database.clone(),
            seq: 1,
            kind: ObjectKind::Snapshot,
            previous_chain_checksum: 0,
            ending_chain_checksum: checksum,
            end_page_count: pages,
            intended_remote_key: "key.hadbp".into(),
            payload_length: bytes.len() as u64,
            payload_sha256: sha256_hex(&bytes),
            source_cursor: SourceCursor::snapshot(),
            local_creation_state: LocalCreationState::Installed,
            remote_upload_state: RemoteUploadState::Pending,
            created_unix_ms: unix_ms(),
            uploaded_unix_ms: None,
            published_unix_ms: None,
            publish_record_sha256: None,
        };
        persist_json(
            &spool.intents_dir,
            &spool.intent_path(1),
            &InstallIntent {
                version: SPOOL_VERSION,
                object: object.clone(),
            },
        )
        .unwrap();
        let final_path = spool.payload_path(&object);
        let tmp = payload_temp_path(&final_path);
        let mut file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&tmp)
            .unwrap();
        file.write_all(&bytes).unwrap();
        file.sync_all().unwrap();
        drop(file);
        drop(spool); // crash before payload rename and journal commit

        let reopened = NativeSpool::create_or_open(&root, id, generous()).unwrap();
        assert_eq!(reopened.admitted_seq(), Some(1));
        assert_eq!(reopened.read_payload(1).unwrap(), bytes);
        assert!(final_path.exists());
        assert!(!tmp.exists());
        assert!(!reopened.intent_path(1).exists());
    }

    #[test]
    fn unproven_payload_orphan_is_retained_and_rejected() {
        let dir = tempdir().unwrap();
        let db = dir.path().join("db.sqlite");
        let (bytes, _checksum, _pages) = snapshot(&db, 1, 0);
        let id = identity(&db);
        let root = NativeSpool::path_for(dir.path(), &id);
        let spool = NativeSpool::create_or_open(&root, id.clone(), generous()).unwrap();
        let orphan = spool.objects_dir.join("0001-0000000000000001.hadbp");
        install_payload(&spool.objects_dir, &orphan, &bytes).unwrap();
        drop(spool);
        let error = match NativeSpool::create_or_open(&root, id, generous()) {
            Ok(_) => panic!("unproven orphan must fail startup"),
            Err(error) => error,
        };
        assert!(error.to_string().contains("unproven native HADBP orphan"));
        assert!(orphan.exists(), "unproven recovery data must be retained");
    }

    #[test]
    fn cleanup_never_deletes_newest_local_snapshot_base_or_descendants() {
        let dir = tempdir().unwrap();
        let db = dir.path().join("db.sqlite");
        let (first, first_checksum, first_pages) = snapshot(&db, 1, 0);
        let id = identity(&db);
        let root = NativeSpool::path_for(dir.path(), &id);
        let mut spool = NativeSpool::create_or_open(&root, id, generous()).unwrap();
        let mut first_cursor = SourceCursor::snapshot();
        first_cursor.shadow_frame_index = 5;
        spool
            .stage(StageObject {
                seq: 1,
                kind: ObjectKind::Snapshot,
                previous_chain_checksum: 0,
                ending_chain_checksum: first_checksum,
                end_page_count: first_pages,
                intended_remote_key: "one.hadbp".into(),
                source_cursor: first_cursor,
                payload: &first,
            })
            .unwrap();
        spool.mark_uploaded(1).unwrap();
        spool.mark_published(1, b"record-one").unwrap();
        spool.begin_checkpoint_window(1).unwrap();
        spool.close_checkpoint_window(1, true, None).unwrap();

        let conn = rusqlite::Connection::open(&db).unwrap();
        conn.execute("INSERT INTO t(v) VALUES ('second')", [])
            .unwrap();
        drop(conn);
        let page_size = rusqlite::Connection::open(&db)
            .unwrap()
            .query_row("PRAGMA page_size", [], |r| r.get::<_, u32>(0))
            .unwrap();
        let second = ltx::encode_snapshot_with_checksum(&db, page_size, 2, first_checksum).unwrap();
        let second_pages = fs::metadata(&db).unwrap().len() / page_size as u64;
        let mut second_cursor = SourceCursor::snapshot();
        second_cursor.shadow_frame_index = 10;
        spool
            .stage(StageObject {
                seq: 2,
                kind: ObjectKind::Snapshot,
                previous_chain_checksum: first_checksum,
                ending_chain_checksum: second.checksum,
                end_page_count: second_pages,
                intended_remote_key: "two.hadbp".into(),
                source_cursor: second_cursor,
                payload: &second.bytes,
            })
            .unwrap();
        spool.mark_uploaded(2).unwrap();
        spool.mark_published(2, b"record-two").unwrap();

        assert_eq!(spool.cleanup_published_before_latest_snapshot().unwrap(), 1);
        assert!(spool.get(1).is_none());
        assert!(spool.get(2).is_some());
        assert_eq!(
            spool.admitted_frames_since_checkpoint(),
            5,
            "checkpoint source cursor must survive cleanup of its object record"
        );
        assert_eq!(spool.read_payload(2).unwrap(), second.bytes);
        drop(spool);
        let reopened = NativeSpool::create_or_open(&root, identity(&db), generous()).unwrap();
        assert!(reopened.get(2).is_some());
        assert_eq!(reopened.admitted_frames_since_checkpoint(), 5);
        let expired_locally = dir.path().join("local-pit-before-base.sqlite");
        assert_eq!(
            crate::native_restore::restore_local_spool(&reopened, &expired_locally, Some(1))
                .unwrap(),
            None,
            "a PIT below the retained local base must fall through to remote restore"
        );
        assert!(!expired_locally.exists());
        let restored = dir.path().join("local-restore.sqlite");
        assert_eq!(
            crate::native_restore::restore_local_spool(&reopened, &restored, None).unwrap(),
            Some(2)
        );
        let restored = rusqlite::Connection::open(restored).unwrap();
        assert_eq!(
            restored
                .query_row("SELECT count(*) FROM t", [], |row| row.get::<_, i64>(0))
                .unwrap(),
            2
        );
    }

    #[test]
    fn complete_base_preflight_accepts_only_proven_interrupted_cleanup() {
        let dir = tempdir().unwrap();
        let db = dir.path().join("db.sqlite");
        let (first, first_checksum, first_pages) = snapshot(&db, 1, 0);
        let id = identity(&db);
        let root = NativeSpool::path_for(dir.path(), &id);
        let mut spool = NativeSpool::create_or_open(&root, id.clone(), generous()).unwrap();
        spool
            .stage(StageObject {
                seq: 1,
                kind: ObjectKind::Snapshot,
                previous_chain_checksum: 0,
                ending_chain_checksum: first_checksum,
                end_page_count: first_pages,
                intended_remote_key: "one.hadbp".into(),
                source_cursor: SourceCursor::snapshot(),
                payload: &first,
            })
            .unwrap();
        spool.mark_uploaded(1).unwrap();
        spool.mark_published(1, b"record-one").unwrap();
        let (second, second_checksum, second_pages) = snapshot(&db, 2, first_checksum);
        spool
            .stage(StageObject {
                seq: 2,
                kind: ObjectKind::Snapshot,
                previous_chain_checksum: first_checksum,
                ending_chain_checksum: second_checksum,
                end_page_count: second_pages,
                intended_remote_key: "two.hadbp".into(),
                source_cursor: SourceCursor::snapshot(),
                payload: &second,
            })
            .unwrap();
        spool.mark_uploaded(2).unwrap();
        spool.mark_published(2, b"record-two").unwrap();
        spool
            .journal
            .objects
            .get_mut(&1)
            .unwrap()
            .local_creation_state = LocalCreationState::Deleting;
        spool.persist_journal().unwrap();

        assert!(NativeSpool::validate_existing_complete_base(&root, &id).unwrap());
        remove_and_sync(
            &spool.payload_path(spool.journal.objects.get(&1).unwrap()),
            &spool.objects_dir,
        )
        .unwrap();
        assert!(
            NativeSpool::validate_existing_complete_base(&root, &id).unwrap(),
            "the newer installed snapshot remains complete after victim payload deletion"
        );

        spool
            .journal
            .objects
            .get_mut(&1)
            .unwrap()
            .local_creation_state = LocalCreationState::Installed;
        spool.persist_journal().unwrap();
        assert!(
            NativeSpool::validate_existing_complete_base(&root, &id).is_err(),
            "a missing ordinary base payload must fail loudly"
        );
    }

    #[test]
    fn interrupted_cleanup_never_deletes_a_pending_object() {
        let dir = tempdir().unwrap();
        let db = dir.path().join("db.sqlite");
        let (bytes, checksum, pages) = snapshot(&db, 1, 0);
        let id = identity(&db);
        let root = NativeSpool::path_for(dir.path(), &id);
        let mut spool = NativeSpool::create_or_open(&root, id.clone(), generous()).unwrap();
        spool
            .stage(StageObject {
                seq: 1,
                kind: ObjectKind::Snapshot,
                previous_chain_checksum: 0,
                ending_chain_checksum: checksum,
                end_page_count: pages,
                intended_remote_key: "one.hadbp".into(),
                source_cursor: SourceCursor::snapshot(),
                payload: &bytes,
            })
            .unwrap();
        let (second, second_checksum, second_pages) = snapshot(&db, 2, checksum);
        spool
            .stage(StageObject {
                seq: 2,
                kind: ObjectKind::Snapshot,
                previous_chain_checksum: checksum,
                ending_chain_checksum: second_checksum,
                end_page_count: second_pages,
                intended_remote_key: "two.hadbp".into(),
                source_cursor: SourceCursor::snapshot(),
                payload: &second,
            })
            .unwrap();
        let payload_path = spool.payload_path(spool.get(1).unwrap());
        spool
            .journal
            .objects
            .get_mut(&1)
            .unwrap()
            .local_creation_state = LocalCreationState::Deleting;
        spool.persist_journal().unwrap();
        drop(spool);

        let error = NativeSpool::create_or_open(&root, id, generous())
            .err()
            .expect("pending cleanup victim must fail spool reopen");
        assert!(
            format!("{error:#}").contains("not a published prefix"),
            "unexpected reopen error: {error:#}"
        );
        assert!(
            payload_path.exists(),
            "fail-closed cleanup validation must retain pending HADBP bytes"
        );
    }

    #[test]
    fn interrupted_cleanup_validates_replacement_chain_before_unlinking_old_base() {
        let dir = tempdir().unwrap();
        let db = dir.path().join("db.sqlite");
        let (first, first_checksum, first_pages) = snapshot(&db, 1, 0);
        let id = identity(&db);
        let root = NativeSpool::path_for(dir.path(), &id);
        let mut spool = NativeSpool::create_or_open(&root, id.clone(), generous()).unwrap();
        spool
            .stage(StageObject {
                seq: 1,
                kind: ObjectKind::Snapshot,
                previous_chain_checksum: 0,
                ending_chain_checksum: first_checksum,
                end_page_count: first_pages,
                intended_remote_key: "one.hadbp".into(),
                source_cursor: SourceCursor::snapshot(),
                payload: &first,
            })
            .unwrap();
        spool.mark_uploaded(1).unwrap();
        spool.mark_published(1, b"record-one").unwrap();
        let (second, second_checksum, second_pages) = snapshot(&db, 2, first_checksum);
        spool
            .stage(StageObject {
                seq: 2,
                kind: ObjectKind::Snapshot,
                previous_chain_checksum: first_checksum,
                ending_chain_checksum: second_checksum,
                end_page_count: second_pages,
                intended_remote_key: "two.hadbp".into(),
                source_cursor: SourceCursor::snapshot(),
                payload: &second,
            })
            .unwrap();
        spool.mark_uploaded(2).unwrap();
        spool.mark_published(2, b"record-two").unwrap();
        let old_base_path = spool.payload_path(spool.get(1).unwrap());
        let replacement_path = spool.payload_path(spool.get(2).unwrap());
        spool
            .journal
            .objects
            .get_mut(&1)
            .unwrap()
            .local_creation_state = LocalCreationState::Deleting;
        spool.persist_journal().unwrap();
        fs::write(&replacement_path, b"corrupt replacement snapshot").unwrap();
        drop(spool);

        let error = NativeSpool::create_or_open(&root, id, generous())
            .err()
            .expect("corrupt replacement base must fail cleanup recovery");
        assert!(
            format!("{error:#}").contains("validate retained native cleanup payload"),
            "unexpected reopen error: {error:#}"
        );
        assert!(
            old_base_path.exists(),
            "cleanup must validate the replacement chain before unlinking the old base"
        );
    }
}
