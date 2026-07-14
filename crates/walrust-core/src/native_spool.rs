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
use std::collections::BTreeMap;
use std::fs::{self, File, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

pub const SPOOL_VERSION: u32 = 1;

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
    pub wal_salt: Option<[u32; 2]>,
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
        let canonical = fs::canonicalize(db_path).with_context(|| {
            format!("canonicalize spool database path {}", db_path.display())
        })?;
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

    fn local_path_digest(&self) -> String {
        let mut h = Sha256::new();
        h.update(b"walrust-native-spool-path-v1");
        h.update(self.canonical_db_path.as_bytes());
        h.update(self.bucket.as_bytes());
        h.update(self.prefix.as_bytes());
        h.update(self.database.as_bytes());
        hex_digest(h.finalize().as_slice())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum LocalCreationState {
    Installed,
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
    admitted_seq: Option<u64>,
    checkpointed_seq: Option<u64>,
    remote_published_seq: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct InstallIntent {
    version: u32,
    object: SpoolObject,
}

pub struct NativeSpool {
    root: PathBuf,
    objects_dir: PathBuf,
    intents_dir: PathBuf,
    journal_path: PathBuf,
    journal: Journal,
    capacity: CapacityPolicy,
}

impl NativeSpool {
    /// Return the collision-safe directory for this stream below a configured
    /// spool root.  The full identity is still persisted and compared on open;
    /// the digest is namespace isolation, not the identity proof by itself.
    pub fn path_for(base: &Path, identity: &SpoolIdentity) -> PathBuf {
        base.join("native-v1").join(identity.local_path_digest())
    }


    pub fn read_identity(root: &Path) -> Result<Option<SpoolIdentity>> {
        let path = root.join("journal.json");
        let bytes = match fs::read(&path) {
            Ok(bytes) => bytes,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(e) => return Err(e).with_context(|| format!("read {}", path.display())),
        };
        let journal: Journal = serde_json::from_slice(&bytes)
            .with_context(|| format!("parse {}", path.display()))?;
        if journal.version != SPOOL_VERSION || journal.identity.version != SPOOL_VERSION {
            bail!("unsupported native spool journal/identity version at {}", path.display());
        }
        Ok(Some(journal.identity))
    }

    pub fn create_or_open(
        root: &Path,
        identity: SpoolIdentity,
        capacity: CapacityPolicy,
    ) -> Result<Self> {
        if identity.version != SPOOL_VERSION {
            bail!("unsupported native spool identity version {}", identity.version);
        }
        if capacity.warning_bytes > capacity.hard_bytes {
            bail!("spool warning watermark exceeds hard capacity");
        }
        fs::create_dir_all(root)
            .with_context(|| format!("create native spool root {}", root.display()))?;
        sync_dir(root.parent().unwrap_or_else(|| Path::new(".")))?;
        let objects_dir = root.join("objects");
        let intents_dir = root.join("intents");
        fs::create_dir_all(&objects_dir)?;
        fs::create_dir_all(&intents_dir)?;
        sync_dir(root)?;

        let journal_path = root.join("journal.json");
        let journal = match fs::read(&journal_path) {
            Ok(bytes) => {
                let journal: Journal = serde_json::from_slice(&bytes)
                    .with_context(|| format!("parse {}", journal_path.display()))?;
                if journal.version != SPOOL_VERSION {
                    bail!("unsupported native spool journal version {}", journal.version);
                }
                if journal.identity != identity {
                    bail!(
                        "native spool identity mismatch at {}; refusing cross-stream reuse",
                        root.display()
                    );
                }
                journal
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                let journal = Journal {
                    version: SPOOL_VERSION,
                    identity,
                    objects: BTreeMap::new(),
                    admitted_seq: None,
                    checkpointed_seq: None,
                    remote_published_seq: None,
                };
                persist_json(root, &journal_path, &journal)?;
                journal
            }
            Err(e) => return Err(e).context("read native spool journal"),
        };

        let mut spool = Self {
            root: root.to_path_buf(),
            objects_dir,
            intents_dir,
            journal_path,
            journal,
            capacity,
        };
        spool.verify_journal_payloads()?;
        spool.reconcile_orphans()?;
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

    pub fn read_payload(&self, seq: u64) -> Result<Vec<u8>> {
        let object = self
            .get(seq)
            .ok_or_else(|| anyhow!("native spool has no sequence {seq}"))?;
        let bytes = fs::read(self.payload_path(object))?;
        validate_payload(object, &bytes, &self.root)?;
        Ok(bytes)
    }

    /// Install immutable HADBP bytes and atomically admit the matching object
    /// record. Returning success is the local checkpoint-release proof.
    pub fn stage(&mut self, stage: StageObject<'_>) -> Result<SpoolObject> {
        if stage.seq == 0 {
            bail!("native spool sequence 0 is invalid");
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
                    return Ok(existing.clone());
                }
            }
            bail!(
                "native spool equivocation at sequence {}: existing object differs",
                stage.seq
            );
        }
        self.validate_next_object(&object)?;

        // Peak accounting includes both the caller's eventual installed object
        // and the same-directory temporary payload.
        self.ensure_capacity(stage.payload.len() as u64 * 2)?;

        let intent_path = self.intent_path(stage.seq);
        persist_json(
            &self.intents_dir,
            &intent_path,
            &InstallIntent {
                version: SPOOL_VERSION,
                object: object.clone(),
            },
        )?;

        let payload_path = self.payload_path(&object);
        install_payload(&self.objects_dir, &payload_path, stage.payload)?;

        self.journal.objects.insert(stage.seq, object.clone());
        self.journal.admitted_seq = Some(stage.seq);
        self.persist_journal()?;

        remove_and_sync(&intent_path, &self.intents_dir)?;
        Ok(object)
    }

    pub fn mark_checkpointed(&mut self, seq: u64) -> Result<()> {
        if !self.journal.objects.contains_key(&seq) {
            bail!("cannot checkpoint unadmitted native spool sequence {seq}");
        }
        self.journal.checkpointed_seq = Some(seq);
        self.persist_journal()
    }

    pub fn mark_uploaded(&mut self, seq: u64) -> Result<()> {
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
        self.persist_journal()
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
        self.persist_journal()
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
        if used.saturating_add(additional_peak_bytes) > self.capacity.hard_bytes
            || free.saturating_sub(additional_peak_bytes) < self.capacity.minimum_free_bytes
        {
            Ok(CapacityState::Full)
        } else if used.saturating_add(additional_peak_bytes) >= self.capacity.warning_bytes {
            Ok(CapacityState::High)
        } else {
            Ok(CapacityState::Healthy)
        }
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
                seq.checked_add(1)
                    .ok_or_else(|| anyhow!("native spool sequence overflow"))?
            }
            None => self.journal.identity.first_native_seq,
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
            if let Some(previous) = prior {
                if object.seq != previous.seq + 1
                    || object.previous_chain_checksum != previous.ending_chain_checksum
                {
                    bail!("native spool journal contains a sequence/checksum gap at {seq}");
                }
            } else if object.seq != self.journal.identity.first_native_seq
                || object.kind != ObjectKind::Snapshot
            {
                bail!("native spool journal does not begin at its snapshot base");
            }
            let bytes = fs::read(self.payload_path(object)).with_context(|| {
                format!("read admitted native payload for sequence {seq}")
            })?;
            validate_payload(object, &bytes, &self.root)?;
            prior = Some(object);
        }
        Ok(())
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
                    // Intent was durable but payload rename never happened.
                    remove_and_sync(&path, &self.intents_dir)?;
                    continue;
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

    fn intent_path(&self, seq: u64) -> PathBuf {
        self.intents_dir.join(format!("{seq:016x}.json"))
    }

    fn persist_journal(&self) -> Result<()> {
        persist_json(&self.root, &self.journal_path, &self.journal)
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
        bail!("native spool object identity/destination mismatch at seq {}", object.seq);
    }
    Ok(())
}

fn validate_payload(object: &SpoolObject, bytes: &[u8], scratch_root: &Path) -> Result<()> {
    if bytes.len() as u64 != object.payload_length || sha256_hex(bytes) != object.payload_sha256 {
        bail!("native spool payload length/digest mismatch at seq {}", object.seq);
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
                bail!("native spool delta checksum/page-count mismatch at seq {}", object.seq);
            }
        }
        ObjectKind::Snapshot => {
            if end_page_count.is_some() {
                bail!("native spool snapshot has a delta end-page marker");
            }
            // Snapshot continuation uses the raw database checksum, not the
            // physical changeset checksum. Reconstruct to a private temporary
            // file so orphan adoption recomputes that value rather than trusting
            // journal metadata.
            let tmp = scratch_root.join(format!(".verify-{:016x}.db", object.seq));
            let _ = fs::remove_file(&tmp);
            let decoded_result = ltx::decode_to_db(bytes, &tmp);
            let remove_result = match fs::remove_file(&tmp) {
                Ok(()) => Ok(()),
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
                Err(e) => Err(e),
            };
            let result = decoded_result?;
            remove_result?;
            if result.checksum != object.ending_chain_checksum {
                bail!("native spool snapshot ending checksum mismatch at seq {}", object.seq);
            }
            if fs::metadata(&tmp).is_ok() {
                bail!("native spool snapshot verification temporary was not removed");
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
    let tmp = final_path.with_extension("hadbp.tmp");
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

#[cfg(not(unix))]
fn filesystem_free_bytes(_path: &Path) -> Result<u64> {
    // Windows support needs a platform API before local-first watch is enabled
    // there. Failing closed prevents a false capacity claim.
    bail!("native spool filesystem free-space accounting is unsupported on this platform")
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
        SpoolIdentity::new(db, "bucket", "prefix/", "db", "lineage-a", 1, None, true)
            .unwrap()
    }

    fn snapshot(db: &Path, seq: u64, prev: u64) -> (Vec<u8>, u64, u64) {
        let conn = rusqlite::Connection::open(db).unwrap();
        conn.execute_batch("CREATE TABLE IF NOT EXISTS t(id INTEGER PRIMARY KEY, v TEXT); INSERT INTO t(v) VALUES ('a');").unwrap();
        let page_size = conn.query_row("PRAGMA page_size", [], |r| r.get::<_, u32>(0)).unwrap();
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
        spool.stage(StageObject {
            seq: 1,
            kind: ObjectKind::Snapshot,
            previous_chain_checksum: 0,
            ending_chain_checksum: checksum,
            end_page_count: pages,
            intended_remote_key: "prefix/db/native/v1/lineages/lineage-a/0001/0000000000000001.hadbp".into(),
            source_cursor: SourceCursor::snapshot(),
            payload: &bytes,
        }).unwrap();
        assert_eq!(spool.admitted_seq(), Some(1));
        drop(spool);
        let reopened = NativeSpool::create_or_open(&root, identity(&db), generous()).unwrap();
        assert_eq!(reopened.read_payload(1).unwrap(), bytes);
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
        assert!(spool.stage(divergent).unwrap_err().to_string().contains("equivocation"));
    }

    #[test]
    fn custom_root_is_collision_safe_and_identity_bound() {
        let dir = tempdir().unwrap();
        let db1 = dir.path().join("a.sqlite");
        let db2 = dir.path().join("b.sqlite");
        File::create(&db1).unwrap();
        File::create(&db2).unwrap();
        let a = identity(&db1);
        let b = SpoolIdentity::new(&db2, "bucket", "prefix/", "db", "lineage-a", 1, None, true).unwrap();
        assert_ne!(NativeSpool::path_for(dir.path(), &a), NativeSpool::path_for(dir.path(), &b));
        let root = NativeSpool::path_for(dir.path(), &a);
        NativeSpool::create_or_open(&root, a, generous()).unwrap();
        assert!(NativeSpool::create_or_open(&root, b, generous()).is_err());
    }

    #[test]
    fn hard_capacity_fails_before_installing_payload() {
        let dir = tempdir().unwrap();
        let db = dir.path().join("db.sqlite");
        let (bytes, checksum, pages) = snapshot(&db, 1, 0);
        let id = identity(&db);
        let root = NativeSpool::path_for(dir.path(), &id);
        let mut spool = NativeSpool::create_or_open(&root, id, CapacityPolicy {
            warning_bytes: 0,
            hard_bytes: 1,
            minimum_free_bytes: 0,
        }).unwrap();
        let err = spool.stage(StageObject {
            seq: 1,
            kind: ObjectKind::Snapshot,
            previous_chain_checksum: 0,
            ending_chain_checksum: checksum,
            end_page_count: pages,
            intended_remote_key: "key.hadbp".into(),
            source_cursor: SourceCursor::snapshot(),
            payload: &bytes,
        }).unwrap_err();
        assert!(err.to_string().contains("local_spool_full"));
        assert_eq!(spool.admitted_seq(), None);
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
}
