//! Restore for the versioned native CLI publication layout.

use crate::ltx;
use crate::native_publish::{PublishRecord, StreamDescriptor, REMOTE_LAYOUT_VERSION};
use crate::native_spool::NativeSpool;
use crate::native_spool::ObjectKind;
use anyhow::{anyhow, bail, Context, Result};
use hadb_storage::StorageBackend;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::fs::{self, File};
use std::path::{Path, PathBuf};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum NativeRestoreAvailability {
    /// No descriptor or no published native base; legacy history remains active.
    LegacyOnly,
    /// The requested PIT is at/before the verified legacy boundary.
    LegacyPoint { boundary_txid: u64 },
    /// Native restore completed at this sequence.
    Restored { seq: u64 },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NativeVisibleState {
    pub head_seq: u64,
    pub object_count: usize,
    pub latest_snapshot_seq: u64,
    pub retention_floor_seq: u64,
    pub snapshot_seqs: Vec<u64>,
    pub legacy_boundary_txid: Option<u64>,
}

pub const RETENTION_FLOOR_VERSION: u32 = 1;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RetentionFloor {
    pub version: u32,
    pub stream_digest: String,
    pub lineage_id: String,
    pub floor_seq: u64,
    pub snapshot_publish_sha256: String,
    pub previous_publish_sha256: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NativePruneOutcome {
    pub floor_seq: u64,
    pub deleted_objects: usize,
    pub visible_head_seq: u64,
}

/// Discover only the contiguous, chain-verified published native recovery
/// point. Raw objects or records beyond a gap are deliberately invisible.
pub async fn inspect_native_v1(
    storage: &dyn StorageBackend,
    bucket: &str,
    prefix: &str,
    database: &str,
) -> Result<Option<NativeVisibleState>> {
    let descriptor_key = format!("{}{database}/native/v1/stream.json", prefix);
    let Some(bytes) = storage.get(&descriptor_key).await? else {
        return Ok(None);
    };
    let descriptor: StreamDescriptor = serde_json::from_slice(&bytes)?;
    validate_descriptor(&descriptor, bucket, prefix, database)?;
    let records = load_visible_records(storage, &descriptor).await?;
    let Some((head, _)) = records.last() else {
        return Ok(None);
    };
    let latest_snapshot_seq = records
        .iter()
        .rev()
        .find(|(record, _)| record.kind == ObjectKind::Snapshot)
        .map(|(record, _)| record.seq)
        .ok_or_else(|| anyhow!("visible native chain has no snapshot base"))?;
    let snapshot_seqs = records
        .iter()
        .filter(|(record, _)| record.kind == ObjectKind::Snapshot)
        .map(|(record, _)| record.seq)
        .collect::<Vec<_>>();
    Ok(Some(NativeVisibleState {
        head_seq: head.seq,
        object_count: records.len(),
        latest_snapshot_seq,
        retention_floor_seq: records.first().unwrap().0.seq,
        snapshot_seqs,
        legacy_boundary_txid: descriptor.legacy_boundary_txid,
    }))
}

pub fn restore_local_spool(
    spool: &NativeSpool,
    output: &Path,
    point_in_time: Option<u64>,
) -> Result<Option<u64>> {
    let head = match spool.admitted_seq() {
        Some(seq) => seq,
        None => return Ok(None),
    };
    let target = point_in_time.unwrap_or(head);
    if target < spool.identity().first_native_seq || target > head {
        return Ok(None);
    }
    let objects = spool
        .objects()
        .filter(|object| object.seq <= target)
        .collect::<Vec<_>>();
    let snapshot_index = objects
        .iter()
        .rposition(|object| object.kind == ObjectKind::Snapshot)
        .ok_or_else(|| anyhow!("local native spool has no snapshot at/before seq {target}"))?;
    let tmp = restore_temp_path(output);
    remove_if_exists(&tmp)?;
    let result = (|| -> Result<u64> {
        let snapshot = objects[snapshot_index];
        let snapshot_bytes = spool.read_payload(snapshot.seq)?;
        let restored = ltx::decode_to_db(&snapshot_bytes, &tmp)?;
        if restored.checksum != snapshot.ending_chain_checksum {
            bail!(
                "local native snapshot checksum mismatch at seq {}",
                snapshot.seq
            );
        }
        let mut checksum = restored.checksum;
        let mut seq = snapshot.seq;
        for object in objects.iter().skip(snapshot_index + 1) {
            if object.kind == ObjectKind::Snapshot {
                bail!("unexpected snapshot inside local native apply suffix");
            }
            let bytes = spool.read_payload(object.seq)?;
            let applied = ltx::apply_changeset_to_db(&bytes, &tmp, checksum)?;
            if applied.checksum != object.ending_chain_checksum {
                bail!("local native delta checksum mismatch at seq {}", object.seq);
            }
            checksum = applied.checksum;
            seq = object.seq;
        }
        validate_sqlite_integrity(&tmp)?;
        File::open(&tmp)?.sync_all()?;
        fs::rename(&tmp, output)?;
        sync_parent(output)?;
        Ok(seq)
    })();
    if result.is_err() {
        let _ = remove_if_exists(&tmp);
    }
    result.map(Some)
}

/// Verify every object at the contiguous visible native head. Returns `None`
/// when no native descriptor/base is visible.
pub async fn verify_native_v1(
    storage: &dyn StorageBackend,
    bucket: &str,
    prefix: &str,
    database: &str,
) -> Result<Option<usize>> {
    let descriptor_key = format!("{}{database}/native/v1/stream.json", prefix);
    let Some(bytes) = storage.get(&descriptor_key).await? else {
        return Ok(None);
    };
    let descriptor: StreamDescriptor = serde_json::from_slice(&bytes)?;
    validate_descriptor(&descriptor, bucket, prefix, database)?;
    let records = load_visible_records(storage, &descriptor).await?;
    if records.is_empty() {
        return Ok(None);
    }
    let scratch = std::env::temp_dir().join(format!(
        ".walrust-native-verify-{}-{}.db",
        std::process::id(),
        descriptor.lineage_id
    ));
    remove_if_exists(&scratch)?;
    let verify_result = (|| async {
        let mut checksum = None;
        for (record, _) in &records {
            let payload = get_verified_object(storage, &descriptor, record).await?;
            match record.kind {
                ObjectKind::Snapshot => {
                    remove_if_exists(&scratch)?;
                    let result = ltx::decode_to_db(&payload, &scratch)?;
                    if result.checksum != record.ending_chain_checksum {
                        bail!(
                            "native snapshot ending checksum mismatch at seq {}",
                            record.seq
                        );
                    }
                    checksum = Some(result.checksum);
                }
                ObjectKind::Delta => {
                    let previous = checksum.ok_or_else(|| {
                        anyhow!("native delta {} has no restored snapshot base", record.seq)
                    })?;
                    let result = ltx::apply_changeset_to_db(&payload, &scratch, previous)?;
                    if result.checksum != record.ending_chain_checksum {
                        bail!(
                            "native delta ending checksum mismatch at seq {}",
                            record.seq
                        );
                    }
                    checksum = Some(result.checksum);
                }
            }
        }
        validate_sqlite_integrity(&scratch)?;
        Ok::<(), anyhow::Error>(())
    })()
    .await;
    let cleanup_result = remove_if_exists(&scratch);
    verify_result?;
    cleanup_result?;
    Ok(Some(records.len()))
}

pub async fn restore_native_v1(
    storage: &dyn StorageBackend,
    bucket: &str,
    prefix: &str,
    database: &str,
    output: &Path,
    point_in_time: Option<u64>,
) -> Result<NativeRestoreAvailability> {
    let descriptor_key = format!("{}{database}/native/v1/stream.json", prefix);
    let Some(descriptor_bytes) = storage.get(&descriptor_key).await? else {
        return Ok(NativeRestoreAvailability::LegacyOnly);
    };
    let descriptor: StreamDescriptor =
        serde_json::from_slice(&descriptor_bytes).context("decode native CLI stream descriptor")?;
    validate_descriptor(&descriptor, bucket, prefix, database)?;

    if let (Some(target), Some(boundary)) = (point_in_time, descriptor.legacy_boundary_txid) {
        if target <= boundary {
            return Ok(NativeRestoreAvailability::LegacyPoint {
                boundary_txid: boundary,
            });
        }
    }

    let records = load_visible_records(storage, &descriptor).await?;
    if records.is_empty() {
        return Ok(NativeRestoreAvailability::LegacyOnly);
    }
    let visible_head = records.last().unwrap().0.seq;
    let target = point_in_time.unwrap_or(visible_head);
    if target < descriptor.first_native_seq {
        if let Some(boundary) = descriptor.legacy_boundary_txid {
            return Ok(NativeRestoreAvailability::LegacyPoint {
                boundary_txid: boundary,
            });
        }
        bail!(
            "native PIT {} predates stream base {} and no legacy boundary exists",
            target,
            descriptor.first_native_seq
        );
    }
    if target > visible_head {
        bail!(
            "native PIT {} is beyond contiguous published head {}",
            target,
            visible_head
        );
    }
    let retention_floor = records.first().unwrap().0.seq;
    if target < retention_floor {
        bail!(
            "native PIT {} intentionally expired below retention floor {}",
            target,
            retention_floor
        );
    }

    let target_records = records
        .iter()
        .filter(|(record, _)| record.seq <= target)
        .collect::<Vec<_>>();
    let snapshot_index = target_records
        .iter()
        .rposition(|(record, _)| record.kind == ObjectKind::Snapshot)
        .ok_or_else(|| anyhow!("native published chain has no snapshot base at/before {target}"))?;

    let tmp = restore_temp_path(output);
    remove_if_exists(&tmp)?;
    let result = async {
        let snapshot = &target_records[snapshot_index].0;
        let snapshot_bytes = get_verified_object(storage, &descriptor, snapshot).await?;
        let restored = ltx::decode_to_db(&snapshot_bytes, &tmp)?;
        if restored.header.seq != snapshot.seq
            || restored.checksum != snapshot.ending_chain_checksum
        {
            bail!(
                "native snapshot restore checksum mismatch at seq {}",
                snapshot.seq
            );
        }
        let mut checksum = restored.checksum;
        let mut applied_seq = snapshot.seq;
        for (record, _) in target_records.iter().skip(snapshot_index + 1) {
            let bytes = get_verified_object(storage, &descriptor, record).await?;
            match record.kind {
                ObjectKind::Snapshot => {
                    bail!("unexpected snapshot inside selected native apply suffix")
                }
                ObjectKind::Delta => {
                    let applied = ltx::apply_changeset_to_db(&bytes, &tmp, checksum)?;
                    if applied.header.seq != record.seq
                        || applied.checksum != record.ending_chain_checksum
                    {
                        bail!("native delta restore mismatch at seq {}", record.seq);
                    }
                    checksum = applied.checksum;
                    applied_seq = record.seq;
                }
            }
        }
        validate_sqlite_integrity(&tmp)?;
        let file = File::open(&tmp)?;
        file.sync_all()?;
        fs::rename(&tmp, output)?;
        sync_parent(output)?;
        Ok::<u64, anyhow::Error>(applied_seq)
    }
    .await;
    if result.is_err() {
        let _ = remove_if_exists(&tmp);
    }
    result.map(|seq| NativeRestoreAvailability::Restored { seq })
}

async fn load_visible_records(
    storage: &dyn StorageBackend,
    descriptor: &StreamDescriptor,
) -> Result<Vec<(PublishRecord, Vec<u8>)>> {
    let publish_prefix = format!(
        "{}{}/native/v1/lineages/{}/published/",
        descriptor.prefix, descriptor.database, descriptor.lineage_id,
    );
    let mut keys = storage.list(&publish_prefix, None).await?;
    keys.sort();
    let floor = load_retention_floor(storage, descriptor).await?;
    let mut expected_seq = floor
        .as_ref()
        .map(|floor| floor.floor_seq)
        .unwrap_or(descriptor.first_native_seq);
    let mut previous_publish_digest = floor
        .as_ref()
        .and_then(|floor| floor.previous_publish_sha256.clone());
    let mut previous_chain_checksum: Option<u64> = None;
    let mut records = Vec::<(PublishRecord, Vec<u8>)>::new();
    for key in keys {
        let Some(seq) = parse_record_seq(&key, &publish_prefix) else {
            continue;
        };
        if seq < expected_seq {
            continue;
        }
        if seq != expected_seq {
            break; // a gap is the visible head, not permission to skip ahead
        }
        let bytes = storage
            .get(&key)
            .await?
            .ok_or_else(|| anyhow!("native publish record vanished during restore: {key}"))?;
        let record: PublishRecord = serde_json::from_slice(&bytes)
            .with_context(|| format!("decode native publish record {key}"))?;
        validate_record(
            &descriptor,
            &record,
            expected_seq,
            previous_publish_digest.as_deref(),
            previous_chain_checksum,
        )?;
        // A publish record without its exact immutable payload is corruption,
        // not a shorter visible chain. Every metadata-only consumer (inspect,
        // list, prune) therefore gets the same payload proof as restore.
        get_verified_object(storage, descriptor, &record).await?;
        previous_publish_digest = Some(sha256_hex(&bytes));
        previous_chain_checksum = Some(record.ending_chain_checksum);
        records.push((record, bytes));
        expected_seq = expected_seq
            .checked_add(1)
            .ok_or_else(|| anyhow!("native restore sequence overflow"))?;
    }

    Ok(records)
}

async fn load_retention_floor(
    storage: &dyn StorageBackend,
    descriptor: &StreamDescriptor,
) -> Result<Option<RetentionFloor>> {
    let floor_prefix = format!(
        "{}{}/native/v1/retention/v1/",
        descriptor.prefix, descriptor.database
    );
    let mut candidates = storage
        .list(&floor_prefix, None)
        .await?
        .into_iter()
        .filter_map(|key| parse_record_seq(&key, &floor_prefix).map(|seq| (seq, key)))
        .collect::<Vec<_>>();
    candidates.sort_by_key(|(seq, _)| *seq);
    let Some((key_seq, key)) = candidates.pop() else {
        return Ok(None);
    };
    let bytes = storage
        .get(&key)
        .await?
        .ok_or_else(|| anyhow!("native retention floor vanished during discovery: {key}"))?;
    let floor: RetentionFloor = serde_json::from_slice(&bytes)
        .with_context(|| format!("decode native retention floor {key}"))?;
    if floor.version != RETENTION_FLOOR_VERSION
        || floor.stream_digest != descriptor.stream_digest
        || floor.lineage_id != descriptor.lineage_id
        || floor.floor_seq != key_seq
        || floor.floor_seq < descriptor.first_native_seq
    {
        bail!("native retention floor identity/sequence mismatch at {key}");
    }
    let publish_key = format!(
        "{}{}/native/v1/lineages/{}/published/{:016x}.json",
        descriptor.prefix, descriptor.database, descriptor.lineage_id, floor.floor_seq
    );
    let publish_bytes = storage.get(&publish_key).await?.ok_or_else(|| {
        anyhow!("native retention floor snapshot record is missing: {publish_key}")
    })?;
    if sha256_hex(&publish_bytes) != floor.snapshot_publish_sha256 {
        bail!("native retention floor snapshot publish digest mismatch");
    }
    let record: PublishRecord = serde_json::from_slice(&publish_bytes)?;
    validate_record(
        descriptor,
        &record,
        floor.floor_seq,
        floor.previous_publish_sha256.as_deref(),
        None,
    )?;
    if record.kind != ObjectKind::Snapshot {
        bail!("native retention floor does not name a snapshot");
    }
    get_verified_object(storage, descriptor, &record).await?;
    Ok(Some(floor))
}

pub async fn prune_native_before_snapshot(
    storage: &dyn StorageBackend,
    bucket: &str,
    prefix: &str,
    database: &str,
    floor_seq: u64,
) -> Result<NativePruneOutcome> {
    let descriptor_key = format!("{}{database}/native/v1/stream.json", prefix);
    let descriptor_bytes = storage
        .get(&descriptor_key)
        .await?
        .ok_or_else(|| anyhow!("native stream descriptor is missing"))?;
    let descriptor: StreamDescriptor = serde_json::from_slice(&descriptor_bytes)?;
    validate_descriptor(&descriptor, bucket, prefix, database)?;
    let records = load_visible_records(storage, &descriptor).await?;
    let head_seq = records
        .last()
        .map(|(record, _)| record.seq)
        .ok_or_else(|| anyhow!("native stream has no visible snapshot base"))?;
    let (snapshot, snapshot_bytes) = records
        .iter()
        .find(|(record, _)| record.seq == floor_seq)
        .ok_or_else(|| anyhow!("native retention floor seq {floor_seq} is not visible"))?;
    if snapshot.kind != ObjectKind::Snapshot {
        bail!("native retention floor seq {floor_seq} is not a snapshot");
    }
    let floor = RetentionFloor {
        version: RETENTION_FLOOR_VERSION,
        stream_digest: descriptor.stream_digest.clone(),
        lineage_id: descriptor.lineage_id.clone(),
        floor_seq,
        snapshot_publish_sha256: sha256_hex(snapshot_bytes),
        previous_publish_sha256: snapshot.previous_publish_sha256.clone(),
    };
    let floor_key = format!(
        "{}{}/native/v1/retention/v1/{floor_seq:016x}.json",
        descriptor.prefix, descriptor.database
    );
    put_immutable_exact(storage, &floor_key, &serde_json::to_vec_pretty(&floor)?).await?;

    let verified = load_visible_records(storage, &descriptor).await?;
    if verified.last().map(|(record, _)| record.seq) != Some(head_seq)
        || verified.first().map(|(record, _)| record.seq) != Some(floor_seq)
    {
        bail!("native retention floor did not preserve the prior visible head");
    }

    let victims = records
        .iter()
        .filter(|(record, _)| record.seq < floor_seq)
        .map(|(record, _)| record.clone())
        .collect::<Vec<_>>();
    for record in &victims {
        let publish_key = format!(
            "{}{}/native/v1/lineages/{}/published/{:016x}.json",
            descriptor.prefix, descriptor.database, descriptor.lineage_id, record.seq
        );
        storage.delete(&publish_key).await?;
        storage.delete(&record.object_key).await?;
    }
    Ok(NativePruneOutcome {
        floor_seq,
        deleted_objects: victims.len(),
        visible_head_seq: head_seq,
    })
}

async fn put_immutable_exact(storage: &dyn StorageBackend, key: &str, bytes: &[u8]) -> Result<()> {
    let result = storage.put_if_absent(key, bytes).await?;
    if result.success {
        return Ok(());
    }
    let existing = storage
        .get(key)
        .await?
        .ok_or_else(|| anyhow!("native retention floor vanished after CAS conflict"))?;
    if existing != bytes {
        bail!("split brain/equivocation: divergent native retention floor at {key}");
    }
    Ok(())
}

fn validate_descriptor(
    descriptor: &StreamDescriptor,
    bucket: &str,
    prefix: &str,
    database: &str,
) -> Result<()> {
    if descriptor.version != REMOTE_LAYOUT_VERSION
        || descriptor.bucket != bucket
        || descriptor.prefix != prefix
        || descriptor.database != database
        || descriptor.first_native_seq == 0
        || descriptor.lineage_id.is_empty()
    {
        bail!("native CLI stream descriptor identity/layout mismatch");
    }
    let mut digest = Sha256::new();
    digest.update(descriptor.version.to_be_bytes());
    for value in [
        descriptor.bucket.as_str(),
        descriptor.prefix.as_str(),
        descriptor.database.as_str(),
        descriptor.lineage_id.as_str(),
    ] {
        digest.update((value.len() as u64).to_be_bytes());
        digest.update(value.as_bytes());
    }
    digest.update(descriptor.first_native_seq.to_be_bytes());
    digest.update(descriptor.legacy_boundary_txid.unwrap_or(0).to_be_bytes());
    let digest = digest.finalize();
    let mut expected_digest = String::with_capacity(64);
    for byte in digest {
        use std::fmt::Write as _;
        let _ = write!(&mut expected_digest, "{byte:02x}");
    }
    if descriptor.stream_digest != expected_digest {
        bail!("native CLI stream descriptor digest mismatch");
    }
    Ok(())
}

fn validate_record(
    descriptor: &StreamDescriptor,
    record: &PublishRecord,
    expected_seq: u64,
    previous_publish_digest: Option<&str>,
    previous_chain_checksum: Option<u64>,
) -> Result<()> {
    if record.version != REMOTE_LAYOUT_VERSION
        || record.stream_digest != descriptor.stream_digest
        || record.lineage_id != descriptor.lineage_id
        || record.seq != expected_seq
        || record.previous_publish_sha256.as_deref() != previous_publish_digest
    {
        bail!("native publish record identity/sequence/predecessor mismatch at {expected_seq}");
    }
    if let Some(checksum) = previous_chain_checksum {
        if record.previous_chain_checksum != checksum {
            bail!("native publish checksum chain mismatch at {expected_seq}");
        }
    } else if record.kind != ObjectKind::Snapshot {
        bail!("native publish chain does not begin with a snapshot");
    }
    let generation = match record.kind {
        ObjectKind::Snapshot => 1,
        ObjectKind::Delta => 0,
    };
    let canonical = format!(
        "{}{}/native/v1/lineages/{}/{generation:04x}/{:016x}.hadbp",
        descriptor.prefix, descriptor.database, descriptor.lineage_id, record.seq
    );
    if record.object_key != canonical {
        bail!("native publish record has noncanonical object key at {expected_seq}");
    }
    Ok(())
}

async fn get_verified_object(
    storage: &dyn StorageBackend,
    descriptor: &StreamDescriptor,
    record: &PublishRecord,
) -> Result<Vec<u8>> {
    let bytes = storage
        .get(&record.object_key)
        .await?
        .ok_or_else(|| anyhow!("published native object is missing: {}", record.object_key))?;
    if bytes.len() as u64 != record.payload_length || sha256_hex(&bytes) != record.payload_sha256 {
        bail!(
            "published native object length/digest mismatch at seq {}",
            record.seq
        );
    }
    let decoded = ltx::decode_sqlite_changeset(&bytes)?;
    if decoded.header.seq != record.seq
        || decoded.header.prev_checksum != record.previous_chain_checksum
    {
        bail!(
            "published native HADBP header mismatch at seq {}",
            record.seq
        );
    }
    let marker = ltx::changeset_end_page_count(&decoded)?;
    match record.kind {
        ObjectKind::Snapshot => {
            if marker.is_some() {
                bail!("published native snapshot has delta marker");
            }
            let scratch = std::env::temp_dir().join(format!(
                ".walrust-native-object-verify-{}-{}.db",
                std::process::id(),
                uuid::Uuid::new_v4()
            ));
            remove_if_exists(&scratch)?;
            let decoded_result = ltx::decode_to_db(&bytes, &scratch);
            let page_count = decoded_result.as_ref().ok().and_then(|result| {
                fs::metadata(&scratch)
                    .ok()
                    .map(|metadata| metadata.len() / result.header.page_size as u64)
            });
            let cleanup_result = remove_if_exists(&scratch);
            let result = decoded_result?;
            cleanup_result?;
            if result.checksum != record.ending_chain_checksum
                || page_count != Some(record.end_page_count)
            {
                bail!(
                    "published native snapshot checksum/page-count mismatch at seq {}",
                    record.seq
                );
            }
        }
        ObjectKind::Delta => {
            if marker != Some(record.end_page_count)
                || decoded.checksum != record.ending_chain_checksum
            {
                bail!(
                    "published native delta checksum/end-page mismatch at seq {}",
                    record.seq
                );
            }
        }
    }
    if record.lineage_id != descriptor.lineage_id {
        bail!("published native object lineage mismatch");
    }
    Ok(bytes)
}

fn parse_record_seq(key: &str, prefix: &str) -> Option<u64> {
    let stem = key.strip_prefix(prefix)?.strip_suffix(".json")?;
    if stem.contains('/') || stem.len() != 16 {
        return None;
    }
    u64::from_str_radix(stem, 16).ok()
}

fn restore_temp_path(output: &Path) -> PathBuf {
    let name = output
        .file_name()
        .and_then(|s| s.to_str())
        .unwrap_or("restore.db");
    output.with_file_name(format!(".{name}.walrust-native-restore.tmp"))
}

fn remove_if_exists(path: &Path) -> Result<()> {
    match fs::remove_file(path) {
        Ok(()) => Ok(()),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(e) => Err(e.into()),
    }
}

fn sync_parent(path: &Path) -> Result<()> {
    let parent = path.parent().unwrap_or_else(|| Path::new("."));
    File::open(parent)?.sync_all()?;
    Ok(())
}

fn validate_sqlite_integrity(path: &Path) -> Result<()> {
    let conn = rusqlite::Connection::open(path)?;
    let result: String = conn.query_row("PRAGMA integrity_check", [], |row| row.get(0))?;
    if result != "ok" {
        bail!("native restored database failed integrity_check: {result}");
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::native_spool::{ObjectKind, SpoolIdentity};
    use async_trait::async_trait;
    use hadb_storage::CasResult;
    use std::collections::BTreeMap;
    use std::sync::Mutex;
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
                .filter(|key| key.starts_with(prefix))
                .cloned()
                .collect())
        }

        async fn put_if_absent(&self, _key: &str, _data: &[u8]) -> Result<CasResult> {
            bail!("unused")
        }

        async fn put_if_match(&self, _key: &str, _data: &[u8], _etag: &str) -> Result<CasResult> {
            bail!("unused")
        }
    }

    #[tokio::test]
    async fn record_beyond_missing_snapshot_base_does_not_create_visible_head() {
        let dir = tempdir().unwrap();
        let db = dir.path().join("db.sqlite");
        File::create(&db).unwrap();
        let identity =
            SpoolIdentity::new(&db, "bucket", "p/", "db", "lineage", 1, None, true).unwrap();
        let descriptor = StreamDescriptor::from(&identity);
        let storage = MemoryStorage::default();
        storage
            .put(&descriptor.key(), &descriptor.bytes().unwrap())
            .await
            .unwrap();
        let stray = PublishRecord {
            version: REMOTE_LAYOUT_VERSION,
            stream_digest: descriptor.stream_digest.clone(),
            lineage_id: descriptor.lineage_id.clone(),
            seq: 2,
            kind: ObjectKind::Delta,
            previous_publish_sha256: Some("missing-base".into()),
            previous_chain_checksum: 1,
            ending_chain_checksum: 2,
            end_page_count: 1,
            object_key: format!(
                "p/db/native/v1/lineages/{}/0000/0000000000000002.hadbp",
                descriptor.lineage_id
            ),
            payload_length: 1,
            payload_sha256: "00".repeat(32),
        };
        storage
            .put(
                &stray.key(&descriptor.prefix, &descriptor.database),
                &stray.bytes().unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(
            inspect_native_v1(&storage, "bucket", "p/", "db")
                .await
                .unwrap(),
            None
        );
    }
}
