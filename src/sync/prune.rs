use anyhow::Result;
use chrono::Utc;
use hadb_storage_s3::S3Storage;
use walrust_core::native_publish::StreamDescriptor;

use crate::errors::{classify_or_else, WalrustError};
use crate::retention::{RetentionPolicy, SnapshotEntry};
use crate::s3::{self, create_client, parse_bucket};

fn native_retention_floor(
    snapshots: &[SnapshotEntry],
    policy: &RetentionPolicy,
    now: chrono::DateTime<Utc>,
) -> Option<u64> {
    let mut tier_policy = policy.clone();
    tier_policy.minimum = 0;
    let tier_plan = crate::retention::analyze_retention(snapshots, &tier_policy, now);
    let mut keep = tier_plan
        .keep
        .iter()
        .map(|entry| entry.sequence)
        .collect::<std::collections::BTreeSet<_>>();
    if keep.len() < policy.minimum {
        let mut newest = snapshots.iter().collect::<Vec<_>>();
        newest.sort_by_key(|entry| std::cmp::Reverse(entry.sequence));
        for entry in newest {
            keep.insert(entry.sequence);
            if keep.len() >= policy.minimum {
                break;
            }
        }
    }
    keep.into_iter().next()
}

pub async fn prune(
    name: &str,
    bucket: &str,
    endpoint: Option<&str>,
    policy: &RetentionPolicy,
    force: bool,
) -> Result<()> {
    let (bucket_name, prefix) = parse_bucket(bucket);
    let client = create_client(endpoint)
        .await
        .map_err(|error| classify_or_else(error, WalrustError::s3))?;
    prune_with_client(&client, &bucket_name, &prefix, name, policy, force).await
}

pub(crate) async fn prune_with_client(
    client: &aws_sdk_s3::Client,
    bucket_name: &str,
    prefix: &str,
    name: &str,
    policy: &RetentionPolicy,
    force: bool,
) -> Result<()> {
    let descriptor_key = format!("{}{}/native/v1/stream.json", prefix, name);
    let descriptor_bytes = match s3::download_bytes(client, bucket_name, &descriptor_key).await {
        Ok(bytes) => bytes,
        Err(error) if s3::download_error_is_not_found(&error) => {
            println!("No native-v1 stream found for database '{}'", name);
            return Ok(());
        }
        Err(error) => return Err(classify_or_else(error, WalrustError::s3)),
    };
    let descriptor: StreamDescriptor = serde_json::from_slice(&descriptor_bytes)?;
    let storage = S3Storage::new(client.clone(), bucket_name.to_string());
    let native =
        walrust_core::native_restore::inspect_native_v1(&storage, bucket_name, prefix, name)
            .await
            .map_err(|error| {
                if error
                    .chain()
                    .any(|cause| cause.is::<walrust_core::native_restore::NativeStorageError>())
                {
                    classify_or_else(error, WalrustError::s3)
                } else {
                    classify_or_else(error, WalrustError::integrity)
                }
            })?
            .ok_or_else(|| {
                WalrustError::restore(format!(
                    "native-v1 stream '{}' has no contiguous published snapshot base",
                    name
                ))
            })?;

    let mut snapshots = Vec::with_capacity(native.snapshot_seqs.len());
    for seq in &native.snapshot_seqs {
        let key = format!(
            "{}{}/native/v1/lineages/{}/published/{seq:016x}.json",
            prefix, name, descriptor.lineage_id
        );
        let meta = s3::head_object_meta(client, bucket_name, &key)
            .await
            .map_err(|error| classify_or_else(error, WalrustError::s3))?;
        snapshots.push(SnapshotEntry {
            key,
            created_at: meta.last_modified,
            sequence: *seq,
            size: meta.size,
        });
    }
    let floor = native_retention_floor(&snapshots, policy, Utc::now())
        .unwrap_or(native.retention_floor_seq);
    println!(
        "Native HADBP retention: keep floor sequence {} through visible head {}",
        floor, native.head_seq
    );
    if floor <= native.retention_floor_seq {
        println!("Nothing to delete - native snapshots fit retention policy.");
        return Ok(());
    }
    if !force {
        println!("Native HADBP prune is a dry run; use --force to advance the floor.");
        return Ok(());
    }
    let outcome = walrust_core::native_restore::prune_native_before_snapshot(
        &storage,
        bucket_name,
        prefix,
        name,
        floor,
    )
    .await
    .map_err(|error| {
        if error
            .chain()
            .any(|cause| cause.is::<walrust_core::native_restore::NativeStorageError>())
        {
            classify_or_else(error, WalrustError::s3)
        } else {
            classify_or_else(error, WalrustError::integrity)
        }
    })?;
    println!(
        "Native HADBP prune complete: deleted {} object/record pair(s); earliest native PIT is {}",
        outcome.deleted_objects, outcome.floor_seq
    );
    Ok(())
}
