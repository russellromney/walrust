use crate::errors::{classify_or_else, WalrustError};
use crate::s3::{create_client, parse_bucket};
use anyhow::{bail, Result};
use hadb_storage::StorageBackend;
use hadb_storage_s3::S3Storage;
use std::path::{Path, PathBuf};

fn unique_local_native_spool(
    dir: &Path,
    bucket: &str,
    prefix: &str,
    database: &str,
) -> Result<Option<(PathBuf, walrust_core::native_spool::SpoolIdentity)>> {
    let mut roots = Vec::new();
    if dir.join("journal.json").exists() {
        roots.push(dir.to_path_buf());
    }
    let native_root = dir.join("native-v1");
    if let Ok(entries) = std::fs::read_dir(&native_root) {
        roots.extend(entries.filter_map(|entry| entry.ok().map(|entry| entry.path())));
    }
    roots.sort();
    roots.dedup();
    let mut matches = Vec::new();
    for root in roots {
        let Some(identity) = walrust_core::native_spool::NativeSpool::read_identity(&root)? else {
            continue;
        };
        if identity.bucket == bucket && identity.prefix == prefix && identity.database == database {
            matches.push((root, identity));
        }
    }
    if matches.len() > 1 {
        let candidates = matches
            .iter()
            .map(|(root, identity)| format!("{} (lineage {})", root.display(), identity.lineage_id))
            .collect::<Vec<_>>()
            .join(", ");
        bail!(
            "multiple local native spools match s3://{}/{}{}; refusing ambiguous restore: {}",
            bucket,
            prefix,
            database,
            candidates
        );
    }
    Ok(matches.pop())
}

pub async fn restore(
    name: &str,
    output: &Path,
    bucket: &str,
    endpoint: Option<&str>,
    point_in_time: Option<&str>,
    spool_dir: Option<&Path>,
    webhook: Option<std::sync::Arc<crate::webhook::WebhookSender>>,
) -> Result<()> {
    let (bucket_name, prefix) = parse_bucket(bucket);
    let point = point_in_time
        .map(|value| {
            value.parse::<u64>().map_err(|_| {
                WalrustError::restore("Invalid point_in_time format. Use native sequence number")
            })
        })
        .transpose()?;

    if let Some(dir) = spool_dir {
        if let Some((root, identity)) = unique_local_native_spool(dir, &bucket_name, &prefix, name)?
        {
            let spool = walrust_core::native_spool::NativeSpool::create_or_open(
                &root,
                identity,
                walrust_core::native_spool::CapacityPolicy {
                    warning_bytes: u64::MAX - 1,
                    hard_bytes: u64::MAX,
                    minimum_free_bytes: 0,
                },
            )?;
            if let Some(seq) =
                walrust_core::native_restore::restore_local_spool(&spool, output, point)?
            {
                println!(
                    "Restored {} to {} from local native HADBP spool (sequence: {})",
                    name,
                    output.display(),
                    seq
                );
                return Ok(());
            }
        }
    }

    let client = create_client(endpoint)
        .await
        .map_err(|error| classify_or_else(error, WalrustError::s3))?;
    let storage = S3Storage::new(client, bucket_name.clone());
    let restored = walrust_core::native_restore::restore_native_v1(
        &storage,
        &bucket_name,
        &prefix,
        name,
        output,
        point,
    )
    .await;
    match restored {
        Ok(walrust_core::native_restore::NativeRestoreAvailability::Restored { seq }) => {
            println!(
                "Restored {} to {} (native HADBP sequence: {})",
                name,
                output.display(),
                seq
            );
            Ok(())
        }
        Ok(_) => Err(WalrustError::restore(format!(
            "no native-v1 HADBP recovery stream found for '{name}'"
        ))
        .into()),
        Err(error) => {
            if let Some(webhook) = webhook {
                webhook
                    .notify_corruption(name, &format!("native HADBP restore failed: {error:#}"))
                    .await;
            }
            if error
                .chain()
                .any(|cause| cause.is::<walrust_core::native_restore::NativeStorageError>())
            {
                Err(classify_or_else(error, WalrustError::s3))
            } else {
                Err(classify_or_else(error, WalrustError::restore))
            }
        }
    }
}

/// List only versioned native-v1 databases in a bucket.
pub async fn list(bucket: &str, endpoint: Option<&str>) -> Result<()> {
    let (bucket_name, prefix) = parse_bucket(bucket);
    let client = create_client(endpoint)
        .await
        .map_err(|error| classify_or_else(error, WalrustError::s3))?;
    let storage = S3Storage::new(client, bucket_name.clone());
    let keys = storage
        .list(&prefix, None)
        .await
        .map_err(|error| classify_or_else(error, WalrustError::s3))?;
    let suffix = "/native/v1/stream.json";
    let mut databases = keys
        .iter()
        .filter_map(|key| {
            key.strip_prefix(&prefix)
                .and_then(|relative| relative.strip_suffix(suffix))
                .filter(|name| !name.is_empty() && !name.contains('/'))
                .map(str::to_owned)
        })
        .collect::<Vec<_>>();
    databases.sort();
    databases.dedup();

    if databases.is_empty() {
        println!(
            "No native-v1 databases found in s3://{}/{}",
            bucket_name, prefix
        );
        return Ok(());
    }
    println!("Native-v1 databases in s3://{}/{}:", bucket_name, prefix);
    for database in databases {
        let native = walrust_core::native_restore::inspect_native_v1(
            &storage,
            &bucket_name,
            &prefix,
            &database,
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
        })?
        .ok_or_else(|| {
            WalrustError::restore(format!(
                "native descriptor for '{database}' has no contiguous published snapshot base"
            ))
        })?;
        println!(
            "  {} (native sequence: {}, {} objects, snapshot sequence {})",
            database, native.head_seq, native.object_count, native.latest_snapshot_seq
        );
    }
    Ok(())
}
