use anyhow::{anyhow, Result};
use chrono::Utc;
use std::path::Path;
use std::time::Duration;

use crate::ltx;
use crate::s3::{self, create_client};

use super::manifest::load_manifest;
use super::types::{Manifest, ReplicaState};

pub async fn replicate(
    source: &str,
    local: &Path,
    interval: Duration,
    endpoint: Option<&str>,
) -> Result<()> {
    // Parse source: "s3://bucket/prefix/dbname" or "s3://bucket/dbname"
    let source = source.strip_prefix("s3://").unwrap_or(source);
    let parts: Vec<&str> = source.splitn(2, '/').collect();
    if parts.len() < 2 {
        return Err(anyhow!(
            "Invalid source format. Expected: s3://bucket/dbname or s3://bucket/prefix/dbname"
        ));
    }

    let bucket_name = parts[0];
    let path_part = parts[1];

    // Split path into prefix and dbname (last component is dbname)
    let (prefix, db_name) = if let Some(idx) = path_part.rfind('/') {
        let p = &path_part[..=idx]; // Include trailing slash
        let n = &path_part[idx + 1..];
        (p.to_string(), n.to_string())
    } else {
        (String::new(), path_part.to_string())
    };

    let client = create_client(endpoint).await?;

    tracing::info!(
        "Starting replica: source=s3://{}/{}{}, local={}",
        bucket_name,
        prefix,
        db_name,
        local.display()
    );

    // Track current TXID (0 = not yet initialized)
    let mut current_txid: u64 = 0;

    // Check if local database exists
    if local.exists() {
        // Try to determine current TXID from local state file
        let state_path = local.with_extension("db-replica-state");
        if state_path.exists() {
            if let Ok(data) = std::fs::read_to_string(&state_path) {
                if let Ok(state) = serde_json::from_str::<ReplicaState>(&data) {
                    current_txid = state.current_txid;
                    tracing::info!("Resuming replica from TXID {}", current_txid);
                }
            }
        }
    }

    println!(
        "Replicating s3://{}/{}{} -> {}",
        bucket_name,
        prefix,
        db_name,
        local.display()
    );
    println!("Poll interval: {:?}", interval);
    println!("Press Ctrl+C to stop\n");

    // Main replication loop
    loop {
        match replicate_poll(
            &client,
            bucket_name,
            &prefix,
            &db_name,
            local,
            &mut current_txid,
        )
        .await
        {
            Ok(applied) => {
                if applied > 0 {
                    println!(
                        "[{}] Applied {} LTX file(s), now at TXID {}",
                        chrono::Local::now().format("%H:%M:%S"),
                        applied,
                        current_txid
                    );
                }
            }
            Err(e) => {
                tracing::error!("Replication error: {}", e);
                eprintln!(
                    "[{}] Error: {}",
                    chrono::Local::now().format("%H:%M:%S"),
                    e
                );
            }
        }

        tokio::time::sleep(interval).await;
    }
}

/// Single poll iteration for replication
async fn replicate_poll(
    client: &aws_sdk_s3::Client,
    bucket: &str,
    prefix: &str,
    db_name: &str,
    local: &Path,
    current_txid: &mut u64,
) -> Result<usize> {
    // Load manifest from S3
    let manifest = load_manifest(client, bucket, prefix, db_name).await?;

    if manifest.files.is_empty() {
        return Err(anyhow!("No LTX files found in manifest for '{}'", db_name));
    }

    // If we haven't initialized yet (current_txid = 0), bootstrap from snapshot
    if *current_txid == 0 || !local.exists() {
        bootstrap_replica(client, bucket, prefix, db_name, local, &manifest).await?;
        // After bootstrap, current_txid is the snapshot's max_txid
        let snapshot = manifest
            .files
            .iter()
            .filter(|f| f.is_snapshot)
            .max_by_key(|f| f.max_txid)
            .ok_or_else(|| anyhow!("No snapshot found for bootstrap"))?;
        *current_txid = snapshot.max_txid;
        save_replica_state(local, *current_txid)?;
        return Ok(1);
    }

    // Find incremental LTX files we need to apply (min_txid > current_txid)
    let mut incrementals: Vec<_> = manifest
        .files
        .iter()
        .filter(|f| !f.is_snapshot && f.min_txid > *current_txid)
        .collect();

    // Also check for newer snapshots that might be more efficient
    // (e.g., if we're very far behind, a snapshot might be faster)
    let latest_snapshot = manifest
        .files
        .iter()
        .filter(|f| f.is_snapshot)
        .max_by_key(|f| f.max_txid);

    // If there's a snapshot newer than our position + all incrementals we'd apply,
    // and we're far behind, consider using the snapshot instead
    if let Some(snap) = latest_snapshot {
        if snap.max_txid > *current_txid && incrementals.is_empty() {
            // We're behind but no incrementals bridge the gap - need snapshot
            tracing::info!(
                "Gap detected: at TXID {}, latest snapshot at TXID {}. Re-bootstrapping.",
                current_txid,
                snap.max_txid
            );
            bootstrap_replica(client, bucket, prefix, db_name, local, &manifest).await?;
            *current_txid = snap.max_txid;
            save_replica_state(local, *current_txid)?;
            return Ok(1);
        }
    }

    if incrementals.is_empty() {
        return Ok(0); // No new data
    }

    // Sort by min_txid to apply in order
    incrementals.sort_by_key(|f| f.min_txid);

    let mut applied = 0;

    for ltx_entry in incrementals {
        // Verify continuity: min_txid should be current_txid + 1
        // (or we accept any min_txid > current_txid for robustness)
        if ltx_entry.min_txid != *current_txid + 1 {
            tracing::warn!(
                "TXID gap: expected {}, got {}. Skipping to avoid corruption.",
                *current_txid + 1,
                ltx_entry.min_txid
            );
            // Could trigger re-bootstrap here, but for now just warn and continue
            continue;
        }

        let ltx_key = format!("{}{}/{}", prefix, db_name, ltx_entry.filename);
        tracing::debug!("Downloading incremental: {}", ltx_key);

        let ltx_data = s3::download_bytes(client, bucket, &ltx_key).await?;
        let cursor = std::io::Cursor::new(ltx_data);

        // Apply in-place (verifies checksum chain)
        let apply_result = ltx::apply_ltx_to_db(cursor, local)?;

        tracing::info!(
            "Applied {} (TXID {}-{}, checksum: {:016x})",
            ltx_entry.filename,
            apply_result.header.min_txid.into_inner(),
            apply_result.header.max_txid.into_inner(),
            apply_result.post_apply_checksum.into_inner()
        );

        *current_txid = ltx_entry.max_txid;
        applied += 1;

        // Save state after each successful apply
        save_replica_state(local, *current_txid)?;
    }

    Ok(applied)
}

/// Bootstrap replica from latest snapshot
async fn bootstrap_replica(
    client: &aws_sdk_s3::Client,
    bucket: &str,
    prefix: &str,
    db_name: &str,
    local: &Path,
    manifest: &Manifest,
) -> Result<()> {
    // Find the best (latest) snapshot
    let snapshot = manifest
        .files
        .iter()
        .filter(|f| f.is_snapshot)
        .max_by_key(|f| f.max_txid)
        .ok_or_else(|| anyhow!("No snapshot found for database '{}'", db_name))?;

    tracing::info!(
        "Bootstrapping replica from snapshot: {} (TXID: {})",
        snapshot.filename,
        snapshot.max_txid
    );

    let ltx_key = format!("{}{}/{}", prefix, db_name, snapshot.filename);
    let ltx_data = s3::download_bytes(client, bucket, &ltx_key).await?;

    // Decode snapshot to local database (verifies checksum)
    let cursor = std::io::Cursor::new(ltx_data);
    let decode_result = ltx::decode_to_db(cursor, local)?;

    println!(
        "Bootstrapped from snapshot: {} pages, TXID {}, checksum: {:016x}",
        decode_result.header.commit.into_inner(),
        decode_result.header.max_txid.into_inner(),
        decode_result.post_apply_checksum.into_inner()
    );

    Ok(())
}

/// Save replica state to local file
fn save_replica_state(local: &Path, current_txid: u64) -> Result<()> {
    let state_path = local.with_extension("db-replica-state");
    let state = ReplicaState {
        current_txid,
        last_updated: Utc::now().to_rfc3339(),
    };
    let data = serde_json::to_string_pretty(&state)?;
    std::fs::write(&state_path, data)?;
    Ok(())
}
