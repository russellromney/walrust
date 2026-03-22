use anyhow::Result;
use std::sync::Arc;

use crate::ltx;
use crate::s3::{self, create_client, parse_bucket};

use super::manifest::{discover_state_from_s3, list_generation_files, load_manifest, GENERATION_LIVE};

/// Verification issue found during verify
#[derive(Debug, Clone)]
pub struct VerifyIssue {
    pub filename: String,
    pub issue: String,
    pub is_orphan: bool,
}

/// Result of backup validation
#[derive(Debug)]
pub struct ValidationResult {
    pub verified_count: usize,
    pub total_files: usize,
    pub issues: Vec<VerifyIssue>,
    pub verified_size_bytes: u64,
    pub is_valid: bool,
}

/// Validate backup integrity for a database (non-blocking, for periodic validation)
pub(crate) async fn validate_backup_integrity(
    client: &aws_sdk_s3::Client,
    bucket: &str,
    prefix: &str,
    db_name: &str,
) -> Result<ValidationResult> {
    // Load manifest
    let manifest = load_manifest(client, bucket, prefix, db_name).await?;

    if manifest.files.is_empty() {
        return Ok(ValidationResult {
            verified_count: 0,
            total_files: 0,
            issues: Vec::new(),
            verified_size_bytes: 0,
            is_valid: true,
        });
    }

    let mut issues: Vec<VerifyIssue> = Vec::new();
    let mut verified_count = 0;
    let mut total_size: u64 = 0;

    // Check each LTX file
    for entry in &manifest.files {
        let ltx_key = format!("{}{}/{}", prefix, db_name, entry.filename);

        match s3::exists(client, bucket, &ltx_key).await {
            Ok(true) => {
                // File exists, download and verify
                match s3::download_bytes(client, bucket, &ltx_key).await {
                    Ok(data) => {
                        let cursor = std::io::Cursor::new(&data);
                        match ltx::verify_ltx(cursor) {
                            Ok(header) => {
                                let header_min = header.min_txid.into_inner();
                                let header_max = header.max_txid.into_inner();

                                if header_min != entry.min_txid || header_max != entry.max_txid {
                                    issues.push(VerifyIssue {
                                        filename: entry.filename.clone(),
                                        issue: format!(
                                            "TXID mismatch: manifest {}-{}, header {}-{}",
                                            entry.min_txid, entry.max_txid,
                                            header_min, header_max
                                        ),
                                        is_orphan: false,
                                    });
                                } else {
                                    verified_count += 1;
                                    total_size += data.len() as u64;
                                }
                            }
                            Err(e) => {
                                issues.push(VerifyIssue {
                                    filename: entry.filename.clone(),
                                    issue: format!("Checksum failed: {}", e),
                                    is_orphan: false,
                                });
                            }
                        }
                    }
                    Err(e) => {
                        issues.push(VerifyIssue {
                            filename: entry.filename.clone(),
                            issue: format!("Download failed: {}", e),
                            is_orphan: false,
                        });
                    }
                }
            }
            Ok(false) => {
                issues.push(VerifyIssue {
                    filename: entry.filename.clone(),
                    issue: "File missing from S3".to_string(),
                    is_orphan: true,
                });
            }
            Err(e) => {
                issues.push(VerifyIssue {
                    filename: entry.filename.clone(),
                    issue: format!("S3 check failed: {}", e),
                    is_orphan: false,
                });
            }
        }
    }

    // Check TXID continuity
    let mut sorted_files: Vec<_> = manifest.files.iter().collect();
    sorted_files.sort_by_key(|f| f.min_txid);

    let mut expected_next_txid: Option<u64> = None;
    for entry in &sorted_files {
        if let Some(expected) = expected_next_txid {
            // For incrementals, check for gaps
            if !entry.is_snapshot && entry.min_txid != expected && entry.min_txid > expected {
                issues.push(VerifyIssue {
                    filename: entry.filename.clone(),
                    issue: format!(
                        "TXID gap: expected {}, got {} (missing {}-{})",
                        expected, entry.min_txid,
                        expected, entry.min_txid - 1
                    ),
                    is_orphan: false,
                });
            }
        }
        expected_next_txid = Some(entry.max_txid + 1);
    }

    Ok(ValidationResult {
        verified_count,
        total_files: manifest.files.len(),
        issues: issues.clone(),
        verified_size_bytes: total_size,
        is_valid: issues.is_empty(),
    })
}

/// Verify integrity of all LTX files in S3 for a database
///
/// Checks:
/// - Each LTX file in manifest exists in S3
/// - LTX headers can be decoded
/// - LTX internal checksums are valid
/// - TXID continuity (no gaps in the chain)
pub async fn verify(
    name: &str,
    bucket: &str,
    endpoint: Option<&str>,
    webhook: Option<std::sync::Arc<crate::webhook::WebhookSender>>,
) -> Result<()> {
    let (bucket_name, prefix) = parse_bucket(bucket);
    let client = create_client(endpoint).await?;

    println!("Verifying integrity of '{}' in s3://{}/{}{}...",
        name, bucket_name, prefix, name);
    println!();

    // Discover state from S3 (litestream format - no manifest)
    let (current_txid, max_gen, _) =
        discover_state_from_s3(&client, &bucket_name, &prefix, name).await?;

    if current_txid == 0 {
        println!("No LTX files found for database: {}", name);
        return Ok(());
    }

    // Collect all files from all generations
    let mut all_files: Vec<(String, u64, u64, u64)> = Vec::new(); // (key, gen, min, max)

    // Get files from generation 0 (live incrementals)
    let live_files = list_generation_files(&client, &bucket_name, &prefix, name, GENERATION_LIVE).await?;
    for (key, min, max) in live_files {
        all_files.push((key, GENERATION_LIVE, min, max));
    }

    // Get files from snapshot generations (1+)
    for gen in 1..=max_gen {
        let gen_files = list_generation_files(&client, &bucket_name, &prefix, name, gen).await?;
        for (key, min, max) in gen_files {
            all_files.push((key, gen, min, max));
        }
    }

    println!("Verifying backup: {} in s3://{}/{}{}", name, bucket_name, prefix, name);
    println!("================================================");
    println!();

    // Check for snapshot existence (critical requirement)
    let has_snapshot = all_files.iter().any(|(_, gen, min, max)| *gen > 0 || (*min == 1 && *max == 1));

    if !has_snapshot {
        println!("CRITICAL: No snapshot found (generation file)");
        println!();
        println!("Cannot verify backup without a base snapshot.");
        println!("Recommendation: Run 'walrust snapshot' to create initial snapshot.");
        anyhow::bail!("No snapshot found - backup is incomplete");
    }

    println!("Snapshot: Found generation {} (TXID range covered)", max_gen);
    println!();

    let mut issues: Vec<VerifyIssue> = Vec::new();
    let mut verified_count = 0;
    let mut total_size: u64 = 0;

    println!("Incremental files: {} files", all_files.len());

    // Verify each file
    for (key, _gen, expected_min, expected_max) in &all_files {
        let filename = key.split('/').last().unwrap_or(key);
        match s3::download_bytes(&client, &bucket_name, key).await {
            Ok(data) => {
                let size_kb = data.len() / 1024;
                let cursor = std::io::Cursor::new(&data);
                match ltx::verify_ltx(cursor) {
                    Ok(header) => {
                        let header_min = header.min_txid.into_inner();
                        let header_max = header.max_txid.into_inner();

                        // Verify header matches filename
                        if header_min != *expected_min || header_max != *expected_max {
                            let txid_count = expected_max - expected_min + 1;
                            println!("  WARNING {} ({} TXIDs, {}KB) - TXID mismatch!",
                                filename, txid_count, size_kb);
                            issues.push(VerifyIssue {
                                filename: key.clone(),
                                issue: format!(
                                    "TXID mismatch: filename says {}-{}, header says {}-{}",
                                    expected_min, expected_max,
                                    header_min, header_max
                                ),
                                is_orphan: false,
                            });
                        } else {
                            let txid_count = expected_max - expected_min + 1;
                            println!("  OK {} ({} TXIDs, {}KB)",
                                filename, txid_count, size_kb);
                            verified_count += 1;
                            total_size += data.len() as u64;
                        }
                    }
                    Err(e) => {
                        println!("  WARNING {} - checksum verification failed!", filename);
                        let error_msg = format!("Checksum verification failed: {}", e);
                        issues.push(VerifyIssue {
                            filename: key.clone(),
                            issue: error_msg.clone(),
                            is_orphan: false,
                        });
                        // Notify webhook of corruption (fire-and-forget)
                        if let Some(ref webhook) = webhook {
                            let webhook = Arc::clone(webhook);
                            let name = name.to_string();
                            let error_msg = error_msg.clone();
                            tokio::spawn(async move {
                                webhook.notify_corruption(&name, &error_msg).await;
                            });
                        }
                    }
                }
            }
            Err(e) => {
                println!("  WARNING {} - download failed!", filename);
                issues.push(VerifyIssue {
                    filename: key.clone(),
                    issue: format!("Download failed: {}", e),
                    is_orphan: false,
                });
            }
        }
    }

    // Check TXID continuity in generation 0 (live)
    let mut live_files: Vec<_> = all_files
        .iter()
        .filter(|(_, gen, _, _)| *gen == GENERATION_LIVE)
        .collect();
    live_files.sort_by_key(|(_, _, min, _)| *min);

    let mut expected_next_txid: Option<u64> = None;
    for (key, _, min_txid, max_txid) in &live_files {
        if let Some(expected) = expected_next_txid {
            if *min_txid != expected && *min_txid > expected {
                issues.push(VerifyIssue {
                    filename: key.clone(),
                    issue: format!(
                        "TXID gap: expected min_txid={}, got {} (missing TXIDs {}-{})",
                        expected, min_txid,
                        expected, min_txid - 1
                    ),
                    is_orphan: false,
                });
            }
        }
        expected_next_txid = Some(max_txid + 1);
    }

    println!();

    // Check TXID continuity and report
    let mut has_critical_gap = false;
    if !live_files.is_empty() {
        let last_txid = live_files.last().map(|(_, _, _, max)| *max).unwrap_or(0);

        // Check for gaps in continuity
        let gap_issues: Vec<_> = issues.iter()
            .filter(|i| i.issue.contains("TXID gap"))
            .collect();

        if gap_issues.is_empty() {
            println!("Continuity: OK No gaps detected (TXID 1-{})", last_txid);
        } else {
            println!("Continuity: WARNING Gaps detected:");
            for issue in gap_issues {
                println!("  - {}", issue.issue);
                has_critical_gap = true;
            }
        }
    } else {
        // Snapshot-only backup with no incrementals
        println!("Continuity: OK Snapshot only (no incrementals to check)");
    }

    println!();

    // Summary of issues
    if !issues.is_empty() {
        println!("Issues found: {}", issues.len());
        for issue in &issues {
            let filename = issue.filename.split('/').last().unwrap_or(&issue.filename);
            println!("  - {} in {}", issue.issue, filename);
        }
        println!();
    }

    println!("Verified: {}/{} files ({:.1} KB total)", verified_count, all_files.len(), total_size as f64 / 1024.0);
    println!();

    // Exit with appropriate code
    if issues.is_empty() {
        println!("All checks passed - backup integrity verified");
        println!();
        println!("Exit code: 0 (success)");
        Ok(())
    } else if has_critical_gap {
        println!("Recommendation: Re-snapshot database to repair backup chain");
        println!();
        println!("Exit code: 2 (critical errors - data may be unrecoverable)");
        anyhow::bail!("Critical integrity issues detected")
    } else {
        println!("Recommendation: Investigate checksum failures or re-upload affected files");
        println!();
        println!("Exit code: 1 (issues found)");
        anyhow::bail!("Integrity issues detected")
    }
}
