use crate::errors::{classify_or_else, WalrustError};
use crate::ltx::Checksum;
use anyhow::Result;
use std::sync::Arc;

use crate::ltx;
use crate::s3::{self, create_client, parse_bucket};

use super::manifest::{
    discover_all_ltx_from_s3, discover_state_from_s3, is_snapshot, list_generation_files,
    GENERATION_LIVE,
};
use hadb_storage::StorageBackend;
use hadb_storage_s3::S3Storage;
use walrust_core::compaction::{list_merged_ranges, ranges_cover, RangeLayout, SeqRange};

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

#[derive(Debug, Clone)]
struct VerifiedLtxFile {
    key: String,
    generation: u64,
    min_txid: u64,
    max_txid: u64,
    pre_apply_checksum: Option<Checksum>,
    post_apply_checksum: Checksum,
}

/// Detect real TXID gaps in the live (generation-0) incremental pool (E3),
/// now **level- and snapshot-supersession-aware**.
///
/// `live` is `(key, min_txid, max_txid)` for each gen-0 file, sorted by
/// `min_txid`; `snapshot_maxes` is the set of full-snapshot max TXIDs;
/// `merged_ranges` are the inclusive seq spans of the merged compaction levels
/// (L1/L2…). A hole `[expected, hole_end]` (`hole_end = min_txid - 1`) between
/// consecutive incrementals is bridged — not a gap — when a restore path covers
/// it. A full snapshot at TXID `S` supersedes **every** TXID `<= S` (it is a
/// complete restore base), so only the part of the hole ABOVE the newest
/// snapshot inside it must be covered by merged level ranges:
///   - a snapshot inside the hole at `S`, with the remaining suffix
///     `[S+1, hole_end]` contiguously covered by level ranges (a snapshot
///     exactly at `hole_end` leaves nothing to cover — the classic
///     "snapshot consumes its own TXID" single-hole shape); **or**
///   - with no snapshot inside the hole, level ranges contiguously covering
///     all of it (the L0 objects were compacted away, not lost).
/// The combined shape arises routinely from the restart re-anchor: the startup
/// snapshot consumes the TXID at the hole's START and compaction then folds the
/// post-restart L0s into levels — snapshot at `expected`, levels covering
/// `[expected+1, hole_end]` (proven healthy by the row-exact soak restore; see
/// `e3_reanchor_snapshot_plus_levels_bridge_is_not_a_gap`).
/// Returns `(key, expected_next, actual_min)` for each genuine gap.
pub(crate) fn detect_live_txid_gaps(
    live: &[(String, u64, u64)],
    snapshot_maxes: &std::collections::BTreeSet<u64>,
    merged_ranges: &[SeqRange],
) -> Vec<(String, u64, u64)> {
    let mut gaps = Vec::new();
    let mut expected_next: Option<u64> = None;
    for (key, min_txid, max_txid) in live {
        if let Some(expected) = expected_next {
            if *min_txid > expected {
                let hole_end = *min_txid - 1;
                // Newest full snapshot inside the hole supersedes everything at
                // or below it; only the suffix above it needs level coverage.
                // (Checking just the newest is equivalent to trying all: a
                // higher snapshot leaves a smaller suffix, and `ranges_cover`
                // tolerates ranges starting below `lo`.)
                let bridged = match snapshot_maxes.range(expected..=hole_end).next_back() {
                    Some(&s) => ranges_cover(s + 1, hole_end, merged_ranges),
                    None => ranges_cover(expected, hole_end, merged_ranges),
                };
                if !bridged {
                    gaps.push((key.clone(), expected, *min_txid));
                }
            }
        }
        expected_next = Some(max_txid + 1);
    }
    gaps
}

/// Verify the snapshot→incremental LTX chain, **level-aware**. A hole between the
/// snapshot (or a prior incremental) and the next incremental is a real gap only
/// when the merged compaction levels do **not** contiguously cover it: when they
/// do, the fine L0 objects were folded up a level (their seqs live in an HADBP
/// merged range that carries the running LTX-domain checksum across the seam), so
/// it is a compaction, not a loss. Across such a seam the surviving L0 tail's
/// `pre_apply` chains from the *merged range's* end, not from the latest snapshot,
/// so the L0-only checksum link is broken **by design** — the bridged branch
/// therefore skips both the gap alarm and that checksum check (the merged object's
/// own content checksum and the DB-anchored restore verify guard its integrity).
fn verify_ltx_chain(files: &[VerifiedLtxFile], merged_ranges: &[SeqRange]) -> Vec<VerifyIssue> {
    let mut issues = Vec::new();

    let Some(snapshot) = files
        .iter()
        .filter(|file| is_snapshot(file.generation, file.min_txid, file.max_txid))
        .max_by_key(|file| (file.generation, file.max_txid))
    else {
        issues.push(VerifyIssue {
            filename: "<chain>".to_string(),
            issue: "No snapshot found - backup is incomplete".to_string(),
            is_orphan: false,
        });
        return issues;
    };

    let mut expected_next_txid = snapshot.max_txid + 1;
    let mut expected_pre_apply = snapshot.post_apply_checksum;
    let mut incrementals: Vec<_> = files
        .iter()
        .filter(|file| file.generation == GENERATION_LIVE && file.max_txid >= expected_next_txid)
        .collect();
    incrementals.sort_by_key(|file| file.min_txid);

    for file in incrementals {
        if file.min_txid != expected_next_txid {
            // Only a hole the merged levels do NOT cover is a genuine gap.
            let level_bridges = ranges_cover(expected_next_txid, file.min_txid - 1, merged_ranges);
            if !level_bridges {
                issues.push(VerifyIssue {
                    filename: file.key.clone(),
                    issue: format!(
                        "TXID gap after snapshot chain: expected min_txid={}, got {}",
                        expected_next_txid, file.min_txid
                    ),
                    is_orphan: false,
                });
            }
            // Whether bridged (compaction seam) or a genuine gap, the L0 checksum
            // link across the discontinuity is not meaningful — advance to a fresh
            // segment anchored on this file.
            expected_next_txid = file.max_txid + 1;
            expected_pre_apply = file.post_apply_checksum;
            continue;
        }

        if file.pre_apply_checksum != Some(expected_pre_apply) {
            issues.push(VerifyIssue {
                filename: file.key.clone(),
                issue: format!(
                    "checksum chain break: expected pre_apply {:#x}, got {}",
                    expected_pre_apply.into_inner(),
                    file.pre_apply_checksum
                        .map(|checksum| format!("{:#x}", checksum.into_inner()))
                        .unwrap_or_else(|| "none".to_string())
                ),
                is_orphan: false,
            });
        }

        expected_next_txid = file.max_txid + 1;
        expected_pre_apply = file.post_apply_checksum;
    }

    issues
}

/// Validate backup integrity for a database (non-blocking, for periodic validation)
pub(crate) async fn validate_backup_integrity(
    client: &aws_sdk_s3::Client,
    bucket: &str,
    prefix: &str,
    db_name: &str,
) -> Result<ValidationResult> {
    let native_storage = S3Storage::new(client.clone(), bucket.to_string());
    let native_verified =
        walrust_core::native_restore::verify_native_v1(&native_storage, prefix, db_name)
            .await?
            .unwrap_or(0);
    let discovered = discover_all_ltx_from_s3(client, bucket, prefix, db_name)
        .await
        .map_err(|e| classify_or_else(e, WalrustError::s3))?;

    if discovered.is_empty() && native_verified == 0 {
        return Err(WalrustError::integrity(format!(
            "{}: no LTX files found during backup validation",
            db_name
        ))
        .into());
    }
    if discovered.is_empty() {
        return Ok(ValidationResult {
            verified_count: native_verified,
            total_files: native_verified,
            issues: Vec::new(),
            verified_size_bytes: 0,
            is_valid: true,
        });
    }

    let mut issues: Vec<VerifyIssue> = Vec::new();
    let mut verified_files: Vec<VerifiedLtxFile> = Vec::new();
    let mut verified_count = 0;
    let mut total_size: u64 = 0;

    // Check each LTX file
    for entry in &discovered {
        match s3::download_bytes(client, bucket, &entry.key).await {
            Ok(data) => {
                let cursor = std::io::Cursor::new(&data);
                match ltx::verify_ltx_with_result(cursor) {
                    Ok(result) => {
                        let header_min = result.header.min_txid.into_inner();
                        let header_max = result.header.max_txid.into_inner();

                        if header_min != entry.min_txid || header_max != entry.max_txid {
                            issues.push(VerifyIssue {
                                filename: entry.key.clone(),
                                issue: format!(
                                    "TXID mismatch: filename {}-{}, header {}-{}",
                                    entry.min_txid, entry.max_txid, header_min, header_max
                                ),
                                is_orphan: false,
                            });
                        } else {
                            verified_count += 1;
                            total_size += data.len() as u64;
                            verified_files.push(VerifiedLtxFile {
                                key: entry.key.clone(),
                                generation: entry.generation,
                                min_txid: entry.min_txid,
                                max_txid: entry.max_txid,
                                pre_apply_checksum: result.header.pre_apply_checksum,
                                post_apply_checksum: result.post_apply_checksum,
                            });
                        }
                    }
                    Err(e) => {
                        issues.push(VerifyIssue {
                            filename: entry.key.clone(),
                            issue: format!("Checksum failed: {}", e),
                            is_orphan: false,
                        });
                    }
                }
            }
            Err(e) => return Err(WalrustError::s3(format!("Download failed: {}", e)).into()),
        }
    }
    // Level-aware chain check: a hole the merged compaction levels cover is a
    // compaction seam, not a gap (same rule as the CLI `verify` path). Periodic
    // validation runs against a possibly-compacting bucket, so it must not
    // false-alarm on a folded L0 pool.
    let merged_ranges: Vec<SeqRange> = {
        let storage: Arc<dyn StorageBackend> =
            Arc::new(S3Storage::new(client.clone(), bucket.to_string()));
        let layout = RangeLayout::new(storage, prefix, db_name);
        list_merged_ranges(&layout).await.unwrap_or_default()
    };
    issues.extend(verify_ltx_chain(&verified_files, &merged_ranges));

    Ok(ValidationResult {
        verified_count: verified_count + native_verified,
        total_files: discovered.len() + native_verified,
        issues: issues.clone(),
        verified_size_bytes: total_size,
        is_valid: issues.is_empty(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn verified_file(
        key: &str,
        generation: u64,
        min_txid: u64,
        max_txid: u64,
        pre_apply_checksum: Option<u64>,
        post_apply_checksum: u64,
    ) -> VerifiedLtxFile {
        VerifiedLtxFile {
            key: key.to_string(),
            generation,
            min_txid,
            max_txid,
            pre_apply_checksum: pre_apply_checksum.map(Checksum::new),
            post_apply_checksum: Checksum::new(post_apply_checksum),
        }
    }

    fn live(key: &str, min: u64, max: u64) -> (String, u64, u64) {
        (key.to_string(), min, max)
    }

    #[test]
    fn e3_snapshot_superseded_holes_are_not_reported_as_gaps() {
        // Healthy post-restart / interval-snapshot chain: the incremental pool
        // has single-TXID holes at 47 and 98 because full snapshots were taken
        // there. Those snapshots supersede the holes, so verify must be silent.
        let liveset = vec![
            live("a", 2, 46),
            live("b", 48, 97),  // hole at 47, bridged by snapshot(max=47)
            live("c", 99, 150), // hole at 98, bridged by snapshot(max=98)
        ];
        let snapshots: std::collections::BTreeSet<u64> = [1, 47, 98].into_iter().collect();
        let gaps = detect_live_txid_gaps(&liveset, &snapshots, &[]);
        assert!(
            gaps.is_empty(),
            "snapshot-superseded holes must not be reported: {gaps:?}"
        );
    }

    #[test]
    fn e3_hole_covered_by_a_merged_level_range_is_not_a_gap() {
        // Compaction folded L0 seqs 47..=97 into a merged L1 range. The L0 pool
        // now jumps 2..46 → 99..150 with NO snapshot at 46/98, but the merged
        // range [40,98] contiguously covers the hole — so it is a compaction,
        // not a gap, and verify must be silent.
        let liveset = vec![live("a", 2, 46), live("c", 99, 150)];
        let snapshots: std::collections::BTreeSet<u64> = [1].into_iter().collect();
        let merged = vec![SeqRange::new(40, 98)];
        let gaps = detect_live_txid_gaps(&liveset, &snapshots, &merged);
        assert!(
            gaps.is_empty(),
            "a merged-range-covered L0 hole must not be a gap: {gaps:?}"
        );
    }

    #[test]
    fn e3_hole_no_level_covers_still_alarms() {
        // The merged range [40,80] stops short of the resume point 99 (the hole
        // is [47,98]); it does NOT bridge, and no snapshot does either → real gap.
        let liveset = vec![live("a", 2, 46), live("c", 99, 150)];
        let snapshots: std::collections::BTreeSet<u64> = [1].into_iter().collect();
        let merged = vec![SeqRange::new(40, 80)];
        let gaps = detect_live_txid_gaps(&liveset, &snapshots, &merged);
        assert_eq!(
            gaps,
            vec![("c".to_string(), 47, 99)],
            "a hole no level fully covers must still alarm: {gaps:?}"
        );
    }

    #[test]
    fn e3_unbridged_hole_is_still_a_real_gap() {
        // Same shape, but no snapshot exists at TXID 47 to bridge the hole, so a
        // restore into 48-97 would genuinely fail — verify must still flag it.
        let liveset = vec![live("a", 2, 46), live("b", 48, 97)];
        let snapshots: std::collections::BTreeSet<u64> = [1].into_iter().collect();
        let gaps = detect_live_txid_gaps(&liveset, &snapshots, &[]);
        assert_eq!(gaps.len(), 1, "unbridged hole must be a real gap: {gaps:?}");
        assert_eq!(gaps[0], ("b".to_string(), 47, 48));
    }

    #[test]
    fn e3_reanchor_snapshot_plus_levels_bridge_is_not_a_gap() {
        // The restart re-anchor shape, pinned verbatim from a live soak run
        // (fail-on-revert for the snapshot-supersession rule). After a
        // kill/restart the startup re-anchor snapshot consumes the TXID at the
        // START of the hole (48), the first post-restart incremental (49-98)
        // chains from it, and compaction then folds that incremental into a
        // merged level and deletes it. The surviving gen-0 pool jumps
        // 40..47 → 99..102: the hole [48, 98] is bridged by snapshot(48) +
        // level [49, 98] COMBINED — neither alone (the snapshot is not at the
        // hole's end; the levels do not cover the snapshot-consumed 48). The
        // bucket is healthy (the soak's restore-to-latest is row-exact), so
        // verify must be silent. A second periodic snapshot inside the same
        // hole (58) must not confuse the rule.
        let liveset = vec![live("a", 40, 47), live("b", 99, 102)];
        let snapshots: std::collections::BTreeSet<u64> = [1, 48, 58].into_iter().collect();
        let merged = vec![SeqRange::new(49, 98)];
        let gaps = detect_live_txid_gaps(&liveset, &snapshots, &merged);
        assert!(
            gaps.is_empty(),
            "a re-anchor-snapshot + level-covered hole must not be a gap: {gaps:?}"
        );
        // The same hole with the level coverage stopping short of the hole's
        // end is NOT restorable above the folded range → still a real gap.
        let short = vec![SeqRange::new(49, 80)];
        let gaps = detect_live_txid_gaps(&liveset, &snapshots, &short);
        assert_eq!(
            gaps,
            vec![("b".to_string(), 48, 99)],
            "a partially-covered re-anchor hole must still alarm"
        );
        // Adversarial: a REAL missing range sits ABOVE the newest in-hole
        // snapshot (58) and BELOW where level coverage resumes — snapshot(58),
        // then [59, 69] uncovered, then levels [70, 98]. The suffix above the
        // superseding snapshot is not contiguously covered from 59, so this is
        // NOT restorable and MUST still alarm (supersession only excuses the
        // prefix at/below the snapshot, never an interior hole above it).
        let interior_hole = vec![SeqRange::new(70, 98)];
        let gaps = detect_live_txid_gaps(&liveset, &snapshots, &interior_hole);
        assert_eq!(
            gaps,
            vec![("b".to_string(), 48, 99)],
            "a hole above the newest in-hole snapshot but below level coverage must still alarm"
        );
    }

    #[test]
    fn e3_gap_below_latest_snapshot_above_older_retained_snapshot_still_alarms() {
        // Priority-5 case. Two retained snapshots: an old base at TXID 1 and the
        // latest at TXID 100. The incremental pool has an UNBRIDGED hole at
        // 51-59 (no snapshot there) and a snapshot-punched hole at 100. A PITR
        // into 60-99 genuinely depends on the missing 51-59 range: the latest
        // snapshot at 100 is ABOVE that range and cannot serve as its base, and
        // the old base at 1 cannot reach 60 across the hole. verify MUST alarm
        // on 51-59, while the snapshot at 100 correctly supersedes the hole at
        // 100 (restores >= 100 use it as base).
        let liveset = vec![live("a", 2, 50), live("b", 60, 99), live("c", 101, 150)];
        let snapshots: std::collections::BTreeSet<u64> = [1, 100].into_iter().collect();
        let gaps = detect_live_txid_gaps(&liveset, &snapshots, &[]);
        assert_eq!(
            gaps,
            vec![("b".to_string(), 51, 60)],
            "the unbridged mid-range hole must alarm and the snapshot@100 hole must not: {gaps:?}"
        );
    }

    #[test]
    fn test_verify_chain_rejects_snapshot_to_incremental_checksum_mismatch() {
        let files = vec![
            verified_file(
                "db/0001/0000000000000001-0000000000000001.ltx",
                1,
                1,
                1,
                None,
                0x1111,
            ),
            verified_file(
                "db/0000/0000000000000002-0000000000000002.ltx",
                0,
                2,
                2,
                Some(0x2222),
                0x3333,
            ),
        ];

        let issues = verify_ltx_chain(&files, &[]);
        assert!(
            issues
                .iter()
                .any(|issue| issue.issue.contains("checksum chain break")),
            "verify must reject an incremental whose pre_apply does not match the snapshot post_apply"
        );
    }

    #[test]
    fn chain_snapshot_to_compacted_tail_is_not_a_gap() {
        // The compacted-CLI shape the e2e hit: snapshot@1, the fine L0 seqs 2..=11
        // folded into merged levels (gone from gen-0), leaving only the L0 tail at
        // seq 12. The snapshot→incremental chain check must see the merged range
        // [2,11] bridge the hole and stay silent — no "TXID gap after snapshot
        // chain" and no false checksum break across the (by-design-broken) seam.
        let files = vec![
            verified_file("db/0001/…-…snap.ltx", 1, 1, 1, None, 0x1111),
            // seq 12 chains from the merged range end (0x9999), NOT the snapshot.
            verified_file("db/0000/…000c.ltx", 0, 12, 12, Some(0x9999), 0xAAAA),
        ];
        let merged = vec![SeqRange::new(2, 11)];
        let issues = verify_ltx_chain(&files, &merged);
        assert!(
            issues.is_empty(),
            "a snapshot→compacted-tail chain bridged by a merged range must be clean: {issues:?}"
        );

        // Fail-on-revert: with NO merged range to bridge, the same hole IS a gap.
        let issues_unbridged = verify_ltx_chain(&files, &[]);
        assert!(
            issues_unbridged
                .iter()
                .any(|i| i.issue.contains("TXID gap after snapshot chain")),
            "without a bridging merged range the hole must alarm: {issues_unbridged:?}"
        );
    }

    #[test]
    fn chain_partially_covered_hole_still_alarms() {
        // A merged range that does not fully cover the hole must NOT suppress the
        // gap: snapshot@1, tail at seq 12, but the merged range only reaches [2,9]
        // (seqs 10,11 are genuinely missing).
        let files = vec![
            verified_file("db/0001/…snap.ltx", 1, 1, 1, None, 0x1111),
            verified_file("db/0000/…000c.ltx", 0, 12, 12, Some(0x9999), 0xAAAA),
        ];
        let merged = vec![SeqRange::new(2, 9)];
        let issues = verify_ltx_chain(&files, &merged);
        assert!(
            issues
                .iter()
                .any(|i| i.issue.contains("TXID gap after snapshot chain")),
            "a hole the merged range only partially covers must still alarm: {issues:?}"
        );
    }
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
    let client = create_client(endpoint)
        .await
        .map_err(|e| classify_or_else(e, WalrustError::s3))?;

    println!(
        "Verifying integrity of '{}' in s3://{}/{}{}...",
        name, bucket_name, prefix, name
    );
    println!();

    let native_storage = S3Storage::new(client.clone(), bucket_name.clone());
    let native_verified =
        walrust_core::native_restore::verify_native_v1(&native_storage, &prefix, name)
            .await
            .map_err(|error| classify_or_else(error, WalrustError::integrity))?
            .unwrap_or(0);
    if native_verified > 0 {
        println!(
            "Native HADBP: verified {} contiguous published object(s)",
            native_verified
        );
        println!();
    }

    // Discover state from S3 (litestream format - no manifest)
    let (current_txid, max_gen, _) = discover_state_from_s3(&client, &bucket_name, &prefix, name)
        .await
        .map_err(|e| classify_or_else(e, WalrustError::s3))?;

    if current_txid == 0 && native_verified == 0 {
        println!("No LTX files found for database: {}", name);
        println!("Exit code: 5 (integrity issues found)");
        return Err(WalrustError::integrity(format!(
            "Integrity verification failed: No LTX files found for database: {}",
            name
        ))
        .into());
    }
    if current_txid == 0 {
        println!("All checks passed - native HADBP backup integrity verified");
        println!();
        println!("Exit code: 0 (success)");
        return Ok(());
    }

    // Collect all files from all generations
    let mut all_files: Vec<(String, u64, u64, u64)> = Vec::new(); // (key, gen, min, max)

    // Get files from generation 0 (live incrementals)
    let live_files = list_generation_files(&client, &bucket_name, &prefix, name, GENERATION_LIVE)
        .await
        .map_err(|e| classify_or_else(e, WalrustError::s3))?;
    for (key, min, max) in live_files {
        all_files.push((key, GENERATION_LIVE, min, max));
    }

    // Get files from snapshot generations (1+)
    for gen in 1..=max_gen {
        let gen_files = list_generation_files(&client, &bucket_name, &prefix, name, gen)
            .await
            .map_err(|e| classify_or_else(e, WalrustError::s3))?;
        for (key, min, max) in gen_files {
            all_files.push((key, gen, min, max));
        }
    }

    println!(
        "Verifying backup: {} in s3://{}/{}{}",
        name, bucket_name, prefix, name
    );
    println!("================================================");
    println!();

    // Check for snapshot existence (critical requirement)
    let has_snapshot = all_files
        .iter()
        .any(|(_, gen, min, max)| is_snapshot(*gen, *min, *max));

    if !has_snapshot {
        println!("CRITICAL: No snapshot found (generation file)");
        println!();
        println!("Cannot verify backup without a base snapshot.");
        println!("Recommendation: Run 'walrust snapshot' to create initial snapshot.");
        return Err(WalrustError::integrity("No snapshot found - backup is incomplete").into());
    }

    println!(
        "Snapshot: Found generation {} (TXID range covered)",
        max_gen
    );
    println!();

    let mut issues: Vec<VerifyIssue> = Vec::new();
    let mut verified_files: Vec<VerifiedLtxFile> = Vec::new();
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
                match ltx::verify_ltx_with_result(cursor) {
                    Ok(result) => {
                        let header_min = result.header.min_txid.into_inner();
                        let header_max = result.header.max_txid.into_inner();

                        // Verify header matches filename
                        if header_min != *expected_min || header_max != *expected_max {
                            let txid_count = expected_max - expected_min + 1;
                            println!(
                                "  WARNING {} ({} TXIDs, {}KB) - TXID mismatch!",
                                filename, txid_count, size_kb
                            );
                            issues.push(VerifyIssue {
                                filename: key.clone(),
                                issue: format!(
                                    "TXID mismatch: filename says {}-{}, header says {}-{}",
                                    expected_min, expected_max, header_min, header_max
                                ),
                                is_orphan: false,
                            });
                        } else {
                            let txid_count = expected_max - expected_min + 1;
                            println!("  OK {} ({} TXIDs, {}KB)", filename, txid_count, size_kb);
                            verified_count += 1;
                            total_size += data.len() as u64;
                            verified_files.push(VerifiedLtxFile {
                                key: key.clone(),
                                generation: *_gen,
                                min_txid: *expected_min,
                                max_txid: *expected_max,
                                pre_apply_checksum: result.header.pre_apply_checksum,
                                post_apply_checksum: result.post_apply_checksum,
                            });
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
            Err(e) => return Err(WalrustError::s3(format!("Download failed: {}", e)).into()),
        }
    }
    // Level-aware: a hole in the L0 pool that a merged L1/L2 range covers is a
    // *compaction*, not a gap. List the merged ranges under `{db}/levels/L*/`
    // (HADBP objects; a header-read failure conservatively yields no bridges, so
    // verify errs toward alarming, never toward silence). Computed once and shared
    // by both the snapshot-chain check and the between-incrementals check.
    let merged_ranges: Vec<SeqRange> = {
        let storage: Arc<dyn StorageBackend> =
            Arc::new(S3Storage::new(client.clone(), bucket_name.clone()));
        let layout = RangeLayout::new(storage, &prefix, name);
        list_merged_ranges(&layout).await.unwrap_or_default()
    };
    issues.extend(verify_ltx_chain(&verified_files, &merged_ranges));

    // Check TXID continuity in generation 0 (live)
    let mut live_files: Vec<_> = all_files
        .iter()
        .filter(|(_, gen, _, _)| *gen == GENERATION_LIVE)
        .collect();
    live_files.sort_by_key(|(_, _, min, _)| *min);

    // E3: a full snapshot at TXID H supersedes any hole in the incremental pool
    // below H — restore uses that snapshot as its base (min==1 full DB) and
    // never needs the missing incrementals. Taking a snapshot at H in fact
    // *punches* a single-TXID hole at H in the gen-0 stream by design, so the
    // naive "every gen-0 TXID must be contiguous" check false-positives
    // "CRITICAL - data may be unrecoverable" on every healthy chain. A hole is
    // only a real gap if no snapshot base bridges it (i.e. no full snapshot
    // exists at the TXID just below where the incrementals resume).
    let snapshot_maxes: std::collections::BTreeSet<u64> = all_files
        .iter()
        .filter(|(_, gen, min, max)| *gen != GENERATION_LIVE || (*min == 1 && *max == 1))
        .map(|(_, _, _, max)| *max)
        .collect();
    let live_triples: Vec<(String, u64, u64)> = live_files
        .iter()
        .map(|(key, _, min, max)| (key.clone(), *min, *max))
        .collect();
    for (key, expected, min_txid) in
        detect_live_txid_gaps(&live_triples, &snapshot_maxes, &merged_ranges)
    {
        issues.push(VerifyIssue {
            filename: key,
            issue: format!(
                "TXID gap: expected min_txid={}, got {} (missing TXIDs {}-{})",
                expected,
                min_txid,
                expected,
                min_txid - 1
            ),
            is_orphan: false,
        });
    }

    println!();

    // Check TXID continuity and report
    let mut has_critical_gap = false;
    if !live_files.is_empty() {
        let last_txid = live_files.last().map(|(_, _, _, max)| *max).unwrap_or(0);

        // Check for gaps in continuity
        let gap_issues: Vec<_> = issues
            .iter()
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

    println!(
        "Verified: {}/{} files ({:.1} KB total)",
        verified_count,
        all_files.len(),
        total_size as f64 / 1024.0
    );
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
        println!("Exit code: 5 (integrity errors - data may be unrecoverable)");
        Err(WalrustError::integrity("Critical integrity issues detected").into())
    } else {
        println!("Recommendation: Investigate checksum failures or re-upload affected files");
        println!();
        println!("Exit code: 5 (integrity issues found)");
        Err(WalrustError::integrity("Integrity issues detected").into())
    }
}
