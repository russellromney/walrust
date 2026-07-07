use anyhow::Result;
use rusqlite::Connection;
use std::path::Path;
use walrust_core::{legacy_ltx, legacy_replica};

fn create_marker_db(path: &Path, marker: &str) -> Result<u32> {
    let conn = Connection::open(path)?;
    conn.execute("CREATE TABLE marker (value TEXT NOT NULL);", [])?;
    conn.execute(
        "INSERT INTO marker (value) VALUES (?1)",
        rusqlite::params![marker],
    )?;
    let page_size = conn.query_row("PRAGMA page_size", [], |row| row.get(0))?;
    drop(conn);
    Ok(page_size)
}

fn marker_value(path: &Path) -> Result<String> {
    let conn = Connection::open(path)?;
    Ok(conn.query_row("SELECT value FROM marker", [], |row| row.get(0))?)
}

#[test]
fn legacy_replica_engine_is_owned_by_core_and_preserves_live_db_on_bad_incremental() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let local = dir.path().join("local.db");
    let page_size = create_marker_db(&local, "local-survives")?;
    let original_bytes = std::fs::read(&local)?;
    let pre_checksum = legacy_ltx::compute_checksum_from_file(&local)?;

    let pages = vec![(1, vec![0xAA; page_size as usize])];
    let mut bad_incremental = Vec::new();
    legacy_ltx::encode_wal_changes(
        &mut bad_incremental,
        &pages,
        page_size,
        2,
        2,
        1,
        Some(pre_checksum),
        legacy_ltx::Checksum::new(0x0bad_c0de),
    )?;

    let err = legacy_replica::apply_incremental_atomically(&bad_incremental, &local)
        .expect_err("bad incremental must fail without mutating the live replica");
    assert!(
        err.to_string().contains("Post-apply checksum mismatch"),
        "expected checksum failure, got: {err}"
    );
    assert_eq!(std::fs::read(&local)?, original_bytes);
    assert_eq!(marker_value(&local)?, "local-survives");
    Ok(())
}

#[test]
fn legacy_replica_engine_bootstraps_snapshot_through_core() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let source = dir.path().join("source.db");
    let local = dir.path().join("local.db");
    let page_size = create_marker_db(&source, "from-snapshot")?;
    create_marker_db(&local, "replace-me")?;

    let (snapshot, _checksum) = legacy_ltx::encode_sqlite_snapshot_to_vec(&source, page_size, 7)?;
    let decoded = legacy_replica::bootstrap_from_snapshot_bytes(&snapshot, &local)?;

    assert_eq!(decoded.header.max_txid.into_inner(), 7);
    assert_eq!(marker_value(&local)?, "from-snapshot");
    Ok(())
}
