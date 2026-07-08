use anyhow::Result;
use std::collections::HashMap;
use std::time::Duration;
use walrust_core::legacy_cache::LocalCache;
use walrust_core::legacy_shadow::ShadowSyncOutput;
use walrust_core::legacy_shadow_watch::{
    apply_shadow_sync_results_strict, load_shadow_progress, save_shadow_progress,
    wait_for_cache_checkpoint_durability, ShadowProgress, ShadowWatchState,
};
use walrust_core::shadow::ShadowWal;

#[tokio::test]
async fn legacy_shadow_progress_persistence_is_owned_by_core() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let db_path = dir.path().join("shadow-progress.db");
    {
        let conn = rusqlite::Connection::open(&db_path)?;
        conn.pragma_update(None, "journal_mode", "WAL")?;
        conn.execute_batch(
            "CREATE TABLE marker (id INTEGER PRIMARY KEY, value TEXT);
             INSERT INTO marker (value) VALUES ('shadow-progress');",
        )?;
    }

    let shadow = ShadowWal::new(&db_path).await?;
    let progress = ShadowProgress {
        version: 1,
        current_txid: 17,
        last_snapshot: None,
        db_checksum: Some(0xfeed_beef),
        shadow_sync_generation: shadow.generation(),
        shadow_sync_offset: 4096,
        wal_copy_offset: 8192,
        wal_salt: Some((0x1111_2222, 0x3333_4444)),
        wal_checksum_chain: Some((0xaaaa_bbbb, 0xcccc_dddd)),
    };

    save_shadow_progress(shadow.shadow_dir(), "shadow-progress", &progress)?;
    let loaded = load_shadow_progress(&shadow, "shadow-progress")?.expect("progress saved");

    assert_eq!(loaded.current_txid, progress.current_txid);
    assert_eq!(loaded.db_checksum, progress.db_checksum);
    assert_eq!(
        loaded.shadow_sync_generation,
        progress.shadow_sync_generation
    );
    assert_eq!(loaded.shadow_sync_offset, progress.shadow_sync_offset);
    // B4 restart-window: the live-WAL read cursor, salt, and running checksum
    // chain must round-trip so the first post-restart read resumes validated.
    assert_eq!(loaded.wal_copy_offset, progress.wal_copy_offset);
    assert_eq!(loaded.wal_salt, progress.wal_salt);
    assert_eq!(loaded.wal_checksum_chain, progress.wal_checksum_chain);
    Ok(())
}

#[tokio::test]
async fn legacy_pre_b4_shadow_progress_loads_with_safe_defaults() -> Result<()> {
    // B4 back-compat: a shadow progress file written BEFORE the B4 read-cursor
    // fields existed (no wal_copy_offset / wal_salt / wal_checksum_chain) must
    // still load -- no panic, no parse error -- and fall back conservatively:
    // wal_copy_offset defaults to 0 (re-read from the WAL head) and salt/chain
    // to None (restore_read_cursor becomes a no-op, so nothing stale is seeded).
    let dir = tempfile::tempdir()?;
    let db_path = dir.path().join("legacy-progress.db");
    {
        let conn = rusqlite::Connection::open(&db_path)?;
        conn.pragma_update(None, "journal_mode", "WAL")?;
        conn.execute_batch(
            "CREATE TABLE marker (id INTEGER PRIMARY KEY);
             INSERT INTO marker (id) VALUES (1);",
        )?;
    }
    let shadow = ShadowWal::new(&db_path).await?;

    // A pre-B4 record: exactly the fields that existed before this PR.
    let legacy_json = format!(
        r#"{{
            "version": 1,
            "current_txid": 42,
            "last_snapshot": null,
            "db_checksum": 12345,
            "shadow_sync_generation": {},
            "shadow_sync_offset": 4096
        }}"#,
        shadow.generation()
    );
    std::fs::write(
        walrust_core::legacy_shadow_watch::shadow_progress_path(shadow.shadow_dir()),
        legacy_json,
    )?;

    let loaded =
        load_shadow_progress(&shadow, "legacy-progress")?.expect("pre-B4 progress must still load");
    assert_eq!(loaded.current_txid, 42);
    assert_eq!(loaded.shadow_sync_offset, 4096);
    // The new fields must default to their conservative fallbacks.
    assert_eq!(loaded.wal_copy_offset, 0, "missing field must default to 0");
    assert_eq!(loaded.wal_salt, None, "missing salt must default to None");
    assert_eq!(
        loaded.wal_checksum_chain, None,
        "missing chain must default to None"
    );
    Ok(())
}

#[tokio::test]
async fn legacy_shadow_checkpoint_drain_wait_is_owned_by_core() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let db_path = dir.path().join("checkpoint-drain.db");
    let cache = LocalCache::new(&db_path)?;

    cache.write_ltx(2, b"pending")?;
    let err = wait_for_cache_checkpoint_durability(
        &cache,
        "checkpoint-drain",
        2,
        Duration::from_millis(1),
    )
    .await
    .expect_err("pending upload must block checkpoint drain");
    assert!(err
        .to_string()
        .contains("durable upload confirmation timed out"));

    cache.mark_uploaded(2)?;
    wait_for_cache_checkpoint_durability(&cache, "checkpoint-drain", 2, Duration::from_millis(1))
        .await?;
    Ok(())
}

#[tokio::test]
async fn legacy_shadow_multi_db_sync_apply_is_owned_by_core() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let db_path = dir.path().join("multi-db-shadow.db");
    {
        let conn = rusqlite::Connection::open(&db_path)?;
        conn.pragma_update(None, "journal_mode", "WAL")?;
        conn.execute_batch(
            "CREATE TABLE marker (id INTEGER PRIMARY KEY, value TEXT);
             INSERT INTO marker (value) VALUES ('multi-db-shadow');",
        )?;
    }

    let shadow = ShadowWal::new(&db_path).await?;
    let mut states = HashMap::new();
    states.insert(
        db_path.clone(),
        ShadowWatchState {
            name: "multi-db-shadow".to_string(),
            db_path: db_path.clone(),
            wal_path: db_path.with_extension("db-wal"),
            current_txid: 0,
            last_snapshot: None,
            db_checksum: None,
            shadow,
            shadow_sync_generation: 0,
            shadow_sync_offset: 0,
            wal_copy_offset: 0,
        },
    );

    apply_shadow_sync_results_strict(
        &mut states,
        vec![Ok(ShadowSyncOutput {
            db_path: db_path.clone(),
            frame_count: 1,
            new_shadow_sync_offset: 4096,
            new_current_txid: 9,
            new_db_checksum: Some(0x1234),
        })],
    )
    .await?;

    let state = states.get(&db_path).expect("state retained");
    assert_eq!(state.shadow_sync_offset, 4096);
    assert_eq!(state.current_txid, 9);
    assert_eq!(state.db_checksum, Some(0x1234));
    let loaded = load_shadow_progress(&state.shadow, &state.name)?.expect("progress saved");
    assert_eq!(loaded.current_txid, 9);
    assert_eq!(loaded.shadow_sync_offset, 4096);
    Ok(())
}
