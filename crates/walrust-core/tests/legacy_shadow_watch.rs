use anyhow::Result;
use std::time::Duration;
use walrust_core::legacy_cache::LocalCache;
use walrust_core::legacy_shadow_watch::{
    load_shadow_progress, save_shadow_progress, wait_for_cache_checkpoint_durability,
    ShadowProgress,
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
