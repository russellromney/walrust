use anyhow::Result;
use walrust_core::legacy_cache::LocalCache;

#[test]
fn legacy_cache_is_owned_by_core_and_persists_pending_ltx() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let db_path = dir.path().join("cache-source.db");

    {
        let cache = LocalCache::new(&db_path)?;
        cache.write_ltx(7, b"cached-ltx")?;
        assert_eq!(cache.pending_uploads(), vec![7]);
    }

    let reopened = LocalCache::new(&db_path)?;
    assert_eq!(reopened.pending_uploads(), vec![7]);
    assert_eq!(reopened.read_ltx(7)?, b"cached-ltx");
    Ok(())
}
