use anyhow::Result;
use std::io::Write;
use walrust_core::legacy_cache::LocalCache;
use walrust_core::legacy_shadow::{sync_shadow_to_cache, ShadowSyncInput};
use walrust_core::legacy_uploader::UploadMessage;

const PAGE_SIZE: u32 = 4096;

fn write_shadow_segment(dir: &std::path::Path, generation: u64, index: u64) -> Result<()> {
    let filename = format!("{generation:016x}-{index:016x}.wal");
    let mut file = std::fs::File::create(dir.join(filename))?;
    let mut header = [0u8; 24];
    header[0..4].copy_from_slice(&1u32.to_be_bytes());
    header[4..8].copy_from_slice(&1u32.to_be_bytes());
    file.write_all(&header)?;
    file.write_all(&vec![0xAB; PAGE_SIZE as usize])?;
    Ok(())
}

#[tokio::test]
async fn legacy_shadow_sync_to_cache_is_owned_by_core() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let db_path = dir.path().join("source.db");
    let shadow_dir = dir.path().join("shadow");
    std::fs::create_dir_all(&shadow_dir)?;
    std::fs::write(&db_path, vec![0u8; PAGE_SIZE as usize])?;
    write_shadow_segment(&shadow_dir, 7, 0)?;

    let cache = LocalCache::new(&db_path)?;
    let (tx, mut rx) = tokio::sync::mpsc::channel::<UploadMessage>(4);
    let output = sync_shadow_to_cache(
        &cache,
        &tx,
        ShadowSyncInput {
            db_path,
            name: "app".to_string(),
            current_txid: 10,
            db_checksum: Some(0x1234_5678),
            generation: 7,
            shadow_sync_offset: 0,
            page_size: PAGE_SIZE,
            shadow_dir,
        },
    )
    .await?;

    assert_eq!(output.frame_count, 1);
    assert_eq!(output.new_current_txid, 11);
    assert_eq!(cache.pending_uploads(), vec![11]);
    assert!(matches!(rx.try_recv()?, UploadMessage::Upload(11)));
    Ok(())
}
