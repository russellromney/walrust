use anyhow::Result;
use async_trait::async_trait;
use hadb_io::webhook::WebhookSender;
use hadb_storage::{CasResult, StorageBackend};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use walrust_core::legacy_cache::LocalCache;
use walrust_core::legacy_ltx;
use walrust_core::legacy_manifest::build_ltx_key;
use walrust_core::legacy_uploader::{spawn_uploader, UploadMessage, Uploader};
use walrust_core::RetryPolicy;

#[derive(Default)]
struct MemoryStorage {
    objects: Mutex<HashMap<String, Vec<u8>>>,
}

#[async_trait]
impl StorageBackend for MemoryStorage {
    async fn get(&self, key: &str) -> Result<Option<Vec<u8>>> {
        Ok(self.objects.lock().unwrap().get(key).cloned())
    }

    async fn put(&self, key: &str, data: &[u8]) -> Result<()> {
        self.objects
            .lock()
            .unwrap()
            .insert(key.to_string(), data.to_vec());
        Ok(())
    }

    async fn put_if_absent(&self, key: &str, data: &[u8]) -> Result<CasResult> {
        let mut objects = self.objects.lock().unwrap();
        if objects.contains_key(key) {
            Ok(CasResult {
                success: false,
                etag: None,
            })
        } else {
            objects.insert(key.to_string(), data.to_vec());
            Ok(CasResult {
                success: true,
                etag: Some("mem".into()),
            })
        }
    }

    async fn put_if_match(&self, key: &str, data: &[u8], _etag: &str) -> Result<CasResult> {
        self.objects
            .lock()
            .unwrap()
            .insert(key.to_string(), data.to_vec());
        Ok(CasResult {
            success: true,
            etag: Some("mem".into()),
        })
    }

    async fn delete(&self, key: &str) -> Result<()> {
        self.objects.lock().unwrap().remove(key);
        Ok(())
    }

    async fn list(&self, prefix: &str, _after: Option<&str>) -> Result<Vec<String>> {
        let mut keys = self
            .objects
            .lock()
            .unwrap()
            .keys()
            .filter(|key| key.starts_with(prefix))
            .cloned()
            .collect::<Vec<_>>();
        keys.sort();
        Ok(keys)
    }
}

#[tokio::test]
async fn legacy_uploader_is_owned_by_core_and_uploads_cached_ltx() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let db_path = dir.path().join("source.db");
    {
        let conn = rusqlite::Connection::open(&db_path)?;
        conn.pragma_update(None, "page_size", 4096)?;
        conn.execute_batch(
            "CREATE TABLE marker (id INTEGER PRIMARY KEY, value TEXT);
             INSERT INTO marker (value) VALUES ('uploaded-through-core');",
        )?;
    }

    let (snapshot, _checksum) = legacy_ltx::encode_sqlite_snapshot_to_vec(&db_path, 4096, 1)?;
    let cache = Arc::new(LocalCache::new(&db_path)?);
    let storage = Arc::new(MemoryStorage::default());

    cache.write_snapshot_ltx(1, &snapshot)?;

    let uploader = Arc::new(Uploader::new(
        "app".to_string(),
        cache.clone(),
        storage.clone(),
        "backups".to_string(),
        Arc::new(RetryPolicy::default_policy()),
        Arc::new(WebhookSender::new(vec![])),
        1,
    ));

    let (tx, handle) = spawn_uploader(uploader);
    tx.send(UploadMessage::Shutdown).await?;
    let stats = handle.await??;

    let key = build_ltx_key("backups", "app", 1, 1, 1);
    assert_eq!(storage.get(&key).await?, Some(snapshot));
    assert_eq!(cache.pending_uploads(), Vec::<u64>::new());
    assert_eq!(stats.uploads_succeeded, 1);
    assert_eq!(stats.last_contiguous_uploaded_txid, 1);

    Ok(())
}
