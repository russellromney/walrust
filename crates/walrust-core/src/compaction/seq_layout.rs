//! Owned-mode **seq layout** adapter.
//!
//! Level-0 files are the existing one-object-per-seq incrementals
//! (`{prefix}{db}/0000/{seq:016x}.hadbp`, hadb's canonical `format_key`).
//! Merged levels use the forever range-name scheme with the `hadbp` extension.
//! See the module header for the full key scheme.

use std::sync::Arc;

use async_trait::async_trait;
use hadb_storage::StorageBackend;

use super::layout::{
    ChangesetPageStream, CompactionLayout, GenLayoutCore, LayoutFile, Level, SeqRange, SourceHeader,
};
use super::CompactionError;

/// Compaction adapter over the owned-mode `{seq}.hadbp` layout.
pub struct SeqLayout {
    core: GenLayoutCore,
}

impl SeqLayout {
    pub fn new(storage: Arc<dyn StorageBackend>, prefix: &str, db_name: &str) -> Self {
        Self {
            core: GenLayoutCore::new(storage, prefix, db_name, "hadbp"),
        }
    }
}

#[async_trait]
impl CompactionLayout for SeqLayout {
    async fn list_level(&self, level: Level) -> Result<Vec<LayoutFile>, CompactionError> {
        self.core.list_level(level).await
    }
    async fn read_header(&self, file: &LayoutFile) -> Result<SourceHeader, CompactionError> {
        self.core.read_header(file).await
    }
    async fn open(
        &self,
        file: &LayoutFile,
    ) -> Result<Box<dyn ChangesetPageStream>, CompactionError> {
        self.core.open(file).await
    }
    async fn write_merged(
        &self,
        level: Level,
        range: SeqRange,
        bytes: &[u8],
    ) -> Result<LayoutFile, CompactionError> {
        self.core.write_merged(level, range, bytes).await
    }
    async fn read_bytes(&self, file: &LayoutFile) -> Result<Vec<u8>, CompactionError> {
        self.core.read_bytes(file).await
    }
    async fn delete(&self, files: &[LayoutFile]) -> Result<(), CompactionError> {
        self.core.delete(files).await
    }
}
