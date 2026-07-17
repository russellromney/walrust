//! Litestream-heritage **range layout** adapter.
//!
//! Every level names files with the min-max range scheme — the filename
//! discipline litestream uses for its live LTX pool. L0 is the live pool at
//! `{prefix}{db}/0000/{min:016x}-{max:016x}.ltx`; merged levels live under the
//! dedicated `{prefix}{db}/levels/L{L}/{min:016x}-{max:016x}.ltx` sub-path
//! (**not** a hex generation folder — see the module header for the legacy
//! snapshot-generation collision that forces this). Payloads are HADBP
//! changesets (see the module header's "both adapters carry HADBP payloads"
//! decision); the `ltx` extension names the heritage key namespace, not the LTX
//! byte format.

use std::sync::Arc;

use async_trait::async_trait;
use hadb_storage::StorageBackend;

use super::layout::{
    ChangesetPageStream, CompactionLayout, GenLayoutCore, LayoutFile, Level, SeqRange, SourceHeader,
};
use super::CompactionError;

/// Compaction adapter over the litestream-heritage `min-max.ltx` range layout.
pub struct RangeLayout {
    core: GenLayoutCore,
}

impl RangeLayout {
    pub fn new(storage: Arc<dyn StorageBackend>, prefix: &str, db_name: &str) -> Self {
        Self {
            core: GenLayoutCore::new(storage, prefix, db_name, "ltx"),
        }
    }
}

#[async_trait]
impl CompactionLayout for RangeLayout {
    async fn list_level(&self, level: Level) -> Result<Vec<LayoutFile>, CompactionError> {
        self.core.list_level(level).await
    }
    async fn count_level(&self, level: Level) -> Result<usize, CompactionError> {
        self.core.count_level(level).await
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
