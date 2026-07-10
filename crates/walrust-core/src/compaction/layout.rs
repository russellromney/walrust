//! The layout trait and the shared forever key scheme.
//!
//! [`CompactionLayout`] is the *thin* seam the merge engine speaks to. It only
//! exposes what the engine needs: list files at a level (with seq range and a
//! last-modified timestamp), open one for streaming reads, write a ranged
//! merged object, and delete a set. Two adapters implement it —
//! [`super::seq_layout::SeqLayout`] and [`super::range_layout::RangeLayout`] —
//! and they must never diverge in capability.
//!
//! The key-naming functions here ([`format_range_name`], [`parse_range_name`],
//! [`level_subpath`]) are the **forever** scheme; see the module header.

use std::sync::Arc;

use async_trait::async_trait;
use hadb_changeset::physical::{PageEntry, PageId, PageIdSize};
use hadb_storage::StorageBackend;

use super::CompactionError;

/// Compaction level. `0` = raw (L0), `1` = L1, `2` = L2, …
pub type Level = u32;

/// Storage sub-path (under `{prefix}{db}/`) that holds a compaction level's
/// merged objects. Levels `L >= 1` live under this dedicated directory, **not**
/// in a hex generation folder. **Forever.**
///
/// Why not a generation number: the litestream-heritage legacy layout stores
/// both live incrementals and *snapshots* under `{db}/{gen:04x}/`, and its
/// snapshot generation **increments by one per snapshot** (`snapshot_gen =
/// current_gen + 1` in `legacy_wal_sync`). The 16th snapshot therefore lands in
/// `0010/` — exactly where a `0x0010`-based compaction L1 would have gone. That
/// is a two-way corruption: legacy discovery (`discover_legacy_snapshots`,
/// which treats every file in generations `1..=max` as a snapshot) would
/// classify compaction merged objects as snapshots, and compaction's
/// `list_level` would ingest real LTX snapshots as merge sources. A dedicated
/// non-hex sub-path is structurally invisible to every existing discovery path:
/// legacy scanners only accept `{gen}/{file}` (2 parts) and `{file}` (1 part)
/// relative shapes, and `parse_generation("levels")` / `("L1")` fail because
/// they are not hex.
pub const LEVELS_DIR: &str = "levels";

/// Directory name for compaction level 0 — the existing incremental pool that
/// the compactor only *reads*. **Forever.**
pub const L0_DIR: &str = "0000";

/// Fixed hex width for a `u64` seq bound in a range filename. **Forever.**
pub const SEQ_HEX_WIDTH: usize = 16;

/// Map a compaction level to its storage sub-path (under `{prefix}{db}/`).
///
/// - Level 0 → `0000` (the incremental pool; read-only for compaction).
/// - Level `L >= 1` → `levels/L{L}` (a dedicated namespace no existing
///   discovery path can misread — see [`LEVELS_DIR`]).
pub fn level_subpath(level: Level) -> String {
    if level == 0 {
        L0_DIR.to_string()
    } else {
        format!("{LEVELS_DIR}/L{level}")
    }
}

/// An inclusive seq span covered by one file.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SeqRange {
    pub min: u64,
    pub max: u64,
}

impl SeqRange {
    pub fn point(seq: u64) -> Self {
        Self { min: seq, max: seq }
    }
    pub fn new(min: u64, max: u64) -> Self {
        Self { min, max }
    }
}

/// Format a merged/ranged filename: `{min:016x}-{max:016x}.{ext}`. **Forever.**
pub fn format_range_name(range: SeqRange, ext: &str) -> String {
    format!(
        "{:0width$x}-{:0width$x}.{}",
        range.min,
        range.max,
        ext,
        width = SEQ_HEX_WIDTH
    )
}

/// Parse a filename back into a seq range. Accepts both the range form
/// `{min:016x}-{max:016x}.{ext}` and the single-seq point form
/// `{seq:016x}.{ext}` (the seq layout's level-0 files). Returns `None` for
/// anything else so junk keys are ignored, not misread.
pub fn parse_range_name(filename: &str, ext: &str) -> Option<SeqRange> {
    let stem = filename.strip_suffix(&format!(".{ext}"))?;
    match stem.split_once('-') {
        Some((lo, hi)) => {
            let min = u64::from_str_radix(lo, 16).ok()?;
            let max = u64::from_str_radix(hi, 16).ok()?;
            if max < min {
                return None;
            }
            Some(SeqRange::new(min, max))
        }
        None => {
            let seq = u64::from_str_radix(stem, 16).ok()?;
            Some(SeqRange::point(seq))
        }
    }
}

/// One file discovered at a level.
#[derive(Debug, Clone)]
pub struct LayoutFile {
    /// Full storage key.
    pub key: String,
    /// Level this file lives at.
    pub level: Level,
    /// Inclusive seq span the file covers.
    pub range: SeqRange,
    /// Last-modified timestamp in Unix milliseconds. Derived from the HADBP
    /// header `created_ms` field (storage-object metadata; `StorageBackend`
    /// exposes no HEAD, so the self-describing changeset header is the honest
    /// source). Used only by `keep_fine_window` to exempt young L0 files.
    pub last_modified_ms: i64,
}

/// A page-at-a-time reader over one source changeset. The engine consumes this;
/// it holds **exactly one page** at a time so the merge stays memory-bounded.
///
/// Pages are yielded in ascending `page_id` order (HADBP canonical order).
#[async_trait]
pub trait ChangesetPageStream: Send {
    async fn next_page(&mut self) -> Result<Option<PageEntry>, CompactionError>;
}

/// The layout seam. Adapters translate levels and ranges into keys; the engine
/// never sees a key format.
#[async_trait]
pub trait CompactionLayout: Send + Sync {
    /// List files at `level`, each with its seq range and last-modified time,
    /// sorted ascending by `range.min`.
    async fn list_level(&self, level: Level) -> Result<Vec<LayoutFile>, CompactionError>;

    /// Count the objects at `level` cheaply — one LIST, no per-object header
    /// reads. Used only to seed trigger state at startup, where the count is all
    /// that matters. The default falls back to `list_level(..).len()` (which
    /// reads headers); adapters over a real backend override it with a LIST-only
    /// count to avoid `O(files)` ranged GETs at startup.
    async fn count_level(&self, level: Level) -> Result<usize, CompactionError> {
        Ok(self.list_level(level).await?.len())
    }

    /// Parse the light header of a source: geometry + linkage metadata, without
    /// reading its pages.
    async fn read_header(&self, file: &LayoutFile) -> Result<SourceHeader, CompactionError>;

    /// Open a source for streaming page reads.
    async fn open(
        &self,
        file: &LayoutFile,
    ) -> Result<Box<dyn ChangesetPageStream>, CompactionError>;

    /// Write a merged object at `level` covering `range`. Returns the file it
    /// wrote. Durable on return.
    async fn write_merged(
        &self,
        level: Level,
        range: SeqRange,
        bytes: &[u8],
    ) -> Result<LayoutFile, CompactionError>;

    /// Read back a previously written object's full bytes (for verification).
    async fn read_bytes(&self, file: &LayoutFile) -> Result<Vec<u8>, CompactionError>;

    /// Delete a set of files. Missing keys are not an error.
    async fn delete(&self, files: &[LayoutFile]) -> Result<(), CompactionError>;
}

/// Light header of a source changeset: geometry + linkage, no pages.
#[derive(Debug, Clone, Copy)]
pub struct SourceHeader {
    pub page_id_size: PageIdSize,
    pub page_size: u32,
    pub seq: u64,
    pub prev_checksum: u64,
    pub page_count: u32,
    pub created_ms: i64,
    /// `Some` for a COMPACTED (v2) source: its declared end-of-range chain
    /// value, read straight from the header. `None` for a normal source (its
    /// chain-end is its trailer content checksum).
    pub declared_end_checksum: Option<u64>,
}

// ── HADBP header wire offsets (mirror hadb-changeset::physical) ──────────────
// magic(5) version(1) flags(1) page_id_size(1) page_size(4) seq(8)
// prev_checksum(8) page_count(4) created_ms(8) [declared_end(8) if v2]
const OFF_VERSION: usize = 5;
const OFF_FLAGS: usize = 6;
const OFF_PAGE_ID_SIZE: usize = 7;
const OFF_PAGE_SIZE: usize = 8;
const OFF_SEQ: usize = 12;
const OFF_PREV: usize = 20;
const OFF_PAGE_COUNT: usize = 28;
const OFF_CREATED: usize = 32;
const HEADER_V1_SIZE: usize = 40;
const DECLARED_FIELD: usize = 8;
const FLAG_COMPACTED: u8 = 0x80;
const VERSION_V1: u8 = 1;
const VERSION_V2: u8 = 2;

/// Parse a source header from the object's leading bytes. Needs at least
/// `HEADER_V1_SIZE + 8` bytes to also cover a v2 declared field.
pub(crate) fn parse_source_header(head: &[u8]) -> Result<SourceHeader, CompactionError> {
    if head.len() < HEADER_V1_SIZE {
        return Err(CompactionError::MixedGeometry(format!(
            "header truncated: {} bytes",
            head.len()
        )));
    }
    if &head[0..5] != b"HADBP" {
        return Err(CompactionError::MixedGeometry("bad magic".into()));
    }
    let version = head[OFF_VERSION];
    let flags = head[OFF_FLAGS];
    let compacted = match version {
        VERSION_V1 => false,
        VERSION_V2 => {
            if flags & FLAG_COMPACTED == 0 {
                return Err(CompactionError::MixedGeometry(
                    "v2 without compacted flag".into(),
                ));
            }
            true
        }
        other => {
            return Err(CompactionError::MixedGeometry(format!(
                "unsupported version {other}"
            )))
        }
    };
    let page_id_size = match head[OFF_PAGE_ID_SIZE] {
        4 => PageIdSize::U32,
        8 => PageIdSize::U64,
        b => {
            return Err(CompactionError::MixedGeometry(format!(
                "bad page_id_size {b}"
            )))
        }
    };
    let page_size = u32::from_be_bytes(head[OFF_PAGE_SIZE..OFF_PAGE_SIZE + 4].try_into().unwrap());
    let seq = u64::from_be_bytes(head[OFF_SEQ..OFF_SEQ + 8].try_into().unwrap());
    let prev_checksum = u64::from_be_bytes(head[OFF_PREV..OFF_PREV + 8].try_into().unwrap());
    let page_count =
        u32::from_be_bytes(head[OFF_PAGE_COUNT..OFF_PAGE_COUNT + 4].try_into().unwrap());
    let created_ms = i64::from_be_bytes(head[OFF_CREATED..OFF_CREATED + 8].try_into().unwrap());
    let declared_end_checksum = if compacted {
        if head.len() < HEADER_V1_SIZE + DECLARED_FIELD {
            return Err(CompactionError::MixedGeometry(
                "v2 header missing declared field".into(),
            ));
        }
        Some(u64::from_be_bytes(
            head[HEADER_V1_SIZE..HEADER_V1_SIZE + 8].try_into().unwrap(),
        ))
    } else {
        None
    };
    Ok(SourceHeader {
        page_id_size,
        page_size,
        seq,
        prev_checksum,
        page_count,
        created_ms,
        declared_end_checksum,
    })
}

/// The byte offset where the page body begins, given the source header.
fn body_offset(header: &SourceHeader) -> u64 {
    if header.declared_end_checksum.is_some() {
        (HEADER_V1_SIZE + DECLARED_FIELD) as u64
    } else {
        HEADER_V1_SIZE as u64
    }
}

/// Read a source header from storage with a single small range GET.
pub(crate) async fn read_header_from_storage(
    storage: &dyn StorageBackend,
    key: &str,
) -> Result<SourceHeader, CompactionError> {
    let head = storage
        .range_get(key, 0, (HEADER_V1_SIZE + DECLARED_FIELD) as u32)
        .await
        .map_err(|e| CompactionError::Storage(e.to_string()))?
        .ok_or_else(|| CompactionError::Storage(format!("missing object {key}")))?;
    parse_source_header(&head)
}

/// A [`ChangesetPageStream`] over a HADBP object in a [`StorageBackend`].
///
/// Streams pages via incremental `range_get`s, buffering exactly one page. This
/// is what keeps a merge `O(page_size × sources)`, not `O(total bytes)`.
pub struct StorageChangesetStream {
    storage: Arc<dyn StorageBackend>,
    key: String,
    page_id_size: PageIdSize,
    cursor: u64,
    remaining: u32,
}

impl StorageChangesetStream {
    pub fn open(storage: Arc<dyn StorageBackend>, key: &str, header: &SourceHeader) -> Self {
        Self {
            storage,
            key: key.to_string(),
            page_id_size: header.page_id_size,
            cursor: body_offset(header),
            remaining: header.page_count,
        }
    }
}

#[async_trait]
impl ChangesetPageStream for StorageChangesetStream {
    async fn next_page(&mut self) -> Result<Option<PageEntry>, CompactionError> {
        if self.remaining == 0 {
            return Ok(None);
        }
        let pid_len = self.page_id_size as usize;
        // Read the fixed prefix: page_id (pid_len) + data_len (4).
        let prefix = self
            .storage
            .range_get(&self.key, self.cursor, (pid_len + 4) as u32)
            .await
            .map_err(|e| CompactionError::Storage(e.to_string()))?
            .ok_or_else(|| CompactionError::Storage(format!("missing object {}", self.key)))?;
        if prefix.len() < pid_len + 4 {
            return Err(CompactionError::Storage(format!(
                "short page prefix in {} at {}",
                self.key, self.cursor
            )));
        }
        let page_id = match self.page_id_size {
            PageIdSize::U32 => PageId::U32(u32::from_be_bytes(prefix[0..4].try_into().unwrap())),
            PageIdSize::U64 => PageId::U64(u64::from_be_bytes(prefix[0..8].try_into().unwrap())),
        };
        let data_len =
            u32::from_be_bytes(prefix[pid_len..pid_len + 4].try_into().unwrap()) as usize;
        let data_start = self.cursor + (pid_len + 4) as u64;
        let data = if data_len == 0 {
            Vec::new()
        } else {
            self.storage
                .range_get(&self.key, data_start, data_len as u32)
                .await
                .map_err(|e| CompactionError::Storage(e.to_string()))?
                .ok_or_else(|| CompactionError::Storage(format!("missing object {}", self.key)))?
        };
        if data.len() != data_len {
            return Err(CompactionError::Storage(format!(
                "short page data in {}: want {} got {}",
                self.key,
                data_len,
                data.len()
            )));
        }
        self.cursor = data_start + data_len as u64;
        self.remaining -= 1;
        Ok(Some(PageEntry { page_id, data }))
    }
}

/// Shared generation-directory layout logic. Both adapters ([`SeqLayout`],
/// [`RangeLayout`][super::range_layout::RangeLayout]) are thin wrappers over
/// this: they differ only in the filename extension. Level-0 files are parsed
/// tolerantly ([`parse_range_name`] accepts both the seq-point and min-max
/// forms), so one code path serves both heritages.
pub(crate) struct GenLayoutCore {
    storage: Arc<dyn StorageBackend>,
    prefix: String,
    db_name: String,
    ext: &'static str,
}

impl GenLayoutCore {
    pub(crate) fn new(
        storage: Arc<dyn StorageBackend>,
        prefix: &str,
        db_name: &str,
        ext: &'static str,
    ) -> Self {
        Self {
            storage,
            prefix: prefix.to_string(),
            db_name: db_name.to_string(),
            ext,
        }
    }

    fn level_dir(&self, level: Level) -> String {
        format!("{}{}/{}/", self.prefix, self.db_name, level_subpath(level))
    }

    pub(crate) async fn list_level(
        &self,
        level: Level,
    ) -> Result<Vec<LayoutFile>, CompactionError> {
        let dir = self.level_dir(level);
        let keys = self
            .storage
            .list(&dir, None)
            .await
            .map_err(|e| CompactionError::Storage(e.to_string()))?;
        let mut files = Vec::new();
        for key in keys {
            let Some(filename) = key.strip_prefix(&dir) else {
                continue;
            };
            let Some(range) = parse_range_name(filename, self.ext) else {
                continue; // ignore junk / other extensions, don't misread
            };
            // last-modified comes from the object's own HADBP header.
            let header = read_header_from_storage(self.storage.as_ref(), &key).await?;
            files.push(LayoutFile {
                key,
                level,
                range,
                last_modified_ms: header.created_ms,
            });
        }
        files.sort_by_key(|f| (f.range.min, f.range.max));
        Ok(files)
    }

    /// Count the parseable objects at `level` with a **single LIST and no
    /// per-object header reads** — the cheap path used to seed trigger state at
    /// startup, where only the count matters. (`list_level` additionally reads
    /// one ranged header per file for `last_modified_ms`; that cost is only paid
    /// at merge time, not for seeding.)
    pub(crate) async fn count_level(&self, level: Level) -> Result<usize, CompactionError> {
        let dir = self.level_dir(level);
        let keys = self
            .storage
            .list(&dir, None)
            .await
            .map_err(|e| CompactionError::Storage(e.to_string()))?;
        let count = keys
            .iter()
            .filter_map(|key| key.strip_prefix(&dir))
            .filter(|filename| parse_range_name(filename, self.ext).is_some())
            .count();
        Ok(count)
    }

    pub(crate) async fn read_header(
        &self,
        file: &LayoutFile,
    ) -> Result<SourceHeader, CompactionError> {
        read_header_from_storage(self.storage.as_ref(), &file.key).await
    }

    pub(crate) async fn open(
        &self,
        file: &LayoutFile,
    ) -> Result<Box<dyn ChangesetPageStream>, CompactionError> {
        let header = read_header_from_storage(self.storage.as_ref(), &file.key).await?;
        Ok(Box::new(StorageChangesetStream::open(
            self.storage.clone(),
            &file.key,
            &header,
        )))
    }

    pub(crate) async fn write_merged(
        &self,
        level: Level,
        range: SeqRange,
        bytes: &[u8],
    ) -> Result<LayoutFile, CompactionError> {
        let key = format!(
            "{}{}",
            self.level_dir(level),
            format_range_name(range, self.ext)
        );
        self.storage
            .put(&key, bytes)
            .await
            .map_err(|e| CompactionError::Storage(e.to_string()))?;
        let header = parse_source_header(bytes)?;
        Ok(LayoutFile {
            key,
            level,
            range,
            last_modified_ms: header.created_ms,
        })
    }

    pub(crate) async fn read_bytes(&self, file: &LayoutFile) -> Result<Vec<u8>, CompactionError> {
        self.storage
            .get(&file.key)
            .await
            .map_err(|e| CompactionError::Storage(e.to_string()))?
            .ok_or_else(|| CompactionError::Storage(format!("missing object {}", file.key)))
    }

    pub(crate) async fn delete(&self, files: &[LayoutFile]) -> Result<(), CompactionError> {
        let keys: Vec<String> = files.iter().map(|f| f.key.clone()).collect();
        self.storage
            .delete_many(&keys)
            .await
            .map_err(|e| CompactionError::Storage(e.to_string()))?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn level_subpath_scheme_is_frozen() {
        // L0 is the existing incremental pool (read-only for compaction).
        assert_eq!(level_subpath(0), "0000");
        // L>=1 live under a dedicated non-hex namespace so no legacy/owned
        // generation scanner can misread them (see the collision note on
        // LEVELS_DIR).
        assert_eq!(level_subpath(1), "levels/L1");
        assert_eq!(level_subpath(2), "levels/L2");
        assert_eq!(level_subpath(3), "levels/L3");
    }

    #[test]
    fn level_dirs_are_not_valid_generation_hex() {
        // The legacy layout parses a generation folder with
        // `u64::from_str_radix(component, 16)`. Every component of a compaction
        // L>=1 sub-path must fail that parse, so a legacy scan can never treat a
        // compaction object as a snapshot generation.
        for level in 1..=8u32 {
            for component in level_subpath(level).split('/') {
                assert!(
                    u64::from_str_radix(component, 16).is_err(),
                    "level {level} path component {component:?} must not parse as hex"
                );
            }
        }
    }

    #[test]
    fn range_name_roundtrip_and_ordering() {
        let a = format_range_name(SeqRange::new(1, 5), "hadbp");
        assert_eq!(a, "0000000000000001-0000000000000005.hadbp");
        let b = format_range_name(SeqRange::new(6, 10), "hadbp");
        // Lexicographic order == numeric order of `min`.
        assert!(a < b);
        assert_eq!(parse_range_name(&a, "hadbp"), Some(SeqRange::new(1, 5)));
        // Point form (seq layout L0).
        assert_eq!(
            parse_range_name("000000000000002a.hadbp", "hadbp"),
            Some(SeqRange::point(42))
        );
    }

    #[test]
    fn separator_sorts_before_hex_digits() {
        // A point file at seq S and a range file starting at S must order by min.
        let point = format_range_name(SeqRange::point(3), "ltx");
        let range = format_range_name(SeqRange::new(3, 9), "ltx");
        // "...0003-...0003" vs "...0003-...0009": both share min, point <= range.
        assert!(point <= range);
    }

    #[test]
    fn parse_rejects_junk_and_inverted_ranges() {
        assert_eq!(parse_range_name("readme.txt", "hadbp"), None);
        assert_eq!(parse_range_name("not-hex.hadbp", "hadbp"), None);
        // max < min is rejected.
        assert_eq!(
            parse_range_name("0000000000000009-0000000000000003.hadbp", "hadbp"),
            None
        );
    }

    #[test]
    fn parse_source_header_reads_v1_and_v2() {
        use hadb_changeset::physical::{self, PhysicalChangeset};
        let cs = PhysicalChangeset::new(
            7,
            42,
            PageIdSize::U64,
            262144,
            vec![PageEntry {
                page_id: PageId::U64(0),
                data: vec![0xAA; 32],
            }],
        );
        let bytes = physical::encode(&cs);
        let h = parse_source_header(&bytes).unwrap();
        assert_eq!(h.seq, 7);
        assert_eq!(h.prev_checksum, 42);
        assert_eq!(h.page_count, 1);
        assert_eq!(h.declared_end_checksum, None);

        let compacted = PhysicalChangeset::new_compacted(
            9,
            42,
            PageIdSize::U64,
            262144,
            vec![PageEntry {
                page_id: PageId::U64(1),
                data: vec![0xBB; 32],
            }],
            0xDEAD_BEEF,
        );
        let cbytes = physical::encode(&compacted);
        let ch = parse_source_header(&cbytes).unwrap();
        assert_eq!(ch.declared_end_checksum, Some(0xDEAD_BEEF));
    }
}
