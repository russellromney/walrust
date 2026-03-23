use anyhow::{anyhow, Result};
use std::path::Path;
use tokio::fs::File;
use tokio::io::{AsyncReadExt, AsyncSeekExt, SeekFrom};

/// SQLite WAL file header (32 bytes)
/// https://www.sqlite.org/walformat.html
#[derive(Debug, Clone)]
pub struct WalHeader {
    pub magic: u32,
    pub format_version: u32,
    pub page_size: u32,
    pub checkpoint_seq: u32,
    pub salt1: u32,
    pub salt2: u32,
    pub checksum1: u32,
    pub checksum2: u32,
}

impl WalHeader {
    /// Get salt values as a tuple for comparison
    /// Salt changes indicate a checkpoint occurred
    pub fn salt(&self) -> (u32, u32) {
        (self.salt1, self.salt2)
    }

    /// Check if this header represents a different WAL generation than another
    /// Used to detect checkpoints
    pub fn is_different_generation(&self, other: &WalHeader) -> bool {
        self.salt1 != other.salt1 || self.salt2 != other.salt2
    }
}

pub const WAL_HEADER_SIZE: u64 = 32;
pub const FRAME_HEADER_SIZE: u64 = 24;

/// Read WAL header
pub async fn read_header(path: &Path) -> Result<Option<WalHeader>> {
    let mut file = match File::open(path).await {
        Ok(f) => f,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(e) => return Err(e.into()),
    };

    let metadata = file.metadata().await?;
    if metadata.len() < WAL_HEADER_SIZE {
        return Ok(None);
    }

    let mut buf = [0u8; 32];
    file.read_exact(&mut buf).await?;

    let magic = u32::from_be_bytes([buf[0], buf[1], buf[2], buf[3]]);

    // Check magic number (0x377F0682 or 0x377F0683)
    if magic != 0x377F0682 && magic != 0x377F0683 {
        return Err(anyhow!("Invalid WAL magic number: {:#x}", magic));
    }

    Ok(Some(WalHeader {
        magic,
        format_version: u32::from_be_bytes([buf[4], buf[5], buf[6], buf[7]]),
        page_size: u32::from_be_bytes([buf[8], buf[9], buf[10], buf[11]]),
        checkpoint_seq: u32::from_be_bytes([buf[12], buf[13], buf[14], buf[15]]),
        salt1: u32::from_be_bytes([buf[16], buf[17], buf[18], buf[19]]),
        salt2: u32::from_be_bytes([buf[20], buf[21], buf[22], buf[23]]),
        checksum1: u32::from_be_bytes([buf[24], buf[25], buf[26], buf[27]]),
        checksum2: u32::from_be_bytes([buf[28], buf[29], buf[30], buf[31]]),
    }))
}

/// Read WAL frames starting from offset, returns (frames_data, new_offset, frame_count)
pub async fn read_frames_from(
    path: &Path,
    page_size: u32,
    start_offset: u64,
) -> Result<(Vec<u8>, u64, usize)> {
    let mut file = File::open(path).await?;
    let file_size = file.metadata().await?.len();

    let frame_size = FRAME_HEADER_SIZE + page_size as u64;

    // Calculate start position
    let start_pos = if start_offset == 0 {
        WAL_HEADER_SIZE
    } else {
        start_offset
    };

    if start_pos >= file_size {
        return Ok((Vec::new(), start_pos, 0));
    }

    file.seek(SeekFrom::Start(start_pos)).await?;

    // Read all available frames
    let available = file_size - start_pos;
    let full_frames = available / frame_size;

    if full_frames == 0 {
        return Ok((Vec::new(), start_pos, 0));
    }

    let bytes_to_read = full_frames * frame_size;
    let mut data = vec![0u8; bytes_to_read as usize];
    file.read_exact(&mut data).await?;

    let new_offset = start_pos + bytes_to_read;

    Ok((data, new_offset, full_frames as usize))
}

/// Get current WAL size (for tracking changes)
pub async fn get_wal_size(path: &Path) -> Result<u64> {
    match tokio::fs::metadata(path).await {
        Ok(m) => Ok(m.len()),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(0),
        Err(e) => Err(e.into()),
    }
}

/// Read entire WAL file
pub async fn read_wal(path: &Path) -> Result<Vec<u8>> {
    match tokio::fs::read(path).await {
        Ok(data) => Ok(data),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(Vec::new()),
        Err(e) => Err(e.into()),
    }
}

/// Parsed WAL frame with page number and data
#[derive(Debug, Clone)]
pub struct ParsedFrame {
    pub page_number: u32,
    pub db_size: u32, // Non-zero on commit frames
    pub data: Vec<u8>,
}

/// Result of reading WAL frames with additional metadata for checkpoint detection
#[derive(Debug)]
pub struct WalReadResult {
    /// Parsed frames
    pub frames: Vec<ParsedFrame>,
    /// New offset after reading
    pub new_offset: u64,
    /// Maximum database size in pages (from commit frames)
    pub max_db_size: u32,
    /// WAL header salt values (for checkpoint detection)
    pub salt: (u32, u32),
    /// Whether WAL was truncated during read (file smaller than expected)
    pub truncated_during_read: bool,
}

/// Read WAL frames, deduplicating into a page map during read.
/// Peak memory = unique pages, not total frames.
/// Returns (page_map, frame_count, new_offset, max_db_size)
pub async fn read_frames_as_page_map(
    path: &Path,
    page_size: u32,
    start_offset: u64,
) -> Result<(std::collections::HashMap<u32, Vec<u8>>, usize, u64, u32)> {
    let mut file = File::open(path).await?;
    let file_size = file.metadata().await?.len();

    let frame_size = FRAME_HEADER_SIZE + page_size as u64;

    let start_pos = if start_offset == 0 {
        WAL_HEADER_SIZE
    } else {
        start_offset
    };

    if start_pos >= file_size {
        return Ok((std::collections::HashMap::new(), 0, start_pos, 0));
    }

    file.seek(SeekFrom::Start(start_pos)).await?;

    let available = file_size - start_pos;
    let full_frames = available / frame_size;

    if full_frames == 0 {
        return Ok((std::collections::HashMap::new(), 0, start_pos, 0));
    }

    let mut page_map = std::collections::HashMap::new();
    let mut max_db_size: u32 = 0;
    let mut page_data = vec![0u8; page_size as usize];

    for _ in 0..full_frames {
        let mut header_buf = [0u8; 24];
        file.read_exact(&mut header_buf).await?;

        let page_number = u32::from_be_bytes([header_buf[0], header_buf[1], header_buf[2], header_buf[3]]);
        let db_size = u32::from_be_bytes([header_buf[4], header_buf[5], header_buf[6], header_buf[7]]);

        file.read_exact(&mut page_data).await?;

        if db_size > max_db_size {
            max_db_size = db_size;
        }

        // Dedup in-place: overwrite previous version of the same page.
        // We reuse the buffer and clone only the final version into the map.
        page_map.insert(page_number, page_data.clone());
    }

    let new_offset = start_pos + full_frames * frame_size;

    Ok((page_map, full_frames as usize, new_offset, max_db_size))
}

/// Read and parse WAL frames into pages, returns (pages, new_offset, max_db_size)
pub async fn read_frames_as_pages(
    path: &Path,
    page_size: u32,
    start_offset: u64,
) -> Result<(Vec<ParsedFrame>, u64, u32)> {
    let mut file = File::open(path).await?;
    let file_size = file.metadata().await?.len();

    let frame_size = FRAME_HEADER_SIZE + page_size as u64;

    let start_pos = if start_offset == 0 {
        WAL_HEADER_SIZE
    } else {
        start_offset
    };

    tracing::debug!("read_frames_as_pages: path={:?}, file_size={}, page_size={}, start_offset={}, start_pos={}, frame_size={}",
        path, file_size, page_size, start_offset, start_pos, frame_size);

    if start_pos >= file_size {
        tracing::debug!("read_frames_as_pages: start_pos ({}) >= file_size ({}), returning empty", start_pos, file_size);
        return Ok((Vec::new(), start_pos, 0));
    }

    file.seek(SeekFrom::Start(start_pos)).await?;

    let available = file_size - start_pos;
    let full_frames = available / frame_size;

    tracing::debug!("read_frames_as_pages: available={}, full_frames={}", available, full_frames);

    if full_frames == 0 {
        tracing::debug!("read_frames_as_pages: full_frames=0, returning empty");
        return Ok((Vec::new(), start_pos, 0));
    }

    let mut frames = Vec::with_capacity(full_frames as usize);
    let mut max_db_size: u32 = 0;

    for _ in 0..full_frames {
        // Read frame header (24 bytes)
        let mut header_buf = [0u8; 24];
        file.read_exact(&mut header_buf).await?;

        let page_number = u32::from_be_bytes([header_buf[0], header_buf[1], header_buf[2], header_buf[3]]);
        let db_size = u32::from_be_bytes([header_buf[4], header_buf[5], header_buf[6], header_buf[7]]);

        // Read page data
        let mut page_data = vec![0u8; page_size as usize];
        file.read_exact(&mut page_data).await?;

        if db_size > max_db_size {
            max_db_size = db_size;
        }

        frames.push(ParsedFrame {
            page_number,
            db_size,
            data: page_data,
        });
    }

    let new_offset = start_pos + full_frames * frame_size;

    Ok((frames, new_offset, max_db_size))
}

/// Read WAL frames with full metadata for robust checkpoint detection
///
/// This function provides:
/// - Salt values for detecting checkpoint (WAL reset)
/// - Post-read size verification to detect truncation during read
/// - All frame data with commit information
pub async fn read_frames_with_metadata(
    path: &Path,
    page_size: u32,
    start_offset: u64,
    expected_salt: Option<(u32, u32)>,
) -> Result<WalReadResult> {
    // Read header first to get salt values
    let header = match read_header(path).await? {
        Some(h) => h,
        None => {
            return Ok(WalReadResult {
                frames: Vec::new(),
                new_offset: start_offset,
                max_db_size: 0,
                salt: (0, 0),
                truncated_during_read: false,
            });
        }
    };

    let current_salt = header.salt();

    // Check if checkpoint occurred (salt changed)
    let checkpoint_detected = expected_salt
        .map(|expected| expected != current_salt && expected != (0, 0))
        .unwrap_or(false);

    if checkpoint_detected {
        tracing::info!(
            "WAL checkpoint detected: salt changed from {:?} to {:?}",
            expected_salt,
            current_salt
        );
        // Return empty result with new salt - caller should take a snapshot
        return Ok(WalReadResult {
            frames: Vec::new(),
            new_offset: 0, // Reset offset since WAL was reset
            max_db_size: 0,
            salt: current_salt,
            truncated_during_read: false,
        });
    }

    let mut file = File::open(path).await?;
    let file_size_before = file.metadata().await?.len();

    let frame_size = FRAME_HEADER_SIZE + page_size as u64;

    let start_pos = if start_offset == 0 {
        WAL_HEADER_SIZE
    } else {
        start_offset
    };

    if start_pos >= file_size_before {
        return Ok(WalReadResult {
            frames: Vec::new(),
            new_offset: start_pos,
            max_db_size: 0,
            salt: current_salt,
            truncated_during_read: false,
        });
    }

    file.seek(SeekFrom::Start(start_pos)).await?;

    let available = file_size_before - start_pos;
    let full_frames = available / frame_size;

    if full_frames == 0 {
        return Ok(WalReadResult {
            frames: Vec::new(),
            new_offset: start_pos,
            max_db_size: 0,
            salt: current_salt,
            truncated_during_read: false,
        });
    }

    let mut frames = Vec::with_capacity(full_frames as usize);
    let mut max_db_size: u32 = 0;
    let mut truncated = false;

    for i in 0..full_frames {
        // Read frame header (24 bytes)
        let mut header_buf = [0u8; 24];
        match file.read_exact(&mut header_buf).await {
            Ok(_) => {}
            Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => {
                // WAL was truncated during read
                tracing::warn!(
                    "WAL truncated during read at frame {} (expected {} frames)",
                    i,
                    full_frames
                );
                truncated = true;
                break;
            }
            Err(e) => return Err(e.into()),
        }

        let page_number =
            u32::from_be_bytes([header_buf[0], header_buf[1], header_buf[2], header_buf[3]]);
        let db_size =
            u32::from_be_bytes([header_buf[4], header_buf[5], header_buf[6], header_buf[7]]);

        // Read page data
        let mut page_data = vec![0u8; page_size as usize];
        match file.read_exact(&mut page_data).await {
            Ok(_) => {}
            Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => {
                tracing::warn!(
                    "WAL truncated during page read at frame {} (expected {} frames)",
                    i,
                    full_frames
                );
                truncated = true;
                break;
            }
            Err(e) => return Err(e.into()),
        }

        if db_size > max_db_size {
            max_db_size = db_size;
        }

        frames.push(ParsedFrame {
            page_number,
            db_size,
            data: page_data,
        });
    }

    // Post-read verification: check if file size changed
    let file_size_after = get_wal_size(path).await?;
    if file_size_after < file_size_before {
        tracing::warn!(
            "WAL size decreased during read ({} -> {}), possible checkpoint",
            file_size_before,
            file_size_after
        );
        truncated = true;
    }

    let new_offset = start_pos + (frames.len() as u64 * frame_size);

    Ok(WalReadResult {
        frames,
        new_offset,
        max_db_size,
        salt: current_salt,
        truncated_during_read: truncated,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;

    #[tokio::test]
    async fn test_read_header_nonexistent_file() {
        let path = PathBuf::from("/tmp/nonexistent-wal-file.db-wal");
        let result = read_header(&path).await.unwrap();
        assert!(result.is_none());
    }

    #[tokio::test]
    async fn test_read_header_empty_file() {
        let path = PathBuf::from(format!("/tmp/walrust-test-{}.db-wal", uuid::Uuid::new_v4()));
        tokio::fs::write(&path, &[]).await.unwrap();

        let result = read_header(&path).await.unwrap();
        assert!(result.is_none());

        tokio::fs::remove_file(&path).await.ok();
    }

    #[tokio::test]
    async fn test_read_header_too_small() {
        let path = PathBuf::from(format!("/tmp/walrust-test-{}.db-wal", uuid::Uuid::new_v4()));
        // Write less than 32 bytes
        tokio::fs::write(&path, &[0u8; 20]).await.unwrap();

        let result = read_header(&path).await.unwrap();
        assert!(result.is_none());

        tokio::fs::remove_file(&path).await.ok();
    }

    #[tokio::test]
    async fn test_read_header_invalid_magic() {
        let path = PathBuf::from(format!("/tmp/walrust-test-{}.db-wal", uuid::Uuid::new_v4()));
        // Write 32 bytes with invalid magic
        tokio::fs::write(&path, &[0u8; 32]).await.unwrap();

        let result = read_header(&path).await;
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("Invalid WAL magic"));

        tokio::fs::remove_file(&path).await.ok();
    }

    #[tokio::test]
    async fn test_read_header_valid_magic_big_endian() {
        let path = PathBuf::from(format!("/tmp/walrust-test-{}.db-wal", uuid::Uuid::new_v4()));

        // Create valid WAL header with magic 0x377F0682 (big-endian checksum)
        let mut header = [0u8; 32];
        header[0..4].copy_from_slice(&0x377F0682u32.to_be_bytes()); // magic
        header[4..8].copy_from_slice(&3007000u32.to_be_bytes());     // format version
        header[8..12].copy_from_slice(&4096u32.to_be_bytes());       // page size

        tokio::fs::write(&path, &header).await.unwrap();

        let result = read_header(&path).await.unwrap().unwrap();
        assert_eq!(result.magic, 0x377F0682);
        assert_eq!(result.format_version, 3007000);
        assert_eq!(result.page_size, 4096);

        tokio::fs::remove_file(&path).await.ok();
    }

    #[tokio::test]
    async fn test_read_header_valid_magic_little_endian() {
        let path = PathBuf::from(format!("/tmp/walrust-test-{}.db-wal", uuid::Uuid::new_v4()));

        // Create valid WAL header with magic 0x377F0683 (little-endian checksum)
        let mut header = [0u8; 32];
        header[0..4].copy_from_slice(&0x377F0683u32.to_be_bytes()); // magic
        header[4..8].copy_from_slice(&3007000u32.to_be_bytes());     // format version
        header[8..12].copy_from_slice(&4096u32.to_be_bytes());       // page size

        tokio::fs::write(&path, &header).await.unwrap();

        let result = read_header(&path).await.unwrap().unwrap();
        assert_eq!(result.magic, 0x377F0683);

        tokio::fs::remove_file(&path).await.ok();
    }

    #[tokio::test]
    async fn test_get_wal_size_nonexistent() {
        let path = PathBuf::from("/tmp/nonexistent-wal-file.db-wal");
        let size = get_wal_size(&path).await.unwrap();
        assert_eq!(size, 0);
    }

    #[tokio::test]
    async fn test_get_wal_size_existing() {
        let path = PathBuf::from(format!("/tmp/walrust-test-{}.db-wal", uuid::Uuid::new_v4()));
        let data = vec![0u8; 1024];
        tokio::fs::write(&path, &data).await.unwrap();

        let size = get_wal_size(&path).await.unwrap();
        assert_eq!(size, 1024);

        tokio::fs::remove_file(&path).await.ok();
    }

    #[tokio::test]
    async fn test_read_wal_nonexistent() {
        let path = PathBuf::from("/tmp/nonexistent-wal-file.db-wal");
        let data = read_wal(&path).await.unwrap();
        assert!(data.is_empty());
    }

    #[tokio::test]
    async fn test_read_wal_existing() {
        let path = PathBuf::from(format!("/tmp/walrust-test-{}.db-wal", uuid::Uuid::new_v4()));
        let expected = vec![1u8, 2, 3, 4, 5];
        tokio::fs::write(&path, &expected).await.unwrap();

        let data = read_wal(&path).await.unwrap();
        assert_eq!(data, expected);

        tokio::fs::remove_file(&path).await.ok();
    }

    #[tokio::test]
    async fn test_read_frames_from_no_frames() {
        let path = PathBuf::from(format!("/tmp/walrust-test-{}.db-wal", uuid::Uuid::new_v4()));

        // Create valid WAL header only (no frames)
        let mut header = [0u8; 32];
        header[0..4].copy_from_slice(&0x377F0682u32.to_be_bytes());
        header[8..12].copy_from_slice(&4096u32.to_be_bytes()); // page size

        tokio::fs::write(&path, &header).await.unwrap();

        let (frames, offset, count) = read_frames_from(&path, 4096, 0).await.unwrap();
        assert!(frames.is_empty());
        assert_eq!(offset, WAL_HEADER_SIZE);
        assert_eq!(count, 0);

        tokio::fs::remove_file(&path).await.ok();
    }

    #[tokio::test]
    async fn test_read_frames_from_with_frames() {
        let path = PathBuf::from(format!("/tmp/walrust-test-{}.db-wal", uuid::Uuid::new_v4()));

        let page_size: u32 = 4096;
        let frame_size = FRAME_HEADER_SIZE as usize + page_size as usize;

        // Create WAL header + 2 frames
        let mut data = vec![0u8; 32 + frame_size * 2];
        data[0..4].copy_from_slice(&0x377F0682u32.to_be_bytes()); // magic
        data[8..12].copy_from_slice(&page_size.to_be_bytes());    // page size

        // Fill frame data with recognizable pattern
        for i in 0..frame_size * 2 {
            data[32 + i] = (i % 256) as u8;
        }

        tokio::fs::write(&path, &data).await.unwrap();

        let (frames, offset, count) = read_frames_from(&path, page_size, 0).await.unwrap();
        assert_eq!(count, 2);
        assert_eq!(frames.len(), frame_size * 2);
        assert_eq!(offset, WAL_HEADER_SIZE + (frame_size * 2) as u64);

        tokio::fs::remove_file(&path).await.ok();
    }

    #[tokio::test]
    async fn test_read_frames_from_with_offset() {
        let path = PathBuf::from(format!("/tmp/walrust-test-{}.db-wal", uuid::Uuid::new_v4()));

        let page_size: u32 = 4096;
        let frame_size = FRAME_HEADER_SIZE as usize + page_size as usize;

        // Create WAL header + 3 frames
        let mut data = vec![0u8; 32 + frame_size * 3];
        data[0..4].copy_from_slice(&0x377F0682u32.to_be_bytes());
        data[8..12].copy_from_slice(&page_size.to_be_bytes());

        tokio::fs::write(&path, &data).await.unwrap();

        // Read starting after first frame
        let start_offset = WAL_HEADER_SIZE + frame_size as u64;
        let (frames, offset, count) = read_frames_from(&path, page_size, start_offset).await.unwrap();

        assert_eq!(count, 2); // Should get remaining 2 frames
        assert_eq!(frames.len(), frame_size * 2);
        assert_eq!(offset, start_offset + (frame_size * 2) as u64);

        tokio::fs::remove_file(&path).await.ok();
    }

    #[tokio::test]
    async fn test_read_frames_partial_frame_ignored() {
        let path = PathBuf::from(format!("/tmp/walrust-test-{}.db-wal", uuid::Uuid::new_v4()));

        let page_size: u32 = 4096;
        let frame_size = FRAME_HEADER_SIZE as usize + page_size as usize;

        // Create WAL header + 1 full frame + partial frame
        let mut data = vec![0u8; 32 + frame_size + 100]; // 100 bytes of partial frame
        data[0..4].copy_from_slice(&0x377F0682u32.to_be_bytes());
        data[8..12].copy_from_slice(&page_size.to_be_bytes());

        tokio::fs::write(&path, &data).await.unwrap();

        let (frames, _offset, count) = read_frames_from(&path, page_size, 0).await.unwrap();

        // Should only return 1 complete frame, ignoring partial
        assert_eq!(count, 1);
        assert_eq!(frames.len(), frame_size);

        tokio::fs::remove_file(&path).await.ok();
    }

    #[test]
    fn test_wal_header_salt() {
        let header = WalHeader {
            magic: 0x377F0682,
            format_version: 3007000,
            page_size: 4096,
            checkpoint_seq: 1,
            salt1: 0x12345678,
            salt2: 0xABCDEF00,
            checksum1: 0,
            checksum2: 0,
        };

        assert_eq!(header.salt(), (0x12345678, 0xABCDEF00));
    }

    #[test]
    fn test_wal_header_is_different_generation() {
        let header1 = WalHeader {
            magic: 0x377F0682,
            format_version: 3007000,
            page_size: 4096,
            checkpoint_seq: 1,
            salt1: 0x12345678,
            salt2: 0xABCDEF00,
            checksum1: 0,
            checksum2: 0,
        };

        let header2 = WalHeader {
            salt1: 0x12345678,
            salt2: 0xABCDEF00,
            ..header1.clone()
        };

        let header3 = WalHeader {
            salt1: 0x87654321, // Different salt
            salt2: 0xABCDEF00,
            ..header1.clone()
        };

        assert!(!header1.is_different_generation(&header2));
        assert!(header1.is_different_generation(&header3));
    }

    #[tokio::test]
    async fn test_read_frames_with_metadata_no_wal() {
        let path = PathBuf::from("/tmp/nonexistent-wal-for-metadata.db-wal");

        let result = read_frames_with_metadata(&path, 4096, 0, None).await.unwrap();

        assert!(result.frames.is_empty());
        assert_eq!(result.salt, (0, 0));
        assert!(!result.truncated_during_read);
    }

    #[tokio::test]
    async fn test_read_frames_with_metadata_checkpoint_detection() {
        let path = PathBuf::from(format!("/tmp/walrust-test-meta-{}.db-wal", uuid::Uuid::new_v4()));

        // Create WAL header with specific salt
        let mut header = [0u8; 32];
        header[0..4].copy_from_slice(&0x377F0682u32.to_be_bytes());
        header[8..12].copy_from_slice(&4096u32.to_be_bytes());
        header[16..20].copy_from_slice(&0x11111111u32.to_be_bytes()); // salt1
        header[20..24].copy_from_slice(&0x22222222u32.to_be_bytes()); // salt2

        tokio::fs::write(&path, &header).await.unwrap();

        // Read with different expected salt - should detect checkpoint
        let old_salt = (0xAAAAAAAA, 0xBBBBBBBB);
        let result = read_frames_with_metadata(&path, 4096, 0, Some(old_salt)).await.unwrap();

        // Should return new salt and reset offset
        assert_eq!(result.salt, (0x11111111, 0x22222222));
        assert_eq!(result.new_offset, 0); // Reset due to checkpoint

        tokio::fs::remove_file(&path).await.ok();
    }

    #[tokio::test]
    async fn test_read_frames_with_metadata_same_salt() {
        let path = PathBuf::from(format!("/tmp/walrust-test-meta-{}.db-wal", uuid::Uuid::new_v4()));

        let page_size: u32 = 4096;
        let frame_size = FRAME_HEADER_SIZE as usize + page_size as usize;

        // Create WAL header + 1 frame with specific salt
        let mut data = vec![0u8; 32 + frame_size];
        data[0..4].copy_from_slice(&0x377F0682u32.to_be_bytes());
        data[8..12].copy_from_slice(&page_size.to_be_bytes());
        data[16..20].copy_from_slice(&0x11111111u32.to_be_bytes()); // salt1
        data[20..24].copy_from_slice(&0x22222222u32.to_be_bytes()); // salt2

        // Frame header: page 1, db_size 1
        data[32..36].copy_from_slice(&1u32.to_be_bytes()); // page_number
        data[36..40].copy_from_slice(&1u32.to_be_bytes()); // db_size

        tokio::fs::write(&path, &data).await.unwrap();

        // Read with same salt - should proceed normally
        let same_salt = (0x11111111, 0x22222222);
        let result = read_frames_with_metadata(&path, page_size, 0, Some(same_salt)).await.unwrap();

        assert_eq!(result.frames.len(), 1);
        assert_eq!(result.salt, same_salt);
        assert!(!result.truncated_during_read);

        tokio::fs::remove_file(&path).await.ok();
    }
}
