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

/// WAL frame header (24 bytes per frame)
#[derive(Debug, Clone)]
pub struct FrameHeader {
    pub page_number: u32,
    pub db_size: u32, // Size of database in pages after commit (0 if not commit frame)
    pub salt1: u32,
    pub salt2: u32,
    pub checksum1: u32,
    pub checksum2: u32,
}

pub const WAL_HEADER_SIZE: u64 = 32;
pub const FRAME_HEADER_SIZE: u64 = 24;

/// WAL magic for the big-endian checksum variant.
pub const WAL_MAGIC_BE: u32 = 0x377F_0682;
/// WAL magic for the little-endian checksum variant.
pub const WAL_MAGIC_LE: u32 = 0x377F_0683;

/// SQLite WAL cumulative checksum (the s0/s1 Fibonacci-weighted sum).
///
/// `data` must be a whole number of 32-bit words (length a multiple of 8 bytes,
/// per the SQLite spec which feeds an even count of 32-bit ints). `big_endian`
/// selects how each 4-byte word is interpreted, chosen by the WAL magic
/// (`0x377f0682` => big-endian, `0x377f0683` => little-endian). The running
/// `(s0, s1)` seed is the previous checksum: `(0, 0)` for the header, the
/// header checksum for frame 1, then the prior frame's checksum thereafter.
///
/// Returns the updated `(s0, s1)`.
pub fn wal_checksum(seed: (u32, u32), data: &[u8], big_endian: bool) -> (u32, u32) {
    debug_assert!(data.len() % 8 == 0, "checksum input must be 8-byte aligned");
    let (mut s0, mut s1) = seed;
    let mut i = 0;
    while i + 8 <= data.len() {
        let x0 = read_u32(&data[i..i + 4], big_endian);
        let x1 = read_u32(&data[i + 4..i + 8], big_endian);
        s0 = s0.wrapping_add(x0).wrapping_add(s1);
        s1 = s1.wrapping_add(x1).wrapping_add(s0);
        i += 8;
    }
    (s0, s1)
}

#[inline]
fn read_u32(b: &[u8], big_endian: bool) -> u32 {
    let arr = [b[0], b[1], b[2], b[3]];
    if big_endian {
        u32::from_be_bytes(arr)
    } else {
        u32::from_le_bytes(arr)
    }
}

/// True if the WAL magic selects big-endian checksum word interpretation.
fn magic_is_big_endian(magic: u32) -> bool {
    // 0x377f0682 => big-endian, 0x377f0683 => little-endian.
    magic & 1 == 0
}

/// Validate the 32-byte WAL header's own checksum.
///
/// The header checksum is computed over the first 24 bytes of the header,
/// seeded with `(0, 0)`, and stored big-endian in bytes 24..32 regardless of
/// the magic-selected body endianness. Returns the header's `(checksum1,
/// checksum2)` seed for the frame chain when valid.
///
/// A synthetic header whose stored checksum is `(0, 0)` (e.g. hand-built test
/// WALs) does not validate; callers treat that as "no checksum chain to verify"
/// rather than an error.
pub fn validate_header_checksum(header_bytes: &[u8; 32], big_endian: bool) -> Option<(u32, u32)> {
    let stored = (
        u32::from_be_bytes([
            header_bytes[24],
            header_bytes[25],
            header_bytes[26],
            header_bytes[27],
        ]),
        u32::from_be_bytes([
            header_bytes[28],
            header_bytes[29],
            header_bytes[30],
            header_bytes[31],
        ]),
    );
    if stored == (0, 0) {
        return None;
    }
    let computed = wal_checksum((0, 0), &header_bytes[0..24], big_endian);
    if computed == stored {
        Some(stored)
    } else {
        None
    }
}

/// Verify one frame against the running checksum chain.
///
/// SQLite computes a frame's checksum seeded with the prior cumulative checksum,
/// over the first 8 bytes of the 24-byte frame header (page number + db size)
/// followed by the entire page body. The result is stored big-endian in frame
/// header bytes 16..24. Returns the new running checksum on success, or `None`
/// on mismatch (a torn or corrupt frame).
pub fn verify_frame_checksum(
    seed: (u32, u32),
    frame_header: &[u8; 24],
    page_body: &[u8],
    big_endian: bool,
) -> Option<(u32, u32)> {
    let stored = (
        u32::from_be_bytes([
            frame_header[16],
            frame_header[17],
            frame_header[18],
            frame_header[19],
        ]),
        u32::from_be_bytes([
            frame_header[20],
            frame_header[21],
            frame_header[22],
            frame_header[23],
        ]),
    );
    let mut running = wal_checksum(seed, &frame_header[0..8], big_endian);
    running = wal_checksum(running, page_body, big_endian);
    if running == stored {
        Some(running)
    } else {
        None
    }
}

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

/// Read WAL frames and deduplicate into a page map in one pass.
///
/// Unlike `read_frames_as_pages()` which returns `Vec<ParsedFrame>` (holding ALL frames
/// in memory), this deduplicates during read: each page number maps to its latest data.
/// Peak memory = unique pages, not total frames. For a WAL with 1000 frames touching
/// 50 unique pages, this uses 50 * page_size instead of 1000 * page_size.
///
/// Returns (page_map, committed_frame_count, new_offset, final_db_size, commit_count).
/// `commit_count` is the number of committed transactions in the batch (frames with
/// non-zero db_size_after_commit). Used for deterministic TXID derivation in WAL mode
/// where the file change counter is not incremented per-transaction.
pub async fn read_frames_as_page_map(
    path: &Path,
    page_size: u32,
    start_offset: u64,
) -> Result<(
    std::collections::HashMap<u32, Vec<u8>>,
    usize,
    u64,
    u32,
    u64,
)> {
    let (map, frames, offset, db_size, commits, _chain) =
        read_frames_as_page_map_checked(path, page_size, start_offset, None).await?;
    Ok((map, frames, offset, db_size, commits))
}

/// Checksum-validating variant of [`read_frames_as_page_map`].
///
/// `chain_seed` is the running WAL checksum `(s0, s1)` of the last frame already
/// consumed at `start_offset`. Pass `None` when starting from the WAL header
/// (offset 0): the seed is then the validated header checksum. The chain is
/// verified per frame; a frame whose stored checksum does not match is treated
/// as a torn / partial tail — reading stops at the last good *committed* frame,
/// exactly as if the bad frame and everything after it were not yet written.
///
/// Validation is skipped only when the WAL header carries no checksum
/// (`(0, 0)`), which is the case for hand-built synthetic WALs but never for a
/// real SQLite WAL. The returned final chain value lets the caller seed the
/// next incremental read.
pub async fn read_frames_as_page_map_checked(
    path: &Path,
    page_size: u32,
    start_offset: u64,
    chain_seed: Option<(u32, u32)>,
) -> Result<(
    std::collections::HashMap<u32, Vec<u8>>,
    usize,
    u64,
    u32,
    u64,
    Option<(u32, u32)>,
)> {
    let mut file = File::open(path).await?;
    let file_size = file.metadata().await?.len();

    let frame_size = FRAME_HEADER_SIZE + page_size as u64;

    let start_pos = if start_offset == 0 {
        WAL_HEADER_SIZE
    } else {
        start_offset
    };

    // Read the 32-byte header to decide checksum endianness and obtain the
    // chain seed for the first frame (offset 0 case).
    let mut header_bytes = [0u8; WAL_HEADER_SIZE as usize];
    let header_seed = if file_size >= WAL_HEADER_SIZE {
        file.seek(SeekFrom::Start(0)).await?;
        file.read_exact(&mut header_bytes).await?;
        let magic = u32::from_be_bytes([
            header_bytes[0],
            header_bytes[1],
            header_bytes[2],
            header_bytes[3],
        ]);
        if magic == WAL_MAGIC_BE || magic == WAL_MAGIC_LE {
            let be = magic_is_big_endian(magic);
            validate_header_checksum(&header_bytes, be).map(|seed| (seed, be))
        } else {
            None
        }
    } else {
        None
    };

    if start_pos >= file_size {
        return Ok((std::collections::HashMap::new(), 0, start_pos, 0, 0, chain_seed));
    }

    file.seek(SeekFrom::Start(start_pos)).await?;

    let available = file_size - start_pos;
    let full_frames = available / frame_size;

    if full_frames == 0 {
        return Ok((std::collections::HashMap::new(), 0, start_pos, 0, 0, chain_seed));
    }

    // Checksum chain. We can only validate when the header carries a checksum.
    // The seed at `start_pos` is the caller-supplied chain (mid-WAL) or the
    // header checksum (start_pos == header).
    let (mut running, big_endian, validate) = match header_seed {
        Some((hdr_seed, be)) => {
            let seed = if start_pos == WAL_HEADER_SIZE {
                hdr_seed
            } else {
                // Mid-WAL incremental read: caller must supply the running chain.
                // If absent we cannot validate this slice; skip rather than
                // false-reject.
                match chain_seed {
                    Some(c) => c,
                    None => (0, 0),
                }
            };
            let validate = start_pos == WAL_HEADER_SIZE || chain_seed.is_some();
            (seed, be, validate)
        }
        None => ((0, 0), true, false),
    };

    let mut frame_headers = Vec::with_capacity(full_frames as usize);
    let mut page_data = vec![0u8; page_size as usize];
    let mut valid_frames: u64 = 0;

    for frame_index in 0..full_frames {
        let mut header_buf = [0u8; 24];
        file.read_exact(&mut header_buf).await?;
        file.read_exact(&mut page_data).await?;

        if validate {
            match verify_frame_checksum(running, &header_buf, &page_data, big_endian) {
                Some(next) => running = next,
                None => {
                    // Torn / corrupt frame: stop. Everything from here on is
                    // unreliable (a torn tail frame whose db_size happens to be
                    // non-zero must not be treated as a commit boundary).
                    tracing::warn!(
                        "WAL checksum mismatch at frame {} ({:?}); treating as torn tail",
                        frame_index + 1,
                        path
                    );
                    break;
                }
            }
        }

        let page_number =
            u32::from_be_bytes([header_buf[0], header_buf[1], header_buf[2], header_buf[3]]);
        let db_size =
            u32::from_be_bytes([header_buf[4], header_buf[5], header_buf[6], header_buf[7]]);

        frame_headers.push((frame_index + 1, page_number, db_size));
        valid_frames += 1;
    }

    // Only frames that passed the checksum chain are eligible.
    frame_headers.truncate(valid_frames as usize);

    let Some((committed_frames, _, final_db_size)) = frame_headers
        .iter()
        .rev()
        .find(|(_, _, db_size)| *db_size > 0)
        .copied()
    else {
        return Ok((std::collections::HashMap::new(), 0, start_pos, 0, 0, chain_seed));
    };
    let committed_frames = committed_frames as usize;
    let commit_count = frame_headers[..committed_frames]
        .iter()
        .filter(|(_, _, db_size)| *db_size > 0)
        .count() as u64;

    file.seek(SeekFrom::Start(start_pos)).await?;

    let mut page_map = std::collections::HashMap::new();
    // Recompute the chain over exactly the committed prefix so the returned
    // chain value matches the last consumed frame.
    let mut out_chain = match header_seed {
        Some((hdr_seed, _)) if start_pos == WAL_HEADER_SIZE => Some(hdr_seed),
        _ => chain_seed,
    };

    for (idx, (_, page_number, _)) in frame_headers
        .into_iter()
        .take(committed_frames)
        .enumerate()
    {
        let mut header_buf = [0u8; 24];
        file.read_exact(&mut header_buf).await?;
        file.read_exact(&mut page_data).await?;

        if validate {
            if let Some(seed) = out_chain {
                if let Some(next) = verify_frame_checksum(seed, &header_buf, &page_data, big_endian)
                {
                    out_chain = Some(next);
                }
            }
        }
        let _ = idx;

        // Dedup in-place: overwrite previous version of the same page.
        // We reuse the buffer and clone only the final committed version into the map.
        page_map.insert(page_number, page_data.clone());
    }

    let new_offset = start_pos + committed_frames as u64 * frame_size;

    Ok((
        page_map,
        committed_frames,
        new_offset,
        final_db_size,
        commit_count,
        out_chain,
    ))
}

/// Count committed transactions in the entire WAL file.
///
/// Scans from the beginning (after the 32-byte header) and counts frames with
/// non-zero db_size_after_commit. Used by take_snapshot to derive a deterministic
/// TXID in WAL mode: TXID = file_change_counter + wal_commit_count.
pub async fn count_wal_commits(path: &Path, page_size: u32) -> Result<u64> {
    let mut file = match File::open(path).await {
        Ok(f) => f,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(0),
        Err(e) => return Err(e.into()),
    };
    let file_size = file.metadata().await?.len();
    let frame_size = FRAME_HEADER_SIZE + page_size as u64;

    if file_size < WAL_HEADER_SIZE {
        return Ok(0);
    }

    file.seek(SeekFrom::Start(WAL_HEADER_SIZE)).await?;

    let available = file_size - WAL_HEADER_SIZE;
    let full_frames = available / frame_size;
    let mut commit_count: u64 = 0;

    // Only need the 8-byte frame header prefix (page_number + db_size), skip page data
    for _ in 0..full_frames {
        let mut header_buf = [0u8; 8];
        file.read_exact(&mut header_buf).await?;
        let db_size =
            u32::from_be_bytes([header_buf[4], header_buf[5], header_buf[6], header_buf[7]]);
        if db_size > 0 {
            commit_count += 1;
        }
        // Skip remaining frame header (16 bytes) + page data
        let skip = (FRAME_HEADER_SIZE - 8) + page_size as u64;
        file.seek(SeekFrom::Current(skip as i64)).await?;
    }

    Ok(commit_count)
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
        tracing::debug!(
            "read_frames_as_pages: start_pos ({}) >= file_size ({}), returning empty",
            start_pos,
            file_size
        );
        return Ok((Vec::new(), start_pos, 0));
    }

    file.seek(SeekFrom::Start(start_pos)).await?;

    let available = file_size - start_pos;
    let full_frames = available / frame_size;

    tracing::debug!(
        "read_frames_as_pages: available={}, full_frames={}",
        available,
        full_frames
    );

    if full_frames == 0 {
        tracing::debug!("read_frames_as_pages: full_frames=0, returning empty");
        return Ok((Vec::new(), start_pos, 0));
    }

    let mut frames = Vec::with_capacity(full_frames as usize);

    for _ in 0..full_frames {
        // Read frame header (24 bytes)
        let mut header_buf = [0u8; 24];
        file.read_exact(&mut header_buf).await?;

        let page_number =
            u32::from_be_bytes([header_buf[0], header_buf[1], header_buf[2], header_buf[3]]);
        let db_size =
            u32::from_be_bytes([header_buf[4], header_buf[5], header_buf[6], header_buf[7]]);

        // Read page data
        let mut page_data = vec![0u8; page_size as usize];
        file.read_exact(&mut page_data).await?;

        frames.push(ParsedFrame {
            page_number,
            db_size,
            data: page_data,
        });
    }

    let committed_frames = frames
        .iter()
        .rposition(|frame| frame.db_size > 0)
        .map(|idx| idx + 1)
        .unwrap_or(0);

    if committed_frames == 0 {
        return Ok((Vec::new(), start_pos, 0));
    }

    frames.truncate(committed_frames);
    let final_db_size = frames
        .iter()
        .rev()
        .find(|frame| frame.db_size > 0)
        .map(|frame| frame.db_size)
        .unwrap_or(0);
    let new_offset = start_pos + committed_frames as u64 * frame_size;

    Ok((frames, new_offset, final_db_size))
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

    /// Build a real SQLite-format WAL with a correctly checksummed header and
    /// frame chain. `frames` is `(page_number, db_size_after_commit, fill)`.
    /// Returns the WAL bytes. Endianness follows the magic.
    fn build_valid_wal(page_size: u32, salt: (u32, u32), frames: &[(u32, u32, u8)]) -> Vec<u8> {
        let magic = WAL_MAGIC_BE;
        let be = magic_is_big_endian(magic);
        let mut header = [0u8; 32];
        header[0..4].copy_from_slice(&magic.to_be_bytes());
        header[4..8].copy_from_slice(&3007000u32.to_be_bytes());
        header[8..12].copy_from_slice(&page_size.to_be_bytes());
        header[12..16].copy_from_slice(&0u32.to_be_bytes()); // checkpoint seq
        header[16..20].copy_from_slice(&salt.0.to_be_bytes());
        header[20..24].copy_from_slice(&salt.1.to_be_bytes());
        // Header checksum over the first 24 bytes, seeded with (0,0).
        let hdr_cs = wal_checksum((0, 0), &header[0..24], be);
        header[24..28].copy_from_slice(&hdr_cs.0.to_be_bytes());
        header[28..32].copy_from_slice(&hdr_cs.1.to_be_bytes());

        let mut wal = header.to_vec();
        let mut running = hdr_cs;
        for &(page_number, db_size, fill) in frames {
            let mut fh = [0u8; 24];
            fh[0..4].copy_from_slice(&page_number.to_be_bytes());
            fh[4..8].copy_from_slice(&db_size.to_be_bytes());
            fh[8..12].copy_from_slice(&salt.0.to_be_bytes());
            fh[12..16].copy_from_slice(&salt.1.to_be_bytes());
            let body = vec![fill; page_size as usize];
            // Frame checksum: prior running checksum + 8 header bytes + body.
            running = wal_checksum(running, &fh[0..8], be);
            running = wal_checksum(running, &body, be);
            fh[16..20].copy_from_slice(&running.0.to_be_bytes());
            fh[20..24].copy_from_slice(&running.1.to_be_bytes());
            wal.extend_from_slice(&fh);
            wal.extend_from_slice(&body);
        }
        wal
    }

    #[test]
    fn test_wal_checksum_golden_vector() {
        // Golden vector for the s0/s1 Fibonacci-weighted sum, big-endian words.
        // Input: two 32-bit words [1, 2] => s0 = 1, s1 = 3.
        let data = [0u8, 0, 0, 1, 0, 0, 0, 2];
        assert_eq!(wal_checksum((0, 0), &data, true), (1, 3));

        // Four words [1,2,3,4]:
        //   i=0: s0 = 0+1+0 = 1; s1 = 0+2+1 = 3
        //   i=1: s0 = 1+3+3 = 7; s1 = 3+4+7 = 14
        let data2 = [0, 0, 0, 1, 0, 0, 0, 2, 0, 0, 0, 3, 0, 0, 0, 4];
        assert_eq!(wal_checksum((0, 0), &data2, true), (7, 14));

        // Little-endian interpretation of the same bytes differs.
        let le = wal_checksum((0, 0), &data2, false);
        assert_ne!(le, (7, 14));
    }

    #[tokio::test]
    async fn test_checked_reader_accepts_valid_chain() {
        let path = PathBuf::from(format!(
            "/tmp/walrust-test-chk-valid-{}.db-wal",
            uuid::Uuid::new_v4()
        ));
        let page_size = 1024u32;
        // Two frames, second is the commit (db_size = 2).
        let wal = build_valid_wal(
            page_size,
            (0x1111_1111, 0x2222_2222),
            &[(1, 0, 0xAA), (2, 2, 0xBB)],
        );
        tokio::fs::write(&path, &wal).await.unwrap();

        let (pages, frame_count, _offset, db_size, commit_count, chain) =
            read_frames_as_page_map_checked(&path, page_size, 0, None)
                .await
                .unwrap();

        assert_eq!(frame_count, 2, "both valid frames must be accepted");
        assert_eq!(db_size, 2);
        assert_eq!(commit_count, 1);
        assert_eq!(pages.len(), 2);
        assert!(chain.is_some(), "valid chain returns a running checksum");

        tokio::fs::remove_file(&path).await.ok();
    }

    #[tokio::test]
    async fn test_checked_reader_stops_at_torn_tail() {
        // A torn tail frame whose db_size is non-zero must NOT be treated as a
        // commit boundary; the reader must stop at the last good commit.
        let path = PathBuf::from(format!(
            "/tmp/walrust-test-chk-torn-{}.db-wal",
            uuid::Uuid::new_v4()
        ));
        let page_size = 1024u32;
        let mut wal = build_valid_wal(
            page_size,
            (0x1111_1111, 0x2222_2222),
            &[(1, 1, 0xAA), (2, 2, 0xBB)],
        );
        // Corrupt the second frame's page body (flip a byte) without fixing its
        // checksum: a classic torn write.
        let second_frame_body = WAL_HEADER_SIZE as usize
            + (FRAME_HEADER_SIZE as usize + page_size as usize)
            + FRAME_HEADER_SIZE as usize;
        wal[second_frame_body] ^= 0xFF;
        tokio::fs::write(&path, &wal).await.unwrap();

        let (pages, frame_count, _offset, db_size, commit_count, _chain) =
            read_frames_as_page_map_checked(&path, page_size, 0, None)
                .await
                .unwrap();

        assert_eq!(
            frame_count, 1,
            "torn second frame must be rejected, only frame 1 survives"
        );
        assert_eq!(db_size, 1, "commit boundary is the last good frame");
        assert_eq!(commit_count, 1);
        assert_eq!(pages.len(), 1);
        assert!(pages.contains_key(&1));
        assert!(!pages.contains_key(&2), "corrupt frame's page must be dropped");

        tokio::fs::remove_file(&path).await.ok();
    }

    #[tokio::test]
    async fn test_checked_reader_rejects_torn_commit_with_bogus_db_size() {
        // The exact F2 scenario: a torn tail frame carrying a non-zero db_size
        // appended after a valid commit. Without checksum validation the old
        // reader would treat the torn frame as the new commit boundary and ship
        // garbage. With validation it must stop at the prior valid commit.
        let path = PathBuf::from(format!(
            "/tmp/walrust-test-chk-bogus-{}.db-wal",
            uuid::Uuid::new_v4()
        ));
        let page_size = 1024u32;
        let mut wal = build_valid_wal(
            page_size,
            (0xDEAD_BEEF, 0xFEED_FACE),
            &[(1, 1, 0x11)],
        );
        // Append a hand-built "frame" with a non-zero db_size but a garbage
        // checksum (simulating a partially written commit frame).
        let mut torn = [0u8; 24];
        torn[0..4].copy_from_slice(&2u32.to_be_bytes()); // page 2
        torn[4..8].copy_from_slice(&2u32.to_be_bytes()); // db_size = 2 (looks like commit)
        // checksum bytes left as a value that will not match
        torn[16..20].copy_from_slice(&0xDEAD_C0DEu32.to_be_bytes());
        torn[20..24].copy_from_slice(&0xC0DE_DEADu32.to_be_bytes());
        wal.extend_from_slice(&torn);
        wal.extend_from_slice(&vec![0x99u8; page_size as usize]);
        tokio::fs::write(&path, &wal).await.unwrap();

        let (pages, frame_count, _offset, db_size, _commit_count, _chain) =
            read_frames_as_page_map_checked(&path, page_size, 0, None)
                .await
                .unwrap();

        assert_eq!(frame_count, 1, "torn commit frame must be rejected");
        assert_eq!(db_size, 1, "db_size stays at the last valid commit");
        assert!(!pages.contains_key(&2));

        tokio::fs::remove_file(&path).await.ok();
    }

    #[tokio::test]
    async fn test_checked_reader_skips_validation_for_synthetic_wal() {
        // Hand-built WALs with a zero header checksum (used elsewhere in the
        // suite) must still parse: validation only engages for real WALs.
        let path = PathBuf::from(format!(
            "/tmp/walrust-test-chk-synth-{}.db-wal",
            uuid::Uuid::new_v4()
        ));
        let page_size = 1024u32;
        let frame_size = FRAME_HEADER_SIZE as usize + page_size as usize;
        let mut data = vec![0u8; 32 + frame_size];
        data[0..4].copy_from_slice(&WAL_MAGIC_BE.to_be_bytes());
        data[8..12].copy_from_slice(&page_size.to_be_bytes());
        // frame: page 1, db_size 1 (commit), zero checksum
        data[32..36].copy_from_slice(&1u32.to_be_bytes());
        data[36..40].copy_from_slice(&1u32.to_be_bytes());
        tokio::fs::write(&path, &data).await.unwrap();

        let (pages, frame_count, _offset, db_size, _commit, chain) =
            read_frames_as_page_map_checked(&path, page_size, 0, None)
                .await
                .unwrap();

        assert_eq!(frame_count, 1);
        assert_eq!(db_size, 1);
        assert_eq!(pages.len(), 1);
        assert!(chain.is_none(), "no header checksum => no chain to track");

        tokio::fs::remove_file(&path).await.ok();
    }

    #[tokio::test]
    async fn test_checked_reader_incremental_with_chain_seed() {
        // Read frame 1, then read frame 2 starting at the new offset using the
        // returned chain seed. Both reads must validate.
        let path = PathBuf::from(format!(
            "/tmp/walrust-test-chk-incr-{}.db-wal",
            uuid::Uuid::new_v4()
        ));
        let page_size = 1024u32;
        let frame_size = FRAME_HEADER_SIZE + page_size as u64;
        let wal = build_valid_wal(
            page_size,
            (0x0102_0304, 0x0506_0708),
            &[(1, 1, 0xAA), (2, 2, 0xBB)],
        );
        tokio::fs::write(&path, &wal).await.unwrap();

        // First read: only frame 1 visible (truncate file to header + 1 frame).
        let one_frame = wal[..(WAL_HEADER_SIZE + frame_size) as usize].to_vec();
        tokio::fs::write(&path, &one_frame).await.unwrap();
        let (_pages, _fc, offset1, _db, _cc, chain1) =
            read_frames_as_page_map_checked(&path, page_size, 0, None)
                .await
                .unwrap();
        assert_eq!(offset1, WAL_HEADER_SIZE + frame_size);
        let chain1 = chain1.expect("first read returns a chain");

        // Second read: full WAL, start at offset1, seed with chain1.
        tokio::fs::write(&path, &wal).await.unwrap();
        let (pages2, fc2, offset2, db2, _cc2, _chain2) =
            read_frames_as_page_map_checked(&path, page_size, offset1, Some(chain1))
                .await
                .unwrap();
        assert_eq!(fc2, 1, "only the new frame is read");
        assert_eq!(offset2, WAL_HEADER_SIZE + frame_size * 2);
        assert_eq!(db2, 2);
        assert!(pages2.contains_key(&2));

        tokio::fs::remove_file(&path).await.ok();
    }

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
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("Invalid WAL magic"));

        tokio::fs::remove_file(&path).await.ok();
    }

    #[tokio::test]
    async fn test_read_header_valid_magic_big_endian() {
        let path = PathBuf::from(format!("/tmp/walrust-test-{}.db-wal", uuid::Uuid::new_v4()));

        // Create valid WAL header with magic 0x377F0682 (big-endian checksum)
        let mut header = [0u8; 32];
        header[0..4].copy_from_slice(&0x377F0682u32.to_be_bytes()); // magic
        header[4..8].copy_from_slice(&3007000u32.to_be_bytes()); // format version
        header[8..12].copy_from_slice(&4096u32.to_be_bytes()); // page size

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
        header[4..8].copy_from_slice(&3007000u32.to_be_bytes()); // format version
        header[8..12].copy_from_slice(&4096u32.to_be_bytes()); // page size

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
        data[8..12].copy_from_slice(&page_size.to_be_bytes()); // page size

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
        let (frames, offset, count) = read_frames_from(&path, page_size, start_offset)
            .await
            .unwrap();

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

        let result = read_frames_with_metadata(&path, 4096, 0, None)
            .await
            .unwrap();

        assert!(result.frames.is_empty());
        assert_eq!(result.salt, (0, 0));
        assert!(!result.truncated_during_read);
    }

    #[tokio::test]
    async fn test_read_frames_with_metadata_checkpoint_detection() {
        let path = PathBuf::from(format!(
            "/tmp/walrust-test-meta-{}.db-wal",
            uuid::Uuid::new_v4()
        ));

        // Create WAL header with specific salt
        let mut header = [0u8; 32];
        header[0..4].copy_from_slice(&0x377F0682u32.to_be_bytes());
        header[8..12].copy_from_slice(&4096u32.to_be_bytes());
        header[16..20].copy_from_slice(&0x11111111u32.to_be_bytes()); // salt1
        header[20..24].copy_from_slice(&0x22222222u32.to_be_bytes()); // salt2

        tokio::fs::write(&path, &header).await.unwrap();

        // Read with different expected salt - should detect checkpoint
        let old_salt = (0xAAAAAAAA, 0xBBBBBBBB);
        let result = read_frames_with_metadata(&path, 4096, 0, Some(old_salt))
            .await
            .unwrap();

        // Should return new salt and reset offset
        assert_eq!(result.salt, (0x11111111, 0x22222222));
        assert_eq!(result.new_offset, 0); // Reset due to checkpoint

        tokio::fs::remove_file(&path).await.ok();
    }

    #[tokio::test]
    async fn test_read_frames_with_metadata_same_salt() {
        let path = PathBuf::from(format!(
            "/tmp/walrust-test-meta-{}.db-wal",
            uuid::Uuid::new_v4()
        ));

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
        let result = read_frames_with_metadata(&path, page_size, 0, Some(same_salt))
            .await
            .unwrap();

        assert_eq!(result.frames.len(), 1);
        assert_eq!(result.salt, same_salt);
        assert!(!result.truncated_during_read);

        tokio::fs::remove_file(&path).await.ok();
    }

    // ============================================
    // read_frames_as_page_map tests
    // ============================================

    #[tokio::test]
    async fn test_read_frames_as_page_map_empty() {
        let path = PathBuf::from(format!(
            "/tmp/walrust-test-pagemap-{}.db-wal",
            uuid::Uuid::new_v4()
        ));

        // Create valid WAL header only (no frames)
        let mut header = [0u8; 32];
        header[0..4].copy_from_slice(&0x377F0682u32.to_be_bytes());
        header[8..12].copy_from_slice(&4096u32.to_be_bytes());

        tokio::fs::write(&path, &header).await.unwrap();

        let (page_map, frame_count, offset, max_db_size, commit_count) =
            read_frames_as_page_map(&path, 4096, 0).await.unwrap();

        assert!(page_map.is_empty());
        assert_eq!(frame_count, 0);
        assert_eq!(offset, WAL_HEADER_SIZE);
        assert_eq!(max_db_size, 0);
        assert_eq!(commit_count, 0);

        tokio::fs::remove_file(&path).await.ok();
    }

    #[tokio::test]
    async fn test_read_frames_as_page_map_deduplicates() {
        // Regression test: read_frames_as_page_map must deduplicate during read.
        // If the same page appears multiple times, only the latest version should be in the map.
        // Peak memory = unique pages, NOT total frames.
        let path = PathBuf::from(format!(
            "/tmp/walrust-test-pagemap-dedup-{}.db-wal",
            uuid::Uuid::new_v4()
        ));

        let page_size: u32 = 4096;
        let frame_header_size = 24usize;
        let frame_size = frame_header_size + page_size as usize;

        // Create WAL with 4 frames: page 1 (v1), page 2, page 1 (v2), page 3
        // Result should have 3 unique pages, with page 1 being v2
        let mut data = vec![0u8; 32 + frame_size * 4];
        data[0..4].copy_from_slice(&0x377F0682u32.to_be_bytes());
        data[8..12].copy_from_slice(&page_size.to_be_bytes());

        // Frame 0: page 1, v1 (will be overwritten)
        let f0 = 32;
        data[f0..f0 + 4].copy_from_slice(&1u32.to_be_bytes()); // page_number=1
        data[f0 + 4..f0 + 8].copy_from_slice(&3u32.to_be_bytes()); // db_size=3
        for b in &mut data[f0 + frame_header_size..f0 + frame_size] {
            *b = 0x11;
        } // v1 data

        // Frame 1: page 2
        let f1 = 32 + frame_size;
        data[f1..f1 + 4].copy_from_slice(&2u32.to_be_bytes()); // page_number=2
        data[f1 + 4..f1 + 8].copy_from_slice(&0u32.to_be_bytes()); // db_size=0
        for b in &mut data[f1 + frame_header_size..f1 + frame_size] {
            *b = 0x22;
        }

        // Frame 2: page 1, v2 (overwrites v1)
        let f2 = 32 + frame_size * 2;
        data[f2..f2 + 4].copy_from_slice(&1u32.to_be_bytes()); // page_number=1
        data[f2 + 4..f2 + 8].copy_from_slice(&0u32.to_be_bytes()); // db_size=0
        for b in &mut data[f2 + frame_header_size..f2 + frame_size] {
            *b = 0xAA;
        } // v2 data

        // Frame 3: page 3
        let f3 = 32 + frame_size * 3;
        data[f3..f3 + 4].copy_from_slice(&3u32.to_be_bytes()); // page_number=3
        data[f3 + 4..f3 + 8].copy_from_slice(&3u32.to_be_bytes()); // db_size=3 (commit)
        for b in &mut data[f3 + frame_header_size..f3 + frame_size] {
            *b = 0x33;
        }

        tokio::fs::write(&path, &data).await.unwrap();

        let (page_map, frame_count, _offset, max_db_size, commit_count) =
            read_frames_as_page_map(&path, page_size, 0).await.unwrap();

        // Should have 3 unique pages from 4 frames
        assert_eq!(frame_count, 4, "Should report all 4 frames were read");
        assert_eq!(page_map.len(), 3, "Should have 3 unique pages");

        // Page 1 should be v2 (0xAA), not v1 (0x11)
        assert_eq!(
            page_map[&1][0], 0xAA,
            "Page 1 should be latest version (v2)"
        );
        assert_eq!(page_map[&2][0], 0x22, "Page 2 should be 0x22");
        assert_eq!(page_map[&3][0], 0x33, "Page 3 should be 0x33");

        assert_eq!(
            max_db_size, 3,
            "final db size should come from the last commit frame"
        );
        // Frames 0 and 3 have db_size > 0 (commit frames)
        assert_eq!(commit_count, 2, "Should count 2 committed transactions");

        tokio::fs::remove_file(&path).await.ok();
    }

    #[tokio::test]
    async fn test_read_frames_as_page_map_matches_old_api() {
        // read_frames_as_page_map must produce the same result as
        // read_frames_as_pages + manual dedup. This is a regression guard.
        let path = PathBuf::from(format!(
            "/tmp/walrust-test-pagemap-compat-{}.db-wal",
            uuid::Uuid::new_v4()
        ));

        let page_size: u32 = 4096;
        let frame_header_size = 24usize;
        let frame_size = frame_header_size + page_size as usize;

        // Create WAL with 3 frames: page 5, page 5 (overwrite), page 10
        let mut data = vec![0u8; 32 + frame_size * 3];
        data[0..4].copy_from_slice(&0x377F0682u32.to_be_bytes());
        data[8..12].copy_from_slice(&page_size.to_be_bytes());

        let f0 = 32;
        data[f0..f0 + 4].copy_from_slice(&5u32.to_be_bytes());
        data[f0 + 4..f0 + 8].copy_from_slice(&10u32.to_be_bytes());
        for b in &mut data[f0 + frame_header_size..f0 + frame_size] {
            *b = 0x55;
        }

        let f1 = 32 + frame_size;
        data[f1..f1 + 4].copy_from_slice(&5u32.to_be_bytes());
        for b in &mut data[f1 + frame_header_size..f1 + frame_size] {
            *b = 0x66;
        } // overwrite

        let f2 = 32 + frame_size * 2;
        data[f2..f2 + 4].copy_from_slice(&10u32.to_be_bytes());
        data[f2 + 4..f2 + 8].copy_from_slice(&10u32.to_be_bytes());
        for b in &mut data[f2 + frame_header_size..f2 + frame_size] {
            *b = 0xAA;
        }

        tokio::fs::write(&path, &data).await.unwrap();

        // Old API: read all frames, then dedup
        let (frames, old_offset, old_max_db) =
            read_frames_as_pages(&path, page_size, 0).await.unwrap();
        let mut old_map = std::collections::HashMap::new();
        for frame in frames {
            old_map.insert(frame.page_number, frame.data);
        }

        // New API: streaming dedup
        let (new_map, _frame_count, new_offset, new_max_db, _commit_count) =
            read_frames_as_page_map(&path, page_size, 0).await.unwrap();

        assert_eq!(old_offset, new_offset, "Offsets must match");
        assert_eq!(old_max_db, new_max_db, "max_db_size must match");
        assert_eq!(old_map.len(), new_map.len(), "Same number of unique pages");

        for (page_num, old_data) in &old_map {
            let new_data = new_map.get(page_num).expect("Page must exist in new map");
            assert_eq!(old_data, new_data, "Page {} data must match", page_num);
        }

        tokio::fs::remove_file(&path).await.ok();
    }

    #[tokio::test]
    async fn test_read_frames_reports_last_commit_db_size_not_max() {
        let path = PathBuf::from(format!(
            "/tmp/walrust-test-pagemap-shrink-{}.db-wal",
            uuid::Uuid::new_v4()
        ));
        let page_size: u32 = 4096;
        let frame_header_size = 24usize;
        let frame_size = frame_header_size + page_size as usize;
        let mut data = vec![0u8; 32 + frame_size * 2];
        data[0..4].copy_from_slice(&0x377F0682u32.to_be_bytes());
        data[8..12].copy_from_slice(&page_size.to_be_bytes());

        let f0 = 32;
        data[f0..f0 + 4].copy_from_slice(&5u32.to_be_bytes());
        data[f0 + 4..f0 + 8].copy_from_slice(&5u32.to_be_bytes());
        for b in &mut data[f0 + frame_header_size..f0 + frame_size] {
            *b = 0x55;
        }

        let f1 = 32 + frame_size;
        data[f1..f1 + 4].copy_from_slice(&3u32.to_be_bytes());
        data[f1 + 4..f1 + 8].copy_from_slice(&3u32.to_be_bytes());
        for b in &mut data[f1 + frame_header_size..f1 + frame_size] {
            *b = 0x33;
        }

        tokio::fs::write(&path, &data).await.unwrap();

        let (_, _, _, final_db_size, commit_count) =
            read_frames_as_page_map(&path, page_size, 0).await.unwrap();
        assert_eq!(final_db_size, 3);
        assert_eq!(commit_count, 2);

        let (_, _, final_db_size_pages) = read_frames_as_pages(&path, page_size, 0).await.unwrap();
        assert_eq!(final_db_size_pages, 3);

        tokio::fs::remove_file(&path).await.ok();
    }

    // ============================================
    // count_wal_commits tests
    // ============================================

    #[tokio::test]
    async fn test_count_wal_commits_nonexistent_file() {
        let path = PathBuf::from("/tmp/walrust-test-nonexistent.db-wal");
        assert_eq!(count_wal_commits(&path, 4096).await.unwrap(), 0);
    }

    #[tokio::test]
    async fn test_count_wal_commits_empty_wal() {
        let path = PathBuf::from(format!(
            "/tmp/walrust-test-commits-empty-{}.db-wal",
            uuid::Uuid::new_v4()
        ));
        let mut header = [0u8; 32];
        header[0..4].copy_from_slice(&0x377F0682u32.to_be_bytes());
        header[8..12].copy_from_slice(&4096u32.to_be_bytes());
        tokio::fs::write(&path, &header).await.unwrap();

        assert_eq!(count_wal_commits(&path, 4096).await.unwrap(), 0);

        tokio::fs::remove_file(&path).await.ok();
    }

    #[tokio::test]
    async fn test_count_wal_commits_counts_correctly() {
        let path = PathBuf::from(format!(
            "/tmp/walrust-test-commits-count-{}.db-wal",
            uuid::Uuid::new_v4()
        ));

        let page_size: u32 = 4096;
        let frame_header_size = 24usize;
        let frame_size = frame_header_size + page_size as usize;

        // 4 frames: 2 commits (db_size > 0), 2 non-commits (db_size = 0)
        let mut data = vec![0u8; 32 + frame_size * 4];
        data[0..4].copy_from_slice(&0x377F0682u32.to_be_bytes());
        data[8..12].copy_from_slice(&page_size.to_be_bytes());

        // Frame 0: page 1, db_size=2 (commit)
        let f0 = 32;
        data[f0..f0 + 4].copy_from_slice(&1u32.to_be_bytes());
        data[f0 + 4..f0 + 8].copy_from_slice(&2u32.to_be_bytes());

        // Frame 1: page 2, db_size=0 (not a commit)
        let f1 = 32 + frame_size;
        data[f1..f1 + 4].copy_from_slice(&2u32.to_be_bytes());

        // Frame 2: page 3, db_size=0 (not a commit)
        let f2 = 32 + frame_size * 2;
        data[f2..f2 + 4].copy_from_slice(&3u32.to_be_bytes());

        // Frame 3: page 1, db_size=3 (commit)
        let f3 = 32 + frame_size * 3;
        data[f3..f3 + 4].copy_from_slice(&1u32.to_be_bytes());
        data[f3 + 4..f3 + 8].copy_from_slice(&3u32.to_be_bytes());

        tokio::fs::write(&path, &data).await.unwrap();

        assert_eq!(count_wal_commits(&path, page_size).await.unwrap(), 2);

        tokio::fs::remove_file(&path).await.ok();
    }

    #[tokio::test]
    async fn test_commit_count_matches_page_map() {
        // count_wal_commits and read_frames_as_page_map must agree on commit count
        let path = PathBuf::from(format!(
            "/tmp/walrust-test-commits-agree-{}.db-wal",
            uuid::Uuid::new_v4()
        ));

        let page_size: u32 = 4096;
        let frame_header_size = 24usize;
        let frame_size = frame_header_size + page_size as usize;

        // 3 frames: commits at frame 0 and 2
        let mut data = vec![0u8; 32 + frame_size * 3];
        data[0..4].copy_from_slice(&0x377F0682u32.to_be_bytes());
        data[8..12].copy_from_slice(&page_size.to_be_bytes());

        let f0 = 32;
        data[f0..f0 + 4].copy_from_slice(&1u32.to_be_bytes());
        data[f0 + 4..f0 + 8].copy_from_slice(&1u32.to_be_bytes()); // commit

        let f1 = 32 + frame_size;
        data[f1..f1 + 4].copy_from_slice(&2u32.to_be_bytes());
        // db_size=0 (no commit)

        let f2 = 32 + frame_size * 2;
        data[f2..f2 + 4].copy_from_slice(&3u32.to_be_bytes());
        data[f2 + 4..f2 + 8].copy_from_slice(&3u32.to_be_bytes()); // commit

        tokio::fs::write(&path, &data).await.unwrap();

        let standalone = count_wal_commits(&path, page_size).await.unwrap();
        let (_, _, _, _, from_page_map) =
            read_frames_as_page_map(&path, page_size, 0).await.unwrap();

        assert_eq!(standalone, from_page_map);
        assert_eq!(standalone, 2);

        tokio::fs::remove_file(&path).await.ok();
    }
}
