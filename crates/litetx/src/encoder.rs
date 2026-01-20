use crate::{
    ltx::{HeaderEncodeError, PageHeader, PageHeaderEncodeError, TrailerEncodeError, CRC64},
    Checksum, Header, HeaderFlags, PageNum, PageSize, Trailer,
};
use lz4_flex::frame::{BlockSize, FrameEncoder, FrameInfo};
use std::io::{self, Write};

/// An error that can be returned by [`Encoder`].
#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("header")]
    Header(#[from] HeaderEncodeError),
    #[error("page header")]
    PageHeader(#[from] PageHeaderEncodeError),
    #[error("trailer")]
    Trailer(#[from] TrailerEncodeError),
    #[error("cannot encode lock page: {0}")]
    LockPage(PageNum),
    #[error("snapshot transaction file must start with page number 1")]
    FirstSnapshotPage,
    #[error("nonsequential page numbers in snapshot transaction: {0}, {1}")]
    NonsequentialPages(PageNum, PageNum),
    #[error("out-of-order page numbers: {0}, {1}")]
    OutOfOrderPage(PageNum, PageNum),
    #[error("invalid page buffer size: {0}, expected {1}")]
    InvalidBufferSize(usize, PageSize),
    #[error("write")]
    Write(#[from] io::Error),
}

impl From<Error> for io::Error {
    fn from(e: Error) -> Self {
        match e {
            Error::Write(ioe) => ioe,
            _ => io::Error::new(io::ErrorKind::Other, e),
        }
    }
}

/// An LTX file encoder.
///
/// # Example
/// ```
/// # use std::time::SystemTime;
/// # use litetx::PageChecksum;
/// # let mut w = Vec::new();
/// # let page = vec![0; 4096];
/// #
/// let mut enc = litetx::Encoder::new(&mut w, &litetx::Header{
///     flags: litetx::HeaderFlags::empty(),
///     page_size: litetx::PageSize::new(4096).unwrap(),
///     commit: litetx::PageNum::new(1).unwrap(),
///     min_txid: litetx::TXID::ONE,
///     max_txid: litetx::TXID::ONE,
///     timestamp: SystemTime::now(),
///     pre_apply_checksum: None,
/// }).expect("encoder");
///
/// let page_num = litetx::PageNum::new(1).unwrap();
/// enc.encode_page(page_num, &page).expect("encode_page");
///
/// enc.finish(page.page_checksum(page_num)).expect("finish");
/// ```
pub struct Encoder<'a, W>
where
    W: io::Write,
{
    w: LTXWriter<W>,
    digest: crc::Digest<'a, u64>,
    page_size: PageSize,
    is_snapshot: bool,
    last_page_num: Option<PageNum>,
}

impl<'a, W> Encoder<'a, W>
where
    W: io::Write,
{
    /// Create a new [`Encoder`] that writes to `w`.
    ///
    /// Depending on the `hdr` flags, the [`Encoder`] will produce either compressed or
    /// uncompressed LTX file.
    pub fn new(mut w: W, hdr: &Header) -> Result<Encoder<'a, W>, Error> {
        let mut digest = CRC64.digest();
        {
            let writer = CrcDigestWrite::new(&mut w, &mut digest);
            hdr.encode_into(writer)?;
        }

        Ok(Encoder {
            w: LTXWriter::new(w, hdr.flags.contains(HeaderFlags::COMPRESS_LZ4)),
            digest,
            page_size: hdr.page_size,
            is_snapshot: hdr.is_snapshot(),
            last_page_num: None,
        })
    }

    fn validate_page_num(&self, page_num: PageNum) -> Result<(), Error> {
        let lock = PageNum::lock_page(self.page_size);

        if page_num == lock {
            return Err(Error::LockPage(page_num));
        }
        if self.is_snapshot {
            if self.last_page_num.is_none() && page_num != PageNum::ONE {
                return Err(Error::FirstSnapshotPage);
            } else if let Some(last) = self.last_page_num {
                if last + 1 != page_num || last + 1 == lock && last + 2 != page_num {
                    return Err(Error::NonsequentialPages(last, page_num));
                }
            }
        } else if let Some(last) = self.last_page_num {
            if last >= page_num {
                return Err(Error::OutOfOrderPage(last, page_num));
            }
        }

        Ok(())
    }

    /// Encode a page with the given `page_num` and `data`.
    ///
    /// Depending on the [`Header`] passed to [`Encoder::new`], the following constraints
    /// are applied:
    ///  - if `min_txid` is 1, the LTX file is considered to be a full snapshot of the database
    ///    and must contain all pages from the first one up to `commit` in increasing oreder.
    ///  - if `min_txid` is greater than 1, the LTX file may contain a subset of database
    ///    pages in increasing order.
    pub fn encode_page(&mut self, page_num: PageNum, data: &[u8]) -> Result<(), Error> {
        self.validate_page_num(page_num)?;
        if data.len() != self.page_size.into_inner() as usize {
            return Err(Error::InvalidBufferSize(data.len(), self.page_size));
        }

        {
            let mut writer = CrcDigestWrite::new(&mut self.w, &mut self.digest);
            PageHeader(Some(page_num)).encode_into(&mut writer)?;
            writer.write_all(data)?;
        }

        self.last_page_num = Some(page_num);

        Ok(())
    }

    /// Consume the encoder and write LTX trailer into the output.
    pub fn finish(mut self, post_apply_checksum: Checksum) -> Result<Trailer, Error> {
        let mut writer = CrcDigestWrite::new(&mut self.w, &mut self.digest);
        PageHeader(None).encode_into(&mut writer)?;

        let writer = self.w.finish()?;
        self.digest
            .update(&post_apply_checksum.into_inner().to_be_bytes());

        let trailer = Trailer {
            post_apply_checksum,
            file_checksum: Checksum::new(self.digest.finalize()),
        };

        trailer.encode_into(writer)?;

        Ok(trailer)
    }
}

struct LTXWriter<W>
where
    W: io::Write,
{
    enc: FrameEncoder<W>,
    compressed: bool,
}

impl<W> LTXWriter<W>
where
    W: io::Write,
{
    fn new(w: W, compressed: bool) -> LTXWriter<W> {
        LTXWriter {
            enc: FrameEncoder::with_frame_info(FrameInfo::new().block_size(BlockSize::Max64KB), w),
            compressed,
        }
    }

    fn finish(self) -> io::Result<W> {
        if self.compressed {
            self.enc
                .finish()
                .map_err(|e| io::Error::new(io::ErrorKind::Other, e))
        } else {
            Ok(self.enc.into_inner())
        }
    }
}

impl<W> io::Write for LTXWriter<W>
where
    W: io::Write,
{
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        if self.compressed {
            self.enc.write(buf)
        } else {
            self.enc.get_mut().write(buf)
        }
    }

    fn flush(&mut self) -> io::Result<()> {
        if self.compressed {
            self.enc.flush()?;
        }
        self.enc.get_mut().flush()
    }
}

/// An [`io::Write`] computing a digest on the bytes written.
struct CrcDigestWrite<'a, 'b, W>
where
    W: io::Write,
{
    inner: W,
    digest: &'a mut crc::Digest<'b, u64>,
}

impl<'a, 'b, W> CrcDigestWrite<'a, 'b, W>
where
    W: io::Write,
{
    fn new(inner: W, digest: &'a mut crc::Digest<'b, u64>) -> Self {
        CrcDigestWrite { inner, digest }
    }
}

impl<'a, 'b, W> io::Write for CrcDigestWrite<'a, 'b, W>
where
    W: io::Write,
{
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        let written = self.inner.write(buf)?;
        self.digest.update(&buf[..written]);
        Ok(written)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.inner.flush()
    }
}

#[cfg(test)]
mod tests {
    use super::{CrcDigestWrite, Encoder, Error};
    use crate::{
        ltx::{self, CRC64},
        Checksum, Header, HeaderFlags, PageNum, PageSize, TXID,
    };
    use std::{io::Write, time};

    #[test]
    fn crc_digest_write() {
        let mut buf = Vec::new();
        let mut digest = CRC64.digest();
        let mut writer = CrcDigestWrite::new(&mut buf, &mut digest);

        assert!(matches!(
            writer.write(&[1, 2, 3, 4, 5, 6, 7, 8, 9, 10]),
            Ok(10)
        ));
        assert_eq!(vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10], buf);
        assert_eq!(6672316476627126589, digest.finalize());
    }

    #[test]
    fn encoder() {
        let mut buf = Vec::new();

        let mut enc = Encoder::new(
            &mut buf,
            &Header {
                flags: HeaderFlags::empty(),
                page_size: PageSize::new(4096).unwrap(),
                commit: PageNum::new(3).unwrap(),
                min_txid: TXID::new(5).unwrap(),
                max_txid: TXID::new(6).unwrap(),
                timestamp: time::SystemTime::now(),
                pre_apply_checksum: Some(Checksum::new(5)),
            },
        )
        .expect("failed to create encoder");

        let page1: Vec<u8> = (0..4096).map(|_| rand::random::<u8>()).collect();
        let page2: Vec<u8> = (0..4096).map(|_| rand::random::<u8>()).collect();

        enc.encode_page(PageNum::new(1).unwrap(), page1.as_slice())
            .expect("failed to encode page1");
        enc.encode_page(PageNum::new(2).unwrap(), page2.as_slice())
            .expect("failed to encode page2");

        let trailer = enc
            .finish(Checksum::new(6))
            .expect("failed to finish encoder");

        assert_eq!(Checksum::new(6), trailer.post_apply_checksum);
        assert_eq!(
            ltx::HEADER_SIZE + (4096 + 4) * 2 + 4 + ltx::TRAILER_SIZE,
            buf.len()
        );
    }

    #[test]
    fn encoder_compressed() {
        let mut buf = Vec::new();

        let mut enc = Encoder::new(
            &mut buf,
            &Header {
                flags: HeaderFlags::COMPRESS_LZ4,
                page_size: PageSize::new(4096).unwrap(),
                commit: PageNum::new(3).unwrap(),
                min_txid: TXID::new(5).unwrap(),
                max_txid: TXID::new(6).unwrap(),
                timestamp: time::SystemTime::now(),
                pre_apply_checksum: Some(Checksum::new(5)),
            },
        )
        .expect("failed to create encoder");

        let page1: Vec<u8> = (0..4096).map(|_| 1).collect();
        let page2: Vec<u8> = (0..4096).map(|_| 2).collect();

        enc.encode_page(PageNum::new(1).unwrap(), page1.as_slice())
            .expect("failed to encode page1");
        enc.encode_page(PageNum::new(2).unwrap(), page2.as_slice())
            .expect("failed to encode page2");

        let trailer = enc
            .finish(Checksum::new(6))
            .expect("failed to finish encoder");
        assert_eq!(Checksum::new(6), trailer.post_apply_checksum);
        assert!(ltx::HEADER_SIZE + (4096 + 4) * 2 + 4 + ltx::TRAILER_SIZE > buf.len());
    }

    #[test]
    fn encoder_lock_page() {
        let mut buf = Vec::new();

        let mut enc = Encoder::new(
            &mut buf,
            &Header {
                flags: HeaderFlags::empty(),
                page_size: PageSize::new(4096).unwrap(),
                commit: PageNum::new(3).unwrap(),
                min_txid: TXID::new(1).unwrap(),
                max_txid: TXID::new(1).unwrap(),
                timestamp: time::SystemTime::now(),
                pre_apply_checksum: None,
            },
        )
        .expect("failed to create encoder");

        let (page1_num, page1) = (
            PageNum::lock_page(PageSize::new(4096).unwrap()),
            vec![0; 4096],
        );

        assert!(matches!(
            enc.encode_page(page1_num, page1.as_slice()),
            Err(Error::LockPage(p)) if p == page1_num
        ));
    }

    #[test]
    fn encoder_non_sequential() {
        let mut buf = Vec::new();

        let mut enc = Encoder::new(
            &mut buf,
            &Header {
                flags: HeaderFlags::empty(),
                page_size: PageSize::new(4096).unwrap(),
                commit: PageNum::new(3).unwrap(),
                min_txid: TXID::new(1).unwrap(),
                max_txid: TXID::new(1).unwrap(),
                timestamp: time::SystemTime::now(),
                pre_apply_checksum: None,
            },
        )
        .expect("failed to create encoder");

        let (page1_num, page1) = (PageNum::new(1).unwrap(), vec![0; 4096]);
        let (page3_num, page3) = (PageNum::new(3).unwrap(), vec![0; 4096]);

        enc.encode_page(page1_num, page1.as_slice())
            .expect("failed to encode page1");
        assert!(matches!(
            enc.encode_page(page3_num, page3.as_slice()),
            Err(Error::NonsequentialPages(a, b)) if a == page1_num && b == page3_num
        ));
    }

    #[test]
    fn encoder_out_of_order() {
        let mut buf = Vec::new();

        let mut enc = Encoder::new(
            &mut buf,
            &Header {
                flags: HeaderFlags::empty(),
                page_size: PageSize::new(4096).unwrap(),
                commit: PageNum::new(3).unwrap(),
                min_txid: TXID::new(2).unwrap(),
                max_txid: TXID::new(5).unwrap(),
                timestamp: time::SystemTime::now(),
                pre_apply_checksum: Some(Checksum::new(1)),
            },
        )
        .expect("failed to create encoder");

        let (page1_num, page1) = (PageNum::new(1).unwrap(), vec![0; 4096]);
        let (page3_num, page3) = (PageNum::new(3).unwrap(), vec![0; 4096]);

        enc.encode_page(page3_num, page3.as_slice())
            .expect("failed to encode page3");
        assert!(matches!(
            enc.encode_page(page1_num, page1.as_slice()),
            Err(Error::OutOfOrderPage(a, b)) if a == page3_num && b == page1_num
        ));
    }

    #[test]
    fn encoder_snapshot() {
        let mut buf = Vec::new();

        let mut enc = Encoder::new(
            &mut buf,
            &Header {
                flags: HeaderFlags::empty(),
                page_size: PageSize::new(4096).unwrap(),
                commit: PageNum::new(3).unwrap(),
                min_txid: TXID::new(1).unwrap(),
                max_txid: TXID::new(1).unwrap(),
                timestamp: time::SystemTime::now(),
                pre_apply_checksum: None,
            },
        )
        .expect("failed to create encoder");

        let (page1_num, page1) = (PageNum::new(2).unwrap(), vec![0; 4096]);

        assert!(matches!(
            enc.encode_page(page1_num, page1.as_slice()),
            Err(Error::FirstSnapshotPage)
        ));
    }
}
