use crate::{
    ltx::{HeaderDecodeError, PageHeader, PageHeaderDecodeError, TrailerDecodeError, CRC64},
    Checksum, Header, HeaderFlags, PageNum, PageSize, Trailer,
};
use lz4_flex::frame::FrameDecoder;
use std::io::{self, Read};

/// An error that can be returned by [`Decoder`].
#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("header")]
    Header(#[from] HeaderDecodeError),
    #[error("page header")]
    PageHeader(#[from] PageHeaderDecodeError),
    #[error("trailer")]
    Trailer(#[from] TrailerDecodeError),
    #[error("invalid page buffer size: {0}, expected {1}")]
    InvalidBufferSize(usize, PageSize),
    #[error("file checksum mismatch")]
    FileChecksumMismatch,
    #[error("read")]
    Read(#[from] io::Error),
}

/// An LTX file decoder.
///
/// # Example
/// ```no_run
/// # use std::time::SystemTime;
/// # let v = Vec::new();
/// # let mut r = &mut &v[..];
/// #
/// let (mut dec, header) = litetx::Decoder::new(r).expect("decoder");
///
/// let mut buf = vec![0; header.page_size.into_inner() as usize];
/// while let Some(page_num) = dec.decode_page(&mut buf).expect("decode_page") {
///     // do something with the page
/// }
///
/// let trailer = dec.finish().expect("finish");
/// ```
pub struct Decoder<'a, R>
where
    R: io::Read,
{
    r: LTXReader<R>,
    digest: crc::Digest<'a, u64>,
    page_size: PageSize,
    pages_done: bool,
}

impl<'a, R> Decoder<'a, R>
where
    R: io::Read,
{
    /// Construct a new [`Decoder`] that reads from `r`.
    pub fn new(mut r: R) -> Result<(Decoder<'a, R>, Header), Error> {
        let mut digest = CRC64.digest();
        let hdr = {
            let reader = CrcDigestRead::new(&mut r, &mut digest);
            Header::decode_from(reader)?
        };

        Ok((
            Decoder {
                r: LTXReader::new(r, hdr.flags.contains(HeaderFlags::COMPRESS_LZ4)),
                digest,
                page_size: hdr.page_size,
                pages_done: false,
            },
            hdr,
        ))
    }

    /// Decode the next page from the LTX file.
    ///
    /// Returns `Ok(Some(page_num))` if a page has been successfully decoded.
    /// Return `Ok(None)` if the LTX file doesn't have any more pages.
    pub fn decode_page(&mut self, data: &mut [u8]) -> Result<Option<PageNum>, Error> {
        if self.pages_done {
            return Ok(None);
        };

        if data.len() != self.page_size.into_inner() as usize {
            return Err(Error::InvalidBufferSize(data.len(), self.page_size));
        }

        let mut reader = CrcDigestRead::new(&mut self.r, &mut self.digest);
        let header = PageHeader::decode_from(&mut reader)?;
        if header.0.is_none() {
            self.pages_done = true;
            return Ok(None);
        };

        reader.read_exact(data)?;

        Ok(header.0)
    }

    /// Consume the decoder and verify file checksum.
    pub fn finish(mut self) -> Result<Trailer, Error> {
        let reader = self.r.finish()?;
        let trailer = Trailer::decode_from(reader)?;

        self.digest
            .update(&trailer.post_apply_checksum.into_inner().to_be_bytes());

        if Checksum::new(self.digest.finalize()) != trailer.file_checksum {
            return Err(Error::FileChecksumMismatch);
        }

        Ok(trailer)
    }
}

struct LTXReader<R>
where
    R: io::Read,
{
    dec: FrameDecoder<R>,
    compressed: bool,
}

impl<R> LTXReader<R>
where
    R: io::Read,
{
    fn new(r: R, compressed: bool) -> LTXReader<R> {
        LTXReader {
            dec: FrameDecoder::new(r),
            compressed,
        }
    }

    fn finish(mut self) -> io::Result<R> {
        // Read lz4 trailer frame.
        if self.compressed {
            let mut buf = [0; 1];
            match self.dec.read_exact(&mut buf) {
                Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => (),
                Err(e) => return Err(e),
                _ => {
                    return Err(io::Error::new(
                        io::ErrorKind::Other,
                        "expected lz4 end frame",
                    ))
                }
            }
        }

        Ok(self.dec.into_inner())
    }
}

impl<R> io::Read for LTXReader<R>
where
    R: io::Read,
{
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        if self.compressed {
            self.dec.read(buf)
        } else {
            self.dec.get_mut().read(buf)
        }
    }
}

/// An [`io::Read`] computing a digest on the bytes read.
struct CrcDigestRead<'a, 'b, R>
where
    R: io::Read,
{
    inner: R,
    digest: &'a mut crc::Digest<'b, u64>,
}

impl<'a, 'b, R> CrcDigestRead<'a, 'b, R>
where
    R: io::Read,
{
    fn new(inner: R, digest: &'a mut crc::Digest<'b, u64>) -> Self {
        CrcDigestRead { inner, digest }
    }
}

impl<'a, 'b, R> io::Read for CrcDigestRead<'a, 'b, R>
where
    R: io::Read,
{
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        let read = self.inner.read(buf)?;
        self.digest.update(&buf[..read]);
        Ok(read)
    }
}

#[cfg(test)]
mod tests {
    use super::{CrcDigestRead, Decoder};
    use crate::{
        ltx::CRC64, utils::TimeRound, Checksum, Encoder, Header, HeaderFlags, PageNum, PageSize,
        TXID,
    };
    use std::{io::Read, time};

    #[test]
    fn crc_digest_read() {
        let buf_in = vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10];
        let mut digest = CRC64.digest();
        let mut reader = CrcDigestRead::new(buf_in.as_slice(), &mut digest);

        let mut buf_out = vec![0; 10];
        assert!(matches!(reader.read(&mut buf_out), Ok(10)));
        assert_eq!(buf_in, buf_out);
        assert_eq!(6672316476627126589, digest.finalize());
    }

    fn decoder_test(flags: HeaderFlags) {
        let mut buf = Vec::new();

        let header = Header {
            flags,
            page_size: PageSize::new(4096).unwrap(),
            commit: PageNum::new(3).unwrap(),
            min_txid: TXID::new(5).unwrap(),
            max_txid: TXID::new(6).unwrap(),
            timestamp: time::SystemTime::now()
                .round(time::Duration::from_millis(1))
                .unwrap(),
            pre_apply_checksum: Some(Checksum::new(5)),
        };

        let mut enc = Encoder::new(&mut buf, &header).expect("failed to create encoder");
        let mut pages: Vec<(PageNum, Vec<_>)> = Vec::new();
        pages.push((
            PageNum::new(4).unwrap(),
            (0..4096).map(|_| rand::random::<u8>()).collect::<Vec<_>>(),
        ));
        pages.push((
            PageNum::new(6).unwrap(),
            (0..4096).map(|_| rand::random::<u8>()).collect::<Vec<_>>(),
        ));

        for (page_num, page) in &pages {
            enc.encode_page(*page_num, page.as_slice())
                .expect("failed to encode page");
        }

        let trailer = enc
            .finish(Checksum::new(6))
            .expect("failed to finish encoder");

        let (mut dec, header_out) = Decoder::new(buf.as_slice()).expect("failed to create decoder");
        assert_eq!(header, header_out);

        let mut page_out = vec![0; 4096];
        for (page_num, page) in pages {
            assert!(matches!(
                dec.decode_page(page_out.as_mut_slice()),
                Ok(Some(num)) if num == page_num
            ));
            assert_eq!(page, page_out);
        }

        assert!(matches!(dec.decode_page(page_out.as_mut_slice()), Ok(None)));

        let trailer_out = dec.finish().expect("failed to finish decoder");
        assert_eq!(trailer, trailer_out);
    }

    #[test]
    fn decoder() {
        decoder_test(HeaderFlags::empty());
    }

    #[test]
    fn decoder_compressed() {
        decoder_test(HeaderFlags::COMPRESS_LZ4);
    }
}
