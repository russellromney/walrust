use crate::types::{Checksum, PageNum, PageNumError, PageSize, PageSizeError, TXIDError, TXID};
use std::{io, time};

pub(crate) const CRC64: crc::Crc<u64> = crc::Crc::<u64>::new(&crc::CRC_64_GO_ISO);

bitflags::bitflags! {
    #[derive(Clone, Copy, Debug, PartialEq, Eq)]
    pub struct HeaderFlags: u32 {
        /// Data is compressed with LZ4
        const COMPRESS_LZ4 = 0b00000001;
        /// No checksums are tracked (litestream compatibility)
        /// When set, pre_apply_checksum and post_apply_checksum should be zero
        const NO_CHECKSUM = 0b00000010;
    }
}

/// A header validation error.
#[derive(thiserror::Error, Debug)]
pub enum HeaderValidateError {
    #[error("transaction ids out of order: ({0}, {1})")]
    TXIDOrder(TXID, TXID),
    #[error("pre-apply checksum must be unset on snapshots")]
    PreApplyChecksumOnSnapshot,
    #[error("pre-apply checksum required on non-snapshot files")]
    NoPreApplyChecksum,
}

/// A header encoding error.
#[derive(thiserror::Error, Debug)]
pub enum HeaderEncodeError {
    #[error("validation failed")]
    Validation(#[from] HeaderValidateError),
    #[error("invalid timestamp: {0}")]
    Timestamp(time::SystemTimeError),
    #[error("write error")]
    Write(#[from] io::Error),
}

/// A header decoding error.
#[derive(thiserror::Error, Debug)]
pub enum HeaderDecodeError {
    #[error("read error")]
    Read(#[from] io::Error),
    #[error("invalid magic record: {0:?}")]
    Magic([u8; 4]),
    #[error("invalid flags record: {0:x}")]
    Flags(u32),
    #[error("invalid page size record")]
    PageSize(#[from] PageSizeError),
    #[error("invalid commit record: {0}")]
    Commit(PageNumError),
    #[error("invalid min TX ID record: {0}")]
    MinTXID(TXIDError),
    #[error("invalid max TX ID record: {0}")]
    MaxTXID(TXIDError),
    #[error("invalid timestamp: {0}")]
    Timestamp(u64),
    #[error("validation failed")]
    Validation(#[from] HeaderValidateError),
}

pub(crate) const HEADER_SIZE: usize = 100;
pub(crate) const TRAILER_SIZE: usize = 16;
pub(crate) const PAGE_HEADER_SIZE: usize = 4;

/// An LTX file header.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Header {
    /// Flags changing the behavior of LTX encoder/decoder.
    pub flags: HeaderFlags,
    /// The size of the database pages encoded in the file.
    pub page_size: PageSize,
    /// The size of the database in pages.
    pub commit: PageNum,
    /// Minimum transaction ID in the file.
    pub min_txid: TXID,
    /// Maximum transaction ID in the file. May be equal to `min_txid` if the file
    /// contains only one transaction.
    pub max_txid: TXID,
    /// The time when the LTX file was created.
    pub timestamp: time::SystemTime,
    /// Running database checksum before this LTX file is applied. `None` if the LTX
    /// file contains the full snapshot of a database.
    pub pre_apply_checksum: Option<Checksum>,
}

impl Header {
    const MAGIC: &'static str = "LTX1";

    pub(crate) fn is_snapshot(&self) -> bool {
        self.min_txid == TXID::ONE
    }

    fn validate(&self) -> Result<(), HeaderValidateError> {
        if self.min_txid > self.max_txid {
            return Err(HeaderValidateError::TXIDOrder(self.min_txid, self.max_txid));
        };

        // Skip checksum validation when NO_CHECKSUM flag is set (litestream compat)
        if self.flags.contains(HeaderFlags::NO_CHECKSUM) {
            return Ok(());
        }

        if self.is_snapshot() && self.pre_apply_checksum.is_some() {
            return Err(HeaderValidateError::PreApplyChecksumOnSnapshot);
        }

        if !self.is_snapshot() && self.pre_apply_checksum.is_none() {
            return Err(HeaderValidateError::NoPreApplyChecksum);
        }

        Ok(())
    }

    pub(crate) fn encode_into<W>(&self, mut w: W) -> Result<(), HeaderEncodeError>
    where
        W: io::Write,
    {
        let mut buf = Vec::with_capacity(HEADER_SIZE);
        let timestamp = self
            .timestamp
            .duration_since(time::SystemTime::UNIX_EPOCH)
            .map_err(HeaderEncodeError::Timestamp)?
            .as_millis() as u64;
        let checksum = if let Some(c) = self.pre_apply_checksum {
            c.into_inner()
        } else {
            0
        };

        self.validate()?;

        buf.extend_from_slice(Self::MAGIC.as_bytes());
        buf.extend_from_slice(&self.flags.bits().to_be_bytes());
        buf.extend_from_slice(&self.page_size.into_inner().to_be_bytes());
        buf.extend_from_slice(&self.commit.into_inner().to_be_bytes());
        buf.extend_from_slice(&self.min_txid.into_inner().to_be_bytes());
        buf.extend_from_slice(&self.max_txid.into_inner().to_be_bytes());
        buf.extend_from_slice(&timestamp.to_be_bytes());
        buf.extend_from_slice(&checksum.to_be_bytes());
        buf.resize(HEADER_SIZE, 0);

        w.write_all(&buf)?;

        Ok(())
    }

    pub(crate) fn decode_from<R>(mut r: R) -> Result<Header, HeaderDecodeError>
    where
        R: io::Read,
    {
        let mut buf = vec![0; HEADER_SIZE];
        r.read_exact(&mut buf)?;

        if &buf[0..4] != Self::MAGIC.as_bytes() {
            return Err(HeaderDecodeError::Magic(buf[0..4].try_into().unwrap()));
        }

        let flags = u32::from_be_bytes(buf[4..8].try_into().unwrap());
        let flags = HeaderFlags::from_bits(flags).ok_or(HeaderDecodeError::Flags(flags))?;

        let page_size = u32::from_be_bytes(buf[8..12].try_into().unwrap());
        let page_size = PageSize::new(page_size)?;

        let commit = u32::from_be_bytes(buf[12..16].try_into().unwrap());
        let commit = PageNum::new(commit).map_err(HeaderDecodeError::Commit)?;

        let min_txid = u64::from_be_bytes(buf[16..24].try_into().unwrap());
        let min_txid = TXID::new(min_txid).map_err(HeaderDecodeError::MinTXID)?;

        let max_txid = u64::from_be_bytes(buf[24..32].try_into().unwrap());
        let max_txid = TXID::new(max_txid).map_err(HeaderDecodeError::MaxTXID)?;

        let timestamp = u64::from_be_bytes(buf[32..40].try_into().unwrap());
        let timestamp = time::SystemTime::UNIX_EPOCH
            .checked_add(time::Duration::from_millis(timestamp))
            .ok_or(HeaderDecodeError::Timestamp(timestamp))?;

        let pre_apply_checksum = u64::from_be_bytes(buf[40..48].try_into().unwrap());
        let pre_apply_checksum = if pre_apply_checksum != 0 {
            Some(Checksum::new(pre_apply_checksum))
        } else {
            None
        };

        let hdr = Header {
            flags,
            page_size,
            commit,
            min_txid,
            max_txid,
            timestamp,
            pre_apply_checksum,
        };

        hdr.validate()?;

        Ok(hdr)
    }
}

/// A trailer encoding error.
#[derive(thiserror::Error, Debug)]
pub enum TrailerEncodeError {
    #[error("write error")]
    Write(#[from] io::Error),
}

/// A trailer decoding error.
#[derive(thiserror::Error, Debug)]
pub enum TrailerDecodeError {
    #[error("read error")]
    Read(#[from] io::Error),
    #[error("invalid post apply checksum: {0}")]
    PostApplyChecksum(u64),
    #[error("invalid file checksum: {0}")]
    FileChecksum(u64),
}

/// An LTX file trailer.
#[derive(Debug, PartialEq, Eq)]
pub struct Trailer {
    /// Running database checksum after this LTX file has been applied.
    pub post_apply_checksum: Checksum,
    /// LTX file checksum.
    pub file_checksum: Checksum,
}

impl Trailer {
    pub(crate) fn encode_into<W>(&self, mut w: W) -> Result<(), TrailerEncodeError>
    where
        W: io::Write,
    {
        let mut buf = Vec::with_capacity(TRAILER_SIZE);

        buf.extend_from_slice(&self.post_apply_checksum.into_inner().to_be_bytes());
        buf.extend_from_slice(&self.file_checksum.into_inner().to_be_bytes());

        w.write_all(&buf)?;

        Ok(())
    }

    pub(crate) fn decode_from<R>(mut r: R) -> Result<Trailer, TrailerDecodeError>
    where
        R: io::Read,
    {
        let mut buf = [0; TRAILER_SIZE];
        r.read_exact(&mut buf)?;

        let post_apply_checksum = u64::from_be_bytes(buf[0..8].try_into().unwrap());
        let file_checksum = u64::from_be_bytes(buf[8..16].try_into().unwrap());

        let trailer = Trailer {
            post_apply_checksum: Checksum::new(post_apply_checksum),
            file_checksum: Checksum::new(file_checksum),
        };
        if trailer.post_apply_checksum.into_inner() != post_apply_checksum {
            return Err(TrailerDecodeError::PostApplyChecksum(post_apply_checksum));
        }
        if trailer.file_checksum.into_inner() != file_checksum {
            return Err(TrailerDecodeError::FileChecksum(file_checksum));
        }

        Ok(trailer)
    }
}

/// A page header encoding error.
#[derive(thiserror::Error, Debug)]
pub enum PageHeaderEncodeError {
    #[error("write error")]
    Write(#[from] io::Error),
}

/// A page header decoding error.
#[derive(thiserror::Error, Debug)]
pub enum PageHeaderDecodeError {
    #[error("read error")]
    Read(#[from] io::Error),
    #[error("invalid page number record: {0}")]
    PageNum(PageNumError),
}

#[derive(Debug, PartialEq, Eq)]
pub(crate) struct PageHeader(pub(crate) Option<PageNum>);

impl PageHeader {
    pub(crate) fn encode_into<W>(&self, mut w: W) -> Result<(), PageHeaderEncodeError>
    where
        W: io::Write,
    {
        let page_num = self.0.map(|n| n.into_inner()).unwrap_or(0);
        w.write_all(&page_num.to_be_bytes())?;

        Ok(())
    }

    pub(crate) fn decode_from<R>(mut r: R) -> Result<PageHeader, PageHeaderDecodeError>
    where
        R: io::Read,
    {
        let mut buf = [0; PAGE_HEADER_SIZE];
        r.read_exact(&mut buf)?;

        let page_num = u32::from_be_bytes(buf[0..4].try_into().unwrap());
        let page_num = if page_num != 0 {
            Some(PageNum::new(page_num).map_err(PageHeaderDecodeError::PageNum)?)
        } else {
            None
        };

        Ok(PageHeader(page_num))
    }
}

/// A trait for page checksum calculation.
pub trait PageChecksum {
    /// Calculate database page checksum for the given page number.
    fn page_checksum(&self, pgno: PageNum) -> Checksum;
}

impl<T> PageChecksum for T
where
    T: AsRef<[u8]>,
{
    fn page_checksum(&self, pgno: PageNum) -> Checksum {
        let mut digest = CRC64.digest();

        digest.update(&pgno.into_inner().to_be_bytes());
        digest.update(self.as_ref());

        Checksum::new(digest.finalize())
    }
}

#[cfg(test)]
mod tests {
    use super::{Header, HeaderFlags, HeaderValidateError, PageHeader, Trailer};
    use crate::{utils::TimeRound, Checksum, PageNum, PageSize, TXID};
    use std::time;

    fn encode_decode_header(mut hdr: Header) {
        let mut buf = Vec::new();

        // round timestamp to milliseconds to be able to compare it later.
        hdr.timestamp = hdr.timestamp.round(time::Duration::from_millis(1)).unwrap();

        hdr.encode_into(&mut buf).expect("failed to encode header");
        let hdr_out = Header::decode_from(buf.as_slice()).expect("failed to decode header");

        assert_eq!(hdr_out, hdr);
    }

    #[test]
    fn snapshot_header() {
        encode_decode_header(Header {
            flags: HeaderFlags::COMPRESS_LZ4,
            page_size: PageSize::new(4096).unwrap(),
            commit: PageNum::new(10).unwrap(),
            min_txid: TXID::new(1).unwrap(),
            max_txid: TXID::new(5).unwrap(),
            timestamp: time::SystemTime::now(),
            pre_apply_checksum: None,
        });
    }

    #[test]
    fn non_snapshot_header() {
        encode_decode_header(Header {
            flags: HeaderFlags::COMPRESS_LZ4,
            page_size: PageSize::new(4096).unwrap(),
            commit: PageNum::new(10).unwrap(),
            min_txid: TXID::new(3).unwrap(),
            max_txid: TXID::new(5).unwrap(),
            timestamp: time::SystemTime::now(),
            pre_apply_checksum: Some(Checksum::new(123)),
        });
    }

    #[test]
    fn validate_header() {
        let hdr = Header {
            flags: HeaderFlags::COMPRESS_LZ4,
            page_size: PageSize::new(4096).unwrap(),
            commit: PageNum::new(10).unwrap(),
            min_txid: TXID::new(5).unwrap(),
            max_txid: TXID::new(3).unwrap(),
            timestamp: time::SystemTime::now(),
            pre_apply_checksum: Some(Checksum::new(123)),
        };
        assert!(matches!(
            hdr.validate(),
            Err(HeaderValidateError::TXIDOrder(min, max)) if min == hdr.min_txid && max == hdr.max_txid));

        let hdr = Header {
            flags: HeaderFlags::COMPRESS_LZ4,
            page_size: PageSize::new(4096).unwrap(),
            commit: PageNum::new(10).unwrap(),
            min_txid: TXID::new(1).unwrap(),
            max_txid: TXID::new(3).unwrap(),
            timestamp: time::SystemTime::now(),
            pre_apply_checksum: Some(Checksum::new(123)),
        };
        assert!(matches!(
            hdr.validate(),
            Err(HeaderValidateError::PreApplyChecksumOnSnapshot)
        ));

        let hdr = Header {
            flags: HeaderFlags::COMPRESS_LZ4,
            page_size: PageSize::new(4096).unwrap(),
            commit: PageNum::new(10).unwrap(),
            min_txid: TXID::new(3).unwrap(),
            max_txid: TXID::new(5).unwrap(),
            timestamp: time::SystemTime::now(),
            pre_apply_checksum: None,
        };
        assert!(matches!(
            hdr.validate(),
            Err(HeaderValidateError::NoPreApplyChecksum)
        ));
    }

    #[test]
    fn trailer() {
        let mut buf = Vec::new();

        let trailer = Trailer {
            post_apply_checksum: Checksum::new(123),
            file_checksum: Checksum::new(123),
        };
        trailer
            .encode_into(&mut buf)
            .expect("failed to encode trailer");
        let trailer_out = Trailer::decode_from(buf.as_slice()).expect("failed to decode trailer");

        assert_eq!(trailer_out, trailer);
    }

    #[test]
    fn page_header() {
        let mut buf = Vec::new();

        let page_header = PageHeader(Some(PageNum::new(10).unwrap()));
        page_header
            .encode_into(&mut buf)
            .expect("failed to encode page header");
        let page_header_out =
            PageHeader::decode_from(buf.as_slice()).expect("failed to decode page header");

        assert_eq!(page_header_out, page_header);
    }

    #[test]
    fn empty_page_header() {
        let mut buf = Vec::new();

        let page_header = PageHeader(None);
        page_header
            .encode_into(&mut buf)
            .expect("failed to encode page header");
        let page_header_out =
            PageHeader::decode_from(buf.as_slice()).expect("failed to decode page header");

        assert_eq!(page_header_out, page_header);
    }
}
