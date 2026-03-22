use std::{
    fmt, io, num, ops,
    path::{Path, PathBuf},
};

/// An ID of a database transaction.
#[derive(
    Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, serde::Serialize, serde::Deserialize,
)]
#[serde(into = "String", try_from = "String")]
pub struct TXID(num::NonZeroU64);

impl TXID {
    pub const ONE: TXID = TXID(num::NonZeroU64::MIN);

    /// Contruct a new database transaction ID.
    pub const fn new(id: u64) -> Result<Self, TXIDError> {
        if let Some(id) = num::NonZeroU64::new(id) {
            Ok(Self(id))
        } else {
            Err(TXIDError::Zero)
        }
    }

    /// Return the underlying integer representation of the transaction ID.
    pub const fn into_inner(&self) -> u64 {
        self.0.get()
    }
}

impl fmt::Display for TXID {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        write!(f, "{:016x}", self.0.get())
    }
}

impl From<TXID> for String {
    fn from(txid: TXID) -> Self {
        txid.to_string()
    }
}

impl TryFrom<String> for TXID {
    type Error = TXIDError;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        let txid = u64::from_str_radix(&value, 16).map_err(|_| TXIDError::NonInteger)?;
        TXID::new(txid)
    }
}

impl ops::Add<u64> for TXID {
    type Output = TXID;

    fn add(self, rhs: u64) -> Self::Output {
        let sum = self.into_inner() + rhs;
        assert!(sum != 0, "TX ID overflow");

        TXID(unsafe { num::NonZeroU64::new_unchecked(sum) })
    }
}

/// An error representing invalid transaction ID.
#[derive(thiserror::Error, Debug)]
#[error("transaction ID must be non-zero")]
pub enum TXIDError {
    #[error("non-integer transaction ID")]
    NonInteger,
    #[error("zero transaction ID")]
    Zero,
}

/// A database checksum.
#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(into = "String", try_from = "String")]
pub struct Checksum(u64);

impl Checksum {
    const NON_ZERO_FLAG: u64 = 1 << 63;

    /// Construct a new database checksum.
    pub const fn new(s: u64) -> Self {
        Self(s | Self::NON_ZERO_FLAG)
    }

    /// Return underlying integer representation of the database checksum.
    pub const fn into_inner(&self) -> u64 {
        self.0
    }
}

impl fmt::Display for Checksum {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        write!(f, "{:016x}", self.0)
    }
}

impl From<Checksum> for String {
    fn from(checksum: Checksum) -> Self {
        checksum.to_string()
    }
}

impl TryFrom<String> for Checksum {
    type Error = ChecksumError;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        let checksum = u64::from_str_radix(&value, 16).map_err(|_| ChecksumError)?;
        Ok(Checksum::new(checksum))
    }
}

impl ops::BitXor<Checksum> for Checksum {
    type Output = Checksum;

    fn bitxor(self, rhs: Checksum) -> Self::Output {
        Checksum::new(self.0 ^ rhs.0)
    }
}

/// An error representing an invalid database checksum.
#[derive(thiserror::Error, Debug)]
#[error("non-integer checksum")]
pub struct ChecksumError;

/// A database page size in bytes.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct PageSize(u32);

impl PageSize {
    const MIN_PAGE_SIZE: u32 = 512;
    const MAX_PAGE_SIZE: u32 = 65536;

    /// Construct a new database page size.
    pub const fn new(s: u32) -> Result<PageSize, PageSizeError> {
        if s < Self::MIN_PAGE_SIZE || s > Self::MAX_PAGE_SIZE || (s & (s - 1)) != 0 {
            Err(PageSizeError(s))
        } else {
            Ok(Self(s))
        }
    }

    /// Return the underlying integer representation of the database page size.
    pub const fn into_inner(&self) -> u32 {
        self.0
    }
}

impl fmt::Display for PageSize {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        write!(f, "{}", self.0)
    }
}

/// An error representing invalid database page size.
#[derive(thiserror::Error, Debug)]
#[error("unsupported page size: {0}")]
pub struct PageSizeError(u32);

/// A database page number.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, serde::Deserialize)]
#[serde(try_from = "u32")]
pub struct PageNum(num::NonZeroU32);

impl PageNum {
    pub const ONE: PageNum = PageNum(num::NonZeroU32::MIN);

    /// Construct a new database page number.
    pub const fn new(n: u32) -> Result<Self, PageNumError> {
        if let Some(n) = num::NonZeroU32::new(n) {
            Ok(PageNum(n))
        } else {
            Err(PageNumError)
        }
    }

    /// Return underlying integer representation of the database page number.
    pub const fn into_inner(&self) -> u32 {
        self.0.get()
    }

    /// Return the [lock page](https://www.sqlite.org/fileformat.html#the_lock_byte_page) number for the
    /// given page size.
    pub const fn lock_page(page_size: PageSize) -> PageNum {
        PageNum(unsafe { num::NonZeroU32::new_unchecked(0x40000000 / page_size.into_inner() + 1) })
    }
}

impl TryFrom<u32> for PageNum {
    type Error = PageNumError;

    fn try_from(v: u32) -> Result<Self, Self::Error> {
        PageNum::new(v)
    }
}

impl From<PageNum> for PathBuf {
    fn from(pgno: PageNum) -> Self {
        format!("{:08x}", pgno.0.get()).into()
    }
}

impl TryFrom<&Path> for PageNum {
    type Error = PageNumError;

    fn try_from(v: &Path) -> Result<Self, Self::Error> {
        let v = v.to_str().ok_or(PageNumError)?;
        let pgno = u32::from_str_radix(v, 16).map_err(|_| PageNumError)?;
        PageNum::new(pgno)
    }
}

impl fmt::Display for PageNum {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        write!(f, "{}", self.0.get())
    }
}

impl ops::Add<u32> for PageNum {
    type Output = PageNum;

    fn add(self, rhs: u32) -> Self::Output {
        let sum = self.into_inner() + rhs;
        assert!(sum != 0, "page number overflow");

        PageNum(unsafe { num::NonZeroU32::new_unchecked(sum) })
    }
}

/// An error representing invalid database page number.
#[derive(thiserror::Error, Debug)]
#[error("transaction ID must be non-zero")]
pub struct PageNumError;

impl From<PageNumError> for io::Error {
    fn from(e: PageNumError) -> Self {
        io::Error::new(io::ErrorKind::InvalidInput, e)
    }
}

/// A position uniquely identifying a state of a database.
#[derive(Copy, Clone, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct Pos {
    #[serde(rename = "txid")]
    /// Last transaction ID.
    pub txid: TXID,
    #[serde(rename = "postApplyChecksum")]
    /// Running database checksum at the given `txid`.
    pub post_apply_checksum: Checksum,
}

impl fmt::Display for Pos {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        write!(f, "{}/{}", self.txid, self.post_apply_checksum)
    }
}

#[cfg(test)]
mod tests {
    use super::{Checksum, PageNum, PageNumError, PageSize, PageSizeError, Pos, TXIDError, TXID};
    use serde_test::{assert_de_tokens, assert_tokens, Token};
    use std::path::{Path, PathBuf};

    #[test]
    fn txid() {
        assert_eq!(10, TXID::new(10).unwrap().into_inner());
        assert!(matches!(TXID::new(0), Err(TXIDError::Zero)));
        assert_eq!("000000000000000a", format!("{}", TXID::new(10).unwrap()))
    }

    #[test]
    fn checksum() {
        assert_eq!(1 | Checksum::NON_ZERO_FLAG, Checksum::new(1).into_inner());
        assert_eq!(Checksum::NON_ZERO_FLAG, Checksum::new(0).into_inner());
    }

    #[test]
    fn page_size() {
        assert_eq!(512, PageSize::new(512).unwrap().into_inner());
        assert_eq!(65536, PageSize::new(65536).unwrap().into_inner());
        assert!(matches!(PageSize::new(513), Err(PageSizeError(513))));
        assert!(matches!(PageSize::new(256), Err(PageSizeError(256))));
        assert!(matches!(PageSize::new(131072), Err(PageSizeError(131072))));
    }

    #[test]
    fn page_num() {
        assert_eq!(10, PageNum::new(10).unwrap().into_inner());
        assert!(matches!(PageNum::new(0), Err(PageNumError)));

        assert_eq!(
            Path::new("000000ff"),
            &PathBuf::from(PageNum::new(255).unwrap())
        );

        assert_eq!(
            PageNum::new(255).unwrap(),
            PageNum::try_from(Path::new("000000ff")).unwrap()
        );
    }

    #[test]
    fn pos_ser_de() {
        let pos = Pos {
            txid: TXID::new(0x123).unwrap(),
            post_apply_checksum: Checksum::new(0x456),
        };

        assert_tokens(
            &pos,
            &[
                Token::Struct {
                    name: "Pos",
                    len: 2,
                },
                Token::Str("txid"),
                Token::Str("0000000000000123"),
                Token::Str("postApplyChecksum"),
                Token::Str("8000000000000456"),
                Token::StructEnd,
            ],
        );
    }

    #[test]
    fn page_num_de() {
        let pgnum = PageNum::new(123).unwrap();

        assert_de_tokens(&pgnum, &[Token::U32(123)]);
    }
}
