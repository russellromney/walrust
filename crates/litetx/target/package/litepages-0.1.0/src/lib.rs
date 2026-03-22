#![doc = include_str!(concat!(env!("CARGO_MANIFEST_DIR"), "/README.md"))]
mod decoder;
mod encoder;
mod ltx;
mod types;
mod utils;

pub use crate::ltx::{Header, HeaderFlags, PageChecksum, Trailer};
pub use types::{Checksum, PageNum, PageSize, Pos, TXID};

pub use decoder::{Decoder, Error as DecodeError};
pub use encoder::{Encoder, Error as EncodeError};
