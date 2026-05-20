//! TLM_DELTA — phase 004 wire envelope for external-base-state deltas.
//!
//! Every walrust external-mode delta object is a canonical CBOR
//! envelope produced by [`encode`]. The envelope carries the full
//! phase 004 fencing + chain tuple alongside the LTX payload:
//!
//! - `seq` — strictly monotonic per `(prefix, db_name)`. Enforced at
//!   upload time via `put_if_absent`.
//! - `epoch` — lease epoch of the publishing writer. Followers reject
//!   deltas whose `epoch != cursor.epoch`.
//! - `writer_id` — lease holder identity. Followers reject deltas
//!   whose `writer_id != base.writer_id`.
//! - `prev_checksum` — BLAKE3 over the prior object's envelope bytes
//!   (the published base for the first delta, or the prior delta for
//!   subsequent ones). This is the chain link.
//! - `end_page_count` — page count at the end of this delta. Handles
//!   shrink / grow / VACUUM / truncate.
//! - `ltx_payload` — the raw LTX bytes (the actual page changes).
//!
//! Envelope layout (v1):
//!
//! ```text
//! +-------+-------+-------+-------+-------+-------+----------+
//! |  'T'  |  'L'  |  'M'  |  'D'  | version_be_u16  |  CBOR  |
//! +-------+-------+-------+-------+-------+-------+----------+
//!  ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^  ^^^^^^^^^^^^^^^  ^^^^^^^^
//!         4-byte magic ("TLMD")     2-byte version    canonical CBOR
//! ```
//!
//! Magic byte is `'T'` (0x54) — same first byte as turbolite's
//! `TLM1` manifest envelope, but the fourth byte (`'D'` vs `'1'`)
//! disambiguates the two payload types if they ever land in the same
//! storage prefix.
//!
//! ### Encoding stability (deterministic, not RFC-8949 canonical)
//!
//! Same approach as `turbolite::tiered::wire`: ciborium serializes the
//! serde-derived `DeltaPayloadV1` *deterministically for this fixed struct
//! definition* — fields emit in declaration order, integers use
//! smallest-length form, and there are no maps or floats (whose ordering /
//! representation ciborium does not canonicalize per RFC 8949). This is
//! sufficient ONLY because every verifier hashes the *stored bytes*, never
//! a re-encoding. Do not add `f64` or map-typed fields, and do not hash a
//! re-encoded payload — either would let two nodes derive different
//! checksums for the same logical delta and break chain verification
//! permanently. The golden BLAKE3 hash in
//! [`tests::canonical_delta_v1_blake3_golden_hash`] is the trip-wire: it
//! fires on any drift across ciborium / serde_bytes / blake3 upgrades.
//! Re-pin only after root-causing the drift and bumping [`VERSION_V1`].

use serde::{Deserialize, Serialize};

/// Magic bytes — "Turbolite Linked-Manifest Delta, format family 1".
const MAGIC: [u8; 4] = *b"TLMD";

/// Current canonical-CBOR envelope version.
const VERSION_V1: u16 = 0x0001;

/// Error surface for delta envelope decode + encode.
#[derive(Debug, thiserror::Error)]
pub enum DeltaPayloadError {
    /// Payload shorter than the envelope header (magic + version).
    #[error("delta payload too short to be a TLMD envelope ({0} bytes)")]
    Truncated(usize),

    /// Magic bytes did not match. Likely a raw LTX or different format.
    #[error("delta magic mismatch: expected TLMD, got {got:?}")]
    MagicMismatch { got: [u8; 4] },

    /// Magic matched, but the version is one this build does not understand.
    #[error(
        "unsupported delta format version 0x{found:04x}; this build supports 0x{supported:04x}"
    )]
    UnsupportedVersion { found: u16, supported: u16 },

    /// CBOR body present but malformed.
    #[error("delta body decode failed: {0}")]
    BodyDecode(String),

    /// CBOR body encode failure on the writer side.
    #[error("delta body encode failed: {0}")]
    BodyEncode(String),
}

/// Canonical delta payload (v1).
///
/// Field order matters — ciborium serializes serde structs as CBOR
/// maps keyed by field name and emits keys in declaration order. The
/// golden-hash test pins the resulting byte layout; renaming or
/// reordering fields here changes the hash.
///
/// `prev_checksum` and `ltx_payload` use `#[serde(with =
/// "serde_bytes")]` so they encode as CBOR byte strings (major type
/// 2) instead of arrays of u8 (major type 4). For a 32-byte
/// `prev_checksum` that's ~30 bytes saved per delta on the wire; for
/// a multi-KB `ltx_payload` it's the difference between a compact
/// byte string and a length-prefixed array of small integers.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DeltaPayloadV1 {
    /// Strictly monotonic per `(prefix, db_name)`. Phase 004 followers
    /// list objects with `seq > cursor.last_applied_seq`.
    pub seq: u64,

    /// Lease epoch under which this delta was published. Followers
    /// reject candidates whose epoch does not match the cursor's
    /// epoch — that's how stale-writer leftover gets fenced even if
    /// the chain link happens to verify.
    pub epoch: u64,

    /// Identifier of the lease holder. Filtered against
    /// `base.writer_id` before chain verification.
    pub writer_id: String,

    /// BLAKE3 over the prior object's canonical envelope bytes. For
    /// the first delta after a published base, this equals the
    /// base's `base_object_checksum`. For subsequent deltas, the
    /// hash of the immediately-prior delta's TLMD envelope.
    ///
    /// 32 bytes for a normal chain link; empty for the genesis delta
    /// (sentinel — never produced by a real writer).
    #[serde(with = "serde_bytes")]
    pub prev_checksum: Vec<u8>,

    /// Page count at the end of this delta's apply. Handles shrink
    /// (VACUUM, truncate) and grow uniformly — the follower sets the
    /// db file's page count to this value after applying.
    pub end_page_count: u64,

    /// Raw LTX bytes (hadb-changeset physical encoding). The actual
    /// page-level changes the follower applies after chain
    /// verification + epoch/writer filtering succeed.
    #[serde(with = "serde_bytes")]
    pub ltx_payload: Vec<u8>,
}

/// Encode a [`DeltaPayloadV1`] into the canonical TLMD envelope.
///
/// `encode(p) == encode(p.clone())` — the encoding is a function of
/// the logical value, not of any runtime ordering. BLAKE3 over these
/// bytes is the chain link the next delta points back to (see
/// [`checksum`]).
pub fn encode(payload: &DeltaPayloadV1) -> Result<Vec<u8>, DeltaPayloadError> {
    let mut body: Vec<u8> = Vec::new();
    ciborium::into_writer(payload, &mut body)
        .map_err(|e| DeltaPayloadError::BodyEncode(e.to_string()))?;

    let mut out = Vec::with_capacity(MAGIC.len() + 2 + body.len());
    out.extend_from_slice(&MAGIC);
    out.extend_from_slice(&VERSION_V1.to_be_bytes());
    out.extend_from_slice(&body);
    Ok(out)
}

/// Decode envelope bytes back into a [`DeltaPayloadV1`].
pub fn decode(bytes: &[u8]) -> Result<DeltaPayloadV1, DeltaPayloadError> {
    if bytes.len() < MAGIC.len() + 2 {
        return Err(DeltaPayloadError::Truncated(bytes.len()));
    }

    let got: [u8; 4] = bytes[..MAGIC.len()]
        .try_into()
        .expect("len-checked above");
    if got != MAGIC {
        return Err(DeltaPayloadError::MagicMismatch { got });
    }

    let version_bytes: [u8; 2] = bytes[MAGIC.len()..MAGIC.len() + 2]
        .try_into()
        .expect("len-checked above");
    let version = u16::from_be_bytes(version_bytes);
    if version != VERSION_V1 {
        return Err(DeltaPayloadError::UnsupportedVersion {
            found: version,
            supported: VERSION_V1,
        });
    }

    let body = &bytes[MAGIC.len() + 2..];
    let payload: DeltaPayloadV1 = ciborium::from_reader(body)
        .map_err(|e| DeltaPayloadError::BodyDecode(e.to_string()))?;
    Ok(payload)
}

/// BLAKE3 over the envelope bytes — the value that the *next* delta's
/// `prev_checksum` must equal, and what a follower's chain verifier
/// computes when walking forward from a known anchor.
pub fn checksum(envelope_bytes: &[u8]) -> [u8; 32] {
    *blake3::hash(envelope_bytes).as_bytes()
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Fixture with realistic byte sizes so the canonical encoding
    /// exercises both the small-integer and byte-string paths.
    fn golden_fixture() -> DeltaPayloadV1 {
        DeltaPayloadV1 {
            seq: 42,
            epoch: 7,
            writer_id: "node-leader-A".into(),
            prev_checksum: vec![0xAA; 32],
            end_page_count: 1024,
            ltx_payload: vec![0xCD; 128],
        }
    }

    #[test]
    fn round_trip_preserves_all_fields() {
        let p = golden_fixture();
        let bytes = encode(&p).expect("encode");
        // Envelope header sanity.
        assert_eq!(&bytes[..4], &MAGIC, "magic");
        assert_eq!(u16::from_be_bytes([bytes[4], bytes[5]]), VERSION_V1);

        let decoded = decode(&bytes).expect("decode");
        assert_eq!(decoded, p);
    }

    /// Canonical encoding is a function of the logical value, not of
    /// runtime state. Re-encoding produces identical bytes.
    #[test]
    fn canonical_encoding_is_deterministic() {
        let p = golden_fixture();
        let bytes1 = encode(&p).expect("encode 1");
        let bytes2 = encode(&p).expect("encode 2");
        assert_eq!(bytes1, bytes2);

        // Even a fresh clone produces identical bytes.
        let p2 = p.clone();
        let bytes3 = encode(&p2).expect("encode 3");
        assert_eq!(bytes1, bytes3);
    }

    /// BLAKE3 golden hash — fires loud if any encoding detail shifts.
    /// To re-pin: `TURBOLITE_PIN_GOLDEN_HASH=1 cargo test
    /// canonical_delta_v1_blake3_golden_hash -- --nocapture` and copy
    /// the printed hex below. Only re-pin after root-causing the
    /// drift and bumping `VERSION_V1`.
    #[test]
    fn canonical_delta_v1_blake3_golden_hash() {
        // Pinned against ciborium 0.2 + blake3 1.x + serde_bytes 0.11
        // + DeltaPayloadV1 as currently defined. Any drift fires this
        // assertion; root-cause it before re-pinning.
        const EXPECTED_HEX: &str =
            "8df5ec375f80ce264406ecd5125540ab321e2e28e844dcc00f700f1a3c079a36";

        let bytes = encode(&golden_fixture()).expect("encode");
        let hash = checksum(&bytes);
        let hex: String = hash.iter().map(|b| format!("{:02x}", b)).collect();

        if std::env::var("TURBOLITE_PIN_GOLDEN_HASH").is_ok() {
            eprintln!("canonical_delta_v1_blake3_golden_hash: {hex}");
            return;
        }

        assert_eq!(
            hex, EXPECTED_HEX,
            "canonical delta encoding changed; root-cause before re-pinning"
        );
    }

    /// Decode-then-encode is the byte-level identity. Without this,
    /// two followers could hash the same delta to different values.
    #[test]
    fn decode_then_re_encode_is_byte_identity() {
        let original = encode(&golden_fixture()).expect("encode");
        let decoded = decode(&original).expect("decode");
        let reencoded = encode(&decoded).expect("re-encode");
        assert_eq!(original, reencoded);
    }

    /// `checksum()` is BLAKE3 over the envelope and is deterministic
    /// for the same logical payload.
    #[test]
    fn checksum_returns_blake3_of_envelope() {
        let bytes = encode(&golden_fixture()).expect("encode");
        let hash = checksum(&bytes);
        assert_eq!(hash.len(), 32);

        let bytes_again = encode(&golden_fixture()).expect("encode again");
        let hash_again = checksum(&bytes_again);
        assert_eq!(hash, hash_again);
    }

    #[test]
    fn empty_bytes_truncated() {
        let err = decode(&[]).expect_err("must reject");
        assert!(matches!(err, DeltaPayloadError::Truncated(0)));
    }

    #[test]
    fn short_bytes_truncated() {
        let err = decode(&[0xAA, 0xBB, 0xCC]).expect_err("must reject");
        assert!(matches!(err, DeltaPayloadError::Truncated(3)));
    }

    #[test]
    fn unknown_magic_returns_magic_mismatch() {
        let bytes: Vec<u8> = b"XXXX\x00\x01junk".to_vec();
        let err = decode(&bytes).expect_err("must reject");
        match err {
            DeltaPayloadError::MagicMismatch { got } => assert_eq!(&got, b"XXXX"),
            other => panic!("expected MagicMismatch, got {other:?}"),
        }
    }

    #[test]
    fn unknown_version_returns_unsupported_version() {
        let mut bytes = MAGIC.to_vec();
        bytes.extend_from_slice(&0x0099u16.to_be_bytes());
        bytes.push(0xff);
        let err = decode(&bytes).expect_err("must reject");
        match err {
            DeltaPayloadError::UnsupportedVersion { found, supported } => {
                assert_eq!(found, 0x0099);
                assert_eq!(supported, VERSION_V1);
            }
            other => panic!("expected UnsupportedVersion, got {other:?}"),
        }
    }

    /// Raw LTX bytes (the prior format) start with the LTX header
    /// magic `0x4c545832` ("LTX2") — first byte 0x4c = 'L'. That's
    /// different from our 'T' = 0x54, so feeding raw LTX bytes
    /// through decode produces a clean MagicMismatch, not a
    /// silent fallback.
    #[test]
    fn raw_ltx_bytes_dont_silently_decode() {
        let bytes: Vec<u8> = vec![0x4c, 0x54, 0x58, 0x32, 0x00, 0x00, 0x01, 0x02];
        let err = decode(&bytes).expect_err("must reject");
        assert!(matches!(err, DeltaPayloadError::MagicMismatch { got } if &got == b"LTX2"));
    }

    /// `epoch` and `writer_id` are stamped on every delta. This test
    /// is named to match the plan's
    /// `walrust_external_epoch_writer_stamped` integration test —
    /// the field-level proof that follower filtering has stable
    /// data to work with. Step 2 unit-level check; step 4 wires it
    /// into the candidate filter.
    #[test]
    fn walrust_external_epoch_writer_stamped() {
        let payload = DeltaPayloadV1 {
            seq: 100,
            epoch: 5,
            writer_id: "leader-zone-a".into(),
            prev_checksum: vec![0u8; 32],
            end_page_count: 256,
            ltx_payload: vec![0u8; 32],
        };
        let bytes = encode(&payload).expect("encode");
        let decoded = decode(&bytes).expect("decode");
        assert_eq!(decoded.epoch, 5, "epoch must survive encode/decode");
        assert_eq!(
            decoded.writer_id, "leader-zone-a",
            "writer_id must survive encode/decode"
        );
    }

    /// The plan-named `walrust_external_chain_strict` unit shape:
    /// `prev_checksum` (the 32-byte BLAKE3) round-trips losslessly
    /// at full length, and is the same as `checksum()` over the
    /// prior envelope when used as designed.
    #[test]
    fn walrust_external_chain_strict() {
        let first = DeltaPayloadV1 {
            seq: 1,
            epoch: 1,
            writer_id: "w".into(),
            prev_checksum: vec![0u8; 32], // genesis-style sentinel
            end_page_count: 10,
            ltx_payload: vec![1, 2, 3],
        };
        let first_bytes = encode(&first).expect("encode first");
        let first_hash = checksum(&first_bytes);

        let second = DeltaPayloadV1 {
            seq: 2,
            epoch: 1,
            writer_id: "w".into(),
            prev_checksum: first_hash.to_vec(),
            end_page_count: 11,
            ltx_payload: vec![4, 5, 6],
        };
        let second_bytes = encode(&second).expect("encode second");
        let second_decoded = decode(&second_bytes).expect("decode second");

        // The chain link is exactly the BLAKE3 of the prior envelope.
        assert_eq!(
            second_decoded.prev_checksum,
            first_hash.to_vec(),
            "second.prev_checksum must equal BLAKE3(first envelope bytes)"
        );
        assert_eq!(second_decoded.prev_checksum.len(), 32);
    }
}
