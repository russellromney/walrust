//! Legacy Litestream-derived LTX object layout helpers.
//!
//! The root CLI still reads and writes this object layout while Phase 4 moves
//! the implementation into `walrust-core`.

/// Live incrementals go to generation 0 (`0000/`).
pub const GENERATION_LIVE: u64 = 0;

/// Format a TXID as 16-char lowercase hex.
pub fn format_txid_hex(txid: u64) -> String {
    format!("{txid:016x}")
}

/// Parse a TXID from a 16-char hex string.
pub fn parse_txid_hex(s: &str) -> Option<u64> {
    u64::from_str_radix(s, 16).ok()
}

/// Format an LTX filename as `{min_txid:016x}-{max_txid:016x}.ltx`.
pub fn format_ltx_filename(min_txid: u64, max_txid: u64) -> String {
    format!(
        "{}-{}.ltx",
        format_txid_hex(min_txid),
        format_txid_hex(max_txid)
    )
}

/// Parse min/max TXID from a legacy LTX filename.
pub fn parse_ltx_filename(filename: &str) -> Option<(u64, u64)> {
    let name = filename.strip_suffix(".ltx")?;
    let parts: Vec<&str> = name.split('-').collect();
    if parts.len() != 2 {
        return None;
    }
    let min_txid = parse_txid_hex(parts[0])?;
    let max_txid = parse_txid_hex(parts[1])?;
    Some((min_txid, max_txid))
}

/// Parse the old flat cache/S3 shape: `00000003.ltx`.
pub fn parse_legacy_flat_ltx_filename(filename: &str) -> Option<u64> {
    let name = filename.strip_suffix(".ltx")?;
    if name.contains('-') || name.len() != 8 {
        return None;
    }
    name.parse::<u64>().ok()
}

/// Ensure a non-empty prefix ends with `/`.
pub fn prefix_with_separator(prefix: &str) -> String {
    if prefix.is_empty() || prefix.ends_with('/') {
        prefix.to_string()
    } else {
        format!("{prefix}/")
    }
}

/// Build the per-database object prefix.
pub fn database_prefix(prefix: &str, db_name: &str) -> String {
    format!("{}{}/", prefix_with_separator(prefix), db_name)
}

/// Format generation folder name as 4-char lowercase hex.
pub fn format_generation(generation: u64) -> String {
    format!("{generation:04x}")
}

/// Parse generation from a folder name.
pub fn parse_generation(s: &str) -> Option<u64> {
    u64::from_str_radix(s, 16).ok()
}

/// Build an S3/object key for an LTX file in legacy layout.
pub fn build_ltx_key(
    prefix: &str,
    db_name: &str,
    generation: u64,
    min_txid: u64,
    max_txid: u64,
) -> String {
    format!(
        "{}{}/{}/{}",
        prefix_with_separator(prefix),
        db_name,
        format_generation(generation),
        format_ltx_filename(min_txid, max_txid)
    )
}

/// Single definition of "is this LTX file a snapshot (full DB base)".
pub fn is_snapshot(generation: u64, min_txid: u64, max_txid: u64) -> bool {
    generation > 0 || (min_txid == 1 && max_txid == 1)
}

/// A discovered legacy LTX file from object listing.
#[derive(Debug, Clone)]
pub struct DiscoveredLtx {
    /// Full object key.
    pub key: String,
    pub generation: u64,
    pub min_txid: u64,
    pub max_txid: u64,
    pub is_snapshot: bool,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn build_ltx_key_normalizes_prefix_separator() {
        assert_eq!(
            build_ltx_key("base", "db", 0, 2, 3),
            "base/db/0000/0000000000000002-0000000000000003.ltx"
        );
        assert_eq!(
            build_ltx_key("base/", "db", 0, 2, 3),
            "base/db/0000/0000000000000002-0000000000000003.ltx"
        );
        assert_eq!(
            build_ltx_key("", "db", 1, 1, 1),
            "db/0001/0000000000000001-0000000000000001.ltx"
        );
    }

    #[test]
    fn parse_legacy_flat_ltx_filename_accepts_only_old_cache_shape() {
        assert_eq!(parse_legacy_flat_ltx_filename("00000003.ltx"), Some(3));
        assert_eq!(parse_legacy_flat_ltx_filename("0000000000000003.ltx"), None);
        assert_eq!(
            parse_legacy_flat_ltx_filename("0000000000000002-0000000000000003.ltx"),
            None
        );
    }
}
