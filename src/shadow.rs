//! Compatibility shim for the canonical shadow WAL implementation.
//!
//! Root sync code keeps its module path while Phase 4 convergence moves shadow
//! WAL behavior to `walrust-core`.

#[allow(unused_imports)]
pub use walrust_core::shadow::{ShadowSegment, ShadowWal};

#[cfg(test)]
/// Hex width for root-side test/helpers that synthesize segment filenames.
pub(crate) const SEGMENT_HEX_WIDTH: usize = 16;

#[cfg(test)]
/// Format a shadow segment filename: `{generation:016x}-{index:016x}.wal`.
pub(crate) fn format_segment_name(generation: u64, index: u64) -> String {
    format!(
        "{:0width$x}-{:0width$x}.wal",
        generation,
        index,
        width = SEGMENT_HEX_WIDTH
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_segment_name_width_keeps_lexical_order_past_u32() {
        let before_wrap = format_segment_name(0xffff_ffff, 0);
        let after_wrap = format_segment_name(0x1_0000_0000, 0);
        assert!(
            before_wrap < after_wrap,
            "lexical order must follow numeric order: {before_wrap} vs {after_wrap}"
        );
        assert_eq!(before_wrap.len(), after_wrap.len(), "fixed width");
        assert_eq!(before_wrap.len(), SEGMENT_HEX_WIDTH * 2 + 5);
    }
}
