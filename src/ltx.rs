//! Compatibility shim for the legacy Litestream-derived LTX implementation.
//!
//! Phase 4 convergence keeps the existing root CLI object format stable while
//! moving the codec and its invariants into `walrust-core`.

pub use walrust_core::legacy_ltx::*;
