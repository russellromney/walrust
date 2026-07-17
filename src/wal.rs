//! Compatibility shim for the canonical WAL implementation.
//!
//! Phase 4 convergence keeps the root CLI surface stable while moving WAL
//! parsing/checksum behavior to `walrust-core`.

#[allow(unused_imports)]
pub use walrust_core::wal::*;
