//! Aggregate integration-test binary.
//!
//! Each module below used to be its own file directly under tests/, which
//! made cargo compile and link (and on macOS, dsymutil) a separate test
//! binary per file. Merging them into one binary removes that per-binary
//! link tax without changing any test. Test code is unmodified; only the
//! module wiring here is new, and test names gain a `module::` prefix.
//!
//! tests/production_e2e.rs intentionally stays a separate binary: it is the
//! load-bearing E2E suite, its SIGKILL tests re-exec their own binary by
//! name, and the nextest serial-group filters reference its test names.

mod cli_exit_codes;
mod legacy_ltx_core_convergence;
mod legacy_manifest_core_convergence;
mod restore_chain;
mod test_explain;
mod test_litestream_ltx_decode;
mod test_verify;
mod test_walrust_to_litestream;
mod test_webhooks;
