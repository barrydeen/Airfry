//! Cross-crate end-to-end tests and adversarial robustness checks.
//!
//! This crate intentionally has no library code: it exists so that integration tests can
//! exercise `airfry-core`, `airfry-store` and the `airfry-relay` binary together without
//! creating dependency cycles between the implementation crates.

/// Re-exported for convenience in tests.
pub use airfry_core as core;
pub use airfry_store as store;
