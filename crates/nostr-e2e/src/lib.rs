//! Cross-crate end-to-end tests and adversarial robustness checks.
//!
//! This crate intentionally has no library code: it exists so that integration tests can
//! exercise `nostr-core`, `nostr-store` and the `nostr-relay` binary together without
//! creating dependency cycles between the implementation crates.

/// Re-exported for convenience in tests.
pub use nostr_core as core;
pub use nostr_store as store;
