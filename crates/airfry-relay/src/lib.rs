//! `airfry-relay` library surface.
//!
//! The binary in `src/main.rs` is a thin CLI wrapper around these modules; exposing them as a
//! library lets the integration tests boot a relay in-process on an ephemeral port.

pub mod config;
pub mod http;
pub mod hub;
pub mod negentropy;
pub mod nip11;
pub mod server;
pub mod util;

pub use config::Config;
pub use server::Relay;
