# Security Policy

## Supported versions

Airfry is pre-1.0. Security fixes are made against the latest release on the `main` branch only.

| Version | Supported |
| --- | --- |
| 0.1.x (latest) | Yes |
| older | No — upgrade to the latest 0.1.x |

## Reporting a vulnerability

**Please do not open a public issue for security problems.**

Report privately by email to **barrydeen@protonmail.com**. Include:

- a description of the issue and its impact,
- the relay version (`target/release/airfry-relay --version`) and the flags you ran,
- the platform and Rust version,
- a minimal reproduction (client messages, filter, or a crafted event) if possible,
- any relevant logs (`RUST_LOG=debug`).

You can expect an acknowledgement within a few days. Once a fix is available it will be released on
`main` and noted in [CHANGELOG.md](CHANGELOG.md); credit will be given unless you ask otherwise.

## Scope

In scope:

- The relay's network-facing surface: the WebSocket protocol endpoint (NIP-01 EVENT/REQ/CLOSE/COUNT/
  AUTH), the NIP-11 document, and `/health`.
- Input validation: event id and BIP-340 signature verification, frame size limits, filter limits,
  tag handling, and NIP-09/NIP-40 semantics.
- The storage engine: record framing, checksums, crash recovery, torn-tail handling, and durability
  guarantees under the configured `--fsync-policy`.
- Denial-of-service vectors reachable by an unauthenticated client (memory growth, connection
  exhaustion, oversized frames, pathological filters).

Out of scope:

- Bugs in third-party dependencies (report upstream; we will bump the dependency).
- Performance characteristics (report as a normal issue with benchmark numbers).
- The strfry comparison harness in `scripts/` (report as a normal issue).
- Anything requiring physical access to the host or the data directory.

## Operational hardening notes

- `--fsync-policy never` (the default) maximises throughput but can lose the most recent events on
  power loss. Use `interval:<seconds>` or `always` if durability matters more than speed.
- `--verify-events false` skips id/signature verification. Only use it for trusted bulk import.
- Set `--max-event-bytes`, `--max-conns`, `--max-subs-per-conn`, `--max-filters` and `--max-limit`
  to bound resource use on a public relay.
- Put the relay behind a TLS-terminating reverse proxy for `wss://` deployments.
