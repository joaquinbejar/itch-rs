//! Demo ITCH 5.0 subscriber binary (workspace scaffold; full
//! implementation lands in issue #11).
//!
//! Once issue #11 merges, this binary will connect to `itch-server`,
//! decode incoming messages, and pretty-print one line per message.
//! Connect address will then be configurable via `ITCH_SERVER`
//! (default `127.0.0.1:9100`); see the workspace `README.md` for
//! runtime instructions.

#![forbid(unsafe_code)]
#![warn(missing_docs)]

fn main() {
    // Stubbed in the workspace-scaffold issue; full implementation
    // lands in issue #11 (decode + pretty-print).
}
