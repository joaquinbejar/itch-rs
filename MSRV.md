# MSRV policy

This document defines the minimum supported Rust version (MSRV) policy
for every crate in this workspace, effective from 1.0.

## Policy

1. **Per-crate MSRV is `rust-version` in that crate's `Cargo.toml`.**
   Most crates inherit `rust-version.workspace = true` from the
   workspace; a crate may pin a higher MSRV if it depends on a
   newer language feature, but must not silently undershoot the
   workspace floor.
2. **MSRV may rise only in minor versions** (per-crate SemVer per
   ADR-0007). A patch release (`X.Y.Z` -> `X.Y.Z+1`) MUST NOT raise
   MSRV.
3. **One-minor-version deprecation window before raising.** Before
   bumping MSRV in `X.(Y+1).0`, the previous release `X.Y.0` MUST
   document the intent in its CHANGELOG (e.g. "MSRV will rise to
   1.79 in 1.5"). This gives downstream consumers a release cycle
   to plan their own toolchain bump.
4. **CI enforces the floor.** The `msrv` job in
   `.github/workflows/ci.yml` builds and tests every crate against
   the pinned MSRV on every PR. A PR that breaks the MSRV without
   explicit policy follow-up fails CI.

## Current MSRV

`rustc 1.75.0` for every crate in the workspace, effective 2026-05-06.

| Crate                   | MSRV  | Source                                 |
|-------------------------|-------|----------------------------------------|
| `itch-protocol`         | 1.75  | `[workspace.package]` inherited        |
| `itch-tcp`              | 1.75  | `[workspace.package]` inherited        |
| `itch-soup`             | 1.75  | `[workspace.package]` inherited        |
| `itch-mold`             | 1.75  | `[workspace.package]` inherited        |
| `itch-source`           | 1.75  | explicit `rust-version` in `Cargo.toml`|
| `itch-replay`           | 1.75  | explicit `rust-version` in `Cargo.toml`|
| `itch-conformance`      | 1.75  | `[workspace.package]` inherited        |
| `itch-book`             | 1.75  | `[workspace.package]` inherited        |
| `itch-orderbook`        | 1.75  | `[workspace.package]` inherited        |
| `itch-rs`               | 1.75  | `[workspace.package]` inherited        |
| `itch-compressed`       | 1.75  | `[workspace.package]` inherited        |
| `itch-source-redis`     | 1.75  | `[workspace.package]` inherited        |
| `itch-source-postgres`  | 1.75  | `[workspace.package]` inherited        |
| `itch-source-kafka`     | 1.75  | `[workspace.package]` inherited        |

`rust-toolchain.toml` stays on `stable` (CI also runs on `stable`);
the MSRV job installs the pinned 1.75 toolchain separately via
`dtolnay/rust-toolchain@1.75`.

`clippy` is **not** pinned to MSRV. Clippy lints move with the
toolchain, not the language; pinning clippy to MSRV would block
useful new lints without buying any compatibility. The `clippy`
CI job uses the default `stable` toolchain.

## Procedure for raising MSRV

To raise the workspace MSRV (e.g. `1.75` -> `1.79`):

1. **Deprecation announcement.** In the current minor's release,
   add a `## Deprecations` section to every affected crate's
   `CHANGELOG.md`:

       ## X.Y.Z (current minor)

       ### Deprecations

       - MSRV will rise from 1.75 to 1.79 in the next minor
         release. Downstream consumers on 1.75 / 1.76 / 1.77 / 1.78
         should plan a toolchain upgrade before our X.(Y+1).0 ships.

2. **Cycle through that release.** Ship the deprecation-announcement
   release; downstream has time to react.
3. **Bump in the next minor.**
   - Update `[workspace.package].rust-version` (or the crate-local
     `rust-version`) in `Cargo.toml`.
   - Update the **MSRV** table in this file with the new floor and
     the date.
   - Update `.github/workflows/ci.yml` `msrv` job: change
     `dtolnay/rust-toolchain@1.75` -> `@1.79` and rename the job to
     `cargo build + test (MSRV 1.79)`.
   - In each affected crate's CHANGELOG, replace the deprecation
     warning with a confirmation under `## X.(Y+1).0` -> `### Changed`:
     `MSRV: 1.75 -> 1.79`.
4. **Reviewer sign-off.** A PR that raises MSRV requires explicit
   reviewer approval (codified in the PR template — open a follow-up
   issue if a process change is needed).

## Procedure for adding a new crate

When adding a new crate to the workspace, prefer
`rust-version.workspace = true` so it inherits the workspace floor.
If the new crate genuinely needs a higher MSRV (e.g. a v3 of a
dependency that needs `Rust 1.80`), set an explicit
`rust-version = "X.Y"` and document the divergence in this file's
**Current MSRV** table.

## Local tooling

```bash
# Build + test against the pinned MSRV (cargo +1.75 must be available
# via `rustup toolchain install 1.75`).
make check-msrv

# Optional: discover the actual minimum a crate compiles on
# (requires `cargo install cargo-msrv`).
cargo msrv find -p itch-protocol
```

## References

- `docs/PRD.md` §4 NFR-7 (local-only).
- `docs/ROADMAP.md` v1.0 (local-only).
- ADR-0007 (independent crates).
- `API.md` (the SemVer commitment that runs alongside this policy).
