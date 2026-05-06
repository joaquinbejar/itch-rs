# 1.0 release checklist

This checklist scripts the coordinated 1.0 release of the four
core crates plus the meta-crate per ADR-0007 and the v1.0 roadmap
entry.

**Scope of this release** — promoted to 1.0:

- `itch-protocol` 1.0.0
- `itch-tcp` 1.0.0
- `itch-soup` 1.0.0
- `itch-mold` 1.0.0
- `itch-rs` 1.0.0 (meta-crate)

NOT in this release (stay on 0.x; promote in a later coordinated
release): `itch-source`, `itch-replay`, `itch-conformance`,
`itch-book`, `itch-orderbook`, `itch-source-redis`,
`itch-source-postgres`, `itch-source-kafka`, `itch-compressed`.

The version bumps, CHANGELOG date stamps, cross-crate dependency
version updates, and the API / MSRV manifests are landed by the
release-prep PR (#44). Everything below is **operator-only** —
the agent does not run `cargo publish` or push tags. Before
executing, set the date stamp in CHANGELOGs to today's actual
release date if it differs from the PR-merge day.

## Pre-flight

Run twice — once dry, once final, both green.

- [ ] `git switch main && git pull --rebase` so the release tag
      lands on the canonical tip.
- [ ] `make pre-publish` — composes `pre-push` + `public-api` +
      `semver-check` + `check-msrv`. Every gate green.
- [ ] `cargo public-api -p <crate>` matches the manifest in
      `API.md` for every promoted crate (no unintended additions).
- [ ] `cargo semver-checks check-release -p <crate>` clean for
      every promoted crate.
- [ ] `cargo doc -p <crate> --no-deps` zero warnings for every
      promoted crate (under `RUSTDOCFLAGS=-D warnings`).
- [ ] `cargo bench -p itch-protocol`, `-p itch-soup`,
      `-p itch-mold` reproduce the published numbers within
      reasonable variance. If they have drifted, re-publish under
      `docs/benchmarks/<release-date>/` and update
      `docs/COMPETITIVE-ANALYSIS.md` (local-only) +
      `BENCH.md` (tracked) before tagging.
- [ ] Fuzz nightly job green for at least 24 h (check
      `.github/workflows/fuzz-nightly.yml` runs).

## Tag and publish (per crate, in order)

Order is **mandatory**. Each crate must be live on crates.io
before the next one publishes; otherwise `cargo publish` rejects
the dependent because its `version = "1.0"` resolves to nothing.

### 1. `itch-protocol` (no internal deps)

```bash
cargo publish -p itch-protocol --dry-run
cargo publish -p itch-protocol
git tag itch-protocol-v1.0.0
git push origin itch-protocol-v1.0.0
```

Wait ~1 minute for the index to update before the next step.

### 2. The three transports (any order; siblings)

```bash
cargo publish -p itch-tcp  --dry-run && cargo publish -p itch-tcp
cargo publish -p itch-soup --dry-run && cargo publish -p itch-soup
cargo publish -p itch-mold --dry-run && cargo publish -p itch-mold

git tag itch-tcp-v1.0.0  && git push origin itch-tcp-v1.0.0
git tag itch-soup-v1.0.0 && git push origin itch-soup-v1.0.0
git tag itch-mold-v1.0.0 && git push origin itch-mold-v1.0.0
```

### 3. The meta-crate (depends on all four above)

```bash
cargo publish -p itch-rs --dry-run
cargo publish -p itch-rs
git tag itch-rs-v1.0.0
git push origin itch-rs-v1.0.0
```

## GitHub Releases

For each of the five crates above:

```bash
gh release create itch-protocol-v1.0.0 \
    --title "itch-protocol 1.0.0" \
    --notes-file crates/itch-protocol/CHANGELOG.md
```

Repeat for each tag. The CHANGELOG section under `## 1.0.0` is
the release-notes body; trim it after the first release section
if Markdown rendering pulls in older entries.

## Post-release smoke

From a fresh directory, prove the published crates resolve and
build cleanly:

```bash
cd /tmp && rm -rf itch-1.0-smoke && cargo new itch-1.0-smoke
cd itch-1.0-smoke
cargo add itch-rs --features full
cargo build --release
```

If the build succeeds, the workspace is canonically published.

## Announce

- [ ] `r/rust` post linking to the meta-crate page on crates.io
      and the `README.md` quickstart.
- [ ] This Week in Rust submission (PR against the `tww-rust`
      repo).
- [ ] Twitter / Mastodon (optional).
- [ ] Update repo `README.md` "Quickstart" to reference
      crates.io versions instead of path dependencies.

## Rollback (if a published 1.0 needs to be withdrawn)

`cargo yank` is the only legitimate retraction:

```bash
cargo yank --vers 1.0.0 itch-protocol
```

Then ship `1.0.1` (or `1.1.0`, depending on the fix scope) per
the standard SemVer rules.

## References

- ADR-0007 (independent crates).
- `API.md` (the SemVer commitment that 1.0 locks in).
- `MSRV.md` (toolchain floor).
- Per-crate `CHANGELOG.md` (each `1.0.0` section).
