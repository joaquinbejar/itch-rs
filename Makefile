.DEFAULT_GOAL := help

CARGO ?= cargo
LOG_LEVEL ?= warn

# ---------- help ----------

help: ## Show common targets
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | \
		awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-20s\033[0m %s\n", $$1, $$2}'

# ---------- build / test ----------

build: ## Build all crates (debug)
	$(CARGO) build --workspace --all-features

release: ## Build all crates (release)
	$(CARGO) build --workspace --all-features --release

test: ## Run all tests (LOGLEVEL=warn for quiet output)
	LOGLEVEL=$(LOG_LEVEL) $(CARGO) test --workspace --all-features

run: ## Run the demo server (Ctrl-C to stop)
	$(CARGO) run -p itch-server

# ---------- formatting ----------

fmt: ## Format all crates
	$(CARGO) fmt --all

fmt-check: ## Check formatting (CI-style)
	$(CARGO) fmt --all --check

# ---------- linting ----------

lint: ## Clippy with -D warnings
	$(CARGO) clippy --all-targets --all-features -- -D warnings

lint-fix: ## Clippy with auto-fix
	$(CARGO) clippy --all-targets --all-features --fix --allow-dirty --allow-staged

check: ## Cargo check (no codegen)
	$(CARGO) check --workspace --all-features

fix: ## Apply suggested fixes (cargo fix + clippy fix)
	$(CARGO) fix --workspace --all-features --allow-dirty --allow-staged
	$(MAKE) lint-fix

# ---------- docs ----------

doc: ## Build rustdoc (deny warnings)
	RUSTDOCFLAGS="-D warnings" $(CARGO) doc --workspace --no-deps

# ---------- coverage ----------

coverage: ## Run cargo tarpaulin (line coverage to stdout)
	$(CARGO) tarpaulin --workspace --all-features --out Stdout

coverage-html: ## Run cargo tarpaulin with HTML output
	$(CARGO) tarpaulin --workspace --all-features --out Html

# ---------- benchmarks ----------

bench: ## Run Criterion benches
	$(CARGO) bench --workspace --all-features

bench-save: ## Save current run as a baseline (HISTORY=label)
ifndef HISTORY
	$(error Set HISTORY=<label>, e.g., make bench-save HISTORY=v0.1.0)
endif
	$(CARGO) bench --workspace --all-features -- --save-baseline $(HISTORY)

bench-compare: ## Compare current run vs baseline (HISTORY=label)
ifndef HISTORY
	$(error Set HISTORY=<label>, e.g., make bench-compare HISTORY=v0.1.0)
endif
	$(CARGO) bench --workspace --all-features -- --baseline $(HISTORY)

# ---------- fuzz ----------

FUZZ_BUDGET ?= 60
FUZZ_TARGETS := message_decode itch_codec_decode soup_packet_decode mold_packet_decode

fuzz-seed-corpus: ## Copy committed seeds into runtime corpus (idempotent)
	@for t in $(FUZZ_TARGETS); do \
		mkdir -p fuzz/corpus/$$t ; \
		if [ -d fuzz/seeds/$$t ]; then \
			cp -n fuzz/seeds/$$t/*.bin fuzz/corpus/$$t/ 2>/dev/null || true ; \
		fi ; \
	done

fuzz: fuzz-seed-corpus ## Run cargo-fuzz smoke (FUZZ_BUDGET=60 s per target; requires nightly)
	@for t in $(FUZZ_TARGETS); do \
		cargo +nightly fuzz run --fuzz-dir fuzz $$t -- -max_total_time=$(FUZZ_BUDGET) || exit $$? ; \
	done

fuzz-list: ## List the configured fuzz targets
	cargo +nightly fuzz list --fuzz-dir fuzz

# ---------- composite ----------

pre-push: fix fmt lint-fix test doc ## Canonical pre-push gate

# ---------- 1.0 release gates ----------

# Promoted crates that ship a 1.0 release. Update this list when
# adding or removing crates from the 1.0 promotion set (see #42).
RELEASE_CRATES ?= itch-protocol itch-tcp itch-soup itch-mold

public-api: ## Print the full public API of every promoted crate (requires `cargo install cargo-public-api`)
	@for c in $(RELEASE_CRATES); do \
		echo "==> $$c"; \
		$(CARGO) public-api -p $$c || exit $$?; \
	done

semver-check: ## Validate no breaking change since the last published version of each promoted crate (requires `cargo install cargo-semver-checks`)
	@for c in $(RELEASE_CRATES); do \
		echo "==> $$c"; \
		$(CARGO) semver-checks check-release -p $$c || exit $$?; \
	done

check-msrv: ## Build + test every promoted crate against the pinned MSRV (requires the MSRV toolchain installed via `rustup toolchain install`)
	@MSRV=$$(grep -E '^rust-version' crates/itch-protocol/Cargo.toml | head -1 | sed -E 's/.*"([0-9.]+)".*/\1/'); \
	if [ -z "$$MSRV" ]; then echo "could not detect MSRV from crates/itch-protocol/Cargo.toml"; exit 1; fi; \
	echo "MSRV: $$MSRV"; \
	for c in $(RELEASE_CRATES); do \
		echo "==> $$c (build)"; $(CARGO) +$$MSRV build -p $$c || exit $$?; \
		echo "==> $$c (test)";  $(CARGO) +$$MSRV test  -p $$c || exit $$?; \
	done

pre-publish: pre-push public-api semver-check check-msrv ## Full pre-publish gate (1.0 release-readiness)

# ---------- workflow helpers ----------

workflow-list: ## List GitHub Actions workflow runs
	gh run list --limit 10

workflow-view: ## View latest run on current branch
	gh run view --log

# ---------- housekeeping ----------

clean: ## Cargo clean
	$(CARGO) clean

.PHONY: help build release test run fmt fmt-check lint lint-fix check fix \
        doc coverage coverage-html bench bench-save bench-compare fuzz \
        pre-push public-api semver-check check-msrv pre-publish \
        workflow-list workflow-view clean
