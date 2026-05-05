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

# ---------- fuzz (post-MVP) ----------

fuzz: ## Run cargo-fuzz smoke (60 s per target — requires nightly toolchain)
	@echo "fuzz harness lands in issue #29 (post-v0.1)"

# ---------- composite ----------

pre-push: fix fmt lint-fix test doc ## Canonical pre-push gate

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
        pre-push workflow-list workflow-view clean
