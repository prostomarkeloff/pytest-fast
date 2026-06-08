.PHONY: lint-heavy test-full test-stress test-parity fuzz-install fuzz-seeds fuzz clean

UV ?= uv

# `LINT_HEAVY_CI=1` switches local-autofix targets into check-only mode (for CI:
# `ruff format --check`, `ruff check` without `--fix`). Don't set it locally — the
# default keeps `format` mutating the working tree + safe autofixes.
LINT_HEAVY_CI ?=
ifeq ($(LINT_HEAVY_CI),1)
RUFF_FORMAT_FLAGS := --check
RUFF_CHECK_FLAGS  :=
else
RUFF_FORMAT_FLAGS :=
RUFF_CHECK_FLAGS  := --fix
endif

# Per-worktree resident socket. Slugged from the repo root so two checkouts (e.g.
# git worktrees) don't fight over the same daemon — each gets its own warm pool.
WT_PATH := $(shell git rev-parse --show-toplevel 2>/dev/null || pwd)
WT_HASH := $(shell printf '%s' "$(WT_PATH)" | shasum | cut -c1-6)
WT_SLUG := $(shell basename "$(WT_PATH)" | tr '[:upper:]' '[:lower:]' | sed -E 's/[^a-z0-9]+/_/g; s/^_+//; s/_+$$//' | cut -c1-40)
PYTEST_FAST_SOCK := /tmp/pytest-fast-$(WT_SLUG)-$(WT_HASH).sock

# `lint-heavy` runs ruff format + ruff check + pyright. Local default applies safe
# autofixes; CI sets `LINT_HEAVY_CI=1` for check-only behavior (same target, same
# semantics, no mutations).
lint-heavy:
	@set -e; \
	echo "=== ruff format ==="; \
	$(UV) run ruff format $(RUFF_FORMAT_FLAGS) src tests; \
	echo ""; \
	echo "=== ruff check ==="; \
	$(UV) run ruff check $(RUFF_CHECK_FLAGS) src tests; \
	echo ""; \
	echo "=== pyright ==="; \
	$(UV) run pyright

# Dogfood: run pytest-fast's own tests through pytest-fast itself. The daemon is
# resident on a per-worktree socket; the first run boots (~3s), subsequent runs
# reuse the warm forkserver (≈ fork() warmup).
#
# `PYTEST_FAST_MARK` (== collection `-m`) excludes the opt-in fuzz/stress tier: those
# spawn their own daemons and Hypothesis loops and belong in `make test-stress` / the
# dedicated CI job, not every dogfood run.
test-full:
	PYTEST_FAST_MARK="not fuzz and not stress and not parity" $(UV) run pytest-fast --address $(PYTEST_FAST_SOCK) --workers 4 --ttl 600

# The opt-in fuzz + stress tier (Hypothesis property tests, live-daemon fuzzing,
# resource-exhaustion). Run via PLAIN pytest (not the dogfood daemon): these spawn
# their own subprocess daemons. `HYPOTHESIS_PROFILE=ci` derandomizes for reproducibility.
test-stress:
	HYPOTHESIS_PROFILE=ci $(UV) run pytest -m "fuzz or stress" -p no:cacheprovider

# Differential parity tier: the engine's per-test outcomes must match plain pytest & xdist.
# `--group xdist-parity` pulls in pytest-xdist for the xdist leg without permanently changing
# the synced env. Each generated suite spawns three subprocess runners, so this is slow.
test-parity:
	HYPOTHESIS_PROFILE=ci $(UV) run --group xdist-parity pytest -m parity -p no:cacheprovider

# ── Atheris coverage-guided fuzzing (POSIX; libFuzzer) ───────────────────────────
# Atheris is NOT a pyproject dependency: it targets py3.11–3.13 (the test matrix goes to
# 3.14) and on macOS must be built against a libFuzzer-capable clang. So it's installed
# ad-hoc into the venv here. The corpus + tests/test_fuzz_corpus.py replay (which need NO
# Atheris) are the durable per-PR guard; this is the deep, scheduled crash hunt.
fuzz-install:
	@if [ "$$(uname)" = "Darwin" ]; then \
		echo "macOS: building atheris against Homebrew LLVM (Apple clang lacks libFuzzer)"; \
		CLANG_BIN="$$(brew --prefix llvm)/bin/clang" $(UV) pip install atheris; \
	else \
		$(UV) pip install atheris; \
	fi

fuzz-seeds:
	$(UV) run python fuzz/make_seeds.py

# Time-boxed coverage-guided run over the wire decoder. Override the budget with
# `make fuzz FUZZ_TIME=600`. `-rss_limit_mb` turns a decode-amplification OOM into a finding.
# New units are written to the gitignored scratch dir (first corpus arg); fuzz/corpus is
# read-only seeds, so it stays curated. Both are read for coverage.
FUZZ_TIME ?= 120
# `--no-sync`: atheris was `uv pip install`ed ad-hoc (not a locked dep). A bare `uv run`
# re-reconciles the env against `.python-version`; if the venv was built on a different
# interpreter it gets torn down and rebuilt, dropping atheris. `--no-sync` uses the
# existing venv as-is (run `make fuzz-install` first to put atheris there).
fuzz: fuzz-seeds
	@mkdir -p fuzz/corpus-work
	$(UV) run --no-sync python fuzz/fuzz_wire.py -max_total_time=$(FUZZ_TIME) -rss_limit_mb=2048 fuzz/corpus-work fuzz/corpus

# Remove pyc/__pycache__ and the resident socket (next test-full will spawn a fresh daemon).
clean:
	find . -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null || true
	rm -f $(PYTEST_FAST_SOCK) $(PYTEST_FAST_SOCK).pid $(PYTEST_FAST_SOCK).respawn.lock $(PYTEST_FAST_SOCK).watcher.lock
	rm -f $(PYTEST_FAST_SOCK).staging $(PYTEST_FAST_SOCK).staging.pid
	rm -f $(patsubst %.sock,%-daemon.log,$(PYTEST_FAST_SOCK))
	rm -f $(patsubst %.sock,%-daemon.staging.log,$(PYTEST_FAST_SOCK))
	rm -f $(patsubst %.sock,%-watcher.log,$(PYTEST_FAST_SOCK))
