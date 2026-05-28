.PHONY: lint-heavy test-full clean

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
test-full:
	$(UV) run pytest-fast --address $(PYTEST_FAST_SOCK) --workers 4 --ttl 600

# Remove pyc/__pycache__ and the resident socket (next test-full will spawn a fresh daemon).
clean:
	find . -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null || true
	rm -f $(PYTEST_FAST_SOCK) $(PYTEST_FAST_SOCK).pid $(PYTEST_FAST_SOCK).respawn.lock $(PYTEST_FAST_SOCK).watcher.lock
	rm -f $(PYTEST_FAST_SOCK).staging $(PYTEST_FAST_SOCK).staging.pid
	rm -f $(patsubst %.sock,%-daemon.log,$(PYTEST_FAST_SOCK))
	rm -f $(patsubst %.sock,%-daemon.staging.log,$(PYTEST_FAST_SOCK))
	rm -f $(patsubst %.sock,%-watcher.log,$(PYTEST_FAST_SOCK))
