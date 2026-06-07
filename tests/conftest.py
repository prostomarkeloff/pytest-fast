"""Shared fixtures for pytest-fast tests.

Tests are run through pytest-fast itself (`pytest-fast --address X` against this
directory) — dogfooding. Because of that the worker of the outer pytest-fast
harness already has `_PYTEST_FAST_COLLECT=1` in its env (set by the outer
`Daemon.__init__`). That doesn't get in the way: all tests that spawn pytest-fast
subprocesses go through `python -m pytest_fast` and `_spawn_daemon` scrubs the
flag via `_subprocess_env()` before passing env to the child. Tests that mutate
`os.environ` directly use `monkeypatch.setenv/delenv` — it reverts changes.
"""

from __future__ import annotations

import os
import sys
from pathlib import Path
from typing import TYPE_CHECKING

import pytest

if TYPE_CHECKING:
    from collections.abc import Iterator


# ── Hypothesis profiles (for the fuzz/stress layer) ──────────────────────────
#
# Determinism is a project value (see the dogfood / non-idempotent-rerun notes): the
# `ci` profile `derandomize`s so a CI run is reproducible from the source alone, and
# `deadline=None` kills time-based flakiness (socket/daemon round-trips vary on loaded
# runners). `HYPOTHESIS_PROFILE` selects (default `dev`); the fuzz CI job sets `ci`.
# Guarded import: the base suite (which skips the fuzz/stress markers) must stay
# collectable even on a minimal install without hypothesis.
try:
    from hypothesis import HealthCheck, settings
except ImportError:
    pass
else:
    settings.register_profile("dev", max_examples=75, deadline=None)
    settings.register_profile(
        "ci",
        max_examples=300,
        derandomize=True,
        deadline=None,
        suppress_health_check=[HealthCheck.too_slow, HealthCheck.function_scoped_fixture],
    )
    settings.load_profile(os.environ.get("HYPOTHESIS_PROFILE", "dev"))


@pytest.fixture
def tmp_address(tmp_path: Path) -> Iterator[str]:
    """Per-test unique UNIX socket for the daemon. Long `tmp_path` paths on macOS
    blow the `sockaddr_un.sun_path` limit (~104 bytes); pytest-fast itself works
    around this via the `_short_unix_path` chdir helper — the fixture uses the
    idiomatic pytest tmpdir and incidentally exercises that workaround.

    Cleans up all derived artifacts (`.pid`, `.respawn.lock`, `.watcher.lock`,
    `-daemon.log`, staging variants) in case the test was killed/failed and the
    daemon's `finally` block didn't run."""
    address = str(tmp_path / "pf.sock")
    yield address
    base = address.removesuffix(".sock")
    for tail in (
        ".sock",
        ".sock.pid",
        ".sock.respawn.lock",
        ".sock.watcher.lock",
        ".sock.staging",
        ".sock.staging.pid",
    ):
        Path(base + tail).unlink(missing_ok=True)
    for tail in ("-daemon.log", "-daemon.staging.log", "-watcher.log"):
        Path(base + tail).unlink(missing_ok=True)


@pytest.fixture
def tmp_project(tmp_path: Path) -> Path:
    """Minimal "project": `src/foo.py`, `tests/test_t.py` (1 pass + 1 fail),
    `pyproject.toml`. Used for:
      * verifying `_max_source_mtime` (it has both `src/`, `tests/`, and `pyproject.toml`);
      * end-to-end runs via `python -m pytest_fast --runs 1` in a subprocess."""
    (tmp_path / "src").mkdir()
    (tmp_path / "tests").mkdir()
    (tmp_path / "src" / "foo.py").write_text("def hello() -> int:\n    return 1\n")
    (tmp_path / "tests" / "test_t.py").write_text(
        "def test_pass() -> None:\n    assert 1 + 1 == 2\n\ndef test_fail() -> None:\n    assert 1 + 1 == 3\n",
    )
    (tmp_path / "pyproject.toml").write_text('[project]\nname = "tmp"\nversion = "0"\n')
    return tmp_path


@pytest.fixture
def pf_cmd() -> list[str]:
    """Command for self-invocation of pytest-fast via `python -m pytest_fast`.
    Anchored on `sys.executable` — it points to the same venv where pytest-fast is
    installed (the one we're running under)."""
    return [sys.executable, "-m", "pytest_fast"]
