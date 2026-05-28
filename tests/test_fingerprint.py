"""Tests for `_env_fingerprint`, `_watch_dirs`, `_watch_files` — daemon staleness logic.

The stale check has TWO axes: source mtime and env fingerprint. This file covers
the second one (mtime is in test_paths.py). Both must work together: a change to a
user-configured env-prefix (via `PYTEST_FAST_ENV_PREFIXES`) without editing source
must force a respawn (which we verify here)."""

from __future__ import annotations

from typing import TYPE_CHECKING

from pytest_fast import _env_fingerprint, _watch_dirs, _watch_files

if TYPE_CHECKING:
    import pytest


def test_fingerprint_is_stable_under_repeat_call(monkeypatch: pytest.MonkeyPatch) -> None:
    """Twice in a row — same hex. If it depended on non-env (PID, time, sys.argv) that'd be a bug."""
    monkeypatch.setenv("PYTEST_ADDOPTS", "-v")
    assert _env_fingerprint() == _env_fingerprint()


def test_fingerprint_changes_on_user_prefix_var(monkeypatch: pytest.MonkeyPatch) -> None:
    """User-configured prefix (via PYTEST_FAST_ENV_PREFIXES) → any matching var
    contributes to the fingerprint; a change must shift the fp."""
    monkeypatch.setenv("PYTEST_FAST_ENV_PREFIXES", "MYAPP_")
    monkeypatch.delenv("MYAPP_DB_HOST", raising=False)
    fp_before = _env_fingerprint()
    monkeypatch.setenv("MYAPP_DB_HOST", "127.0.0.1")
    fp_after = _env_fingerprint()
    assert fp_before != fp_after


def test_fingerprint_changes_on_pytest_addopts(monkeypatch: pytest.MonkeyPatch) -> None:
    """PYTEST_ADDOPTS affects collection/run — must be in the fingerprint."""
    monkeypatch.delenv("PYTEST_ADDOPTS", raising=False)
    fp_before = _env_fingerprint()
    monkeypatch.setenv("PYTEST_ADDOPTS", "-v")
    fp_after = _env_fingerprint()
    assert fp_before != fp_after


def test_fingerprint_ignores_irrelevant_var(monkeypatch: pytest.MonkeyPatch) -> None:
    """A random env var (no matching prefix, not in the whitelist) does NOT shift
    the fingerprint — otherwise any mutation in the shell environment between runs
    would force a respawn."""
    monkeypatch.delenv("PYTEST_FAST_ENV_PREFIXES", raising=False)
    fp_before = _env_fingerprint()
    monkeypatch.setenv("RANDOM_UNRELATED_VAR_XYZZY", "anything")
    fp_after = _env_fingerprint()
    assert fp_before == fp_after


def test_fingerprint_changes_on_pytest_fast_root(monkeypatch: pytest.MonkeyPatch) -> None:
    """PYTEST_FAST_ROOT changes the mtime-scan root — critical for the fingerprint
    (otherwise a daemon booted on one root would reply "fresh" to a request from another)."""
    monkeypatch.delenv("PYTEST_FAST_ROOT", raising=False)
    fp_before = _env_fingerprint()
    monkeypatch.setenv("PYTEST_FAST_ROOT", "/tmp/some-root")
    fp_after = _env_fingerprint()
    assert fp_before != fp_after


def test_fingerprint_changes_on_prefix_list_itself(monkeypatch: pytest.MonkeyPatch) -> None:
    """Changing PYTEST_FAST_ENV_PREFIXES itself must respawn — otherwise a daemon
    booted with one prefix-set could miss app-config changes the new caller cares about."""
    monkeypatch.delenv("PYTEST_FAST_ENV_PREFIXES", raising=False)
    fp_before = _env_fingerprint()
    monkeypatch.setenv("PYTEST_FAST_ENV_PREFIXES", "MYAPP_")
    fp_after = _env_fingerprint()
    assert fp_before != fp_after


def test_fingerprint_changes_on_watch_dirs(monkeypatch: pytest.MonkeyPatch) -> None:
    """PYTEST_FAST_WATCH_DIRS determines what's scanned — a change must respawn."""
    monkeypatch.delenv("PYTEST_FAST_WATCH_DIRS", raising=False)
    fp_before = _env_fingerprint()
    monkeypatch.setenv("PYTEST_FAST_WATCH_DIRS", "mypkg,tests")
    fp_after = _env_fingerprint()
    assert fp_before != fp_after


def test_fingerprint_changes_on_watch_files(monkeypatch: pytest.MonkeyPatch) -> None:
    """PYTEST_FAST_WATCH_FILES determines which config files trigger respawn — a change must respawn."""
    monkeypatch.delenv("PYTEST_FAST_WATCH_FILES", raising=False)
    fp_before = _env_fingerprint()
    monkeypatch.setenv("PYTEST_FAST_WATCH_FILES", "setup.cfg,tox.ini")
    fp_after = _env_fingerprint()
    assert fp_before != fp_after


# ── _watch_dirs ──────────────────────────────────────────────────────────────


def test_watch_dirs_default_is_src_and_tests(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("PYTEST_FAST_WATCH_DIRS", raising=False)
    assert _watch_dirs() == ["src", "tests"]


def test_watch_dirs_env_replaces_default_comma(monkeypatch: pytest.MonkeyPatch) -> None:
    """REPLACE semantics: env var fully replaces the default (does not add to it)."""
    monkeypatch.setenv("PYTEST_FAST_WATCH_DIRS", "mypkg,integration")
    assert _watch_dirs() == ["mypkg", "integration"]


def test_watch_dirs_env_replaces_default_colon(monkeypatch: pytest.MonkeyPatch) -> None:
    """`:` separator is supported alongside `,` (PATH-style)."""
    monkeypatch.setenv("PYTEST_FAST_WATCH_DIRS", "mypkg:integration")
    assert _watch_dirs() == ["mypkg", "integration"]


def test_watch_dirs_empty_env_yields_empty_list(monkeypatch: pytest.MonkeyPatch) -> None:
    """An explicit empty value scans nothing — occasionally useful for tooling."""
    monkeypatch.setenv("PYTEST_FAST_WATCH_DIRS", "")
    assert _watch_dirs() == []


# ── _watch_files ─────────────────────────────────────────────────────────────


def test_watch_files_default_is_pyproject_and_pytest_ini(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("PYTEST_FAST_WATCH_FILES", raising=False)
    assert _watch_files() == ["pyproject.toml", "pytest.ini"]


def test_watch_files_env_replaces_default(monkeypatch: pytest.MonkeyPatch) -> None:
    """REPLACE semantics: e.g. tox-using project sets `tox.ini,setup.cfg`."""
    monkeypatch.setenv("PYTEST_FAST_WATCH_FILES", "tox.ini,setup.cfg,conftest.py")
    assert _watch_files() == ["tox.ini", "setup.cfg", "conftest.py"]


def test_watch_files_empty_env_yields_empty_list(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("PYTEST_FAST_WATCH_FILES", "")
    assert _watch_files() == []
