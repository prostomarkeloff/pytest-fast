"""Tests for `_env_fingerprint` and `_watch_roots` — daemon staleness logic.

The stale check has TWO axes: source mtime and env fingerprint. This file covers
the second one (mtime is in test_paths.py). Both must work together: a change to a
user-configured env-prefix (via `PYTEST_FAST_ENV_PREFIXES`) without editing source
must force a respawn (which we verify here)."""

from __future__ import annotations

from typing import TYPE_CHECKING

from pytest_fast import _env_fingerprint, _watch_roots

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


def test_watch_roots_default_includes_src_and_tests(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("PYTEST_FAST_WATCH", raising=False)
    roots = _watch_roots()
    assert "src" in roots
    assert "tests" in roots


def test_watch_roots_extras_via_env_comma(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("PYTEST_FAST_WATCH", "engine,shim")
    roots = _watch_roots()
    assert "engine" in roots
    assert "shim" in roots


def test_watch_roots_extras_via_env_colon(monkeypatch: pytest.MonkeyPatch) -> None:
    """`:` separator is supported alongside `,` (PATH-style)."""
    monkeypatch.setenv("PYTEST_FAST_WATCH", "engine:shim")
    roots = _watch_roots()
    assert "engine" in roots
    assert "shim" in roots


def test_watch_roots_no_duplicates_against_defaults(monkeypatch: pytest.MonkeyPatch) -> None:
    """If PYTEST_FAST_WATCH repeats `src`, we don't duplicate (the mtime scan loop
    would enter the same dir twice, semantically pointless)."""
    monkeypatch.setenv("PYTEST_FAST_WATCH", "src,tests,engine")
    roots = _watch_roots()
    assert roots.count("src") == 1
    assert roots.count("tests") == 1
    assert "engine" in roots
