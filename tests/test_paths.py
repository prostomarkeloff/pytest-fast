"""Tests for derived paths (`_daemon_log_path`), the mtime root (`_project_root`),
and `_max_source_mtime` + `_stale_reason`."""

from __future__ import annotations

import os
import time
from pathlib import Path
from typing import TYPE_CHECKING

from pytest_fast import (
    _daemon_log_path,
    _env_fingerprint,
    _max_source_mtime,
    _project_root,
    _stale_reason,
)

if TYPE_CHECKING:
    import pytest


def test_daemon_log_path_canonical() -> None:
    assert _daemon_log_path("/tmp/pytest-fast-x.sock") == Path("/tmp/pytest-fast-x-daemon.log")


def test_daemon_log_path_staging() -> None:
    """A staging daemon must write to a SEPARATE log (otherwise lines interleave with canonical's)."""
    assert _daemon_log_path("/tmp/pytest-fast-x.sock.staging") == Path(
        "/tmp/pytest-fast-x-daemon.staging.log",
    )


def test_daemon_log_path_no_sock_suffix() -> None:
    """An address without a `.sock` suffix (non-standard usage) is still valid — the
    log name just isn't pretty (but we don't crash)."""
    assert _daemon_log_path("/tmp/weird-address") == Path("/tmp/weird-address-daemon.log")


def test_project_root_default_is_cwd(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.delenv("PYTEST_FAST_ROOT", raising=False)
    monkeypatch.chdir(tmp_path)
    assert _project_root() == tmp_path


def test_project_root_override_via_env(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.setenv("PYTEST_FAST_ROOT", str(tmp_path))
    assert _project_root() == tmp_path.resolve()


def test_max_source_mtime_picks_latest_file(
    monkeypatch: pytest.MonkeyPatch,
    tmp_project: Path,
) -> None:
    """Touching the latest file must move max(mtime) up."""
    monkeypatch.setenv("PYTEST_FAST_ROOT", str(tmp_project))
    baseline = _max_source_mtime()
    assert baseline > 0  # at least one file found

    target = tmp_project / "src" / "foo.py"
    new_mtime = baseline + 5.0
    os.utime(target, (new_mtime, new_mtime))

    later = _max_source_mtime()
    assert later >= new_mtime
    assert later > baseline


def test_max_source_mtime_scans_pyproject(
    monkeypatch: pytest.MonkeyPatch,
    tmp_project: Path,
) -> None:
    """`pyproject.toml` is outside watch-roots dirs but still relevant for collection."""
    monkeypatch.setenv("PYTEST_FAST_ROOT", str(tmp_project))
    target = tmp_project / "pyproject.toml"
    future = time.time() + 10_000.0
    os.utime(target, (future, future))
    assert _max_source_mtime() >= future


def test_stale_reason_none_for_fresh_daemon(
    monkeypatch: pytest.MonkeyPatch,
    tmp_project: Path,
) -> None:
    """Right after boot (boot_mtime == current_mtime, boot_fp == client_fp) — not stale."""
    monkeypatch.setenv("PYTEST_FAST_ROOT", str(tmp_project))
    boot_mtime = _max_source_mtime()
    boot_fp = _env_fingerprint()
    assert _stale_reason(boot_mtime, boot_fp, boot_fp) is None


def test_stale_reason_sources_changed(
    monkeypatch: pytest.MonkeyPatch,
    tmp_project: Path,
) -> None:
    monkeypatch.setenv("PYTEST_FAST_ROOT", str(tmp_project))
    boot_mtime = _max_source_mtime()
    boot_fp = _env_fingerprint()
    # bump mtime forward
    future = boot_mtime + 10.0
    os.utime(tmp_project / "src" / "foo.py", (future, future))
    assert _stale_reason(boot_mtime, boot_fp, boot_fp) == "sources changed"


def test_stale_reason_can_skip_source_scan_but_keeps_env_guard(
    monkeypatch: pytest.MonkeyPatch,
    tmp_project: Path,
) -> None:
    """Immutable-source campaigns avoid the O(tree) hot-path scan, not env validation."""
    monkeypatch.setenv("PYTEST_FAST_ROOT", str(tmp_project))
    boot_mtime = _max_source_mtime()
    boot_fp = _env_fingerprint()
    future = boot_mtime + 10.0
    os.utime(tmp_project / "src" / "foo.py", (future, future))

    assert _stale_reason(boot_mtime, boot_fp, boot_fp, check_sources=False) is None
    assert _stale_reason(boot_mtime, boot_fp, "changed", check_sources=False) == "env changed"


def test_stale_reason_env_changed(
    monkeypatch: pytest.MonkeyPatch,
    tmp_project: Path,
) -> None:
    monkeypatch.setenv("PYTEST_FAST_ROOT", str(tmp_project))
    boot_mtime = _max_source_mtime()
    boot_fp = _env_fingerprint()
    # PYTEST_ADDOPTS is in the explicit fingerprint keys → changing it shifts fp
    monkeypatch.setenv("PYTEST_ADDOPTS", "-v")
    new_fp = _env_fingerprint()
    assert _stale_reason(boot_mtime, boot_fp, new_fp) == "env changed"


def test_stale_reason_legacy_client_skips_env_check(
    monkeypatch: pytest.MonkeyPatch,
    tmp_project: Path,
) -> None:
    """`client_fp=None` (legacy without fingerprint) — env check is skipped, mtime still works."""
    monkeypatch.setenv("PYTEST_FAST_ROOT", str(tmp_project))
    boot_mtime = _max_source_mtime()
    boot_fp = _env_fingerprint()
    # env changed but the client doesn't send fp — no respawn driven by env
    monkeypatch.setenv("PYTEST_ADDOPTS", "-v")
    assert _stale_reason(boot_mtime, boot_fp, None) is None
