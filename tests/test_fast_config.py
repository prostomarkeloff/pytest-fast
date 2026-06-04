"""Config resolution (workers / ttl / address, with env-var precedence) and the `--fast`
address→rootdir leak fix.

The leak: pytest determines rootdir/inifile from the raw argv BEFORE any plugin loads, keeping
any non-`-` arg that `.exists()`. So `--fast-address /tmp/x.sock` (space form) — once the socket
file exists (warm daemon) — makes pytest root at the socket's dir, losing `pythonpath`/inifile.
`PYTEST_FAST_ADDRESS` (and the `=` form) keep the path out of that scan. We verify the env-var
path respects `pytest.ini` even with a daemon up and the socket outside the project.
"""

from __future__ import annotations

import os
import subprocess
import sys
import time
from pathlib import Path
from typing import TYPE_CHECKING

import pytest

from pytest_fast import (
    _default_fast_address,
    _default_workers,
    _resolve_fast_address,
    _resolve_ttl,
    _resolve_workers,
    _status,
)

if TYPE_CHECKING:
    from collections.abc import Iterator


# ── resolution precedence (pure, fast) ───────────────────────────────────────


def test_default_workers_is_sane() -> None:
    """Auto-detect returns a positive count no larger than the logical CPU count (on Apple
    Silicon it's the performance-core count, e.g. 6 of 12)."""
    n = _default_workers()
    assert 1 <= n <= (os.cpu_count() or 1)


def test_resolve_workers_precedence(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("PYTEST_FAST_WORKERS", raising=False)
    assert _resolve_workers(3) == 3  # explicit wins
    assert _resolve_workers(None) == _default_workers()  # nothing → auto
    monkeypatch.setenv("PYTEST_FAST_WORKERS", "5")
    assert _resolve_workers(None) == 5  # env next
    assert _resolve_workers(2) == 2  # explicit still beats env
    monkeypatch.setenv("PYTEST_FAST_WORKERS", "garbage")
    assert _resolve_workers(None) == _default_workers()  # unparseable env → auto


def test_resolve_ttl_precedence(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("PYTEST_FAST_TTL", raising=False)
    assert _resolve_ttl(30.0) == 30.0
    assert _resolve_ttl(None) == 600.0
    monkeypatch.setenv("PYTEST_FAST_TTL", "45")
    assert _resolve_ttl(None) == 45.0
    assert _resolve_ttl(10.0) == 10.0  # explicit beats env
    monkeypatch.setenv("PYTEST_FAST_TTL", "nope")
    assert _resolve_ttl(None) == 600.0  # unparseable → default


def test_resolve_address_precedence(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("PYTEST_FAST_ADDRESS", raising=False)
    assert _resolve_fast_address("/opt.sock") == "/opt.sock"  # option wins
    assert _resolve_fast_address(None) == _default_fast_address()  # nothing → derived
    monkeypatch.setenv("PYTEST_FAST_ADDRESS", "/env.sock")
    assert _resolve_fast_address(None) == "/env.sock"  # env next
    assert _resolve_fast_address("/opt.sock") == "/opt.sock"  # option still beats env


# ── the rootdir-leak fix, end to end ─────────────────────────────────────────


@pytest.fixture
def leak_project(tmp_path: Path) -> Iterator[tuple[Path, str]]:
    """A `pythonpath = src` project whose conftest imports from src, with a daemon socket placed
    OUTSIDE the project (in tmp_path's parent-ish) — the exact shape that triggered the leak.
    Yields (project_dir, socket_path) and shuts the daemon down on teardown."""
    proj = tmp_path / "proj"
    (proj / "src" / "mypkg").mkdir(parents=True)
    (proj / "tests").mkdir()
    (proj / "src" / "mypkg" / "__init__.py").write_text("VALUE = 42\n")
    (proj / "tests" / "conftest.py").write_text("import mypkg  # only importable via pythonpath=src\n")
    (proj / "tests" / "test_t.py").write_text("import mypkg\ndef test_v() -> None:\n    assert mypkg.VALUE == 42\n")
    (proj / "pytest.ini").write_text("[pytest]\npythonpath = src\ntestpaths = tests\n")
    (proj / "pyproject.toml").write_text('[project]\nname = "rl"\nversion = "0"\n')
    # socket OUTSIDE the project dir
    sock = str(tmp_path / "ext.sock")
    yield proj, sock
    from pytest_fast import _shutdown_daemon

    _shutdown_daemon(sock)


def _boot_daemon(project: Path, sock: str) -> subprocess.Popen[bytes]:
    env = os.environ.copy()
    env["PYTEST_FAST_ROOT"] = str(project)
    env.pop("_PYTEST_FAST_COLLECT", None)
    log = (project / "daemon.log").open("w")
    return subprocess.Popen(
        [sys.executable, "-m", "pytest_fast", "--serve", "--address", sock, "--ttl", "30", "--workers", "2"],
        stdout=log,
        stderr=subprocess.STDOUT,
        cwd=str(project),
        env=env,
        start_new_session=True,
    )


def test_fast_respects_pytest_ini_via_env_address(leak_project: tuple[Path, str]) -> None:
    """With a daemon already up and the socket OUTSIDE the project, `PYTEST_FAST_ADDRESS` keeps
    the socket path out of pytest's rootdir scan → `pytest.ini` (and its `pythonpath = src`) is
    respected → the conftest's `import mypkg` works and the test passes."""
    project, sock = leak_project
    proc = _boot_daemon(project, sock)
    deadline = time.monotonic() + 30.0
    while time.monotonic() < deadline and _status(sock) is None:
        if proc.poll() is not None:
            pytest.fail(f"daemon died: {(project / 'daemon.log').read_text()}")
        time.sleep(0.2)
    assert _status(sock) is not None, "daemon never became ready"

    env = os.environ.copy()
    env["PYTEST_FAST_ROOT"] = str(project)
    env["PYTEST_FAST_ADDRESS"] = sock  # ← the fix: address via env, not a bare argv path
    env.pop("_PYTEST_FAST_COLLECT", None)
    run = subprocess.run(
        [sys.executable, "-m", "pytest", "--fast", "-q"],
        cwd=str(project),
        env=env,
        capture_output=True,
        text=True,
        check=False,
        timeout=120,
    )
    out = run.stdout + run.stderr
    assert "No module named" not in out, f"rootdir leak — pythonpath not applied:\n{out}"
    assert run.returncode == 0, f"expected the suite to pass via --fast.\n{out}"
    assert "1 passed" in out
