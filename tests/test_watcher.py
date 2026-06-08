"""Tests for the resident `--watch` watcher and staging-promote.

The watcher is the trickiest part: debounce + flock single-instance + staging-spawn
+ soft-shutdown of the old daemon + promote over the socket. These tests bring up
a real daemon and a real watcher (as subprocesses) and verify end-to-end:
  * source-change → new pid on the canonical address (promote worked)
  * broken conftest (`pytest_configure` raises) → original pid stays, watcher logs «did not collect»
  * second-watcher-instance → flock conflict → exits without doing work

Two lib details these tests exercise:
  1. `_spawn_watcher` accepts `cwd=` → the watcher (and via it the staging daemons
     it spawns) collects `tmp_project`, not the pytest-fast project root (under which
     the outer pytest-fast harness lives — otherwise: recursion).
  2. `_staging_promote` uses `_await_socket_gone(address, ...)` instead of
     `_await_pid_dead(pid, ...)` — the latter on a zombie child of test worker
     returned False until an explicit reap and blocked promote on timeout.
"""

from __future__ import annotations

import os
import subprocess
import time
from pathlib import Path
from typing import TYPE_CHECKING

import pytest

from pytest_fast import (
    _await_ready,
    _read_pid,
    _shutdown_daemon,
    _spawn_watcher,
)

if TYPE_CHECKING:
    from collections.abc import Iterator


@pytest.fixture(autouse=True)
def _fast_watch(monkeypatch: pytest.MonkeyPatch) -> None:
    """Drop the watcher poll/debounce to ~0.05s for the whole module — the watcher subprocesses
    (and the staging daemons they spawn) inherit this env, so these end-to-end tests promote in
    ~0.3s instead of ~2.7s. Autouse + function scope → set before the `alive_daemon` fixture spawns,
    so the daemon also boots with it."""
    monkeypatch.setenv("PYTEST_FAST_WATCH_POLL", "0.05")
    monkeypatch.setenv("PYTEST_FAST_WATCH_DEBOUNCE", "0.05")


def _wait_for_log(path: Path, substr: str, timeout: float = 15.0) -> bool:
    """Poll a log file until it contains `substr` (or timeout) — replaces fixed sleeps with a wait
    on the actual event, so the test returns the instant the watcher reaches the expected state."""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if path.exists() and substr in path.read_text():
            return True
        time.sleep(0.03)
    return False


def _spawn_daemon_proc(
    pf_cmd: list[str],
    *,
    address: str,
    cwd: Path,
    ttl: float = 60.0,
    workers: int = 2,
) -> subprocess.Popen[bytes]:
    log_path = Path(address.removesuffix(".sock") + "-daemon.log")
    env = os.environ.copy()
    env["PYTEST_FAST_ROOT"] = str(cwd)
    cmd = [
        *pf_cmd,
        "--serve",
        "--address",
        address,
        "--ttl",
        str(ttl),
        "--workers",
        str(workers),
    ]
    with log_path.open("w") as f:
        return subprocess.Popen(
            cmd,
            stdout=f,
            stderr=subprocess.STDOUT,
            cwd=cwd,
            env=env,
            start_new_session=True,
        )


@pytest.fixture
def alive_daemon(
    tmp_project: Path,
    tmp_address: str,
    pf_cmd: list[str],
    monkeypatch: pytest.MonkeyPatch,
) -> Iterator[subprocess.Popen[bytes]]:
    monkeypatch.setenv("PYTEST_FAST_ROOT", str(tmp_project))
    proc = _spawn_daemon_proc(pf_cmd, address=tmp_address, cwd=tmp_project)
    try:
        if not _await_ready(tmp_address, proc, timeout=30.0):
            log = Path(tmp_address.removesuffix(".sock") + "-daemon.log")
            pytest.fail(f"daemon failed to boot. log:\n{log.read_text() if log.exists() else '<none>'}")
        yield proc
    finally:
        if proc.poll() is None:
            _shutdown_daemon(tmp_address)
            try:
                proc.wait(timeout=10.0)
            except subprocess.TimeoutExpired:
                proc.kill()
                proc.wait(timeout=5.0)


def _wait_for_new_pid(address: str, original_pid: int, timeout: float) -> int | None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        pid = _read_pid(address)
        if pid is not None and pid != original_pid:
            return pid
        time.sleep(0.03)
    return None


def _dump_diag(tmp_address: str) -> str:
    """Read both logs (watcher + canonical daemon + staging daemon, if present) and
    fold them into one block — for meaningful `pytest.fail` diagnostics."""
    base = tmp_address.removesuffix(".sock")
    parts = []
    for tail in ("-watcher.log", "-daemon.log", "-daemon.staging.log"):
        p = Path(base + tail)
        if p.exists():
            parts.append(f"=== {p.name} ===\n{p.read_text()}")
    return "\n\n".join(parts) or "<no logs>"


def test_watcher_single_instance_via_flock(
    alive_daemon: subprocess.Popen[bytes],
    tmp_project: Path,
    tmp_address: str,
) -> None:
    """A second `_spawn_watcher` must immediately see the foreign lock and exit
    quietly. The second watcher's log must contain «already holds the lock — exiting»."""
    log = Path(tmp_address.removesuffix(".sock") + "-watcher.log")
    _spawn_watcher(workers=2, start_method="forkserver", address=tmp_address, ttl=10.0, cwd=str(tmp_project))
    assert _wait_for_log(log, "pre-warming"), "first watcher never entered its poll loop"

    _spawn_watcher(workers=2, start_method="forkserver", address=tmp_address, ttl=10.0, cwd=str(tmp_project))
    assert _wait_for_log(log, "already holds the lock"), (
        f"second watcher should have exited on flock conflict. log:\n{log.read_text() if log.exists() else '<none>'}"
    )


def test_watcher_promotes_on_source_change(
    alive_daemon: subprocess.Popen[bytes],
    tmp_project: Path,
    tmp_address: str,
) -> None:
    """End-to-end: edit src/foo.py → watcher → staging-promote → new pid on canonical."""
    original_pid = _read_pid(tmp_address)
    assert original_pid is not None, "daemon pidfile must exist"

    watcher_log = Path(tmp_address.removesuffix(".sock") + "-watcher.log")
    _spawn_watcher(workers=2, start_method="forkserver", address=tmp_address, ttl=15.0, cwd=str(tmp_project))
    assert _wait_for_log(watcher_log, "pre-warming"), "watcher never entered its poll loop"

    # Shift mtime forward — wake the watcher
    target = tmp_project / "src" / "foo.py"
    future = target.stat().st_mtime + 10.0
    os.utime(target, (future, future))

    # Watcher: poll + debounce (tiny under _fast_watch) + staging boot + canonical shutdown +
    # socket-gone wait + promote. ~0.5s here; 30s of headroom for a loaded CI runner.
    new_pid = _wait_for_new_pid(tmp_address, original_pid, timeout=30.0)
    if new_pid is None:
        pytest.fail(f"watcher didn't promote.\n{_dump_diag(tmp_address)}")
    assert new_pid != original_pid

    text = watcher_log.read_text()
    assert "source change settled" in text
    assert "promoted fresh warm daemon" in text, f"missing 'promoted' line. log:\n{text}"


def test_watcher_skips_promote_on_broken_collect(
    alive_daemon: subprocess.Popen[bytes],
    tmp_project: Path,
    tmp_address: str,
) -> None:
    """A broken conftest (`pytest_configure` raises) → staging collect fails on the
    preload import → staging process dies → watcher logs «did not collect» and
    keeps the canonical pid unchanged."""
    # `pytest_configure` is registered via `call_historic` inside `_collect()`. When
    # tests/conftest.py loads during `pytest_collection`, the historic replay fires
    # our `pytest_configure(config)` → it raises RuntimeError → propagates out of
    # `_collect()` → forkserver preload import fails → forkserver process dies →
    # the daemon main process dies too (boot.start raises ConnectionError).
    broken_conftest = tmp_project / "tests" / "conftest.py"
    broken_conftest.write_text(
        "def pytest_configure(config):\n    raise RuntimeError('intentional broken conftest for pytest-fast test')\n",
    )

    original_pid = _read_pid(tmp_address)
    assert original_pid is not None

    watcher_log = Path(tmp_address.removesuffix(".sock") + "-watcher.log")
    _spawn_watcher(workers=2, start_method="forkserver", address=tmp_address, ttl=15.0, cwd=str(tmp_project))
    assert _wait_for_log(watcher_log, "pre-warming"), "watcher never entered its poll loop"

    # Touch something so the watcher notices mtime change and tries to promote
    target = tmp_project / "src" / "foo.py"
    future = target.stat().st_mtime + 10.0
    os.utime(target, (future, future))

    # Wait for «did not collect» (max staging boot timeout is 90s, but a broken edit fails
    # immediately via `proc.poll()` in `_await_ready` — really <1s).
    if not _wait_for_log(watcher_log, "did not collect", timeout=30.0):
        pytest.fail(f"watcher didn't log a failure for the broken conftest.\n{_dump_diag(tmp_address)}")

    # Canonical pid must not change (the old daemon stays alive, staging failed)
    current_pid = _read_pid(tmp_address)
    assert current_pid == original_pid, (
        f"original pid={original_pid} should have stayed, current pid={current_pid}.\n{_dump_diag(tmp_address)}"
    )
