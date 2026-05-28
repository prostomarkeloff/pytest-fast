"""Tests for the resident `--serve` mode through subprocess.

This is the most interesting chunk of behavior: connecting to the control socket,
status/run/shutdown commands, and stale logic (mtime change → daemon replies
{stale} and exits). The tests drive a **real** daemon subprocess, no in-process
import of `Daemon` — that's the contract the client actually sees.

Notes:
- The `daemon_proc` fixture sets `PYTEST_FAST_ROOT=tmp_project` in BOTH the daemon
  subprocess env AND the test-process env (via monkeypatch). Without that the
  client-side `_env_fingerprint()` wouldn't match the daemon-side and every `run`
  would bounce off into a respawn (the daemon would reply `{stale}`).
- For "pid died" checks we use `proc.wait(timeout)`, NOT `_await_pid_dead(pid, ...)`.
  The daemon is our subprocess child; until we reap the zombie via `wait()`,
  `os.kill(pid, 0)` keeps returning success and `_await_pid_dead` reports False
  even after the process exited.
- Long `tmp_path` paths under `/private/var/folders/...` blow the AF_UNIX limit
  (~104 bytes on macOS); the lib itself works around this via `_short_unix_path`
  (chdir + relative bind/connect). In tests that open a socket manually, we also
  use this helper.
"""

from __future__ import annotations

import os
import socket
import subprocess
from pathlib import Path
from typing import TYPE_CHECKING

import pytest

from pytest_fast import (
    _await_ready,
    _env_fingerprint,
    _recv,
    _send,
    _short_unix_path,
    _shutdown_daemon,
    _status,
    request_run,
)

if TYPE_CHECKING:
    from collections.abc import Iterator


def _spawn(
    pf_cmd: list[str],
    *,
    address: str,
    cwd: Path,
    ttl: float = 30.0,
    workers: int = 2,
) -> subprocess.Popen[bytes]:
    """Spawn a daemon subprocess. stdout/stderr → log file next to the socket
    (for diagnostics if the test fails)."""
    log_path = Path(address.removesuffix(".sock") + "-daemon.log")
    env = os.environ.copy()
    # PYTEST_FAST_ROOT pins the daemon's mtime scan to tmp_project, not the outer
    # pytest-fast's cwd (otherwise the mtime of the outer test files would shift on
    # any chmod and the stale logic would misfire).
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
def daemon_proc(
    tmp_project: Path,
    tmp_address: str,
    pf_cmd: list[str],
    monkeypatch: pytest.MonkeyPatch,
) -> Iterator[subprocess.Popen[bytes]]:
    """Spawn a daemon, guarantee exit in teardown.

    IMPORTANT: along with launching the daemon we monkeypatch `PYTEST_FAST_ROOT`
    in the test process env — so the client-side `_env_fingerprint()` matches the
    daemon-side (otherwise every `run` would bounce off into a respawn)."""
    monkeypatch.setenv("PYTEST_FAST_ROOT", str(tmp_project))
    proc = _spawn(pf_cmd, address=tmp_address, cwd=tmp_project)
    try:
        ok = _await_ready(tmp_address, proc, timeout=30.0)
        if not ok:
            log_path = Path(tmp_address.removesuffix(".sock") + "-daemon.log")
            log_text = log_path.read_text() if log_path.exists() else "<no log>"
            pytest.fail(f"daemon did not become ready in 30s. log:\n{log_text}")
        yield proc
    finally:
        if proc.poll() is None:
            _shutdown_daemon(tmp_address)
            try:
                proc.wait(timeout=10.0)
            except subprocess.TimeoutExpired:
                proc.kill()
                proc.wait(timeout=5.0)


def test_daemon_becomes_ready(daemon_proc: subprocess.Popen[bytes], tmp_address: str) -> None:
    """The fixture already awaited ready=True; status confirms + fresh (because the
    fp matches — the fixture monkeypatches PYTEST_FAST_ROOT in the test process)."""
    st = _status(tmp_address)
    assert st is not None, "daemon dropped out of accept or isn't replying"
    assert st["ready"] is True
    assert st["stale"] is False


def test_status_with_mismatched_fp_returns_stale_true(
    daemon_proc: subprocess.Popen[bytes],
    tmp_address: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Status with client_fp ≠ boot_fp → stale=true (but the daemon does NOT exit
    on status — only on run). Sanity check on env-fingerprint in the status path."""
    # PYTEST_ADDOPTS is in the explicit fingerprint key set, so flipping it shifts fp.
    monkeypatch.setenv("PYTEST_ADDOPTS", "-v")
    new_fp = _env_fingerprint()
    sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    sock.settimeout(2.0)
    # `tmp_address` under /private/var/folders/... may blow the AF_UNIX limit —
    # connect via the same helper the lib uses.
    with _short_unix_path(tmp_address) as connect_path:
        sock.connect(connect_path)
    try:
        _send(sock, ("status", new_fp))
        reply, _ = _recv(sock)
    finally:
        sock.close()
    assert isinstance(reply, dict)
    assert reply["ready"] is True
    assert reply["stale"] is True


def test_run_against_resident_daemon(
    daemon_proc: subprocess.Popen[bytes],
    tmp_address: str,
) -> None:
    """`request_run` must return `{rc, summary}` with a coherent summary (1 failed,
    1 passed in tmp_project)."""
    reply = request_run(tmp_address)
    assert "rc" in reply, f"expected rc/summary, got {reply}"
    assert reply["rc"] == 1  # tmp_project has one failing test
    summary = reply["summary"]
    assert isinstance(summary, str)
    assert "n=2/2" in summary
    assert "1 failed" in summary
    assert "1 passed" in summary


def test_shutdown_terminates_daemon(
    daemon_proc: subprocess.Popen[bytes],
    tmp_address: str,
) -> None:
    """A clean `shutdown` → daemon exits, socket and pidfile removed.
    `proc.wait(timeout)` reaps the zombie — otherwise `os.kill(pid,0)` keeps
    reporting "alive" until the parent calls wait()."""
    _shutdown_daemon(tmp_address)
    rc = daemon_proc.wait(timeout=10.0)
    assert rc == 0
    assert not Path(tmp_address).exists()
    assert not Path(tmp_address + ".pid").exists()


def test_run_then_run_reuses_warm_daemon(
    daemon_proc: subprocess.Popen[bytes],
    tmp_address: str,
) -> None:
    """Two runs back-to-back — both green per contract, the second is warm (label
    contains 'warm'). This is pytest-fast's core value: subsequent runs skip collect."""
    first = request_run(tmp_address)
    assert first.get("rc") == 1, f"first run unexpected reply: {first}"
    second = request_run(tmp_address)
    assert second.get("rc") == 1, f"second run unexpected reply: {second}"
    summary = second["summary"]
    assert isinstance(summary, str)
    assert "warm" in summary


def test_daemon_exits_stale_after_source_change(
    daemon_proc: subprocess.Popen[bytes],
    tmp_project: Path,
    tmp_address: str,
) -> None:
    """Touching `src/foo.py` shifts mtime; the next `run` must receive `{stale: True}`,
    the daemon SELF-exits and releases the socket."""
    # IMPORTANT: filesystem timestamp granularity on some FSes is ~1s, so we force
    # mtime forward via os.utime rather than racing the clock.
    target = tmp_project / "src" / "foo.py"
    future = target.stat().st_mtime + 5.0
    os.utime(target, (future, future))

    reply = request_run(tmp_address)
    assert reply.get("stale") is True, f"expected stale, got: {reply}"
    # daemon closed the socket and exited — reap the zombie via proc.wait
    rc = daemon_proc.wait(timeout=10.0)
    assert rc == 0


def test_daemon_idle_ttl_self_shutdown(
    tmp_project: Path,
    tmp_address: str,
    pf_cmd: list[str],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """With no activity longer than `--ttl` the daemon exits on its own. ttl=2s,
    we wait <10s — `proc.wait` reaps after self-shutdown."""
    monkeypatch.setenv("PYTEST_FAST_ROOT", str(tmp_project))
    proc = _spawn(pf_cmd, address=tmp_address, cwd=tmp_project, ttl=2.0)
    try:
        assert _await_ready(tmp_address, proc, timeout=30.0)
        rc = proc.wait(timeout=10.0)
        assert rc == 0, f"daemon should self-shutdown on idle-ttl=2s, exit={rc}"
        assert not Path(tmp_address).exists()
    finally:
        if proc.poll() is None:
            proc.kill()
            proc.wait(timeout=5.0)
