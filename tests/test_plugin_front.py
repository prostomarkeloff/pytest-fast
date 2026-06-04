"""Front B — the `pytest -p pytest_fast --fast` plugin.

The controller stays a real pytest session; execution is handed to the resident
warm daemon and its streamed per-phase reports are republished through the
controller's own hooks → fully native reporting (terminalreporter, --durations,
failures, exit code) on top of the warm forkserver engine.

These drive a real `pytest -p pytest_fast --fast` subprocess against a tmp project
and assert the NATIVE pytest output (not pytest-fast's bespoke summary)."""

from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path
from typing import TYPE_CHECKING

import pytest

from pytest_fast import _shutdown_daemon

if TYPE_CHECKING:
    from collections.abc import Iterator


def _make_project(root: Path) -> None:
    (root / "src").mkdir(exist_ok=True)
    (root / "tests").mkdir(exist_ok=True)
    (root / "src" / "foo.py").write_text("def f() -> int:\n    return 1\n")
    (root / "tests" / "test_t.py").write_text(
        "import time\n"
        "def test_slow() -> None:\n    time.sleep(0.20)\n    assert True\n"
        "def test_fast() -> None:\n    assert 1 + 1 == 2\n"
        "def test_fail() -> None:\n    assert 1 == 2, 'boom'\n",
    )
    (root / "pyproject.toml").write_text('[project]\nname = "x"\nversion = "0"\n')


@pytest.fixture
def fast_env(tmp_project: Path, tmp_address: str) -> Iterator[tuple[Path, str]]:
    """A project + daemon address; tears the daemon down afterwards (tmp_address's
    conftest fixture removes the socket/pid artifacts)."""
    _make_project(tmp_project)
    try:
        yield tmp_project, tmp_address
    finally:
        _shutdown_daemon(tmp_address)


def _run(project: Path, address: str, *args: str) -> subprocess.CompletedProcess[str]:
    # No `-p pytest_fast`: the pytest11 entry point auto-loads the plugin, so `--fast` is
    # available out of the box — that's the path we want to exercise.
    env = os.environ.copy()
    env["PYTEST_FAST_ROOT"] = str(project)
    env.pop("_PYTEST_FAST_COLLECT", None)
    return subprocess.run(
        [
            sys.executable,
            "-m",
            "pytest",
            "--fast",
            "--fast-address",
            address,
            "--fast-workers",
            "2",
            "--fast-ttl",
            "20",
            *args,
        ],
        cwd=str(project),
        env=env,
        capture_output=True,
        text=True,
        check=False,
        timeout=120,
    )


def test_fast_native_output_and_exit_code(fast_env: tuple[Path, str]) -> None:
    """`--fast` produces NATIVE pytest output (session header, native --durations table,
    FAILURES, the 'N failed, M passed' line) and the correct non-zero exit code."""
    project, address = fast_env
    proc = _run(project, address, "--durations=5")
    out = proc.stdout + proc.stderr
    assert proc.returncode == 1, f"expected rc=1 (one failing test).\n{out}"
    # native pytest chrome (would be absent in pytest-fast's bespoke CLI summary)
    assert "test session starts" in out
    assert "slowest" in out and "durations" in out, f"native --durations missing:\n{out}"
    assert "call" in out and "test_slow" in out  # per-phase durations from streamed reports
    assert "1 failed, 2 passed" in out, f"native summary line missing:\n{out}"
    assert "test_fail" in out and "boom" in out  # native FAILURES traceback


def test_fast_k_filter_runs_subset(fast_env: tuple[Path, str]) -> None:
    """`-k` selection is forwarded to the daemon: it runs ONLY the selected test (not the
    whole suite), and pytest's native deselection display is correct."""
    project, address = fast_env
    proc = _run(project, address, "-k", "test_fast")
    out = proc.stdout + proc.stderr
    assert proc.returncode == 0, f"-k test_fast selects only a passing test → rc 0.\n{out}"
    assert "1 passed" in out
    assert "deselected" in out, f"native deselection display missing:\n{out}"
    # the failing test must NOT have run (it was deselected)
    assert "test_fail" not in out or "FAILED" not in out


def test_fast_watch_spawns_a_watcher(fast_env: tuple[Path, str]) -> None:
    """`--fast-watch` wires the pre-warm watcher into the plugin path: a watcher process is
    ensured for the daemon (it writes its own log). The watcher's promote mechanics are
    covered by test_watcher.py; here we just confirm the plugin spawns one."""
    import time

    project, address = fast_env
    proc = _run(project, address, "--fast-watch")
    assert proc.returncode == 1, proc.stdout + proc.stderr  # tmp project has a failing test
    watcher_log = Path(address.removesuffix(".sock") + "-watcher.log")
    deadline = time.monotonic() + 5.0
    while time.monotonic() < deadline and not watcher_log.exists():
        time.sleep(0.1)
    assert watcher_log.exists(), "--fast-watch did not spawn a watcher (no watcher log)"


def test_plugin_inert_without_fast_flag(tmp_project: Path) -> None:
    """The auto-loaded plugin (pytest11 entry point) must be INERT without --fast: plain
    `pytest` runs the suite in-process as usual and spawns no daemon."""
    _make_project(tmp_project)
    env = os.environ.copy()
    env["PYTEST_FAST_ROOT"] = str(tmp_project)
    env.pop("_PYTEST_FAST_COLLECT", None)
    proc = subprocess.run(
        [sys.executable, "-m", "pytest", "-q"],  # no --fast, no -p: entry point loads it, inert
        cwd=str(tmp_project),
        env=env,
        capture_output=True,
        text=True,
        check=False,
        timeout=60,
    )
    out = proc.stdout + proc.stderr
    assert proc.returncode == 1, f"plain in-process run should still fail on test_fail.\n{out}"
    assert "1 failed, 2 passed" in out
    assert "starting resident daemon" not in out, "no daemon should be spawned without --fast"
