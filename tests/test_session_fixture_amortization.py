"""Characterization: a session-scoped fixture is set up ONCE PER RUN, not once per daemon.

The forkserver amortizes *collect* across runs, but each run (`--runs`, or each `request_run_streamed`
in serve mode) forks fresh workers that `os._exit(0)` when the run ends — so any session-scoped
fixture set up during the run is torn down with the worker, and the next run re-pays its setup.

For a client that issues MANY small runs (e.g. a mutation tester: one run per mutant), an expensive
session-scoped fixture (a DB engine + schema seed, ~hundreds of ms) is re-set-up on every run and
dominates wall-clock. This test pins the current behavior — N runs ⇒ N setups — as the baseline the
persistent-worker experiment aims to amortize down to 1.
"""

from __future__ import annotations

import os
import subprocess
import sys
import textwrap
from pathlib import Path

import pytest

from pytest_fast import _await_ready, _shutdown_daemon, request_run


def _make_project(root: Path, counter: Path) -> None:
    """Project whose session-scoped fixture appends one line to `counter` every time it is set up."""
    (root / "tests").mkdir(parents=True)
    (root / "pyproject.toml").write_text(
        '[project]\nname = "sfa"\nversion = "0"\n\n[tool.pytest.ini_options]\ntestpaths = ["tests"]\n'
    )
    (root / "conftest.py").write_text(
        textwrap.dedent(f"""
        import pytest
        _COUNTER = {str(counter)!r}

        @pytest.fixture(scope="session")
        def expensive_session_resource():
            # Stands in for a DB engine + schema seed: set up ONCE per pytest session. Each fork
            # is a fresh session, so this fires once per worker — i.e. once per run.
            with open(_COUNTER, "a") as fh:
                fh.write("setup\\n")
            yield object()
        """)
    )
    (root / "tests" / "test_uses_resource.py").write_text(
        textwrap.dedent("""
        def test_a(expensive_session_resource):
            assert expensive_session_resource is not None

        def test_b(expensive_session_resource):
            assert expensive_session_resource is not None
        """)
    )


def _run(project: Path, *args: str, workers: int = 1) -> subprocess.CompletedProcess[str]:
    env = os.environ.copy()
    env["PYTEST_FAST_ROOT"] = str(project)
    env.pop("_PYTEST_FAST_COLLECT", None)
    return subprocess.run(
        [sys.executable, "-m", "pytest_fast", "--workers", str(workers), *args],
        cwd=str(project),
        env=env,
        capture_output=True,
        text=True,
        check=False,
        timeout=120,
    )


def test_persist_multiworker_reruns_items_correctly(tmp_path: Path) -> None:
    """Regression: a warm worker must correctly RE-RUN items across runs. With >1 worker, work-stealing
    can hand one worker the same item repeatedly, and the run-boundary teardown must use a nextitem that
    DIFFERS from the item — else pytest keeps its function-scoped fixtures and the re-run can't re-inject
    them (KeyError). All runs must pass; the session fixture is set up once per worker that ran an item."""
    project, counter = tmp_path / "proj", tmp_path / "setups.txt"
    project.mkdir()
    _make_project(project, counter)

    proc = _run(project, "--runs", "3", "--persist-workers", workers=2)
    assert proc.returncode == 0, proc.stdout + proc.stderr  # no re-run KeyError / fixture breakage

    setups = counter.read_text().count("setup") if counter.exists() else 0
    assert 1 <= setups <= 2, f"expected one session-fixture setup per active worker (≤2), got {setups}"


def test_session_fixture_reset_up_once_per_run(tmp_path: Path) -> None:
    """Baseline: with a single warm daemon and 3 runs, the session fixture is set up 3 times —
    once per run — because each run forks a fresh worker that tears it down on exit. This is the
    per-run re-setup cost the persistent-worker experiment targets."""
    project, counter = tmp_path / "proj", tmp_path / "setups.txt"
    project.mkdir()
    _make_project(project, counter)

    proc = _run(project, "--runs", "3")
    assert proc.returncode == 0, proc.stdout + proc.stderr

    setups = counter.read_text().count("setup") if counter.exists() else 0
    assert setups == 3, f"expected one session-fixture setup per run (3), got {setups}"


def test_persist_workers_amortizes_session_fixture(tmp_path: Path) -> None:
    """Opt-in `--persist-workers`: the N runs reuse one warm worker whose pytest session spans them,
    so a session-scoped fixture is set up ONCE across all runs (not once per run). This is the whole
    point — amortize the expensive session setup that the default per-run fork re-pays every time."""
    project, counter = tmp_path / "proj", tmp_path / "setups.txt"
    project.mkdir()
    _make_project(project, counter)

    proc = _run(project, "--runs", "3", "--persist-workers")
    assert proc.returncode == 0, proc.stdout + proc.stderr

    setups = counter.read_text().count("setup") if counter.exists() else 0
    assert setups == 1, f"--persist-workers should set up the session fixture once, got {setups}"


def _spawn_daemon(project: Path, address: str, *extra: str) -> subprocess.Popen[bytes]:
    log = Path(address.removesuffix(".sock") + "-daemon.log")
    env = os.environ.copy()
    env["PYTEST_FAST_ROOT"] = str(project)
    with log.open("w") as fh:
        return subprocess.Popen(
            [
                sys.executable,
                "-m",
                "pytest_fast",
                "--serve",
                "--address",
                address,
                "--ttl",
                "30",
                "--workers",
                "1",
                *extra,
            ],
            stdout=fh,
            stderr=subprocess.STDOUT,
            cwd=str(project),
            env=env,
            start_new_session=True,
        )


def test_persist_serve_amortizes_across_run_requests(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Serve mode is what the many-small-runs client actually uses: a resident daemon answering a
    stream of `run` requests. With `--persist-workers`, the daemon holds a warm worker pool across
    requests, so a session-scoped fixture is set up ONCE — not once per request."""
    project, counter = tmp_path / "proj", tmp_path / "setups.txt"
    project.mkdir()
    _make_project(project, counter)
    address = str(tmp_path / "pf.sock")
    monkeypatch.setenv("PYTEST_FAST_ROOT", str(project))  # client env fingerprint must match the daemon's
    proc = _spawn_daemon(project, address, "--persist-workers")
    try:
        assert _await_ready(address, proc, timeout=30.0), "daemon did not become ready"
        first = request_run(address)
        second = request_run(address)
        third = request_run(address)
        assert first.get("rc") == 0, first
        assert second.get("rc") == 0, second
        assert third.get("rc") == 0, third
        setups = counter.read_text().count("setup") if counter.exists() else 0
        assert setups == 1, (
            f"--persist-workers (serve) should set up the session fixture once across runs, got {setups}"
        )
    finally:
        _stop(proc, address)


def _stop(proc: subprocess.Popen[bytes], address: str) -> None:
    if proc.poll() is None:
        _shutdown_daemon(address)
        try:
            proc.wait(timeout=10.0)
        except subprocess.TimeoutExpired:
            proc.kill()
            proc.wait(timeout=5.0)


def test_persist_serve_verdicts_correct_and_amortized(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Correctness, not just setup-count: the warm pool must produce the SAME verdicts as the default
    per-run path. Every item runs with the right outcome (the anchor/flush protocol must not skip or
    double-run items), a failure propagates to rc, and it's stable across warm requests — while the
    session-scoped fixture is set up ONCE."""
    project, counter = tmp_path / "proj", tmp_path / "setups.txt"
    (project / "tests").mkdir(parents=True)
    (project / "pyproject.toml").write_text(
        '[project]\nname = "vc"\nversion = "0"\n\n[tool.pytest.ini_options]\ntestpaths = ["tests"]\n'
    )
    (project / "conftest.py").write_text(
        textwrap.dedent(f"""
        import pytest
        _COUNTER = {str(counter)!r}

        @pytest.fixture(scope="session")
        def res():
            with open(_COUNTER, "a") as fh:
                fh.write("setup\\n")
            yield object()
        """)
    )
    (project / "tests" / "test_v.py").write_text(
        textwrap.dedent("""
        def test_ok(res):
            assert 1 + 1 == 2

        def test_bad(res):
            assert 1 + 1 == 3
        """)
    )
    address = str(tmp_path / "pf.sock")
    monkeypatch.setenv("PYTEST_FAST_ROOT", str(project))
    proc = _spawn_daemon(project, address, "--persist-workers")
    try:
        assert _await_ready(address, proc, timeout=30.0), "daemon did not become ready"
        for _ in range(2):  # verdicts must be stable across warm requests
            reply = request_run(address)
            assert reply.get("rc") == 1, reply  # the failing test propagates to rc
            summary = reply["summary"]
            assert isinstance(summary, str)
            assert "n=2/2" in summary, summary  # BOTH items ran (no skip / double-run)
            assert "1 failed" in summary and "1 passed" in summary, summary  # correct per-item outcomes
        assert counter.read_text().count("setup") == 1, "session fixture must be set up once across warm runs"
    finally:
        _stop(proc, address)
