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
from typing import cast

import pytest

from pytest_fast import (
    RunResult,
    _await_ready,
    _shutdown_daemon,
    request_run,
    request_run_results,
    request_run_streamed,
)


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


def _spawn_daemon(project: Path, address: str, *extra: str, workers: int = 1) -> subprocess.Popen[bytes]:
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
                str(workers),
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


def test_persist_lean_results_skip_report_serialization_and_keep_phase_timings(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A lean persistent request keeps exact phase timings but does not build native wire reports."""
    project, counter = tmp_path / "proj", tmp_path / "setups.txt"
    serialized = tmp_path / "serialized-reports.txt"
    project.mkdir()
    _make_project(project, counter)
    conftest = project / "conftest.py"
    conftest.write_text(
        conftest.read_text()
        + textwrap.dedent(f"""

        @pytest.hookimpl(tryfirst=True)
        def pytest_report_to_serializable(config, report):
            with open({str(serialized)!r}, "a") as stream:
                stream.write(f"{{report.nodeid}}:{{report.when}}\\n")
            return None
        """)
    )
    address = str(tmp_path / "pf.sock")
    monkeypatch.setenv("PYTEST_FAST_ROOT", str(project))
    proc = _spawn_daemon(project, address, "--persist-workers")
    selected = ["tests/test_uses_resource.py::test_a"]
    try:
        assert _await_ready(address, proc, timeout=30.0), "daemon did not become ready"
        lean = request_run(address, nodeids=selected, want_results=True)
        assert lean.get("rc") == 0, lean
        results = cast("list[dict[str, object]]", lean.get("results"))
        assert len(results) == 1
        phases = cast("dict[str, float]", results[0]["phases"])
        assert set(phases) == {"setup", "call", "teardown"}
        assert all(duration >= 0 for duration in phases.values())
        assert not serialized.exists(), "lean request must not invoke pytest_report_to_serializable"

        reports: list[dict[str, object]] = []
        native = request_run_streamed(address, reports.append, nodeids=selected)
        assert native.get("rc") == 0, native
        assert reports
        assert serialized.read_text().splitlines() == [
            f"{selected[0]}:setup",
            f"{selected[0]}:call",
            f"{selected[0]}:teardown",
        ]
    finally:
        _stop(proc, address)


def test_fresh_workers_reuses_collection_but_restarts_the_pytest_session(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Fresh requests retire the persistent pool and fork a new pytest session per request."""
    project, counter = tmp_path / "proj", tmp_path / "setups.txt"
    project.mkdir()
    _make_project(project, counter)
    address = str(tmp_path / "pf.sock")
    monkeypatch.setenv("PYTEST_FAST_ROOT", str(project))
    proc = _spawn_daemon(project, address, "--persist-workers")
    selected = ["tests/test_uses_resource.py::test_a"]
    fresh_results: list[RunResult] = []
    try:
        assert _await_ready(address, proc, timeout=30.0), "daemon did not become ready"
        warm = request_run(address, nodeids=selected)
        transition = request_run(address, nodeids=[], fresh_workers=True)
        fresh_a = request_run_results(address, fresh_results.append, nodeids=selected, fresh_workers=True)
        fresh_b = request_run(address, nodeids=selected, fresh_workers=True)
        warm_again = request_run(address, nodeids=selected)

        assert [reply.get("rc") for reply in (warm, transition, fresh_a, fresh_b, warm_again)] == [0] * 5
        assert transition.get("fresh_workers") is True
        assert fresh_a.get("fresh_workers") is True
        assert [result["nodeid"] for result in fresh_results] == selected
        assert fresh_b.get("fresh_workers") is True
        assert counter.read_text().splitlines() == ["setup", "setup", "setup", "setup"]
    finally:
        _stop(proc, address)


def test_fresh_workers_rejects_stop_first(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """A fork-per-request run cannot promise persistent-pool stop-first scheduling."""
    project, counter = tmp_path / "proj", tmp_path / "setups.txt"
    project.mkdir()
    _make_project(project, counter)
    address = str(tmp_path / "pf.sock")
    monkeypatch.setenv("PYTEST_FAST_ROOT", str(project))
    proc = _spawn_daemon(project, address, "--persist-workers")
    try:
        assert _await_ready(address, proc, timeout=30.0), "daemon did not become ready"
        reply = request_run(address, fresh_workers=True, stop_on_failure=True)
        assert reply.get("rc") == 2, reply
        assert "cannot be combined" in cast("str", reply.get("summary", ""))
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


def _make_stop_first_project(root: Path, sentinel: Path) -> None:
    """Project that makes execution after the first failure externally observable."""
    (root / "tests").mkdir(parents=True)
    (root / "pyproject.toml").write_text(
        '[project]\nname = "stop-first"\nversion = "0"\n\n[tool.pytest.ini_options]\ntestpaths = ["tests"]\n'
    )
    (root / "tests" / "test_stop.py").write_text(
        textwrap.dedent(f"""
        from pathlib import Path

        SENTINEL = Path({str(sentinel)!r})

        def test_fail():
            assert False, "first test kills the lease"

        def test_must_not_run():
            SENTINEL.write_text("ran", encoding="utf-8")

        def test_green_a():
            assert True

        def test_green_b():
            assert True
        """)
    )


def _make_process_exit_project(root: Path) -> None:
    """Project with a worker-level exit between two ordinary green items."""
    (root / "tests").mkdir(parents=True)
    (root / "pyproject.toml").write_text(
        '[project]\nname = "process-exit"\nversion = "0"\n\n[tool.pytest.ini_options]\ntestpaths = ["tests"]\n'
    )
    (root / "tests" / "test_exit.py").write_text(
        textwrap.dedent("""
        import os

        def test_green_before():
            assert True

        def test_process_exit():
            os._exit(17)

        def test_green_after():
            assert True
        """)
    )


def _make_duplicate_process_exit_project(root: Path, marker: Path) -> None:
    """Project whose repeated item exits the worker only on its second execution."""
    (root / "tests").mkdir(parents=True)
    (root / "pyproject.toml").write_text(
        '[project]\nname = "duplicate-process-exit"\nversion = "0"\n\n'
        '[tool.pytest.ini_options]\ntestpaths = ["tests"]\n'
    )
    (root / "tests" / "test_exit.py").write_text(
        textwrap.dedent(f"""
        import os
        from pathlib import Path

        MARKER = Path({str(marker)!r})

        def test_process_exit_on_second_execution():
            if MARKER.exists():
                os._exit(17)
            MARKER.write_text("first execution completed", encoding="utf-8")
        """)
    )


@pytest.mark.parametrize("fresh_workers", [False, True])
def test_compact_run_reports_exact_active_item_on_worker_exit(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    fresh_workers: bool,
) -> None:
    """An untrusted result undercount identifies only the item active inside pytest protocol."""
    project = tmp_path / "proj"
    _make_process_exit_project(project)
    address = str(tmp_path / "pf.sock")
    monkeypatch.setenv("PYTEST_FAST_ROOT", str(project))
    proc = _spawn_daemon(project, address, "--persist-workers")
    selected = [
        "tests/test_exit.py::test_green_before",
        "tests/test_exit.py::test_process_exit",
        "tests/test_exit.py::test_green_after",
    ]
    results: list[RunResult] = []
    try:
        assert _await_ready(address, proc, timeout=30.0), "daemon did not become ready"
        reply = request_run_results(
            address,
            results.append,
            nodeids=selected,
            stop_on_failure=not fresh_workers,
            fresh_workers=fresh_workers,
        )
        assert reply.get("rc") == 1, reply
        assert [result["nodeid"] for result in results] == [selected[0]]
        meta = cast("dict[str, object]", reply["run_meta"])
        assert meta["process_failures"] == [selected[1]]
        assert "UNTRUSTED" in cast("str", reply["summary"])
    finally:
        _stop(proc, address)


def test_compact_run_reports_repeated_active_item_after_same_nodeid_completed(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A completed occurrence cannot hide a later crash of the same selected nodeid."""
    project = tmp_path / "proj"
    marker = tmp_path / "first-execution-completed"
    _make_duplicate_process_exit_project(project, marker)
    address = str(tmp_path / "pf.sock")
    monkeypatch.setenv("PYTEST_FAST_ROOT", str(project))
    proc = _spawn_daemon(project, address, "--persist-workers")
    nodeid = "tests/test_exit.py::test_process_exit_on_second_execution"
    results: list[RunResult] = []
    try:
        assert _await_ready(address, proc, timeout=30.0), "daemon did not become ready"
        reply = request_run_results(
            address,
            results.append,
            nodeids=[nodeid, nodeid],
            stop_on_failure=True,
        )
        assert reply.get("rc") == 1, reply
        assert [result["nodeid"] for result in results] == [nodeid]
        meta = cast("dict[str, object]", reply["run_meta"])
        assert meta["process_failures"] == [nodeid]
        assert "UNTRUSTED" in cast("str", reply["summary"])
    finally:
        _stop(proc, address)


def test_persist_stop_on_failure_stops_dispatch_and_reports_trusted_short_circuit(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A one-worker persistent request stops exactly at the first observed failure."""
    project, sentinel = tmp_path / "proj", tmp_path / "must-not-run"
    _make_stop_first_project(project, sentinel)
    address = str(tmp_path / "pf.sock")
    monkeypatch.setenv("PYTEST_FAST_ROOT", str(project))
    proc = _spawn_daemon(project, address, "--persist-workers")
    try:
        assert _await_ready(address, proc, timeout=30.0), "daemon did not become ready"
        reply = request_run(
            address,
            nodeids=[
                "tests/test_stop.py::test_fail",
                "tests/test_stop.py::test_must_not_run",
                "tests/test_stop.py::test_green_a",
            ],
            stop_on_failure=True,
            want_results=True,
        )
        assert reply.get("rc") == 1, reply
        assert not sentinel.exists(), "dispatcher issued work after the first failure"
        results = cast("list[dict[str, object]]", reply["results"])
        meta = cast("dict[str, object]", reply["run_meta"])
        assert [result["nodeid"] for result in results] == ["tests/test_stop.py::test_fail"]
        assert meta["total"] == 1
        assert meta["planned_total"] == 3
        assert meta["short_circuited"] is True
        assert "UNTRUSTED" not in cast("str", reply["summary"])
    finally:
        _stop(proc, address)


def test_persist_stop_on_failure_runs_all_green_selection(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Stop-first remains a complete trusted run when no selected item fails."""
    project, sentinel = tmp_path / "proj", tmp_path / "must-not-run"
    _make_stop_first_project(project, sentinel)
    address = str(tmp_path / "pf.sock")
    monkeypatch.setenv("PYTEST_FAST_ROOT", str(project))
    proc = _spawn_daemon(project, address, "--persist-workers")
    try:
        assert _await_ready(address, proc, timeout=30.0), "daemon did not become ready"
        reply = request_run(
            address,
            nodeids=["tests/test_stop.py::test_green_a", "tests/test_stop.py::test_green_b"],
            stop_on_failure=True,
            want_results=True,
        )
        assert reply.get("rc") == 0, reply
        results = cast("list[dict[str, object]]", reply["results"])
        meta = cast("dict[str, object]", reply["run_meta"])
        assert [result["nodeid"] for result in results] == [
            "tests/test_stop.py::test_green_a",
            "tests/test_stop.py::test_green_b",
        ]
        assert meta["total"] == 2
        assert meta["planned_total"] == 2
        assert meta["short_circuited"] is False
        assert "UNTRUSTED" not in cast("str", reply["summary"])
    finally:
        _stop(proc, address)


def test_persist_selection_rejects_unknown_nodeids(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """A stale coverage selection must fail closed instead of becoming a trusted empty green run."""
    project, counter = tmp_path / "proj", tmp_path / "setups.txt"
    project.mkdir()
    _make_project(project, counter)
    address = str(tmp_path / "pf.sock")
    monkeypatch.setenv("PYTEST_FAST_ROOT", str(project))
    proc = _spawn_daemon(project, address, "--persist-workers")
    try:
        assert _await_ready(address, proc, timeout=30.0), "daemon did not become ready"
        reply = request_run(
            address,
            nodeids=[
                "tests/test_uses_resource.py::test_a",
                "tests/test_uses_resource.py::test_was_deleted",
            ],
            stop_on_failure=True,
            want_results=True,
        )
        assert reply.get("rc") == 2, reply
        assert "unknown selected nodeid" in cast("str", reply.get("summary", ""))
        assert "test_was_deleted" in cast("str", reply.get("summary", ""))
        assert "results" not in reply
        assert not counter.exists(), "selection validation must happen before a known test executes"
    finally:
        _stop(proc, address)


def test_fresh_worker_selection_rejects_unknown_nodeids(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """The fork-per-request path applies the same fail-closed selection contract."""
    project, counter = tmp_path / "proj", tmp_path / "setups.txt"
    project.mkdir()
    _make_project(project, counter)
    address = str(tmp_path / "pf.sock")
    monkeypatch.setenv("PYTEST_FAST_ROOT", str(project))
    proc = _spawn_daemon(project, address)
    try:
        assert _await_ready(address, proc, timeout=30.0), "daemon did not become ready"
        reply = request_run(
            address,
            nodeids=[
                "tests/test_uses_resource.py::test_a",
                "tests/test_uses_resource.py::test_was_deleted",
            ],
            want_results=True,
        )
        assert reply.get("rc") == 2, reply
        assert "unknown selected nodeid" in cast("str", reply.get("summary", ""))
        assert "results" not in reply
        assert not counter.exists(), "selection validation must happen before a known test executes"
    finally:
        _stop(proc, address)


def test_persist_streamed_stop_on_failure_uses_the_same_dispatch_contract(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The streamed client used by wrapper tools also stops before the next selected item."""
    project, sentinel = tmp_path / "proj", tmp_path / "must-not-run"
    _make_stop_first_project(project, sentinel)
    address = str(tmp_path / "pf.sock")
    monkeypatch.setenv("PYTEST_FAST_ROOT", str(project))
    proc = _spawn_daemon(project, address, "--persist-workers")
    reports: list[dict[str, object]] = []
    try:
        assert _await_ready(address, proc, timeout=30.0), "daemon did not become ready"
        reply = request_run_streamed(
            address,
            reports.append,
            nodeids=["tests/test_stop.py::test_fail", "tests/test_stop.py::test_must_not_run"],
            stop_on_failure=True,
        )
        assert reply.get("rc") == 1, reply
        assert not sentinel.exists()
        assert {report["nodeid"] for report in reports} == {"tests/test_stop.py::test_fail"}
    finally:
        _stop(proc, address)


def test_persist_compact_results_stop_on_failure_uses_the_same_dispatch_contract(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The compact wrapper client stops at the same item and returns its exact lean result."""
    project, sentinel = tmp_path / "proj", tmp_path / "must-not-run"
    _make_stop_first_project(project, sentinel)
    address = str(tmp_path / "pf.sock")
    monkeypatch.setenv("PYTEST_FAST_ROOT", str(project))
    proc = _spawn_daemon(project, address, "--persist-workers")
    results: list[RunResult] = []
    progress: list[tuple[int, int]] = []
    try:
        assert _await_ready(address, proc, timeout=30.0), "daemon did not become ready"
        reply = request_run_results(
            address,
            results.append,
            on_progress=lambda done, total: progress.append((done, total)),
            nodeids=["tests/test_stop.py::test_fail", "tests/test_stop.py::test_must_not_run"],
            stop_on_failure=True,
        )
        assert reply.get("rc") == 1, reply
        assert reply.get("stop_on_failure") is True
        assert not sentinel.exists()
        assert [result["nodeid"] for result in results] == ["tests/test_stop.py::test_fail"]
        assert [result["outcome"] for result in results] == ["failed"]
        assert progress == [(1, 2)]
    finally:
        _stop(proc, address)


def test_persist_stop_first_preserves_module_fixture_across_adjacent_selected_items(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Lookahead keeps pytest's nextitem fixture sharing while the next item remains undispatched."""
    project, counter = tmp_path / "proj", tmp_path / "module-setups"
    (project / "tests").mkdir(parents=True)
    (project / "pyproject.toml").write_text(
        '[project]\nname = "stop-fixtures"\nversion = "0"\n\n[tool.pytest.ini_options]\ntestpaths = ["tests"]\n'
    )
    (project / "tests" / "test_anchor.py").write_text("def test_anchor():\n    assert True\n")
    (project / "tests" / "test_selected.py").write_text(
        textwrap.dedent(f"""
        from pathlib import Path
        import pytest

        @pytest.fixture(scope="module")
        def resource():
            path = Path({str(counter)!r})
            with path.open("a") as stream:
                stream.write("setup\\n")
            return object()

        def test_a(resource):
            assert resource is not None

        def test_b(resource):
            assert resource is not None

        def test_c(resource):
            assert resource is not None
        """)
    )
    address = str(tmp_path / "pf.sock")
    monkeypatch.setenv("PYTEST_FAST_ROOT", str(project))
    proc = _spawn_daemon(project, address, "--persist-workers")
    try:
        assert _await_ready(address, proc, timeout=30.0), "daemon did not become ready"
        reply = request_run(
            address,
            nodeids=[
                "tests/test_selected.py::test_a",
                "tests/test_selected.py::test_b",
                "tests/test_selected.py::test_c",
            ],
            stop_on_failure=True,
        )
        assert reply.get("rc") == 0, reply
        assert counter.read_text().splitlines() == ["setup"]
    finally:
        _stop(proc, address)


def test_persist_stop_first_reserves_fixture_lookahead_for_each_worker(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A worker must receive the item used as its fixture-preserving nextitem."""
    project = tmp_path / "proj"
    (project / "tests").mkdir(parents=True)
    (project / "pyproject.toml").write_text(
        '[project]\nname = "stop-fixtures-multi"\nversion = "0"\n\n[tool.pytest.ini_options]\ntestpaths = ["tests"]\n'
    )
    (project / "tests" / "test_a.py").write_text(
        textwrap.dedent("""
        import time

        def test_a1():
            assert True

        def test_a2():
            time.sleep(0.5)
        """)
    )
    (project / "tests" / "test_b.py").write_text("def test_b1():\n    assert True\n\ndef test_b2():\n    assert True\n")
    address = str(tmp_path / "pf.sock")
    monkeypatch.setenv("PYTEST_FAST_ROOT", str(project))
    proc = _spawn_daemon(project, address, "--persist-workers", workers=2)
    try:
        assert _await_ready(address, proc, timeout=30.0), "daemon did not become ready"
        reply = request_run(
            address,
            nodeids=[
                "tests/test_a.py::test_a1",
                "tests/test_a.py::test_a2",
                "tests/test_b.py::test_b1",
                "tests/test_b.py::test_b2",
            ],
            stop_on_failure=True,
            want_results=True,
        )
        assert reply.get("rc") == 0, reply
        results = cast("list[dict[str, object]]", reply["results"])
        assert {result["nodeid"] for result in results} == {
            "tests/test_a.py::test_a1",
            "tests/test_a.py::test_a2",
            "tests/test_b.py::test_b1",
            "tests/test_b.py::test_b2",
        }
    finally:
        _stop(proc, address)


def test_persist_stop_first_reuses_session_fixture_with_single_collected_item(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The fixture boundary preserves session scope even when no second item exists."""
    project, counter = tmp_path / "proj", tmp_path / "session-setups"
    (project / "tests").mkdir(parents=True)
    (project / "pyproject.toml").write_text(
        '[project]\nname = "single-item"\nversion = "0"\n\n[tool.pytest.ini_options]\ntestpaths = ["tests"]\n'
    )
    (project / "conftest.py").write_text(
        textwrap.dedent(f"""
        from pathlib import Path
        import pytest

        def pytest_runtest_teardown(item, nextitem):
            assert nextitem is None or isinstance(nextitem, pytest.Item)

        @pytest.fixture(scope="session")
        def resource():
            path = Path({str(counter)!r})
            with path.open("a") as stream:
                stream.write("setup\\n")
            return object()
        """)
    )
    (project / "tests" / "test_only.py").write_text("def test_only(resource):\n    assert resource is not None\n")
    address = str(tmp_path / "pf.sock")
    monkeypatch.setenv("PYTEST_FAST_ROOT", str(project))
    proc = _spawn_daemon(project, address, "--persist-workers")
    try:
        assert _await_ready(address, proc, timeout=30.0), "daemon did not become ready"
        for _ in range(3):
            reply = request_run(
                address,
                nodeids=["tests/test_only.py::test_only"],
                stop_on_failure=True,
            )
            assert reply.get("rc") == 0, reply
        assert counter.read_text().splitlines() == ["setup"]
    finally:
        _stop(proc, address)


def test_persist_stop_first_tears_down_module_fixture_after_short_circuit(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Only session-scoped fixtures may survive a persistent request boundary."""
    project, counter = tmp_path / "proj", tmp_path / "module-setups"
    (project / "tests").mkdir(parents=True)
    (project / "pyproject.toml").write_text(
        '[project]\nname = "stop-module-boundary"\nversion = "0"\n\n[tool.pytest.ini_options]\ntestpaths = ["tests"]\n'
    )
    (project / "tests" / "test_selected.py").write_text(
        textwrap.dedent(f"""
        from pathlib import Path
        import pytest

        @pytest.fixture(scope="module")
        def resource():
            path = Path({str(counter)!r})
            with path.open("a") as stream:
                stream.write("setup\\n")
            return object()

        def test_fail(resource):
            assert False

        def test_after(resource):
            assert resource is not None
        """)
    )
    address = str(tmp_path / "pf.sock")
    monkeypatch.setenv("PYTEST_FAST_ROOT", str(project))
    proc = _spawn_daemon(project, address, "--persist-workers")
    try:
        assert _await_ready(address, proc, timeout=30.0), "daemon did not become ready"
        failed = request_run(
            address,
            nodeids=[
                "tests/test_selected.py::test_fail",
                "tests/test_selected.py::test_after",
            ],
            stop_on_failure=True,
        )
        assert failed.get("rc") == 1, failed
        passed = request_run(
            address,
            nodeids=["tests/test_selected.py::test_after"],
            stop_on_failure=True,
        )
        assert passed.get("rc") == 0, passed
        assert counter.read_text().splitlines() == ["setup", "setup"]
    finally:
        _stop(proc, address)


def test_persist_rejects_bench_with_stop_on_failure(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Benchmark mode must reject a stop-first contract it cannot honor."""
    project, sentinel = tmp_path / "proj", tmp_path / "must-not-run"
    _make_stop_first_project(project, sentinel)
    address = str(tmp_path / "pf.sock")
    monkeypatch.setenv("PYTEST_FAST_ROOT", str(project))
    proc = _spawn_daemon(project, address, "--persist-workers")
    try:
        assert _await_ready(address, proc, timeout=30.0), "daemon did not become ready"
        reply = request_run(address, bench=2, stop_on_failure=True)
        assert reply.get("rc") == 2, reply
        assert "cannot be combined with bench" in cast("str", reply.get("summary", ""))
    finally:
        _stop(proc, address)
