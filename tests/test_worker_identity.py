"""The opt-in per-worker identity feature (`--worker-identity` / `$PYTEST_FAST_WORKER_IDENTITY`).

Default OFF — a normal run gets no identity (full xdist-parity, zero behavior change). ON, each forked
worker gets a distinct `PYTEST_XDIST_WORKER` + `config.workerinput`, and pytest-fast's own
`pytest_fast_configure_worker` hook fires once per worker, before any test runs. The feature is
self-contained: no third-party dependency (not even pytest-xdist) is needed for any of this.
"""

from __future__ import annotations

import os
import subprocess
import sys
import textwrap
from pathlib import Path


def _make_project(root: Path, out: Path) -> None:
    """A tiny project whose conftest implements the hook (recording each worker it fires for) and
    whose tests record the PYTEST_XDIST_WORKER they observed — both written to `out` for assertions."""
    out.mkdir(parents=True, exist_ok=True)
    (root / "tests").mkdir()
    (root / "pyproject.toml").write_text(
        '[project]\nname = "wid"\nversion = "0"\n\n[tool.pytest.ini_options]\ntestpaths = ["tests"]\n'
    )
    (root / "conftest.py").write_text(
        textwrap.dedent(f"""
        import os
        _OUT = {str(out)!r}

        def pytest_fast_configure_worker(config, workerid, workercount):
            # Fires once per worker when --worker-identity is on. Record id/count + that
            # config.workerinput was populated, so the outer test can assert per-worker firing.
            wi = getattr(config, "workerinput", None) or {{}}
            with open(os.path.join(_OUT, f"hook-{{workerid}}.txt"), "w") as fh:
                fh.write(f"{{workerid}} {{workercount}} {{wi.get('workerid')}}")

        def pytest_fast_worker_shutdown(config, workerid):
            with open(os.path.join(_OUT, f"shutdown-{{workerid}}.txt"), "w") as fh:
                fh.write(workerid)
        """)
    )
    (root / "tests" / "test_w.py").write_text(
        textwrap.dedent(f"""
        import os
        _OUT = {str(out)!r}

        def test_records_worker_env():
            with open(os.path.join(_OUT, f"env-{{os.getpid()}}.txt"), "w") as fh:
                fh.write(str(os.environ.get("PYTEST_XDIST_WORKER")))

        def test_other():
            assert True
        """)
    )


def _run(project: Path, *args: str, workers: int = 2) -> subprocess.CompletedProcess[str]:
    env = os.environ.copy()
    env["PYTEST_FAST_ROOT"] = str(project)
    env.pop("_PYTEST_FAST_COLLECT", None)
    env.pop("PYTEST_FAST_WORKER_IDENTITY", None)  # start from a clean slate regardless of outer run
    return subprocess.run(
        [sys.executable, "-m", "pytest_fast", "--runs", "1", "--workers", str(workers), *args],
        cwd=str(project),
        env=env,
        capture_output=True,
        text=True,
        check=False,
        timeout=120,
    )


def test_off_by_default(tmp_path: Path) -> None:
    """No flag → the hook never fires and no PYTEST_XDIST_WORKER leaks into workers. Loading a
    conftest that defines the hook must NOT error (the hookspec is always registered)."""
    project, out = tmp_path / "proj", tmp_path / "out"
    project.mkdir()
    _make_project(project, out)

    proc = _run(project)
    assert proc.returncode == 0, proc.stdout + proc.stderr
    assert not list(out.glob("hook-*.txt")), "hook fired without --worker-identity"
    observed = {p.read_text() for p in out.glob("env-*.txt")}
    assert observed <= {"None"}, f"PYTEST_XDIST_WORKER set without opt-in: {observed}"
    assert sorted(p.name for p in out.glob("shutdown-*.txt")) == ["shutdown-gw0.txt", "shutdown-gw1.txt"]


def test_on_fires_hook_per_worker_and_sets_env(tmp_path: Path) -> None:
    """`--worker-identity` → the hook fires once per forked worker with a distinct id and the right
    worker count, and workers see a real PYTEST_XDIST_WORKER. No pytest-xdist installed/needed."""
    project, out = tmp_path / "proj", tmp_path / "out"
    project.mkdir()
    _make_project(project, out)

    proc = _run(project, "--worker-identity")
    assert proc.returncode == 0, proc.stdout + proc.stderr

    hooks = sorted(p.name for p in out.glob("hook-*.txt"))
    assert hooks == ["hook-gw0.txt", "hook-gw1.txt"], f"hook didn't fire once per worker: {hooks}"
    for p in out.glob("hook-*.txt"):
        wid, count, wi_wid = p.read_text().split()
        assert count == "2", f"workercount wrong: {count}"
        assert wi_wid == wid, "config.workerinput not populated for the worker"

    observed = {p.read_text() for p in out.glob("env-*.txt")}
    assert any(v.startswith("gw") for v in observed), f"PYTEST_XDIST_WORKER not set in workers: {observed}"
    assert sorted(p.name for p in out.glob("shutdown-*.txt")) == ["shutdown-gw0.txt", "shutdown-gw1.txt"]


def test_on_fires_hook_for_single_worker(tmp_path: Path) -> None:
    """Worker-owned adapters need the same lifecycle when parallelism is one."""
    project, out = tmp_path / "proj", tmp_path / "out"
    project.mkdir()
    _make_project(project, out)

    proc = _run(project, "--worker-identity", workers=1)
    assert proc.returncode == 0, proc.stdout + proc.stderr
    assert (out / "hook-gw0.txt").read_text() == "gw0 1 gw0"
    assert {p.read_text() for p in out.glob("env-*.txt")} == {"gw0"}
    assert (out / "shutdown-gw0.txt").read_text() == "gw0"
