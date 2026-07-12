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


def _run(project: Path, *args: str) -> subprocess.CompletedProcess[str]:
    env = os.environ.copy()
    env["PYTEST_FAST_ROOT"] = str(project)
    env.pop("_PYTEST_FAST_COLLECT", None)
    return subprocess.run(
        [sys.executable, "-m", "pytest_fast", "--workers", "1", *args],
        cwd=str(project),
        env=env,
        capture_output=True,
        text=True,
        check=False,
        timeout=120,
    )


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
