"""Differential parity tests — the pytest-fast engine must agree, test-for-test, with
plain pytest (the oracle) and with pytest-xdist.

This targets the project's nastiest class of regression: the warm-forkserver engine quietly
producing a *different outcome* than a normal pytest session (collection drift, a fixture /
gc.freeze / COW interaction, a `categorize` edge). We generate synthetic suites spanning the
whole outcome spectrum and run each three ways, comparing the `{nodeid: outcome}` dumps that
all three already emit (plain & xdist via the `OUTCOME_DUMP` plugin hook; the engine via its
`--dump`). Plain pytest is the ground truth; the engine — and xdist, as a control — must match
it exactly. Hypothesis shrinks any divergence to a minimal suite.

Only parallel-SAFE tests are generated (each independent, no shared mutable state), so a
divergence is a real engine bug, not an artifact of distributing tests across workers.

Opt-in (`@pytest.mark.parity`): spawns three pytest/engine subprocesses per suite and needs
pytest-xdist (the `xdist-parity` group) for the xdist leg. Run with `make test-parity`.
"""

from __future__ import annotations

import importlib.util
import json
import os
import subprocess
import sys
from typing import TYPE_CHECKING

import pytest
from hypothesis import given, settings
from hypothesis import strategies as st

if TYPE_CHECKING:
    from collections.abc import Callable
    from pathlib import Path

pytestmark = pytest.mark.parity

# Each kind → a self-contained, parallel-safe test snippet whose outcome is well-defined.
# Together they span: passed / failed / error / skipped / xfailed / xpassed / parametrize.
_KINDS: dict[str, Callable[[int], str]] = {
    "pass": lambda i: f"def test_{i}_pass():\n    assert True",
    "fail": lambda i: f"def test_{i}_fail():\n    assert 1 == 2",
    "raise": lambda i: f"def test_{i}_raise():\n    raise ValueError('boom')",  # call-phase failure
    "error_setup": lambda i: f"def test_{i}_errsetup(broken):\n    assert True",  # setup error
    "skip": lambda i: f"@pytest.mark.skip(reason='r')\ndef test_{i}_skip():\n    assert True",
    "skipif_true": lambda i: f"@pytest.mark.skipif(True, reason='r')\ndef test_{i}_skipt():\n    assert True",
    "skipif_false": lambda i: f"@pytest.mark.skipif(False, reason='r')\ndef test_{i}_skipf():\n    assert True",
    "xfail_fail": lambda i: f"@pytest.mark.xfail(reason='r')\ndef test_{i}_xff():\n    assert False",
    "xfail_pass": lambda i: f"@pytest.mark.xfail(reason='r')\ndef test_{i}_xfp():\n    assert True",
    "param": lambda i: f"@pytest.mark.parametrize('v', [1, 2, 3])\ndef test_{i}_param(v):\n    assert v > 0",
}

_MODULE_PRELUDE = "import pytest\n\n\n@pytest.fixture\ndef broken():\n    raise RuntimeError('setup boom')\n\n\n"


def _write_project(root: Path, kinds: list[str]) -> None:
    """Materialize a one-module synthetic project under `root` from a list of test kinds."""
    (root / "tests").mkdir(exist_ok=True)
    # An explicit (empty) [tool.pytest.ini_options] pins rootdir to this dir so no stray
    # ancestor ini can leak in and skew collection.
    (root / "pyproject.toml").write_text('[project]\nname = "pf-parity"\nversion = "0"\n\n[tool.pytest.ini_options]\n')
    snippets = [_KINDS[kind](i) for i, kind in enumerate(kinds)]
    (root / "tests" / "test_gen.py").write_text(_MODULE_PRELUDE + "\n\n\n".join(snippets) + "\n")


def _clean_env(projdir: Path) -> dict[str, str]:
    """Subprocess env scrubbed of anything that could skew a child run (inherited addopts,
    a stale collect flag, leaked pytest-fast vars), pinned to this project root."""
    env = os.environ.copy()
    for key in ("_PYTEST_FAST_COLLECT", "PYTEST_ADDOPTS", "OUTCOME_DUMP"):
        env.pop(key, None)
    for key in [k for k in env if k.startswith("PYTEST_FAST_")]:
        env.pop(key, None)
    env["PYTEST_FAST_ROOT"] = str(projdir)
    return env


def _read_dump(path: Path, proc: subprocess.CompletedProcess[str], label: str) -> dict[str, str]:
    if not path.exists():
        pytest.fail(
            f"[{label}] wrote no outcome dump (rc={proc.returncode}).\nstdout:\n{proc.stdout}\nstderr:\n{proc.stderr}"
        )
    return json.loads(path.read_text())


def _run_three_ways(projdir: Path, *, with_xdist: bool) -> dict[str, dict[str, str]]:
    """Run the project under plain pytest, the pytest-fast engine, and (optionally) xdist;
    return each runner's `{nodeid: outcome}` dump."""
    env = _clean_env(projdir)
    dumps: dict[str, dict[str, str]] = {}

    # Plain pytest (the oracle). The pytest_fast plugin is auto-loaded; OUTCOME_DUMP makes
    # its sessionfinish hook write the dump. Runs in-process, cold — the reference semantics.
    p_plain = projdir / "d_plain.json"
    proc = subprocess.run(
        [sys.executable, "-m", "pytest", "-q", "-p", "no:cacheprovider"],
        cwd=projdir,
        env={**env, "OUTCOME_DUMP": str(p_plain)},
        capture_output=True,
        text=True,
        timeout=180,
        check=False,
    )
    dumps["plain"] = _read_dump(p_plain, proc, "plain")

    # The pytest-fast engine (warm forkserver workers running the full protocol). `--dump`
    # writes the engine's own categorization of what the workers actually executed.
    p_engine = projdir / "d_engine.json"
    proc = subprocess.run(
        [sys.executable, "-m", "pytest_fast", "--dump", str(p_engine), "--runs", "1", "--workers", "2"],
        cwd=projdir,
        env=env,
        capture_output=True,
        text=True,
        timeout=180,
        check=False,
    )
    dumps["engine"] = _read_dump(p_engine, proc, "engine")

    # xdist control — same plugin/dump path, cold workers. Only if the group is installed.
    if with_xdist and importlib.util.find_spec("xdist") is not None:
        p_xdist = projdir / "d_xdist.json"
        proc = subprocess.run(
            [sys.executable, "-m", "pytest", "-q", "-p", "no:cacheprovider", "-n2"],
            cwd=projdir,
            env={**env, "OUTCOME_DUMP": str(p_xdist)},
            capture_output=True,
            text=True,
            timeout=180,
            check=False,
        )
        dumps["xdist"] = _read_dump(p_xdist, proc, "xdist")
    return dumps


def _diff(oracle: dict[str, str], other: dict[str, str]) -> str:
    rows = [
        f"  {nid}: oracle={oracle.get(nid, '<MISSING>')} other={other.get(nid, '<MISSING>')}"
        for nid in sorted(set(oracle) | set(other))
        if oracle.get(nid) != other.get(nid)
    ]
    return "outcome divergence (engine/xdist vs plain pytest):\n" + "\n".join(rows)


def test_parity_kitchen_sink(tmp_path: Path) -> None:
    """Every outcome kind, once: the engine and xdist must each reproduce plain pytest's
    full `{nodeid: outcome}` map exactly."""
    kinds = sorted(_KINDS)
    dumps = _write_and_run(tmp_path, kinds, with_xdist=True)
    plain = dumps["plain"]
    # Guard against a silently-empty oracle (collection broke) making `{} == {}` a false pass.
    assert len(plain) >= len(kinds), f"oracle collected too few tests ({len(plain)}) — harness broken"
    assert dumps["engine"] == plain, _diff(plain, dumps["engine"])
    if "xdist" in dumps:
        assert dumps["xdist"] == plain, _diff(plain, dumps["xdist"])


@given(kinds=st.lists(st.sampled_from(sorted(_KINDS)), min_size=1, max_size=10))
@settings(max_examples=20, deadline=None)
def test_parity_engine_matches_plain(tmp_path_factory: pytest.TempPathFactory, kinds: list[str]) -> None:
    """Random parallel-safe suites: the engine's outcome map must equal plain pytest's.
    (xdist is exercised by the kitchen-sink test; skipped here to keep the loop affordable.)"""
    projdir = tmp_path_factory.mktemp("parity")
    dumps = _write_and_run(projdir, kinds, with_xdist=False)
    # Guard against a silently-empty oracle (collection broke) making `{} == {}` a false pass.
    assert len(dumps["plain"]) >= len(kinds), "oracle collected too few tests — harness broken"
    assert dumps["engine"] == dumps["plain"], _diff(dumps["plain"], dumps["engine"])


def _write_and_run(projdir: Path, kinds: list[str], *, with_xdist: bool) -> dict[str, dict[str, str]]:
    _write_project(projdir, kinds)
    return _run_three_ways(projdir, with_xdist=with_xdist)
