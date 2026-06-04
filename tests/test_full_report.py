"""Tier 1 — full serialized reports on the wire.

In `full_report=True` mode each worker attaches every phase report in pytest's
serializable form (`pytest_report_to_serializable` → plain builtins) to the
`RunResult`. This is the foundation for replaying reports through a real
terminalreporter (--durations / junit / -v/-s / plugins) on the master or the
plugin controller — without touching the warm-fork speed (collection is still
done once in the forkserver).

The driver runs in a SUBPROCESS (the suite is dogfooded through pytest-fast's own
daemonic workers, which may not spawn multiprocessing children — see the F3 note
in test_stresstest_findings.py). It wraps `Daemon._report` to capture the raw
`results` and asserts each one carries well-formed serialized reports. The mere
fact the reports arrive proves they survived the builtins-only `_SafeUnpickler`
whitelist on the bus.
"""

from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path
from typing import TYPE_CHECKING, cast

if TYPE_CHECKING:
    from pytest_fast import RunResult


_DRIVER = """\
import sys

import pytest_fast as pf

captured = {}
_orig_report = pf.Daemon._report


def _cap_report(self, results, *a, **k):
    captured["results"] = results
    return _orig_report(self, results, *a, **k)


pf.Daemon._report = _cap_report

_PHASES = {"setup", "call", "teardown"}


def main():
    d = pf.Daemon(num_workers=2, start_method="forkserver")
    d._run_once(full_report=True)
    results = captured.get("results", [])
    if not results:
        print("no results captured", file=sys.stderr)
        sys.exit(2)

    missing = [r["nodeid"] for r in results if "reports" not in r]
    if missing:
        print(f"results missing serialized reports: {missing}", file=sys.stderr)
        sys.exit(3)

    total = 0
    for r in results:
        reps = r["reports"]
        if not isinstance(reps, list) or not reps:
            print(f"{r['nodeid']}: empty/!list reports", file=sys.stderr)
            sys.exit(4)
        for rep in reps:
            total += 1
            # arrived over the SafeUnpickler bus → already proven builtins-only; now
            # assert it is a genuine serialized pytest TestReport with phase + timing.
            if rep.get("$report_type") != "TestReport":
                print(f"bad $report_type: {rep.get('$report_type')!r}", file=sys.stderr)
                sys.exit(5)
            if rep.get("when") not in _PHASES:
                print(f"bad when: {rep.get('when')!r}", file=sys.stderr)
                sys.exit(6)
            if not isinstance(rep.get("duration"), float):
                print(f"missing/!float duration: {rep.get('duration')!r}", file=sys.stderr)
                sys.exit(7)

    print(f"OK results={len(results)} reports={total}")
    sys.exit(0)


if __name__ == "__main__":
    main()
"""


def test_full_report_ships_serialized_reports(tmp_project: Path, tmp_path: Path) -> None:
    driver = tmp_path / "fr_drive.py"
    driver.write_text(_DRIVER)
    env = os.environ.copy()
    env["PYTEST_FAST_ROOT"] = str(tmp_project)
    env.pop("_PYTEST_FAST_COLLECT", None)
    proc = subprocess.run(
        [sys.executable, str(driver)],
        cwd=str(tmp_project),
        env=env,
        capture_output=True,
        text=True,
        check=False,
        timeout=120,
    )
    assert proc.returncode == 0, (
        f"full_report=True did not ship well-formed serialized reports.\nstdout:\n{proc.stdout}\nstderr:\n{proc.stderr}"
    )
    # tmp_project has 2 tests; each yields setup+call+teardown phase reports.
    assert "reports=" in proc.stdout
    n_reports = int(proc.stdout.split("reports=")[1].split()[0])
    assert n_reports >= 2, f"expected ≥2 phase reports, got {n_reports}: {proc.stdout}"


def test_lean_mode_omits_reports_by_default(tmp_project: Path, tmp_path: Path) -> None:
    """Default (lean) mode must NOT attach serialized reports — keeps the bus cheap.
    Same driver, but `full_report` defaults to False, so `reports` is absent → exit 3."""
    driver = tmp_path / "lean_drive.py"
    driver.write_text(_DRIVER.replace("d._run_once(full_report=True)", "d._run_once()"))
    env = os.environ.copy()
    env["PYTEST_FAST_ROOT"] = str(tmp_project)
    env.pop("_PYTEST_FAST_COLLECT", None)
    proc = subprocess.run(
        [sys.executable, str(driver)],
        cwd=str(tmp_project),
        env=env,
        capture_output=True,
        text=True,
        check=False,
        timeout=120,
    )
    assert proc.returncode == 3, (
        f"lean mode should omit 'reports' (driver exits 3), got rc={proc.returncode}.\n"
        f"stdout:\n{proc.stdout}\nstderr:\n{proc.stderr}"
    )


# ── _durations_lines (pure, fast) ────────────────────────────────────────────


def test_durations_lines_sorts_and_thresholds() -> None:
    """The --durations table: per-phase, slowest-first, sub-threshold phases hidden."""
    from pytest_fast import _durations_lines

    results = [
        {
            "nodeid": "t::slow",
            "outcome": "passed",
            "duration": 0.30,
            "reports": [
                {"$report_type": "TestReport", "nodeid": "t::slow", "when": "setup", "duration": 0.001},
                {"$report_type": "TestReport", "nodeid": "t::slow", "when": "call", "duration": 0.30},
            ],
        },
        {
            "nodeid": "t::med",
            "outcome": "passed",
            "duration": 0.12,
            "reports": [
                {"$report_type": "TestReport", "nodeid": "t::med", "when": "call", "duration": 0.12},
            ],
        },
    ]
    lines = _durations_lines(cast("list[RunResult]", results), limit=15, min_dur=0.005)
    assert lines, "expected a durations table"
    assert "DURATIONS" in lines[0]
    body = lines[1:]
    # slowest-first
    assert "t::slow" in body[0] and "0.300s" in body[0]
    assert "t::med" in body[1] and "0.121s" not in body[1]  # 0.120 → '0.120s'
    assert "0.120s" in body[1]
    # the 0.001s setup phase is below the 5ms threshold → hidden
    assert all("setup" not in ln for ln in body)


def test_durations_lines_empty_without_reports() -> None:
    """Lean results (no serialized reports) → no durations table at all."""
    from pytest_fast import _durations_lines

    lean = [{"nodeid": "t::a", "outcome": "passed", "duration": 0.5}]
    assert _durations_lines(cast("list[RunResult]", lean)) == []


def test_cli_full_report_renders_durations(tmp_path: Path, pf_cmd: list[str]) -> None:
    """End-to-end CLI: `--full-report` local run prints a per-phase DURATIONS table;
    without it (lean) the table is absent."""
    (tmp_path / "src").mkdir()
    (tmp_path / "tests").mkdir()
    (tmp_path / "src" / "foo.py").write_text("def f() -> int:\n    return 1\n")
    (tmp_path / "tests" / "test_t.py").write_text(
        "import time\n"
        "def test_slow() -> None:\n    time.sleep(0.20)\n    assert True\n"
        "def test_fast() -> None:\n    assert 1 + 1 == 2\n",
    )
    (tmp_path / "pyproject.toml").write_text('[project]\nname = "x"\nversion = "0"\n')

    full = subprocess.run(
        [*pf_cmd, "--workers", "2", "--runs", "1", "--full-report"],
        cwd=tmp_path,
        capture_output=True,
        text=True,
        check=False,
        timeout=120,
    )
    assert full.returncode == 0, f"stdout:\n{full.stdout}\nstderr:\n{full.stderr}"
    assert "DURATIONS" in full.stdout, f"missing durations table:\n{full.stdout}"
    assert "call" in full.stdout and "test_slow" in full.stdout

    lean = subprocess.run(
        [*pf_cmd, "--workers", "2", "--runs", "1"],
        cwd=tmp_path,
        capture_output=True,
        text=True,
        check=False,
        timeout=120,
    )
    assert lean.returncode == 0
    assert "DURATIONS" not in lean.stdout, f"lean mode must not render durations:\n{lean.stdout}"
