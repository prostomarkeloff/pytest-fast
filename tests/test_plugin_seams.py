"""The 0.12 extension seams: worker annotate hook, daemon-side plugins, `want_results` frames.

Three seams, one per process role (see README «Extending pytest-fast»):
  * worker — `pytest_fast_annotate_result` (a pytest hook; conftest impls run inside forked
    workers) writes into `extra`, which rides the worker→master bus as `result["extra"]`;
  * daemon — `PYTEST_FAST_DAEMON_PLUGINS=mod` names plain modules whose
    `pytest_fast_run_completed(run_info)` contributes summary lines spliced INSIDE the box;
    every failure mode (import error / missing symbol / raising impl) degrades to a visible
    ⚠ line — never a crashed daemon, never a failed run;
  * client — `request_run(want_results=True)` puts lean per-test results + worker stats +
    run meta into the final frame (daemons predating the flag: keys simply absent).
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
import textwrap
from pathlib import Path
from typing import TYPE_CHECKING, cast

import pytest

from pytest_fast import _await_ready, _shutdown_daemon, request_run

if TYPE_CHECKING:
    from collections.abc import Iterator


def _make_seams_project(root: Path, out: Path) -> None:
    """A tiny project exercising both in-engine seams: the conftest annotates every result
    (worker seam), `gateplug.py` consumes run_info and contributes a summary line (daemon
    seam, importable because `python -m pytest_fast` puts the cwd on sys.path)."""
    out.mkdir(parents=True, exist_ok=True)
    (root / "tests").mkdir()
    (root / "pyproject.toml").write_text('[project]\nname = "seams"\nversion = "0"\n')
    (root / "conftest.py").write_text(
        textwrap.dedent(
            """
            def pytest_fast_annotate_result(item, result, extra):
                extra["marker"] = f"annotated:{item.nodeid}"
                extra["saw_final_result"] = isinstance(result.get("duration"), float)
            """
        )
    )
    (root / "gateplug.py").write_text(
        textwrap.dedent(
            f"""
            import json
            import os

            _OUT = {str(out)!r}

            def pytest_fast_run_completed(run_info):
                results = run_info["results"]
                payload = {{
                    "n": len(results),
                    "markers": sorted(str((r.get("extra") or {{}}).get("marker")) for r in results),
                    "have_cpu": all("cpu" in r for r in results),
                    "total": run_info["total"],
                    "workers": run_info["num_workers"],
                    "label": run_info["label"],
                }}
                with open(os.path.join(_OUT, "run_info.json"), "w") as fh:
                    json.dump(payload, fh)
                return [f"  ⏱ gateplug: saw {{len(results)}} results"]
            """
        )
    )
    (root / "tests" / "test_s.py").write_text(
        textwrap.dedent(
            """
            def test_a() -> None:
                assert 1 + 1 == 2

            def test_b() -> None:
                assert "x" * 3 == "xxx"
            """
        )
    )


def _run_local(project: Path, extra_env: dict[str, str]) -> subprocess.CompletedProcess[str]:
    """One local-mode run (`--runs 1`, no resident daemon) with the given env additions."""
    env = os.environ.copy()
    env["PYTEST_FAST_ROOT"] = str(project)
    env.pop("_PYTEST_FAST_COLLECT", None)
    env.pop("PYTEST_FAST_DAEMON_PLUGINS", None)
    env.update(extra_env)
    return subprocess.run(
        [sys.executable, "-m", "pytest_fast", "--runs", "1", "--workers", "2"],
        cwd=str(project),
        env=env,
        capture_output=True,
        text=True,
        check=False,
        timeout=120,
    )


def test_annotate_extra_reaches_daemon_plugin_and_summary(tmp_path: Path) -> None:
    """End-to-end through both in-engine seams: conftest's annotate hook writes `extra` in the
    workers, the daemon plugin sees it in `run_info["results"]` (bus round-trip proven), and the
    plugin's line lands INSIDE the summary box (before the closing rule)."""
    project, out = tmp_path / "proj", tmp_path / "out"
    project.mkdir()
    _make_seams_project(project, out)

    proc = _run_local(project, {"PYTEST_FAST_DAEMON_PLUGINS": "gateplug"})
    assert proc.returncode == 0, proc.stdout + proc.stderr
    assert "⏱ gateplug: saw 2 results" in proc.stdout

    info = json.loads((out / "run_info.json").read_text())
    assert info["n"] == 2
    assert info["total"] == 2
    assert info["workers"] == 2
    assert info["have_cpu"] is True, "lean per-test cpu must ship on every run"
    assert info["markers"] == [
        "annotated:tests/test_s.py::test_a",
        "annotated:tests/test_s.py::test_b",
    ]

    # Spliced INSIDE the box, right after the stats block: bus line above, closing rule below —
    # plugin verdicts are summary-grade signal, detail dumps (FAILURES/SLOWEST) stay lower.
    lines = proc.stdout.splitlines()
    bus_at = next(i for i, ln in enumerate(lines) if ln.startswith("  bus     :"))
    gate_at = next(i for i, ln in enumerate(lines) if "gateplug: saw" in ln)
    closing_at = max(i for i, ln in enumerate(lines) if ln.startswith("═"))
    assert bus_at < gate_at < closing_at, "plugin section must sit between the stats block and the closing rule"


def test_daemon_plugin_failures_degrade_to_visible_warnings(tmp_path: Path) -> None:
    """Import error / missing symbol / raising impl → one ⚠ line each in the summary; the run
    itself stays green (rc 0) and healthy plugins in the same list still contribute."""
    project, out = tmp_path / "proj", tmp_path / "out"
    project.mkdir()
    _make_seams_project(project, out)
    (project / "emptyplug.py").write_text("HOOKLESS = True\n")
    (project / "badplug.py").write_text(
        "def pytest_fast_run_completed(run_info):\n    raise RuntimeError('gate exploded')\n"
    )

    plugins = "definitely_missing_mod,emptyplug,badplug,gateplug"
    proc = _run_local(project, {"PYTEST_FAST_DAEMON_PLUGINS": plugins})
    assert proc.returncode == 0, proc.stdout + proc.stderr
    assert "⚠ daemon plugin 'definitely_missing_mod': import failed" in proc.stdout
    assert "⚠ daemon plugin 'emptyplug': no pytest_fast_run_completed" in proc.stdout
    assert "⚠ daemon plugin 'badplug': pytest_fast_run_completed raised" in proc.stdout
    assert "⏱ gateplug: saw 2 results" in proc.stdout, "healthy plugin must still run after broken ones"


# ── resident daemon: the client seam (`want_results`) ────────────────────────


def _spawn_daemon_proc(pf_cmd: list[str], *, address: str, cwd: Path) -> subprocess.Popen[bytes]:
    """Spawn a resident daemon subprocess (same shape as test_daemon_lifecycle's helper)."""
    log_path = Path(address.removesuffix(".sock") + "-daemon.log")
    env = os.environ.copy()
    env["PYTEST_FAST_ROOT"] = str(cwd)
    cmd = [*pf_cmd, "--serve", "--address", address, "--ttl", "30", "--workers", "2"]
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
def seams_daemon(
    tmp_path: Path,
    tmp_address: str,
    pf_cmd: list[str],
    monkeypatch: pytest.MonkeyPatch,
) -> Iterator[Path]:
    """A resident daemon collected over the seams project (2 passing tests, annotate conftest).
    `PYTEST_FAST_ROOT` is monkeypatched in the test process too, so the client fingerprint
    matches the daemon's boot fingerprint."""
    project, out = tmp_path / "proj", tmp_path / "out"
    project.mkdir()
    _make_seams_project(project, out)
    monkeypatch.setenv("PYTEST_FAST_ROOT", str(project))
    monkeypatch.delenv("PYTEST_FAST_DAEMON_PLUGINS", raising=False)
    proc = _spawn_daemon_proc(pf_cmd, address=tmp_address, cwd=project)
    try:
        if not _await_ready(tmp_address, proc, timeout=30.0):
            log_path = Path(tmp_address.removesuffix(".sock") + "-daemon.log")
            log_text = log_path.read_text() if log_path.exists() else "<no log>"
            pytest.fail(f"daemon did not become ready in 30s. log:\n{log_text}")
        yield project
    finally:
        if proc.poll() is None:
            _shutdown_daemon(tmp_address)
            try:
                proc.wait(timeout=10.0)
            except subprocess.TimeoutExpired:
                proc.kill()
                proc.wait(timeout=5.0)


def test_want_results_ships_lean_results_stats_and_meta(seams_daemon: Path, tmp_address: str) -> None:
    """`want_results=True` → the final frame carries lean results (duration/cpu/extra present,
    heavy `reports` never), worker stats and run meta. Without the flag the frame stays plain —
    the old wire shape is untouched."""
    frame = request_run(tmp_address, want_results=True)
    assert frame.get("rc") == 0, f"expected green run, got {frame.get('summary')}"

    results = cast("list[dict[str, object]]", frame["results"])
    assert sorted(str(r["nodeid"]) for r in results) == [
        "tests/test_s.py::test_a",
        "tests/test_s.py::test_b",
    ]
    for r in results:
        assert r["outcome"] == "passed"
        assert isinstance(r["duration"], float)
        assert isinstance(r["cpu"], float), "lean cpu must ride along for wrapper clients"
        assert "reports" not in r, "want_results must ship the LEAN projection"
        extra = cast("dict[str, object]", r["extra"])
        assert str(extra["marker"]).startswith("annotated:tests/test_s.py::test_")

    stats = cast("list[dict[str, object]]", frame["worker_stats"])
    assert sum(int(cast("int", s["ran"])) for s in stats) == 2

    meta = cast("dict[str, object]", frame["run_meta"])
    assert meta["total"] == 2
    assert meta["num_workers"] == 2
    assert meta["start_method"] == "forkserver"
    assert isinstance(meta["run_wall"], float)

    plain = request_run(tmp_address)
    assert plain.get("rc") == 0
    assert "results" not in plain
    assert "worker_stats" not in plain
    assert "run_meta" not in plain
