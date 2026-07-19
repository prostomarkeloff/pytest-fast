"""End-to-end smoke for `python -m pytest_fast` via subprocess.

These are the most "expensive" tests — each one boots a forkserver + collect (~3s).
But they verify the contract a real user sees: `pytest-fast --runs 1` against a
tmp project with 1 pass + 1 fail must return rc=1 and print a summary with `n=2/2`."""

from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path


def test_help_exits_zero() -> None:
    """`-h` must return 0 and mention `--address`/`--serve` (CLI contract)."""
    proc = subprocess.run(
        [sys.executable, "-m", "pytest_fast", "--help"],
        capture_output=True,
        text=True,
        check=False,
        timeout=10,
    )
    assert proc.returncode == 0
    assert "--address" in proc.stdout
    assert "--serve" in proc.stdout


def test_local_run_against_tmp_project(tmp_project: Path, pf_cmd: list[str]) -> None:
    """Local mode (`--runs 1`): forkserver-preload + collect + 2 tests + summary.
    One test in tmp_project fails → rc=1; the output must contain `n=2/2`."""
    proc = subprocess.run(
        [*pf_cmd, "--workers", "2", "--runs", "1"],
        cwd=tmp_project,
        capture_output=True,
        text=True,
        check=False,
        timeout=120,
    )
    assert proc.returncode == 1, f"stdout:\n{proc.stdout}\n\nstderr:\n{proc.stderr}"
    assert "n=2/2" in proc.stdout, f"summary missing n=2/2:\n{proc.stdout}"
    assert "1 failed" in proc.stdout
    assert "1 passed" in proc.stdout
    assert "test_fail" in proc.stdout  # nodeid of the failing test in the FAILURES section


def test_local_run_all_green_when_no_failures(tmp_path: Path, pf_cmd: list[str]) -> None:
    """Symmetric: when all tests are green, rc=0 and there's no `FAILURES` section."""
    (tmp_path / "src").mkdir()
    (tmp_path / "tests").mkdir()
    (tmp_path / "tests" / "test_green.py").write_text(
        "def test_a() -> None: assert True\ndef test_b() -> None: assert 1 == 1\n",
    )
    (tmp_path / "pyproject.toml").write_text('[project]\nname = "tmp"\nversion = "0"\n')

    proc = subprocess.run(
        [*pf_cmd, "--workers", "2", "--runs", "1"],
        cwd=tmp_path,
        capture_output=True,
        text=True,
        check=False,
        timeout=120,
    )
    assert proc.returncode == 0, f"stdout:\n{proc.stdout}\n\nstderr:\n{proc.stderr}"
    assert "n=2/2" in proc.stdout
    assert "FAILURES" not in proc.stdout


def test_local_run_fails_closed_on_collection_error(tmp_path: Path, pf_cmd: list[str]) -> None:
    """A partially collected suite is never executed or reported as trusted."""
    tests_dir = tmp_path / "tests"
    tests_dir.mkdir()
    ran_marker = tmp_path / "valid-test-ran"
    (tests_dir / "test_valid.py").write_text(
        f"from pathlib import Path\ndef test_valid() -> None:\n    Path({str(ran_marker)!r}).write_text('ran')\n",
    )
    (tests_dir / "test_broken.py").write_text("import module_that_does_not_exist\n")
    (tmp_path / "pyproject.toml").write_text('[project]\nname = "tmp"\nversion = "0"\n')

    proc = subprocess.run(
        [*pf_cmd, "--workers", "2", "--runs", "1"],
        cwd=tmp_path,
        capture_output=True,
        text=True,
        check=False,
        timeout=120,
    )

    output = f"{proc.stdout}\n{proc.stderr}"
    assert proc.returncode != 0, output
    assert not ran_marker.exists(), output
    assert "collection" in output.lower(), output


def test_detailed_flag_toggles_parallelism_block(tmp_path: Path, pf_cmd: list[str]) -> None:
    """`--detailed` adds the extended parallelism block (eff / CPU vs I/O / lost / floor); a plain
    run stays lean. Drives the real CLI → daemon-rendered summary path end to end."""
    (tmp_path / "tests").mkdir()
    (tmp_path / "tests" / "test_green.py").write_text(
        "def test_a() -> None: assert True\ndef test_b() -> None: assert True\n",
    )
    (tmp_path / "pyproject.toml").write_text('[project]\nname = "tmp"\nversion = "0"\n')

    def run(*extra: str) -> str:
        proc = subprocess.run(
            [*pf_cmd, "--workers", "2", "--runs", "1", *extra],
            cwd=tmp_path,
            capture_output=True,
            text=True,
            check=False,
            timeout=120,
        )
        assert proc.returncode == 0, f"stdout:\n{proc.stdout}\n\nstderr:\n{proc.stderr}"
        return proc.stdout

    lean = run()
    assert "detail —" not in lean and "I/O" not in lean, f"lean run leaked the detailed block:\n{lean}"

    detailed = run("--detailed")
    for token in ("detail —", "eff", "cpu", "lost", "floor"):
        assert token in detailed, f"--detailed missing {token!r}:\n{detailed}"


def test_local_run_dump_writes_outcome_json(tmp_project: Path, pf_cmd: list[str]) -> None:
    """`--dump PATH` must write `{nodeid: outcome}` JSON — that's the reference for
    outcome-diff against xdist."""
    dump = tmp_project / "outcomes.json"
    proc = subprocess.run(
        [*pf_cmd, "--workers", "2", "--runs", "1", "--dump", str(dump)],
        cwd=tmp_project,
        capture_output=True,
        text=True,
        check=False,
        timeout=120,
    )
    assert proc.returncode == 1
    assert dump.exists(), f"dump file not written:\n{proc.stdout}"
    # nodeids are relative to cwd = tmp_project → 'tests/test_t.py::test_pass'
    outcomes: dict[str, str] = json.loads(dump.read_text())
    assert any(nodeid.endswith("::test_pass") for nodeid in outcomes)
    assert any(nodeid.endswith("::test_fail") for nodeid in outcomes)
    failing = next(v for k, v in outcomes.items() if k.endswith("::test_fail"))
    passing = next(v for k, v in outcomes.items() if k.endswith("::test_pass"))
    assert failing == "failed"
    assert passing == "passed"
