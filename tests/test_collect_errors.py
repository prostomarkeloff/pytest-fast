"""Collection errors must fail closed — never a silently shortened green suite.

Regression source (observed in the wild on a large host repo): two test files with
ImportError were dropped from collection; the daemon served the shorter suite with a
green summary and rc=0 — no error text anywhere. These tests pin the fixed contract:
the broken file is named (with its traceback) in a `COLLECT ERRORS` block, rc=1, and
the *successfully collected* tests still run.
"""

from __future__ import annotations

import subprocess
from pathlib import Path


def _write_mixed_project(tmp_path: Path) -> None:
    """1 green file + 1 file that fails to import at collect time."""
    (tmp_path / "tests").mkdir()
    (tmp_path / "tests" / "test_green.py").write_text(
        "def test_a() -> None: assert True\ndef test_b() -> None: assert True\n",
    )
    (tmp_path / "tests" / "test_broken.py").write_text(
        "import module_that_does_not_exist_anywhere  # noqa: F401\n\ndef test_never_collected() -> None: ...\n",
    )
    (tmp_path / "pyproject.toml").write_text('[project]\nname = "tmp"\nversion = "0"\n')


def test_import_error_is_loud_and_red(tmp_path: Path, pf_cmd: list[str]) -> None:
    """The exact silent-swallow scenario: rc must be 1, the block must name the broken
    file with its ImportError traceback, and the green tests must still have run."""
    _write_mixed_project(tmp_path)
    proc = subprocess.run(
        [*pf_cmd, "--workers", "2", "--runs", "1"],
        cwd=tmp_path,
        capture_output=True,
        text=True,
        check=False,
        timeout=120,
    )
    assert proc.returncode == 1, f"collect error must be red\nstdout:\n{proc.stdout}\n\nstderr:\n{proc.stderr}"
    assert "COLLECT ERRORS (1)" in proc.stdout, f"missing collect-errors block:\n{proc.stdout}"
    assert "test_broken.py" in proc.stdout
    assert "module_that_does_not_exist_anywhere" in proc.stdout, "traceback must be shown"
    assert "2 passed" in proc.stdout, "collected tests must still run"
    assert "n=2/2" in proc.stdout, "n counts only collected tests; the gap is explained by the block"


def test_clean_project_has_no_collect_errors_block(tmp_path: Path, pf_cmd: list[str]) -> None:
    """Symmetric guard: a fully collectable project must not grow the block (and stays rc=0)."""
    (tmp_path / "tests").mkdir()
    (tmp_path / "tests" / "test_green.py").write_text("def test_a() -> None: assert True\n")
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
    assert "COLLECT ERRORS" not in proc.stdout
