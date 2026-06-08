"""The `--bench` deterministic bottleneck report: phase split, the lever rules, and N-run
averaging. Pure (`_bench_report` over synthetic results) — no daemon."""

from __future__ import annotations

from pytest_fast import RunResult, _bench_report, _phase_split, _top_profile_rows


def _r(nodeid: str, *, cpu: float, setup: float = 0.0, call: float = 0.0, teardown: float = 0.0) -> RunResult:
    reports: list[dict[str, object]] = []
    if setup:
        reports.append({"when": "setup", "duration": setup, "nodeid": nodeid})
    reports.append({"when": "call", "duration": call, "nodeid": nodeid})
    if teardown:
        reports.append({"when": "teardown", "duration": teardown, "nodeid": nodeid})
    return {"nodeid": nodeid, "outcome": "passed", "duration": setup + call + teardown, "cpu": cpu, "reports": reports}


def test_phase_split_sums_by_when() -> None:
    r = _r("a::b", cpu=0.1, setup=0.3, call=1.0, teardown=0.05)
    assert _phase_split(r) == (0.3, 1.0, 0.05)
    assert _phase_split({"nodeid": "x", "outcome": "passed", "duration": 0.0}) == (0.0, 0.0, 0.0)  # no reports


def test_bench_levers_and_classification() -> None:
    results: list[RunResult] = []
    # a shared-setup cluster: 6 tests in one file, each ~0.2s setup (the big reclaimable lever).
    for i in range(6):
        results.append(_r(f"tests/api.py::test_{i}", cpu=0.01, setup=0.2, call=0.02))
    # one I/O-bound slow test (call mostly off-CPU) and one CPU-bound slow test.
    results.append(_r("tests/x.py::test_io", cpu=0.05, call=2.0))  # 2.5% CPU
    results.append(_r("tests/x.py::test_cpu", cpu=1.9, call=2.0))  # 95% CPU

    blob = _bench_report([results], run=3.0, cores=4)
    assert "pytest-fast bench" in blob
    assert "best @ 4 cores" in blob
    assert "where time goes" in blob
    assert "SHARED SETUP" in blob and "tests/api.py" in blob
    assert "I/O-BOUND" in blob and "off-CPU" in blob
    assert "CPU-BOUND" in blob and "on-CPU" in blob
    # the I/O test (2s call) is the floor / a bigger lever than any single 0.2s setup
    assert "test_io" in blob
    # no heuristic cause-guessing leaked back in
    assert "N+1" not in blob and "missing index" not in blob


def test_bench_averages_across_runs_and_drops_to_zero_safely() -> None:
    # same test measured at 1.0s and 3.0s across two runs → averaged to 2.0s.
    run_a = [_r("t::slow", cpu=0.05, call=1.0)]
    run_b = [_r("t::slow", cpu=0.05, call=3.0)]
    blob = _bench_report([run_a, run_b], run=2.0, cores=2)
    assert "1 tests" in blob
    assert "2.00s" in blob  # averaged call/floor
    # empty input must not divide by zero
    assert "pytest-fast bench" in _bench_report([[]], run=0.0, cores=4)
    assert "pytest-fast bench" in _bench_report([], run=0.0, cores=4)


def test_bench_flags_unstable_timing_with_multiple_runs() -> None:
    # a steady test (1.0s every run) and a wildly unstable one (0.5/3.5/0.6 → cv high).
    runs = [
        [_r("t::steady", cpu=0.1, call=1.0), _r("t::jumpy", cpu=0.1, call=0.5)],
        [_r("t::steady", cpu=0.1, call=1.0), _r("t::jumpy", cpu=0.1, call=3.5)],
        [_r("t::steady", cpu=0.1, call=1.0), _r("t::jumpy", cpu=0.1, call=0.6)],
    ]
    blob = _bench_report(runs, run=2.0, cores=2)
    assert "unstable timing" in blob
    assert "t::jumpy" in blob
    assert "t::steady" not in blob.split("unstable timing")[1]  # the steady one is NOT flagged
    # with a single run, no variance data — point the user at more runs.
    one = _bench_report([[_r("t::steady", cpu=0.1, call=1.0)]], run=2.0, cores=2)
    assert "needs ≥2 measured runs" in one
    assert "p50" in one  # percentiles always present


def test_top_profile_rows_counts_calls_exactly() -> None:
    """cProfile call counts are exact → the deterministic N+1 / hot-call signal."""
    import cProfile

    def inner() -> int:
        return sum(range(50))

    def outer() -> None:
        for _ in range(47):
            inner()

    pr = cProfile.Profile()
    pr.enable()
    outer()
    pr.disable()
    rows = _top_profile_rows(pr, limit=30)
    inner_rows = [r for r in rows if "inner" in r[0]]
    assert inner_rows and inner_rows[0][1] == 47  # (label, ncalls, self, cum) — exact 47 calls


def test_bench_folds_profile_attribution_into_levers() -> None:
    results = [_r("t::slow", cpu=0.05, call=2.0)]
    profiles = {
        "t::slow": [
            ("<method 'recv' of 'socket'>", 8, 1.9, 1.95),
            ("get_subscribers (repo.py:21)", 47, 0.05, 0.9),  # high count = N+1, self-evident
        ]
    }
    blob = _bench_report([results], run=2.0, cores=2, profiles=profiles)
    assert "profile (top by SELF wall" in blob
    assert "recv" in blob
    assert "47×" in blob  # the call count makes N+1 visible without a guess
