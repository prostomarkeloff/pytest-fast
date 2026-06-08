"""The `--detailed` parallelism metrics: the pure aggregation (`_parallelism_metrics`) and its
render (`_detailed_par_lines`), plus an end-to-end check that the CLI flag toggles the block.

The metrics decompose the N×run rectangle into Σbusy (useful) + Σbus_wait (bus overhead) +
Σ(run−run_wall) (tail/straggler drain). These tests pin that decomposition and the I/O-vs-CPU
classification on synthetic worker stats — no daemon needed for the math.
"""

from __future__ import annotations

import pytest

from pytest_fast import (
    RunResult,
    WorkerStats,
    _detailed_par_lines,
    _par_verdict,
    _parallelism_metrics,
    _suggest_workers,
)


def _ws(n: int, *, busy: float, cpu: float, bus_wait: float, run_wall: float, ran: int) -> list[WorkerStats]:
    return [
        {"wid": i, "ran": ran, "busy": busy, "cpu": cpu, "bus_wait": bus_wait, "run_wall": run_wall} for i in range(n)
    ]


def _results(floor_dur: float, floor_id: str, n: int = 50) -> list[RunResult]:
    rs: list[RunResult] = [{"nodeid": f"t{i}", "outcome": "passed", "duration": 0.01} for i in range(n)]
    rs.append({"nodeid": floor_id, "outcome": "passed", "duration": floor_dur})
    return rs


def test_metrics_core_values_and_decomposition() -> None:
    """Exact values on clean numbers + the rectangle identity holds (useful + bus + tail +
    bookkeeping == N, all in '×' units)."""
    n, run = 4, 10.0
    ws = _ws(n, busy=9.0, cpu=4.5, bus_wait=0.5, run_wall=9.6, ran=100)
    m = _parallelism_metrics(ws, run=run, num_workers=n, results=_results(2.13, "pkg/test_x.py::test_slow"))

    assert m["par"] == pytest.approx(3.6)  # Σbusy/run = 36/10
    assert m["eff"] == pytest.approx(0.9)  # par/N
    assert m["cpu_par"] == pytest.approx(1.8)  # Σcpu/run = 18/10
    assert m["cpu_sat"] == pytest.approx(0.5)  # Σcpu/Σbusy = 18/36
    assert m["bus_lost"] == pytest.approx(0.2)  # Σbus_wait/run = 2/10
    assert m["tail_lost"] == pytest.approx(0.16)  # N − Σrun_wall/run = 4 − 38.4/10
    assert m["ideal_wall"] == pytest.approx(9.0)  # Σbusy/N
    assert m["floor"] == pytest.approx(2.13)
    assert m["floor_nodeid"] == "pkg/test_x.py::test_slow"

    # N = useful(par) + bus + tail + bookkeeping, every bucket ≥ 0.
    bookkeeping = (sum(w["run_wall"] - w["busy"] - w["bus_wait"] for w in ws)) / run
    assert m["par"] + m["bus_lost"] + m["tail_lost"] + bookkeeping == pytest.approx(n)
    assert m["bus_lost"] >= 0 and m["tail_lost"] >= 0 and bookkeeping >= 0


def test_par_never_exceeds_workers_and_floor_is_max_duration() -> None:
    ws = _ws(6, busy=10.0, cpu=10.0, bus_wait=0.0, run_wall=10.0, ran=200)  # perfectly saturated
    m = _parallelism_metrics(ws, run=10.0, num_workers=6, results=_results(3.0, "a::b"))
    assert m["par"] == pytest.approx(6.0)  # Σbusy/run = 60/10, capped at N by construction
    assert m["eff"] == pytest.approx(1.0)
    assert m["floor"] == pytest.approx(3.0)


def test_cpu_saturation_classifies_io_vs_compute() -> None:
    io = _parallelism_metrics(
        _ws(4, busy=10.0, cpu=0.6, bus_wait=0.0, run_wall=10.0, ran=10), 10.0, 4, _results(1.0, "a::b")
    )
    compute = _parallelism_metrics(
        _ws(4, busy=10.0, cpu=9.5, bus_wait=0.0, run_wall=10.0, ran=10), 10.0, 4, _results(1.0, "a::b")
    )
    assert io["cpu_sat"] < 0.4
    assert compute["cpu_sat"] > 0.75
    assert "I/O-bound" in "\n".join(_detailed_par_lines(io, 10.0, 4, cores=4, logical=8))
    assert "compute-bound" in "\n".join(_detailed_par_lines(compute, 10.0, 4, cores=4, logical=8))


def test_metrics_guard_empty_and_zero_run() -> None:
    """No ZeroDivision on an empty run / instant wall — every ratio degrades to 0."""
    empty = _parallelism_metrics([], run=0.0, num_workers=4, results=[])
    assert empty["par"] == 0.0
    assert empty["eff"] == 0.0
    assert empty["cpu_sat"] == 0.0
    assert empty["floor"] == 0.0
    assert empty["floor_nodeid"] == ""
    # workers present but run wall is 0 (instant) — still safe.
    z = _parallelism_metrics(_ws(2, busy=0.0, cpu=0.0, bus_wait=0.0, run_wall=0.0, ran=0), 0.0, 2, [])
    assert z["par"] == 0.0


def test_derived_quantities_absolute_and_shape() -> None:
    """Absolute seconds, idle/io core-equivalents, count-spread, wall-spread, and the tail
    distribution — the values that turn the '×' ratios into a readable diagnosis."""
    # 4 workers: busy 9.0 (cpu 4.5 → 50% on-CPU), bus_wait 0.5, run_wall 9.6; run 10.0.
    ws = _ws(4, busy=9.0, cpu=4.5, bus_wait=0.5, run_wall=9.6, ran=600)
    ws[0]["ran"] = 300  # introduce a 2x count spread at identical wall → healthy by-time balance
    m = _parallelism_metrics(ws, run=10.0, num_workers=4, results=_results(2.13, "pkg::test_slow"))
    assert m["busy_s"] == pytest.approx(36.0)
    assert m["cpu_s"] == pytest.approx(18.0)
    assert m["bus_wait_s"] == pytest.approx(2.0)  # 4 × 0.5
    assert m["drain_s"] == pytest.approx(4 * 10.0 - 4 * 9.6)  # N·run − Σrun_wall = 1.6
    assert m["idle_cores"] == pytest.approx(4 - 1.8)  # N − cpu_par (cpu_par = 18/10)
    assert m["io_cores"] == pytest.approx(3.6 - 1.8)  # par − cpu_par
    assert m["wall_spread"] == pytest.approx(0.0)  # all run_walls equal
    assert m["ran_ratio"] == pytest.approx(2.0)  # 600 / 300
    assert m["n_slow"] == 1 and m["p99"] >= 0.0  # only the floor test is ≥1s here

    blob = "\n".join(_detailed_par_lines(m, run=10.0, num_workers=4, cores=4, logical=8))
    for token in ("detail —", "eff", "cpu", "cores idle", "lost", "worker-s", "balance", "floor", "p99", "verdict"):
        assert token in blob, f"missing {token!r} in detailed block:\n{blob}"


def test_suggest_workers_formula_discounts_e_cores() -> None:
    """Pool size cores/cpu_sat, but capped at cores + (logical−cores)·(1−cpu_sat): the E-cores past
    `cores` count only in proportion to the I/O fraction (they run CPU work at ~half speed)."""
    # cpu_sat 0.6: pool ceil(10)=10, E-core cap 6 + 6·0.4 = 8.4 → 8 (NOT the raw 10).
    assert _suggest_workers(0.60, cores=6, logical=12) == 8
    # cpu_sat 0.2 (very I/O-bound): pool 30, E-core cap 6 + 6·0.8 = 10.8 → 11.
    assert _suggest_workers(0.20, cores=6, logical=12) == 11
    # cpu_sat 0.95 (CPU-bound): E-core cap 6.3 → 6, so no scale-up even though pool=7.
    assert _suggest_workers(0.95, cores=6, logical=12) == 6
    assert _suggest_workers(1.0, cores=6, logical=12) == 6  # fully CPU → cores
    assert _suggest_workers(0.0, cores=6, logical=12) == 6  # no CPU signal → cores
    # Uniform machine (cores == logical, e.g. Linux): cap collapses to cores — never oversubscribe.
    assert _suggest_workers(0.30, cores=8, logical=8) == 8
    assert _suggest_workers(0.50, cores=4, logical=4) == 4


def test_verdict_regimes_and_deterministic_suggestion() -> None:
    floor = _results(2.0, "slow::test")

    # CPU-saturated: bound by cores; never suggest more.
    cpu_bound = _parallelism_metrics(_ws(6, busy=10.0, cpu=9.8, bus_wait=0.0, run_wall=10.0, ran=50), 10.0, 6, floor)
    assert "CPU-saturated" in _par_verdict(cpu_bound, num_workers=6, cores=6, logical=12)

    # I/O slack at the default (6w = cores): a CONCRETE suggested count + the honest caveat.
    io_bound = _parallelism_metrics(_ws(6, busy=10.0, cpu=2.0, bus_wait=0.0, run_wall=10.0, ran=50), 10.0, 6, floor)
    v = _par_verdict(io_bound, num_workers=6, cores=6, logical=12)
    assert "--workers 11" in v  # pool 30, E-core cap 10.8 → 11
    assert "measure" in v and "E-core" in v  # honest caveat naming both traps, not an order

    # Already oversubscribed (>cores): NO number (cpu_sat is contention-depressed) — just describe.
    over = _parallelism_metrics(_ws(9, busy=8.0, cpu=4.0, bus_wait=0.0, run_wall=8.0, ran=50), 8.0, 9, floor)
    vo = _par_verdict(over, num_workers=9, cores=6, logical=12)
    assert "above 6 perf cores" in vo and "--workers" not in vo

    # Never recommend past the E-core-discounted cap (not the raw pool=60, not even logical=12).
    deep_io = _parallelism_metrics(_ws(6, busy=10.0, cpu=1.0, bus_wait=0.0, run_wall=10.0, ran=50), 10.0, 6, floor)
    assert "--workers 11" in _par_verdict(deep_io, num_workers=6, cores=6, logical=12)  # cap 6+6·0.9=11.4→11

    # Straggler wins over everything.
    ws = _ws(5, busy=9.0, cpu=8.0, bus_wait=0.0, run_wall=9.0, ran=50)
    ws.append({"wid": 5, "ran": 10, "busy": 15.0, "cpu": 14.0, "bus_wait": 0.0, "run_wall": 15.0})
    straggler = _parallelism_metrics(ws, 15.0, 6, _results(8.3, "slow::giant"))
    assert "straggler" in _par_verdict(straggler, num_workers=6, cores=6, logical=12)
    assert "UNEVEN" in "\n".join(_detailed_par_lines(straggler, 15.0, 6, cores=6, logical=12))
