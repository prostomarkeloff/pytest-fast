"""pytest-fast — resident forkserver-based test accelerator (single-file, xdist alt).

Why not xdist: xdist cold-spawns N workers, EACH re-imports the app graph
(~4.5s × N CPU/run). Why not bare fork(): on macOS fork-without-exec segfaults
inside CoreFoundation/SystemConfiguration (psycopg2→getaddrinfo, httpx→getproxies).
The solution — **forkserver** (POSIX default in modern Python): one clean SINGLE-THREADED
server process preloads the app AND COLLECTS TESTS ONCE, forks workers off itself →
warm imports + a pre-built ITEMS list, no thread/framework fork crashes.

Socket address and TTL are passed by the CALLER, not baked in.
Modes (CLI `pytest-fast` or `python -m pytest_fast`):
  * `--address X` (ensure+run):     connect to a daemon at X (spawn one with `--ttl`
        if absent) → run, print summary. Warmup on reruns ≈ fork(). With `--with-watcher`
        also spawns a background watcher (pre-warm on source changes).
  * `--serve --address X --ttl N`:  be the resident daemon: collect ONCE, hold a warm
        forkserver. src/tests changed OR relevant env (addopts + any prefix listed in
        `PYTEST_FAST_ENV_PREFIXES`, see `_env_fingerprint`) changed → daemon replies
        {'stale'} and exits (the client will spawn a fresh one). idle>N seconds → exit.
        Control protocol: run / status / shutdown / promote (see `serve`).
  * `--watch --address X --ttl N`:  (internal) resident watcher: poll mtime →
        debounce → staging-promote the daemon (boot a successor on a staging socket,
        verify collect, soft-shutdown the old one, promote to canonical). Exits once
        the daemon is gone via its own idle-ttl. Single-instance via flock.
  * `--runs N` / `--dump PATH`:     local in-process run.

Also a pytest plugin (auto-loaded via the `pytest11` entry point):
  * `pytest --fast`:                run the suite through the resident daemon while staying a
        real pytest session — the daemon streams full per-phase reports, which we republish
        through the controller's hooks → fully NATIVE reporting (terminalreporter, --durations,
        -v/-s, junit, plugins, exit code) on top of warm fork-server execution. Forwards the
        collected selection (-k/-m). `--fast-address/-workers/-ttl/-watch` tune it. Inert
        unless `--fast` is passed (so a plain `pytest` run is unaffected).
  * `OUTCOME_DUMP=PATH pytest -p pytest_fast`: writes {nodeid: outcome} — a reference dump for
        outcome-diff comparison against xdist.

Behaviorally identical to xdist (same test set; marks/skip/xfail/reruns 1-to-1 —
runs go through the FULL pytest protocol `pytest_runtest_protocol`); reports are lossy.

⚠ macOS fork safety: code that resolves `localhost` via `getaddrinfo` inside a fork
will segfault (mDNS/CoreFoundation init). If your app code does this, pre-resolve to
a numeric IP (e.g. `127.0.0.1`) in your config — pytest-fast doesn't auto-rewrite.
"""

from __future__ import annotations

import os

# macOS fork-safety (no-op on Linux): no_proxy=* routes getproxies through the env path,
# bypassing SystemConfiguration; the OBJC guard suppresses ObjC initialize.
os.environ.setdefault("no_proxy", "*")
os.environ.setdefault("NO_PROXY", "*")
os.environ.setdefault("OBJC_DISABLE_INITIALIZE_FORK_SAFETY", "YES")

import argparse
import fcntl
import hashlib
import json
import math
import multiprocessing as mp
import pickle
import selectors
import socket
import struct
import subprocess
import sys
import tempfile
import threading
import time
import uuid
from contextlib import contextmanager, suppress
from multiprocessing.process import BaseProcess
from pathlib import Path
from typing import TYPE_CHECKING, NamedTuple, NotRequired, Protocol, TypedDict, cast

import pluggy

if TYPE_CHECKING:
    import cProfile
    from collections.abc import Callable, Iterable, Iterator
    from multiprocessing.context import DefaultContext
    from types import ModuleType

    from _pytest.config import Config
    from _pytest.config.argparsing import Parser
    from _pytest.main import Session
    from _pytest.nodes import Item
    from _pytest.reports import CollectReport, TestReport

    class _ActiveItemSlots(Protocol):
        """Shared per-worker item indices used only as crash evidence."""

        def __len__(self) -> int: ...

        def __getitem__(self, index: int) -> int: ...

        def __setitem__(self, index: int, value: int) -> None: ...

    class _WorkerConfig(Protocol):
        """The xdist-style worker attributes we graft onto a configured `Config` post-fork.
        pytest's `Config` doesn't declare these (xdist adds them dynamically); casting to this
        Protocol lets us set them without a `# type: ignore`, matching this module's cast convention."""

        workerinput: dict[str, object]
        workeroutput: dict[str, object]


# Public API. Everything with a `_` prefix is an implementation detail (NOT covered by
# this package's semver promises). Tests/self-test code uses `_*`-names intentionally,
# but downstream consumers should rely on this list.
__all__ = [
    "Daemon",
    "RunResult",
    "SummaryBlock",  # named piece of the summary box (the daemon-plugin render contract)
    "WorkerStats",
    "categorize",
    "default_workers",  # public worker-count API (auto-detect, ignores overrides)
    "main",
    "main_cli",
    "request_run",  # client-side, module-level
    "request_run_results",  # quiet compact-result client for wrapper tools
    "request_run_streamed",  # client-side streaming (used by the --fast plugin controller)
    "resolve_workers",  # public worker-count API (full precedence; stable for external tooling)
    "run_via_daemon",  # client-side ensure/stale-respawn orchestration (stable for external tooling)
]


class RunResult(TypedDict):
    """A single test outcome, shipped over the worker→master bus (pickle-serialized)."""

    nodeid: str
    outcome: str
    duration: float
    # Maximum duration of each pytest phase. Compact clients get this without serializing TestReport;
    # a phase can be absent when setup/call short-circuits.
    phases: NotRequired[dict[str, float]]
    # Per-test process CPU time (duration − cpu ≈ I/O wait). Stamped by the standard worker loop
    # on EVERY run (lean included) — consumers (`bench` I/O-vs-CPU, timing gates via
    # `want_results`) may rely on it there; absent in `--persist-workers` mode.
    cpu: NotRequired[float]
    # cProfile rows (qualname, ncalls, tottime, cumtime) for the test, top-by-cumtime — only on the
    # targeted profiling pass `bench` runs over its top bottleneck tests. Deterministic call counts.
    profile: NotRequired[list[tuple[str, int, float, float]]]
    longrepr: NotRequired[str]  # failure text — only for failed/error
    # Every phase report in pytest's serializable wire form (plain builtins, whitelist-safe),
    # present only in full-report mode — lets the master/controller replay them through a real
    # terminalreporter (--durations, junit, -v/-s, plugins). See `_run_one_item(full_report=...)`.
    reports: NotRequired[list[dict[str, object]]]
    # Plugin-attached per-test payload — the write channel of the `pytest_fast_annotate_result`
    # worker hook (see `_PytestFastHookSpecs`). Custom measurements ride the existing bus next to
    # duration/cpu and surface to daemon plugins (`PYTEST_FAST_DAEMON_PLUGINS`) and to clients
    # that requested `want_results`. Absent when no plugin wrote anything.
    extra: NotRequired[dict[str, object]]


class WorkerStats(TypedDict):
    """Worker summary emitted at the end of a run (drives the par. metric + `--detailed` block).

    `busy` is the WALL time spent inside the runtest protocol; `cpu` is this worker process's
    CPU time over the same span (busy − cpu ≈ time blocked on I/O, e.g. a DB round-trip — that's
    what makes par look full while cores idle). `bus_wait` is time the worker sat idle between
    tests waiting for the master to hand out the next index. `run_wall` ≈ busy + bus_wait +
    bookkeeping, so the rectangle N×run decomposes into Σbusy (useful) + Σbus_wait (bus overhead)
    + Σ(run−run_wall) (tail/straggler drain)."""

    wid: int
    ran: int
    busy: float
    cpu: float
    bus_wait: float
    run_wall: float


class _RunOutcome(NamedTuple):
    """Raw output of one fork→serve→collect cycle (`Daemon._execute_run`), before rendering —
    shared by the single-run summary path and the N-run `--bench` aggregation."""

    results: list[RunResult]
    worker_stats: list[WorkerStats]
    bus: dict[str, float]
    warmup: float  # fork+spawn time (t_ready − t0)
    run_wall: float  # execution wall (t_done − t_ready)
    total: int  # items actually dispatched in this run
    planned_total: int  # selected items before an optional stop-first short-circuit
    short_circuited: bool
    idx: int
    exitcodes: list[int | None]
    process_failures: list[str]


class _PersistPool(NamedTuple):
    """A warm worker pool held across serve `run` requests (opt-in ``--persist-workers``). Workers are
    forked ONCE; each holds a long-lived pytest session, so a session-scoped fixture set up once is
    reused for every request. ``total``/``nodeids`` come from the workers' one-time 'ready' frame."""

    server: socket.socket
    sock_path: str
    procs: list[BaseProcess]
    conns: list[socket.socket]
    total: int
    nodeids: list[str] | None
    active_items: _ActiveItemSlots


_ACTIVE_ITEM_IDLE = -1
_ACTIVE_ITEM_COMPLETED = -2


def _process_failure_nodeids(
    active_items: _ActiveItemSlots,
    nodeids: list[str] | None,
    results: list[RunResult],
) -> list[str]:
    """Resolve items whose workers exited while their pytest protocol was active."""
    if nodeids is None:
        return []
    completed = {result["nodeid"] for result in results}
    failures: list[str] = []
    for slot in range(len(active_items)):
        index = active_items[slot]
        if not isinstance(index, int) or not 0 <= index < len(nodeids):
            continue
        nodeid = nodeids[index]
        if nodeid not in completed and nodeid not in failures:
            failures.append(nodeid)
    return failures


class ParMetrics(TypedDict):
    """Derived parallelism metrics behind the `--detailed` block. Ratios are in '×' units
    (worker-equivalents): a worker-seconds quantity divided by the run wall. See `WorkerStats`
    for the N×run decomposition these come from."""

    par: float  # Σbusy / run — effective parallel speedup (≤ num_workers)
    eff: float  # par / num_workers — parallel efficiency in 0..1
    cpu_par: float  # Σcpu / run — cores' worth of CPU actually burned
    cpu_sat: float  # Σcpu / Σbusy — fraction of test-wall spent on-CPU (low → I/O-bound)
    bus_lost: float  # Σbus_wait / run — parallelism lost to inter-test bus round-trips
    tail_lost: float  # num_workers − Σrun_wall/run — lost to end-of-run straggler drain
    ideal_wall: float  # Σbusy / num_workers — best wall a work-conserving scheduler could reach
    busy_s: float  # Σbusy — total test-execution worker-seconds (serial-equivalent time)
    cpu_s: float  # Σcpu — total CPU-seconds
    bus_wait_s: float  # Σbus_wait — worker-seconds idle on the bus between tests
    drain_s: float  # N·run − Σrun_wall — worker-seconds idle while stragglers finish
    idle_cores: float  # num_workers − cpu_par — core-equivalents NOT computing (I/O wait + idle)
    io_cores: float  # par − cpu_par — workers in a test but blocked on I/O (not on CPU)
    run_wall_min: float
    run_wall_max: float
    wall_spread: float  # (run_wall_max − run_wall_min) / run — load imbalance, 0 = perfect
    ran_min: int
    ran_max: int
    ran_ratio: float  # ran_max / ran_min — test-COUNT spread (high + low wall_spread = healthy)
    floor: float  # longest single test — a hard lower bound on wall at ANY worker count
    floor_nodeid: str
    n_slow: int  # tests ≥ 1s — the heavy tail
    p99: float  # 99th-percentile single-test duration


def _parallelism_metrics(
    worker_stats: list[WorkerStats], run: float, num_workers: int, results: list[RunResult]
) -> ParMetrics:
    """Pure aggregation of worker stats → the `--detailed` parallelism metrics. Kept separate from
    rendering so it's unit-testable without a live daemon. Every division is guarded (run / Σbusy /
    num_workers / ran_min can be 0 on an empty or instant run)."""
    sum_busy = sum(s["busy"] for s in worker_stats)
    sum_cpu = sum(s["cpu"] for s in worker_stats)
    sum_bus = sum(s["bus_wait"] for s in worker_stats)
    sum_run_wall = sum(s["run_wall"] for s in worker_stats)
    rans = [s["ran"] for s in worker_stats]
    run_walls = [s["run_wall"] for s in worker_stats]
    durs = sorted(r["duration"] for r in results)
    floor, floor_nodeid = max(((r["duration"], r["nodeid"]) for r in results), default=(0.0, ""))
    par = sum_busy / run if run else 0.0
    cpu_par = sum_cpu / run if run else 0.0
    ran_min, ran_max = (min(rans), max(rans)) if rans else (0, 0)
    rw_min, rw_max = (min(run_walls), max(run_walls)) if run_walls else (0.0, 0.0)
    p99 = durs[min(len(durs) - 1, round(0.99 * (len(durs) - 1)))] if durs else 0.0
    return {
        "par": par,
        "eff": (par / num_workers) if num_workers else 0.0,
        "cpu_par": cpu_par,
        "cpu_sat": sum_cpu / sum_busy if sum_busy else 0.0,
        "bus_lost": sum_bus / run if run else 0.0,
        "tail_lost": max(0.0, num_workers - sum_run_wall / run) if run else 0.0,
        "ideal_wall": sum_busy / num_workers if num_workers else 0.0,
        "busy_s": sum_busy,
        "cpu_s": sum_cpu,
        "bus_wait_s": sum_bus,
        "drain_s": max(0.0, num_workers * run - sum_run_wall),
        "idle_cores": max(0.0, num_workers - cpu_par),
        "io_cores": max(0.0, par - cpu_par),
        "run_wall_min": rw_min,
        "run_wall_max": rw_max,
        "wall_spread": (rw_max - rw_min) / run if run else 0.0,
        "ran_min": ran_min,
        "ran_max": ran_max,
        "ran_ratio": ran_max / ran_min if ran_min else 1.0,
        "floor": floor,
        "floor_nodeid": floor_nodeid,
        "n_slow": sum(1 for d in durs if d >= 1.0),
        "p99": p99,
    }


def _suggest_workers(cpu_sat: float, cores: int, logical: int) -> int:
    """Deterministic pool size (NOT a heuristic): if each worker is CPU-busy only `cpu_sat` of its
    wall (the rest blocked on I/O), keeping `cores` cores saturated needs ≈`cores / cpu_sat` workers —
    so that at any instant ~`cores` are in their CPU phase while the others overlap I/O (Little's-law
    pool sizing).

    But the cap is NOT the raw logical-core count. On big.LITTLE the workers past `cores` (the perf
    cores) land on slower E-cores: empirically they add CPU-seconds (the work runs at ~half speed)
    and only pay off by hiding the I/O-WAIT fraction `(1 − cpu_sat)`, not by adding CPU throughput.
    So the ceiling is `cores + (logical − cores)·(1 − cpu_sat)` — the E-cores discounted by how
    I/O-bound the suite is. A CPU-bound suite (cpu_sat→1) caps at `cores`; an I/O-bound one
    (cpu_sat→0) can reach toward `logical`. On a uniform machine `cores == logical`, so it collapses
    to a plain `cores` cap (no oversubscription suggestion — the right answer there too).
    Verified on PRM2: raw `cores/cpu_sat` said 10 and 10w REGRESSED on per-db (E-core tax); the
    discounted cap lands at ~8."""
    if cpu_sat <= 0:
        return cores
    pool = math.ceil(cores / cpu_sat)  # ideal if extra workers had free, full-speed cores
    e_core_cap = cores + (logical - cores) * (1.0 - cpu_sat)  # discount E-cores by the I/O fraction
    return max(cores, min(pool, round(e_core_cap)))


def _par_verdict(m: ParMetrics, num_workers: int, cores: int, logical: int) -> str:
    """One-line synthesis → what to actually do. DESCRIPTIVE, not over-claiming: the worker-count
    suggestion is a deterministic formula (`_suggest_workers`), shown ONLY in the clean regime
    (`num_workers ≤ cores`, where cpu_sat isn't depressed by oversubscription) and always with the
    shared-resource caveat (the I/O overlap only pays off if the resource scales). Priority: a
    straggler tail is the loudest problem; then overhead; then the CPU-vs-I/O ceiling."""
    if m["wall_spread"] > 0.15 and m["floor_nodeid"]:
        return (
            f"straggler — walls spread {m['wall_spread']:.0%}; tail likely "
            f"{m['floor_nodeid']} ({m['floor']:.1f}s). Split/redistribute it."
        )
    if m["eff"] < 0.90:
        return "low efficiency — bus chatter (tests too short?) or oversubscribed past useful work."
    cpu_sat = m["cpu_sat"]
    if cpu_sat >= 0.85:
        return f"CPU-saturated ({cpu_sat:.0%} on-CPU) — bound by {cores} cores; more workers won't help."
    if num_workers > cores:
        # Already above perf cores → cpu_sat is contention-depressed; don't trust it for a number.
        return (
            f"running {num_workers}w above {cores} perf cores — CPU can't speed past {cores}; "
            f"extra workers only overlap I/O (watch contention / E-core stragglers)."
        )
    w_opt = _suggest_workers(cpu_sat, cores, logical)
    if w_opt > num_workers:
        return (
            f"{cpu_sat:.0%} CPU/test → your {cores} cores sit ~{1 - cpu_sat:.0%} idle on I/O; "
            f"≈{w_opt} workers may overlap that (pool size cores÷CPU-frac, E-cores past {cores} "
            f"discounted by the I/O fraction). Try --workers {w_opt} — measure: a shared DB or the "
            f"E-core tax can still cancel the gain."
        )
    return f"near-optimal at {num_workers}w — wall ≈ work/cores, little headroom."


def _detailed_par_lines(m: ParMetrics, run: float, num_workers: int, cores: int, logical: int) -> list[str]:
    """Render the `--detailed` parallelism block. Beyond the raw ratios it surfaces: absolute
    idle-seconds (not just '×'), idle core-equivalents (I/O-wait headroom — a FACT; the verdict
    decides what it means), a balance read (count-spread is healthy when wall-spread is tiny —
    work-stealing balances by TIME), the duration tail (floor is only the max), and a verdict with
    a deterministic worker-count suggestion. `cores` = perf cores, `logical` = the hard cap."""
    if m["cpu_sat"] >= 0.75:
        bound = "compute-bound"
    elif m["cpu_sat"] <= 0.40:
        bound = "I/O-bound"
    else:
        bound = "mixed"
    if m["wall_spread"] <= 0.10:
        balance = f"by time — counts vary {m['ran_ratio']:.1f}x, walls within {m['wall_spread']:.0%} (healthy)"
    else:
        balance = f"UNEVEN — walls spread {m['wall_spread']:.0%} (straggler — see verdict)"
    return [
        "  detail —",
        f"    eff     : {m['eff']:.0%}   (ideal wall {m['ideal_wall']:.2f}s vs {run:.2f}s actual)",
        f"    cpu     : {m['cpu_par']:.2f}x of {num_workers}  ·  {m['cpu_sat']:.0%} CPU / {1 - m['cpu_sat']:.0%} I/O "
        f"({bound})  ·  ~{m['idle_cores']:.1f} cores idle ({m['io_cores']:.1f} on I/O)",
        f"    lost    : {m['bus_wait_s'] + m['drain_s']:.2f} worker-s idle  =  "
        f"bus {m['bus_wait_s']:.2f}s + tail {m['drain_s']:.2f}s",
        f"    balance : {balance}  (ran {m['ran_min']}–{m['ran_max']}/w)",
        f"    floor   : {m['floor']:.2f}s  {m['floor_nodeid']}  ·  {m['n_slow']} tests ≥1s, p99 {m['p99']:.2f}s",
        f"    verdict : {_par_verdict(m, num_workers, cores, logical)}",
    ]


# ── logging helper ───────────────────────────────────────────────────────────


def _log(tag: str, msg: str) -> None:
    """Timestamped log line — for daemon/watcher lifecycle messages. We avoid the
    `logging` module on purpose: extra overhead and another init point in
    forkserver-preload. `flush=True` is mandatory — otherwise with
    `subprocess.Popen` stdout→file the lines may get stuck in the buffer until the
    process exits."""
    print(f"[{time.strftime('%H:%M:%S')}] [{tag}] {msg}", flush=True)


# ── AF_UNIX path-too-long workaround ─────────────────────────────────────────
#
# On macOS `sockaddr_un.sun_path` is only 104 bytes (108 on Linux). A long `address`
# (e.g. pytest's `tmp_path` under /private/var/folders/…) blows the limit → Python
# proactively raises `OSError: AF_UNIX path too long` BEFORE the syscall. Classic
# Unix trick: chdir into the dirname → bind/connect with the relative basename
# (10–20 bytes). The socket file physically sits at the same absolute path; the
# path in the kernel fits the limit. The context manager restores cwd.
#
# ⚠ chdir is process-wide. From multithreaded code do NOT call this from several
# threads at once. Our bind/connect AF_UNIX calls are synchronous (main thread of
# daemon, client, worker), so it's safe.

_AF_UNIX_SOFT_LIMIT = 100  # macOS hard limit is 104; leave headroom for padding/null-terminator/etc.

# Process-wide chdir is inherently race-prone (it affects the WHOLE process). The lock
# guarantees that two threads simultaneously bind/connecting to long paths won't
# chdir concurrently and trip each other's cwd. On single-threaded callsites
# (daemon, watcher, tests) the overhead of one uncontended lock.acquire is nanos.
_CHDIR_LOCK = threading.Lock()


@contextmanager
def _short_unix_path(address: str) -> Iterator[str]:
    """Yields a path usable for AF_UNIX bind/connect. Short — returned as is (no
    chdir side effect). Long — chdir into the dirname, yield the basename; cwd is
    restored on block exit even if an exception is raised. The process-wide chdir
    is wrapped in `_CHDIR_LOCK` so multithreaded clients don't race on cwd."""
    if len(address.encode("utf-8")) <= _AF_UNIX_SOFT_LIMIT:
        yield address
        return
    p = Path(address)
    with _CHDIR_LOCK:
        saved_cwd = os.getcwd()
        os.chdir(p.parent)
        try:
            yield p.name
        finally:
            os.chdir(saved_cwd)


# ── thin bus: length-prefixed pickle ─────────────────────────────────────────

# Hard cap on a single frame (header is uint32, uncapped that's up to 4GB). A corrupted
# or malicious frame with a huge `length` → an attempt to allocate gigabytes inside
# `_recvn`. In full-report mode whole serialized TestReports ride the bus (longrepr +
# captured stdout/stderr/log sections), so a single test that prints a lot can produce a
# chunky frame; 256MB is generous headroom while still bounding a corrupt/hostile header.
_MAX_FRAME_BYTES = 256 * 1024 * 1024

# Cap on the number of pickle opcodes `_loads` will accept (decode-amplification guard).
# `_MAX_FRAME_BYTES` bounds the WIRE size but NOT the decoded object size: pickle memo
# back-references let a small frame fan out into a large object graph, and a bogus
# length-prefixed opcode makes the C unpickler pre-allocate gigabytes from a ~5-byte frame.
# Every constructed node costs ≥1 opcode, so bounding the opcode count bounds both the
# decoded node count and the cost of `_is_plain_builtins`. 1M is 10×+ above any real frame
# (a forwarded selection of N tests is ~N opcodes; no suite has a million tests). Found by
# the Atheris harness.
_MAX_PICKLE_OPS = 1_000_000

# Opcodes whose integer arg drives a C-unpickler PRE-ALLOCATION (the memo array resize for
# PUT/GET indices, the read-ahead buffer for a FRAME length) rather than being plain data.
# The C unpickler allocates from these BEFORE bounds-checking, so a ~5-byte LONG_BINPUT with
# a 2-billion index grows the memo to ~18 GB. `_loads` rejects any whose arg exceeds the
# frame size — a valid pickle never references or declares more than its own bytes. (1-byte
# BINPUT/BINGET are capped at 255 → harmless; string/bytes lengths are already caught by
# genops, which reads the data and fails 'truncated' on a bogus length.) Found by Atheris.
_ALLOC_ARG_OPCODES = frozenset({"FRAME", "LONG_BINPUT", "PUT", "LONG_BINGET", "GET"})


# Whitelist for `_SafeUnpickler.find_class`. Our wire protocol carries:
#   - control messages: tuple/dict/str/int/float/bool/None/bytes
#   - test results: `RunResult`/`WorkerStats` — these are TypedDicts, plain `dict` at runtime
# No user-defined classes traverse the bus — so the whitelist is pure builtins.
# Any attempt to deserialize a non-builtin → `UnpicklingError`.
#
# Why: pickle = arbitrary code execution. The Unix socket under /tmp is connectable
# by any process owned by the current user. On a single-user dev box the surface
# is small, but on a shared CI runner (or if pytest-fast runs in a sandbox with
# elevated privileges) — a malicious local pickle → RCE. The whitelist closes this.
_PICKLE_ALLOWED_BUILTINS = frozenset(
    {
        "builtins.tuple",
        "builtins.dict",
        "builtins.list",
        "builtins.set",
        "builtins.frozenset",
        "builtins.str",
        "builtins.int",
        "builtins.float",
        "builtins.bool",
        "builtins.NoneType",
        "builtins.bytes",
        "builtins.bytearray",
        "builtins.complex",
    }
)


class _SafeUnpickler(pickle.Unpickler):
    """`pickle.Unpickler` with a `find_class` whitelist — only builtin types pass.
    Defense against malicious pickles on our bus (see `_PICKLE_ALLOWED_BUILTINS`)."""

    def find_class(self, module: str, name: str) -> object:
        qualname = f"{module}.{name}"
        if qualname in _PICKLE_ALLOWED_BUILTINS:
            return super().find_class(module, name)
        msg = f"forbidden class in pickle stream: {qualname}"
        raise pickle.UnpicklingError(msg)


# Concrete builtin VALUE types the bus legitimately carries. `_loads` rejects a decoded
# result containing anything else — notably a builtin *class* object. `find_class` must
# hand back builtin classes (`complex`, `frozenset`, …) so REDUCE can reconstruct their
# INSTANCES, but a frame that returns the class ITSELF as a value (`cbuiltins\ncomplex\n.`)
# is never legitimate protocol data. Not an RCE — every whitelisted class is a safe
# constructor — but it tightens the bus to plain data only (found by the Atheris harness).
_BUS_VALUE_TYPES = (type(None), bool, int, float, complex, str, bytes, bytearray, tuple, list, dict, set, frozenset)


def _is_plain_builtins(obj: object) -> bool:
    """Iterative check that `obj` is composed solely of plain builtin VALUES — no class /
    callable objects. Iterative (no recursion → no RecursionError on deep nesting) AND
    identity-deduplicated: a pickle memo 'billion laughs' DAG (`m=[m,m]` ×N) is tiny in
    memory via shared refs, but a naive walk visits 2^N paths and OOMs — tracking visited
    ids collapses it to the number of DISTINCT objects (bounded by `_MAX_PICKLE_OPS`)."""
    stack = [obj]
    seen: set[int] = set()
    while stack:
        o = stack.pop()
        oid = id(o)
        if oid in seen:
            continue
        seen.add(oid)
        if type(o) not in _BUS_VALUE_TYPES:
            return False
        if isinstance(o, dict):
            stack.extend(o.keys())
            stack.extend(o.values())
        elif isinstance(o, list | tuple | set | frozenset):
            stack.extend(o)
    return True


def _loads(data: bytes) -> object:
    """Safe analog of `pickle.loads` — routed through `_SafeUnpickler`, behind a pre-scan
    and a post-check. Three layers, three threats:

      * whitelist (`_SafeUnpickler.find_class`) → no RCE (only builtins deserialize);
      * pre-scan (`pickletools.genops`) → no decode-amplification OOM: the C unpickler
        pre-allocates a buffer for a length-prefixed opcode BEFORE checking the bytes are
        present, so a ~5-byte frame claiming gigabytes OOMs the process under
        `_MAX_FRAME_BYTES` entirely; genops reads from a BytesIO so a bogus length reads
        short and raises (no allocation), and `_MAX_PICKLE_OPS` caps memo-based fan-out;
      * post-check (`_is_plain_builtins`) → the result must be plain builtin *data*; a frame
        returning a builtin *class* as a value (allowed by find_class for REDUCE) is rejected.

    Both extra checks were driven by the Atheris harness (fuzz/fuzz_wire.py)."""
    import io
    import pickletools

    n_ops = 0
    data_len = len(data)
    try:
        for opcode, arg, _pos in pickletools.genops(data):
            n_ops += 1
            if n_ops > _MAX_PICKLE_OPS:
                msg = f"pickle exceeds the opcode budget ({_MAX_PICKLE_OPS})"
                raise pickle.UnpicklingError(msg)
            if isinstance(arg, int) and arg > data_len and opcode.name in _ALLOC_ARG_OPCODES:
                msg = f"{opcode.name} arg {arg} exceeds the {data_len}-byte frame (pre-allocation guard)"
                raise pickle.UnpicklingError(msg)
    except pickle.UnpicklingError:
        raise
    except Exception as exc:
        # genops raised on a malformed / oversized-length frame — reject before it can
        # reach the C unpickler and pre-allocate. Normalize to UnpicklingError so callers
        # (`_recv`) treat it as a corrupt frame, exactly like any other decode failure.
        msg = f"malformed pickle rejected before decode: {exc!r}"
        raise pickle.UnpicklingError(msg) from exc
    result = _SafeUnpickler(io.BytesIO(data)).load()
    if not _is_plain_builtins(result):
        msg = "decoded a non-data object (a builtin class/callable); the bus carries plain values only"
        raise pickle.UnpicklingError(msg)
    return result


def _send(sock: socket.socket, obj: object) -> int:
    data = pickle.dumps(obj, protocol=pickle.HIGHEST_PROTOCOL)
    sock.sendall(struct.pack("!I", len(data)) + data)
    return len(data) + 4


def _try_send(sock: socket.socket, obj: object) -> bool:
    """Best-effort control reply. A same-user peer that disconnects BEFORE reading its
    reply (e.g. a `status` client that hit its own recv timeout while the daemon was
    momentarily busy) would otherwise make `sendall` raise BrokenPipe/ConnectionReset
    and CRASH the resident daemon. The state change the reply accompanies (shutdown,
    promote, stale-exit) must still proceed, so we swallow the error and report it.
    Returns False if the peer was already gone."""
    try:
        _send(sock, obj)
    except OSError:
        return False
    return True


def _recvn(sock: socket.socket, n: int) -> bytes | None:
    buf = bytearray()
    while len(buf) < n:
        chunk = sock.recv(n - len(buf))
        if not chunk:
            return None
        buf.extend(chunk)
    return bytes(buf)


def _recv(sock: socket.socket) -> tuple[object, int]:
    header = _recvn(sock, 4)
    if header is None:
        return None, 0
    (length,) = struct.unpack("!I", header)
    # Guard BEFORE `_recvn(sock, length)`: otherwise a corrupted header with length=4GB
    # would allocate a 4GB bytearray inside `_recvn`. Return the same sentinel as for a
    # truncated payload — callers (master, daemon, client) already treat that as
    # "corrupted frame / peer gone" and close the connection.
    if length > _MAX_FRAME_BYTES:
        return None, 4
    payload = _recvn(sock, length)
    if payload is None:
        return None, 4
    try:
        return _loads(payload), length + 4
    except Exception:
        # A zero-length or corrupt/hostile payload makes `_loads` raise (EOFError,
        # UnpicklingError, …). That's a corrupted frame, not a fatal condition — return the
        # same sentinel as a truncated payload; callers (master, daemon, client) already
        # treat that as "corrupted frame / peer gone" and close the connection.
        return None, length + 4


# ── pytest-faithful test outcome categorization ──────────────────────────────

_OUTCOME_PRIORITY = {"error": 5, "failed": 4, "xpassed": 3, "xfailed": 2, "skipped": 1, "passed": 0}


def categorize(config: Config, reports: list[TestReport]) -> str:
    """Test category derived from its reports — same logic as pytest and plugins
    (skipping, rerunfailures), via the `pytest_report_teststatus` hook. We ignore
    'rerun' (intermediate retries) and pick the most significant final category."""
    best, best_p = "passed", -1
    for rep in reports:
        cat = config.hook.pytest_report_teststatus(report=rep, config=config)[0]
        if not cat or cat == "rerun":
            continue
        p = _OUTCOME_PRIORITY.get(cat)
        if p is None:
            continue  # unrecognized category (unknown third-party plugin) — don't let it win over passed
        if p > best_p:
            best, best_p = cat, p
    return best


class _ReportCollector:
    """Worker plugin: accumulates TestReports for the current item (pytest_runtest_logreport)."""

    def __init__(self) -> None:
        super().__init__()
        self.reports: list[TestReport] = []

    def pytest_runtest_logreport(self, report: TestReport) -> None:
        self.reports.append(report)


# ── collection-once (runs at import time as the preload module "pytest_fast") ─
#
# forkserver calls `set_forkserver_preload(["pytest_fast"])` → imports THIS file
# as module "pytest_fast" and runs collection ONCE; forked workers inherit the
# ready-made items. The `__name__ == "pytest_fast"` guard matters: when launched as
# a script the module is named "__main__"/"__mp_main__" (mp target resolution) —
# collection is NOT needed there (otherwise it would run twice). Workers read the
# items as module globals (the fork inherits the heap).

# Public (no underscore, not ALL_CAPS) module globals are intentional: they are set
# by `_collect()` in the PRELOADED "pytest_fast" module (forkserver) or at import
# (spawn), and workers read them via `import pytest_fast` (their own globals are
# __main__/__mp_main__, where collect did NOT run). Underscore would trip pyright's
# cross-module private-access; ALL_CAPS — reportConstantRedefinition on reassign.
collected_config: Config | None = None
collected_items: list[Item] = []
# Collection errors (nodeid, longrepr text) captured during `_collect()` — e.g. a test
# module that fails to import. pytest quietly drops such files from `session.items`;
# without surfacing these, the daemon would serve a SHORTER suite while looking green
# (observed in the wild: 2 files with ImportError → n dropped silently, no error, rc=0).
# Workers ship this list to the daemon in their one-time 'ready' frame; the daemon
# renders a `collect-errors` summary block and forces a non-zero rc.
collected_errors: list[tuple[str, str]] = []


# Seconds: after this long inside `_collect()` the watchdog thread prints all-threads
# stack traces to stderr. Goal — diagnosing a "hanging conftest" / `pytest_configure`
# hook that loops forever. Normal collect is sub-second on small repos and a few
# seconds on large ones; 30s is a generous bound that catches real hangs without
# spamming on slow CI.
_COLLECT_WATCHDOG_TIMEOUT = 30.0


class _CollectErrorRecorder:
    """Collection-time plugin: record failed collect reports (import errors, syntax
    errors, broken fixtures at module scope). These never make it into `session.items`,
    so they are invisible to the run loop — this recorder is their only witness."""

    def __init__(self) -> None:
        self.errors: list[tuple[str, str]] = []

    def pytest_collectreport(self, report: CollectReport) -> None:
        if report.failed:
            self.errors.append((report.nodeid, report.longreprtext))


def _collect() -> None:
    global collected_config, collected_items, collected_errors
    import faulthandler
    import gc
    import importlib.util

    import pytest
    from _pytest.config import get_config

    # Watchdog: if collect hangs, after `_COLLECT_WATCHDOG_TIMEOUT` seconds we dump
    # stack traces for all threads (including the current one — where pytest import/
    # configure is wedged). This log lands in daemon.log → the hang site becomes
    # obvious post-mortem. Thread daemon=True → if the process dies before the watchdog
    # fires, the thread dies with it.
    collect_done = threading.Event()

    def _watchdog() -> None:
        if collect_done.wait(timeout=_COLLECT_WATCHDOG_TIMEOUT):
            return  # collect finished in time — exit silently
        print(
            f"[pytest-fast] WARNING: _collect() taking >{_COLLECT_WATCHDOG_TIMEOUT}s; dumping all-threads stack:",
            file=sys.stderr,
            flush=True,
        )
        faulthandler.dump_traceback(file=sys.stderr, all_threads=True)
        sys.stderr.flush()

    threading.Thread(target=_watchdog, daemon=True, name="pytest-fast-collect-watchdog").start()

    try:
        args = ["-m", os.environ.get("PYTEST_FAST_MARK", ""), "-q"]
        # `-n0` neutralizes ambient `-n auto` (from PYTEST_ADDOPTS / pytest.ini), but the
        # option is owned by pytest-xdist: without it pytest fails with `UsageError:
        # unrecognized arguments: -n0`. Append only when xdist is actually installed.
        if importlib.util.find_spec("xdist") is not None:
            args.append("-n0")
        config = get_config(args)
        config.parse(args)
        # public counterpart of the private config._do_configure(): historic call of pytest_configure
        config.hook.pytest_configure.call_historic(kwargs={"config": config})
        session = pytest.Session.from_config(config)
        config.hook.pytest_sessionstart(session=session)
        # Witness collection failures: a module that fails to import is silently absent
        # from `session.items` — without this recorder the daemon would serve the
        # shortened suite as if nothing happened (green run, quietly missing tests).
        error_recorder = _CollectErrorRecorder()
        config.pluginmanager.register(error_recorder)
        config.hook.pytest_collection(session=session)
        collected_config, collected_items = config, session.items
        collected_errors = error_recorder.errors
        # Reap collect-time cyclic garbage BEFORE freezing. Importing test modules leaves
        # transient garbage that only cyclic GC reclaims — notably stale pre-slots classes
        # from `@attrs.define`/`@dataclass(slots=True)`, which linger as DUPLICATES in their
        # base's `__subclasses__()` until collected. `gc.freeze()` pins whatever is live into
        # the permanent generation, so without this collect first, those duplicates are
        # frozen forever and every forked worker inherits a polluted `__subclasses__()` —
        # breaking libraries that walk it (e.g. cattrs `include_subclasses`). Plain pytest
        # avoids this because natural GC runs between collect and the test; the forkserver
        # forks immediately, so we must reap explicitly here.
        gc.collect()
        gc.freeze()  # heap (app+items) into the permanent generation → GC won't scan shared COW pages
    finally:
        collect_done.set()  # watchdog thread exits quietly (success or exception — no stack dump)


# `_collect()` trigger is INTENTIONALLY at the bottom of the file (see block at end
# of __init__.py). It's NOT here: when pytest collects, it imports test files that do
# `from pytest_fast import <symbol>`. If the trigger fires now (while the module is
# still mid-load), the test-file import lands in a cache hit on the partially-loaded
# module — symbols declared later in this file are not yet available → silent ImportError
# → pytest skips the file entirely. So we collect only AFTER the whole module is initialized.


# ── worker (forkserver-child) ─────────────────────────────────────────────────


def _noop() -> None:
    """Trivial target: starting it boots the forkserver + runs preload (collect)."""


def _failure_text(reports: list[TestReport]) -> str:
    """Failure text to print: longrepr (traceback / assert diff / exception) for failed
    phases plus their captured sections (stdout/stderr/log). We use `longreprtext` (str)
    — it pickles trivially across the bus, unlike the longrepr object itself."""
    parts: list[str] = []
    for rep in reports:
        if not rep.failed:
            continue
        prefix = "" if rep.when == "call" else f"[{rep.when}] "
        if rep.longreprtext:
            parts.append(prefix + rep.longreprtext)
        parts.extend(f"----- {title} -----\n{content}" for title, content in rep.sections)
    return "\n".join(parts)


def _durations_lines(results: list[RunResult], limit: int = 15, min_dur: float = 0.005) -> list[str]:
    """A pytest `--durations`-style table from the full per-phase reports (full-report mode
    only). Flattens every (duration, when, nodeid) phase across all results, slowest first.
    Empty when no result carries serialized reports (lean mode)."""
    phases: list[tuple[float, str, str]] = []
    for r in results:
        for rep in r.get("reports", []):
            dur, when, nodeid = rep.get("duration"), rep.get("when"), rep.get("nodeid")
            # `dur == dur` filters NaN (nan != nan): a NaN duration from a malformed/
            # hostile serialized report poisons `list.sort()` below (NaN comparisons are
            # all False → the sort silently leaves the table mis-ordered).
            if isinstance(dur, int | float) and dur == dur and isinstance(when, str) and isinstance(nodeid, str):
                phases.append((float(dur), when, nodeid))
    phases.sort(reverse=True)
    shown = [p for p in phases if p[0] >= min_dur][:limit]
    if not shown:
        return []
    out = [f"  DURATIONS (top {len(shown)}, ≥{min_dur * 1000:.0f}ms — per phase):"]
    out.extend(f"    {dur:8.3f}s  {when:<9}{nodeid}" for dur, when, nodeid in shown)
    return out


# `bench` thresholds — fixed constants so every finding is a deterministic function of the
# measured numbers, never a tuned/learned guess.
_BENCH_CLUSTER_MIN = 5  # ≥ this many tests sharing a heavy setup → a scope-widening cluster
_BENCH_SETUP_MIN_S = 0.05  # a setup phase this long counts as "heavy"
_BENCH_LEVER_MIN_S = 0.5  # don't report a lever that reclaims less than this
_BENCH_TOP_CALLS = 20  # how many slowest CALL phases to classify
_BENCH_MAX_LEVERS = 12
_BENCH_IO_FRAC = 0.20  # cpu/total below this → I/O-bound
_BENCH_CPU_FRAC = 0.80  # cpu/total above this → CPU-bound
_BENCH_CV = 0.40  # per-test coefficient of variation above this (≥2 runs) → unstable timing
_BENCH_PROFILE_NODES = 12  # how many top bottleneck tests the targeted cProfile pass re-runs


def _phase_split(result: RunResult) -> tuple[float, float, float]:
    """(setup, call, teardown) wall seconds from a result's per-phase reports (full-report mode)."""
    setup = call = teardown = 0.0
    for rep in result.get("reports", []):
        when, dur = rep.get("when"), rep.get("duration")
        if isinstance(dur, int | float) and dur == dur:  # dur == dur drops NaN
            if when == "setup":
                setup += float(dur)
            elif when == "call":
                call += float(dur)
            elif when == "teardown":
                teardown += float(dur)
    return setup, call, teardown


def _bench_report(
    result_runs: list[list[RunResult]],
    run: float,
    cores: int,
    warmup_dropped: bool = False,
    profiles: dict[str, list[tuple[str, int, float, float]]] | None = None,
) -> str:
    """Deterministic bottleneck report for `pytest-fast --bench[=N]`. Every lever is (measured number →
    fixed rule → reclaimable worker-seconds), ranked by impact — NO heuristics. Needs full-report
    mode (per-phase setup/call/teardown) + per-test `cpu`. Two lever families:

      • SHARED SETUP — K tests in one file each paying ~S setup is K·S worker-seconds; a
        session/module-scoped fixture pays it once → reclaim ≈ (K−1)·S. Marked [potential] because
        whether the fixture is scope-widenable can't be read from timings (only the upper bound can).
      • per-test CALL hot-spots — the slowest call phases, classified by `cpu/total`: I/O-bound
        (waiting on DB/network), CPU-bound (algorithmic), or setup-heavy.

    `result_runs` is one or more runs (the caller drops the warmup); per-test timings are AVERAGED
    across them so the ranking isn't ruled by one noisy sample. The header states the deterministic
    ceiling: best wall ≈ max(Σbusy/cores, longest-test)."""
    from collections import defaultdict

    line = "═" * 66
    # Average each test's timings across the runs it appeared in: [appearances, total, cpu, s, c, t].
    acc: dict[str, list[float]] = defaultdict(lambda: [0.0, 0.0, 0.0, 0.0, 0.0, 0.0])
    samples: dict[str, list[float]] = defaultdict(list)  # per-test total durations across runs → variance
    for results in result_runs:
        for r in results:
            s, c, t = _phase_split(r)
            a = acc[r["nodeid"]]
            a[0] += 1
            a[1] += r["duration"]
            a[2] += max(0.0, r.get("cpu", 0.0))
            a[3] += s
            a[4] += c
            a[5] += t
            samples[r["nodeid"]].append(r["duration"])
    recs = [
        (nid, nid.split("::", 1)[0], a[1] / a[0], a[2] / a[0], a[3] / a[0], a[4] / a[0], a[5] / a[0])
        for nid, a in acc.items()
        if a[0]
    ]
    n_tests = len(recs)
    n_runs = len(result_runs)
    sum_total = sum(x[2] for x in recs)
    sum_setup = sum(x[4] for x in recs)
    sum_call = sum(x[5] for x in recs)
    sum_teardown = sum(x[6] for x in recs)
    floor, floor_id = max(((x[2], x[0]) for x in recs), default=(0.0, ""))
    ideal = sum_total / cores if cores else 0.0
    best = max(ideal, floor)
    means = sorted(x[2] for x in recs)

    def _pct(q: float) -> float:
        return means[min(len(means) - 1, round(q * (len(means) - 1)))] if means else 0.0

    avg_note = f"avg of {n_runs} run{'s' if n_runs != 1 else ''}" + (" + warmup dropped" if warmup_dropped else "")
    out = [
        f"\n{line}",
        f"  pytest-fast bench  —  {n_tests} tests, {run:.2f}s wall @ {cores}w  ({avg_note})",
        line,
        f"  best @ {cores} cores ≈ {best:.2f}s   ·   floor (longest test) {floor:.2f}s  {floor_id}",
    ]
    if sum_total > 0:
        out.append(
            f"  where time goes: setup {sum_setup / sum_total:.0%} · call {sum_call / sum_total:.0%} · "
            f"teardown {sum_teardown / sum_total:.0%}   (of {sum_total:.0f}s test-wall)"
        )
        out.append(
            f"  per-test wall : p50 {_pct(0.5):.3f}s · p90 {_pct(0.9):.3f}s · p99 {_pct(0.99):.3f}s · max {floor:.2f}s"
        )

    levers: list[tuple[float, str, list[str]]] = []

    # 1. SHARED-SETUP clusters — by file.
    by_file: dict[str, list[float]] = defaultdict(list)
    for _nodeid, file, _total, _cpu, setup, _call, _teardown in recs:
        if setup >= _BENCH_SETUP_MIN_S:
            by_file[file].append(setup)
    for file, setups in by_file.items():
        if len(setups) < _BENCH_CLUSTER_MIN:
            continue
        tot = sum(setups)
        one = tot / len(setups)
        saving = tot - one  # session-scope pays one setup instead of len(setups)
        if saving >= _BENCH_LEVER_MIN_S:
            levers.append(
                (
                    saving,
                    "SHARED SETUP",
                    [
                        f"{file} — {len(setups)} tests × ~{one:.2f}s setup = {tot:.1f}s total",
                        f"→ session/module-scope the fixture (if scope-widenable): ~{one:.2f}s once → "
                        f"reclaim ~{saving:.1f} worker-s (~{saving / cores:.1f}s wall@{cores}w) [potential]",
                    ],
                )
            )

    # 2. per-test CALL hot-spots — slowest call phases, classified.
    for nodeid, _file, total, cpu, setup, call, teardown in sorted(recs, key=lambda x: x[5], reverse=True)[
        :_BENCH_TOP_CALLS
    ]:
        if call < _BENCH_LEVER_MIN_S:
            break
        cpu_frac = (cpu / total) if (cpu >= 0 and total > 0) else -1.0
        off_cpu = max(0.0, total - cpu) if cpu >= 0 else -1.0
        # Tips state only what the timings DETERMINE (where the cost is), never a guessed cause/fix
        # (whether it's a query, a sleep, a subprocess, an algorithm — timings can't tell).
        if setup > call and setup >= _BENCH_SETUP_MIN_S:
            cat, tip = "SETUP-HEAVY", f"cost is fixture setup ({setup:.2f}s > call {call:.2f}s), not the test body"
        elif 0 <= cpu_frac < _BENCH_IO_FRAC:
            cat, tip = (
                "I/O-BOUND",
                f"{off_cpu:.2f}s off-CPU (I/O wait) — cost is outside Python; CPU/more-workers won't cut it",
            )
        elif cpu_frac > _BENCH_CPU_FRAC:
            cat, tip = "CPU-BOUND", f"{cpu:.2f}s on-CPU — real compute; bounded by core speed"
        else:
            cat, tip = "MIXED", "cost split across CPU and I/O — see the phase breakdown"
        cpu_note = f", {cpu_frac:.0%} CPU" if cpu_frac >= 0 else ""
        body = [
            f"{nodeid}  ({total:.2f}s: setup {setup:.2f}/call {call:.2f}/teardown {teardown:.2f}{cpu_note})",
            f"→ {tip}",
        ]
        rows = (profiles or {}).get(nodeid)
        if rows:
            body.append("profile (top by SELF wall — where it's actually burned; ncalls exact):")
            body.extend(f"  {self_t:6.3f}s self  {nc:>8,}×  {name}" for name, nc, self_t, _cum in rows)
        levers.append((call, cat, body))

    levers.sort(key=lambda x: x[0], reverse=True)
    if levers:
        out.append("  ── levers (ranked by reclaimable worker-seconds) ─────────────────")
        for i, (saving, cat, body) in enumerate(levers[:_BENCH_MAX_LEVERS], 1):
            out.append(f"  {i:>2}. {cat:<12} ~{saving:5.1f} w-s")
            out.extend(f"      {ln}" for ln in body)
    else:
        out.append("  no levers above the reporting threshold — the suite is already lean.")

    # Unstable timing — needs ≥2 measured runs (so --bench=3+). cv = stdev/mean per test; a high cv
    # is a measured fact (flaky perf / ordering-sensitive / contended), not a heuristic. Ranked by
    # cv·mean (the wall actually at stake), only for tests big enough to matter.
    unstable: list[tuple[float, str, float, float]] = []
    for nid, xs in samples.items():
        if len(xs) >= 2:
            m = sum(xs) / len(xs)
            if m >= _BENCH_LEVER_MIN_S:
                sd = math.sqrt(sum((x - m) ** 2 for x in xs) / len(xs))
                if m and sd / m >= _BENCH_CV:
                    unstable.append((sd / m, nid, m, sd))
    if unstable:
        unstable.sort(key=lambda u: u[0] * u[2], reverse=True)
        out.append(f"  ── unstable timing (cv ≥ {_BENCH_CV:.0%} across {n_runs} runs) ──────────────────────")
        out.extend(f"      {nid}  {m:.2f}s ±{sd:.2f}s  (cv {cv:.0%})" for cv, nid, m, sd in unstable[:8])
    elif n_runs < 2:
        out.append("  (timing-stability needs ≥2 measured runs — try --bench=3+)")
    out.append(line)
    return "\n".join(out)


def _top_profile_rows(pr: cProfile.Profile, limit: int = 8) -> list[tuple[str, int, float, float]]:
    """Top functions of a finished `cProfile.Profile`, by SELF time (`inlinetime`) — time spent IN
    the function, excluding subcalls. Self time (not cumulative) surfaces the actual leaves where the
    wall is burned — a blocking syscall, a hot compute loop, a repeated query — instead of the
    pytest/pluggy wrapper chain (whose cumulative time is ~the whole test but tells you nothing).
    Each row is (qualname, ncalls, selftime, cumtime); ncalls are EXACT, so a leaf called 47× in one
    test is the measured (not guessed) N+1 / hot-call signal. Plain builtins → whitelist-safe."""
    rows: list[tuple[float, float, int, str]] = []
    for e in pr.getstats():
        code = e.code
        if isinstance(code, str):
            label = code  # a built-in, e.g. "<built-in method ... read>" / "<method 'recv' ...>"
        else:
            short = code.co_filename.rsplit("/", 1)[-1]
            label = f"{code.co_name} ({short}:{code.co_firstlineno})"
        rows.append((e.inlinetime, e.totaltime, e.callcount, label[:70]))
    rows.sort(reverse=True)  # by self time
    return [(lbl, nc, round(self_t, 4), round(cum_t, 4)) for self_t, cum_t, nc, lbl in rows[:limit]]


def _run_one_item(
    item: Item, nextitem: Item | None, collector: _ReportCollector, *, full_report: bool = False, profile: bool = False
) -> RunResult:
    """Run a test via the FULL pytest protocol (hook, not function): setup/call/
    teardown, capture, rerunfailures, makereport — behavior 1-to-1 with regular pytest.

    `full_report=True` also attaches every phase report in pytest's serializable wire form
    (`pytest_report_to_serializable` → plain builtins, whitelist-safe) so the master/controller
    can replay them through a real terminalreporter (--durations, junit, -v/-s, plugins). Off by
    default — the lean path ships only the outcome summary.

    `profile=True` (the `bench` targeted pass over its top bottleneck tests) runs the protocol under
    `cProfile` and attaches the top-by-cumtime functions → deterministic where-in-the-code attribution."""
    collector.reports.clear()
    pr = None
    if profile:
        import cProfile

        pr = cProfile.Profile()
        pr.enable()
    try:
        item.ihook.pytest_runtest_protocol(item=item, nextitem=nextitem)
    finally:
        if pr is not None:
            pr.disable()
    duration = sum(r.duration for r in collector.reports)
    outcome = categorize(item.config, collector.reports)
    result: RunResult = {"nodeid": item.nodeid, "outcome": outcome, "duration": duration}
    phases: dict[str, float] = {}
    for report in collector.reports:
        if report.when in {"setup", "call", "teardown"}:
            phases[report.when] = max(phases.get(report.when, 0.0), report.duration)
    result["phases"] = phases
    if pr is not None:
        result["profile"] = _top_profile_rows(pr)
    if outcome in {"failed", "error"}:
        result["longrepr"] = _failure_text(collector.reports)  # traceback only for reds
    if full_report:
        config = item.config
        result["reports"] = [
            cast("dict[str, object]", config.hook.pytest_report_to_serializable(config=config, report=rep))
            for rep in collector.reports
        ]
    # Plugin measurement channel (see `pytest_fast_annotate_result` hookspec). Guarded: a broken
    # measurement plugin must degrade to a missing annotation + a stderr note (→ daemon log), never
    # to a failed test or a crashed worker (which would flag the whole run untrusted).
    extra: dict[str, object] = {}
    try:
        item.ihook.pytest_fast_annotate_result(item=item, result=result, extra=extra)
    except Exception as exc:  # isolation barrier around third-party hookimpls
        print(f"[pytest-fast] pytest_fast_annotate_result failed on {item.nodeid!r}: {exc!r}", file=sys.stderr)
    if extra:
        result["extra"] = extra
    return result


def _worker_hang_timeout() -> float:
    """Seconds after which a worker still running a single test dumps all-threads stack
    traces to stderr (which lands in the daemon log). Diagnoses runaway tests / GIL
    deadlocks / blocked I/O inside `pytest_runtest_protocol`. 0 = disabled (default,
    so legitimately-slow tests don't dump on every run); typical opt-in is 60–120s.

    Env var: `PYTEST_FAST_WORKER_HANG_TIMEOUT=<seconds>`."""
    try:
        return max(0.0, float(os.environ.get("PYTEST_FAST_WORKER_HANG_TIMEOUT", "0")))
    except ValueError:
        return 0.0


def _worker_identity_enabled() -> bool:
    """Whether the OPT-IN per-worker xdist-identity feature is active.

    Off by default — most suites don't need it and it adds per-worker setup cost. No third-party
    dependency (not even pytest-xdist): it sets the standard ``PYTEST_XDIST_*`` env + a
    ``config.workerinput`` dict (plain process state) and fires pytest-fast's own
    ``pytest_fast_configure_worker`` hook, which an adapter implements to do the per-worker resource
    setup. Turn it on with ``pytest-fast --worker-identity`` / ``pytest --fast --fast-worker-identity``
    (both set this env), or directly via ``PYTEST_FAST_WORKER_IDENTITY=1``. Only matters for suites
    whose plugins isolate per-worker resources (SQLAlchemy follower DBs, pytest-django test DBs, ...)."""
    return os.environ.get("PYTEST_FAST_WORKER_IDENTITY", "").strip().lower() in ("1", "true", "yes", "on")


def _setup_worker_identity(config: Config, wid: int, num_workers: int, testrunuid: str) -> None:
    """Give this forked worker an xdist-compatible identity plus a per-worker setup hook, so plugins
    that isolate per-worker resources (SQLAlchemy follower DBs, pytest-django test DBs, ...) don't
    collide on the controller's single shared default.

    Why it's needed: xdist spawns a FRESH worker process whose ``pytest_configure`` runs with
    ``config.workerinput`` already set, so worker-aware plugins self-isolate. pytest-fast instead
    collects+configures ONCE in the forkserver parent (no worker identity) and forks workers — so
    those plugins never saw a worker id and every worker shares the controller's defaults (e.g.
    SQLAlchemy's ``FOLLOWER_IDENT=None`` → every worker ATTACHes the same ``*_test_schema.db``).

    Reconstructed post-fork:
      1. set the standard ``PYTEST_XDIST_*`` env + a ``config.workerinput`` dict — enough on its own
         for plugins that key per-worker resources off ``PYTEST_XDIST_WORKER`` (e.g. pytest-django);
      2. fire ``pytest_fast_configure_worker(config, workerid, workercount)`` so an adapter can finish
         the worker-role setup the fork can't infer — notably re-binding resources a plugin pinned at
         COLLECTION time in the parent (e.g. SQLAlchemy rebuilds each ``Config``'s engine onto a
         per-worker follower). A plugin/conftest opts in by defining that hook; pytest-fast ships none.

    Needs no third-party dependency (not even pytest-xdist) — the env + workerinput are plain process
    state and the hook is pytest-fast's own. Opt-in (see `_worker_identity_enabled`), fully
    best-effort and guarded — any failure leaves the worker as it was (shared identity), never worse.
    """
    if not _worker_identity_enabled() or hasattr(config, "workerinput"):
        return

    workerid = f"gw{wid}"
    workerinput: dict[str, object] = {
        "workerid": workerid,
        "workercount": num_workers,
        "testrunuid": testrunuid,
        "mainargv": list(sys.argv) or ["pytest"],
        "option_dict": {},
    }
    os.environ["PYTEST_XDIST_WORKER"] = workerid
    os.environ["PYTEST_XDIST_WORKER_COUNT"] = str(num_workers)
    os.environ["PYTEST_XDIST_TESTRUNUID"] = testrunuid

    wconfig = cast("_WorkerConfig", config)
    wconfig.workerinput = workerinput
    wconfig.workeroutput = {}

    # Fire pytest-fast's own per-worker hook so adapters finish the worker-role setup the fork can't
    # infer — rebinding resources a plugin pinned at COLLECTION time in the parent (e.g. SQLAlchemy
    # rebuilds each Config's engine onto a per-worker follower). Registered as a real hookspec in
    # pytest_addhooks; impls read the now-populated config.workerinput / PYTEST_XDIST_* env. No xdist
    # involved — the env + workerinput above are plain process state, and this hook is pytest-fast's own.
    try:
        config.hook.pytest_fast_configure_worker(config=config, workerid=workerid, workercount=num_workers)
    except Exception as exc:
        _log("worker", f"{workerid}: pytest_fast_configure_worker hook failed: {exc!r}")


def _shutdown_worker_plugins(config: Config, wid: int) -> None:
    """Finalize worker-owned plugin state before the successful ``fin`` frame.

    The hook runs before ``os._exit`` so plugins can durably flush process-local data. An exception is
    deliberately fail-closed: the worker does not send ``fin``, and the controller rejects the run as
    incomplete instead of accepting missing plugin output.
    """
    config.hook.pytest_fast_worker_shutdown(config=config, workerid=f"gw{wid}")


def _worker_main(
    wid: int,
    sock_path: str,
    full_report: bool = False,
    send_nodeids: bool = False,
    profile: bool = False,
    num_workers: int = 1,
    testrunuid: str = "",
    active_items: _ActiveItemSlots | None = None,
) -> None:
    # IMPORTANT: read globals via `import pytest_fast`, NOT as bare names. `_collect()`
    # set them on the PRELOADED "pytest_fast" module (forkserver) / at import (spawn),
    # whereas `_worker_main`'s own globals are __main__/__mp_main__ (collect did NOT run there).
    t_start = time.perf_counter()
    import pytest_fast  # forkserver: cache hit (preloaded+collected); spawn: imports+collects here

    config = pytest_fast.collected_config
    assert config is not None, "forkserver/spawn must have collected tests before worker start"
    items = pytest_fast.collected_items
    collector = _ReportCollector()
    config.pluginmanager.register(collector)
    # Graft an xdist-compatible per-worker identity so worker-aware plugins (SQLAlchemy follower DBs,
    # pytest-django, ...) isolate their resources instead of colliding on the controller's defaults.
    # Must run before any item executes (provisions the per-worker DB). No-op for a single worker.
    _setup_worker_identity(config, wid, num_workers, testrunuid)
    collect_wall = time.perf_counter() - t_start

    sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    sock.connect(sock_path)
    # In selection mode the master needs nodeid→index to run a subset; ship the nodeid list
    # once per worker in 'ready' (None otherwise → lean).
    ready_nodeids = [it.nodeid for it in items] if send_nodeids else None
    _send(sock, ("ready", wid, len(items), collect_wall, ready_nodeids, pytest_fast.collected_errors))

    # Per-test hang watchdog: when `PYTEST_FAST_WORKER_HANG_TIMEOUT` > 0, arm a
    # `faulthandler` timer before each `_run_one_item` and cancel it after the test
    # returns. If a test exceeds the timeout, faulthandler dumps all-threads tracebacks
    # to stderr (→ daemon log in resident mode) AND prints the nodeid we were running,
    # so a deadlock pinpoints the offending test instead of presenting as silent hang.
    hang_timeout = _worker_hang_timeout()
    faulthandler_mod = None
    if hang_timeout > 0:
        import faulthandler

        faulthandler_mod = faulthandler
        if not faulthandler.is_enabled():
            faulthandler.enable()

    run_start = time.perf_counter()
    busy = 0.0  # wall inside the runtest protocol
    cpu = 0.0  # this worker's CPU time over that same span (busy − cpu ≈ I/O wait)
    bus_wait = 0.0  # idle between tests, waiting for the master to hand out the next index
    ran = 0
    prev: Item | None = None
    prev_idx: int | None = None
    pending: RunResult | None = None
    while True:
        t_bus = time.perf_counter()
        _send(sock, ("req", wid, pending))
        if pending is not None and active_items is not None:
            active_items[wid] = _ACTIVE_ITEM_IDLE
        reply, _ = _recv(sock)
        bus_wait += time.perf_counter() - t_bus  # send result + wait for next idx = non-busy gap
        # Master gone / malformed reply → break out and exit cleanly (os._exit below). The
        # master sees EOF and the run is flagged untrusted via the result undercount.
        if not isinstance(reply, tuple) or len(reply) < 2:
            break
        idx_msg = cast("tuple[object, object]", reply)  # master → ('idx', pick)
        idx = idx_msg[1]
        cur = items[idx] if isinstance(idx, int) and 0 <= idx < len(items) else None
        if prev is not None:
            if active_items is not None and prev_idx is not None:
                active_items[wid] = prev_idx
            t0 = time.perf_counter()
            c0 = time.process_time()
            if faulthandler_mod is not None:
                faulthandler_mod.dump_traceback_later(hang_timeout, repeat=True, exit=False)
            try:
                pending = _run_one_item(prev, cur, collector, full_report=full_report, profile=profile)
            except BaseException:
                if faulthandler_mod is not None:
                    faulthandler_mod.cancel_dump_traceback_later()
                # Worker died mid-test (runtime error in the protocol itself, NOT a test
                # failure — those are captured as reports). Print the offending nodeid
                # so the daemon log shows which test we were on, then re-raise so the
                # process exits with non-zero and the master flags the run untrusted.
                print(
                    f"[pytest-fast] worker {wid} crashed while running {prev.nodeid!r}",
                    file=sys.stderr,
                    flush=True,
                )
                raise
            if faulthandler_mod is not None:
                faulthandler_mod.cancel_dump_traceback_later()
            busy += time.perf_counter() - t0
            cpu_this = time.process_time() - c0
            cpu += cpu_this
            pending["cpu"] = cpu_this  # per-test CPU → `bench` I/O-vs-CPU classification
            if active_items is not None:
                active_items[wid] = _ACTIVE_ITEM_COMPLETED
            ran += 1
        else:
            pending = None
        if cur is None:
            _shutdown_worker_plugins(config, wid)
            stats: WorkerStats = {
                "wid": wid,
                "ran": ran,
                "busy": busy,
                "cpu": cpu,
                "bus_wait": bus_wait,
                "run_wall": time.perf_counter() - run_start,
            }
            _send(sock, ("fin", wid, pending, stats))
            if pending is not None and active_items is not None:
                active_items[wid] = _ACTIVE_ITEM_IDLE
            break
        prev = cur
        prev_idx = idx if isinstance(idx, int) else None
    sock.close()
    # Worker exit: `os._exit(0)` — skip atexit hooks AND non-daemon thread joins.
    # Returning normally would let interpreter shutdown join() every alive non-daemon
    # thread; tests that spawn worker threads (intentionally — `test_run_given_concurrently`
    # — or unintentionally — pytest's threadexception plugin warning on an orphan thread)
    # leave those threads alive, and the worker would never exit → `procs[wid].join()` in
    # master hangs forever, presenting as a silent post-`F` deadlock. We've already sent
    # `fin` and closed the bus socket, so a hard exit is correct (the master got every
    # report; nothing else legitimate is pending). Mirrors stdlib multiprocessing's own
    # advice for worker children whose application code may leave threads running.
    os._exit(0)


def _worker_persist_main(
    wid: int,
    pool_sock_path: str,
    *,
    send_nodeids: bool = False,
    num_workers: int = 1,
    testrunuid: str = "",
    active_items: _ActiveItemSlots | None = None,
) -> None:
    """Persist-mode worker (opt-in `--persist-workers`, serve mode): forked ONCE, its pytest session
    spans MANY run requests. Between runs the session — and its session-scoped fixtures — stays alive:
    at each run's end the last item tears down against a synthetic Item whose parent is the Session,
    so pytest keeps only session-scoped fixtures. Only at ``stop`` (daemon shutdown) does setup-state
    tear down with ``nextitem=None`` (full session teardown), then the worker exits.

    Per-run protocol over the pool socket: the master answers the worker's ``('req', result)`` with
    ``('idx', pick)`` to run the next item, ``('flush',)`` once the run's queue is drained, or
    ``('stop',)`` at shutdown. The worker replies ``('run_done', wid, stats)`` at each run boundary and
    stays warm. The one-item lag (a result ships on the NEXT req) is preserved; ``flush`` runs the held
    last item against the anchor and ships its result before ``run_done``."""
    t_start = time.perf_counter()
    from _pytest.nodes import Item as RuntimeItem

    import pytest_fast

    class _SessionBoundaryItem(RuntimeItem):
        """Real pytest Item used only as the fixture boundary between persistent requests."""

        def runtest(self) -> None:
            raise AssertionError("session boundary items are never executed")

        def reportinfo(self) -> tuple[Path, int | None, str]:
            return self.config.rootpath, None, self.name

    config = pytest_fast.collected_config
    assert config is not None, "forkserver/spawn must have collected tests before worker start"
    items = pytest_fast.collected_items
    collector = _ReportCollector()
    config.pluginmanager.register(collector)
    _setup_worker_identity(config, wid, num_workers, testrunuid)
    collect_wall = time.perf_counter() - t_start

    sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    sock.connect(pool_sock_path)
    anchor = items[0] if items else None
    # A genuine Item keeps third-party pytest_runtest_protocol hooks on their declared contract.
    # Its direct Session parent makes the common setup-state prefix exactly the session scope.
    session_boundary = (
        _SessionBoundaryItem.from_parent(anchor.session, name="pytest-fast-session-boundary")
        if anchor is not None
        else None
    )
    ready_nodeids = [it.nodeid for it in items] if send_nodeids else None
    _send(sock, ("ready", wid, len(items), collect_wall, ready_nodeids, pytest_fast.collected_errors))

    stop = False
    while not stop:  # spans runs — session persists across this outer loop
        if active_items is not None:
            active_items[wid] = _ACTIVE_ITEM_IDLE
        prev: Item | None = None
        prev_idx: int | None = None
        pending: RunResult | None = None
        ran = 0
        busy = 0.0
        eager_run = False
        run_start = time.perf_counter()
        while True:  # one run
            _send(sock, ("req", wid, pending))
            if pending is not None and active_items is not None:
                active_items[wid] = _ACTIVE_ITEM_IDLE
            reply, _ = _recv(sock)
            if not isinstance(reply, tuple) or not reply:
                stop = True  # master gone → shut this worker down (final teardown below)
                break
            tag = reply[0]
            if tag == "stop":
                stop = True
                break
            if tag == "flush":
                full_report = len(reply) > 1 and bool(reply[1])
                if prev is not None:
                    if active_items is not None and prev_idx is not None:
                        active_items[wid] = prev_idx
                    t0 = time.perf_counter()
                    pending = _run_one_item(prev, session_boundary, collector, full_report=full_report)
                    busy += time.perf_counter() - t0
                    if active_items is not None:
                        active_items[wid] = _ACTIVE_ITEM_COMPLETED
                    ran += 1
                    _send(sock, ("req", wid, pending))  # ship the run's last result
                    if active_items is not None:
                        active_items[wid] = _ACTIVE_ITEM_IDLE
                elif eager_run and session_boundary is not None:
                    # A short-circuit may leave setup-state pointing at the undispatched lookahead
                    # item used as pytest's nextitem. Retain only session-scoped fixtures across requests.
                    session_boundary.session._setupstate.teardown_exact(session_boundary)
                stats: WorkerStats = {
                    "wid": wid,
                    "ran": ran,
                    "busy": busy,
                    "cpu": 0.0,
                    "bus_wait": 0.0,
                    "run_wall": time.perf_counter() - run_start,
                }
                _send(sock, ("run_done", wid, stats))
                break  # wait for the next run; session (and session fixtures) stays warm
            idx = reply[1]  # ('idx', pick)
            cur = items[idx] if isinstance(idx, int) and 0 <= idx < len(items) else None
            eager = len(reply) > 2 and bool(reply[2])
            full_report = len(reply) > 4 and bool(reply[4])
            if eager and cur is not None:
                # Stop-first requests cannot use the normal one-item lag: by the time the master
                # sees item N fail, item N+1 would already be held and `flush` would execute it.
                # Run the assigned item before asking for more work, using a different collected
                # item as nextitem so function fixtures tear down while the session stays warm.
                next_idx = reply[3] if len(reply) > 3 else None
                next_selected = items[next_idx] if isinstance(next_idx, int) and 0 <= next_idx < len(items) else None
                nxt = next_selected if next_selected is not cur else None
                if nxt is None:
                    nxt = session_boundary
                if active_items is not None:
                    active_items[wid] = idx
                t0 = time.perf_counter()
                pending = _run_one_item(cur, nxt, collector, full_report=full_report)
                busy += time.perf_counter() - t0
                if active_items is not None:
                    active_items[wid] = _ACTIVE_ITEM_COMPLETED
                ran += 1
                eager_run = True
                prev = None
                continue
            if prev is not None:
                if active_items is not None and prev_idx is not None:
                    active_items[wid] = prev_idx
                t0 = time.perf_counter()
                pending = _run_one_item(prev, cur, collector, full_report=full_report)
                busy += time.perf_counter() - t0
                if active_items is not None:
                    active_items[wid] = _ACTIVE_ITEM_COMPLETED
                ran += 1
            else:
                pending = None
            prev = cur
            prev_idx = idx if isinstance(idx, int) else None
    # Daemon shutdown: finalize the long-lived session (its session-scoped fixtures persisted across all
    # runs via the anchor). At a run boundary `prev` is already run+shipped, so there's no item to tear
    # down against None — drain the session's setup-state directly (parity with the per-run path, whose
    # last item tears down with nextitem=None every run). A held-but-unrun `prev` (master died mid-run)
    # tears down normally. Best-effort: we're exiting, so a finalizer error must not wedge shutdown.
    if prev is not None:
        with suppress(Exception):
            _run_one_item(prev, None, collector)
    elif anchor is not None:
        with suppress(Exception):
            anchor.session._setupstate.teardown_exact(None)  # finalize session-scoped fixtures
    _shutdown_worker_plugins(config, wid)
    with suppress(OSError):
        _send(sock, ("bye", wid))
    sock.close()
    os._exit(0)


# ── reference outcome-dump (when loaded as `-p pytest_fast` with OUTCOME_DUMP) ─
#
# Under xdist the controller re-publishes worker reports → its hook sees ALL tests.
# On sessionfinish (not an xdist worker) we write {nodeid: outcome} for outcome-diff.

_DUMP_REPORTS: dict[str, list[TestReport]] = {}


def pytest_runtest_logreport(report: TestReport) -> None:
    if os.environ.get("OUTCOME_DUMP"):
        _DUMP_REPORTS.setdefault(report.nodeid, []).append(report)


def pytest_sessionfinish(session: object) -> None:
    dump = os.environ.get("OUTCOME_DUMP")
    config = getattr(session, "config", None)
    if not dump or config is None or hasattr(config, "workerinput"):
        return  # no dump configured / xdist worker (controller aggregates)
    out = {nodeid: categorize(config, reps) for nodeid, reps in _DUMP_REPORTS.items()}
    with Path(dump).open("w") as f:
        json.dump(out, f, indent=0, sort_keys=True)


# ── master ───────────────────────────────────────────────────────────────────


class Daemon:
    def __init__(
        self, num_workers: int, start_method: str, dump_path: str | None = None, *, persist_workers: bool = False
    ) -> None:
        super().__init__()
        # Real raise, not `assert` — this is a safety invariant (0 workers → nothing runs →
        # silent green), and `assert` is stripped under `python -O`. Entry points reject `< 1`
        # earlier with a friendlier message; this is the last-line guard for direct callers.
        if num_workers < 1:
            msg = f"num_workers must be >= 1, got {num_workers}"
            raise ValueError(msg)
        self.num_workers = num_workers
        self.start_method = start_method
        self.dump_path = dump_path
        self.persist_workers = persist_workers
        # Context + preload are created ONCE; the forkserver lazy-spawns on the first
        # Process.start() and collects tests there, subsequent forks reuse the ready items.
        # get_context(str) in typeshed → BaseContext (no .Process); at runtime the context
        # is always concrete (Default/Spawn/Fork/ForkServer) and .Process exists — the cast
        # to DefaultContext gives the correct .Process(...) signature. set_forkserver_preload
        # is declared on BaseContext directly, so it's accessible too.
        self.ctx = cast("DefaultContext", mp.get_context(start_method))
        if start_method == "forkserver":
            self.ctx.set_forkserver_preload(["pytest_fast"])
        self._run_counter = 0
        # Raw outcome of the most recent run request — the `want_results` data source (see the
        # serve loop). Reset before each request; left None by modes that don't produce a single
        # canonical outcome (bench).
        self._last_outcome: _RunOutcome | None = None
        # Collection errors reported by workers in their 'ready' frame (static per boot —
        # collect runs once in the forkserver preload, which is a DIFFERENT process from
        # this daemon, so the bus is the only channel). Rendered as the `collect-errors`
        # summary block and forcing rc=1: a silently shortened suite must never look green.
        self._collect_errors: list[tuple[str, str]] = []
        # The `_PYTEST_FAST_COLLECT` flag is NOT set here on purpose — it's a global
        # side effect that would leak into env even if the object is built but not used.
        # We set it immediately before the first `Process.start()` (see `_arm_collect_flag`),
        # which is where it semantically belongs.

    def _arm_collect_flag(self) -> None:
        """Set the env flag for the forkserver preload — right before the first `Process.start`.

        The forkserver lazy-spawns on the first `.start()`; the flag must be in its env
        snapshot, otherwise the preload import of `pytest_fast` won't run `_collect()`.
        Idempotent: repeated calls are safe (same string reassigned)."""
        os.environ["_PYTEST_FAST_COLLECT"] = "1"

    # ── public modes ─────────────────────────────────────────────────────────

    def run(
        self,
        runs: int,
        *,
        full_report: bool = False,
        detailed: bool = False,
        bench: int = 0,
        persist_workers: bool = False,
    ) -> int:
        """Local mode: single-shot (runs=1) or N runs in one process. `bench=N` is its own N-run
        loop (warmup dropped) → one bottleneck report; it ignores `runs`. `persist_workers` reuses one
        warm worker across the N runs (session fixtures set up once)."""
        if bench > 0:
            rc, summary = self._run_bench(bench)
            print(summary)
            return rc
        if persist_workers:
            rc, summary = self._run_persistent(runs, full_report=full_report, detailed=detailed)
            print(summary)
            return rc
        rc = 0
        for _ in range(runs):
            rc, summary = self._run_once(full_report=full_report, detailed=detailed)
            print(summary)
        return rc

    def _run_persistent(self, runs: int, *, full_report: bool, detailed: bool) -> tuple[int, str]:
        """Persist mode: spawn the warm pool ONCE, dispatch the suite `runs` times through it (each
        worker's session — and its session-scoped fixtures — set up once and reused for every run), tear
        down. Same robust pool + anchor path as serve mode: each run dispatches the suite ONCE, so no
        worker re-runs the same item back-to-back. The default per-run fork (`_run_once`) re-pays session
        setup every run; this amortizes it."""
        pool = self._spawn_persist_pool(send_nodeids=False)
        try:
            rc = 0
            summaries: list[str] = []
            for _ in range(max(1, runs)):
                run_rc, summary = self._run_persist_request(
                    pool,
                    selection=None,
                    progress_conn=None,
                    report_conn=None,
                    full_report=full_report,
                    detailed=detailed,
                    stop_on_failure=False,
                )
                rc = rc or run_rc
                summaries.append(summary)
            return rc, "\n\n".join(summaries)
        finally:
            self._teardown_persist_pool(pool)

    # ── persistent worker pool (serve --persist-workers) ─────────────────────
    def _spawn_persist_pool(self, *, send_nodeids: bool) -> _PersistPool:
        """Fork the warm worker pool ONCE (serve persist mode): each worker holds a long-lived pytest
        session. Binds a stable pool socket, starts the workers, and captures their one-time 'ready'
        (item count +, in selection mode, the collected nodeids used to resolve a request's selection)."""
        sock_path = f"{tempfile.gettempdir()}/pytest_fast_persist_{os.getpid()}.sock"
        Path(sock_path).unlink(missing_ok=True)
        server = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        server.bind(sock_path)
        server.listen(self.num_workers)
        testrunuid = uuid.uuid4().hex
        active_items = cast(
            "_ActiveItemSlots",
            self.ctx.RawArray("q", [_ACTIVE_ITEM_IDLE] * self.num_workers),
        )
        procs: list[BaseProcess] = [
            self.ctx.Process(
                target=_worker_persist_main,
                args=(wid, sock_path),
                kwargs={
                    "send_nodeids": send_nodeids,
                    "num_workers": self.num_workers,
                    "testrunuid": testrunuid,
                    "active_items": active_items,
                },
                daemon=True,
            )
            for wid in range(self.num_workers)
        ]
        self._arm_collect_flag()
        for p in procs:
            p.start()
        conns: list[socket.socket] = []
        total = 0
        nodeids: list[str] | None = None
        server.settimeout(_WORKER_ACCEPT_TIMEOUT)
        for _ in range(self.num_workers):
            conn, _addr = server.accept()
            msg, _ = _recv(conn)  # ('ready', wid, count, collect_wall, nodeids, collect_errors)
            conns.append(conn)
            if isinstance(msg, tuple) and len(msg) >= 5 and msg[0] == "ready":
                total = cast("int", msg[2])
                if send_nodeids and isinstance(msg[4], list):
                    nodeids = cast("list[str]", msg[4])
                if len(msg) > 5 and isinstance(msg[5], list):
                    self._collect_errors = cast("list[tuple[str, str]]", msg[5])
        server.settimeout(None)
        return _PersistPool(
            server=server,
            sock_path=sock_path,
            procs=procs,
            conns=conns,
            total=total,
            nodeids=nodeids,
            active_items=active_items,
        )

    def _dispatch_persist(
        self,
        pool: _PersistPool,
        *,
        selection: list[str] | None,
        progress_conn: socket.socket | None,
        report_conn: socket.socket | None,
        full_report: bool,
        stop_on_failure: bool,
    ) -> _RunOutcome:
        """One run over the WARM pool: hand each worker item indices (work-stealing), send 'flush' when
        the queue drains, stream reports, collect results + per-run stats. Workers stay alive after
        'run_done' (session warm). The one-item lag means a worker's last result arrives post-'flush'."""
        t0 = time.perf_counter()
        for wid in range(len(pool.active_items)):
            pool.active_items[wid] = _ACTIVE_ITEM_IDLE
        pick_list: list[int] | None = None
        if selection is not None and pool.nodeids is not None:
            idx_of = {nid: i for i, nid in enumerate(pool.nodeids)}
            pick_list = [idx_of[n] for n in selection if n in idx_of]
        planned_total = len(pick_list) if pick_list is not None else pool.total
        results: list[RunResult] = []
        worker_stats: list[WorkerStats] = []
        flushed: set[socket.socket] = set()
        done: set[socket.socket] = set()
        reserved: dict[socket.socket, int] = {}
        queue_pos = 0
        dispatched_total = 0
        stop_dispatch = False
        tx = rx = req_count = 0

        def pick_at(position: int) -> int | None:
            if stop_dispatch or position >= planned_total:
                return None
            if pick_list is not None:
                return pick_list[position]
            return position

        def reserve_next() -> int | None:
            nonlocal queue_pos
            pick = pick_at(queue_pos)
            if pick is not None:
                queue_pos += 1
            return pick

        sel = selectors.DefaultSelector()
        for conn in pool.conns:
            sel.register(conn, selectors.EVENT_READ)
        t_ready = time.perf_counter()
        try:
            while len(done) < len(pool.conns):
                for key, _mask in sel.select():
                    conn = cast("socket.socket", key.fileobj)
                    msg, nbytes = _recv(conn)
                    rx += nbytes
                    if not isinstance(msg, tuple) or not msg:
                        done.add(conn)  # worker died → result undercount flags the run untrusted
                        reserved.pop(conn, None)
                        sel.unregister(conn)
                        continue
                    parts = cast("tuple[object, ...]", msg)
                    kind = parts[0]
                    if kind == "run_done":
                        worker_stats.append(cast("WorkerStats", parts[2]))
                        done.add(conn)
                        sel.unregister(conn)
                        continue
                    req_count += 1
                    # kind == "req": ship any carried result, then hand out the next index or 'flush'
                    result = cast("RunResult | None", parts[2])
                    if result is not None:
                        results.append(result)
                        if progress_conn is not None:
                            with suppress(OSError):
                                _send(progress_conn, {"progress": (len(results), planned_total)})
                        if report_conn is not None:
                            for rep in result.get("reports", []):
                                with suppress(OSError):
                                    _send(report_conn, {"report": rep})
                        if stop_on_failure and result["outcome"] in {"failed", "error"}:
                            stop_dispatch = True
                            reserved.clear()
                    if conn in flushed:
                        continue  # post-flush last result already shipped; just await run_done

                    pick = None if stop_dispatch else reserved.pop(conn, None)
                    if pick is None:
                        pick = reserve_next()
                    if pick is not None:
                        dispatched_total += 1
                        next_pick = reserve_next() if stop_on_failure else None
                        if next_pick is not None:
                            reserved[conn] = next_pick
                        tx += _send(conn, ("idx", pick, stop_on_failure, next_pick, full_report))
                    else:
                        tx += _send(conn, ("flush", full_report))
                        flushed.add(conn)
        finally:
            sel.close()
        t_done = time.perf_counter()
        # Workers stay ALIVE (persist) → exitcode None → never 'crashed'; a died worker undercounts results.
        exitcodes = [p.exitcode for p in pool.procs]
        short_circuited = stop_dispatch and dispatched_total < planned_total
        total = dispatched_total if short_circuited else planned_total
        return _RunOutcome(
            results=results,
            worker_stats=worker_stats,
            bus={"tx": float(tx), "rx": float(rx), "req_count": float(req_count)},
            warmup=t_ready - t0,
            run_wall=t_done - t_ready,
            total=total,
            planned_total=planned_total,
            short_circuited=short_circuited,
            idx=1,
            exitcodes=exitcodes,
            process_failures=_process_failure_nodeids(pool.active_items, pool.nodeids, results),
        )

    def _run_persist_request(
        self,
        pool: _PersistPool,
        *,
        selection: list[str] | None,
        progress_conn: socket.socket | None,
        report_conn: socket.socket | None,
        full_report: bool,
        detailed: bool,
        stop_on_failure: bool,
    ) -> tuple[int, str]:
        """Serve a single `run` request against the warm pool, render its summary (workers stay warm)."""
        if selection is not None:
            collected = set(pool.nodeids or ())
            missing = list(dict.fromkeys(nodeid for nodeid in selection if nodeid not in collected))
            if missing:
                self._last_outcome = None
                preview = ", ".join(missing[:5])
                suffix = f" (+{len(missing) - 5} more)" if len(missing) > 5 else ""
                return 2, f"[pytest-fast] unknown selected nodeid(s): {preview}{suffix}"
        o = self._dispatch_persist(
            pool,
            selection=selection,
            progress_conn=progress_conn,
            report_conn=report_conn,
            full_report=full_report,
            stop_on_failure=stop_on_failure,
        )
        self._last_outcome = o
        summary = self._report(
            o.results,
            o.worker_stats,
            o.bus,
            o.total,
            warmup=o.warmup,
            run=o.run_wall,
            label="run (persist, warm session)",
            full_report=full_report,
            detailed=detailed,
        )
        rc = 1 if self._collect_errors or any(r["outcome"] in {"failed", "error"} for r in o.results) else 0
        if self._run_untrusted(o):
            rc = 1
            summary += (
                f"\n  ⚠ UNTRUSTED RUN — worker crashed / result undercount: "
                f"results={len(o.results)}/{o.total}, worker exitcodes={o.exitcodes} (see daemon log)"
            )
        return rc, summary

    def _teardown_persist_pool(self, pool: _PersistPool) -> None:
        """Stop the warm pool at daemon shutdown: tell each worker to finalize its session and exit."""
        for conn in pool.conns:
            with suppress(OSError):
                _send(conn, ("stop",))
        for p in pool.procs:
            p.join(timeout=_WORKER_JOIN_TIMEOUT)
            if p.is_alive():
                p.kill()
                p.join(timeout=1.0)
        for conn in pool.conns:
            with suppress(OSError):
                conn.close()
        pool.server.close()
        Path(pool.sock_path).unlink(missing_ok=True)

    def serve(self, address: str, ttl: float) -> int:
        """Resident daemon. Collect once; idle>ttl → exit; sources changed → stale-exit.

        The forkserver holds the code AND env loaded AT BOOT TIME. If src/tests were
        edited afterwards — forks would run STALE code; if relevant env (any var
        whose prefix is in `PYTEST_FAST_ENV_PREFIXES`, plus addopts) changed — they
        would run with the stale collect/patches. So on every `run`/`status` request
        we compare max(mtime) of sources AND the caller's env fingerprint against the
        boot snapshot (see `_stale_reason`): on mismatch we reply {'stale': True} and
        exit, the client spawns a fresh daemon (fresh collect).

        Control protocol (one message per connect, serialized by the accept loop —
        which is why it never tears an active run apart):
          * ('run', fp)            → stale check (mtime+env fp), then run + stream + summary;
            trailing fresh_workers retires the persistent pool and uses a new worker for this request;
          * ('status', fp)         → {'ready': True, 'stale': bool} (cheap, for watcher/client);
          * ('shutdown',)          → {'bye': True} and exit (watcher shuts the old one AFTER its run);
          * ('promote', new_addr)  → rebind to new_addr (staging→canonical on promote).
        """
        boot_mtime = _max_source_mtime()  # baseline BEFORE boot: an edit mid-build → stale
        boot_fp = _env_fingerprint()  # env snapshot at boot: change to relevant env → stale-respawn
        check_sources = not _immutable_sources_enabled()
        _log("daemon", f"booting — collect once ({self.start_method}, {self.num_workers}w)…")
        t0 = time.perf_counter()
        self._arm_collect_flag()  # arm the env flag right before the first Process.start()
        boot = self.ctx.Process(target=_noop)
        boot.start()
        boot.join()  # forks the forkserver → it imports preload (collect) → warm
        _log("daemon", f"ready in {time.perf_counter() - t0:.2f}s, listening {address}, ttl={ttl}s")

        cur = address  # current listening address — may change via ('promote', …)
        ctl = _bind_ctl(cur, ttl)
        # Warm worker pool for --persist-workers: forked lazily on the first run and reused across all
        # requests (session-scoped fixtures set up once). None in the default per-run-fork mode.
        pool: _PersistPool | None = None
        try:
            while True:
                try:
                    conn, _addr = ctl.accept()
                except TimeoutError:
                    _log("daemon", f"idle > {ttl}s — shutting down")
                    return 0
                with conn:
                    # Bound the command read against a "slowloris" peer: the accept loop is
                    # serial, so a same-user process that connects and sends nothing (or a
                    # partial header) would otherwise block this blocking `_recv` forever and
                    # wedge the daemon for everyone. Legitimate clients send the whole command
                    # frame up front (it's already in the socket buffer by the time we accept),
                    # so a short read deadline never trips them. TimeoutError ⊂ OSError.
                    conn.settimeout(_CONTROL_CMD_TIMEOUT)
                    try:
                        msg, _ = _recv(conn)
                    except OSError:
                        continue  # stalled / reset peer — drop it, keep serving
                    # A run streams progress/reports for as long as the suite takes; clear the
                    # deadline so a long but healthy run isn't aborted mid-stream.
                    conn.settimeout(None)
                    if not isinstance(msg, tuple) or not msg:
                        continue  # empty/garbled connect (ping/probe) or empty tuple
                    parts = cast("tuple[object, ...]", msg)  # control: (cmd, *args)
                    cmd = parts[0]
                    # Slice for fp, NOT `parts[1] if len(parts) > 1`: the len() guard makes
                    # pyright narrow tuple arity and breaks `parts[1]` in the promote branch.
                    fp_args = parts[1:]
                    client_fp = str(fp_args[0]) if fp_args else None  # caller env fingerprint
                    if cmd == "status":
                        # _try_send (not _send): a status client that already hit its own recv
                        # timeout and disconnected must not crash us with a BrokenPipe on reply.
                        _try_send(
                            conn,
                            {
                                "ready": True,
                                "stale": _stale_reason(
                                    boot_mtime,
                                    boot_fp,
                                    client_fp,
                                    check_sources=check_sources,
                                )
                                is not None,
                            },
                        )
                        continue
                    if cmd == "shutdown":
                        _try_send(conn, {"bye": True})  # reply best-effort; shut down regardless
                        _log("daemon", "shutdown requested — exiting")
                        return 0  # finally releases socket+pid
                    if cmd == "promote":
                        # Derive new_addr from the fp_args slice (not parts[1]) to keep pyright's
                        # tuple-arity narrowing happy (see the fp_args comment above).
                        if not fp_args:
                            continue  # malformed promote (no address)
                        new_addr = str(fp_args[0])
                        # A promote may only retarget within the SAME directory as the current
                        # address (staging→canonical are siblings). The control socket is
                        # connectable by any same-user process and new_addr flows into
                        # _redirect_stdio's log path — an arbitrary path would let a stray/hostile
                        # peer redirect the daemon's stdio. Reject anything else.
                        if Path(new_addr).parent != Path(cur).parent or new_addr == cur:
                            _try_send(conn, {"promoted": False})
                            _log("daemon", f"refused promote to unexpected address {new_addr!r}")
                            continue
                        ctl.close()
                        _remove_pid(cur)
                        Path(cur).unlink(missing_ok=True)
                        cur = new_addr
                        ctl = _bind_ctl(cur, ttl)
                        _redirect_stdio(_daemon_log_path(cur))  # lifecycle logs → log of the new address
                        _try_send(conn, {"promoted": True})
                        _log("daemon", f"promoted → listening {cur}")
                        continue
                    if cmd != "run":
                        # Unknown/garbage command from a same-user peer. Must be ignored, NOT
                        # treated as a run: an arbitrary tuple like ('x', 'y') used to fall into
                        # the run branch, where a non-matching fingerprint made the daemon reply
                        # {stale} and EXIT — i.e. any stray frame could shut the resident daemon
                        # down (a same-user DoS). Real clients always send the verb "run".
                        continue
                    # run request: ('run', fp[, full_report[, stream[, nodeids[, detailed]]]])
                    reason = _stale_reason(
                        boot_mtime,
                        boot_fp,
                        client_fp,
                        check_sources=check_sources,
                    )
                    if reason is not None:
                        _log("daemon", f"{reason} — exiting stale")
                        _try_send(conn, {"stale": True})  # best-effort; exit stale regardless
                        return 0  # finally releases the socket → client spawns a fresh daemon
                    # Optional args: ('run', fp, full_report[, stream[, nodeids[, detailed[, bench
                    # [, want_results]]]]]). Old clients send a shorter tuple → positional defaults;
                    # newer clients' extra elements are simply not read by an older daemon. `stream`
                    # (the plugin controller) asks the daemon to stream the serialized per-phase
                    # reports back; `nodeids` (the controller's collected set) restricts the run to
                    # that selection. Execution-mode fields are deliberate exceptions to silent compatibility:
                    # their clients require acknowledgements in the final frame.
                    full_report = bool(fp_args[1]) if len(fp_args) > 1 else False
                    stream = len(fp_args) > 2 and bool(fp_args[2])
                    selection = (
                        cast("list[str]", fp_args[3]) if len(fp_args) > 3 and isinstance(fp_args[3], list) else None
                    )
                    # `detailed` (CLI `--detailed`) adds the extended parallelism block; `bench`
                    # (CLI `--bench=N`, an int run-count) renders the deterministic bottleneck report
                    # instead (N runs, warmup dropped, full reports forced internally). Both are
                    # irrelevant in stream mode (the plugin controller renders natively).
                    detailed = len(fp_args) > 4 and bool(fp_args[4])
                    bench = int(fp_args[5]) if len(fp_args) > 5 and isinstance(fp_args[5], int) else 0
                    # `want_results` (wrapper clients): also ship the run's lean per-test results +
                    # worker stats + run meta in the final frame. See `request_run(want_results=...)`.
                    want_results = len(fp_args) > 6 and bool(fp_args[6])
                    # `stop_on_failure` is a persistent-pool scheduler contract: do not dispatch a
                    # new selected item after an observed failed/error result. It trails every older
                    # field so old clients and daemons keep their existing behavior.
                    stop_on_failure = len(fp_args) > 7 and bool(fp_args[7])
                    # `fresh_workers` retires an existing persistent pool and executes this request
                    # with the fork-per-request path while keeping the collected controller alive.
                    fresh_workers = len(fp_args) > 8 and bool(fp_args[8])
                    self._last_outcome = None  # a mode that doesn't produce one must not ship a stale one
                    if fresh_workers and stop_on_failure:
                        _try_send(
                            conn,
                            {
                                "rc": 2,
                                "summary": "[pytest-fast] fresh_workers cannot be combined with stop_on_failure",
                                "stop_on_failure": True,
                                "fresh_workers": True,
                            },
                        )
                        continue
                    if fresh_workers and bench > 0:
                        _try_send(
                            conn,
                            {
                                "rc": 2,
                                "summary": "[pytest-fast] fresh_workers cannot be combined with bench",
                                "fresh_workers": True,
                            },
                        )
                        continue
                    if stop_on_failure and bench > 0:
                        _try_send(
                            conn,
                            {
                                "rc": 2,
                                "summary": "[pytest-fast] stop_on_failure cannot be combined with bench",
                                "stop_on_failure": True,
                            },
                        )
                        continue
                    if stop_on_failure and not self.persist_workers:
                        _try_send(
                            conn,
                            {
                                "rc": 2,
                                "summary": "[pytest-fast] stop_on_failure requires a --persist-workers daemon",
                                "stop_on_failure": True,
                            },
                        )
                        continue
                    if want_results and self._collect_errors:
                        frame: dict[str, object] = {
                            "rc": 2,
                            "summary": "[pytest-fast] collection errors make compact results incomplete",
                            "collection_errors": len(self._collect_errors),
                        }
                        if stop_on_failure:
                            frame["stop_on_failure"] = True
                        if fresh_workers:
                            frame["fresh_workers"] = True
                        _try_send(conn, frame)
                        continue
                    if fresh_workers:
                        if pool is not None:
                            self._teardown_persist_pool(pool)
                            pool = None
                        if stream:
                            rc, summary = self._run_once(
                                full_report=True,
                                report_conn=conn,
                                selection=selection,
                                abort_on_collect_errors=want_results,
                            )
                        else:
                            rc, summary = self._run_once(
                                progress_conn=conn,
                                full_report=full_report,
                                selection=selection,
                                detailed=detailed,
                                abort_on_collect_errors=want_results,
                            )
                    elif self.persist_workers and bench == 0:
                        # Warm pool: fork once (lazily), reuse across requests → session fixtures set up
                        # once. Always capture nodeids so any request's selection resolves to indices.
                        if pool is None:
                            pool = self._spawn_persist_pool(send_nodeids=True)
                        if want_results and self._collect_errors:
                            rc = 2
                            summary = "[pytest-fast] collection errors make compact results incomplete"
                        elif stream:
                            rc, summary = self._run_persist_request(
                                pool,
                                selection=selection,
                                progress_conn=None,
                                report_conn=conn,
                                full_report=True,
                                detailed=False,
                                stop_on_failure=stop_on_failure,
                            )
                        else:
                            rc, summary = self._run_persist_request(
                                pool,
                                selection=selection,
                                progress_conn=conn,
                                report_conn=None,
                                full_report=full_report,
                                detailed=detailed,
                                stop_on_failure=stop_on_failure,
                            )
                    elif stream:
                        # Controller renders natively from the streamed reports → no daemon-side
                        # progress frames (progress_conn=None), full reports required.
                        rc, summary = self._run_once(full_report=True, report_conn=conn, selection=selection)
                    elif bench > 0:
                        rc, summary = self._run_bench(bench, progress_conn=conn)
                    else:
                        # progress_conn=conn: workers write dots into the DAEMON log, not the
                        # client's terminal — so we stream progress over this same socket
                        # (otherwise the client sits silent the whole run and looks frozen).
                        rc, summary = self._run_once(
                            progress_conn=conn,
                            full_report=full_report,
                            selection=selection,
                            detailed=detailed,
                            abort_on_collect_errors=want_results,
                        )
                    if want_results and self._collect_errors:
                        final_frame = {
                            "rc": 2,
                            "summary": "[pytest-fast] collection errors make compact results incomplete",
                            "collection_errors": len(self._collect_errors),
                        }
                    else:
                        final_frame = {"rc": rc, "summary": summary}
                    if stop_on_failure:
                        final_frame["stop_on_failure"] = True
                    if fresh_workers:
                        final_frame["fresh_workers"] = True
                    if want_results and self._last_outcome is not None:
                        o = self._last_outcome
                        final_frame["results"] = _lean_results(o.results)
                        final_frame["worker_stats"] = o.worker_stats
                        final_frame["run_meta"] = {
                            "total": o.total,
                            "planned_total": o.planned_total,
                            "short_circuited": o.short_circuited,
                            "process_failures": o.process_failures,
                            "warmup": o.warmup,
                            "run_wall": o.run_wall,
                            "num_workers": self.num_workers,
                            "start_method": self.start_method,
                        }
                    try:
                        _send(conn, final_frame)
                    except OSError:
                        # client gone (Ctrl-C) before the final frame — the run is already done,
                        # the daemon does NOT crash (used to crash on BrokenPipe here), stays warm
                        _log("daemon", "client gone before summary; run completed, staying warm")
        finally:
            if pool is not None:
                self._teardown_persist_pool(pool)  # stop the warm workers → finalize their sessions
            ctl.close()
            _remove_pid(cur)
            Path(cur).unlink(missing_ok=True)

    # ── one run (fork workers + work-stealing dispatch) ──────────────────────

    def _execute_run(
        self,
        progress_conn: socket.socket | None,
        *,
        full_report: bool,
        report_conn: socket.socket | None,
        selection: list[str] | None,
        profile: bool = False,
        abort_on_collect_errors: bool = False,
    ) -> _RunOutcome:
        """One fork→serve→collect cycle. Returns raw results + timing + integrity, NO rendering —
        shared by the single-run `_run_once` and the N-run `_run_bench`. `profile` (the bench
        targeted pass) makes the workers run each test under cProfile."""
        idx = self._run_counter
        self._run_counter += 1
        t0 = time.perf_counter()
        # Per-run worker socket (short name in TMPDIR — pid+idx unique, AF_UNIX limit
        # not breached). `tempfile.gettempdir()` honors $TMPDIR (matters for sandboxes
        # and tmpfs setups where `/tmp` might not exist or be read-only).
        sock_path = f"{tempfile.gettempdir()}/pytest_fast_{os.getpid()}_{idx}.sock"
        Path(sock_path).unlink(missing_ok=True)
        server = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        server.bind(sock_path)
        server.listen(self.num_workers)

        send_nodeids = selection is not None
        # One run id shared by every worker in this run (xdist's PYTEST_XDIST_TESTRUNUID semantics);
        # per-worker isolation comes from the distinct workerid, not this.
        testrunuid = uuid.uuid4().hex
        active_items = cast(
            "_ActiveItemSlots",
            self.ctx.RawArray("q", [_ACTIVE_ITEM_IDLE] * self.num_workers),
        )
        procs = [
            self.ctx.Process(
                target=_worker_main,
                args=(wid, sock_path, full_report, send_nodeids, profile, self.num_workers, testrunuid, active_items),
                daemon=True,
            )
            for wid in range(self.num_workers)
        ]
        self._arm_collect_flag()  # local-run mode (serve() also calls; repeated invocation is idempotent)
        for p in procs:
            p.start()

        try:
            results, worker_stats, bus, t_ready, total, process_failures = self._serve_bus(
                server,
                progress_conn,
                report_conn,
                selection,
                active_items,
                abort_on_collect_errors=abort_on_collect_errors,
            )
        finally:
            # Bounded join: a healthy worker exits within milliseconds of sending `fin`
            # (it calls `os._exit(0)`). If join exceeds the budget, the worker is wedged
            # (rare — non-daemon thread the `os._exit` guard missed, or a crash before
            # the exit call) and we kill it rather than wait forever. The bus has already
            # closed; nothing more is pending from a wedged worker.
            for p in procs:
                p.join(timeout=_WORKER_JOIN_TIMEOUT)
                if p.is_alive():
                    print(
                        f"[pytest-fast] worker pid={p.pid} did not exit within "
                        f"{_WORKER_JOIN_TIMEOUT}s after fin — killing",
                        file=sys.stderr,
                        flush=True,
                    )
                    p.kill()
                    p.join(timeout=1.0)
            server.close()
            Path(sock_path).unlink(missing_ok=True)
        t_done = time.perf_counter()

        if self.dump_path is not None:
            with Path(self.dump_path).open("w") as f:
                json.dump({r["nodeid"]: r["outcome"] for r in results}, f, indent=0, sort_keys=True)

        return _RunOutcome(
            results=results,
            worker_stats=worker_stats,
            bus=bus,
            warmup=t_ready - t0,
            run_wall=t_done - t_ready,
            total=total,
            planned_total=total,
            short_circuited=False,
            idx=idx,
            exitcodes=[p.exitcode for p in procs],
            process_failures=process_failures,
        )

    @staticmethod
    def _run_untrusted(o: _RunOutcome) -> bool:
        """A worker may die BEFORE sending results (import/assert in `_worker_main`) → results are
        partial and rc would be a false green (possibly n=0/0). A non-zero worker exitcode OR a
        result undercount (< dispatched total) → the run is NOT trusted. The only trusted planned
        shortfall is a stop-first run backed by an observed failed/error result."""
        crashed = any(code not in (0, None) for code in o.exitcodes)
        incomplete = o.total > 0 and len(o.results) < o.total
        observed_failure = any(result["outcome"] in {"failed", "error"} for result in o.results)
        invalid_short_circuit = o.short_circuited and (not observed_failure or o.total >= o.planned_total)
        unexplained_shortfall = o.total < o.planned_total and not o.short_circuited
        return crashed or incomplete or invalid_short_circuit or unexplained_shortfall

    def _run_once(
        self,
        progress_conn: socket.socket | None = None,
        *,
        full_report: bool = False,
        report_conn: socket.socket | None = None,
        selection: list[str] | None = None,
        detailed: bool = False,
        abort_on_collect_errors: bool = False,
    ) -> tuple[int, str]:
        o = self._execute_run(
            progress_conn,
            full_report=full_report,
            report_conn=report_conn,
            selection=selection,
            abort_on_collect_errors=abort_on_collect_errors,
        )
        self._last_outcome = o
        label = "BOOT (collect once)" if o.idx == 0 else f"run #{o.idx} (warm)"
        summary = self._report(
            o.results,
            o.worker_stats,
            o.bus,
            o.total,
            warmup=o.warmup,
            run=o.run_wall,
            label=label,
            full_report=full_report,
            detailed=detailed,
        )
        if selection is not None and o.total != len(selection):
            self._last_outcome = None
            missing_count = len(selection) - o.total
            return 2, f"{summary}\n[pytest-fast] {missing_count} unknown selected nodeid(s)"
        rc = 1 if self._collect_errors or any(r["outcome"] in {"failed", "error"} for r in o.results) else 0
        if self._run_untrusted(o):
            rc = 1
            summary += (
                f"\n  ⚠ UNTRUSTED RUN — worker crashed / result undercount: "
                f"results={len(o.results)}/{o.total}, worker exitcodes={o.exitcodes} (see daemon log)"
            )
        return rc, summary

    def _run_bench(self, n_runs: int, progress_conn: socket.socket | None = None) -> tuple[int, str]:
        """`--bench=N`: run the suite N times (full reports), drop the FIRST as warmup (its fork +
        first-touch DB/cache costs are unrepresentative), render the deterministic bottleneck report
        over the averaged remainder. N=1 keeps the single (warmup-tainted) run. Then a TARGETED
        cProfile pass over just the top bottleneck tests adds function-level attribution — wise
        orchestration: pay the profiler's overhead only on the handful of tests that hold the wall."""
        runs = [
            self._execute_run(progress_conn, full_report=True, report_conn=None, selection=None)
            for _ in range(max(1, n_runs))
        ]
        measured = runs[1:] if len(runs) > 1 else runs  # drop warmup when we have more than one
        avg_wall = sum(o.run_wall for o in measured) / len(measured)
        result_runs = [o.results for o in measured]
        profiles = self._profile_top_tests(result_runs)
        summary = _bench_report(
            result_runs,
            run=avg_wall,
            cores=self.num_workers,  # the ACTUAL parallelism of this run, not the machine default
            warmup_dropped=len(runs) > 1,
            profiles=profiles,
        )
        rc = (
            1
            if self._collect_errors
            or any(self._run_untrusted(o) or any(r["outcome"] in {"failed", "error"} for r in o.results) for o in runs)
            else 0
        )
        return rc, summary

    def _profile_top_tests(self, result_runs: list[list[RunResult]]) -> dict[str, list[tuple[str, int, float, float]]]:
        """Targeted cProfile pass: rank tests by average wall, re-run ONLY the top
        `_BENCH_PROFILE_NODES` under cProfile (one extra run of a handful of tests, cheap against the
        warm forkserver), and return {nodeid: top cProfile rows}. Best-effort — any failure just
        means the bench report renders without function-level attribution."""
        from collections import defaultdict

        agg: dict[str, list[float]] = defaultdict(lambda: [0.0, 0.0])  # nodeid → [count, sum(duration)]
        for results in result_runs:
            for r in results:
                a = agg[r["nodeid"]]
                a[0] += 1
                a[1] += r["duration"]
        ranked = sorted(((a[1] / a[0], nid) for nid, a in agg.items() if a[0]), reverse=True)
        top = [nid for _dur, nid in ranked[:_BENCH_PROFILE_NODES]]
        if not top:
            return {}
        try:
            o = self._execute_run(None, full_report=False, report_conn=None, selection=top, profile=True)
        except Exception as exc:
            _log("daemon", f"bench profiling pass failed ({exc!r}) — report renders without profiles")
            return {}
        return {r["nodeid"]: r["profile"] for r in o.results if "profile" in r}

    def _serve_bus(
        self,
        server: socket.socket,
        progress_conn: socket.socket | None = None,
        report_conn: socket.socket | None = None,
        selection: list[str] | None = None,
        active_items: _ActiveItemSlots | None = None,
        *,
        abort_on_collect_errors: bool = False,
    ) -> tuple[list[RunResult], list[WorkerStats], dict[str, float], float, int, list[str]]:
        # Worker connect with timeout: if a forked worker died BEFORE connect (warmup
        # crash), we don't block in accept() forever — start with whoever made it.
        sel = selectors.DefaultSelector()
        # try/finally so the selector's kernel fd (kqueue/epoll) is always closed: the
        # selector is part of a reference cycle (BaseSelector ↔ its map), so refcounting
        # alone won't free it until cyclic GC — and the resident daemon gc.freeze()s at boot
        # and rarely GCs → one leaked fd per run → eventual EMFILE.
        try:
            server.settimeout(_WORKER_ACCEPT_TIMEOUT)
            for _ in range(self.num_workers):
                try:
                    conn, _addr = server.accept()
                except TimeoutError:
                    break
                sel.register(conn, selectors.EVENT_READ)
            server.settimeout(None)
            expected = len(sel.get_map())

            total: int | None = None
            # When `selection` is set (the --fast plugin forwards the controller's collected
            # nodeids), pick_list holds the daemon-side item indices to actually run, built from
            # the first worker 'ready' that carries the nodeid list. None → run the full suite.
            pick_list: list[int] | None = None
            collected_nodeids: list[str] | None = None
            selection_known_total: int | None = None
            queue_pos = 0
            results: list[RunResult] = []
            worker_stats: list[WorkerStats] = []
            tx = rx = req_count = 0
            t_ready = 0.0
            ready_seen = 0
            active = expected
            last_emit = 0.0

            def emit_progress(*, force: bool = False) -> None:
                nonlocal progress_conn, last_emit
                tgt = len(pick_list) if pick_list is not None else total  # how many we actually run
                if progress_conn is None or tgt is None:
                    return
                now = time.perf_counter()
                done = len(results)
                if not force and done < tgt and now - last_emit < _PROGRESS_THROTTLE_SEC:
                    return  # throttled to _PROGRESS_THROTTLE_SEC; the final frame (done==tgt) is always sent
                last_emit = now
                try:
                    _send(progress_conn, {"progress": (done, tgt)})
                except OSError:
                    progress_conn = None  # client gone (Ctrl-C) — stop sending, but complete the run

            def emit_reports(result: RunResult) -> None:
                # Stream each phase report ({'report': <serialized>}) to the controller as it
                # arrives (full-report/plugin mode) so it can republish into a real terminalreporter
                # live. No-op unless report_conn is set and the result carries full reports.
                nonlocal report_conn
                if report_conn is None:
                    return
                reps = result.get("reports")
                if not reps:
                    return
                for rep in reps:
                    try:
                        _send(report_conn, {"report": rep})
                    except OSError:
                        report_conn = None  # controller gone — stop streaming, complete the run
                        return

            while active > 0:
                for key, _mask in sel.select():
                    conn = key.fileobj
                    assert isinstance(conn, socket.socket)
                    msg, nbytes = _recv(conn)
                    rx += nbytes
                    if not isinstance(msg, tuple):
                        sel.unregister(conn)
                        conn.close()
                        active -= 1
                        continue
                    parts = cast("tuple[object, ...]", msg)  # worker msg: ('ready'|'req'|'fin', …)
                    kind = parts[0]
                    if kind == "ready":
                        total = cast("int", parts[2])
                        if len(parts) > 4 and isinstance(parts[4], list):
                            collected_nodeids = cast("list[str]", parts[4])
                        if len(parts) > 5 and isinstance(parts[5], list):
                            self._collect_errors = cast("list[tuple[str, str]]", parts[5])
                        # Selection mode: resolve the controller's nodeids → daemon item indices
                        # from the first 'ready' that carries the worker's nodeid list. A mixed
                        # known/unknown request must be rejected before any known item runs: test
                        # setup can mutate external state even though the final protocol rc is 2.
                        if selection is not None and pick_list is None and len(parts) > 4 and parts[4] is not None:
                            idx_of = {nid: i for i, nid in enumerate(cast("list[str]", parts[4]))}
                            valid_picks = [idx_of[n] for n in selection if n in idx_of]
                            selection_known_total = len(valid_picks)
                            pick_list = [] if len(valid_picks) != len(selection) else valid_picks
                        ready_seen += 1
                        if ready_seen == expected:
                            t_ready = time.perf_counter()
                    elif kind == "req":
                        result = cast("RunResult | None", parts[2])
                        if result is not None:
                            results.append(result)
                            emit_progress()
                            emit_reports(result)
                        if abort_on_collect_errors and self._collect_errors:
                            pick = None
                        elif pick_list is not None:
                            pick = pick_list[queue_pos] if queue_pos < len(pick_list) else None
                        else:
                            pick = queue_pos if total is not None and queue_pos < total else None
                        if pick is not None:
                            queue_pos += 1
                        try:
                            tx += _send(conn, ("idx", pick))
                        except OSError:
                            # Worker died after sending 'req' (rare). Treat as a disconnect and
                            # finish the run — the result undercount (and the worker's nonzero
                            # exitcode) flag it untrusted in _run_once, rather than crashing the daemon.
                            sel.unregister(conn)
                            conn.close()
                            active -= 1
                            continue
                        req_count += 1
                    else:  # "fin"
                        result = cast("RunResult | None", parts[2])
                        if result is not None:
                            results.append(result)
                            emit_reports(result)
                        worker_stats.append(cast("WorkerStats", parts[3]))
                        sel.unregister(conn)
                        conn.close()
                        active -= 1

            emit_progress(force=True)  # final frame (done==target) — guaranteed
            bus = {"tx": float(tx), "rx": float(rx), "req_count": float(req_count)}
            run_total = selection_known_total if selection_known_total is not None else (total or 0)
            process_failures = (
                _process_failure_nodeids(active_items, collected_nodeids, results) if active_items is not None else []
            )
            return results, worker_stats, bus, t_ready, run_total, process_failures
        finally:
            sel.close()

    def _report(
        self,
        results: list[RunResult],
        worker_stats: list[WorkerStats],
        bus: dict[str, float],
        total: int,
        warmup: float,
        run: float,
        label: str,
        full_report: bool = False,
        detailed: bool = False,
    ) -> str:
        """Render the summary box: build the default named blocks, run them through the
        daemon-plugin pipeline (which may append blocks or rewrite the whole layout — see
        `_apply_daemon_plugins`), then flatten to text."""
        blocks = self._report_blocks(
            results,
            worker_stats,
            bus,
            total,
            warmup=warmup,
            run=run,
            label=label,
            full_report=full_report,
            detailed=detailed,
        )
        run_info: dict[str, object] = {
            "results": results,
            "worker_stats": worker_stats,
            "bus": bus,
            "total": total,
            "warmup": warmup,
            "run_wall": run,
            "num_workers": self.num_workers,
            "start_method": self.start_method,
            "label": label,
            # Additive (0.15): collection failures of this daemon boot — (nodeid, longrepr).
            "collect_errors": list(self._collect_errors),
        }
        blocks = _apply_daemon_plugins(run_info, blocks)
        return "\n".join(ln for block in blocks for ln in block["lines"])

    def _report_blocks(
        self,
        results: list[RunResult],
        worker_stats: list[WorkerStats],
        bus: dict[str, float],
        total: int,
        *,
        warmup: float,
        run: float,
        label: str,
        full_report: bool,
        detailed: bool,
    ) -> list[SummaryBlock]:
        """The DEFAULT summary as named blocks (stable names — the plugin-facing contract):
        `top`, `results`, [`collect-errors`], `warmup`, `run`, `par`, [`detailed`], `bus`, [`failures`], [`xpass`],
        [`durations` (full-report) | `slowest` (lean)], `bottom`. Conditional blocks are simply
        absent when empty. Flattened output is byte-identical to the pre-block renderer."""
        from collections import Counter

        counts = Counter(r["outcome"] for r in results)
        failed = counts["failed"] + counts["error"]
        sum_busy = sum(s["busy"] for s in worker_stats)
        run_walls = [s["run_wall"] for s in worker_stats]
        breakdown = ", ".join(f"{n} {cat}" for cat, n in sorted(counts.items()))
        line = "═" * 66
        blocks: list[SummaryBlock] = [
            {
                "name": "top",
                "lines": [
                    f"\n{line}",
                    f"  {self.start_method.upper()} DAEMON  —  {self.num_workers}w  —  {label}",
                    line,
                ],
            },
            {"name": "results", "lines": [f"  results : {breakdown}  (n={len(results)}/{total})"]},
            {"name": "warmup", "lines": [f"  warmup  : {warmup:6.2f}s   (fork+spawn; ~0 for resident rerun)"]},
            {"name": "run", "lines": [f"  RUN     : {run:6.2f}s   ← wall"]},
            {
                "name": "par",
                "lines": [
                    f"  par.    : {(sum_busy / run if run else 0):.2f}x of {self.num_workers}"
                    f"   (run-wall max={max(run_walls) if run_walls else 0:.2f}"
                    f" min={min(run_walls) if run_walls else 0:.2f})"
                ],
            },
        ]
        if self._collect_errors:
            # Right under `results` (index 2): n above counts only COLLECTED tests — these
            # files never made it into the suite at all, which is exactly why the block is loud.
            error_lines = [
                f"  COLLECT ERRORS ({len(self._collect_errors)}) — files not collected, their tests DID NOT RUN:"
            ]
            for nodeid, longrepr in self._collect_errors:
                error_lines.append(f"    ✗ {nodeid}")
                if longrepr.strip():
                    error_lines.extend(f"      {ln}" for ln in longrepr.splitlines())
            blocks.insert(2, {"name": "collect-errors", "lines": error_lines})
        if detailed:
            metrics = _parallelism_metrics(worker_stats, run, self.num_workers, results)
            # cores = perf-core count (the throughput baseline + the default worker count);
            # logical = the hard cap for any worker-count suggestion.
            cores = _default_workers()
            logical = os.cpu_count() or cores
            blocks.append(
                {"name": "detailed", "lines": _detailed_par_lines(metrics, run, self.num_workers, cores, logical)}
            )
        blocks.append(
            {"name": "bus", "lines": [f"  bus     : {int(bus['req_count'])} round-trips, {bus['rx'] / 1024:.0f}KB rx"]}
        )
        if failed:
            failure_lines = [f"  FAILURES ({failed}):"]
            for r in results:
                if r["outcome"] not in {"failed", "error"}:
                    continue
                failure_lines.append(f"    ✗ {r['nodeid']}")
                longrepr = r.get("longrepr")
                if isinstance(longrepr, str) and longrepr.strip():
                    failure_lines.extend(f"      {ln}" for ln in longrepr.splitlines())
            blocks.append({"name": "failures", "lines": failure_lines})
        xpassed = [r for r in results if r["outcome"] == "xpassed"]
        if xpassed:
            blocks.append(
                {
                    "name": "xpass",
                    "lines": [f"  XPASS ({len(xpassed)}) — stale xfail entries (now pass, drop them):"]
                    + [f"    ? {r['nodeid']}" for r in xpassed],
                }
            )
        if full_report:
            # Full-report mode: a real per-phase --durations table (from serialized reports).
            blocks.append({"name": "durations", "lines": _durations_lines(results)})
        else:
            # Lean mode: only whole-test durations are known — show the ≥1s offenders.
            slow = sorted(results, key=lambda r: r["duration"], reverse=True)
            slow = [r for r in slow if r["duration"] >= 1.0][:10]
            if slow:
                blocks.append(
                    {
                        "name": "slowest",
                        "lines": [f"  SLOWEST (≥1s, top {len(slow)}):"]
                        + [f"    {r['duration']:7.2f}s  {r['nodeid']}" for r in slow],
                    }
                )
        blocks.append({"name": "bottom", "lines": [line]})
        return blocks


def _lean_results(results: list[RunResult]) -> list[RunResult]:
    """Project results to their lean shape for the `want_results` final frame: drop the heavy
    per-phase `reports` (full-report runs) — wrapper clients want nodeid/outcome/duration/cpu/
    extra/longrepr, not a re-ship of the whole wire-form report stream."""
    return [cast("RunResult", {k: v for k, v in r.items() if k != "reports"}) for r in results]


# ── client-side: request a run from the resident daemon ──────────────────────


def _validate_stop_on_failure_ack(frame: dict[str, object], *, requested: bool) -> dict[str, object]:
    """Reject a completed response from a daemon that cannot prove stop-first support."""
    if requested and frame.get("stale") is not True and frame.get("stop_on_failure") is not True:
        return {
            "rc": 2,
            "summary": "[pytest-fast] daemon does not support stop_on_failure; restart it with pytest-fast >=0.14",
        }
    return frame


def _validate_fresh_workers_ack(frame: dict[str, object], *, requested: bool) -> dict[str, object]:
    """Reject a completed response from a daemon that cannot prove fresh-worker support."""
    if requested and frame.get("stale") is not True and frame.get("fresh_workers") is not True:
        return {
            "rc": 2,
            "summary": "[pytest-fast] daemon does not support fresh_workers; restart it with pytest-fast >=0.14",
        }
    return frame


def request_run(
    address: str,
    *,
    full_report: bool = False,
    detailed: bool = False,
    bench: int = 0,
    want_results: bool = False,
    nodeids: list[str] | None = None,
    stop_on_failure: bool = False,
    fresh_workers: bool = False,
) -> dict[str, object]:
    """Trigger a run on the daemon; stream progress to stdout, return the final frame
    (`{rc, summary}` or `{stale: True}`). The daemon sends N `{'progress': (done,total)}`
    frames then one final frame — we recv in a loop until a non-progress frame arrives.

    `full_report` asks the daemon to run workers in full-report mode (per-phase reports →
    a real --durations table in the summary). The flag rides as the 2nd tuple element, so
    a daemon that predates it simply ignores it and runs lean.

    `want_results` (wrapper clients) asks the daemon to also put the run's per-test data into
    the final frame: `results` (lean `RunResult`s — nodeid/outcome/duration/cpu/extra/longrepr,
    never the heavy per-phase `reports`), `worker_stats` (list[WorkerStats]) and `run_meta`
    ({total, process_failures, warmup, run_wall, num_workers, start_method}). `process_failures`
    lists only nodeids whose worker exited while that item's pytest protocol was active. ~100 bytes/test
    on the wire. A daemon predating the flag returns the plain `{rc, summary}` frame — treat the keys as optional.

    `nodeids` restricts the request to an ordered selection. `stop_on_failure` asks a persistent
    worker pool to stop dispatching that selection after the first observed failed/error result.
    The latter intentionally requires a daemon started with `--persist-workers`: a non-persistent
    daemon, a benchmark request, or a daemon predating the acknowledged contract returns rc=2 instead
    of silently running a different contract.

    `fresh_workers` retires an existing persistent pool and runs this request in a newly forked worker
    while retaining the daemon's collected controller. Each fresh request gets a new pytest session.
    It cannot be combined with stop-first or benchmark mode and also requires an explicit daemon ack.

    Module-level (not a method of `Daemon`) — this is the **client**, not a server
    method; keeping it on `Daemon` would mix both protocol sides into one class."""
    if bench > 0 and stop_on_failure:
        return {
            "rc": 2,
            "summary": "[pytest-fast] stop_on_failure cannot be combined with bench",
        }
    if fresh_workers and stop_on_failure:
        return {
            "rc": 2,
            "summary": "[pytest-fast] fresh_workers cannot be combined with stop_on_failure",
        }
    if fresh_workers and bench > 0:
        return {
            "rc": 2,
            "summary": "[pytest-fast] fresh_workers cannot be combined with bench",
        }
    sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    with _short_unix_path(address) as connect_path:
        sock.connect(connect_path)
    with sock:
        # ('run', fp, full_report, stream, nodeids, detailed, bench, want_results,
        # stop_on_failure, fresh_workers) — non-streamed
        # CLI run, so stream=False / nodeids=None; `bench` (an int run-count, 0=off) makes the daemon
        # render the bottleneck report (it forces full reports itself). Trailing elements are ignored
        # by a daemon predating them. Execution-mode responses are accepted only with explicit
        # acknowledgements, so an older daemon cannot silently weaken those contracts.
        _send(
            sock,
            (
                "run",
                _env_fingerprint(),
                full_report,
                False,
                nodeids,
                detailed,
                bench,
                want_results,
                stop_on_failure,
                fresh_workers,
            ),
        )
        while True:
            raw, _ = _recv(sock)
            if not isinstance(raw, dict):
                return {"rc": 1, "summary": "[pytest-fast] daemon closed connection mid-run"}
            frame = cast("dict[str, object]", raw)  # daemon frame: progress | stale | rc/summary
            if "progress" in frame:
                done, total = cast("tuple[int, int]", frame["progress"])
                print(f"\r  running {done}/{total} …", end="", flush=True)
                continue
            print("\r" + " " * 32 + "\r", end="", flush=True)  # erase the progress line
            frame = _validate_stop_on_failure_ack(frame, requested=stop_on_failure)
            return _validate_fresh_workers_ack(frame, requested=fresh_workers)


def _valid_run_result(value: object) -> bool:
    if not isinstance(value, dict):
        return False
    nodeid = value.get("nodeid")
    outcome = value.get("outcome")
    duration = value.get("duration")
    if not (
        isinstance(nodeid, str)
        and outcome in _OUTCOME_PRIORITY
        and isinstance(duration, int | float)
        and not isinstance(duration, bool)
        and math.isfinite(duration)
        and duration >= 0
    ):
        return False
    phases = value.get("phases")
    if phases is None:
        return True
    return isinstance(phases, dict) and all(
        phase in {"setup", "call", "teardown"}
        and isinstance(seconds, int | float)
        and not isinstance(seconds, bool)
        and math.isfinite(seconds)
        and seconds >= 0
        for phase, seconds in phases.items()
    )


def request_run_results(
    address: str,
    on_result: Callable[[RunResult], None],
    *,
    on_progress: Callable[[int, int], None] | None = None,
    nodeids: list[str] | None = None,
    stop_on_failure: bool = False,
    fresh_workers: bool = False,
) -> dict[str, object]:
    """Run a selection through the compact protocol and deliver validated lean results.

    Unlike :func:`request_run`, this wrapper never writes progress to stdout. Unlike
    :func:`request_run_streamed`, it does not ask workers to serialize full pytest reports. The daemon
    still runs the complete pytest protocol and returns one ``RunResult`` per executed item.
    """
    if fresh_workers and stop_on_failure:
        return {
            "rc": 2,
            "summary": "[pytest-fast] fresh_workers cannot be combined with stop_on_failure",
        }
    sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    with _short_unix_path(address) as connect_path:
        sock.connect(connect_path)
    with sock:
        _send(
            sock,
            (
                "run",
                _env_fingerprint(),
                False,
                False,
                nodeids,
                False,
                0,
                True,
                stop_on_failure,
                fresh_workers,
            ),
        )
        while True:
            raw, _ = _recv(sock)
            if not isinstance(raw, dict):
                return {"rc": 1, "summary": "[pytest-fast] daemon closed connection mid-run"}
            frame = cast("dict[str, object]", raw)
            progress = frame.get("progress")
            if progress is not None:
                if not (
                    isinstance(progress, tuple)
                    and len(progress) == 2
                    and all(isinstance(value, int) and not isinstance(value, bool) for value in progress)
                ):
                    return {"rc": 2, "summary": "[pytest-fast] daemon returned malformed progress"}
                if on_progress is not None:
                    on_progress(progress[0], progress[1])
                continue

            frame = _validate_stop_on_failure_ack(frame, requested=stop_on_failure)
            frame = _validate_fresh_workers_ack(frame, requested=fresh_workers)
            if frame.get("stale") is True or frame.get("rc") not in {0, 1}:
                return frame
            results = frame.get("results")
            if not isinstance(results, list) or any(not _valid_run_result(result) for result in results):
                return {"rc": 2, "summary": "[pytest-fast] daemon returned missing or malformed results"}
            for result in results:
                on_result(cast("RunResult", result))
            return frame


def request_run_streamed(
    address: str,
    on_report: Callable[[dict[str, object]], None],
    nodeids: list[str] | None = None,
    *,
    stop_on_failure: bool = False,
    fresh_workers: bool = False,
) -> dict[str, object]:
    """Client for full-report **streaming** (the `pytest --fast` plugin controller). Triggers a
    run in stream mode, invokes `on_report(serialized_report)` for each per-phase report as it
    arrives, and returns the final frame (`{rc, summary}` or `{stale: True}`).

    `nodeids` restricts the run to the controller's collected selection (so -k/-m/paths work);
    None runs the daemon's full suite.

    `stop_on_failure` and `fresh_workers` have the same contracts as in `request_run`.

    The controller replays the streamed reports through its own real terminalreporter, so native
    pytest reporting (--durations / junit / -v/-s / plugins) all work — while the warm forkserver
    daemon does the actual execution (amortized collect, fork-warm workers)."""
    sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    with _short_unix_path(address) as connect_path:
        sock.connect(connect_path)
    with sock:
        # Fill the older trailing fields so execution modes remain backward-compatible tuple tails:
        # full_report, stream, selection, detailed, bench, want_results, stop, fresh.
        _send(
            sock,
            ("run", _env_fingerprint(), True, True, nodeids, False, 0, False, stop_on_failure, fresh_workers),
        )
        while True:
            raw, _ = _recv(sock)
            if not isinstance(raw, dict):
                return {"rc": 1, "summary": "[pytest-fast] daemon closed connection mid-run"}
            frame = cast("dict[str, object]", raw)  # daemon frame: report | stale | rc/summary
            rep = frame.get("report")
            if rep is not None:
                on_report(cast("dict[str, object]", rep))
                continue
            frame = _validate_stop_on_failure_ack(frame, requested=stop_on_failure)
            return _validate_fresh_workers_ack(frame, requested=fresh_workers)


# ── orchestration: ensure resident daemon + run / stale-restart ──────────────


def _split_env_list(name: str, default: list[str]) -> list[str]:
    """Parse a comma/colon-separated env var into a list, falling back to `default`
    when unset. PATH-style semantics: env REPLACES the default (does not add to it).
    An explicit empty value (`PYTEST_FAST_WATCH_DIRS=""`) yields an empty list — that
    is, "scan nothing", which is occasionally useful for tooling."""
    raw = os.environ.get(name)
    if raw is None:
        return default
    return [p.strip() for p in raw.replace(":", ",").split(",") if p.strip()]


def _watch_dirs() -> list[str]:
    """Dirs scanned recursively for `*.py` mtime. Default `src,tests`.
    `PYTEST_FAST_WATCH_DIRS` (comma/colon-separated, repo-relative) REPLACES the
    default — e.g. a flat-layout project sets `PYTEST_FAST_WATCH_DIRS=mypkg,tests`."""
    return _split_env_list("PYTEST_FAST_WATCH_DIRS", ["src", "tests"])


def _watch_files() -> list[str]:
    """Standalone config files included in the mtime scan (repo-relative). Default
    `pyproject.toml,pytest.ini`. `PYTEST_FAST_WATCH_FILES` (comma/colon-separated)
    REPLACES the default — add `setup.cfg`, `tox.ini`, `conftest.py`, etc. as your
    project needs."""
    return _split_env_list("PYTEST_FAST_WATCH_FILES", ["pyproject.toml", "pytest.ini"])


# ── daemon-side plugins (`PYTEST_FAST_DAEMON_PLUGINS`) ───────────────────────
#
# The daemon process holds every run's aggregated data (results incl. per-test
# duration/cpu/extra, worker stats, wall/warmup) but — by architecture — NO pytest
# session (collection lives in the forkserver preload). So daemon-side extensions are
# plain importable modules, not pytest plugins: `PYTEST_FAST_DAEMON_PLUGINS=pkg.mod`
# (comma/colon-separated import paths) names modules exposing either or both of:
#
#     def pytest_fast_run_completed(run_info: dict) -> Iterable[str] | None: ...
#         # additive: the returned lines become a `plugin:<mod>` block placed right
#         # after the `bus` stats block.
#
#     def pytest_fast_render_summary(run_info: dict, blocks: list[SummaryBlock]) -> list[SummaryBlock] | None: ...
#         # FULL control of the summary: receives every named block of the default
#         # render (`top`/`results`/`warmup`/`run`/`par`/[`detailed`]/`bus`/
#         # [`failures`]/[`xpass`]/[`durations`|`slowest`]/`bottom` + earlier
#         # `plugin:*` blocks) and returns the new layout — reorder, drop, rewrite,
#         # inject. Returning None keeps the (possibly mutated in place) list.
#
# Both fire after every non-bench run; run_info keys: `results` (list[RunResult]),
# `worker_stats` (list[WorkerStats]), `bus`, `total`, `warmup`, `run_wall`,
# `num_workers`, `start_method`, `label`. Plugins run in env-list order, each
# `render_summary` seeing the previous one's output (a pipeline).
#
# Failure policy: an import error / missing hooks / raising plugin / malformed
# render return degrades to a ⚠ line INSIDE the summary (never a crashed daemon,
# never a failed run, never a silently-skipped gate). The env var is part of
# `_FINGERPRINT_KEYS`, so editing the plugin LIST respawns the warm daemon; editing
# plugin CODE respawns it only if the module lives under the watched dirs
# (`PYTEST_FAST_WATCH_DIRS`) — put it there.


class SummaryBlock(TypedDict):
    """One named piece of the summary box. `name` is the stable id plugins address
    blocks by; `lines` are printed verbatim (joined with newlines, no wrapping)."""

    name: str
    lines: list[str]


_daemon_plugins_cache: list[tuple[str, ModuleType | None, str | None]] | None = None


def _load_daemon_plugins() -> list[tuple[str, ModuleType | None, str | None]]:
    """Import the `PYTEST_FAST_DAEMON_PLUGINS` modules once per process.
    Entries: (name, module | None, import_error | None)."""
    global _daemon_plugins_cache
    if _daemon_plugins_cache is None:
        import importlib

        loaded: list[tuple[str, ModuleType | None, str | None]] = []
        for name in _split_env_list("PYTEST_FAST_DAEMON_PLUGINS", []):
            try:
                loaded.append((name, importlib.import_module(name), None))
            except Exception as exc:  # isolation barrier: a typo'd plugin must not kill the daemon
                loaded.append((name, None, repr(exc)))
        _daemon_plugins_cache = loaded
    return _daemon_plugins_cache


def _insert_plugin_block(blocks: list[SummaryBlock], block: SummaryBlock) -> None:
    """Place an additive `plugin:*` block after `bus` (after any earlier plugin blocks,
    preserving env-list order); if a render hook removed `bus`, fall back to before
    `bottom`, else append."""
    anchor = -1
    for index, existing in enumerate(blocks):
        if existing["name"] == "bus" or existing["name"].startswith("plugin:"):
            anchor = index
    if anchor >= 0:
        blocks.insert(anchor + 1, block)
        return
    for index, existing in enumerate(blocks):
        if existing["name"] == "bottom":
            blocks.insert(index, block)
            return
    blocks.append(block)


def _coerce_blocks(raw: object) -> list[SummaryBlock] | None:
    """Validate/normalize a render hook's return value. None → malformed (caller keeps
    the previous layout and reports); lines are str()-coerced, names must be strings."""
    if not isinstance(raw, list):
        return None
    coerced: list[SummaryBlock] = []
    for entry in raw:
        if not isinstance(entry, dict):
            return None
        entry_dict = cast("dict[str, object]", entry)
        name = entry_dict.get("name")
        lines = entry_dict.get("lines")
        if not isinstance(name, str) or not isinstance(lines, list):
            return None
        coerced.append({"name": name, "lines": [str(ln) for ln in cast("list[object]", lines)]})
    return coerced


def _apply_daemon_plugins(run_info: dict[str, object], blocks: list[SummaryBlock]) -> list[SummaryBlock]:
    """The daemon-plugin pipeline over the summary blocks (see the module comment above).

    Per plugin, in env-list order: `pytest_fast_run_completed` contributes an additive
    `plugin:<mod>` block after `bus`; `pytest_fast_render_summary` then gets the whole
    layout and may return a replacement. Every failure mode becomes a ⚠ line in a
    `plugin-errors` block before `bottom` — visible, never fatal, never silent."""
    errors: list[str] = []
    for name, module, import_error in _load_daemon_plugins():
        if import_error is not None:
            errors.append(f"  ⚠ daemon plugin {name!r}: import failed: {import_error}")
            continue
        completed_hook = getattr(module, "pytest_fast_run_completed", None)
        render_hook = getattr(module, "pytest_fast_render_summary", None)
        if not callable(completed_hook) and not callable(render_hook):
            errors.append(
                f"  ⚠ daemon plugin {name!r}: neither pytest_fast_run_completed nor pytest_fast_render_summary found"
            )
            continue
        if callable(completed_hook):
            try:
                contributed = cast("Iterable[object] | None", completed_hook(run_info))
            except Exception as exc:  # isolation barrier: plugin bug → visible line, run unaffected
                errors.append(f"  ⚠ daemon plugin {name!r}: pytest_fast_run_completed raised: {exc!r}")
                contributed = None
            if contributed:
                lines = [str(entry) for entry in contributed]
                if lines:
                    _insert_plugin_block(blocks, {"name": f"plugin:{name}", "lines": lines})
        if callable(render_hook):
            try:
                replaced = render_hook(run_info, blocks)
            except Exception as exc:  # isolation barrier: broken layout hook keeps the default render
                errors.append(f"  ⚠ daemon plugin {name!r}: pytest_fast_render_summary raised: {exc!r}")
                continue
            if replaced is None:
                continue  # None = keep (possibly mutated in place) layout
            coerced = _coerce_blocks(replaced)
            if coerced is None:
                errors.append(
                    f"  ⚠ daemon plugin {name!r}: pytest_fast_render_summary returned malformed blocks — ignored"
                )
                continue
            blocks = coerced
    if errors:
        _insert_error_block(blocks, errors)
    return blocks


def _insert_error_block(blocks: list[SummaryBlock], errors: list[str]) -> None:
    for index, existing in enumerate(blocks):
        if existing["name"] == "bottom":
            blocks.insert(index, {"name": "plugin-errors", "lines": errors})
            return
    blocks.append({"name": "plugin-errors", "lines": errors})


def _project_root() -> Path:
    """Project root for the `*.py` mtime scan. Default — `os.getcwd()` at call time
    (where `pytest-fast` was launched from). Override — `PYTEST_FAST_ROOT` (absolute
    or relative path); useful if you launch outside the repo root, or for pytest-fast
    self-tests."""
    override = os.environ.get("PYTEST_FAST_ROOT")
    return Path(override).resolve() if override else Path.cwd()


def _iter_source_paths() -> Iterator[Path]:
    """All files under watch dirs + watch files — a single traversal point for both
    `_max_source_mtime` (which needs max) and `_any_source_newer` (which needs early-exit)."""
    root = _project_root()
    for name in _watch_dirs():
        base = root / name
        yield from base.rglob("*.py")
    for name in _watch_files():
        yield root / name


def _max_source_mtime() -> float:
    """max(mtime) over watch dirs + watch files — cheaply detects code/config changes.
    At boot/watcher we need the actual MAX (cached as baseline and polled). For the
    stale check in the hot path we use `_any_source_newer` — it short-circuits on
    the first newer file."""
    latest = 0.0
    for p in _iter_source_paths():
        try:
            latest = max(latest, p.stat().st_mtime)
        except OSError:
            continue
    return latest


def _any_source_newer(threshold: float) -> bool:
    """Early-exit variant of `_max_source_mtime` for the stale check: stop at the
    first file with mtime > threshold. On large repos (thousands of .py) after the
    first edit this runs in O(1) instead of O(N) — every `request_run` against a
    staled daemon drops from tens of ms to single ms. On a fresh daemon (no edits)
    there's no win — we walk everything."""
    for p in _iter_source_paths():
        try:
            if p.stat().st_mtime > threshold:
                return True
        except OSError:
            continue
    return False


# Env vars whose change must invalidate the warm daemon: collection/patch-time
# inputs the forkserver baked at boot, which DON'T touch any source file mtime.
# Flipping any of these from the caller auto-triggers a stale-respawn — no manual
# daemon kill. The explicit keys affect collection/run (marker filter, addopts,
# dump, watch-root). User-app env prefixes are configurable via
# `PYTEST_FAST_ENV_PREFIXES` (comma-separated) — set e.g. `MYAPP_,FEATURE_` so any
# `MYAPP_DB__HOST=...` or `FEATURE_X=...` shift triggers a respawn.
_FINGERPRINT_KEYS = (
    "PYTEST_FAST_MARK",
    "PYTEST_ADDOPTS",
    "OUTCOME_DUMP",
    "PYTEST_FAST_WATCH_DIRS",
    "PYTEST_FAST_WATCH_FILES",
    "PYTEST_FAST_ROOT",
    "PYTEST_FAST_ENV_PREFIXES",  # change in the prefix list itself must respawn too
    "PYTEST_FAST_WORKER_IDENTITY",  # opt-in per-worker xdist identity changes worker setup → respawn
    "PYTEST_FAST_DAEMON_PLUGINS",  # daemon-side plugin list is baked at first use → respawn on change
    "PYTEST_FAST_IMMUTABLE_SOURCES",  # opt-in: caller proves the source tree is immutable during the daemon run
)


def _fingerprint_prefixes() -> tuple[str, ...]:
    """User-configured env-var prefixes that should drive staleness. Parsed from
    `PYTEST_FAST_ENV_PREFIXES` (comma-separated). Empty by default — only the
    explicit `_FINGERPRINT_KEYS` matter unless the caller opts in to app config."""
    raw = os.environ.get("PYTEST_FAST_ENV_PREFIXES", "")
    return tuple(p.strip() for p in raw.split(",") if p.strip())


def _env_fingerprint() -> str:
    """Stable hash of env vars that influence collection/patching. Daemon snapshots
    it at boot; caller sends its current one on run/status → mismatch ⇒ stale-respawn."""
    prefixes = _fingerprint_prefixes()
    items = {k: v for k, v in os.environ.items() if k in _FINGERPRINT_KEYS or any(k.startswith(p) for p in prefixes)}
    blob = "\0".join(f"{k}={items[k]}" for k in sorted(items))
    # `surrogateescape`, NOT a strict `.encode()`: a non-UTF-8 byte env value (common for
    # an app var matched via PYTEST_FAST_ENV_PREFIXES) is decoded into os.environ as
    # surrogate-escaped chars (\udc80–\udcff); strict UTF-8 then raises UnicodeEncodeError
    # — and this runs on EVERY client request, so it would crash the whole run. The
    # surrogateescape handler reverses the decode to the original bytes → stable + total.
    return hashlib.sha1(blob.encode("utf-8", "surrogateescape")).hexdigest()


def _immutable_sources_enabled() -> bool:
    """Whether the caller guarantees that source files stay unchanged for this daemon lifetime."""
    return os.environ.get("PYTEST_FAST_IMMUTABLE_SOURCES", "").strip().lower() in ("1", "true", "yes", "on")


def _stale_reason(
    boot_mtime: float,
    boot_fp: str,
    client_fp: str | None,
    *,
    check_sources: bool = True,
) -> str | None:
    """Why a warm daemon must be discarded, or None if still fresh. Source edits beat
    env changes in the message only; either alone forces a respawn. `client_fp` is
    None for legacy callers that don't send a fingerprint → env check is skipped.

    Uses `_any_source_newer` (early-exit), NOT `_max_source_mtime` — on large repos
    that's O(1) instead of O(N) once the first newer file is found. Immutable campaigns can disable
    this scan when an outer start/end fingerprint proves the stronger whole-run invariant."""
    if check_sources and _any_source_newer(boot_mtime):
        return "sources changed"
    if client_fp is not None and client_fp != boot_fp:
        return "env changed"
    return None


# ── lifecycle helpers: pidfile + control-socket bind + status/shutdown/promote ─


def _pid_path(address: str) -> Path:
    return Path(address + ".pid")


def _write_pid(address: str) -> None:
    """Atomically write the pidfile via write-temp-then-rename. Naive `write_text` =
    open+truncate+write+close: between truncate and write a concurrent `_read_pid` could
    read empty → `int("")` → ValueError → `_daemon_alive` falsely False. POSIX rename
    is atomic — readers see either the old or the new content, never empty."""
    pid_path = _pid_path(address)
    tmp = pid_path.with_suffix(pid_path.suffix + ".tmp")
    tmp.write_text(str(os.getpid()))
    tmp.replace(pid_path)


def _read_pid(address: str) -> int | None:
    try:
        return int(_pid_path(address).read_text().strip())
    except (OSError, ValueError):
        return None


def _remove_pid(address: str) -> None:
    _pid_path(address).unlink(missing_ok=True)


def _bind_ctl(address: str, ttl: float) -> socket.socket:
    """Bind the control unix socket at `address` (unlink+bind+listen) and write the pidfile."""
    Path(address).unlink(missing_ok=True)
    s = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    with _short_unix_path(address) as bind_path:
        s.bind(bind_path)
    # Generous backlog: the accept loop is serial (one control message at a time), so a
    # burst of near-simultaneous probes (e.g. parallel `status` pings) can pile up faster
    # than we accept them. 8 was small enough that a flood got connections refused; 64
    # absorbs realistic bursts without dropping callers.
    s.listen(64)
    s.settimeout(ttl)
    _write_pid(address)
    return s


def _daemon_alive(address: str) -> bool:
    """Is the daemon alive — via pidfile + os.kill(pid,0). Cheap and does NOT block
    during a run (unlike status: a daemon busy with a run is not in accept and won't
    reply in time)."""
    pid = _read_pid(address)
    if pid is None:
        return False
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    return True


def _status(address: str) -> dict[str, object] | None:
    """Ping the daemon: ('status',) → {ready, stale}. None if there's no socket /
    the daemon is busy with a run (settimeout: a busy daemon isn't in the accept
    loop and won't reply in time)."""
    try:
        s = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        s.settimeout(_STATUS_PING_TIMEOUT)
        with _short_unix_path(address) as connect_path:
            s.connect(connect_path)
    except OSError:
        return None
    with s:
        try:
            _send(s, ("status", _env_fingerprint()))  # fp → status accounts for env change, not only mtime
            reply, _ = _recv(s)
        except OSError:
            return None
    return cast("dict[str, object]", reply) if isinstance(reply, dict) else None


def _await_ready(address: str, proc: subprocess.Popen[bytes], timeout: float) -> bool:
    """Wait for the daemon to be ready (ready=True). Early exit if the process DIED
    (broken edit → forkserver-preload/collect crashed at startup): we don't wait the
    whole timeout, return False immediately."""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if proc.poll() is not None:
            return False  # successor crashed (didn't collect) — give up at once
        st = _status(address)
        if st is not None and st.get("ready"):
            return True
        time.sleep(_READY_POLL_INTERVAL)
    return False


def _await_socket_gone(address: str, timeout: float) -> bool:
    """Wait until the daemon's control socket file disappears — that's the signal
    "its `finally` in `serve()` ran and released the address". Used as a replacement
    for the "pid is dead" check: `os.kill(pid, 0)` on a zombie child returns success
    until an explicit `wait()` (which may never happen if the parent doesn't reap),
    whereas the socket file is simply there-or-not, regardless of reap status."""
    deadline = time.monotonic() + timeout
    sock_path = Path(address)
    while time.monotonic() < deadline:
        if not sock_path.exists():
            return True
        time.sleep(_PID_DEAD_POLL_INTERVAL)
    return False


def _shutdown_daemon(address: str) -> None:
    """Ask the daemon to exit cleanly. The message is serialized through its accept
    loop AFTER the current run — an active run is never torn (unlike SIGKILL)."""
    try:
        s = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        with _short_unix_path(address) as connect_path:
            s.connect(connect_path)
    except OSError:
        return
    with s:
        try:
            _send(s, ("shutdown",))
            _recv(s)  # {bye} (or close) → daemon released resources and is exiting
        except OSError:
            pass


def _promote(staging: str, canonical: str) -> bool:
    """Tell the staging daemon to rebind to the canonical address."""
    try:
        s = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        with _short_unix_path(staging) as connect_path:
            s.connect(connect_path)
    except OSError:
        return False
    with s:
        try:
            _send(s, ("promote", canonical))
            reply, _ = _recv(s)
        except OSError:
            return False
    if not isinstance(reply, dict):
        return False
    return bool(cast("dict[str, object]", reply).get("promoted"))


@contextmanager
def _respawn_lock(address: str) -> Iterator[None]:
    """Exclusive flock around (re)spawning the daemon: watcher-promote and the
    client's stale-respawn don't race for the canonical socket (otherwise double-boot
    / orphan daemon)."""
    with Path(address + ".respawn.lock").open("w") as f:
        try:
            fcntl.flock(f, fcntl.LOCK_EX)
            yield
        finally:
            fcntl.flock(f, fcntl.LOCK_UN)


def _daemon_log_path(address: str) -> Path:
    """Daemon log file for `address`: a staging daemon (*.staging) writes into a separate
    log to avoid clobbering canonical's. Single source of truth for _spawn_daemon and
    promote-redirect.

    Derived from `address` (per-worktree socket → per-worktree log): otherwise two daemons
    from different worktrees would write to the same file and interleave lines. We strip
    `.staging`/`.sock` and append `-daemon[.staging].log`."""
    staging = address.endswith(".staging")
    base = address.removesuffix(".staging") if staging else address
    base = base.removesuffix(".sock")
    suffix = ".staging" if staging else ""
    return Path(f"{base}-daemon{suffix}.log")


def _redirect_stdio(path: Path) -> None:
    """Redirect fd 1/2 of the CURRENT process into `path` (append). Needed on promote:
    the daemon was spawned with stdout→staging-log, after rebinding to canonical its
    lifecycle logs should land in canonical's log (otherwise the "current" daemon writes
    into …-daemon.staging.log → debugging confusion). dup2 copies the fd over 1/2;
    sys.stdout/sys.stderr (wrappers around fd 1/2) then automatically write to the new file.

    O_NOFOLLOW: refuse to follow a symlink at the log path. The control socket is connectable
    by any same-user process, so a stray/hostile peer could pre-plant a symlink there to capture
    the daemon's stdio. On any open error we keep the current stdio rather than crash."""
    try:
        fd = os.open(str(path), os.O_WRONLY | os.O_CREAT | os.O_APPEND | os.O_NOFOLLOW, 0o600)
    except OSError:
        return
    try:
        os.dup2(fd, 1)
        os.dup2(fd, 2)
    finally:
        os.close(fd)


def _self_invocation() -> list[str]:
    """Command for re-exec'ing pytest-fast itself in a background process (daemon/watcher).

    We use `python -m pytest_fast` rather than a file path — the package is already on
    sys.path (we got imported, after all); it doesn't depend on how the package is
    installed (editable, wheel, src-layout). `__main__.py` proxies argv into `main()`."""
    return [sys.executable, "-m", "pytest_fast"]


def _subprocess_env() -> dict[str, str]:
    """Env for spawning a fresh pytest-fast subprocess (daemon or watcher).

    We scrub `_PYTEST_FAST_COLLECT`: if the parent is another pytest-fast whose
    `Daemon.__init__` armed the flag, the child's main process would needlessly
    run `_collect()` at the top of `__init__.py`. The child's own `Daemon.__init__`
    will arm the flag again right before booting forkserver, where collect is actually
    needed — env flows into the forkserver through `ctx.Process.start()`."""
    env = os.environ.copy()
    env.pop("_PYTEST_FAST_COLLECT", None)
    return env


def _append_restart_marker(log: Path) -> None:
    """Append a `=== restart at TS ===` separator to the log before a new spawn.
    Append mode (rather than truncate): keep the post-mortem of the previous daemon/
    watcher incarnation. Without this `_spawn_daemon` on every stale-respawn wiped
    out the previous logs (which matters when debugging a flapping daemon)."""
    with log.open("a") as f:
        f.write(f"\n=== restart at {time.strftime('%Y-%m-%d %H:%M:%S')} ===\n")


def _spawn_daemon(workers: int, start_method: str, address: str, ttl: float) -> subprocess.Popen[bytes]:
    """Bring up a resident daemon as a detached process (survives the caller). Returns
    Popen → the caller can detect death early (broken collect). A staging daemon
    (address ends in `.staging`) writes into a separate log to avoid disturbing canonical."""
    log = _daemon_log_path(address)
    cmd = [
        *_self_invocation(),
        "--serve",
        "--address",
        address,
        "--ttl",
        str(ttl),
        "--workers",
        str(workers),
        "--start-method",
        start_method,
    ]
    _append_restart_marker(log)
    with log.open("a") as f:
        proc = subprocess.Popen(cmd, stdout=f, stderr=subprocess.STDOUT, start_new_session=True, env=_subprocess_env())
    print(f"[pytest-fast] starting resident daemon (socket {address}, ttl {int(ttl)}s, log {log})", file=sys.stderr)
    return proc


def _coordinated_spawn(workers: int, start_method: str, address: str, ttl: float) -> None:
    """Spawn the canonical daemon under the respawn lock. If a fresh daemon is already
    up (the watcher pre-warmed it) — do nothing, the client just reconnects."""
    with _respawn_lock(address):
        st = _status(address)
        if st is not None and st.get("ready") and not st.get("stale", True):
            return
        _spawn_daemon(workers, start_method, address, ttl)


def run_via_daemon(
    workers: int,
    start_method: str,
    address: str,
    ttl: float,
    *,
    with_watcher: bool,
    run: Callable[[str], dict[str, object]],
) -> dict[str, object]:
    """Ensure a resident daemon at `address`, then execute `run(address)` against it —
    spawning the daemon if absent and respawning it on a `{stale}` reply, bounded by the
    boot deadline. Returns the final frame from `run` (`{rc, summary}`), or `{rc: 1}` if the
    daemon never came up / kept reporting stale. Shared by the CLI client (`_ensure_and_run`),
    the `--fast` plugin controller (`pytest_runtestloop`) — and PUBLIC (stable for external
    tooling): a wrapper client composes it with `request_run(..., want_results=True)` /
    `request_run_streamed(...)` as the `run` callable and gets the full ensure/stale/respawn
    orchestration for free.

    Stale is detected by the daemon BEFORE it runs anything (it replies `{stale}` and exits),
    so a streaming `run` that gets respawned never double-emits reports."""
    if with_watcher:
        _ensure_watcher(workers, start_method, address, ttl)
    deadline = time.monotonic() + _DAEMON_BOOT_TIMEOUT
    spawned = False
    while True:
        try:
            reply = run(address)
        except (FileNotFoundError, ConnectionRefusedError):
            if not spawned:
                _coordinated_spawn(workers, start_method, address, ttl)
                spawned = True
            if time.monotonic() > deadline:
                print("[pytest-fast] daemon failed to start in time", file=sys.stderr)
                return {"rc": 1}
            time.sleep(_DAEMON_BACKOFF_AFTER_SPAWN)
            continue
        if reply.get("stale"):
            print("[pytest-fast] sources/env changed — restarting daemon (fresh collect)", file=sys.stderr)
            _coordinated_spawn(workers, start_method, address, ttl)
            spawned = True
            if time.monotonic() > deadline:
                # Perpetual staleness (e.g. two callers with different env fingerprints sharing
                # one socket, or a watched file with a future mtime) — give up at the deadline
                # instead of spinning the client forever.
                print("[pytest-fast] daemon kept reporting stale past boot deadline", file=sys.stderr)
                return {"rc": 1}
            time.sleep(_DAEMON_BACKOFF_AFTER_STALE)  # let the old release the socket and the new boot
            continue
        return reply


def _ensure_and_run(
    workers: int,
    start_method: str,
    address: str,
    ttl: float,
    *,
    with_watcher: bool,
    full_report: bool = False,
    detailed: bool = False,
    bench: int = 0,
) -> int:
    """CLI client (front A): connect to the daemon → run → print the daemon-rendered summary."""
    reply = run_via_daemon(
        workers,
        start_method,
        address,
        ttl,
        with_watcher=with_watcher,
        run=lambda addr: request_run(addr, full_report=full_report, detailed=detailed, bench=bench),
    )
    summary = reply.get("summary")
    if summary is not None:
        print(summary)
    rc = reply.get("rc")
    return rc if isinstance(rc, int) else 1


# ── pytest plugin front-end (`pytest -p pytest_fast --fast`) ─────────────────
#
# Run the suite through the resident warm daemon while THIS process stays a real pytest
# session — so native reporting (terminalreporter, --durations, junit, -v/-s, plugins) all
# work: we just republish the daemon's streamed per-phase reports through the controller's
# own `pytest_runtest_logreport` hook (the same mechanism xdist uses). The forkserver daemon
# does the execution (amortized collect + fork-warm workers), so it stays fast across reruns.
#
# The hooks are INERT unless --fast is passed (like xdist with -n), so loading `-p pytest_fast`
# for the OUTCOME_DUMP reference mode is unaffected.


def _default_workers() -> int:
    """Default worker count — the number of PERFORMANCE cores.

    On Apple Silicon (and other big.LITTLE designs) cores split into performance (P) and
    efficiency (E) cores; E-cores run roughly half the throughput. The work-stealing dispatch
    finishes when the SLOWEST worker drains, so a worker scheduled onto an E-core becomes a
    straggler that bounds the whole run — more workers than P-cores doesn't speed things up,
    it just adds stragglers plus memory/scheduler contention. So default to the P-core count
    (macOS: `hw.perflevel0.physicalcpu`). Other platforms fall back to the logical CPU count."""
    if sys.platform == "darwin":
        try:
            out = subprocess.run(
                ["sysctl", "-n", "hw.perflevel0.physicalcpu"],
                capture_output=True,
                text=True,
                timeout=2.0,
                check=False,
            )
            n = int(out.stdout.strip())
            if n > 0:
                return n
        except (OSError, ValueError):
            pass  # not Apple Silicon / sysctl unavailable → fall through
    return os.cpu_count() or 1


def _resolve_workers(cli_value: int | None) -> int:
    """Worker count precedence: explicit CLI/option value → `PYTEST_FAST_WORKERS` env →
    performance-core auto-detect (`_default_workers`).

    The single chokepoint that guarantees a VALID (`>= 1`) count for every caller — the CLI,
    the `--fast` plugin, and external tooling via the public `resolve_workers`. An explicit
    `< 1` (a `--workers`/`--fast-workers` option or a *parseable* `PYTEST_FAST_WORKERS`) is a
    user error and raises `ValueError`: 0 workers means no worker ever runs, so the suite
    exits green having executed nothing — a silent false-pass a test runner must never produce.
    Callers surface the error idiomatically (CLI → `parser.error`, plugin → `pytest.UsageError`).
    An UNPARSEABLE env value (e.g. `garbage`) is treated as unset and falls back to auto-detect,
    which always returns `>= 1` — so this function never returns a value below 1."""
    if cli_value is not None:
        if cli_value < 1:
            msg = f"worker count must be >= 1, got {cli_value}"
            raise ValueError(msg)
        return cli_value
    env = os.environ.get("PYTEST_FAST_WORKERS")
    if env:
        try:
            n = int(env)
        except ValueError:
            n = None  # unparseable → treat as unset, fall through to auto-detect
        if n is not None:
            if n < 1:
                msg = f"PYTEST_FAST_WORKERS must be >= 1, got {n}"
                raise ValueError(msg)
            return n
    return _default_workers()


def resolve_workers(cli_value: int | None = None) -> int:
    """The worker count pytest-fast will use, by the documented precedence: an explicit
    `cli_value` → `PYTEST_FAST_WORKERS` → performance-core auto-detect. Stable public API for
    external tooling that needs to size a per-worker resource pool to match the run (prefer this
    over the private `_resolve_workers`; behavior is identical). Raises `ValueError` on an
    explicit `< 1` value; an unparseable env var falls back to auto-detect. See also the
    `pytest-fast --print-inferred-workers` CLI, which prints exactly `resolve_workers()`."""
    return _resolve_workers(cli_value)


def default_workers() -> int:
    """The auto-detected default worker count — performance cores on Apple Silicon, logical CPUs
    elsewhere — ignoring any `--workers` / `PYTEST_FAST_WORKERS` override. Public; always `>= 1`.
    Use `resolve_workers` instead when overrides should win."""
    return _default_workers()


def _resolve_ttl(cli_value: float | None) -> float:
    """Idle-TTL precedence: explicit CLI/option value → `PYTEST_FAST_TTL` env → 600s."""
    if cli_value is not None:
        return cli_value
    env = os.environ.get("PYTEST_FAST_TTL")
    if env:
        try:
            return float(env)
        except ValueError:
            pass
    return 600.0


def _default_fast_address() -> str:
    """Per-project daemon socket when no address is given: a short, stable name in TMPDIR
    derived from the project root (so two checkouts don't share one daemon)."""
    slug = hashlib.sha1(str(_project_root()).encode()).hexdigest()[:10]
    return f"{tempfile.gettempdir()}/pytest-fast-{slug}.sock"


def _resolve_fast_address(cli_value: str | None) -> str:
    """Daemon address precedence: `--fast-address` option → `PYTEST_FAST_ADDRESS` env →
    per-project default.

    ⚠ Prefer `PYTEST_FAST_ADDRESS` (or `--fast-address=PATH`, with an `=`) over the space form
    `--fast-address PATH`: pytest determines rootdir/inifile from the raw argv BEFORE any plugin
    loads, scanning it for existing paths — so once the daemon's socket file exists, a bare
    `--fast-address /tmp/x.sock` makes pytest root at `/tmp`, silently losing `pythonpath`/ini
    discovery. The `=` form and the env var keep the path out of that positional scan."""
    return cli_value or os.environ.get("PYTEST_FAST_ADDRESS") or _default_fast_address()


_pytest_fast_hookspec = pluggy.HookspecMarker("pytest")


class _PytestFastHookSpecs:
    """pytest-fast's own hookspecs (registered via :func:`pytest_addhooks`)."""

    @_pytest_fast_hookspec
    def pytest_fast_configure_worker(self, config: Config, workerid: str, workercount: int) -> None:
        """Fired once per forked worker — post-fork, before any test runs — when the opt-in
        ``--worker-identity`` feature is active, AFTER the worker's xdist-style identity
        (``PYTEST_XDIST_WORKER`` + ``config.workerinput``) is in place.

        Implement it in a plugin/conftest to (re)initialize per-worker resources the fork can't
        infer — e.g. rebind a SQLAlchemy engine onto a per-worker follower database. ``config``
        carries the populated ``config.workerinput``; ``workerid`` / ``workercount`` mirror
        ``PYTEST_XDIST_WORKER`` / ``PYTEST_XDIST_WORKER_COUNT``."""

    @_pytest_fast_hookspec
    def pytest_fast_worker_shutdown(self, config: Config, workerid: str) -> None:
        """Fired once in every worker after its final fixture teardown and before its successful
        completion frame. Plugins use it to flush process-local artifacts that ``os._exit`` would
        otherwise discard. Raising rejects the run as incomplete."""

    @_pytest_fast_hookspec
    def pytest_fast_annotate_result(self, item: Item, result: RunResult, extra: dict[str, object]) -> None:
        """Fired in the WORKER right after each test's runtest protocol completes — the bus channel
        for custom per-test measurements. ``result`` is the read side (outcome/duration/longrepr,
        already final); ``extra`` is the WRITE side: keys a hookimpl puts there ride the worker→master
        bus as ``result["extra"]`` and surface to daemon plugins (``PYTEST_FAST_DAEMON_PLUGINS``) and
        to clients that requested ``want_results``. Values must be pickle-able plain data.

        The measuring itself belongs in ordinary pytest hooks/fixtures — they already run inside
        pytest-fast workers (the full protocol is used); stash what you measured on the item (e.g.
        ``item.stash``) and harvest it here. HOT PATH: fired once per test — keep impls cheap and
        allocation-light. Exceptions are caught, reported to stderr (→ daemon log) and never fail
        the test."""


def pytest_addhooks(pluginmanager: pluggy.PluginManager) -> None:
    """Register pytest-fast's worker lifecycle and result-annotation hookspecs."""
    pluginmanager.add_hookspecs(_PytestFastHookSpecs)


def pytest_addoption(parser: Parser) -> None:
    group = parser.getgroup("pytest-fast", "resident forkserver accelerator")
    group.addoption(
        "--fast",
        action="store_true",
        default=False,
        help="run the suite via a resident pytest-fast daemon (warm forkserver workers, native reporting)",
    )
    group.addoption(
        "--fast-address",
        default=None,
        help="daemon unix socket (or $PYTEST_FAST_ADDRESS; default: derived from the project root). "
        "Use the '=' form (--fast-address=PATH) or the env var — a bare space-separated path can be "
        "mistaken for the rootdir once the socket exists.",
    )
    group.addoption(
        "--fast-workers",
        type=int,
        default=None,
        help="worker count for --fast (or $PYTEST_FAST_WORKERS; default: performance-core count)",
    )
    group.addoption(
        "--fast-ttl",
        type=float,
        default=None,
        help="daemon idle TTL seconds for --fast (or $PYTEST_FAST_TTL; default 600)",
    )
    group.addoption(
        "--fast-watch",
        action="store_true",
        default=False,
        help="also keep a background watcher pre-warming the daemon on source changes",
    )
    group.addoption(
        "--fast-worker-identity",
        action="store_true",
        default=False,
        help="OPT-IN: give each --fast worker an xdist-compatible identity + fire "
        "pytest_fast_configure_worker (per-worker resource isolation for SQLAlchemy/pytest-django/...). "
        "No extra deps. Equivalent to $PYTEST_FAST_WORKER_IDENTITY=1",
    )


def pytest_runtestloop(session: Session) -> bool | None:
    """When --fast: hand execution to the resident daemon and republish its streamed reports
    through this controller's hooks (native reporting). Returns True (loop handled). Inert
    (returns None → pytest's normal in-process loop) otherwise."""
    config = session.config
    if not config.getoption("fast", default=False):
        return None
    if config.getoption("fast_worker_identity", default=False):
        # Propagate the opt-in to the daemon (env-inherited by its forked workers) before we ensure it.
        os.environ["PYTEST_FAST_WORKER_IDENTITY"] = "1"
    if session.testsfailed and not config.getvalue("continue_on_collection_errors"):
        raise session.Interrupted(f"{session.testsfailed} error(s) during collection")

    address = _resolve_fast_address(cast("str | None", config.getoption("fast_address")))
    try:
        workers = _resolve_workers(cast("int | None", config.getoption("fast_workers")))
    except ValueError as exc:
        # An invalid --fast-workers / PYTEST_FAST_WORKERS must fail this session cleanly, NOT
        # spawn a daemon with 0 workers (which would run nothing and exit green). UsageError is
        # pytest's idiom for a bad invocation — same as the collection-match guard below.
        import pytest

        raise pytest.UsageError(f"--fast: {exc}") from exc
    ttl = _resolve_ttl(cast("float | None", config.getoption("fast_ttl")))
    with_watcher = bool(config.getoption("fast_watch"))

    collected = [item.nodeid for item in session.items]
    collected_set = set(collected)
    seen: set[str] = set()

    def on_report(data: dict[str, object]) -> None:
        rep = config.hook.pytest_report_from_serializable(config=config, data=data)
        seen.add(rep.nodeid)
        # Republish into the controller's real terminalreporter / plugins / pass-fail accounting.
        config.hook.pytest_runtest_logreport(report=rep)

    # Forward THIS session's collected nodeids → the daemon runs exactly that selection (so
    # -k/-m/explicit paths work; the full suite is just "all nodeids").
    reply = run_via_daemon(
        workers,
        "forkserver",
        address,
        ttl,
        with_watcher=with_watcher,
        run=lambda addr: request_run_streamed(addr, on_report, collected),
    )

    # Collection-match guard: every selected test must have been run by the daemon. A `missing`
    # nodeid means the daemon's collection lacks it (drifted/stale despite the fingerprint check)
    # — fail loudly rather than silently under-report.
    missing = collected_set - seen
    if missing:
        import pytest

        raise pytest.UsageError(
            f"--fast: {len(missing)} selected test(s) were not run by the daemon "
            f"(e.g. {sorted(missing)[:3]}) — its collection may differ from this session. "
            "Try again (the daemon will respawn on a source/env change), or run without --fast."
        )

    rc = reply.get("rc")
    if rc not in (0, None) and not session.testsfailed:
        # Daemon flagged the run untrusted (worker crash / result undercount) but no republished
        # report marked a failure — surface it so a green exit can't hide a broken run.
        session.shouldfail = "pytest-fast: daemon reported an untrusted run (see daemon log)"
    return True


# ── source watcher (--watch): pre-warm staging successor, then promote ────────
#
# Optional (--with-watcher on the client spawns it detached). Lives in THIS file —
# spawns itself as `… --watch` (same trick as _spawn_daemon). No extra dependencies:
# poll mtime + staging-promote. Idea: ~2.8s of new-forkserver boot is amortized into
# the idle gap AFTER an edit, so by the time the user re-runs tests the daemon is
# already warm and fresh.

# Watch poll/debounce are env-overridable (`PYTEST_FAST_WATCH_POLL` / `_DEBOUNCE`, seconds): tune
# reactivity vs CPU, and let the test suite drop them to ~0.05 so watcher tests run in ~0.3s instead
# of ~2.7s. Read at module load → a freshly-spawned watcher subprocess picks up the caller's env.
_WATCH_POLL = float(os.environ.get("PYTEST_FAST_WATCH_POLL", "0.5"))  # seconds between max(mtime) polls
_WATCH_DEBOUNCE = float(os.environ.get("PYTEST_FAST_WATCH_DEBOUNCE", "0.7"))  # silence after last edit → one reboot
_WATCH_GONE_GRACE = 3.0  # seconds without the daemon → watcher exits (lifetime tied to daemon ttl)
_STAGING_BOOT_TIMEOUT = 90.0  # upper bound on successor boot (normal ~3s;
# a broken edit is caught immediately via process death in _await_ready, not this timeout)

# Poll intervals inside await-loops (sleep between two condition checks). Smaller =
# faster response + slightly more CPU; larger = more reaction delay. Kept small — a daemon boots
# in well under 100ms, so a 0.2s ready-poll was pure dead time on every spawn (×20+ tests).
_READY_POLL_INTERVAL = 0.02
_PID_DEAD_POLL_INTERVAL = 0.02
_DEBOUNCE_POLL_INTERVAL = 0.05

# Network/IPC timeouts.
_WORKER_ACCEPT_TIMEOUT = 60.0  # seconds for each worker's connect to the master server
_WORKER_JOIN_TIMEOUT = 10.0  # seconds master waits for a worker process to exit after `fin`
_STATUS_PING_TIMEOUT = 2.0  # seconds for a status ping; a daemon busy with a run isn't in accept
_CONTROL_CMD_TIMEOUT = 5.0  # seconds to read a control command before dropping the conn (anti-slowloris)
_PROGRESS_THROTTLE_SEC = 0.1  # 10 frames/s; the final frame is force-flushed anyway

# Daemon-spawn orchestration (only in `_ensure_and_run` / client side).
_DAEMON_BOOT_TIMEOUT = 120.0  # upper bound waiting for the spawned daemon to answer status
_DAEMON_BACKOFF_AFTER_SPAWN = 0.3  # pause between a failed connect and the next attempt
_DAEMON_BACKOFF_AFTER_STALE = 0.5  # pause between a {stale} reply and the connect to the fresh daemon


def _await_stable_mtime() -> float:
    """Block until max(mtime) has been "quiet" for `_WATCH_DEBOUNCE` seconds → return it.
    Protects against rebooting mid-batch when an agent makes N consecutive edits."""
    prev = _max_source_mtime()
    quiet_deadline = time.monotonic() + _WATCH_DEBOUNCE
    while time.monotonic() < quiet_deadline:
        time.sleep(_DEBOUNCE_POLL_INTERVAL)
        cur = _max_source_mtime()
        if cur != prev:
            prev = cur
            quiet_deadline = time.monotonic() + _WATCH_DEBOUNCE
    return prev


def _staging_promote(workers: int, start_method: str, address: str, ttl: float) -> bool:
    """Build the successor on the staging socket, await ready, then softly shut down
    the old one (after its current run) and rebind the successor to canonical. Broken
    edit → successor doesn't collect → return False, leaving the current daemon
    untouched."""
    staging = address + ".staging"
    Path(staging).unlink(missing_ok=True)
    _remove_pid(staging)
    with _respawn_lock(address):
        st = _status(address)
        if st is not None and st.get("ready") and not st.get("stale", True):
            return True  # already fresh (the client raced us) — nothing to pre-warm
        proc = _spawn_daemon(workers, start_method, staging, ttl)
        if not _await_ready(staging, proc, _STAGING_BOOT_TIMEOUT):
            _shutdown_daemon(staging)  # best effort: in case it came up but too late
            Path(staging).unlink(missing_ok=True)
            _remove_pid(staging)
            return False
        _shutdown_daemon(address)  # blocks until the current run finishes — we don't tear it
        if not _await_socket_gone(address, 30.0):
            # Old daemon never released the canonical socket (stuck in a very long run). Abort
            # rather than bind over a live socket; shut the staging successor so it isn't orphaned.
            _log("watcher", "old daemon didn't release canonical socket — aborting promote")
            _shutdown_daemon(staging)
            Path(staging).unlink(missing_ok=True)
            _remove_pid(staging)
            return False
        return _promote(staging, address)  # old's finally released the canonical socket → we can bind


def _spawn_watcher(workers: int, start_method: str, address: str, ttl: float, cwd: str | None = None) -> None:
    """Detached watcher process (self-exec of the same package with --watch).

    `cwd` controls where the watcher (and, via `_staging_promote → _spawn_daemon`,
    the staging daemons it spawns) runs pytest collection. Default `None` = inherit
    from the caller; for external users that's the project root (where they invoked
    `pytest-fast`). pytest-fast's own tests pass `cwd=tmp_project` explicitly, otherwise
    staging-spawn under self-test would collect itself — infinite recursion."""
    # Per-worktree log (derived from address): otherwise watchers from different worktrees
    # would write into the same file.
    log = Path(address.removesuffix(".sock") + "-watcher.log")
    cmd = [
        *_self_invocation(),
        "--watch",
        "--address",
        address,
        "--ttl",
        str(ttl),
        "--workers",
        str(workers),
        "--start-method",
        start_method,
    ]
    _append_restart_marker(log)
    with log.open("a") as f:
        subprocess.Popen(
            cmd,
            stdout=f,
            stderr=subprocess.STDOUT,
            start_new_session=True,
            env=_subprocess_env(),
            cwd=cwd,
        )
    print(f"[pytest-fast] starting source watcher (pre-warm; log {log})", file=sys.stderr)


def _ensure_watcher(workers: int, start_method: str, address: str, ttl: float) -> None:
    """Bring up the watcher if it's not already running (single-instance via watcher
    flock). Spawn is idempotent: a redundant watcher exits on its own when it can't
    take the lock."""
    with Path(address + ".watcher.lock").open("w") as probe:
        try:
            fcntl.flock(probe, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError:
            return  # lock taken → a watcher is already alive
        fcntl.flock(probe, fcntl.LOCK_UN)  # free → release and spawn the real one
    _spawn_watcher(workers, start_method, address, ttl)


def _watch(workers: int, start_method: str, address: str, ttl: float) -> int:
    """Resident watcher: poll mtime → debounce → staging-promote the daemon. Single
    instance via flock. Exits when the daemon is gone via its own idle-ttl
    (watcher is NOT keep-alive)."""
    lock_path = address + ".watcher.lock"
    with Path(lock_path).open("w") as lockf:
        try:
            fcntl.flock(lockf, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError:
            _log("watcher", "another watcher already holds the lock — exiting")
            return 0
        _log(
            "watcher",
            f"up; pre-warming {address} on source change (poll {_WATCH_POLL}s, debounce {_WATCH_DEBOUNCE}s)",
        )
        last_warmed = _max_source_mtime()
        last_attempted = last_warmed
        gone_since: float | None = None
        try:
            while True:
                time.sleep(_WATCH_POLL)
                if not _daemon_alive(address):
                    if gone_since is None:
                        gone_since = time.monotonic()
                    elif time.monotonic() - gone_since > _WATCH_GONE_GRACE:
                        _log("watcher", "daemon gone (idle-ttl) — exiting")
                        return 0
                    continue
                gone_since = None
                mtime = _max_source_mtime()
                if mtime <= last_warmed or mtime == last_attempted:
                    continue  # no new edits (or we already tried exactly this state)
                settled = _await_stable_mtime()
                if settled <= last_warmed:
                    continue  # edits rolled back
                last_attempted = settled
                _log("watcher", "source change settled — pre-warming successor…")
                if _staging_promote(workers, start_method, address, ttl):
                    last_warmed = settled
                    _log("watcher", "promoted fresh warm daemon")
                else:
                    _log("watcher", "successor did not collect (broken edit?) — kept current daemon")
        finally:
            Path(lock_path).unlink(missing_ok=True)


def main(argv: list[str]) -> int:
    parser = argparse.ArgumentParser(description="pytest-fast: resident forkserver test accelerator")
    parser.add_argument(
        "--workers", type=int, default=None, help="worker count (or $PYTEST_FAST_WORKERS; default: performance cores)"
    )
    parser.add_argument(
        "--print-inferred-workers",
        action="store_true",
        help="print the resolved worker count (honoring --workers / $PYTEST_FAST_WORKERS / "
        "performance-core auto-detect) and exit — so external tooling can size a per-worker "
        "pool to match the run without importing pytest-fast internals",
    )
    parser.add_argument("--start-method", choices=["spawn", "forkserver", "fork"], default="forkserver")
    parser.add_argument(
        "--worker-identity",
        action="store_true",
        help="OPT-IN: give each forked worker an xdist-style identity (PYTEST_XDIST_WORKER + "
        "config.workerinput) and fire the pytest_fast_configure_worker hook, so an adapter can isolate "
        "per-worker resources (SQLAlchemy follower DBs, pytest-django, ...). No third-party deps. "
        "Equivalent to $PYTEST_FAST_WORKER_IDENTITY=1",
    )
    parser.add_argument("--address", help="unix socket of the resident daemon (or $PYTEST_FAST_ADDRESS)")
    parser.add_argument(
        "--ttl", type=float, default=None, help="serve/ensure: idle seconds before self-shutdown (or $PYTEST_FAST_TTL)"
    )
    parser.add_argument("--serve", action="store_true", help="be the resident daemon (needs --address)")
    parser.add_argument(
        "--watch", action="store_true", help="(internal) be the resident source watcher (needs --address)"
    )
    parser.add_argument(
        "--with-watcher",
        action="store_true",
        help="ensure a background source watcher pre-warms the daemon on every src/tests change",
    )
    parser.add_argument("--runs", type=int, default=1, help="local single-process mode: number of in-process runs")
    parser.add_argument(
        "--persist-workers",
        action="store_true",
        help="local mode: reuse one warm worker across --runs (session-scoped fixtures set up ONCE, not "
        "per run) — opt-in, trades away cross-run isolation for many-small-runs clients",
    )
    parser.add_argument("--dump", help="local mode: write {nodeid: outcome} JSON (for the outcome-diff harness)")
    parser.add_argument(
        "--full-report",
        action="store_true",
        help="ship full per-phase reports → a real --durations table in the summary (heavier bus)",
    )
    parser.add_argument(
        "--detailed",
        action="store_true",
        help="add the extended parallelism block to the summary (eff%%, CPU vs I/O, lost-time "
        "breakdown, per-worker spread, the wall-bounding test)",
    )
    parser.add_argument(
        "--bench",
        nargs="?",
        const=2,
        default=0,
        type=int,
        metavar="N",
        help="run the suite N times (default 2; the first is dropped as warmup) and print a "
        "deterministic bottleneck report instead of the run summary — shared-setup clusters, slowest "
        "CPU/IO calls, the wall ceiling — what to optimize to go faster. More runs → steadier ranking",
    )
    ns = parser.parse_args(argv)
    if ns.worker_identity:
        # Propagate to the (possibly subprocess) daemon + its forked workers via env, and make it part
        # of the daemon's env fingerprint so toggling it respawns a stale warm daemon.
        os.environ["PYTEST_FAST_WORKER_IDENTITY"] = "1"
    try:
        workers = _resolve_workers(ns.workers)
    except ValueError as exc:
        parser.error(str(exc))  # exits 2 — never proceeds with an invalid count
    if ns.print_inferred_workers:
        print(workers)
        return 0
    ttl = _resolve_ttl(ns.ttl)
    address = ns.address or os.environ.get("PYTEST_FAST_ADDRESS")

    if ns.watch:
        if not address:
            parser.error("--watch requires --address")
        return _watch(workers, ns.start_method, address, ttl)
    if ns.serve:
        if not address:
            parser.error("--serve requires --address")
        return Daemon(num_workers=workers, start_method=ns.start_method, persist_workers=ns.persist_workers).serve(
            address, ttl
        )
    if address:
        return _ensure_and_run(
            workers,
            ns.start_method,
            address,
            ttl,
            with_watcher=ns.with_watcher,
            full_report=ns.full_report,
            detailed=ns.detailed,
            bench=ns.bench,
        )
    return Daemon(num_workers=workers, start_method=ns.start_method, dump_path=ns.dump).run(
        ns.runs, full_report=ns.full_report, detailed=ns.detailed, bench=ns.bench, persist_workers=ns.persist_workers
    )


def main_cli() -> int:
    """Console-script entry: `pytest-fast …` (see `[project.scripts]` in pyproject.toml).
    Thin wrapper over `main()` — Click-style, so the entry point doesn't call `main(argv=None)`."""
    return main(sys.argv[1:])


# ── forkserver-preload trigger (AT THE BOTTOM of the file — see rationale near `_collect`) ──
#
# forkserver does `__import__("pytest_fast")` → loads the WHOLE __init__.py → then
# triggers this block (it's guaranteed to be last). By this point every public/private
# symbol of the package is defined, so when pytest at collect time imports test files
# (and they reach for `from pytest_fast import _env_fingerprint`, `Daemon`,
# `_max_source_mtime`, ...), all those names are already available.
#
# If the trigger were higher up (like in the original single-file PoC under bin/),
# test-file imports would hit a cache hit on the partially-loaded module and silently
# ImportError on every symbol declared below the trigger — pytest swallows those
# ImportErrors during collect and just skips the file entirely.

if __name__ == "pytest_fast" and os.environ.get("_PYTEST_FAST_COLLECT"):
    # forkserver/multiprocessing swallows ImportError from `__import__(preload)` (see
    # `Lib/multiprocessing/forkserver.py:main`). If `_collect()` raises something else,
    # the forkserver keeps going but `collected_config` stays None → workers crash on
    # the assert with a mysterious "config is None". So we catch EVERYTHING here, dump
    # the traceback to stderr (lands in daemon.log), and re-raise — the forkserver then
    # sees that preload failed.
    import traceback as _tb

    try:
        _collect()
    except BaseException:
        print("[pytest-fast] FATAL: _collect() raised in forkserver preload:", file=sys.stderr)
        _tb.print_exc(file=sys.stderr)
        sys.stderr.flush()
        raise


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
