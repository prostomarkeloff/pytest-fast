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
  * `-p pytest_fast` (as a plugin): when OUTCOME_DUMP is set, writes {nodeid: outcome} —
        a reference dump for outcome-diff comparison against xdist.

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
from contextlib import contextmanager
from pathlib import Path
from typing import TYPE_CHECKING, NotRequired, TypedDict, cast

if TYPE_CHECKING:
    from collections.abc import Iterator
    from multiprocessing.context import DefaultContext

    from _pytest.config import Config
    from _pytest.nodes import Item
    from _pytest.reports import TestReport


# Public API. Everything with a `_` prefix is an implementation detail (NOT covered by
# this package's semver promises). Tests/self-test code uses `_*`-names intentionally,
# but downstream consumers should rely on this list.
__all__ = [
    "Daemon",
    "RunResult",
    "WorkerStats",
    "categorize",
    "main",
    "main_cli",
    "request_run",  # client-side, module-level
]


class RunResult(TypedDict):
    """A single test outcome, shipped over the worker→master bus (pickle-serialized)."""

    nodeid: str
    outcome: str
    duration: float
    longrepr: NotRequired[str]  # failure text — only for failed/error


class WorkerStats(TypedDict):
    """Worker summary emitted at the end of a run (drives the par. metric in summary)."""

    wid: int
    ran: int
    busy: float
    run_wall: float


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
# `_recvn`. Real max traffic: a longrepr failure text can be chunky but not megabytes;
# 64MB is generous headroom for the most pathological tracebacks.
_MAX_FRAME_BYTES = 64 * 1024 * 1024


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


def _loads(data: bytes) -> object:
    """Safe analog of `pickle.loads` — routed through `_SafeUnpickler`."""
    import io

    return _SafeUnpickler(io.BytesIO(data)).load()


def _send(sock: socket.socket, obj: object) -> int:
    data = pickle.dumps(obj, protocol=pickle.HIGHEST_PROTOCOL)
    sock.sendall(struct.pack("!I", len(data)) + data)
    return len(data) + 4


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


# Seconds: after this long inside `_collect()` the watchdog thread prints all-threads
# stack traces to stderr. Goal — diagnosing a "hanging conftest" / `pytest_configure`
# hook that loops forever. Normal collect is sub-second on small repos and a few
# seconds on large ones; 30s is a generous bound that catches real hangs without
# spamming on slow CI.
_COLLECT_WATCHDOG_TIMEOUT = 30.0


def _collect() -> None:
    global collected_config, collected_items
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
        config.hook.pytest_collection(session=session)
        collected_config, collected_items = config, session.items
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


def _run_one_item(item: Item, nextitem: Item | None, collector: _ReportCollector) -> RunResult:
    """Run a test via the FULL pytest protocol (hook, not function): setup/call/
    teardown, capture, rerunfailures, makereport — behavior 1-to-1 with regular pytest."""
    collector.reports.clear()
    item.ihook.pytest_runtest_protocol(item=item, nextitem=nextitem)
    duration = sum(r.duration for r in collector.reports)
    outcome = categorize(item.config, collector.reports)
    result: RunResult = {"nodeid": item.nodeid, "outcome": outcome, "duration": duration}
    if outcome in {"failed", "error"}:
        result["longrepr"] = _failure_text(collector.reports)  # traceback only for reds
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


def _worker_main(wid: int, sock_path: str) -> None:
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
    collect_wall = time.perf_counter() - t_start

    sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    sock.connect(sock_path)
    _send(sock, ("ready", wid, len(items), collect_wall))

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
    busy = 0.0
    ran = 0
    prev: Item | None = None
    pending: RunResult | None = None
    while True:
        _send(sock, ("req", wid, pending))
        reply, _ = _recv(sock)
        # Master gone / malformed reply → break out and exit cleanly (os._exit below). The
        # master sees EOF and the run is flagged untrusted via the result undercount.
        if not isinstance(reply, tuple) or len(reply) < 2:
            break
        idx_msg = cast("tuple[object, object]", reply)  # master → ('idx', pick)
        idx = idx_msg[1]
        cur = items[idx] if isinstance(idx, int) and 0 <= idx < len(items) else None
        if prev is not None:
            t0 = time.perf_counter()
            if faulthandler_mod is not None:
                faulthandler_mod.dump_traceback_later(hang_timeout, repeat=True, exit=False)
            try:
                pending = _run_one_item(prev, cur, collector)
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
            ran += 1
        else:
            pending = None
        if cur is None:
            stats: WorkerStats = {"wid": wid, "ran": ran, "busy": busy, "run_wall": time.perf_counter() - run_start}
            _send(sock, ("fin", wid, pending, stats))
            break
        prev = cur
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
    def __init__(self, num_workers: int, start_method: str, dump_path: str | None = None) -> None:
        super().__init__()
        assert num_workers >= 1, "num_workers must be >= 1"
        self.num_workers = num_workers
        self.start_method = start_method
        self.dump_path = dump_path
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

    def run(self, runs: int) -> int:
        """Local mode: single-shot (runs=1) or N runs in one process."""
        rc = 0
        for _ in range(runs):
            rc, summary = self._run_once()
            print(summary)
        return rc

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
          * ('status', fp)         → {'ready': True, 'stale': bool} (cheap, for watcher/client);
          * ('shutdown',)          → {'bye': True} and exit (watcher shuts the old one AFTER its run);
          * ('promote', new_addr)  → rebind to new_addr (staging→canonical on promote).
        """
        boot_mtime = _max_source_mtime()  # baseline BEFORE boot: an edit mid-build → stale
        boot_fp = _env_fingerprint()  # env snapshot at boot: change to relevant env → stale-respawn
        _log("daemon", f"booting — collect once ({self.start_method}, {self.num_workers}w)…")
        t0 = time.perf_counter()
        self._arm_collect_flag()  # arm the env flag right before the first Process.start()
        boot = self.ctx.Process(target=_noop)
        boot.start()
        boot.join()  # forks the forkserver → it imports preload (collect) → warm
        _log("daemon", f"ready in {time.perf_counter() - t0:.2f}s, listening {address}, ttl={ttl}s")

        cur = address  # current listening address — may change via ('promote', …)
        ctl = _bind_ctl(cur, ttl)
        try:
            while True:
                try:
                    conn, _addr = ctl.accept()
                except TimeoutError:
                    _log("daemon", f"idle > {ttl}s — shutting down")
                    return 0
                with conn:
                    msg, _ = _recv(conn)
                    if not isinstance(msg, tuple) or not msg:
                        continue  # empty/garbled connect (ping/probe) or empty tuple
                    parts = cast("tuple[object, ...]", msg)  # control: (cmd, *args)
                    cmd = parts[0]
                    # Slice for fp, NOT `parts[1] if len(parts) > 1`: the len() guard makes
                    # pyright narrow tuple arity and breaks `parts[1]` in the promote branch.
                    fp_args = parts[1:]
                    client_fp = str(fp_args[0]) if fp_args else None  # caller env fingerprint
                    if cmd == "status":
                        _send(conn, {"ready": True, "stale": _stale_reason(boot_mtime, boot_fp, client_fp) is not None})
                        continue
                    if cmd == "shutdown":
                        _send(conn, {"bye": True})
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
                            _send(conn, {"promoted": False})
                            _log("daemon", f"refused promote to unexpected address {new_addr!r}")
                            continue
                        ctl.close()
                        _remove_pid(cur)
                        Path(cur).unlink(missing_ok=True)
                        cur = new_addr
                        ctl = _bind_ctl(cur, ttl)
                        _redirect_stdio(_daemon_log_path(cur))  # lifecycle logs → log of the new address
                        _send(conn, {"promoted": True})
                        _log("daemon", f"promoted → listening {cur}")
                        continue
                    # default — this is a run request
                    reason = _stale_reason(boot_mtime, boot_fp, client_fp)
                    if reason is not None:
                        _log("daemon", f"{reason} — exiting stale")
                        _send(conn, {"stale": True})
                        return 0  # finally releases the socket → client spawns a fresh daemon
                    # progress_conn=conn: workers write dots into the DAEMON log, not the client's
                    # terminal — so we stream progress over this same socket (otherwise the client
                    # sits silent the whole run and looks frozen).
                    rc, summary = self._run_once(progress_conn=conn)
                    try:
                        _send(conn, {"rc": rc, "summary": summary})
                    except OSError:
                        # client gone (Ctrl-C) before the final frame — the run is already done,
                        # the daemon does NOT crash (used to crash on BrokenPipe here), stays warm
                        _log("daemon", "client gone before summary; run completed, staying warm")
        finally:
            ctl.close()
            _remove_pid(cur)
            Path(cur).unlink(missing_ok=True)

    # ── one run (fork workers + work-stealing dispatch) ──────────────────────

    def _run_once(self, progress_conn: socket.socket | None = None) -> tuple[int, str]:
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

        procs = [
            self.ctx.Process(target=_worker_main, args=(wid, sock_path), daemon=True) for wid in range(self.num_workers)
        ]
        self._arm_collect_flag()  # local-run mode (serve() also calls; repeated invocation is idempotent)
        for p in procs:
            p.start()

        try:
            results, worker_stats, bus, t_ready, total = self._serve_bus(server, progress_conn)
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

        label = "BOOT (collect once)" if idx == 0 else f"run #{idx} (warm)"
        summary = self._report(
            results, worker_stats, bus, total, warmup=t_ready - t0, run=t_done - t_ready, label=label
        )
        rc = 1 if any(r["outcome"] in {"failed", "error"} for r in results) else 0
        # Run integrity: a worker may have died BEFORE sending results (import/assert in
        # `_worker_main`) — then `results` are empty/partial and rc would be 0 (a false
        # green, possibly n=0/0). Any non-zero worker exitcode OR a result undercount
        # (< collected total) → run is NOT trusted, force rc=1 (codex P1).
        exitcodes = [p.exitcode for p in procs]
        crashed = any(code not in (0, None) for code in exitcodes)
        incomplete = total > 0 and len(results) < total
        if crashed or incomplete:
            rc = 1
            summary += (
                f"\n  ⚠ UNTRUSTED RUN — worker crashed / result undercount: "
                f"results={len(results)}/{total}, worker exitcodes={exitcodes} (see daemon log)"
            )
        return rc, summary

    def _serve_bus(
        self, server: socket.socket, progress_conn: socket.socket | None = None
    ) -> tuple[list[RunResult], list[WorkerStats], dict[str, float], float, int]:
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
                if progress_conn is None or total is None:
                    return
                now = time.perf_counter()
                done = len(results)
                if not force and done < total and now - last_emit < _PROGRESS_THROTTLE_SEC:
                    return  # throttled to _PROGRESS_THROTTLE_SEC; the final frame (done==total) is always sent
                last_emit = now
                try:
                    _send(progress_conn, {"progress": (done, total)})
                except OSError:
                    progress_conn = None  # client gone (Ctrl-C) — stop sending, but complete the run

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
                        ready_seen += 1
                        if ready_seen == expected:
                            t_ready = time.perf_counter()
                    elif kind == "req":
                        result = cast("RunResult | None", parts[2])
                        if result is not None:
                            results.append(result)
                            emit_progress()
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
                        worker_stats.append(cast("WorkerStats", parts[3]))
                        sel.unregister(conn)
                        conn.close()
                        active -= 1

            emit_progress(force=True)  # final frame (done==total) — guaranteed
            bus = {"tx": float(tx), "rx": float(rx), "req_count": float(req_count)}
            return results, worker_stats, bus, t_ready, total or 0
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
    ) -> str:
        from collections import Counter

        counts = Counter(r["outcome"] for r in results)
        failed = counts["failed"] + counts["error"]
        sum_busy = sum(s["busy"] for s in worker_stats)
        run_walls = [s["run_wall"] for s in worker_stats]
        breakdown = ", ".join(f"{n} {cat}" for cat, n in sorted(counts.items()))
        line = "═" * 66
        out = [
            f"\n{line}",
            f"  {self.start_method.upper()} DAEMON  —  {self.num_workers}w  —  {label}",
            line,
            f"  results : {breakdown}  (n={len(results)}/{total})",
            f"  warmup  : {warmup:6.2f}s   (fork+spawn; ~0 for resident rerun)",
            f"  RUN     : {run:6.2f}s   ← wall",
            f"  par.    : {(sum_busy / run if run else 0):.2f}x of {self.num_workers}"
            f"   (run-wall max={max(run_walls) if run_walls else 0:.2f} min={min(run_walls) if run_walls else 0:.2f})",
            f"  bus     : {int(bus['req_count'])} round-trips, {bus['rx'] / 1024:.0f}KB rx",
        ]
        if failed:
            out.append(f"  FAILURES ({failed}):")
            for r in results:
                if r["outcome"] not in {"failed", "error"}:
                    continue
                out.append(f"    ✗ {r['nodeid']}")
                longrepr = r.get("longrepr")
                if isinstance(longrepr, str) and longrepr.strip():
                    out.extend(f"      {ln}" for ln in longrepr.splitlines())
        xpassed = [r for r in results if r["outcome"] == "xpassed"]
        if xpassed:
            out.append(f"  XPASS ({len(xpassed)}) — stale xfail entries (now pass, drop them):")
            out.extend(f"    ? {r['nodeid']}" for r in xpassed)
        slow = sorted(results, key=lambda r: r["duration"], reverse=True)
        slow = [r for r in slow if r["duration"] >= 1.0][:10]
        if slow:
            out.append(f"  SLOWEST (≥1s, top {len(slow)}):")
            out.extend(f"    {r['duration']:7.2f}s  {r['nodeid']}" for r in slow)
        out.append(line)
        return "\n".join(out)


# ── client-side: request a run from the resident daemon ──────────────────────


def request_run(address: str) -> dict[str, object]:
    """Trigger a run on the daemon; stream progress to stdout, return the final frame
    (`{rc, summary}` or `{stale: True}`). The daemon sends N `{'progress': (done,total)}`
    frames then one final frame — we recv in a loop until a non-progress frame arrives.

    Module-level (not a method of `Daemon`) — this is the **client**, not a server
    method; keeping it on `Daemon` would mix both protocol sides into one class."""
    sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    with _short_unix_path(address) as connect_path:
        sock.connect(connect_path)
    with sock:
        _send(sock, ("run", _env_fingerprint()))  # fp → daemon stale-exits on env change
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
            return frame


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
    return hashlib.sha1(blob.encode()).hexdigest()


def _stale_reason(boot_mtime: float, boot_fp: str, client_fp: str | None) -> str | None:
    """Why a warm daemon must be discarded, or None if still fresh. Source edits beat
    env changes in the message only; either alone forces a respawn. `client_fp` is
    None for legacy callers that don't send a fingerprint → env check is skipped.

    Uses `_any_source_newer` (early-exit), NOT `_max_source_mtime` — on large repos
    that's O(1) instead of O(N) once the first newer file is found."""
    if _any_source_newer(boot_mtime):
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
    s.listen(8)
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


def _ensure_and_run(workers: int, start_method: str, address: str, ttl: float, *, with_watcher: bool) -> int:
    """Connect to the daemon at `address` → run. Spawns the daemon if absent and
    restarts it if the daemon reports stale code. With `with_watcher` — also
    guarantees a background watcher (pre-warm on source changes)."""
    if with_watcher:
        _ensure_watcher(workers, start_method, address, ttl)
    deadline = time.monotonic() + _DAEMON_BOOT_TIMEOUT
    spawned = False
    while True:
        try:
            reply = request_run(address)
        except (FileNotFoundError, ConnectionRefusedError):
            if not spawned:
                _coordinated_spawn(workers, start_method, address, ttl)
                spawned = True
            if time.monotonic() > deadline:
                print("[pytest-fast] daemon failed to start in time", file=sys.stderr)
                return 1
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
                return 1
            time.sleep(_DAEMON_BACKOFF_AFTER_STALE)  # let the old release the socket and the new boot
            continue
        summary = reply.get("summary")
        if summary is not None:
            print(summary)
        rc = reply.get("rc")
        return rc if isinstance(rc, int) else 1


# ── source watcher (--watch): pre-warm staging successor, then promote ────────
#
# Optional (--with-watcher on the client spawns it detached). Lives in THIS file —
# spawns itself as `… --watch` (same trick as _spawn_daemon). No extra dependencies:
# poll mtime + staging-promote. Idea: ~2.8s of new-forkserver boot is amortized into
# the idle gap AFTER an edit, so by the time the user re-runs tests the daemon is
# already warm and fresh.

_WATCH_POLL = 0.5  # seconds between max(mtime) polls
_WATCH_DEBOUNCE = 0.7  # seconds of silence after the last edit → one reboot per batch of Edits
_WATCH_GONE_GRACE = 3.0  # seconds without the daemon → watcher exits (lifetime tied to daemon ttl)
_STAGING_BOOT_TIMEOUT = 90.0  # upper bound on successor boot (normal ~3s;
# a broken edit is caught immediately via process death in _await_ready, not this timeout)

# Poll intervals inside await-loops (sleep between two condition checks). Smaller =
# faster response + slightly more CPU; larger = more reaction delay. These differ on
# purpose: ready-status is more expensive than a PID probe, hence rarer; PID probe is
# cheap → faster.
_READY_POLL_INTERVAL = 0.2
_PID_DEAD_POLL_INTERVAL = 0.05
_DEBOUNCE_POLL_INTERVAL = 0.1

# Network/IPC timeouts.
_WORKER_ACCEPT_TIMEOUT = 60.0  # seconds for each worker's connect to the master server
_WORKER_JOIN_TIMEOUT = 10.0  # seconds master waits for a worker process to exit after `fin`
_STATUS_PING_TIMEOUT = 2.0  # seconds for a status ping; a daemon busy with a run isn't in accept
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
    parser.add_argument("--workers", type=int, default=6)
    parser.add_argument("--start-method", choices=["spawn", "forkserver", "fork"], default="forkserver")
    parser.add_argument("--address", help="unix socket of the resident daemon (caller hardcodes this)")
    parser.add_argument("--ttl", type=float, default=600.0, help="serve/ensure: idle seconds before self-shutdown")
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
    parser.add_argument("--dump", help="local mode: write {nodeid: outcome} JSON (for the outcome-diff harness)")
    ns = parser.parse_args(argv)
    if ns.workers < 1:
        parser.error("--workers must be >= 1")

    if ns.watch:
        if not ns.address:
            parser.error("--watch requires --address")
        return _watch(ns.workers, ns.start_method, ns.address, ns.ttl)
    if ns.serve:
        if not ns.address:
            parser.error("--serve requires --address")
        return Daemon(num_workers=ns.workers, start_method=ns.start_method).serve(ns.address, ns.ttl)
    if ns.address:
        return _ensure_and_run(ns.workers, ns.start_method, ns.address, ns.ttl, with_watcher=ns.with_watcher)
    return Daemon(num_workers=ns.workers, start_method=ns.start_method, dump_path=ns.dump).run(ns.runs)


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
