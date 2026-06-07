"""Stresstest findings — failing tests that pin down real bugs (tough mode).

Each test asserts the CORRECT behavior and therefore FAILS against the current
implementation. They are reproductions, one per finding:

  F1  control protocol must tolerate malformed frames from a same-user peer.
      Currently CRASHES the resident daemon on:
        * empty tuple ()            → IndexError `cmd = parts[0]`        (serve())
        * short tuple ('promote',)  → IndexError `str(parts[1])`         (serve())
        * zero-length frame         → `_loads(b'')` raises EOFError      (_recv)
        * corrupt pickle payload    → `_loads(...)` raises UnpicklingErr (_recv)
      (oversized-length-header and non-tuple frames are ALREADY tolerated — they
      are kept as regression guards so a fix doesn't break the working cases.)
      serve() *intends* to tolerate "empty/garbled connect (ping/probe)" — it
      guards `not isinstance(msg, tuple)` — but these cases slip through.

  F2  --workers 0  → SILENT false-green rc=0 even with failing tests (total stays
      0 → the `incomplete = total > 0 and …` integrity check is bypassed).

  F3  resident daemon leaks one fd per run — `_serve_bus` builds a
      `selectors.DefaultSelector()` and never closes it.

  F4  stale-respawn livelock — `_ensure_and_run`'s stale branch has no deadline
      guard, so a daemon stuck reporting stale spins the client forever.
"""

from __future__ import annotations

import contextlib
import os
import socket
import struct
import subprocess
import sys
import time
from pathlib import Path
from typing import TYPE_CHECKING, cast

import pytest

from pytest_fast import (
    _MAX_FRAME_BYTES,
    _await_ready,
    _recv,
    _send,
    _short_unix_path,
    _shutdown_daemon,
    _status,
    categorize,
)

if TYPE_CHECKING:
    from collections.abc import Callable, Iterator

    from _pytest.config import Config
    from _pytest.reports import TestReport

# Import the real cap (don't hard-code it): a stale local copy drifted below the actual
# value, so the "oversized header" case below stopped exceeding the cap and silently
# degraded into a truncated-frame test instead of exercising the size guard.
_MAX_FRAME = _MAX_FRAME_BYTES


# ── helpers ──────────────────────────────────────────────────────────────────


def _spawn_daemon(
    pf_cmd: list[str], *, address: str, cwd: Path, ttl: float = 30.0, workers: int = 2
) -> subprocess.Popen[bytes]:
    log_path = Path(address.removesuffix(".sock") + "-daemon.log")
    env = os.environ.copy()
    env["PYTEST_FAST_ROOT"] = str(cwd)
    cmd = [*pf_cmd, "--serve", "--address", address, "--ttl", str(ttl), "--workers", str(workers)]
    with log_path.open("w") as f:
        return subprocess.Popen(cmd, stdout=f, stderr=subprocess.STDOUT, cwd=cwd, env=env, start_new_session=True)


@pytest.fixture
def ready_daemon(
    tmp_project: Path, tmp_address: str, pf_cmd: list[str], monkeypatch: pytest.MonkeyPatch
) -> Iterator[subprocess.Popen[bytes]]:
    """A booted, ready daemon pinned to tmp_project. monkeypatches PYTEST_FAST_ROOT
    in the test process too, so client-side fingerprints match the daemon's."""
    monkeypatch.setenv("PYTEST_FAST_ROOT", str(tmp_project))
    proc = _spawn_daemon(pf_cmd, address=tmp_address, cwd=tmp_project)
    try:
        if not _await_ready(tmp_address, proc, timeout=30.0):
            log = Path(tmp_address.removesuffix(".sock") + "-daemon.log")
            pytest.fail(f"daemon never became ready. log:\n{log.read_text() if log.exists() else '<none>'}")
        yield proc
    finally:
        if proc.poll() is None:
            _shutdown_daemon(tmp_address)
            try:
                proc.wait(timeout=10.0)
            except subprocess.TimeoutExpired:
                proc.kill()
                proc.wait(timeout=5.0)


def _send_obj(address: str, obj: object) -> None:
    s = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    s.settimeout(3.0)
    with _short_unix_path(address) as p:
        s.connect(p)
    with s:
        _send(s, obj)
        with contextlib.suppress(OSError):
            s.recv(64)


def _send_raw(address: str, payload: bytes) -> None:
    s = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    s.settimeout(3.0)
    with _short_unix_path(address) as p:
        s.connect(p)
    with s:
        s.sendall(payload)
        with contextlib.suppress(OSError):
            s.recv(64)


# ── F1: control protocol must survive malformed frames ───────────────────────

_MALFORMED_FRAMES: list[tuple[str, Callable[[str], None]]] = [
    # Currently CRASH the daemon (these are the bugs):
    ("empty_tuple", lambda a: _send_obj(a, ())),
    ("short_promote_tuple", lambda a: _send_obj(a, ("promote",))),
    ("zero_length_frame", lambda a: _send_raw(a, struct.pack("!I", 0))),
    ("corrupt_pickle_payload", lambda a: _send_raw(a, struct.pack("!I", 2) + b"\xff\xff")),
    # Already tolerated — regression guards that a fix keeps them working:
    ("oversized_length_header", lambda a: _send_raw(a, struct.pack("!I", _MAX_FRAME + 1))),
    ("non_tuple_dict", lambda a: _send_obj(a, {"not": "a control tuple"})),
]


@pytest.mark.parametrize("name,send_bad", _MALFORMED_FRAMES, ids=[n for n, _ in _MALFORMED_FRAMES])
def test_f1_daemon_survives_malformed_control_frame(
    ready_daemon: subprocess.Popen[bytes],
    tmp_address: str,
    name: str,
    send_bad: Callable[[str], None],
) -> None:
    """A malformed frame from any same-user process must be ignored, not fatal.

    Strong detection (no fixed-sleep guess):
      1. control — the daemon answers status BEFORE the bad frame (it's reachable);
      2. send the malformed frame;
      3. poll up to 3s: if the process exits → it crashed (bug); if it answers a
         fresh status ping → it stayed in its accept loop and recovered (correct).
    """
    proc = ready_daemon
    assert _status(tmp_address) is not None, "precondition: daemon must answer status before the bad frame"

    send_bad(tmp_address)

    deadline = time.monotonic() + 3.0
    while time.monotonic() < deadline:
        exit_code = proc.poll()
        if exit_code is not None:
            pytest.fail(
                f"[{name}] daemon CRASHED (exitcode={exit_code}) on a malformed control frame — "
                f"a stray/probe frame from a same-user process must be tolerated, not kill the "
                f"resident daemon"
            )
        st = _status(tmp_address)
        if st is not None and st.get("ready"):
            return  # survived AND still serving — correct behavior
        time.sleep(0.1)

    if proc.poll() is not None:
        pytest.fail(f"[{name}] daemon crashed (exitcode={proc.poll()}) on the malformed frame")
    pytest.fail(f"[{name}] daemon alive but stopped answering status after the malformed frame")


# ── F2: --workers 0 must not silently report success ─────────────────────────


def test_f2_zero_workers_is_not_a_silent_false_green(tmp_path: Path, pf_cmd: list[str]) -> None:
    """A project with a FAILING test, run with --workers 0. No worker ever sends
    'ready', so `total` stays 0; the run-integrity check (`incomplete = total > 0
    and …`) is bypassed and `crashed` is False (no worker procs), so rc=0 with
    n=0/0 — a SILENT GREEN that hides every failing test. A test runner must never
    exit 0 when it could not actually run the suite."""
    (tmp_path / "src").mkdir()
    (tmp_path / "tests").mkdir()
    (tmp_path / "src" / "foo.py").write_text("def f() -> int:\n    return 1\n")
    (tmp_path / "tests" / "test_x.py").write_text(
        "def test_pass() -> None:\n    assert True\n\ndef test_fail() -> None:\n    assert False\n",
    )
    (tmp_path / "pyproject.toml").write_text('[project]\nname = "x"\nversion = "0"\n')

    proc = subprocess.run(
        [*pf_cmd, "--workers", "0", "--runs", "1"],
        cwd=tmp_path,
        capture_output=True,
        text=True,
        check=False,
        timeout=60,
    )
    assert proc.returncode != 0, (
        f"--workers 0 returned rc=0 (false green) — a failing suite reported as passing.\n"
        f"stdout:\n{proc.stdout}\nstderr:\n{proc.stderr}"
    )


# ── F3: resident daemon must close its per-run selector ──────────────────────


# Driver run in a SUBPROCESS (not in-process): the suite is dogfooded through
# pytest-fast's own daemonic workers (`make test-full`), and a daemonic process may
# not spawn multiprocessing children — `Daemon._run_once()` would raise "daemonic
# processes are not allowed to have children". A plain subprocess sidesteps that:
# its main process is non-daemonic, so forkserver workers spawn fine. The driver
# installs a selector close-spy and exits 0 iff every per-run selector was closed.
_F3_DRIVER = """\
import os
import selectors
import sys

import pytest_fast as pf

created = []
closed = []
real_cls = selectors.DefaultSelector


def factory():
    sel = real_cls()
    created.append(sel)
    real_close = sel.close

    def tracked_close():
        closed.append(sel)
        real_close()

    # Per-instance override (pure-Python selector → has __dict__). _serve_bus calls
    # `sel.close()`, and a `with selectors.DefaultSelector() as sel:` fix routes its
    # __exit__ through the same close — so this records the close either way.
    sel.close = tracked_close
    return sel


selectors.DefaultSelector = factory


def main():
    d = pf.Daemon(num_workers=2, start_method="forkserver")
    d._run_once()  # warmup boot (forkserver spin-up)
    d._run_once()  # measured run
    if not created:
        print("no selector created by _serve_bus", file=sys.stderr)
        sys.exit(2)
    leaked = len(created) - len(closed)
    print(f"created={len(created)} closed={len(closed)} leaked={leaked}")
    sys.exit(0 if leaked == 0 else 1)


if __name__ == "__main__":
    main()
"""


def test_f3_serve_bus_closes_its_selector(tmp_project: Path, tmp_path: Path) -> None:
    """`_serve_bus` creates a `selectors.DefaultSelector()` and never closes it. The
    selector participates in a reference cycle (BaseSelector ↔ _SelectorMapping), so
    refcounting won't free it — its kqueue/epoll fd survives until cyclic GC. In a
    resident daemon (which calls gc.freeze() at boot and rarely runs gc) this leaks
    one fd per run → eventually `OSError: too many open files`.

    Precise, non-flaky check: spy on every selector instance and assert each one is
    closed. (An empirically-confirmed fd-count probe showed exactly +1 fd/run, fully
    reclaimed by gc.collect() — i.e. the unclosed cyclic selector.)"""
    driver = tmp_path / "fd_drive.py"
    driver.write_text(_F3_DRIVER)
    env = os.environ.copy()
    env["PYTEST_FAST_ROOT"] = str(tmp_project)
    env.pop("_PYTEST_FAST_COLLECT", None)  # don't double-collect in the driver's own main
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
        f"per-run selector(s) were never closed — _serve_bus must close its selector "
        f"(e.g. `with selectors.DefaultSelector() as sel:` or sel.close() in a finally), "
        f"else a long-lived --serve daemon leaks an fd per run.\n"
        f"stdout:\n{proc.stdout}\nstderr:\n{proc.stderr}"
    )


# ── F4: stale-respawn loop must respect the boot deadline ────────────────────


def test_f4_stale_respawn_loop_has_a_deadline(tmp_path: Path) -> None:
    """`_ensure_and_run` checks `deadline` only in the connect-failure branch. The
    stale branch (`if reply.get("stale")`) loops forever with no deadline check, so
    a daemon stuck reporting stale (two callers with different env fingerprints on
    one socket, or a perpetually-newer source mtime) livelocks the client.

    Run the loop in an ISOLATED subprocess (so a pre-fix livelock can't leak a
    spinning thread into the rest of the suite). `request_run` is forced to always
    reply {stale: True} and respawn is a no-op. A correct `_ensure_and_run` gives up
    at the deadline (rc=1) within the timeout; the buggy one never returns → the
    subprocess times out → this test fails."""
    driver = tmp_path / "drive.py"
    driver.write_text(
        "import sys\n"
        "import pytest_fast as pf\n"
        "pf.request_run = lambda _a: {'stale': True}\n"
        "pf._coordinated_spawn = lambda *a, **k: None\n"
        "pf._DAEMON_BOOT_TIMEOUT = 1.0\n"
        "pf._DAEMON_BACKOFF_AFTER_STALE = 0.02\n"
        "rc = pf._ensure_and_run(2, 'forkserver', '/tmp/pf-nonexistent.sock', 30.0, with_watcher=False)\n"
        "sys.exit(rc)\n"
    )
    try:
        proc = subprocess.run(
            [sys.executable, str(driver)],
            capture_output=True,
            text=True,
            check=False,
            timeout=8.0,
        )
    except subprocess.TimeoutExpired:
        pytest.fail(
            "_ensure_and_run never returned under perpetual staleness — it livelocks because "
            "the stale branch has no `time.monotonic() > deadline` guard"
        )
    assert proc.returncode == 1, (
        f"expected rc=1 (gave up at the boot deadline), got rc={proc.returncode}.\n"
        f"stdout:\n{proc.stdout}\nstderr:\n{proc.stderr}"
    )


# ── F5 (#12): categorize must ignore unknown outcome categories ──────────────


def test_f5_categorize_ignores_unknown_category() -> None:
    """`categorize` seeds best='passed' at priority -1 and looked up unknown categories
    with `.get(cat, 0)` → an unrecognized status (e.g. from an unknown third-party plugin's
    `pytest_report_teststatus` hook) scored 0 > -1 and WON, mis-bucketing a passing test as
    that unknown string. Known categories must drive the outcome; unknown ones must not win."""

    class _Hook:
        def __init__(self, cats: list[str]) -> None:
            self._cats = cats
            self._i = 0

        def pytest_report_teststatus(self, report: object, config: object) -> tuple[str, str, str]:
            cat = self._cats[self._i]
            self._i += 1
            return cat, "", ""

    class _Config:
        def __init__(self, cats: list[str]) -> None:
            self.hook = _Hook(cats)

    # A passing report followed by one the hook tags with an unknown category.
    cfg = cast("Config", _Config(["passed", "totally_unknown_status"]))
    reports = cast("list[TestReport]", [object(), object()])
    assert categorize(cfg, reports) == "passed"


# ── F6 (#13): promote must refuse a foreign-directory address ────────────────


def _send_recv(address: str, obj: object) -> object:
    s = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    s.settimeout(3.0)
    with _short_unix_path(address) as p:
        s.connect(p)
    with s:
        _send(s, obj)
        reply, _ = _recv(s)
    return reply


def test_f6_promote_to_foreign_directory_is_refused(
    ready_daemon: subprocess.Popen[bytes],
    tmp_address: str,
) -> None:
    """A `('promote', new_addr)` from any same-user peer must be refused unless new_addr is
    a sibling of the current address — otherwise an arbitrary path flows into
    `_redirect_stdio`'s log path, letting a stray/hostile peer symlink-redirect the daemon's
    stdio. The daemon must reply {'promoted': False}, stay alive, and keep serving."""
    reply = _send_recv(tmp_address, ("promote", "/etc/pf-evil.sock"))
    assert reply == {"promoted": False}, f"expected promote refusal, got {reply!r}"
    assert ready_daemon.poll() is None, "daemon must stay alive after refusing a foreign promote"
    st = _status(tmp_address)
    assert st is not None and st.get("ready") is True, "daemon must still serve on its original address"
