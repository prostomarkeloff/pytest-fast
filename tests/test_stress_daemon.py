"""Stress the LIVE resident daemon — heavy, opt-in (`-m stress`), spawns real processes.

Where `test_stresstest_findings.py` pins a fixed set of malformed frames, this fuzzes the
running daemon with Hypothesis-generated garbage and piles on resource pressure:

  * wire-byte / control-tuple STORMS — hundreds of generated frames at one daemon; it
    must never die and must keep serving (generalizes F1 from 6 cases to the whole space).
  * slowloris — a peer that connects and sends nothing must not wedge the serial accept
    loop (a same-user DoS).
  * connection flood — many concurrent control requests, all served, daemon stays up.
  * worker crash — a test that `os._exit`s mid-run yields an UNTRUSTED (rc!=0) run, never
    a silent green, and the engine survives.
  * fd hygiene at scale — many sequential runs must not leak file descriptors.
"""

from __future__ import annotations

import contextlib
import os
import socket
import struct
import subprocess
import sys
import threading
import time
from pathlib import Path
from typing import TYPE_CHECKING

import pytest
from hypothesis import given, settings
from hypothesis import strategies as st

from pytest_fast import (
    _await_ready,
    _send,
    _short_unix_path,
    _shutdown_daemon,
    _status,
    request_run,
)

if TYPE_CHECKING:
    from collections.abc import Iterator

pytestmark = pytest.mark.stress

_VERBS = {"run", "status", "shutdown", "promote"}


# ── helpers ──────────────────────────────────────────────────────────────────


def _build_project(root: Path) -> None:
    """A minimal collectable project: 4 trivial tests under src/ + tests/."""
    (root / "src").mkdir(exist_ok=True)
    (root / "tests").mkdir(exist_ok=True)
    (root / "src" / "foo.py").write_text("def f() -> int:\n    return 1\n")
    (root / "tests" / "test_t.py").write_text(
        "def test_a() -> None:\n    assert True\n"
        "def test_b() -> None:\n    assert True\n"
        "def test_c() -> None:\n    assert True\n"
        "def test_d() -> None:\n    assert True\n"
    )
    (root / "pyproject.toml").write_text('[project]\nname = "tmp"\nversion = "0"\n')


def _spawn_daemon(
    pf_cmd: list[str], *, address: str, cwd: Path, ttl: float = 60.0, workers: int = 2
) -> subprocess.Popen[bytes]:
    log_path = Path(address.removesuffix(".sock") + "-daemon.log")
    env = os.environ.copy()
    env["PYTEST_FAST_ROOT"] = str(cwd)
    env.pop("_PYTEST_FAST_COLLECT", None)
    cmd = [*pf_cmd, "--serve", "--address", address, "--ttl", str(ttl), "--workers", str(workers)]
    with log_path.open("w") as f:
        return subprocess.Popen(cmd, stdout=f, stderr=subprocess.STDOUT, cwd=cwd, env=env, start_new_session=True)


def _send_raw(address: str, payload: bytes) -> None:
    """Connect, dump raw bytes, half-close, read whatever comes back. The `SHUT_WR`
    makes the daemon see EOF promptly when our bytes are an incomplete frame (header
    promises more than we sent) — otherwise its read would block until the command
    timeout. We still read so we observe the daemon's reply/close."""
    s = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    s.settimeout(3.0)
    with _short_unix_path(address) as p:
        s.connect(p)
    with s, contextlib.suppress(OSError):
        s.sendall(payload)
        s.shutdown(socket.SHUT_WR)
        s.recv(64)


def _send_obj(address: str, obj: object) -> None:
    s = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    s.settimeout(3.0)
    with _short_unix_path(address) as p:
        s.connect(p)
    with s, contextlib.suppress(OSError):
        _send(s, obj)
        s.recv(64)


def _alive_and_serving(proc: subprocess.Popen[bytes], address: str) -> bool:
    return proc.poll() is None and (_status(address) or {}).get("ready") is True


# ── fixtures ─────────────────────────────────────────────────────────────────


@pytest.fixture(scope="module")
def storm_daemon(tmp_path_factory: pytest.TempPathFactory) -> Iterator[tuple[str, subprocess.Popen[bytes]]]:
    """One booted daemon shared across all the Hypothesis examples of the storm tests
    (module-scoped → Hypothesis won't re-fork it per example)."""
    proj = tmp_path_factory.mktemp("storm_proj")
    _build_project(proj)
    address = str(tmp_path_factory.mktemp("storm_sock") / "pf.sock")
    pf_cmd = [sys.executable, "-m", "pytest_fast"]
    proc = _spawn_daemon(pf_cmd, address=address, cwd=proj)
    try:
        if not _await_ready(address, proc, timeout=60.0):
            log = Path(address.removesuffix(".sock") + "-daemon.log")
            pytest.fail(f"storm daemon never became ready. log:\n{log.read_text() if log.exists() else '<none>'}")
        yield address, proc
    finally:
        if proc.poll() is None:
            _shutdown_daemon(address)
            with contextlib.suppress(subprocess.TimeoutExpired):
                proc.wait(timeout=10.0)
            if proc.poll() is None:
                proc.kill()


@pytest.fixture
def fresh_daemon(
    tmp_project: Path, tmp_address: str, pf_cmd: list[str], monkeypatch: pytest.MonkeyPatch
) -> Iterator[tuple[str, subprocess.Popen[bytes]]]:
    """A per-test daemon on `tmp_project` (1 pass + 1 fail). Monkeypatches PYTEST_FAST_ROOT
    in this process too, so client fingerprints match the daemon's (runs aren't 'stale')."""
    monkeypatch.setenv("PYTEST_FAST_ROOT", str(tmp_project))
    proc = _spawn_daemon(pf_cmd, address=tmp_address, cwd=tmp_project)
    try:
        if not _await_ready(tmp_address, proc, timeout=30.0):
            log = Path(tmp_address.removesuffix(".sock") + "-daemon.log")
            pytest.fail(f"daemon never became ready. log:\n{log.read_text() if log.exists() else '<none>'}")
        yield tmp_address, proc
    finally:
        if proc.poll() is None:
            _shutdown_daemon(tmp_address)
            with contextlib.suppress(subprocess.TimeoutExpired):
                proc.wait(timeout=10.0)
            if proc.poll() is None:
                proc.kill()


# ── wire-byte storm: arbitrary bytes never kill the daemon ───────────────────────


@given(blob=st.binary(max_size=512))
@settings(max_examples=200)
def test_daemon_survives_arbitrary_wire_byte_storm(
    storm_daemon: tuple[str, subprocess.Popen[bytes]], blob: bytes
) -> None:
    address, proc = storm_daemon
    _send_raw(address, blob)
    assert _alive_and_serving(proc, address), f"daemon died/wedged on raw frame {blob!r}"


# Frame-correct but arbitrary length headers + payloads (exercises the recv path with a
# valid 4-byte header followed by hostile content).
@given(length=st.integers(min_value=0, max_value=2**32 - 1), payload=st.binary(max_size=512))
@settings(max_examples=150)
def test_daemon_survives_arbitrary_framed_storm(
    storm_daemon: tuple[str, subprocess.Popen[bytes]], length: int, payload: bytes
) -> None:
    address, proc = storm_daemon
    _send_raw(address, struct.pack("!I", length) + payload)
    assert _alive_and_serving(proc, address), f"daemon died/wedged on framed garbage (len={length})"


# Well-framed arbitrary control tuples (never a real verb, so we don't accidentally
# trigger a run/shutdown on the shared daemon) — exercises serve()'s command dispatch.
_control_tuple = (
    st.lists(st.text() | st.integers() | st.none() | st.binary(), max_size=5)
    .map(tuple)
    .filter(lambda t: not (t and t[0] in _VERBS))
)


@given(msg=_control_tuple)
@settings(max_examples=150)
def test_daemon_survives_arbitrary_control_tuple_storm(
    storm_daemon: tuple[str, subprocess.Popen[bytes]], msg: tuple[object, ...]
) -> None:
    address, proc = storm_daemon
    _send_obj(address, msg)
    assert _alive_and_serving(proc, address), f"daemon died/wedged on control tuple {msg!r}"


# ── slowloris: a stalled connection must not wedge the accept loop ───────────────


def test_daemon_survives_slowloris_connect(fresh_daemon: tuple[str, subprocess.Popen[bytes]]) -> None:
    """A same-user peer connects and sends NOTHING. The daemon's accept loop reads the
    command with a blocking recv — if that read has no timeout, this one idle socket
    wedges the daemon forever and every legitimate run hangs. The daemon must instead
    time the read out, drop the connection, and keep serving."""
    address, proc = fresh_daemon
    stuck = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    with _short_unix_path(address) as p:
        stuck.connect(p)
    try:
        # Don't send anything on `stuck`. Give the daemon a moment to accept it and block.
        time.sleep(0.5)
        deadline = time.monotonic() + 12.0
        while time.monotonic() < deadline:
            if proc.poll() is not None:
                pytest.fail(f"daemon exited (rc={proc.poll()}) while a peer held an idle connection")
            st = _status(address)
            if st is not None and st.get("ready"):
                return  # recovered — it reaped the idle connection and kept serving
            time.sleep(0.25)
        pytest.fail("daemon stopped answering — a single idle (slowloris) connection wedged the accept loop")
    finally:
        stuck.close()


# ── connection flood: many concurrent control requests, all served ───────────────


def test_daemon_survives_connection_flood(fresh_daemon: tuple[str, subprocess.Popen[bytes]]) -> None:
    """50 concurrent status probes. The serial accept loop must drain them all and the
    daemon must stay up — no crash, no dropped-into-unrecoverable-state."""
    address, proc = fresh_daemon
    n = 50
    results: list[bool] = []
    lock = threading.Lock()

    def probe() -> None:
        st = _status(address)
        ok = bool(st and st.get("ready"))
        with lock:
            results.append(ok)

    threads = [threading.Thread(target=probe) for _ in range(n)]
    for t in threads:
        t.start()
    for t in threads:
        t.join(timeout=30.0)

    assert proc.poll() is None, "daemon died under a connection flood"
    # The serial accept loop + listen backlog mean a few of a 50-wide burst may be refused
    # under load; the contract is "survives and keeps serving", so allow some slack but
    # require the bulk to be served.
    assert sum(results) >= int(n * 0.8), f"only {sum(results)}/{n} probes were served under flood"
    assert _alive_and_serving(proc, address), "daemon not serving after the flood"


# ── worker crash: a hard exit mid-test → UNTRUSTED run, never a silent green ──────


def test_worker_hard_exit_is_an_untrusted_run(tmp_path: Path, pf_cmd: list[str]) -> None:
    """One test calls `os._exit(1)` — the worker vanishes mid-protocol without reporting
    that test. The result undercount + nonzero worker exitcode must force rc!=0 and an
    'UNTRUSTED RUN' notice, and the engine must not hang or false-green."""
    (tmp_path / "src").mkdir()
    (tmp_path / "tests").mkdir()
    (tmp_path / "src" / "foo.py").write_text("def f() -> int:\n    return 1\n")
    (tmp_path / "tests" / "test_x.py").write_text(
        "import os\n"
        "def test_a() -> None:\n    assert True\n"
        "def test_crash() -> None:\n    os._exit(1)\n"
        "def test_b() -> None:\n    assert True\n"
        "def test_c() -> None:\n    assert True\n"
    )
    (tmp_path / "pyproject.toml").write_text('[project]\nname = "x"\nversion = "0"\n')

    proc = subprocess.run(
        [*pf_cmd, "--workers", "2", "--runs", "1"],
        cwd=tmp_path,
        capture_output=True,
        text=True,
        check=False,
        timeout=120,
    )
    assert proc.returncode != 0, f"worker hard-exit reported as green!\nstdout:\n{proc.stdout}\nstderr:\n{proc.stderr}"
    assert "UNTRUSTED" in proc.stdout, f"missing UNTRUSTED notice.\nstdout:\n{proc.stdout}\nstderr:\n{proc.stderr}"


# ── fd hygiene: many sequential runs must not leak descriptors ───────────────────


def _fd_count(pid: int) -> int | None:
    """Open-fd count for `pid` on Linux (`/proc/<pid>/fd`); None where unavailable."""
    fd_dir = Path(f"/proc/{pid}/fd")
    if not fd_dir.exists():
        return None
    try:
        return len(list(fd_dir.iterdir()))
    except OSError:
        return None


def test_many_sequential_runs_do_not_leak_fds(fresh_daemon: tuple[str, subprocess.Popen[bytes]]) -> None:
    """Drive 25 runs at one resident daemon. Each run builds a `selectors.DefaultSelector`
    and a per-run socket; if any leak, the daemon's fd count climbs run-over-run (and
    eventually hits EMFILE). Assert it's flat after warmup, and every run is consistent."""
    address, proc = fresh_daemon
    rcs: list[int] = []
    for _ in range(3):  # warmup — let one-time fds (forkserver, logs) settle
        request_run(address)
    baseline = _fd_count(proc.pid)
    for _ in range(25):
        reply = request_run(address)
        rc = reply.get("rc")
        assert isinstance(rc, int), f"run produced no rc: {reply!r}"
        rcs.append(rc)
    assert proc.poll() is None, "daemon died across sequential runs"
    assert len(set(rcs)) == 1, f"inconsistent rc across identical runs: {sorted(set(rcs))}"
    if baseline is not None:
        after = _fd_count(proc.pid)
        assert after is not None
        assert after <= baseline + 3, f"fd leak: {baseline} → {after} over 25 runs"


# ── decode-amplification: a memo DAG must not explode the plain-builtins walk ─────


def test_decode_dag_does_not_explode(tmp_path: Path) -> None:
    """A pickle memo 'billion laughs' DAG (`m = [m, m]` × N): a few hundred bytes and a
    few dozen objects in memory (shared refs), but a naive walk visits 2**N paths. `_loads`'
    plain-builtins post-check must deduplicate by identity, or it OOMs/hangs on the DAG.
    Run in a subprocess so a regression surfaces as a clean timeout, not a hung suite.
    (Atheris found this OOM against the harness in fuzz/fuzz_wire.py.)"""
    driver = tmp_path / "dag.py"
    driver.write_text(
        "import pickle\nimport sys\nimport pytest_fast\n"
        "o = []\n"
        "for _ in range(64):\n"
        "    o = [o, o]\n"
        "data = pickle.dumps(o)\n"
        "result = pytest_fast._loads(data)\n"
        "sys.exit(0 if isinstance(result, list) else 3)\n"
    )
    try:
        proc = subprocess.run([sys.executable, str(driver)], capture_output=True, text=True, timeout=25, check=False)
    except subprocess.TimeoutExpired:
        pytest.fail("`_loads` did not terminate on a memo DAG — _is_plain_builtins must dedup by identity")
    assert proc.returncode == 0, (
        f"DAG decode failed (rc={proc.returncode}):\nstdout:{proc.stdout}\nstderr:{proc.stderr}"
    )
