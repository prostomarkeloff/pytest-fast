"""Fuzz the wire codec: `_send` / `_recv` / `_loads` / `_SafeUnpickler`.

These run in-process over an `AF_UNIX` `socketpair` — no daemon, fast, deterministic
(Hypothesis `derandomize` in CI). They assert the three invariants the bus relies on:

  1. round-trip — every legitimate builtins-only structure survives `_send`→`_recv`
     unchanged (no false negatives in the safe unpickler).
  2. robustness — arbitrary wire bytes never crash the reader and never yield a
     non-builtin object (`_recv` swallows to `(None, n)`; `_loads` raises cleanly).
  3. no escalation — a hostile pickle referencing a forbidden global (`os.system`,
     `builtins.eval`, …) is rejected at `find_class`, before any REDUCE runs (RCE proof).

Plus the guard-only OOM check: an oversized length header is rejected by the
`_MAX_FRAME_BYTES` guard *without* allocating the claimed gigabytes.
"""

from __future__ import annotations

import pickle
import socket
import struct
import threading
import time
from typing import TYPE_CHECKING

import pytest
from hypothesis import assume, given, settings
from hypothesis import strategies as st

from pytest_fast import _MAX_FRAME_BYTES, _loads, _recv, _send

if TYPE_CHECKING:
    from pathlib import Path

pytestmark = pytest.mark.fuzz

# Concrete builtin types the bus is allowed to carry (mirror of `_PICKLE_ALLOWED_BUILTINS`,
# as runtime types). `_is_pure_builtins` walks a decoded object against this set.
_BUILTIN_TYPES = (
    type(None),
    bool,
    int,
    float,
    complex,
    str,
    bytes,
    bytearray,
    tuple,
    list,
    dict,
    set,
    frozenset,
)


def _is_pure_builtins(obj: object) -> bool:
    """True iff `obj` is composed solely of whitelisted builtin types. Iterative (no
    RecursionError on deep nesting) and identity-deduplicated (no 2^N blow-up on a memo
    DAG). `type(x) in ...` (not isinstance) so a builtin *subclass* would still fail."""
    stack = [obj]
    seen: set[int] = set()
    while stack:
        o = stack.pop()
        if id(o) in seen:
            continue
        seen.add(id(o))
        if type(o) not in _BUILTIN_TYPES:
            return False
        if isinstance(o, dict):
            stack.extend(o.keys())
            stack.extend(o.values())
        elif isinstance(o, list | tuple | set | frozenset):
            stack.extend(o)
    return True


# ── strategies ────────────────────────────────────────────────────────────────

# Hashable atoms — usable as dict keys / set members. NaN is excluded so round-trip
# equality holds (nan != nan); it gets its own non-crash coverage below.
_hashable = (
    st.none()
    | st.booleans()
    | st.integers()
    | st.floats(allow_nan=False)
    | st.complex_numbers(allow_nan=False, allow_infinity=False)
    | st.text()
    | st.binary()
)
# Values may additionally be the mutable/unhashable builtins.
_value_atom = _hashable | st.binary().map(bytearray)


def _builtins_values() -> st.SearchStrategy[object]:
    return st.recursive(
        _value_atom,
        lambda children: (
            st.lists(children)
            | st.lists(children).map(tuple)
            | st.dictionaries(_hashable, children)
            | st.sets(_hashable)
            | st.frozensets(_hashable)
        ),
        max_leaves=25,
    )


def _pair() -> tuple[socket.socket, socket.socket]:
    return socket.socketpair(socket.AF_UNIX, socket.SOCK_STREAM)


# ── 1. round-trip: every builtins structure survives the bus ────────────────────


@given(value=_builtins_values())
def test_send_recv_roundtrip_builtins(value: object) -> None:
    a, b = _pair()
    try:
        nbytes = _send(a, value)
        got, recv_bytes = _recv(b)
        assert got == value
        assert recv_bytes == nbytes  # framed length accounting is exact
    finally:
        a.close()
        b.close()


@given(value=_builtins_values())
def test_loads_inverts_dumps(value: object) -> None:
    """The safe unpickler accepts everything `pickle.dumps` produces for builtins —
    no legitimate payload is wrongly rejected by the whitelist."""
    assert _loads(pickle.dumps(value, protocol=pickle.HIGHEST_PROTOCOL)) == value


# ── 2. robustness: arbitrary bytes never crash, never yield a non-builtin ────────


@given(blob=st.binary(max_size=4096))
def test_loads_arbitrary_bytes_is_safe(blob: bytes) -> None:
    """`_loads` on random bytes either raises a normal exception or returns a value
    that is *purely* builtins — it can never construct an arbitrary object."""
    try:
        result = _loads(blob)
    except Exception:
        return
    assert _is_pure_builtins(result)


@given(payload=st.binary(max_size=4096))
def test_recv_arbitrary_framed_payload_is_safe(payload: bytes) -> None:
    """A correctly-framed but arbitrary payload: `_recv` returns either a builtins
    value or the `(None, ...)` corrupt-frame sentinel — never raises, never escalates."""
    a, b = _pair()
    try:
        b.sendall(struct.pack("!I", len(payload)) + payload)
        b.shutdown(socket.SHUT_WR)
        got, _ = _recv(a)
        assert got is None or _is_pure_builtins(got)
    finally:
        a.close()
        b.close()


@given(
    value=_builtins_values(),
    special=st.sampled_from([float("nan"), float("inf"), float("-inf")]),
)
def test_recv_tolerates_special_floats(value: object, special: float) -> None:
    """NaN / ±inf are legal IEEE floats and must cross the bus without crashing the
    reader (NaN won't compare equal to itself — so we only assert no crash)."""
    a, b = _pair()
    try:
        _send(a, (value, special))
        got, _ = _recv(b)
        assert isinstance(got, tuple)
    finally:
        a.close()
        b.close()


# ── 3. no escalation: forbidden globals are blocked before REDUCE ────────────────

# Minimal protocol-0 GLOBAL pickles: `c<module>\n<name>\n.` makes the unpickler call
# `find_class(module, name)` then STOP — so we exercise the whitelist gate directly,
# without ever reaching a REDUCE that would execute anything.
_FORBIDDEN_GLOBALS = [
    ("os", "system"),
    ("posix", "system"),
    ("nt", "system"),
    ("subprocess", "Popen"),
    ("subprocess", "run"),
    ("builtins", "eval"),
    ("builtins", "exec"),
    ("builtins", "compile"),
    ("builtins", "__import__"),
    ("builtins", "open"),
    ("builtins", "getattr"),
    ("builtins", "globals"),
    ("__main__", "anything"),
    ("functools", "partial"),
    ("operator", "attrgetter"),
]


@pytest.mark.parametrize("module,name", _FORBIDDEN_GLOBALS, ids=[f"{m}.{n}" for m, n in _FORBIDDEN_GLOBALS])
def test_safe_unpickler_blocks_forbidden_global(module: str, name: str) -> None:
    evil = f"c{module}\n{name}\n.".encode()
    with pytest.raises(pickle.UnpicklingError):
        _loads(evil)


@pytest.mark.parametrize("module,name", _FORBIDDEN_GLOBALS, ids=[f"{m}.{n}" for m, n in _FORBIDDEN_GLOBALS])
def test_recv_swallows_forbidden_global_frame(module: str, name: str) -> None:
    """The same hostile frame arriving on a real connection is reported as a corrupt
    frame `(None, ...)` — never an exception that would tear the daemon's accept loop."""
    a, b = _pair()
    try:
        _send(a, b"")  # warm the pair; ignored
        _recv(b)
        evil = f"c{module}\n{name}\n.".encode()
        b.sendall(struct.pack("!I", len(evil)) + evil)
        b.shutdown(socket.SHUT_WR)
        got, _ = _recv(a)
        assert got is None
    finally:
        a.close()
        b.close()


def test_malicious_reduce_never_executes(tmp_path: Path) -> None:
    """End-to-end RCE proof. A real `os.system` REDUCE pickle that, if find_class let
    `os.system` through, would create a sentinel file. `_loads` must raise at the GLOBAL
    gate and the sentinel must never appear — the REDUCE is never reached."""
    sentinel = tmp_path / "PWNED"
    cmd = f"touch {sentinel}"
    # cos\nsystem\n(S'<cmd>'\ntR.  — GLOBAL os.system, build args tuple, REDUCE, STOP.
    evil = b"cos\nsystem\n(S'" + cmd.encode() + b"'\ntR."
    with pytest.raises(pickle.UnpicklingError):
        _loads(evil)
    assert not sentinel.exists(), "os.system REDUCE executed — the whitelist failed!"


# ── guard-only OOM: oversized length header rejected without allocation ──────────


@given(length=st.integers(min_value=_MAX_FRAME_BYTES + 1, max_value=2**32 - 1))
@settings(max_examples=60)
def test_oversize_header_rejected_without_allocation(length: int) -> None:
    """A 4-byte header claiming up to ~4 GiB must be rejected by the `_MAX_FRAME_BYTES`
    guard the instant it's read — `_recv` returns `(None, 4)` and never tries to
    `recv`/allocate the claimed payload. We send ONLY the header (no payload): if the
    guard were missing, `_recv` would block forever trying to read `length` bytes."""
    a, b = _pair()
    try:
        b.sendall(struct.pack("!I", length))
        t0 = time.perf_counter()
        got, n = _recv(a)
        elapsed = time.perf_counter() - t0
        assert got is None
        assert n == 4  # consumed the header only
        assert elapsed < 2.0, "guard did not short-circuit — it tried to read the payload"
    finally:
        a.close()
        b.close()


def test_max_uint32_header_is_rejected() -> None:
    """The extreme: header = 0xFFFFFFFF (~4 GiB). Same guard, no 4 GiB bytearray."""
    a, b = _pair()
    try:
        b.sendall(struct.pack("!I", 0xFFFFFFFF))
        got, n = _recv(a)
        assert got is None
        assert n == 4
    finally:
        a.close()
        b.close()


# Decode-amplification: a frame can be tiny yet ask the C unpickler to PRE-ALLOCATE
# gigabytes via a length-prefixed opcode whose declared size dwarfs the bytes present.
# `_MAX_FRAME_BYTES` (a wire-size cap) is no defense — the frame is bytes long. The Atheris
# harness (fuzz/fuzz_wire.py) found this as an OOM in ~30s; `_loads`' genops pre-scan now
# rejects it before any allocation.
_DECODE_BOMBS = [
    (b"B", "<I", 2**31),  # BINBYTES, 4-byte length ~2 GiB
    (b"X", "<I", 2**31),  # BINUNICODE
    (b"\x8e", "<Q", 2**40),  # BINBYTES8, 8-byte length ~1 TiB
    (b"\x8d", "<Q", 2**40),  # BINUNICODE8
]


@pytest.mark.parametrize("opcode,width,size", _DECODE_BOMBS, ids=["BINBYTES", "BINUNICODE", "BINBYTES8", "BINUNICODE8"])
def test_decode_amplification_bomb_is_rejected(opcode: bytes, width: str, size: int) -> None:
    """A ~13-byte frame claiming a multi-GB string must be rejected by the pre-scan, not
    pre-allocated. If this regresses it OOMs the process — so the guard is load-bearing."""
    bomb = b"\x80\x04" + opcode + struct.pack(width, size) + b"\x00\x00\x00\x00"
    with pytest.raises(pickle.UnpicklingError):
        _loads(bomb)


def test_decode_bomb_is_swallowed_by_recv() -> None:
    """The same bomb arriving on a real connection is a corrupt frame `(None, …)`, never
    an OOM or an exception that tears the daemon's accept loop."""
    bomb = b"\x80\x04" + b"B" + struct.pack("<I", 2**31) + b"\x00\x00\x00\x00"
    a, b = _pair()
    try:
        b.sendall(struct.pack("!I", len(bomb)) + bomb)
        b.shutdown(socket.SHUT_WR)
        got, _ = _recv(a)
        assert got is None
    finally:
        a.close()
        b.close()


# A `GLOBAL builtins.<cls>` + STOP returns the builtin CLASS itself. find_class must allow
# these classes (REDUCE reconstructs frozenset/complex INSTANCES through them), but the
# class as a standalone VALUE is never legitimate data. Atheris found this for `complex`.
_WHITELISTED_CLASSES = ["complex", "frozenset", "tuple", "dict", "list", "set", "bytearray", "bytes", "int", "float"]


@pytest.mark.parametrize("clsname", _WHITELISTED_CLASSES)
def test_whitelisted_class_is_not_returned_as_value(clsname: str) -> None:
    payload = f"cbuiltins\n{clsname}\n.".encode()
    with pytest.raises(pickle.UnpicklingError):
        _loads(payload)


# Pre-allocation via a memo index (PUT) or FRAME length: the C unpickler resizes its memo
# array / frame buffer from these fields BEFORE bounds-checking, so a tiny frame triggers a
# multi-GB allocation. Atheris found the memo case (LONG_BINPUT ≈ 2.3e9 → ~18 GiB memo).
_PREALLOC_BOMBS = [
    (b"\x80\x05r" + struct.pack("<I", 2_000_000_000) + b".", "memo_index"),
    (b"\x80\x05\x95" + struct.pack("<Q", 2_000_000_000) + b"N.", "frame_length"),
]


@pytest.mark.parametrize("payload,name", _PREALLOC_BOMBS, ids=[n for _, n in _PREALLOC_BOMBS])
def test_preallocation_bomb_is_rejected(payload: bytes, name: str) -> None:
    """A tiny frame whose memo-index / FRAME-length field would drive a gigabyte
    pre-allocation in the C unpickler must be rejected by the genops bounds-scan."""
    with pytest.raises(pickle.UnpicklingError):
        _loads(payload)


# ── truncation & fragmentation ──────────────────────────────────────────────────


@given(payload=st.binary(min_size=8, max_size=2048), missing=st.integers(min_value=1, max_value=7))
def test_recv_truncated_payload_returns_none(payload: bytes, missing: int) -> None:
    """Header promises N bytes, peer delivers N-missing then closes → `_recv` must
    report the truncation as `(None, ...)`, not hang or mis-decode."""
    assume(missing < len(payload))
    a, b = _pair()
    try:
        b.sendall(struct.pack("!I", len(payload)) + payload[: len(payload) - missing])
        b.shutdown(socket.SHUT_WR)
        got, _ = _recv(a)
        assert got is None
    finally:
        a.close()
        b.close()


@given(value=_builtins_values(), splits=st.lists(st.integers(min_value=1, max_value=64), min_size=1, max_size=8))
@settings(max_examples=40)
def test_recv_reassembles_fragmented_frame(value: object, splits: list[int]) -> None:
    """A frame delivered in many small TCP-style chunks must reassemble byte-perfectly
    (`_recvn`'s loop). We slice the framed bytes at arbitrary offsets and send piecemeal."""
    a, b = _pair()
    try:
        frame = struct.pack("!I", len(pickle.dumps(value, protocol=pickle.HIGHEST_PROTOCOL))) + pickle.dumps(
            value, protocol=pickle.HIGHEST_PROTOCOL
        )
        pos = 0
        for step in splits:
            b.sendall(frame[pos : pos + step])
            pos += step
        b.sendall(frame[pos:])
        got, _ = _recv(a)
        assert got == value
    finally:
        a.close()
        b.close()


# bounded real-load (OOM tier, but safe): a genuinely large frame round-trips intact.
@pytest.mark.stress
def test_large_frame_roundtrip_under_cap() -> None:
    """~32 MiB of real bytes crosses the bus and reassembles — exercises `_recvn`'s
    multi-chunk loop and confirms `_MAX_FRAME_BYTES` (256 MiB) has real headroom.

    Read concurrently: a 32 MiB `sendall` blocks until the peer drains the socket
    buffer, so the reader has to run in parallel or the pair self-deadlocks."""
    big = b"x" * (32 * 1024 * 1024)
    a, b = _pair()
    received: list[object] = []

    def _reader() -> None:
        received.append(_recv(b)[0])

    reader = threading.Thread(target=_reader)
    reader.start()
    try:
        _send(a, big)
        reader.join(timeout=30.0)
        assert not reader.is_alive(), "reader did not finish — large frame stalled"
        assert received == [big]
    finally:
        a.close()
        b.close()
