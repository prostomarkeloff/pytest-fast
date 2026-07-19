"""Tests for the thin `_send`/`_recv` bus (length-prefixed pickle).

The bus is the contract between master/worker and client/daemon: break it and
everything breaks. We test roundtrip, EOF-handling, and multi-message ordering."""

from __future__ import annotations

import socket
import threading
from pathlib import Path
from typing import cast

import pytest

from pytest_fast import (
    RunResult,
    _recv,
    _send,
    _short_unix_path,
    request_run,
    request_run_results,
    request_run_streamed,
)


def test_roundtrip_basic_types() -> None:
    a, b = socket.socketpair()
    try:
        _send(a, ("hello", 42, [1, 2, 3]))
        msg, nbytes = _recv(b)
        assert msg == ("hello", 42, [1, 2, 3])
        assert nbytes > 4  # ≥ header + at least 1 byte of payload
    finally:
        a.close()
        b.close()


def test_roundtrip_dict_with_nested_tuple() -> None:
    """Mimics a real daemon frame `{'progress': (done, total)}`."""
    a, b = socket.socketpair()
    try:
        _send(a, {"progress": (17, 42)})
        msg, _ = _recv(b)
        assert msg == {"progress": (17, 42)}
    finally:
        a.close()
        b.close()


def test_recv_returns_none_on_closed_peer() -> None:
    """EOF before header ⇒ None, nbytes=0 — expected when the client has gone."""
    a, b = socket.socketpair()
    a.close()
    try:
        msg, nbytes = _recv(b)
        assert msg is None
        assert nbytes == 0
    finally:
        b.close()


def test_recv_returns_none_on_truncated_payload() -> None:
    """Header received (announcing N bytes of payload), but the payload was cut off ⇒ None, nbytes=4."""
    a, b = socket.socketpair()
    try:
        # Hand-craft a header announcing "4 bytes of payload", but don't send the payload
        a.sendall(b"\x00\x00\x00\x04")
        a.close()
        msg, nbytes = _recv(b)
        assert msg is None
        assert nbytes == 4
    finally:
        b.close()


def test_messages_are_framed_independently() -> None:
    """Two `_send` in a row → two separate `_recv` (length-prefixing guarantees the
    boundary; without it consecutive sends would glue into one pickle stream)."""
    a, b = socket.socketpair()
    try:
        _send(a, ("first", 1))
        _send(a, ("second", 2))
        msg1, _ = _recv(b)
        msg2, _ = _recv(b)
        assert msg1 == ("first", 1)
        assert msg2 == ("second", 2)
    finally:
        a.close()
        b.close()


def test_run_frame_keeps_execution_modes_as_trailing_fields() -> None:
    """Execution-mode extensions remain safe for daemons that ignore unknown tuple tails."""
    a, b = socket.socketpair()
    frame = ("run", "fingerprint", True, True, ["tests/test_x.py::test_x"], False, 0, False, True, False)
    try:
        _send(a, frame)
        received, _ = _recv(b)
        assert received == frame
        assert isinstance(received, tuple)
        assert received[-2:] == (True, False)
    finally:
        a.close()
        b.close()


def _serve_legacy_run_frame(address: Path) -> threading.Thread:
    listener = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    with _short_unix_path(str(address)) as bind_path:
        listener.bind(bind_path)
    listener.listen(1)

    def serve() -> None:
        with listener:
            conn, _ = listener.accept()
            with conn:
                _recv(conn)
                _send(conn, {"rc": 0, "summary": "legacy daemon ran the request"})

    thread = threading.Thread(target=serve)
    thread.start()
    return thread


def _serve_result_frames(
    address: Path,
    frames: list[dict[str, object]],
    requests: list[tuple[object, ...]],
) -> threading.Thread:
    listener = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    with _short_unix_path(str(address)) as bind_path:
        listener.bind(bind_path)
    listener.listen(1)

    def serve() -> None:
        with listener:
            conn, _ = listener.accept()
            with conn:
                request, _ = _recv(conn)
                requests.append(cast("tuple[object, ...]", request))
                for frame in frames:
                    _send(conn, frame)

    thread = threading.Thread(target=serve)
    thread.start()
    return thread


def test_results_client_delivers_quiet_progress_and_compact_results(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    address = tmp_path / "results.sock"
    requests: list[tuple[object, ...]] = []
    result = {
        "nodeid": "tests/test_x.py::test_x",
        "outcome": "failed",
        "duration": 0.3,
        "phases": {"setup": 0.1, "call": 0.2, "teardown": 0.0},
    }
    thread = _serve_result_frames(
        address,
        [
            {"progress": (1, 1)},
            {
                "rc": 1,
                "summary": "one failed",
                "results": [result],
                "stop_on_failure": True,
            },
        ],
        requests,
    )
    observed_results: list[RunResult] = []
    observed_progress: list[tuple[int, int]] = []
    try:
        reply = request_run_results(
            str(address),
            observed_results.append,
            on_progress=lambda done, total: observed_progress.append((done, total)),
            nodeids=["tests/test_x.py::test_x"],
            stop_on_failure=True,
        )
    finally:
        thread.join(timeout=5)

    assert not thread.is_alive()
    assert reply.get("rc") == 1
    assert observed_progress == [(1, 1)]
    assert observed_results == [result]
    assert capsys.readouterr() == ("", "")
    assert len(requests) == 1
    request = requests[0]
    assert request[0] == "run"
    assert request[2:4] == (False, False)
    assert request[4] == ["tests/test_x.py::test_x"]
    assert request[7:10] == (True, True, False)


@pytest.mark.parametrize(
    "final_frame",
    [
        {"rc": 0, "summary": "missing results"},
        {"rc": 0, "summary": "invalid results", "results": ["not a result"]},
        {"rc": 0, "summary": "invalid result shape", "results": [{}]},
    ],
)
def test_results_client_rejects_missing_or_malformed_results(tmp_path: Path, final_frame: dict[str, object]) -> None:
    address = tmp_path / "invalid-results.sock"
    requests: list[tuple[object, ...]] = []
    thread = _serve_result_frames(address, [final_frame], requests)
    observed_results: list[RunResult] = []
    try:
        reply = request_run_results(str(address), observed_results.append)
    finally:
        thread.join(timeout=5)

    assert not thread.is_alive()
    assert reply.get("rc") == 2
    assert "results" in cast("str", reply.get("summary", ""))
    assert observed_results == []


def test_stop_on_failure_rejects_legacy_daemon_without_capability_ack(tmp_path: Path) -> None:
    """A legacy daemon must not produce a trusted stop-first result."""
    address = tmp_path / "legacy.sock"
    thread = _serve_legacy_run_frame(address)
    try:
        reply = request_run(str(address), stop_on_failure=True)
    finally:
        thread.join(timeout=5)
    assert not thread.is_alive()
    assert reply.get("rc") == 2
    assert "does not support stop_on_failure" in str(reply.get("summary"))


def test_streamed_stop_on_failure_rejects_legacy_daemon_without_capability_ack(tmp_path: Path) -> None:
    """The streamed client enforces the same stop-first capability contract."""
    address = tmp_path / "legacy-stream.sock"
    thread = _serve_legacy_run_frame(address)
    try:
        reply = request_run_streamed(str(address), lambda _report: None, stop_on_failure=True)
    finally:
        thread.join(timeout=5)
    assert not thread.is_alive()
    assert reply.get("rc") == 2
    assert "does not support stop_on_failure" in str(reply.get("summary"))


def test_fresh_workers_rejects_legacy_daemon_without_capability_ack(tmp_path: Path) -> None:
    """A legacy daemon must not produce a trusted fresh-worker result."""
    address = tmp_path / "legacy-fresh.sock"
    thread = _serve_legacy_run_frame(address)
    try:
        reply = request_run(str(address), fresh_workers=True)
    finally:
        thread.join(timeout=5)
    assert not thread.is_alive()
    assert reply.get("rc") == 2
    assert "does not support fresh_workers" in str(reply.get("summary"))


def test_streamed_fresh_workers_rejects_legacy_daemon_without_capability_ack(tmp_path: Path) -> None:
    """The streamed client enforces the same fresh-worker capability contract."""
    address = tmp_path / "legacy-stream-fresh.sock"
    thread = _serve_legacy_run_frame(address)
    try:
        reply = request_run_streamed(str(address), lambda _report: None, fresh_workers=True)
    finally:
        thread.join(timeout=5)
    assert not thread.is_alive()
    assert reply.get("rc") == 2
    assert "does not support fresh_workers" in str(reply.get("summary"))
