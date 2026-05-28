"""Tests for the thin `_send`/`_recv` bus (length-prefixed pickle).

The bus is the contract between master/worker and client/daemon: break it and
everything breaks. We test roundtrip, EOF-handling, and multi-message ordering."""

from __future__ import annotations

import socket

from pytest_fast import _recv, _send


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
