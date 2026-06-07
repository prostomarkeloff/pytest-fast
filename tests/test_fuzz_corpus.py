"""Replay the fuzz corpus through the wire decoder — the durable regression guard.

The Atheris harness (`fuzz/fuzz_wire.py`) explores coverage and writes interesting /
crashing inputs into `fuzz/corpus/`. This test re-runs every corpus file through `_loads`
on every fuzz-tier run — *without* Atheris — so a crasher Atheris once surfaced can never
silently regress, and the seed corpus doubles as a fast decode-robustness sweep.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from pytest_fast import _loads

pytestmark = pytest.mark.fuzz

_CORPUS = Path(__file__).parent.parent / "fuzz" / "corpus"
_CORPUS_FILES = sorted(_CORPUS.glob("*")) if _CORPUS.is_dir() else []

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
    """Iterative + identity-deduplicated whitelist check — no recursion (deep nesting) and
    no 2^N blow-up (a committed memo-DAG crasher) can trip the checker itself."""
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


@pytest.mark.skipif(not _CORPUS_FILES, reason="no fuzz corpus present")
@pytest.mark.parametrize("path", _CORPUS_FILES, ids=[p.name for p in _CORPUS_FILES])
def test_corpus_input_decodes_safely(path: Path) -> None:
    data = path.read_bytes()
    try:
        result = _loads(data)
    except Exception:
        return  # a decode error is the expected outcome for many corpus inputs
    assert _is_pure_builtins(result), f"{path.name}: _loads returned a non-builtin (whitelist escape)"
