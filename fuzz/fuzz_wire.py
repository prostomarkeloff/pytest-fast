"""Atheris coverage-guided fuzz harness for the pytest-fast wire decoder.

Targets `_loads` — the `_SafeUnpickler` parser, the highest-value attack surface: a
same-user peer can write arbitrary bytes to the control / per-run sockets. libFuzzer
mutates inputs guided by *bytecode coverage* of `pytest_fast` (notably the `find_class`
security gate), exploring the pickle opcode space far deeper than random generation —
this is the part Hypothesis's `st.binary()` can't do.

The invariant: if `_loads` returns, the value must be composed **solely of builtin
types**. A non-builtin return is a whitelist ESCAPE (a step toward RCE) and aborts the
run. Normal decode errors on garbage bytes are expected and ignored. Process crashes,
OOM (`-rss_limit_mb`), and hangs (`-timeout`) are caught by libFuzzer natively — so a
decode-amplification pickle (small frame → multi-GB object) shows up as an OOM here.

Not a pytest test (it's a libFuzzer loop). Run via:
    make fuzz                 # time-boxed, against fuzz/corpus
    uv run python fuzz/fuzz_wire.py -max_total_time=300 -rss_limit_mb=2048 fuzz/corpus

Crashers libFuzzer finds are written as `crash-*` in the CWD; drop them into
`fuzz/corpus/` and `tests/test_fuzz_corpus.py` will replay them forever (no Atheris
needed for the replay).
"""

import sys

import atheris

# Instrument pytest_fast (and the stdlib it pulls) so libFuzzer gets coverage feedback
# from the decoder + the `find_class` whitelist gate. Importing pytest_fast here does NOT
# trigger collection (`_PYTEST_FAST_COLLECT` is unset), so this stays cheap.
with atheris.instrument_imports():
    import pytest_fast

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


def _is_pure_builtins(obj):
    """Iterative (no RecursionError on deep nesting) and identity-deduplicated (no 2^N
    blow-up on a memo DAG) check that `obj` is composed solely of whitelisted builtin
    types. Without the id() dedup this checker would OOM on the very `m=[m,m]` DAG we want
    `_loads` to survive — i.e. the harness must be at least as robust as the target."""
    stack = [obj]
    seen = set()
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
        elif isinstance(o, (list, tuple, set, frozenset)):
            stack.extend(o)
    return True


def TestOneInput(data):
    try:
        result = pytest_fast._loads(data)
    except Exception:
        return  # any decode failure on arbitrary bytes is acceptable, not a finding
    if not _is_pure_builtins(result):
        raise RuntimeError(f"SECURITY: _SafeUnpickler returned a non-builtin object: {type(result)!r}")


def main():
    atheris.Setup(sys.argv, TestOneInput)
    atheris.Fuzz()


if __name__ == "__main__":
    main()
