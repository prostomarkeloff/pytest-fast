"""(Re)generate the wire fuzzer's seed corpus. Idempotent — safe to run any time.

Seeds are pickled builtins (so libFuzzer starts from valid structure and mutates from
there) plus a couple of raw malformed frames. Crashers found by `fuzz_wire.py` should
also be committed under `corpus/` so `tests/test_fuzz_corpus.py` replays them forever.
"""

import pickle
from pathlib import Path

CORPUS = Path(__file__).parent / "corpus"

# Representative whitelisted-builtins values — the legitimate shapes the bus carries.
_SEEDS = [
    None,
    True,
    0,
    -1,
    255,
    3.14,
    2j,
    "",
    "hello",
    "юникод-текст",
    b"",
    b"\x00\xff\x80",
    (),
    (1, 2, 3),
    [],
    [1, [2, [3, [4]]]],
    {},
    {"a": 1, "b": [1, 2, 3]},
    set(),
    frozenset({1, 2, 3}),
    bytearray(b"abc"),
    # realistic protocol frames
    ("run", "deadbeefcafe", True, True, ["tests/test_x.py::test_a", "tests/test_x.py::test_b"]),
    ("status", "deadbeefcafe"),
    ("ready", 0, 413, 0.12, None),
    {"nodeid": "tests/test_x.py::test_a", "outcome": "passed", "duration": 0.01},
    {"rc": 1, "summary": "1 failed, 2 passed"},
]

# Raw, non-pickle frames — bootstrap the malformed-input neighborhood.
_RAW = {
    "raw_empty": b"",
    "raw_truncated_global": b"cos\nsyst",
    "raw_high_protocol": b"\x80\x05",
}


def main():
    CORPUS.mkdir(exist_ok=True)
    for i, obj in enumerate(_SEEDS):
        (CORPUS / f"seed_{i:03d}").write_bytes(pickle.dumps(obj, protocol=pickle.HIGHEST_PROTOCOL))
    for name, blob in _RAW.items():
        (CORPUS / name).write_bytes(blob)
    print(f"wrote {len(_SEEDS) + len(_RAW)} seeds to {CORPUS}")


if __name__ == "__main__":
    main()
