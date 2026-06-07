# fuzz/ — Atheris coverage-guided fuzzing

Coverage-guided ([libFuzzer](https://llvm.org/docs/LibFuzzer.html)-backed) fuzzing of the
pytest-fast wire decoder — the highest-value attack surface, since a same-user peer can
write arbitrary bytes to the control / per-run sockets. This complements the in-process
Hypothesis tests (`tests/test_fuzz_*.py`): Hypothesis checks invariants with shrinking on
every PR; Atheris mutates bytes guided by bytecode coverage and digs far deeper into the
pickle opcode space.

## Layout

- `fuzz_wire.py` — the harness (`TestOneInput` → `_loads`). Invariant: a returned value
  must be plain builtins; process crash / OOM (`-rss_limit_mb`) / hang (`-timeout`) are
  caught by libFuzzer natively.
- `make_seeds.py` — (re)generates the seed corpus (valid builtins + a few malformed frames).
- `corpus/` — committed seeds **and curated reproducers** of past findings (curated = small,
  reviewable). Replayed on every PR by `tests/test_fuzz_corpus.py` (no Atheris needed) — the
  durable regression guard.
- `corpus-work/` — gitignored scratch dir where libFuzzer writes coverage-increasing units
  during a run, so `corpus/` stays clean.

## Run it

```bash
make fuzz-install          # install Atheris (macOS: builds against Homebrew LLVM)
make fuzz                  # 120s by default; FUZZ_TIME=600 make fuzz for longer
# or directly (scratch dir first = where new units are written; corpus/ = read-only seeds):
uv run python fuzz/fuzz_wire.py -max_total_time=300 -rss_limit_mb=2048 fuzz/corpus-work fuzz/corpus
```

A finding is written to the CWD as `crash-*` / `oom-*` / `timeout-*`. To turn it into a
permanent regression: copy it into `corpus/` (give it a descriptive name) — the corpus
replay test will exercise it forever.

## What it has already found

Each of these is now fixed in `_loads` and pinned by a test:

- **decode-amplification OOM** — a ~5-byte frame with a bogus length-prefixed opcode made
  the C unpickler pre-allocate gigabytes (`genops` length pre-scan).
- **memo-index / FRAME pre-allocation** — `LONG_BINPUT` with a ~2.3-billion index resized
  the memo array to ~18 GB (`genops` arg-vs-frame-size bounds).
- **whitelist class-as-value** — `cbuiltins\ncomplex\n.` returned the `complex` *class*
  itself (post-decode plain-data check).
- **memo-DAG traversal blow-up** — `m=[m,m]`×N is tiny but a naive walk visits 2^N paths
  (identity-deduplicated traversal).

## Notes

- POSIX only (Atheris targets Python 3.11–3.13). On Linux it installs a prebuilt wheel; on
  macOS it builds against `brew install llvm` (Apple clang lacks libFuzzer).
- Coverage feedback is partial: the actual pickle VM is the C `_pickle` module (not
  bytecode-instrumentable), so guidance comes from the `find_class` gate and our wrapper —
  still enough to have surfaced the findings above quickly.
