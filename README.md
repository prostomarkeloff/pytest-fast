<div align="center">

# pytest-fast

**Collect once. Fork warm. Skip the cold start.**

[![Python 3.11+](https://img.shields.io/badge/python-3.11+-blue.svg)](https://www.python.org/downloads/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![Types: pyright](https://img.shields.io/badge/types-pyright-blue)](https://github.com/microsoft/pyright)
[![Lint: ruff](https://img.shields.io/badge/lint-ruff-orange.svg)](https://github.com/astral-sh/ruff)

</div>

A resident `forkserver`-based pytest accelerator. The first run boots a daemon that imports your app graph and collects tests **once**. Every subsequent run forks warm workers — collect-free, import-free, ready in milliseconds. Edit a file? The daemon notices and re-collects automatically.

Two ways to drive it, one warm engine underneath:

- **`pytest-fast --address …`** — a standalone CLI. A thin client triggers the daemon and prints a lean, bespoke summary. **Lowest overhead** — the fast path for the tight TDD loop and CI.
- **`pytest --fast`** — a pytest plugin. Your `pytest` invocation stays a real pytest session, so reporting is **100% native** (terminal, `--durations`, `-v/-s`, `--junitxml`, plugins, exit codes) — the warm daemon just does the execution.

> ### ⚠ The two front-ends are NOT equally fast — this is the most important thing to know
>
> **`pytest --fast` is much slower than `pytest-fast --address`.** Because the plugin keeps your `pytest` process a *real* session, it **re-pays the controller-side import + collection on every run** — loading conftest, plugins, and your whole app graph (the very ~4–5 s cost this tool exists to amortize). Only the *execution* is warm; the cold start comes back on the controller.
>
> The thin CLI `pytest-fast --address` skips all of that — the client just pings the resident daemon, which already holds the collected suite — so warmup ≈ `fork()` and a warm rerun is **sub-second**.
>
> **Rule of thumb:** reach for **`pytest-fast --address`** by default (tight loops, CI, anything where you re-run constantly). Use **`pytest --fast` only when you actually need native reporting** — `--junitxml`, native `-v`, third-party report plugins — and can eat the per-run controller-side cold start. (It's still far faster than cold `xdist`, which re-pays that import *N* times; it's just nowhere near the CLI.)

```bash
uv add pytest-fast
```

```bash
pytest-fast --address /tmp/myproj.sock    # the fast path — warm rerun ≈ fork(), sub-second
pytest --fast                             # native pytest output, but re-pays controller import each run
```

> **POSIX only** (uses `forkserver` / `AF_UNIX` / `fcntl`). On Windows, use `pytest-xdist`.

---

## Why

`pytest-xdist` cold-spawns N workers, **each** of which re-imports the entire app graph. A FastAPI service with SQLAlchemy + Pydantic + 30 internal modules can spend **4–5 seconds per worker just importing** before a single test runs. At `-n 6` that's 25+ seconds of pure import overhead per CI run, per local re-run, per TDD iteration.

`pytest-fast` pays that cost **once** in a resident daemon:

```
                       cold (xdist)              warm (pytest-fast)
                       ───────────────           ──────────────────
import app graph       4.5s × N workers          ~0 (preloaded)
collect tests          0.5s × N workers          ~0 (cached)
fork worker            fork()                    fork()
run tests              ←─── work ─────→          ←─── work ──→
total wall, N=6        ~30s + tests              ~0.3s + tests
```

The daemon stays alive for `--ttl` seconds of idle. Edit a source file → next run sees the change, the daemon re-collects and forks fresh workers transparently. Change a relevant env var → same. No manual restart, no `kill`-ing PIDs.

The speed win lives entirely in **collection amortization** — it's independent of how results are reported. That's why `pytest --fast` can give you *both* warm execution *and* full native reporting.

---

## `pytest --fast` — the plugin (native reporting)

Auto-registered via pytest's `pytest11` entry point, so it works out of the box — no `-p` needed. It's **inert unless you pass `--fast`** (exactly like `xdist` is inert without `-n`), so a plain `pytest` run is completely unaffected.

> ⚠ **Slower than the CLI runner.** `pytest --fast` re-pays the controller-side import + collection every run (see the callout above) — choose it for native reporting, not for raw speed. For the fastest loop, use [`pytest-fast --address`](#pytest-fast--the-cli-runner-lean--fast).

```bash
pytest --fast                          # whole suite, native output, via the warm daemon
pytest --fast -k payment               # selection is forwarded to the daemon
pytest --fast -v --durations=10        # native verbose + native slowest-durations
pytest --fast --junitxml=out.xml       # native junit — the controller IS pytest
```

```text
============================= test session starts ==============================
collected 413 items
............................................F............................ [ 17%]
...
=================================== FAILURES ===================================
____________________________ test_invalid_token ________________________________
    def test_invalid_token():
>       assert client.post("/login").status_code == 401
E       assert 200 == 401
tests/api/test_auth.py:42: AssertionError
============================= slowest 10 durations ==============================
2.13s call     tests/integration/test_payment_flow.py::test_full_purchase
0.92s setup    tests/db/test_migrations.py::test_full_upgrade
...
=========================== short test summary info ============================
FAILED tests/api/test_auth.py::test_invalid_token - assert 200 == 401
======================== 1 failed, 412 passed in 8.4s ==========================
```

That's **real pytest output**, not a re-implementation. How: your `pytest` process stays the controller (a real pytest session with a real `terminalreporter`); `pytest_runtestloop` hands execution to the resident daemon; the daemon runs your tests in warm fork workers and **streams full per-phase reports** back; the controller republishes each through its own `pytest_runtest_logreport` hook — the same mechanism xdist uses. So everything subscribed to that hook (terminal, durations, junit, html, coverage-ish, custom plugins, exit-code accounting) just works.

| option | env var | default | meaning |
|---|---|---|---|
| `--fast` | — | off | run via the resident daemon (otherwise pytest runs normally) |
| `--fast-address PATH` | `PYTEST_FAST_ADDRESS` | derived from project root | daemon Unix socket |
| `--fast-workers N` | `PYTEST_FAST_WORKERS` | **performance cores** | worker count (auto-detected, see [Workers](#workers-why-the-default-is-performance-cores)) |
| `--fast-ttl SECONDS` | `PYTEST_FAST_TTL` | 600 | daemon idle TTL |
| `--fast-watch` | — | off | also keep a pre-warm watcher running (see below) |

> ⚠ **Set the address via `PYTEST_FAST_ADDRESS` (or the `=` form `--fast-address=PATH`)**, not the bare space form. pytest computes its rootdir/inifile from the raw argv *before any plugin loads*, scanning it for existing paths — so once the daemon's socket file exists, a bare `--fast-address /tmp/x.sock` makes pytest root at `/tmp` and silently lose `pythonpath`/`pytest.ini`. The env var and the `=` form keep the path out of that scan. (This is a pytest limitation a plugin can't intercept.)

**Selection** (`-k`, `-m`) is forwarded — the daemon runs exactly the tests your session collected. **Caveat:** explicit path/nodeid args (`pytest --fast tests/x.py::test_y`) can produce rootdir-relative nodeids that don't line up with the daemon's collection (an xdist-class issue); when that happens the run fails loudly with a clear message rather than silently mis-reporting. Use `-k`/`-m` or a full run.

### Workers: why the default is performance cores

The default worker count is the number of **performance cores**, not the logical CPU count. On Apple Silicon (and other big.LITTLE designs) cores split into performance (P) and efficiency (E) cores; E-cores run ~half the throughput. The work-stealing dispatcher finishes when the **slowest** worker drains, so a worker scheduled onto an E-core becomes a straggler that bounds the whole run — more workers than P-cores doesn't speed things up, it adds stragglers plus memory/scheduler contention. So pytest-fast pins to the P-core count (macOS: `hw.perflevel0.physicalcpu`; e.g. 6 on a 6P+6E machine), falling back to the logical CPU count elsewhere. Override with `--fast-workers` / `--workers` / `PYTEST_FAST_WORKERS`.

**Need the resolved count in a script** (e.g. a Makefile sizing a per-worker resource pool to match the run)? Don't replicate the detection — ask the tool. `pytest-fast --print-inferred-workers` prints the count it would use (honoring the override precedence) and exits; the same value is available programmatically as `pytest_fast.resolve_workers()` (and `default_workers()` for the override-ignoring auto-detect). An invalid count (`--workers 0`, `PYTEST_FAST_WORKERS=-1`) is rejected loudly on every front-end — a 0-worker run would execute nothing and exit green, a false pass a test runner must never produce.

---

## `pytest-fast` — the CLI runner (lean & fast)

A standalone client/daemon. The client is trivial; the daemon renders a compact, bespoke summary. This is the lowest-overhead path — no controller-side collection, the thinnest possible bus.

```bash
# One-shot: connect to (or spawn) a resident daemon, run all tests, print summary
pytest-fast --address /tmp/myproj.sock --workers 6

# Same again — the daemon is already warm → just fork + run, no collect
pytest-fast --address /tmp/myproj.sock --workers 6

# Per-phase --durations in the summary (ships full reports on the bus)
pytest-fast --address /tmp/myproj.sock --workers 6 --full-report

# Extended parallelism diagnostics (eff, CPU-vs-I/O, lost-time, a worker-count hint)
pytest-fast --address /tmp/myproj.sock --workers 6 --detailed

# Deterministic bottleneck report — what to optimize to go faster (see below)
pytest-fast --address /tmp/myproj.sock --workers 6 --bench=4

# Pre-warm: a watcher refreshes the daemon BEFORE you re-run (see Watcher)
pytest-fast --address /tmp/myproj.sock --workers 6 --with-watcher

# Local single-process mode (no resident daemon, useful for CI smoke)
pytest-fast --runs 1 --workers 4
```

```text
══════════════════════════════════════════════════════════════════
  FORKSERVER DAEMON  —  6w  —  run #3 (warm)
══════════════════════════════════════════════════════════════════
  results : 412 passed, 1 failed  (n=413/413)
  warmup  :   0.01s   (fork+spawn; ~0 for resident rerun)
  RUN     :   8.42s   ← wall
  par.    : 5.21x of 6   (run-wall max=8.42 min=4.13)
  bus     : 467 round-trips, 24KB rx
  FAILURES (1):
    ✗ tests/api/test_auth.py::test_invalid_token
      >       assert client.post("/login").status_code == 401
      E       assert 200 == 401
      tests/api/test_auth.py:42: AssertionError
  DURATIONS (top 3, ≥5ms — per phase):     # only with --full-report
     2.130s  call     tests/integration/test_payment_flow.py::test_full_purchase
     0.920s  setup    tests/db/test_migrations.py::test_full_upgrade
     1.210s  call     tests/api/test_search.py::test_complex_filter
══════════════════════════════════════════════════════════════════
```

`par. 5.21x of 6` is the actual parallelism — total worker-busy time divided by wall. The closer to N, the better the work-stealing dispatcher kept your workers busy.

The CLI summary is **lossy by design** (counts, failure tracebacks, durations) — it's a bespoke render, not pytest's. Want full native reporting? Use `pytest --fast`. Want the absolute thinnest, fastest loop? Stay here.

---

## Performance diagnostics: `--detailed` and `--bench`

Two opt-in, CLI-runner-only reports (both rendered by the warm daemon — no native-pytest controller cost). Everything they say is a **deterministic** function of measured numbers and a fixed rule — never a heuristic guess. Each worker now also records its CPU time and inter-test bus idle, so the `N × wall` rectangle decomposes exactly and per-test CPU-vs-I/O is known.

### `--detailed` — *why* wasn't it faster, and *should I add workers?*

Adds a block under the `par.` line:

```text
  par.    : 5.94x of 6   (run-wall max=9.85 min=9.84)
  detail —
    eff     : 99%   (ideal wall 9.76s vs 9.86s actual)
    cpu     : 3.63x of 6  ·  61% CPU / 39% I/O (mixed)  ·  ~2.4 cores idle (2.3 on I/O)
    lost    : 0.61 worker-s idle  =  bus 0.49s + tail 0.12s
    balance : by time — counts vary 1.9x, walls within 0% (healthy)  (ran 347–670/w)
    floor   : 1.40s  tests/…::test_round_trip  ·  3 tests ≥1s, p99 0.36s
    verdict : 61% CPU/test → your 6 cores sit ~39% idle on I/O; ≈8 workers may overlap
              that … Try --workers 8 — measure: a shared DB or the E-core tax can cancel it.
```

- **eff** = parallel efficiency vs the work-conserving ideal (`Σwork/cores`). 99% means the work-stealing scheduler is maxed — better scheduling buys nothing.
- **cpu** = cores' worth of CPU actually burned. Low `% CPU` means the workers are blocked on I/O (a DB round-trip), so `par.` can look full while cores idle. `~N cores idle` is your headroom.
- **lost** = the parallelism deficit in absolute worker-seconds, split into bus chatter vs end-of-run straggler drain.
- **balance** = a big test-*count* spread at near-equal *walls* is **healthy** — work-stealing balances by time, not count.
- **verdict** = a deterministic worker-count suggestion: keeping the cores busy through I/O waits wants `cores / cpu_sat` workers (Little's-law pool sizing), capped by an E-core discount (workers past the perf cores run on slower E-cores and only hide I/O). It fires only in the clean regime and always says **measure** — a shared external resource or the E-core tax can cancel the gain.

### `--bench[=N]` — *what should I optimize?*

Runs the suite `N` times (default 2; the first is dropped as warmup — more runs steady the ranking and unlock per-test variance), then a **targeted [cProfile](https://docs.python.org/3/library/profile.html) pass over just the top bottleneck tests** adds function-level attribution. It prints a report ranked by reclaimable worker-seconds instead of the run summary:

```text
  pytest-fast bench  —  3172 tests, 9.06s wall @ 6w  (avg of 3 runs + warmup dropped)
  best @ 6 cores ≈ 8.73s   ·   floor (longest test) 1.17s  tests/…::test_returns_list
  where time goes: setup 13% · call 85% · teardown 2%   (of 52s test-wall)
  per-test wall : p50 0.001s · p90 0.033s · p99 0.291s · max 1.17s
  ── levers (ranked by reclaimable worker-seconds) ─────────────────
   1. SHARED SETUP ~  1.6 w-s
      tests/api/  — 8 tests × ~0.20s setup = 1.6s total
      → session/module-scope the fixture (if scope-widenable) … [potential]
   6. MIXED        ~  1.0 w-s
      tests/migrations/…::test_round_trip  (0.97s: setup 0.00/call 0.97/…, 30% CPU)
      profile (top by SELF wall — where it's actually burned; ncalls exact):
         0.834s self    667×  <method 'execute' of 'psycopg2.extensions.cursor'>
         0.057s self      1×  <method 'executemany' …>
```

- **best @ cores** = the deterministic floor: you cannot beat `max(Σwork/cores, longest-test)` no matter the worker count.
- **levers** = each is *(measured number → fixed rule → reclaimable worker-seconds)*, ranked by impact:
  - **SHARED SETUP** — K tests in one file each paying ~S setup is `K·S` worker-seconds; a session/module-scoped fixture pays it once. Flagged `[potential]` — scope-widenability can't be read from timings, only the upper bound.
  - **per-test hot-spots** — the slowest calls, classified I/O-BOUND / CPU-BOUND / SETUP-HEAVY by `cpu/total`. The tip states only what the timings determine; it never guesses the cause.
- **profile** rows (on the top tests) are the leaves where wall is actually burned, by **self** time, with **exact** call counts — `667× execute` in one test is a *measured* N+1 / hot-call signal, not a guess. cProfile is stdlib (no dependency), and its overhead is paid only on the handful of tests that hold the wall.
- **unstable timing** — with ≥2 measured runs (`--bench=3+`), per-test coefficient of variation flags flaky/contended timings.

---

## How it works

### One process imports your code; many fork off of it

```
                     ┌──────────────────────────────────────┐
client / pytest ─►   │  DAEMON (main process)               │
  --fast             │  - forkserver context                │
                     │  - control socket (run/status/...)   │
                     └──────────────────────────────────────┘
                                  │ first Process.start()
                                  ▼
                     ┌──────────────────────────────────────┐
                     │  FORKSERVER (one process, preloaded) │
                     │  - imports `pytest_fast` ONCE        │
                     │  - runs `_collect()` ONCE            │
                     │  - holds: items[], config            │
                     └──────────────────────────────────────┘
                                  │ fork() per worker per run
       ┌──────────────┬──────────┴───┬──────────────┐
       ▼              ▼              ▼              ▼
  WORKER 0       WORKER 1       WORKER 2       WORKER 3
  inherit items+config (copy-on-write), pull a test index from the
  master over a Unix socket (work-stealing), run pytest_runtest_protocol,
  ship the result back — lean RunResult, or full serialized reports.
```

`forkserver` is Python's stdlib `multiprocessing` start method that holds one clean, preloaded process and forks workers from it on demand. We set `set_forkserver_preload(["pytest_fast"])`; importing the package triggers `_collect()` at the bottom of `__init__.py`, so the forkserver process holds the collected items in its heap. Each worker is a `fork()` — **copy-on-write**, so items/config aren't re-allocated. `gc.freeze()` after collect moves everything to the permanent generation so GC doesn't dirty the COW pages.

### Two front-ends, one engine

The warm forkserver + work-stealing bus is shared. The difference is **who renders the report — and what the client side costs**:

- **CLI runner** — the daemon itself renders the bespoke summary and the thin client just prints it. The client does **no collection and no app import**, so it's the lowest-overhead path (warm rerun ≈ `fork()`).
- **`--fast` plugin** — the controller IS a real pytest session: it imports your app graph and **collects every run** (to know which nodeids to forward), then `pytest_runtestloop` hands execution to the daemon and republishes the streamed per-phase reports through its own real `terminalreporter`. So you get native reporting on top of warm execution — but the controller-side import/collect is **re-paid on every invocation** (it is *not* amortized by the resident daemon; only the workers are warm). That's why `pytest --fast` is much slower than the CLI runner, while still beating cold `xdist` (which re-pays that import on *every worker*).

Full reports cross the bus as plain-builtins dicts (`pytest_report_to_serializable`), so the pickle whitelist (below) is unchanged. The bus is heavier in full-report mode (~6× per test — longrepr + captured sections) but it's a local Unix socket, negligible against test time. `_MAX_FRAME_BYTES` is 256 MB.

### Stale detection — two axes

A warm daemon is wrong if:

1. **Source files changed** — `max(mtime)` of dirs in `PYTEST_FAST_WATCH_DIRS` (default `src,tests`) plus files in `PYTEST_FAST_WATCH_FILES` (default `pyproject.toml,pytest.ini`), compared against the snapshot taken at boot. Both use PATH-style REPLACE semantics. Implemented via early-exit `_any_source_newer(threshold)` so on large repos a single newer file short-circuits the scan.
2. **Relevant env changed** — `PYTEST_ADDOPTS`, `PYTEST_FAST_*`, and any prefix you list in `PYTEST_FAST_ENV_PREFIXES` (e.g. `MYAPP_,FEATURE_`) are hashed into an env fingerprint. The client sends its current fp on every request; the daemon compares against its boot fp.

On mismatch, the daemon replies `{stale: True}` and exits. The client coordinates a respawn under a `flock` and reconnects — invisible except for one "restarting daemon" line in stderr. The respawn loop is deadline-bounded, so a perpetually-stale condition can't livelock the client.

### Watcher (`--with-watcher` / `--fast-watch`)

Optional, opt-in. A background poll loop watches the same source set and, on a debounced change, boots a successor daemon on a `*.staging` socket. Once the successor is ready (collect succeeded), it cleanly shuts down the old one and rebinds onto the canonical address. **The next run finds a warm-and-fresh daemon instead of paying the boot cost on the critical path.** A broken edit (conftest error) leaves the current daemon untouched.

### Control protocol

One length-prefixed pickle message per connection, serialized through the daemon's `accept()` loop — so an active run is **never** interrupted by a control command:

```python
('run', fp[, full_report[, stream[, nodeids]]])
        → {progress}/{report} frames + final {rc, summary}  (or {stale})
('status',   fp)            → {ready: True, stale: bool}     # cheap probe
('shutdown',)               → {bye: True}; exit              # watcher-promote
('promote',  new_address)   → rebind onto a new address      # staging → canonical
```

`full_report` ships per-phase reports; `stream` makes the daemon stream them live (the `--fast` controller); `nodeids` restricts the run to a forwarded selection.

Pickle, but locked down: a `_SafeUnpickler` whitelists `builtins.*` only. Every frame — control messages *and* serialized reports — is `tuple`/`dict`/`list`/`str`/`int`/`float`/`bool`/`None`/`bytes`. A malicious local pickle into the socket can't escalate to code execution. Malformed frames (empty/short tuples, garbage, oversized headers) are tolerated, never fatal.

---

## vs `pytest-xdist`

| | `pytest-xdist -n N` | `pytest-fast` |
|---|---|---|
| Workers | N | N |
| App import, first run | N × full import | 1 × full import |
| App import, later runs | N × full import | **0** (warm daemon) |
| Collect, first run | N × collect | 1 × collect |
| Collect, later runs | N × collect | **0** (cached) |
| Source change → respawn | manual | **automatic** |
| Env change → respawn | manual | **automatic** (fingerprint) |
| `pytest_runtest_protocol` | yes | yes |
| Marks / skip / xfail / reruns | yes | yes |
| `pytest.ini` / `pyproject.toml` | yes | yes |
| Native reporting (junit / html / `--durations` / `-v`) | yes | **yes** via `pytest --fast`; lossy in the CLI runner |
| Test selection (`-k`/`-m`) | yes | yes (`--fast`); full-suite in the CLI runner |
| Remote / multi-host | yes (`--tx ssh=…`) | no (single host) |
| Cross-platform | win + posix | **POSIX only** |

If you need Windows or remote fan-out across machines — use xdist. If you spend 30 seconds re-importing your app graph every time you re-run a 5-second suite — `pytest-fast` is for you, and `pytest --fast` gives you xdist-grade reporting on top of it.

`pytest-xdist` lives in the optional `xdist-parity` dependency group (used only to cross-check behavior): `uv sync --group xdist-parity`.

---

## Configuration

`pytest-fast` is configured entirely via env vars — no config file.

| Variable | Default | Semantics | What it does |
|---|---|---|---|
| `PYTEST_FAST_ROOT` | `os.getcwd()` | path | Project root for the mtime scan. Override when launching outside the repo root. |
| `PYTEST_FAST_WATCH_DIRS` | `src,tests` | comma/colon, **REPLACE** | Dirs scanned recursively for `*.py` mtime. Flat layouts: `mypkg,tests`. Empty value scans no dirs. |
| `PYTEST_FAST_WATCH_FILES` | `pyproject.toml,pytest.ini` | comma/colon, **REPLACE** | Standalone config files in the mtime scan — add `setup.cfg`, `tox.ini`, root `conftest.py`, etc. |
| `PYTEST_FAST_MARK` | `""` | string | Marker expression, passed as `-m` during collection. |
| `PYTEST_ADDOPTS` | (inherited) | pytest opts | Standard pytest addopts. In the env fingerprint → a change forces respawn. |
| `PYTEST_FAST_ENV_PREFIXES` | `""` | comma-separated | Env-var prefixes whose change forces respawn. Mark your app config: `MYAPP_,FEATURE_`. |
| `PYTEST_FAST_ADDRESS` | (derived) | path | Daemon socket — used by **both** the CLI runner and `pytest --fast`. Prefer this over a bare `--fast-address` path (see the `--fast` caveat above). |
| `PYTEST_FAST_WORKERS` | (perf cores) | int | Worker count for both front-ends. |
| `PYTEST_FAST_TTL` | `600` | seconds | Daemon idle TTL for both front-ends. |
| `PYTEST_FAST_WATCH_POLL` | `0.5` | seconds | `--fast-watch` watcher: interval between source `max(mtime)` polls. Lower = snappier pre-warm, slightly more CPU. |
| `PYTEST_FAST_WATCH_DEBOUNCE` | `0.7` | seconds | `--fast-watch` watcher: quiet period after the last edit before it promotes a successor — one reboot per burst of edits. |
| `OUTCOME_DUMP` | `""` | path | With `pytest -p pytest_fast`, writes `{nodeid: outcome}` JSON on sessionfinish — a reference dump for outcome-diff against xdist. |

All listed variables are in the env fingerprint; changing any forces a fresh daemon (you never need to manually kill one).

```bash
# A FastAPI project: app/ + tests/ layout, SQLAlchemy + Pydantic, tox.ini
export PYTEST_FAST_WATCH_DIRS=app,tests
export PYTEST_FAST_WATCH_FILES=pyproject.toml,pytest.ini,tox.ini
export PYTEST_FAST_ENV_PREFIXES=APP_,DB_

# Now any of these triggers an automatic respawn:
#   - edit app/**/*.py or tests/**/*.py
#   - edit pyproject.toml / pytest.ini / tox.ini
#   - flip APP_DEBUG or DB_HOST
pytest --fast                # or: pytest-fast --address /tmp/myapp.sock --workers 6
```

### CLI flags (`pytest-fast`)

```
--address PATH       Unix socket of the resident daemon (or $PYTEST_FAST_ADDRESS)
--ttl SECONDS        Idle seconds before daemon self-shutdown (or $PYTEST_FAST_TTL; default 600)
--workers N          Parallel worker count (or $PYTEST_FAST_WORKERS; default: performance cores, >= 1)
--start-method M     spawn / forkserver / fork (default forkserver)
--full-report        Ship full per-phase reports → a real --durations table in the summary
--detailed           Extended parallelism diagnostics block (eff, CPU-vs-I/O, lost-time,
                     load balance, a deterministic worker-count hint) — see "Performance diagnostics"
--bench[=N]          Run N times (default 2; first dropped as warmup) → a deterministic bottleneck
                     report (shared-setup clusters, slowest CPU/IO calls + cProfile attribution,
                     the wall ceiling) instead of the run summary — what to optimize to go faster
--with-watcher       Spawn a pre-warm watcher alongside the daemon
--print-inferred-workers  Print the resolved worker count and exit (honors --workers /
                     $PYTEST_FAST_WORKERS / perf-core auto-detect) — for external tooling
                     sizing a per-worker pool to the run, without importing internals
--runs N             Local single-process mode (no daemon)
--persist-workers    Opt-in: reuse a warm worker pool across runs/requests so SESSION-scoped
                     fixtures are set up once, not re-paid per run. Trades cross-run isolation
                     for speed — for many-small-runs clients (a mutation tester issuing one run
                     per mutant, a fuzz loop, a watch-driven re-runner) with an expensive session fixture
--dump PATH          Local mode: write {nodeid: outcome} JSON
--serve / --watch    Internal (the daemon / watcher processes spawn themselves with these)
```

#### `--persist-workers` — amortize expensive session-scoped fixtures

By default every run forks fresh workers that exit on completion, so a `scope="session"` fixture
(a DB engine + schema seed, an app factory, a warmed cache) is set up **again on every run** — the
forkserver amortizes *collect*, not *fixtures*. A client that issues many small runs against the same
warm daemon (a mutation tester: one `run` per mutant; a fuzz loop; a watch-driven re-runner) re-pays
that setup every time, and it dominates wall-clock.

`--persist-workers` holds a warm worker pool across runs: each worker is forked once and its pytest
session spans every run, so session-scoped fixtures are set up **once** and reused. Function- and
module-scoped fixtures still tear down normally between items (isolation within a run is unchanged);
what's traded away is **cross-run** isolation — arbitrary global state a test mutates persists to the
next run — which is why it's opt-in. The saving is the whole session-fixture setup, per run: it scales
with how expensive that fixture is and how many runs you issue, so it's largest for a long stream of
small runs against a heavyweight session fixture (a DB engine + schema seed, an app factory).

---

## Extending pytest-fast (0.12+): wrappers & plugins

Three extension seams, one per process role. Each uses the mechanism native to where it runs: pytest's
own hook system where a pytest session exists (workers), plain importable modules where it doesn't
(the daemon), and the wire protocol for clients.

### Worker seam — custom per-test measurements on the bus

Workers run the **full pytest protocol**, so ordinary pytest hooks, hookwrappers and fixtures already
execute inside them — measure whatever you want the usual way. The missing piece was the channel to
ship a measurement back: implement pytest-fast's own hook (in `conftest.py` or any pytest plugin) and
write into `extra` — it rides the worker→master bus next to `duration`/`cpu` as `result["extra"]`:

```python
# conftest.py
def pytest_fast_annotate_result(item, result, extra):
    # result is read-only context: {"nodeid", "outcome", "duration", ...}
    extra["sql_queries"] = getattr(item, "_my_query_counter", 0)
```

Fired once per test in the worker (hot path — keep it cheap). Exceptions degrade to a stderr note in
the daemon log, never a failed test. Per-test **CPU time is already built in**: every lean run ships
`result["cpu"]` (`duration − cpu ≈ I/O wait`) — no plugin needed.

### Daemon seam — sections inside the summary box

The daemon holds every run's aggregated data but (by architecture) no pytest session — collection
lives in the forkserver preload. So daemon-side extensions are **plain modules**, named in
`PYTEST_FAST_DAEMON_PLUGINS` (comma-separated import paths):

```python
# myproj/timing_gate.py
def pytest_fast_run_completed(run_info):
    slow = [r for r in run_info["results"] if r["duration"] > 5.0]
    return [f"  ⏱ my-gate: {len(slow)} slow tests"]  # spliced into the box before the closing rule
```

`run_info` keys: `results` (list of `RunResult` — nodeid/outcome/duration/cpu/extra/…),
`worker_stats`, `bus`, `total`, `warmup`, `run_wall`, `num_workers`, `start_method`, `label`.
Failure policy: import errors / missing symbol / raising plugins become a visible `⚠` line in the
summary — never a crashed daemon, never a silently-missing gate. The env var participates in the
staleness fingerprint (changing the list respawns the warm daemon); keep plugin **code** under the
watched dirs (`PYTEST_FAST_WATCH_DIRS`) so editing it respawns too.

### Client seam — wrapper clients with full data access

Build your own front-end on two public calls: `run_via_daemon` (the ensure/stale-respawn
orchestration the CLI and the `--fast` plugin themselves use) and `request_run(want_results=True)`
(the final frame then carries `results`, `worker_stats`, `run_meta` — ~100 bytes/test, no full-report
bus cost):

```python
from pytest_fast import request_run, resolve_workers, run_via_daemon

frame = run_via_daemon(
    resolve_workers(None), "forkserver", "/tmp/my.sock", ttl=600, with_watcher=False,
    run=lambda addr: request_run(addr, want_results=True),
)
for r in frame.get("results", []):   # keys are optional on daemons predating 0.12
    ...                              # r["nodeid"], r["duration"], r["cpu"], r.get("extra")
print(frame["summary"])              # the pretty box — post-process/splice as you like
raise SystemExit(int(frame.get("rc", 1)))
```

Protocol compatibility contract: run-request tuples grow positionally (older daemons ignore trailing
elements), reply frames are dicts (treat unknown keys as optional). A streaming variant with the same
orchestration: `request_run_streamed(addr, on_report=...)` — every per-phase report as it happens.

---

## Limitations

- **POSIX only.** `fcntl`, `AF_UNIX`, `multiprocessing.forkserver` are required; the package imports `fcntl` at the top, so Windows fails on import. Use xdist on Windows.
- **CLI-runner reports are lossy.** The `pytest-fast --address` summary is a bespoke render (counts, tracebacks, durations). For full `--junitxml`/`--html`/plugin-grade reporting, use **`pytest --fast`**, which is natively pytest.
- **`--fast` selection caveat.** `-k`/`-m` and full runs are forwarded to the daemon; explicit path/nodeid args can mismatch on rootdir-derived nodeids and are rejected with a clear error (run them without `--fast`).
- **macOS fork safety.** Code resolving hostnames via `getaddrinfo("localhost")` inside a fork can segfault (mDNS/CoreFoundation). Pre-resolve to numeric IPs in your config; pytest-fast doesn't auto-rewrite.
- **Single host.** No remote workers. For fan-out across machines, use xdist + ssh.

---

## Development

```bash
git clone https://github.com/prostomarkeloff/pytest-fast
cd pytest-fast
uv sync

make lint-heavy     # ruff format + ruff check --fix + pyright
make test-full      # run pytest-fast's own tests through pytest-fast (dogfood)
make test-stress    # opt-in fuzz + stress tier (Hypothesis), via plain pytest
make test-parity    # opt-in differential parity tier (engine vs plain pytest vs xdist)
make fuzz           # Atheris coverage-guided fuzzing of the wire decoder (needs: make fuzz-install)
```

The unit suite covers the bus protocol & malformed-frame robustness, env fingerprint & watch-root parsing, daemon lifecycle (spawn / status / run / shutdown / idle-ttl), the watcher (flock single-instance / promote / no-promote on broken collect), full-report wire format, the `pytest --fast` plugin (native output, selection forwarding, inert-without-`--fast`), and CLI smoke.

On top of that, an **opt-in fuzz + stress tier** (`@pytest.mark.fuzz` / `@pytest.mark.stress`, [Hypothesis](https://hypothesis.readthedocs.io)-driven, excluded from the default run) hammers the attack surface: property-based fuzzing of the wire codec (round-trip, arbitrary-bytes robustness, the `_SafeUnpickler` whitelist / RCE resistance, fragmentation, the oversized-frame guard, decode-amplification / memo / class-as-value rejection); fuzzing of the pure aggregation helpers (`categorize`, the `--durations` table, env/fingerprint parsing); and live-daemon stress — Hypothesis-generated frame storms, a slowloris idle-connection check, a connection flood, mid-run worker-crash → UNTRUSTED accounting, and fd-leak hygiene across many runs. Run it with `make test-stress` (sets `HYPOTHESIS_PROFILE=ci` → derandomized, 300 examples/property).

There's also a **differential parity tier** (`@pytest.mark.parity`, `make test-parity`): it generates synthetic suites spanning every outcome (pass / fail / error / skip / xfail / xpass / parametrize) and runs each three ways — **plain pytest (the oracle) ↔ the pytest-fast engine ↔ pytest-xdist** — asserting the `{nodeid: outcome}` maps match exactly (Hypothesis shrinks any divergence to a minimal suite). This is the guard against the warm-forkserver engine ever drifting from a normal pytest session. Needs the `xdist-parity` group.

Deeper still, **`make fuzz`** runs [Atheris](https://github.com/google/atheris) (coverage-guided, libFuzzer-backed) against the pickle decoder — see [`fuzz/`](fuzz/). It already surfaced (and pinned regressions for) a decode-amplification OOM, a memo-index pre-allocation bomb, a whitelist class-as-value escape, and a memo-DAG traversal blow-up. Crashers it finds are curated into `fuzz/corpus/` and replayed on every PR by `tests/test_fuzz_corpus.py` — no Atheris required for the replay.

CI runs lint, an `os: [ubuntu, macos, windows] × python: [3.11–3.14]` matrix for the unit suite (skipped on Windows — POSIX only), the fuzz + stress and parity tiers as dedicated `os: [ubuntu, macos]` jobs, and a nightly/​dispatch Atheris fuzzing job on Linux.

---

<div align="center">

**Stop re-importing your app. Start running your tests.**

Made with 🪓 by [@prostomarkeloff](https://github.com/prostomarkeloff)

</div>
