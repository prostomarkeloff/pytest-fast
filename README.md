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

- **`pytest --fast`** — a pytest plugin. Your `pytest` invocation stays a real pytest session, so reporting is **100% native** (terminal, `--durations`, `-v/-s`, `--junitxml`, plugins, exit codes) — the warm daemon just does the execution.
- **`pytest-fast --address …`** — a standalone CLI. A thin client triggers the daemon and prints a lean, bespoke summary. Lowest overhead; great for tight TDD loops and CI.

```bash
uv add git+https://github.com/prostomarkeloff/pytest-fast.git
```

```bash
pytest --fast                 # native pytest output, warm forkserver execution
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

The warm forkserver + work-stealing bus is shared. The only difference is **who renders the report**:

- **CLI runner** — the daemon itself renders the bespoke summary and the thin client prints it. Lowest overhead.
- **`--fast` plugin** — the daemon streams full, serialized per-phase reports; the pytest controller republishes them into its own real `terminalreporter`. Because the daemon is resident, the controller's config/reporter cost is paid once and amortized — so you get native reporting *and* warmth, which a per-run cold controller (xdist) cannot.

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
--with-watcher       Spawn a pre-warm watcher alongside the daemon
--runs N             Local single-process mode (no daemon)
--dump PATH          Local mode: write {nodeid: outcome} JSON
--serve / --watch    Internal (the daemon / watcher processes spawn themselves with these)
```

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
```

The suite covers the bus protocol & malformed-frame robustness, env fingerprint & watch-root parsing, daemon lifecycle (spawn / status / run / shutdown / idle-ttl), the watcher (flock single-instance / promote / no-promote on broken collect), full-report wire format, the `pytest --fast` plugin (native output, selection forwarding, inert-without-`--fast`), and CLI smoke. CI runs lint plus an `os: [ubuntu, macos, windows] × python: [3.11–3.14]` matrix (tests are skipped on Windows — POSIX only).

---

<div align="center">

**Stop re-importing your app. Start running your tests.**

Made with 🪓 by [@prostomarkeloff](https://github.com/prostomarkeloff)

</div>
