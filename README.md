<div align="center">

# pytest-fast

**Collect once. Fork warm. Skip the cold start.**

[![Python 3.11+](https://img.shields.io/badge/python-3.11+-blue.svg)](https://www.python.org/downloads/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![Types: pyright](https://img.shields.io/badge/types-pyright-blue)](https://github.com/microsoft/pyright)
[![Lint: ruff](https://img.shields.io/badge/lint-ruff-orange.svg)](https://github.com/astral-sh/ruff)

</div>

A resident `forkserver`-based pytest accelerator. The first run boots a daemon that imports your app graph and collects tests **once**. Every subsequent run forks warm workers — collect-free, import-free, ready in milliseconds. Edit a file? The daemon notices and re-collects automatically.

Drop-in alternative to `pytest-xdist` for the common case (`-n N` parallel workers). Same set of tests, same `marks`/`skip`/`xfail`/`reruns` behavior — they run through the **full** pytest protocol (`pytest_runtest_protocol`), not a custom executor.

```bash
uv add git+https://github.com/prostomarkeloff/pytest-fast.git
```

```bash
pytest-fast --address /tmp/myproj.sock --workers 6
#                                                ^ first run: boot (~3s) + run
#                                                  subsequent runs: just fork()+run
```

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

---

## What you actually run

```bash
# One-shot: connect to (or spawn) a resident daemon, run all tests, print summary
pytest-fast --address /tmp/myproj.sock --workers 6

# Same again — the daemon is already warm
pytest-fast --address /tmp/myproj.sock --workers 6
#   → warm fork + run, no collect

# Local single-process mode (no resident daemon, useful for CI smoke)
pytest-fast --runs 1 --workers 4

# Pre-warm: a background watcher polls source mtimes and refreshes the daemon
# BEFORE you re-run, so even after an edit the next run is still warm
pytest-fast --address /tmp/myproj.sock --workers 6 --with-watcher
```

Output:

```
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
      def test_invalid_token():
      >       assert client.post("/login").status_code == 401
      E       assert 200 == 401
      tests/api/test_auth.py:42: AssertionError
  SLOWEST (≥1s, top 3):
       2.13s  tests/integration/test_payment_flow.py::test_full_purchase
       1.84s  tests/db/test_migrations.py::test_full_upgrade
       1.21s  tests/api/test_search.py::test_complex_filter
══════════════════════════════════════════════════════════════════
```

`par. 5.21x of 6` is the actual parallelism — total worker-busy time divided by wall. The closer to N, the better the work-stealing dispatcher kept your workers busy.

---

## How it works

### One process imports your code; many fork off of it

```
                     ┌──────────────────────────────────────┐
client ──────────►   │  DAEMON (main process)               │
make test-full       │  - forkserver context                │
                     │  - control socket (run/status/...)   │
                     └──────────────────────────────────────┘
                                  │
                                  │ first Process.start()
                                  ▼
                     ┌──────────────────────────────────────┐
                     │  FORKSERVER (one process, preloaded) │
                     │  - imports `pytest_fast` ONCE        │
                     │  - runs `_collect()` ONCE            │
                     │  - holds: items[], config            │
                     └──────────────────────────────────────┘
                                  │ fork() per worker per run
                                  ▼
       ┌──────────────┬──────────────┬──────────────┐
       ▼              ▼              ▼              ▼
  WORKER 0       WORKER 1       WORKER 2       WORKER 3
  inherits       inherits       inherits       inherits
  items+config   items+config   items+config   items+config
  ↓              ↓              ↓              ↓
  pulls test idx from master via Unix socket (work-stealing)
  runs pytest_runtest_protocol(item, nextitem)
  ships RunResult back
```

The `forkserver` is Python's stdlib `multiprocessing` start method that holds a single clean, preloaded process and forks workers out of it on demand. We set `set_forkserver_preload(["pytest_fast"])`, which imports our package — and at the bottom of `__init__.py`, that import triggers `_collect()`. The forkserver process now holds the collected items in its heap.

Each worker is a `fork()` out of the forkserver — **copy-on-write**, so the test items and config aren't re-allocated, just referenced. `gc.freeze()` after collect moves everything into the permanent generation so GC doesn't dirty the COW pages.

### Stale detection — two axes

A warm daemon is wrong if:

1. **Source files changed** — `max(mtime)` of `src/`, `tests/`, `pyproject.toml`, `pytest.ini` (plus anything in `PYTEST_FAST_WATCH`) is compared against the snapshot taken at boot. Implemented via early-exit `_any_source_newer(threshold)` so on large repos a single newer file short-circuits the scan.

2. **Relevant env changed** — `PYTEST_ADDOPTS`, `PYTEST_FAST_*`, and any prefix you list in `PYTEST_FAST_ENV_PREFIXES` (e.g. `MYAPP_,FEATURE_`) are hashed into an env fingerprint. The client sends its current fp on every request; the daemon compares against its boot fp.

On mismatch, the daemon replies `{stale: True}` and exits. The client (`_ensure_and_run`) catches that, coordinates a respawn under a `flock`, and reconnects — invisible to the user except for one "restarting daemon" line in stderr.

### Watcher (`--with-watcher`)

Optional, opt-in. A background poll loop watches the same source set and, on a debounced change, spawns a successor daemon on a `*.staging` socket. Once the successor is ready (collect succeeded), it cleanly shuts down the old one and rebinds onto the canonical address. **The next user `run` finds a warm-and-fresh daemon instead of paying the boot cost on the critical path.**

If the staging successor fails (broken edit, conftest error), the old daemon stays untouched.

### Control protocol

One length-prefixed pickle message per connection, serialized through the daemon's `accept()` loop — so an active test run is **never** interrupted by a control command. Four commands:

```python
('run',      fingerprint)  → stream {progress} frames + final {rc, summary} (or {stale})
('status',   fingerprint)  → {ready: True, stale: bool}     # cheap probe
('shutdown',)              → {bye: True}; exit              # used by watcher-promote
('promote',  new_address)  → rebind onto a new address      # staging → canonical
```

Pickle, but locked down: a `_SafeUnpickler` whitelists `builtins.*` only. Our wire protocol carries `tuple`/`dict`/`str`/`int`/`bool`/`None`/`bytes` — nothing else. A malicious local pickle into the socket can't escalate to code execution.

---

## Drop-in vs xdist

| | `pytest-xdist -n N` | `pytest-fast` |
|---|---|---|
| Workers | N | N |
| App import on first run | N × full import | 1 × full import |
| App import on subsequent runs | N × full import | 0 (warm daemon) |
| Collect on first run | N × collect | 1 × collect |
| Collect on subsequent runs | N × collect | 0 (cached) |
| Source change → respawn | manual | automatic |
| Env change → respawn | manual | automatic (via fingerprint) |
| `pytest_runtest_protocol` | yes | yes |
| Marks / skip / xfail / reruns | yes | yes |
| `pytest.ini` / `pyproject.toml` config | yes | yes |
| Custom report plugins (junit, html) | yes | **lossy** (text summary only) |
| Cross-platform | win + posix | **POSIX only** (uses forkserver/AF_UNIX/fcntl) |

If you need junit XML, allure, html reports, or you run on Windows — stay on xdist. If you spend 30 seconds re-importing your app graph every time you re-run a 5-second test suite — `pytest-fast` is for you.

---

## Configuration

All env-driven. No config file.

| Env | Default | What |
|---|---|---|
| `PYTEST_FAST_MARK` | `""` (no filter) | Marker expression passed to pytest as `-m` |
| `PYTEST_ADDOPTS` | (inherited) | Standard pytest addopts; in the fingerprint |
| `PYTEST_FAST_ROOT` | `os.getcwd()` | Root for source-mtime scanning (override for non-cwd projects) |
| `PYTEST_FAST_WATCH` | `src,tests` | Comma/colon-separated dirs to add to the mtime scan |
| `PYTEST_FAST_ENV_PREFIXES` | `""` | Comma-separated env-var prefixes whose change must force respawn (e.g. `MYAPP_,FEATURE_`) |
| `OUTCOME_DUMP` | `""` | Path to write `{nodeid: outcome}` JSON (plugin mode, for outcome-diff vs xdist) |

CLI flags:

```
--address PATH       Unix socket of the resident daemon (caller picks the path)
--ttl SECONDS        Idle seconds before daemon self-shutdown (default 600)
--workers N          Parallel worker count (default 6)
--start-method M     spawn / forkserver / fork (default forkserver)
--serve              Be the resident daemon (internal — clients trigger this)
--watch              Be the source watcher (internal — `--with-watcher` triggers this)
--with-watcher       Spawn a pre-warm watcher alongside the daemon
--runs N             Local single-process mode (no daemon)
--dump PATH          Local mode: write {nodeid: outcome} JSON
```

---

## Limitations

- **POSIX only.** `fcntl`, `AF_UNIX`, `multiprocessing.forkserver` are required. The package imports `fcntl` at the top — Windows fails on import. The Windows CI matrix is `continue-on-error` until someone ports the watcher/flock/socket bits.
- **Reports are lossy.** Failure tracebacks, captured stdout/stderr/log, slowest tests, and outcome counts are preserved. Full `TestReport` objects don't survive the pickle wire — so xdist-style `--junitxml`/`--html` plugins won't see what they expect. The reference `OUTCOME_DUMP` plugin mode exists for outcome-diff with xdist.
- **macOS fork safety.** Code that resolves hostnames via `getaddrinfo("localhost")` inside a fork will segfault (mDNS/CoreFoundation init). Pre-resolve to numeric IPs in your config. pytest-fast doesn't auto-rewrite.
- **Single host.** No remote workers. If you need fan-out across machines, use xdist + ssh.

---

## Development

```bash
git clone https://github.com/prostomarkeloff/pytest-fast
cd pytest-fast
uv sync

# Lint (ruff format + ruff check --fix + pyright)
make lint-heavy

# Run pytest-fast's own tests through pytest-fast (dogfood)
make test-full
```

The test suite is **40 tests** covering the bus protocol, env fingerprint, watch-root parsing, daemon lifecycle (spawn / status / run / shutdown / idle-ttl), watcher (flock single-instance / promote on source change / no-promote on broken collect), and CLI smoke. CI runs the lint + a `os: [ubuntu, macos, windows] × python: [3.11, 3.12, 3.13, 3.14]` matrix.

---

<div align="center">

**Stop re-importing your app. Start running your tests.**

Made with 🪓 by [@prostomarkeloff](https://github.com/prostomarkeloff)

</div>
