"""Fuzz the pure, in-process helpers — no sockets, no daemon, fully deterministic.

Targets the parsing/aggregation logic that ingests *attacker- or plugin-influenced*
data and must never crash or mis-bucket:

  * `categorize`        — outcome bucketing over third-party `pytest_report_teststatus`
                          categories (unknown/empty/"rerun" must not win).
  * `_durations_lines`  — the `--durations` table built from serialized phase reports
                          with arbitrary (possibly malformed) duration/when/nodeid fields.
  * env parsing         — `_split_env_list` / `_env_fingerprint` over arbitrary unicode.
  * `_daemon_log_path`  — address→logfile mapping invariants.
"""

from __future__ import annotations

import contextlib
import math
import os
from typing import TYPE_CHECKING, cast

import pytest
from hypothesis import assume, given
from hypothesis import strategies as st

from pytest_fast import (
    _FINGERPRINT_KEYS,
    _daemon_log_path,
    _durations_lines,
    _env_fingerprint,
    _split_env_list,
    categorize,
)

if TYPE_CHECKING:
    from collections.abc import Iterator

    from _pytest.config import Config
    from _pytest.reports import TestReport

    from pytest_fast import RunResult

pytestmark = pytest.mark.fuzz

_KNOWN_OUTCOMES = {"error", "failed", "xpassed", "xfailed", "skipped", "passed"}


# ── categorize: never crashes, never returns an unknown bucket ───────────────────


class _FakeHook:
    """Serves one preset category per `pytest_report_teststatus` call — stands in for
    the real hook (incl. third-party plugins that may return unknown categories)."""

    def __init__(self, cats: list[str]) -> None:
        self._cats = cats
        self._i = 0

    def pytest_report_teststatus(self, report: object, config: object) -> tuple[str, str, str]:
        cat = self._cats[self._i]
        self._i += 1
        return cat, "", ""


class _FakeConfig:
    def __init__(self, cats: list[str]) -> None:
        self.hook = _FakeHook(cats)


# Categories the hook might emit: the known set, plus the noise that must be ignored.
_category = st.sampled_from(sorted(_KNOWN_OUTCOMES)) | st.just("rerun") | st.just("") | st.text()


@given(cats=st.lists(_category, max_size=8))
def test_categorize_only_ever_returns_a_known_bucket(cats: list[str]) -> None:
    config = cast("Config", _FakeConfig(cats))
    reports = cast("list[TestReport]", [object() for _ in cats])
    assert categorize(config, reports) in _KNOWN_OUTCOMES


# Noise = strings categorize must IGNORE. `st.text()` can randomly produce a KNOWN
# outcome (hypothesis found `noise=['failed']` around `known='passed'` — and 'failed'
# legitimately outranks 'passed', which is categorize working as designed, not noise
# being ignored). The invariant's domain is genuinely-unknown strings — filter, don't
# weaken the assert.
_noise = st.text().filter(lambda s: s not in _KNOWN_OUTCOMES) | st.just("rerun") | st.just("")


@given(known=st.sampled_from(sorted(_KNOWN_OUTCOMES)), noise=st.lists(_noise))
def test_categorize_ignores_noise_around_a_known_category(known: str, noise: list[str]) -> None:
    """A single legitimate category surrounded by unknown/empty/'rerun' noise must
    still produce a real bucket — never an unknown string and never 'passed' demotion
    when a more-significant known category is present."""
    cats = [*noise, known]
    config = cast("Config", _FakeConfig(cats))
    reports = cast("list[TestReport]", [object() for _ in cats])
    result = categorize(config, reports)
    assert result in _KNOWN_OUTCOMES
    # `known` (the only recognized one) should win over all the noise.
    assert result == known


def test_categorize_significance_beats_order() -> None:
    """Pin the edge hypothesis surfaced: two KNOWN categories are NOT noise-vs-known —
    the more significant one wins regardless of position ('failed' over 'passed')."""
    config = cast("Config", _FakeConfig(["failed", "passed"]))
    reports = cast("list[TestReport]", [object(), object()])
    assert categorize(config, reports) == "failed"


# ── _durations_lines: robust against malformed serialized reports ────────────────

# A phase report dict with deliberately under-specified / wrong-typed fields.
_report_dict = st.fixed_dictionaries(
    {},
    optional={
        "duration": st.floats() | st.integers() | st.text() | st.none() | st.booleans(),
        "when": st.sampled_from(["setup", "call", "teardown"]) | st.integers() | st.none(),
        "nodeid": st.text() | st.integers() | st.none(),
    },
)
_run_result = st.fixed_dictionaries(
    {"nodeid": st.text(), "outcome": st.sampled_from(["passed", "failed"]), "duration": st.floats(allow_nan=False)},
    optional={"reports": st.lists(_report_dict, max_size=6)},
)


@given(results=st.lists(_run_result, max_size=12), limit=st.integers(min_value=0, max_value=20))
def test_durations_lines_never_crashes(results: list[RunResult], limit: int) -> None:
    out = _durations_lines(results, limit=limit)
    assert isinstance(out, list)
    assert all(isinstance(x, str) for x in out)
    if out:
        assert out[0].startswith("  DURATIONS")
        assert len(out) - 1 <= limit  # body lines never exceed the limit


# Well-formed reports whose durations include NaN/inf — the table must still emit the
# finite ones in correct descending order (NaN poisons `list.sort()` if not filtered).
_finite_or_special = (
    st.floats(min_value=0.0, max_value=9999.0, allow_nan=False, allow_infinity=False)
    | st.just(float("nan"))
    | st.just(float("inf"))
)


@given(durations=st.lists(_finite_or_special, max_size=20))
def test_durations_lines_sorted_descending_ignoring_nan(durations: list[float]) -> None:
    results: list[RunResult] = [
        cast("RunResult", {"reports": [{"duration": d, "when": "call", "nodeid": f"t::x{i}"}]})
        for i, d in enumerate(durations)
    ]
    out = _durations_lines(results, limit=50, min_dur=0.005)
    body = out[1:] if out else []
    # The formatted field is `{dur:8.3f}` right after a 4-space indent → cols [4:12]
    # parse back as the duration (covers "     inf" too).
    shown = [float(line[4:12]) for line in body]
    assert shown == sorted(shown, reverse=True), f"durations table is not sorted desc: {shown}"
    assert all(d >= 0.005 for d in shown)
    assert not any(math.isnan(d) for d in shown), "a NaN duration leaked into the table"


# ── env parsing fuzz ─────────────────────────────────────────────────────────────

# Realistic env-value text. Two OS constraints, both mirrored here:
#   * no NUL — the C env API is NUL-terminated; os.environ raises ValueError on it. (This
#     is also why the fingerprint's "\0"-join separator is collision-safe by construction.)
#   * no lone surrogates beyond the surrogateescape range — os.environ can't encode them
#     (`codec="utf-8"` excludes all surrogates). Surrogateescape values (\udc80–\udcff,
#     from non-UTF-8 byte env vars) ARE realizable and get dedicated coverage below.
_env_text = st.text(alphabet=st.characters(codec="utf-8", exclude_characters="\x00"))


@contextlib.contextmanager
def _env(values: dict[str, str | None]) -> Iterator[None]:
    """Set (str) / clear (None) env vars for the block, restoring originals after.
    Self-contained — unlike the function-scoped `monkeypatch` fixture, this composes
    with `@given`, which does NOT reset fixtures between generated examples."""
    saved = {k: os.environ.get(k) for k in values}
    try:
        for k, v in values.items():
            if v is None:
                os.environ.pop(k, None)
            else:
                os.environ[k] = v
        yield
    finally:
        for k, old in saved.items():
            if old is None:
                os.environ.pop(k, None)
            else:
                os.environ[k] = old


@given(raw=_env_text, default=st.lists(st.text(), max_size=4))
def test_split_env_list_is_clean(raw: str, default: list[str]) -> None:
    with _env({"PYTEST_FAST_FUZZ_LIST": raw}):
        out = _split_env_list("PYTEST_FAST_FUZZ_LIST", default)
    assert all(item == item.strip() for item in out), "entries must be stripped"
    assert all(item for item in out), "no empty entries"
    assert "," not in "".join(out) and ":" not in "".join(out)  # both are separators


@given(default=st.lists(st.text(), min_size=1, max_size=4))
def test_split_env_list_unset_returns_default(default: list[str]) -> None:
    with _env({"PYTEST_FAST_FUZZ_LIST": None}):
        assert _split_env_list("PYTEST_FAST_FUZZ_LIST", default) == default


@given(values=st.fixed_dictionaries({k: st.none() | _env_text for k in _FINGERPRINT_KEYS}))
def test_env_fingerprint_is_deterministic(values: dict[str, str | None]) -> None:
    """Same env → same fingerprint, every time. Arbitrary (non-NUL) unicode in the
    relevant vars must not crash the hash and must produce a stable 40-char SHA-1."""
    with _env(values):
        fp1 = _env_fingerprint()
        fp2 = _env_fingerprint()
    assert fp1 == fp2
    assert len(fp1) == 40 and all(c in "0123456789abcdef" for c in fp1)


@given(key=st.sampled_from(_FINGERPRINT_KEYS), a=_env_text, b=_env_text)
def test_env_fingerprint_changes_when_a_relevant_var_changes(key: str, a: str, b: str) -> None:
    assume(a != b)
    # Pin every other relevant key to absent so only `key` varies between the two hashes.
    cleared: dict[str, str | None] = dict.fromkeys(_FINGERPRINT_KEYS, None)
    with _env({**cleared, key: a}):
        fp_a = _env_fingerprint()
    with _env({**cleared, key: b}):
        fp_b = _env_fingerprint()
    assert fp_a != fp_b


def test_env_fingerprint_handles_non_utf8_value() -> None:
    """A fingerprinted var can hold surrogateescape chars — a non-UTF-8 *byte* env value
    (e.g. a latin-1 path in an app var matched by PYTEST_FAST_ENV_PREFIXES) is decoded
    into os.environ as \\udc80–\\udcff. `_env_fingerprint` must hash it, not crash on a
    strict `str.encode` (this runs on every client request — a crash here breaks all runs)."""
    with _env({"PYTEST_FAST_MARK": "\udc80\udcfe-value"}):
        fp = _env_fingerprint()
    assert len(fp) == 40 and all(c in "0123456789abcdef" for c in fp)


# ── _daemon_log_path invariants ──────────────────────────────────────────────────


@given(stem=st.text(alphabet=st.characters(exclude_characters="\x00/"), min_size=1, max_size=40))
def test_daemon_log_path_staging_distinction(stem: str) -> None:
    canonical = _daemon_log_path(f"/tmp/{stem}.sock")
    staging = _daemon_log_path(f"/tmp/{stem}.sock.staging")
    assert str(canonical).endswith("-daemon.log")
    assert str(staging).endswith("-daemon.staging.log")
    assert canonical != staging  # the two incarnations never share a log file
