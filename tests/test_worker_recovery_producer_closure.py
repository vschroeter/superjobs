"""Deterministic checks for worker recovery producer observation closure."""

from __future__ import annotations

import asyncio
import importlib.util
import sys
from pathlib import Path
from types import ModuleType, SimpleNamespace

import pytest

from superjobs import JobCompleted, JobState, JobSucceeded, ObservationExpiredError

SUPPORT_DIR = Path(__file__).resolve().parents[1] / "tools" / "worker_recovery_support"
_PRODUCER_MODULE_NAME = "_issue21_worker_recovery_producer_closure"
_GENERIC_SUPPORT_MODULE_NAMES = (
    "protocol",
    "queue_config",
    "runtime_isolation",
    "scenario_jobs",
)


@pytest.fixture
def producer_module(monkeypatch: pytest.MonkeyPatch) -> ModuleType:
    stashed_support = {
        name: sys.modules.pop(name, None) for name in _GENERIC_SUPPORT_MODULE_NAMES
    }
    stashed_producer = sys.modules.pop(_PRODUCER_MODULE_NAME, None)
    touched: set[str] = set()

    monkeypatch.syspath_prepend(str(SUPPORT_DIR))
    producer_path = SUPPORT_DIR / "producer_app.py"
    spec = importlib.util.spec_from_file_location(_PRODUCER_MODULE_NAME, producer_path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"cannot load producer module from {producer_path}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[_PRODUCER_MODULE_NAME] = module
    touched.add(_PRODUCER_MODULE_NAME)
    spec.loader.exec_module(module)
    for name in _GENERIC_SUPPORT_MODULE_NAMES:
        if name in sys.modules:
            touched.add(name)
    try:
        yield module
    finally:
        for name in touched:
            sys.modules.pop(name, None)
        for name, previous in stashed_support.items():
            if previous is not None:
                sys.modules[name] = previous
        if stashed_producer is not None:
            sys.modules[_PRODUCER_MODULE_NAME] = stashed_producer


class _Event:
    def __init__(self, sequence: int, data: object) -> None:
        self.sequence = sequence
        self.data = data


class _ObservationHandle:
    def __init__(self, events: list[_Event]) -> None:
        self._events = events

    def events(self, after=None):
        if after is None:
            return self._stream(self._events)

        tail = [event for event in self._events if event.sequence > after.sequence]
        return self._stream(tail)

    async def _stream(self, events: list[_Event]):
        for event in events:
            yield event


def test_strictly_increasing_sequences_accepts_multiple_observations(
    producer_module: ModuleType,
) -> None:
    events = [_Event(1, object()), _Event(2, object()), _Event(5, object())]
    producer_module._assert_strictly_increasing_sequences(events)


def test_strictly_increasing_sequences_rejects_equal_adjacent(
    producer_module: ModuleType,
) -> None:
    events = [_Event(1, object()), _Event(1, object())]
    with pytest.raises(AssertionError, match="strictly"):
        producer_module._assert_strictly_increasing_sequences(events)


def test_strictly_increasing_sequences_rejects_descending(
    producer_module: ModuleType,
) -> None:
    events = [_Event(3, object()), _Event(2, object())]
    with pytest.raises(AssertionError, match="strictly"):
        producer_module._assert_strictly_increasing_sequences(events)


@pytest.mark.asyncio
async def test_observation_closure_replays_multiple_retained_events(
    producer_module: ModuleType,
) -> None:
    terminal = JobCompleted()
    events = [
        _Event(10, JobSucceeded(result="x")),
        _Event(11, terminal),
    ]
    handle = _ObservationHandle(events)
    wrapped = await producer_module._assert_observation_closure(handle, timeout=1.0)
    assert wrapped == events


@pytest.mark.asyncio
async def test_observation_closure_replays_terminal_only_with_empty_tail(
    producer_module: ModuleType,
) -> None:
    terminal = JobCompleted()
    events = [_Event(42, terminal)]
    handle = _ObservationHandle(events)
    wrapped = await producer_module._assert_observation_closure(handle, timeout=1.0)
    assert wrapped == events


@pytest.mark.asyncio
async def test_observation_closure_rejects_mismatching_replay_payload(
    producer_module: ModuleType,
) -> None:
    terminal = JobCompleted()
    events = [
        _Event(10, JobSucceeded(result="x")),
        _Event(11, terminal),
    ]
    handle = _BadReplayPayloadHandle(events)
    with pytest.raises(AssertionError, match="unexpected observation payloads"):
        await producer_module._assert_observation_closure(handle, timeout=1.0)


@pytest.mark.asyncio
async def test_observation_closure_propagates_retention_or_cursor_error(
    producer_module: ModuleType,
) -> None:
    terminal = JobCompleted()
    events = [
        _Event(10, JobSucceeded(result="x")),
        _Event(11, terminal),
    ]
    expired = ObservationExpiredError("The requested observation cursor has expired")
    handle = _ExpiredOnReplayHandle(events, expired)
    with pytest.raises(ObservationExpiredError) as exc_info:
        await producer_module._assert_observation_closure(handle, timeout=1.0)
    assert exc_info.value is expired


@pytest.mark.asyncio
async def test_observation_closure_times_out_on_hanging_event_stream(
    producer_module: ModuleType,
) -> None:
    release = asyncio.Event()

    class _HangingHandle:
        def events(self, after=None):
            return _hanging_stream(release)

    async def _hanging_stream(_release: asyncio.Event):
        await _release.wait()
        yield _Event(1, JobCompleted())

    with pytest.raises(TimeoutError):
        await producer_module._assert_observation_closure(
            _HangingHandle(),
            timeout=0.05,
        )


def test_retry_scenario_uses_thirty_second_recover_cap(producer_module: ModuleType) -> None:
    assert (
        producer_module._recover_timeout_for_scenario("recovery_after_retry_publication")
        == 30.0
    )
    assert (
        producer_module._phase_timeout_seconds("recovery_after_retry_publication", "recover")
        == 30.0
    )


def test_baseline_scenarios_keep_forty_five_second_recover_defaults(
    producer_module: ModuleType,
) -> None:
    assert producer_module._recover_timeout_for_scenario("recovery_before_completion") == 45.0
    assert (
        producer_module._phase_timeout_seconds("recovery_before_completion", "recover") == 50.0
    )


@pytest.mark.asyncio
async def test_assert_result_and_outcome_checks_completed_status_after_result(
    producer_module: ModuleType,
) -> None:
    calls: list[str] = []

    class _Handle:
        async def result(self, *, wait_timeout: float):
            calls.append("result")
            return "ok"

        async def status(self):
            calls.append("status")
            return SimpleNamespace(state=JobState.COMPLETED)

        async def outcome(self, *, wait_timeout: float):
            calls.append("outcome")
            return JobSucceeded(result="ok")

    await producer_module._assert_result_and_succeeded_outcome(
        _Handle(),
        "ok",
        timeout=1.0,
        assert_completed_status=True,
    )
    assert calls == ["result", "status", "outcome"]


class _BadReplayPayloadHandle:
    def __init__(self, events: list[_Event]) -> None:
        self._events = events

    def events(self, after=None):
        if after is None:
            return self._stream(self._events)
        tail = [event for event in self._events if event.sequence > after.sequence]
        if tail:
            wrong = _Event(tail[0].sequence, JobSucceeded(result="wrong"))
            return self._stream([wrong])
        return self._stream([])

    async def _stream(self, events: list[_Event]):
        for event in events:
            yield event


class _ExpiredOnReplayHandle:
    def __init__(self, events: list[_Event], expired: ObservationExpiredError) -> None:
        self._events = events
        self._expired = expired

    def events(self, after=None):
        if after is None:
            return self._stream(self._events)
        return self._raise_expired()

    async def _stream(self, events: list[_Event]):
        for event in events:
            yield event

    async def _raise_expired(self):
        raise self._expired
        yield  # pragma: no cover
