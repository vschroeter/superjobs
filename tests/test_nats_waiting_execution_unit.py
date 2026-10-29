from __future__ import annotations

import time
from datetime import UTC, datetime
from unittest.mock import AsyncMock, MagicMock

import pytest
from nats.js.errors import APIError, NotFoundError

from superjobs.jobs.execution import JobState
from superjobs.jobs.job_identity import JobIdentity
from superjobs.resources import WaitingExecutionCompleteness
from superjobs.resources.errors import WaitingExecutionUnavailableError
from superjobs.transport.backend import ExecutionRecord
from superjobs.transport.nats_backend import NatsJobBackend


def _bucket_status(*, messages: int) -> MagicMock:
    status = MagicMock()
    status.values = messages
    return status


def _waiting_record(execution_id: str) -> ExecutionRecord:
    evaluated_at = datetime.now(tz=UTC)
    return ExecutionRecord(
        identity=JobIdentity(name="tests.waiting", version="v1"),
        job_id=execution_id,
        request_payload=b"",
        request_media_type=None,
        fingerprint="fp",
        idempotency_key=f"key-{execution_id}",
        caller_scope="default",
        timeout=None,
        deadline=None,
        created_at=evaluated_at,
        state=JobState.WAITING_FOR_RESOURCE,
    )


def _wire_record(record: ExecutionRecord) -> bytes:
    from superjobs.transport import nats_backend as backend_module

    payload = backend_module._pack(
        backend_module._record_to_wire(record),
    )
    return payload


def _install_watchall_keys(kv: MagicMock, keys: tuple[str, ...]) -> None:
    class _Entry:
        __slots__ = ("key",)

        def __init__(self, key: str) -> None:
            self.key = key

    class _Watcher:
        def __init__(self, remaining: list[str]) -> None:
            self._remaining = remaining
            self.stop = AsyncMock()

        def __aiter__(self):
            return self

        async def __anext__(self):
            if not self._remaining:
                return None
            key = self._remaining.pop(0)
            return _Entry(key)

    async def watchall(**_kwargs):
        return _Watcher(list(keys))

    kv.watchall = watchall


@pytest.fixture
def started_backend() -> NatsJobBackend:
    backend = NatsJobBackend(MagicMock())
    backend.started = True
    backend._completion_kv = MagicMock()
    return backend


@pytest.mark.asyncio
async def test_waiting_view_verified_empty_completion_bucket(
    started_backend: NatsJobBackend,
) -> None:
    kv = started_backend._completion_kv
    kv.status = AsyncMock(return_value=_bucket_status(messages=0))
    kv.watchall = AsyncMock()

    view = await started_backend.read_waiting_execution_view(
        evaluated_at=datetime.now(tz=UTC),
        max_entries=10,
        max_scan_keys=100,
        scan_deadline_monotonic=time.monotonic() + 5.0,
    )

    assert view.entries == ()
    assert view.completeness is WaitingExecutionCompleteness.COMPLETE
    kv.watchall.assert_not_called()


@pytest.mark.asyncio
async def test_waiting_view_reads_via_bounded_watchall(
    started_backend: NatsJobBackend,
) -> None:
    kv = started_backend._completion_kv
    kv.status = AsyncMock(return_value=_bucket_status(messages=1))
    record = _waiting_record("exec-wait")
    _install_watchall_keys(kv, ("exec-wait",))
    entry = MagicMock()
    entry.value = _wire_record(record)
    kv.get = AsyncMock(return_value=entry)

    view = await started_backend.read_waiting_execution_view(
        evaluated_at=datetime.now(tz=UTC),
        max_entries=10,
        max_scan_keys=100,
        scan_deadline_monotonic=time.monotonic() + 5.0,
    )

    assert {item.execution_id for item in view.entries} == {"exec-wait"}
    assert view.completeness is WaitingExecutionCompleteness.COMPLETE


@pytest.mark.asyncio
async def test_waiting_view_watchall_listing_unavailable(
    started_backend: NatsJobBackend,
) -> None:
    kv = started_backend._completion_kv
    kv.status = AsyncMock(return_value=_bucket_status(messages=2))
    kv.watchall = AsyncMock(side_effect=APIError())

    with pytest.raises(WaitingExecutionUnavailableError, match="list completion records"):
        await started_backend.read_waiting_execution_view(
            evaluated_at=datetime.now(tz=UTC),
            max_entries=10,
            max_scan_keys=100,
            scan_deadline_monotonic=time.monotonic() + 5.0,
        )


@pytest.mark.asyncio
async def test_waiting_view_bounded_scan_keys_truncates(
    started_backend: NatsJobBackend,
) -> None:
    kv = started_backend._completion_kv
    kv.status = AsyncMock(return_value=_bucket_status(messages=4))
    _install_watchall_keys(kv, tuple(f"exec-{index}" for index in range(4)))

    async def get_entry(key: str) -> MagicMock:
        entry = MagicMock()
        entry.value = _wire_record(_waiting_record(key))
        return entry

    kv.get = AsyncMock(side_effect=get_entry)

    view = await started_backend.read_waiting_execution_view(
        evaluated_at=datetime.now(tz=UTC),
        max_entries=10,
        max_scan_keys=2,
        scan_deadline_monotonic=time.monotonic() + 5.0,
    )

    assert view.completeness is WaitingExecutionCompleteness.TRUNCATED
    assert len(view.entries) == 2


@pytest.mark.asyncio
async def test_waiting_view_requires_started_transport() -> None:
    backend = NatsJobBackend(MagicMock())
    backend._completion_kv = MagicMock()

    with pytest.raises(WaitingExecutionUnavailableError, match="started transport"):
        await backend.read_waiting_execution_view(
            evaluated_at=datetime.now(tz=UTC),
            max_entries=10,
            max_scan_keys=100,
            scan_deadline_monotonic=time.monotonic() + 5.0,
        )


@pytest.mark.asyncio
async def test_waiting_view_partial_when_record_missing_on_read(
    started_backend: NatsJobBackend,
) -> None:
    kv = started_backend._completion_kv
    kv.status = AsyncMock(return_value=_bucket_status(messages=2))
    _install_watchall_keys(kv, ("exec-a", "exec-b"))
    record = _waiting_record("exec-a")
    entry = MagicMock()
    entry.value = _wire_record(record)

    async def get_side_effect(key: str) -> MagicMock:
        if key == "exec-a":
            return entry
        raise NotFoundError()

    kv.get = AsyncMock(side_effect=get_side_effect)

    view = await started_backend.read_waiting_execution_view(
        evaluated_at=datetime.now(tz=UTC),
        max_entries=10,
        max_scan_keys=100,
        scan_deadline_monotonic=time.monotonic() + 5.0,
    )

    assert view.completeness is WaitingExecutionCompleteness.PARTIAL
    assert {item.execution_id for item in view.entries} == {"exec-a"}


@pytest.mark.asyncio
async def test_waiting_view_bucket_status_unavailable(
    started_backend: NatsJobBackend,
) -> None:
    kv = started_backend._completion_kv
    kv.status = AsyncMock(side_effect=APIError())

    with pytest.raises(WaitingExecutionUnavailableError):
        await started_backend.read_waiting_execution_view(
            evaluated_at=datetime.now(tz=UTC),
            max_entries=10,
            max_scan_keys=100,
            scan_deadline_monotonic=time.monotonic() + 5.0,
        )

@pytest.mark.asyncio
async def test_waiting_view_rejects_ambiguous_empty_watch(
    started_backend: NatsJobBackend,
) -> None:
    kv = started_backend._completion_kv
    kv.status = AsyncMock(return_value=_bucket_status(messages=1))
    _install_watchall_keys(kv, ())
    with pytest.raises(WaitingExecutionUnavailableError, match="scan was empty"):
        await started_backend.read_waiting_execution_view(
            evaluated_at=datetime.now(tz=UTC), max_entries=10, max_scan_keys=100,
            scan_deadline_monotonic=time.monotonic() + 5.0,
        )
