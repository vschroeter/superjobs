from __future__ import annotations

from datetime import UTC, datetime, timedelta
from unittest.mock import AsyncMock, MagicMock

import pytest
from faststream.nats import NatsBroker
from nats.js.errors import APIError, NoKeysError

from superjobs.jobs.execution import JobState
from superjobs.jobs.job_identity import JobIdentity
from superjobs.transport.backend import ExecutionRecord
from superjobs.transport.nats_backend import (
    NatsJobBackend,
    _pack,
    _record_to_wire,
)


def _pending_record(job_id: str) -> ExecutionRecord:
    return ExecutionRecord(
        identity=JobIdentity("tests.restore-deadlines", version="v1"),
        job_id=job_id,
        request_payload=b"{}",
        request_media_type="application/json",
        fingerprint="fp",
        idempotency_key="idem",
        caller_scope="default",
        timeout=None,
        deadline=datetime.now(UTC) + timedelta(hours=1),
        created_at=datetime.now(UTC),
        state=JobState.PENDING,
    )


def _backend_with_completion_kv(completion_kv: MagicMock) -> NatsJobBackend:
    backend = NatsJobBackend(NatsBroker())
    backend.started = True
    backend._completion_kv = completion_kv
    return backend


@pytest.mark.asyncio
async def test_restore_deadlines_empty_completion_bucket_succeeds() -> None:
    completion_kv = MagicMock()
    completion_kv.keys = AsyncMock(side_effect=NoKeysError())
    backend = _backend_with_completion_kv(completion_kv)

    await backend._restore_deadlines()

    completion_kv.get.assert_not_called()
    assert backend._deadline_tasks == {}


@pytest.mark.asyncio
async def test_restore_deadlines_schedules_pending_deadline() -> None:
    record = _pending_record("job-1")
    entry = MagicMock()
    entry.value = _pack(_record_to_wire(record))
    completion_kv = MagicMock()
    completion_kv.keys = AsyncMock(return_value=["job-1"])
    completion_kv.get = AsyncMock(return_value=entry)
    backend = _backend_with_completion_kv(completion_kv)

    await backend._restore_deadlines()

    assert "job-1" in backend._deadline_tasks
    assert not backend._deadline_tasks["job-1"].done()


@pytest.mark.asyncio
async def test_restore_deadlines_propagates_unrelated_keys_failure() -> None:
    completion_kv = MagicMock()
    completion_kv.keys = AsyncMock(side_effect=APIError())
    backend = _backend_with_completion_kv(completion_kv)

    with pytest.raises(APIError):
        await backend._restore_deadlines()