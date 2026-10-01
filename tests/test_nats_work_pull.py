"""Regression tests for bounded NATS work pull iteration."""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from superjobs.jobs.execution import JobState
from superjobs.jobs.job_identity import JobIdentity
from superjobs.transport.backend import ExecutionRecord
from superjobs.transport.implementations.nats import NatsQueueConfig
from superjobs.transport.nats_backend import (
    _NatsWorkSubscription,
    _pack,
)


def _sample_execution(job_id: str = "job-1") -> ExecutionRecord:
    identity = JobIdentity(name="demo.job", version="v1")
    now = datetime.now(UTC)
    return ExecutionRecord(
        identity=identity,
        job_id=job_id,
        request_payload=b"{}",
        request_media_type="application/json",
        fingerprint="fp",
        idempotency_key="key",
        caller_scope="scope",
        timeout=None,
        deadline=None,
        created_at=now,
        state=JobState.PENDING,
    )


def _sample_message(job_id: str) -> SimpleNamespace:
    return SimpleNamespace(
        body=_pack({"job_id": job_id, "attempt": 1}),
        metadata=SimpleNamespace(num_delivered=1),
        reject=AsyncMock(),
        nack=AsyncMock(),
        ack_sync=AsyncMock(),
    )


@pytest.mark.asyncio
async def test_work_subscription_delivers_after_idle_pulls() -> None:
    execution = _sample_execution()
    backend = SimpleNamespace(
        queue_config=NatsQueueConfig(),
        get_execution=AsyncMock(return_value=execution),
    )
    subscriber = AsyncMock()
    subscriber.get_one = AsyncMock(
        side_effect=[None, None, _sample_message(execution.job_id)],
    )
    identity = execution.identity
    subscription = _NatsWorkSubscription(backend, identity, subscriber)

    item = await subscription.__anext__()

    assert item.execution.job_id == execution.job_id
    assert subscriber.get_one.await_count == 3


@pytest.mark.asyncio
async def test_work_subscription_close_during_pending_pull_is_bounded() -> None:
    pull_started = asyncio.Event()
    pull_release = asyncio.Event()

    backend = SimpleNamespace(
        queue_config=NatsQueueConfig(),
        get_execution=AsyncMock(),
    )
    subscriber = AsyncMock()
    subscription = _NatsWorkSubscription(
        backend,
        JobIdentity(name="demo.job", version="v1"),
        subscriber,
    )

    async def blocked_get_one(timeout: float) -> object:
        pull_started.set()
        await pull_release.wait()
        return None

    subscriber.get_one = blocked_get_one
    subscriber.stop = AsyncMock()

    task = asyncio.create_task(subscription.__anext__())
    await asyncio.wait_for(pull_started.wait(), timeout=1.0)
    await subscription.close()
    pull_release.set()
    with pytest.raises(StopAsyncIteration):
        await asyncio.wait_for(task, timeout=1.0)


@pytest.mark.asyncio
async def test_work_subscription_close_during_get_execution_does_not_reject() -> None:
    execution = _sample_execution()
    message = _sample_message(execution.job_id)
    get_exec_started = asyncio.Event()
    release_get_exec = asyncio.Event()

    async def slow_get_execution(identity: JobIdentity, job_id: str) -> ExecutionRecord:
        get_exec_started.set()
        await release_get_exec.wait()
        return execution

    backend = SimpleNamespace(
        queue_config=NatsQueueConfig(),
        get_execution=slow_get_execution,
    )
    subscriber = AsyncMock()
    subscriber.get_one = AsyncMock(return_value=message)
    subscription = _NatsWorkSubscription(
        backend,
        execution.identity,
        subscriber,
    )
    subscriber.stop = AsyncMock()

    task = asyncio.create_task(subscription.__anext__())
    await asyncio.wait_for(get_exec_started.wait(), timeout=1.0)
    await subscription.close()
    release_get_exec.set()
    with pytest.raises(StopAsyncIteration):
        await asyncio.wait_for(task, timeout=1.0)

    message.reject.assert_not_called()
    message.nack.assert_not_called()
    message.ack_sync.assert_not_called()
