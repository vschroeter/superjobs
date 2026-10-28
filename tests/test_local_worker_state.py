from __future__ import annotations

import asyncio
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta

import pytest

from superjobs import Job, JobContext, SuperJobs
from superjobs.discovery.config import PresenceConfig
from superjobs.discovery.memory import InMemoryDiscoveryBackend
from superjobs.discovery.models import WorkerRegistrationState
from superjobs.transport.in_memory import InMemoryTransport


@dataclass(frozen=True, slots=True)
class Request:
    value: int


@dataclass(frozen=True, slots=True)
class Result:
    value: str


JOB = Job("tests.local.worker.state", version="v1", request=Request, result=Result)


class MutableClock:
    def __init__(self, start: datetime) -> None:
        self._now = start.astimezone(UTC)

    def __call__(self) -> datetime:
        return self._now


@pytest.mark.asyncio
async def test_wait_until_readiness_withdrawn_completes_without_discovery_poll() -> None:
    clock = MutableClock(datetime(2026, 3, 1, tzinfo=UTC))
    store = InMemoryDiscoveryBackend(clock=clock)
    transport = InMemoryTransport(discovery_store=store, discovery_clock=clock)
    jobs = SuperJobs(transport=transport, presence_clock=clock)

    @jobs.handler(JOB)
    async def handler(request: Request, context: JobContext[None]) -> Result:
        return Result(value="ok")

    await jobs.start()
    try:
        handle = jobs.worker(JOB)
        assert not handle.readiness_withdrawn
        wait_task = asyncio.create_task(handle.wait_until_readiness_withdrawn())
        await asyncio.sleep(0)
        assert not wait_task.done()
        await jobs._presence.withdraw_readiness(JOB)
        await asyncio.wait_for(wait_task, timeout=1)
        assert handle.readiness_withdrawn
        own = await handle.read_own_registration(include_stale=True)
        assert own is not None
        assert own.state is WorkerRegistrationState.DRAINING
    finally:
        await jobs.stop()


@pytest.mark.asyncio
async def test_readiness_withdrawn_property_tracks_local_decision_immediately() -> None:
    clock = MutableClock(datetime(2026, 3, 2, tzinfo=UTC))
    store = InMemoryDiscoveryBackend(clock=clock)
    transport = InMemoryTransport(discovery_store=store, discovery_clock=clock)
    jobs = SuperJobs(transport=transport, presence_clock=clock)

    @jobs.handler(JOB)
    async def handler(request: Request, context: JobContext[None]) -> Result:
        return Result(value="ok")

    await jobs.start()
    try:
        handle = jobs.worker(JOB)
        await jobs._presence.withdraw_readiness(JOB)
        assert handle.readiness_withdrawn
    finally:
        await jobs.stop()


@pytest.mark.asyncio
async def test_read_own_registration_uses_single_store_read() -> None:
    clock = MutableClock(datetime(2026, 3, 3, tzinfo=UTC))
    store = InMemoryDiscoveryBackend(clock=clock)
    reads = 0
    original = store.read_registration

    async def counting_read(worker_id: str, job):
        nonlocal reads
        reads += 1
        return await original(worker_id, job)

    store.read_registration = counting_read  # type: ignore[method-assign]
    transport = InMemoryTransport(discovery_store=store, discovery_clock=clock)
    jobs = SuperJobs(transport=transport, presence_clock=clock)

    @jobs.handler(JOB)
    async def handler(request: Request, context: JobContext[None]) -> Result:
        return Result(value="ok")

    await jobs.start()
    try:
        handle = jobs.worker(JOB)
        before = reads
        reg = await handle.read_own_registration()
        assert reg is not None
        assert reg.worker_id == jobs.worker_id
        assert reads - before >= 1
    finally:
        await jobs.stop()


@pytest.mark.asyncio
async def test_new_serving_generation_clears_withdrawn_wait_state() -> None:
    clock = MutableClock(datetime(2026, 3, 4, tzinfo=UTC))
    store = InMemoryDiscoveryBackend(
        config=PresenceConfig(
            renewal_interval=timedelta(hours=1),
            lease_timeout=timedelta(hours=3),
        ),
        clock=clock,
    )
    transport = InMemoryTransport(discovery_store=store, discovery_clock=clock)
    jobs = SuperJobs(transport=transport, presence_clock=clock)

    @jobs.handler(JOB)
    async def handler(request: Request, context: JobContext[None]) -> Result:
        return Result(value="ok")

    await jobs.start()
    try:
        handle = jobs.worker(JOB)
        await jobs._presence.withdraw_readiness(JOB)
        await handle.wait_until_readiness_withdrawn()
    finally:
        await jobs.stop()

    await jobs.start()
    try:
        handle = jobs.worker(JOB)
        assert not handle.readiness_withdrawn
        wait_task = asyncio.create_task(handle.wait_until_readiness_withdrawn(timeout=0.05))
        with pytest.raises(asyncio.TimeoutError):
            await wait_task
    finally:
        await jobs.stop()
