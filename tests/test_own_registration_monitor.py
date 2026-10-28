from __future__ import annotations

import asyncio
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from unittest.mock import AsyncMock

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


JOB = Job("tests.own.registration", version="v1", request=Request, result=Result)


class MutableClock:
    def __init__(self, start: datetime) -> None:
        self._now = start.astimezone(UTC)

    def advance(self, delta: timedelta) -> None:
        self._now += delta

    def __call__(self) -> datetime:
        return self._now


@pytest.mark.asyncio
async def test_external_delete_signals_inactive_without_snapshot_poll() -> None:
    clock = MutableClock(datetime(2026, 4, 1, tzinfo=UTC))
    store = InMemoryDiscoveryBackend(clock=clock)
    transport = InMemoryTransport(discovery_store=store, discovery_clock=clock)
    jobs = SuperJobs(transport=transport, presence_clock=clock)
    snapshot_reads = 0
    original = store.read_snapshot

    async def counting_snapshot(**kwargs):
        nonlocal snapshot_reads
        snapshot_reads += 1
        return await original(**kwargs)

    store.read_snapshot = counting_snapshot  # type: ignore[method-assign]

    @jobs.handler(JOB)
    async def handler(request: Request, context: JobContext[None]) -> Result:
        return Result(value="ok")

    await jobs.start()
    try:
        handle = jobs.worker(JOB)
        assert handle.own_registration_active
        wait_task = asyncio.create_task(handle.wait_until_own_registration_inactive())
        await asyncio.sleep(0.05)
        await store.delete_registration(jobs.worker_id, JOB.identity)
        await asyncio.wait_for(wait_task, timeout=1)
        assert not handle.own_registration_active
        assert snapshot_reads == 0
    finally:
        await jobs.stop()


@pytest.mark.asyncio
async def test_local_withdrawal_signals_inactive_without_snapshot_poll() -> None:
    clock = MutableClock(datetime(2026, 4, 2, tzinfo=UTC))
    store = InMemoryDiscoveryBackend(clock=clock)
    jobs = SuperJobs(
        transport=InMemoryTransport(discovery_store=store, discovery_clock=clock),
        presence_clock=clock,
    )
    snapshot_reads = 0
    original = store.read_snapshot

    async def counting_snapshot(**kwargs):
        nonlocal snapshot_reads
        snapshot_reads += 1
        return await original(**kwargs)

    store.read_snapshot = counting_snapshot  # type: ignore[method-assign]

    @jobs.handler(JOB)
    async def handler(request: Request, context: JobContext[None]) -> Result:
        return Result(value="ok")

    await jobs.start()
    try:
        handle = jobs.worker(JOB)
        wait_task = asyncio.create_task(handle.wait_until_own_registration_inactive())
        await jobs._presence.withdraw_readiness(JOB)
        await asyncio.wait_for(wait_task, timeout=1)
        assert handle.readiness_withdrawn
        assert not handle.own_registration_active
        assert snapshot_reads == 0
    finally:
        await jobs.stop()


@pytest.mark.asyncio
async def test_store_unavailable_marks_inactive_without_local_ready_assumption() -> None:
    clock = MutableClock(datetime(2026, 4, 3, tzinfo=UTC))
    store = InMemoryDiscoveryBackend(clock=clock)
    jobs = SuperJobs(
        transport=InMemoryTransport(discovery_store=store, discovery_clock=clock),
        presence_clock=clock,
    )

    @jobs.handler(JOB)
    async def handler(request: Request, context: JobContext[None]) -> Result:
        return Result(value="ok")

    await jobs.start()
    try:
        handle = jobs.worker(JOB)
        assert handle.own_registration_active

        async def failing_read(*_args, **_kwargs):
            raise ConnectionError("registry unavailable")

        store.read_registration = failing_read  # type: ignore[method-assign]
        with pytest.raises(ConnectionError, match="registry unavailable"):
            await jobs._presence.reconcile_own_registration(JOB)
        assert not handle.own_registration_active
        assert jobs._presence._entries[JOB.canonical_name].own_registration_monitor is not None
        assert not jobs._presence._entries[JOB.canonical_name].own_registration_monitor.done()
    finally:
        await jobs.stop()


@pytest.mark.asyncio
async def test_reconcile_confirmed_missing_returns_none_without_error() -> None:
    clock = MutableClock(datetime(2026, 4, 7, tzinfo=UTC))
    store = InMemoryDiscoveryBackend(clock=clock)
    jobs = SuperJobs(
        transport=InMemoryTransport(discovery_store=store, discovery_clock=clock),
        presence_clock=clock,
    )

    @jobs.handler(JOB)
    async def handler(request: Request, context: JobContext[None]) -> Result:
        return Result(value="ok")

    await jobs.start()
    try:
        await store.delete_registration(jobs.worker_id, JOB.identity)
        stored = await jobs._presence.reconcile_own_registration(JOB)
        assert stored is None
        assert not jobs.worker(JOB).own_registration_active
    finally:
        await jobs.stop()


@pytest.mark.asyncio
async def test_handle_reconcile_raises_on_registry_unavailable_not_none() -> None:
    clock = MutableClock(datetime(2026, 4, 8, tzinfo=UTC))
    store = InMemoryDiscoveryBackend(clock=clock)
    jobs = SuperJobs(
        transport=InMemoryTransport(discovery_store=store, discovery_clock=clock),
        presence_clock=clock,
    )

    @jobs.handler(JOB)
    async def handler(request: Request, context: JobContext[None]) -> Result:
        return Result(value="ok")

    await jobs.start()
    try:
        handle = jobs.worker(JOB)

        async def failing_read(*_args, **_kwargs):
            raise OSError("broker partition")

        store.read_registration = failing_read  # type: ignore[method-assign]
        with pytest.raises(OSError, match="broker partition"):
            await handle.reconcile_own_registration()
        assert not handle.own_registration_active
    finally:
        await jobs.stop()


@pytest.mark.asyncio
async def test_runtime_stop_cancels_own_registration_watch_tasks() -> None:
    clock = MutableClock(datetime(2026, 4, 4, tzinfo=UTC))
    store = InMemoryDiscoveryBackend(clock=clock)
    jobs = SuperJobs(
        transport=InMemoryTransport(discovery_store=store, discovery_clock=clock),
        presence_clock=clock,
    )

    @jobs.handler(JOB)
    async def handler(request: Request, context: JobContext[None]) -> Result:
        return Result(value="ok")

    await jobs.start()
    entry = jobs._presence._entries[JOB.canonical_name]
    monitor = entry.own_registration_monitor
    assert monitor is not None and not monitor.done()
    await jobs.stop()
    await asyncio.sleep(0)
    assert monitor.done()


@pytest.mark.asyncio
async def test_lease_expiry_observed_via_watch_without_snapshot_poll() -> None:
    clock = MutableClock(
        datetime(2026, 4, 5, tzinfo=UTC),
    )
    config = PresenceConfig(
        renewal_interval=timedelta(milliseconds=50),
        lease_timeout=timedelta(milliseconds=200),
    )
    store = InMemoryDiscoveryBackend(config=config, clock=clock)
    jobs = SuperJobs(
        transport=InMemoryTransport(discovery_store=store, discovery_clock=clock),
        presence_clock=clock,
        presence_config=config,
    )
    snapshot_reads = 0
    original = store.read_snapshot

    async def counting_snapshot(**kwargs):
        nonlocal snapshot_reads
        snapshot_reads += 1
        return await original(**kwargs)

    store.read_snapshot = counting_snapshot  # type: ignore[method-assign]

    @jobs.handler(JOB)
    async def handler(request: Request, context: JobContext[None]) -> Result:
        return Result(value="ok")

    await jobs.start()
    try:
        handle = jobs.worker(JOB)
        wait_task = asyncio.create_task(handle.wait_until_own_registration_inactive())
        clock.advance(timedelta(milliseconds=250))
        await asyncio.wait_for(wait_task, timeout=1)
        assert not handle.own_registration_active
        assert snapshot_reads == 0
    finally:
        await jobs.stop()


@pytest.mark.asyncio
async def test_runtime_restart_reconciles_own_registration_active() -> None:
    clock = MutableClock(datetime(2026, 4, 6, tzinfo=UTC))
    store = InMemoryDiscoveryBackend(clock=clock)
    jobs = SuperJobs(
        transport=InMemoryTransport(discovery_store=store, discovery_clock=clock),
        presence_clock=clock,
    )

    @jobs.handler(JOB)
    async def handler(request: Request, context: JobContext[None]) -> Result:
        return Result(value="ok")

    await jobs.start()
    await jobs.stop()
    await jobs.start()
    try:
        handle = jobs.worker(JOB)
        deadline = asyncio.get_running_loop().time() + 1.0
        while asyncio.get_running_loop().time() < deadline:
            if handle.own_registration_active:
                break
            await asyncio.sleep(0.01)
        else:
            raise AssertionError("own registration did not become active after restart")
    finally:
        await jobs.stop()
