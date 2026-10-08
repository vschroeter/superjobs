"""End-to-end worker presence over real NATS (issues #53 / #54 integration)."""

from __future__ import annotations

import asyncio
import uuid
from contextlib import asynccontextmanager
from dataclasses import dataclass
from datetime import timedelta

import pytest
from faststream.nats import NatsBroker
from pydantic import ValidationError

from superjobs import HandlerCatalog, Job, JobContext, JobIdentity, SuperJobs
from superjobs.discovery.config import PresenceConfig
from superjobs.payload import PayloadValidationError
from superjobs.transport.nats_backend import NatsJobBackend


@dataclass(frozen=True, slots=True, kw_only=True)
class WorkerCapability:
    locale: str


def _presence_config() -> PresenceConfig:
    return PresenceConfig(
        renewal_interval=timedelta(milliseconds=100),
        lease_timeout=timedelta(milliseconds=350),
        stale_retention=timedelta(seconds=2),
        read_timeout=timedelta(seconds=2),
    )


async def _connect_backend(
    nats_url: str,
    queue_config,
    *,
    presence_config: PresenceConfig,
) -> tuple[NatsBroker, NatsJobBackend]:
    broker = NatsBroker(nats_url, connect_timeout=2, max_reconnect_attempts=0)
    backend = NatsJobBackend(
        broker,
        queue_config,
        presence_config=presence_config,
    )
    await backend.start()
    return broker, backend


async def _poll_until(
    condition,
    *,
    timeout: float = 5.0,
    interval: float = 0.05,
) -> None:
    deadline = asyncio.get_running_loop().time() + timeout
    while asyncio.get_running_loop().time() < deadline:
        if await condition():
            return
        await asyncio.sleep(interval)
    raise AssertionError("condition not met before timeout")


async def _cancel_and_gather(*tasks: asyncio.Task[object] | None) -> None:
    pending = [task for task in tasks if task is not None and not task.done()]
    for task in pending:
        task.cancel()
    if pending:
        await asyncio.gather(*pending, return_exceptions=True)


def _capability_job(name: str) -> Job:
    return Job(
        name,
        version="v1",
        request=None,
        result=WorkerCapability,
        capabilities=WorkerCapability,
    )


@pytest.mark.nats
@pytest.mark.asyncio
async def test_nats_worker_presence_catalog_provider_and_capability_updates(
    nats_url: str,
    nats_queue_config,
) -> None:
    presence = _presence_config()
    job = _capability_job(f"tests.nats.presence.catalog.{uuid.uuid4().hex}")
    factory_calls = 0

    def capability_factory() -> WorkerCapability:
        nonlocal factory_calls
        factory_calls += 1
        return WorkerCapability(locale=f"factory-{factory_calls}")

    provider_ready = asyncio.Event()
    provider_release = asyncio.Event()

    catalog = HandlerCatalog()

    @asynccontextmanager
    async def managed_provider():
        provider_ready.set()
        await provider_release.wait()

        async def inner(_context: JobContext[None]) -> WorkerCapability:
            return WorkerCapability(locale="unused")

        yield inner

    catalog.bind(
        job,
        provider=managed_provider,
        capabilities=capability_factory,
    )

    worker_broker, worker_backend = await _connect_backend(
        nats_url,
        nats_queue_config,
        presence_config=presence,
    )
    producer_broker, producer_backend = await _connect_backend(
        nats_url,
        nats_queue_config,
        presence_config=presence,
    )
    worker_jobs = SuperJobs(
        transport=worker_backend,
        handlers=catalog,
        presence_config=presence,
    )
    producer_jobs = SuperJobs(transport=producer_backend, presence_config=presence)

    start_task: asyncio.Task[None] | None = None
    try:
        assert await producer_jobs.discovery.jobs() == []
        assert factory_calls == 0

        start_task = asyncio.create_task(worker_jobs.start())
        await asyncio.wait_for(provider_ready.wait(), timeout=3)
        assert await producer_jobs.discovery.jobs() == []
        assert factory_calls == 0

        provider_release.set()
        await asyncio.wait_for(start_task, timeout=5)
        assert factory_calls == 1

        await _poll_until(
            lambda: _has_worker(producer_jobs, job),
            timeout=4,
        )
        typed = await producer_jobs.discovery.workers(job)
        assert len(typed) == 1
        assert typed[0].capabilities == WorkerCapability(locale="factory-1")
        assert typed[0].job == job.identity
        assert await producer_jobs.client(job).workers() == typed

        raw = await producer_jobs.discovery.workers(job.identity)
        assert len(raw) == 1
        assert raw[0].worker_id == worker_jobs.worker_id
        assert raw[0].capabilities is not None
        assert await producer_jobs.discovery.jobs() == [job.identity]

        handle = worker_jobs.worker(job)
        await handle.update_capabilities(WorkerCapability(locale="updated"))
        updated = await producer_jobs.client(job).workers()
        assert updated[0].capabilities == WorkerCapability(locale="updated")

        with pytest.raises((PayloadValidationError, ValidationError)):
            await handle.update_capabilities("not-a-capability")
        preserved = await producer_jobs.client(job).workers()
        assert preserved[0].capabilities == WorkerCapability(locale="updated")

        await handle.refresh_capabilities()
        assert factory_calls == 2
        refreshed = await producer_jobs.client(job).workers()
        assert refreshed[0].capabilities == WorkerCapability(locale="factory-2")
    finally:
        provider_release.set()
        await _cancel_and_gather(start_task)
        await worker_jobs.stop()
        await producer_broker.stop()
        await worker_broker.stop()


async def _has_worker(producer_jobs: SuperJobs, job: Job) -> bool:
    workers = await producer_jobs.discovery.workers(job)
    return len(workers) == 1


@pytest.mark.nats
@pytest.mark.asyncio
async def test_nats_worker_presence_renewal_preserves_identity_timestamps(
    nats_url: str,
    nats_queue_config,
) -> None:
    presence = _presence_config()
    job = _capability_job(f"tests.nats.presence.renew.{uuid.uuid4().hex}")
    catalog = HandlerCatalog()

    @catalog.handler(job, capabilities=WorkerCapability(locale="renew-me"))
    async def _handler(_context: JobContext[None]) -> WorkerCapability:
        return WorkerCapability(locale="renew-me")

    worker_broker, worker_backend = await _connect_backend(
        nats_url,
        nats_queue_config,
        presence_config=presence,
    )
    producer_broker, producer_backend = await _connect_backend(
        nats_url,
        nats_queue_config,
        presence_config=presence,
    )
    worker_jobs = SuperJobs(
        transport=worker_backend,
        handlers=catalog,
        presence_config=presence,
    )
    producer_jobs = SuperJobs(transport=producer_backend, presence_config=presence)

    try:
        await worker_jobs.start()
        await _poll_until(
            lambda: _has_worker(producer_jobs, job),
            timeout=4,
        )
        first = (await producer_jobs.discovery.workers(job))[0]
        worker_id = first.worker_id
        registered_at = first.registered_at

        await asyncio.sleep(presence.renewal_interval.total_seconds() * 2.5)
        second = (await producer_jobs.discovery.workers(job))[0]
        assert second.worker_id == worker_id
        assert second.registered_at == registered_at
        assert second.last_seen_at > first.last_seen_at
    finally:
        await worker_jobs.stop()
        await producer_broker.stop()
        await worker_broker.stop()


@pytest.mark.nats
@pytest.mark.asyncio
async def test_nats_worker_shutdown_clears_discovery_before_long_job_finishes(
    nats_url: str,
    nats_queue_config,
) -> None:
    presence = _presence_config()
    job_name = f"tests.nats.presence.shutdown.{uuid.uuid4().hex}"
    job = Job(
        job_name,
        version="v1",
        request=None,
        result=WorkerCapability,
    )
    execution_gate = asyncio.Event()
    release_execution = asyncio.Event()
    catalog = HandlerCatalog()

    @catalog.handler(job)
    async def long_running(context: JobContext[None]) -> WorkerCapability:
        execution_gate.set()
        await release_execution.wait()
        return WorkerCapability(locale="done")

    worker_broker, worker_backend = await _connect_backend(
        nats_url,
        nats_queue_config,
        presence_config=presence,
    )
    producer_broker, producer_backend = await _connect_backend(
        nats_url,
        nats_queue_config,
        presence_config=presence,
    )
    worker_jobs = SuperJobs(
        transport=worker_backend,
        handlers=catalog,
        presence_config=presence,
        graceful_shutdown_timeout=5.0,
    )
    producer_jobs = SuperJobs(transport=producer_backend, presence_config=presence)

    submit_task: asyncio.Task[object] | None = None
    stop_task: asyncio.Task[None] | None = None
    try:
        await worker_jobs.start()
        await _poll_until(
            lambda: _jobs_include(producer_jobs, job.identity),
            timeout=4,
        )

        submit_task = asyncio.create_task(
            producer_jobs.client(job).run(None, wait_timeout=10.0),
        )
        await asyncio.wait_for(execution_gate.wait(), timeout=5)

        stop_task = asyncio.create_task(worker_jobs.stop())
        await _poll_until(
            lambda: _jobs_empty(producer_jobs),
            timeout=5,
        )
        assert await producer_jobs.discovery.jobs() == []

        release_execution.set()
        await asyncio.wait_for(stop_task, timeout=8)
        await asyncio.wait_for(submit_task, timeout=8)

        assert await producer_jobs.discovery.jobs() == []
        assert await producer_jobs.discovery.workers(job.identity) == []
    finally:
        release_execution.set()
        await _cancel_and_gather(submit_task, stop_task)
        if worker_jobs.started:
            await worker_jobs.stop()
        await producer_broker.stop()
        await worker_broker.stop()


async def _jobs_include(producer_jobs: SuperJobs, identity: JobIdentity) -> bool:
    return identity in await producer_jobs.discovery.jobs()


async def _jobs_empty(producer_jobs: SuperJobs) -> bool:
    return await producer_jobs.discovery.jobs() == []
