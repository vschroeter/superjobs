from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta

import pytest

from superjobs import HandlerCatalog, Job, JobContext, PresenceConfig, SuperJobs
from superjobs.discovery.envelope import StoredWorkerRegistration
from superjobs.discovery.errors import DiscoveryWriteError
from superjobs.discovery.memory import InMemoryDiscoveryBackend
from superjobs.discovery.models import WorkerRegistrationState
from superjobs.transport.in_memory import InMemoryTransport


@dataclass
class Capability:
    locale: str = "default"


@dataclass
class CallableCapability:
    locale: str = "callable"

    def __call__(self) -> str:
        raise AssertionError("A capability instance must not be invoked")


class Clock:
    def __init__(self):
        self.now = datetime(2026, 1, 1, tzinfo=UTC)

    def __call__(self):
        return self.now


JOB = Job("presence.review", capabilities=Capability)


class Registry:
    def __init__(self, clock, config=None):
        self.config = config or PresenceConfig()
        self.store = InMemoryDiscoveryBackend(clock=clock, config=self.config)
        self.writes: asyncio.Queue[StoredWorkerRegistration] = asyncio.Queue()
        self.attempts: asyncio.Queue[StoredWorkerRegistration] = asyncio.Queue()
        self.deleted = asyncio.Event()
        self.fail = False
        self.block_write = False
        self.write_entered = asyncio.Event()
        self.release_write = asyncio.Event()

    async def read_snapshot(self, *, include_stale=False):
        return await self.store.read_snapshot(include_stale=include_stale)

    async def write_registration(self, registration, *, max_envelope_bytes):
        self.attempts.put_nowait(registration)
        if self.fail:
            raise OSError("registry unavailable")
        if self.block_write:
            self.write_entered.set()
            await self.release_write.wait()
        await self.store.write_registration(registration, max_envelope_bytes=max_envelope_bytes)
        self.writes.put_nowait(registration)

    async def delete_registration(self, worker_id, job):
        await self.store.delete_registration(worker_id, job)
        self.deleted.set()


def runtime(registry, clock):
    return SuperJobs(transport=InMemoryTransport(discovery_store=registry), presence_clock=clock)


async def next_record(queue):
    return await asyncio.wait_for(queue.get(), timeout=1)


@pytest.mark.asyncio
async def test_refresh_serializes_factory_with_updates_and_renewals():
    clock = Clock()
    registry = Registry(clock)
    jobs = runtime(registry, clock)
    entered, release = asyncio.Event(), asyncio.Event()
    calls = 0

    async def factory():
        nonlocal calls
        calls += 1
        if calls == 2:
            entered.set()
            await release.wait()
        return Capability(f"factory-{calls}")

    @jobs.handler(JOB, capabilities=factory)
    async def handler(context: JobContext[None]) -> None:
        return None

    await jobs.start()
    await next_record(registry.writes)
    handle = jobs.worker(JOB)
    refresh = asyncio.create_task(handle.refresh_capabilities())
    await asyncio.wait_for(entered.wait(), 1)
    update = asyncio.create_task(handle.update_capabilities(Capability("manual")))
    # Let the queued update reach the operation lock; no publication is allowed yet.
    await asyncio.sleep(0)
    assert registry.writes.empty()
    release.set()
    await asyncio.gather(refresh, update)
    try:
        assert (await jobs.client(JOB).workers())[0].capabilities == Capability("manual")
        assert calls == 2
    finally:
        await jobs.stop()


@pytest.mark.asyncio
async def test_stop_cancels_blocked_refresh_and_queued_update_before_delete():
    clock = Clock()
    registry = Registry(clock)
    jobs = runtime(registry, clock)
    entered = asyncio.Event()
    calls = 0

    async def factory():
        nonlocal calls
        calls += 1
        if calls > 1:
            entered.set()
            await asyncio.Event().wait()
        return Capability("initial")

    @jobs.handler(JOB, capabilities=factory)
    async def handler(context: JobContext[None]) -> None:
        return None

    await jobs.start()
    await next_record(registry.writes)
    refresh = asyncio.create_task(jobs.worker(JOB).refresh_capabilities())
    await asyncio.wait_for(entered.wait(), 1)
    update = asyncio.create_task(jobs.worker(JOB).update_capabilities(Capability("queued")))
    await asyncio.sleep(0)
    await asyncio.wait_for(jobs.stop(), 1)
    results = await asyncio.gather(refresh, update, return_exceptions=True)
    assert all(isinstance(result, BaseException) for result in results)
    assert registry.deleted.is_set()
    assert await jobs.client(JOB).workers() == []
    assert all(record.state == WorkerRegistrationState.DRAINING for record in list(registry.writes._queue))


@pytest.mark.asyncio
async def test_failed_publication_and_outage_preserve_ack_then_recover_latest_raw():
    clock = Clock()
    config = PresenceConfig(renewal_interval=timedelta(milliseconds=10), lease_timeout=timedelta(milliseconds=30))
    registry = Registry(clock, config)
    jobs = runtime(registry, clock)
    calls = 0

    def factory():
        nonlocal calls
        calls += 1
        return Capability("factory")

    @jobs.handler(JOB, capabilities=factory)
    async def handler(context: JobContext[None]) -> None:
        return None

    await jobs.start()
    original = await next_record(registry.writes)
    await next_record(registry.attempts)
    handle = jobs.worker(JOB)
    await handle.update_capabilities(Capability("accepted"))
    accepted = await next_record(registry.writes)
    await next_record(registry.attempts)
    registry.fail = True
    clock.now += timedelta(milliseconds=5)
    with pytest.raises(DiscoveryWriteError):
        await handle.update_capabilities(Capability("rejected"))
    await next_record(registry.attempts)
    # Observe a real scheduled renewal attempt while the registry is unavailable.
    await next_record(registry.attempts)
    current = (await jobs.client(JOB).workers())[0]
    assert current.last_seen_at == accepted.last_seen_at
    assert current.capabilities == Capability("accepted")
    registry.fail = False
    clock.now += timedelta(milliseconds=5)
    renewed = await next_record(registry.writes)
    try:
        assert renewed.last_seen_at == clock.now
        assert renewed.registered_at == original.registered_at
        assert (await jobs.client(JOB).workers())[0].capabilities == Capability("accepted")
        assert calls == 1
    finally:
        await jobs.stop()


@pytest.mark.asyncio
async def test_renewal_recreates_deleted_registration_without_reinvoking_factory():
    clock = Clock()
    registry = Registry(clock, PresenceConfig(renewal_interval=timedelta(milliseconds=10), lease_timeout=timedelta(milliseconds=30)))
    jobs = runtime(registry, clock)
    calls = 0

    def factory():
        nonlocal calls
        calls += 1
        return Capability("first")

    @jobs.handler(JOB, capabilities=factory)
    async def handler(context: JobContext[None]) -> None:
        return None

    await jobs.start()
    original = await next_record(registry.writes)
    await jobs.worker(JOB).update_capabilities(Capability("latest"))
    await next_record(registry.writes)
    await registry.delete_registration(jobs.worker_id, JOB.identity)
    clock.now += timedelta(milliseconds=5)
    recreated = await next_record(registry.writes)
    try:
        worker = (await jobs.client(JOB).workers())[0]
        assert worker.capabilities == Capability("latest")
        assert recreated.registered_at == original.registered_at
        assert recreated.worker_id == jobs.worker_id
        assert calls == 1
    finally:
        await jobs.stop()


@pytest.mark.asyncio
async def test_ack_and_deregistration_precede_wait_for_active_attempt():
    clock = Clock()
    registry = Registry(clock)
    jobs = runtime(registry, clock)
    entered, release = asyncio.Event(), asyncio.Event()

    @jobs.handler(JOB)
    async def handler(context: JobContext[None]) -> None:
        entered.set()
        await release.wait()

    await jobs.start()
    handle = await jobs.client(JOB).submit()
    await asyncio.wait_for(entered.wait(), 1)
    stopped = asyncio.create_task(jobs.stop())
    await asyncio.wait_for(registry.deleted.wait(), 1)
    assert not stopped.done()
    assert await jobs.client(JOB).workers() == []
    release.set()
    await stopped
    await handle.result()


@pytest.mark.asyncio
async def test_classes_are_factories_and_callable_dto_instances_are_values():
    clock = Clock()
    registry = Registry(clock)
    jobs = runtime(registry, clock)
    callable_job = Job("presence.callable", capabilities=CallableCapability)

    @jobs.handler(JOB, capabilities=Capability)
    async def first(context: JobContext[None]) -> None:
        return None

    @jobs.handler(callable_job, capabilities=CallableCapability("static"))
    async def second(context: JobContext[None]) -> None:
        return None

    await jobs.start()
    try:
        assert (await jobs.client(JOB).workers())[0].capabilities == Capability()
        assert (await jobs.client(callable_job).workers())[0].capabilities == CallableCapability("static")
    finally:
        await jobs.stop()


@pytest.mark.asyncio
async def test_runtime_catalog_preserves_factory_for_another_runtime():
    source = SuperJobs()
    calls = 0

    def factory():
        nonlocal calls
        calls += 1
        return Capability("copied")

    @source.handler(JOB, capabilities=factory)
    async def handler(context: JobContext[None]) -> None:
        return None

    target = SuperJobs(handlers=source.catalog)
    assert calls == 0
    await target.start()
    try:
        assert (await target.client(JOB).workers())[0].capabilities == Capability("copied")
        assert calls == 1
    finally:
        await target.stop()


def test_cli_help_keeps_factories_lazy_and_run_publishes_catalog_values():
    from superjobs.cli import JobCLI
    from typer.testing import CliRunner

    catalog = HandlerCatalog()
    calls = 0

    def factory():
        nonlocal calls
        calls += 1
        return Capability("cli")

    @catalog.handler(JOB, cli="check", capabilities=factory)
    async def handler(context: JobContext[None]) -> None:
        return None

    cli = JobCLI(handlers=catalog)
    assert CliRunner().invoke(cli.build_typer(), ["run", "check", "--help"]).exit_code == 0
    assert calls == 0
    result = CliRunner().invoke(cli.build_typer(), ["run", "check"])
    assert result.exit_code == 0, result.output
    assert calls == 1

@pytest.mark.asyncio
async def test_cancelled_refresh_settles_async_factory_cleanup_before_provider_release():
    clock = Clock()
    registry = Registry(clock, PresenceConfig(read_timeout=timedelta(seconds=1)))
    catalog = HandlerCatalog()
    factory_entered, cleanup_entered = asyncio.Event(), asyncio.Event()
    cleanup_release, cleanup_finished, provider_released = asyncio.Event(), asyncio.Event(), asyncio.Event()
    calls = 0

    async def factory():
        nonlocal calls
        calls += 1
        if calls == 1:
            return Capability("initial")
        factory_entered.set()
        try:
            await asyncio.Event().wait()
        finally:
            cleanup_entered.set()
            await cleanup_release.wait()
            cleanup_finished.set()

    @asynccontextmanager
    async def provider():
        async def handler(context: JobContext[None]) -> None:
            return None
        try:
            yield handler
        finally:
            assert cleanup_finished.is_set()
            provider_released.set()

    catalog.bind(JOB, provider=provider, capabilities=factory)
    jobs = SuperJobs(transport=InMemoryTransport(discovery_store=registry), handlers=catalog, presence_clock=clock)
    await jobs.start()
    refresh = asyncio.create_task(jobs.worker(JOB).refresh_capabilities())
    await asyncio.wait_for(factory_entered.wait(), 1)
    refresh.cancel()
    await asyncio.wait_for(cleanup_entered.wait(), 1)
    stopped = asyncio.create_task(jobs.stop())
    # Repeat user cancellation while runtime shutdown is also waiting for cleanup.
    refresh.cancel()
    await asyncio.sleep(0)
    assert not provider_released.is_set()
    cleanup_release.set()
    await asyncio.wait_for(stopped, 1)
    await asyncio.gather(refresh, return_exceptions=True)
    assert cleanup_finished.is_set()
    assert provider_released.is_set()


@pytest.mark.asyncio
async def test_producer_only_without_discovery_support_never_inspects_registry():
    class ProducerBackend:
        started = False

        @property
        def discovery_backend(self):
            raise AssertionError("Producer-only lifecycle must not inspect discovery")

        async def start(self):
            self.started = True

        async def stop(self):
            self.started = False

    backend = ProducerBackend()
    jobs = SuperJobs(transport=backend)
    await jobs.start()
    await jobs.stop()
    assert not backend.started


@pytest.mark.asyncio
async def test_hung_registry_shutdown_is_bounded_and_releases_provider():
    clock = Clock()
    config = PresenceConfig(read_timeout=timedelta(milliseconds=10))
    registry = Registry(clock, config)
    catalog = HandlerCatalog()
    released = asyncio.Event()

    @asynccontextmanager
    async def provider():
        async def handler(context: JobContext[None]) -> None:
            return None
        try:
            yield handler
        finally:
            released.set()

    catalog.bind(JOB, provider=provider)
    jobs = SuperJobs(transport=InMemoryTransport(discovery_store=registry), handlers=catalog, presence_clock=clock)
    await jobs.start()
    registry.block_write = True
    with pytest.raises(Exception, match="timed out"):
        await asyncio.wait_for(jobs.stop(), 1)
    assert released.is_set()
    assert registry.deleted.is_set()
