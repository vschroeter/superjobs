from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta

import pytest

from superjobs import Job, JobContext, SuperJobs
from superjobs.discovery.config import PresenceConfig
from superjobs.discovery.memory import InMemoryDiscoveryBackend
from superjobs.payload import PayloadValidationError
from superjobs.presence.errors import WorkerDiscoveryDisabledError
from superjobs.transport.in_memory import InMemoryTransport


@dataclass(frozen=True, slots=True)
class Request:
    value: int


@dataclass(frozen=True, slots=True)
class Result:
    value: str


@dataclass(frozen=True, slots=True)
class WorkerCapability:
    locale: str


class MutableClock:
    def __init__(self, start: datetime) -> None:
        self._now = start.astimezone(UTC)

    def advance(self, delta: timedelta) -> None:
        self._now += delta

    def __call__(self) -> datetime:
        return self._now


CAP_JOB = Job(
    "tests.presence.cap",
    version="v1",
    request=Request,
    result=Result,
    capabilities=WorkerCapability,
)

PLAIN_JOB = Job("tests.presence.plain", version="v1", request=Request, result=Result)


class _ProducerOnlyBackend:
    started = False

    async def start(self) -> None:
        self.started = True

    async def stop(self) -> None:
        self.started = False


@pytest.mark.asyncio
async def test_presence_published_before_subscription_waits() -> None:
    clock = MutableClock(datetime(2026, 1, 1, tzinfo=UTC))
    store = InMemoryDiscoveryBackend(
        config=PresenceConfig(
            renewal_interval=timedelta(hours=1),
            lease_timeout=timedelta(hours=3),
        ),
        clock=clock,
    )
    transport = InMemoryTransport(discovery_store=store, discovery_clock=clock)
    jobs = SuperJobs(transport=transport, presence_clock=clock)
    release_subscribe = asyncio.Event()
    original_subscribe = transport.subscribe_work
    workers_visible_before_subscribe = asyncio.Event()

    async def tracked_subscribe(identity):
        workers = await jobs.client(CAP_JOB).workers()
        assert len(workers) == 1
        assert workers[0].capabilities == WorkerCapability(locale="de")
        workers_visible_before_subscribe.set()
        await release_subscribe.wait()
        return await original_subscribe(identity)

    transport.subscribe_work = tracked_subscribe  # type: ignore[method-assign]

    @jobs.handler(CAP_JOB, capabilities=WorkerCapability(locale="de"))
    async def handler(request: Request, context: JobContext[None]) -> Result:
        return Result(value=str(request.value))

    start_task = asyncio.create_task(jobs.start())
    await asyncio.wait_for(workers_visible_before_subscribe.wait(), timeout=1)
    release_subscribe.set()
    await asyncio.wait_for(start_task, timeout=1)
    try:
        workers = await jobs.client(CAP_JOB).workers()
        assert workers[0].worker_id == jobs.worker_id
    finally:
        await jobs.stop()


@pytest.mark.asyncio
async def test_factory_failure_rolls_back_startup() -> None:
    clock = MutableClock(datetime(2026, 1, 2, tzinfo=UTC))
    store = InMemoryDiscoveryBackend(clock=clock)
    transport = InMemoryTransport(discovery_store=store, discovery_clock=clock)
    jobs = SuperJobs(transport=transport, presence_clock=clock)

    def bad_factory() -> WorkerCapability:
        raise ValueError("factory failed")

    @jobs.handler(CAP_JOB, capabilities=bad_factory)
    async def handler(request: Request, context: JobContext[None]) -> Result:
        return Result(value="ok")

    with pytest.raises(ValueError, match="factory failed"):
        await jobs.start()
    workers = await jobs.client(CAP_JOB).workers()
    assert workers == []
    assert not jobs.started


@pytest.mark.asyncio
async def test_update_capabilities_preserves_snapshot_on_invalid_value() -> None:
    clock = MutableClock(datetime(2026, 1, 3, tzinfo=UTC))
    store = InMemoryDiscoveryBackend(clock=clock)
    transport = InMemoryTransport(discovery_store=store, discovery_clock=clock)
    jobs = SuperJobs(transport=transport, presence_clock=clock)

    @jobs.handler(CAP_JOB, capabilities=WorkerCapability(locale="de"))
    async def handler(request: Request, context: JobContext[None]) -> Result:
        return Result(value="ok")

    await jobs.start()
    try:
        handle = jobs.worker(CAP_JOB)
        with pytest.raises(PayloadValidationError):
            await handle.update_capabilities("invalid")
        workers = await jobs.client(CAP_JOB).workers()
        assert workers[0].capabilities == WorkerCapability(locale="de")
    finally:
        await jobs.stop()


@pytest.mark.asyncio
async def test_refresh_without_factory_fails() -> None:
    clock = MutableClock(datetime(2026, 1, 4, tzinfo=UTC))
    transport = InMemoryTransport(
        discovery_store=InMemoryDiscoveryBackend(clock=clock),
        discovery_clock=clock,
    )
    jobs = SuperJobs(transport=transport, presence_clock=clock)

    @jobs.handler(CAP_JOB)
    async def handler(request: Request, context: JobContext[None]) -> Result:
        return Result(value="ok")

    await jobs.start()
    try:
        with pytest.raises(Exception, match="factory"):
            await jobs.worker(CAP_JOB).refresh_capabilities()
    finally:
        await jobs.stop()


@pytest.mark.asyncio
async def test_discovery_disabled_blocks_local_worker_only() -> None:
    clock = MutableClock(datetime(2026, 1, 7, tzinfo=UTC))
    transport = InMemoryTransport(
        discovery_store=InMemoryDiscoveryBackend(clock=clock),
        discovery_clock=clock,
    )
    jobs = SuperJobs(transport=transport, discovery=False)

    @jobs.handler(CAP_JOB, capabilities=WorkerCapability(locale="de"))
    async def handler(request: Request, context: JobContext[None]) -> Result:
        return Result(value="ok")

    await jobs.start()
    try:
        with pytest.raises(WorkerDiscoveryDisabledError):
            jobs.worker(CAP_JOB)
        assert await jobs.discovery.jobs() == []
        assert await jobs.client(CAP_JOB).workers() == []
    finally:
        await jobs.stop()


@pytest.mark.asyncio
async def test_producer_only_runtime_has_no_registrations() -> None:
    clock = MutableClock(datetime(2026, 1, 5, tzinfo=UTC))
    transport = InMemoryTransport(
        discovery_store=InMemoryDiscoveryBackend(clock=clock),
        discovery_clock=clock,
    )
    jobs = SuperJobs(transport=transport, presence_clock=clock)
    await jobs.start()
    try:
        assert await jobs.discovery.jobs() == []
    finally:
        await jobs.stop()


@pytest.mark.asyncio
async def test_worker_id_stable_across_stop_start() -> None:
    clock = MutableClock(datetime(2026, 1, 10, tzinfo=UTC))
    transport = InMemoryTransport(
        discovery_store=InMemoryDiscoveryBackend(clock=clock),
        discovery_clock=clock,
    )
    jobs = SuperJobs(transport=transport, presence_clock=clock)
    worker_id = jobs.worker_id

    @jobs.handler(PLAIN_JOB)
    async def handler(request: Request, context: JobContext[None]) -> Result:
        return Result(value="ok")

    await jobs.start()
    await jobs.stop()
    await jobs.start()
    await jobs.stop()
    assert jobs.worker_id == worker_id


@pytest.mark.asyncio
async def test_provider_binding_publishes_capabilities_after_enter() -> None:
    clock = MutableClock(datetime(2026, 1, 6, tzinfo=UTC))
    store = InMemoryDiscoveryBackend(clock=clock)
    transport = InMemoryTransport(discovery_store=store, discovery_clock=clock)
    catalog = SuperJobs(transport=transport, presence_clock=clock).catalog
    entered = asyncio.Event()

    @asynccontextmanager
    async def provider():
        entered.set()

        async def inner(request: Request, context: JobContext[None]) -> Result:
            return Result(value="ok")

        yield inner

    catalog.bind(
        CAP_JOB,
        provider=provider,
        capabilities=lambda: WorkerCapability(locale="fr"),
    )
    jobs = SuperJobs(transport=transport, handlers=catalog, presence_clock=clock)
    await jobs.start()
    try:
        await asyncio.wait_for(entered.wait(), timeout=1)
        workers = await jobs.client(CAP_JOB).workers()
        assert len(workers) == 1
        assert workers[0].capabilities == WorkerCapability(locale="fr")
    finally:
        await jobs.stop()


@pytest.mark.asyncio
async def test_producer_without_discovery_backend_starts_with_optout() -> None:
    jobs = SuperJobs(transport=_ProducerOnlyBackend(), discovery=False)
    await jobs.start()
    assert jobs.started
    await jobs.stop()


@pytest.mark.asyncio
async def test_worker_without_discovery_backend_fails_at_publish() -> None:
    from superjobs.discovery.errors import UnsupportedDiscoveryBackendError

    jobs = SuperJobs(transport=_ProducerOnlyBackend())

    @jobs.handler(PLAIN_JOB)
    async def handler(request: Request, context: JobContext[None]) -> Result:
        return Result(value="ok")

    with pytest.raises(UnsupportedDiscoveryBackendError):
        await jobs.start()


@pytest.mark.asyncio
async def test_manual_update_then_refresh_keeps_factory() -> None:
    clock = MutableClock(datetime(2026, 1, 8, tzinfo=UTC))
    store = InMemoryDiscoveryBackend(clock=clock)
    transport = InMemoryTransport(discovery_store=store, discovery_clock=clock)
    jobs = SuperJobs(transport=transport, presence_clock=clock)
    calls = {"n": 0}

    def factory() -> WorkerCapability:
        calls["n"] += 1
        return WorkerCapability(locale=f"n{calls['n']}")

    @jobs.handler(CAP_JOB, capabilities=factory)
    async def handler(request: Request, context: JobContext[None]) -> Result:
        return Result(value="ok")

    await jobs.start()
    try:
        handle = jobs.worker(CAP_JOB)
        await handle.update_capabilities(WorkerCapability(locale="manual"))
        workers = await jobs.client(CAP_JOB).workers()
        assert workers[0].capabilities == WorkerCapability(locale="manual")
        await handle.refresh_capabilities()
        workers = await jobs.client(CAP_JOB).workers()
        assert workers[0].capabilities == WorkerCapability(locale="n2")
    finally:
        await jobs.stop()


@pytest.mark.asyncio
async def test_multiple_job_ids_publish_distinct_registrations() -> None:
    clock = MutableClock(datetime(2026, 1, 9, tzinfo=UTC))
    store = InMemoryDiscoveryBackend(clock=clock)
    transport = InMemoryTransport(discovery_store=store, discovery_clock=clock)
    jobs = SuperJobs(transport=transport, presence_clock=clock)
    other = Job(
        "tests.presence.other",
        version="v1",
        request=Request,
        result=Result,
        capabilities=WorkerCapability,
    )

    @jobs.handler(CAP_JOB, capabilities=WorkerCapability(locale="a"))
    async def first(request: Request, context: JobContext[None]) -> Result:
        return Result(value="a")

    @jobs.handler(other, capabilities=WorkerCapability(locale="b"))
    async def second(request: Request, context: JobContext[None]) -> Result:
        return Result(value="b")

    await jobs.start()
    try:
        assert len(await jobs.client(CAP_JOB).workers()) == 1
        assert len(await jobs.client(other).workers()) == 1
        assert await jobs.discovery.jobs() == sorted(
            [CAP_JOB.identity, other.identity],
            key=lambda identity: (identity.name, identity.version or ""),
        )
    finally:
        await jobs.stop()


@pytest.mark.asyncio
async def test_presence_config_lazy_from_discovery_store() -> None:
    clock = MutableClock(datetime(2026, 2, 1, tzinfo=UTC))
    store_config = PresenceConfig(
        renewal_interval=timedelta(seconds=7),
        lease_timeout=timedelta(seconds=21),
    )
    store = InMemoryDiscoveryBackend(config=store_config, clock=clock)
    transport = InMemoryTransport(discovery_store=store, discovery_clock=clock)
    jobs = SuperJobs(transport=transport, presence_clock=clock)

    @jobs.handler(CAP_JOB, capabilities=WorkerCapability(locale="de"))
    async def handler(request: Request, context: JobContext[None]) -> Result:
        return Result(value="ok")

    await jobs.start()
    try:
        workers = await jobs.client(CAP_JOB).workers()
        assert len(workers) == 1
        worker = workers[0]
        assert worker.expires_at == worker.last_seen_at + timedelta(seconds=21)
        assert jobs._presence._resolve_config().renewal_interval == timedelta(seconds=7)
    finally:
        await jobs.stop()


@pytest.mark.asyncio
async def test_malformed_capability_codec_fails_before_backend_write() -> None:
    from superjobs.payload.codec.payloadcodec import PayloadCodec
    from superjobs.registry import registry

    clock = MutableClock(datetime(2026, 2, 6, tzinfo=UTC))
    store = InMemoryDiscoveryBackend(clock=clock)
    transport = InMemoryTransport(discovery_store=store, discovery_clock=clock)
    writes_before = 0
    original_write = store.write_registration

    async def counting_write(registration, *, max_envelope_bytes):
        nonlocal writes_before
        writes_before += 1
        return await original_write(registration, max_envelope_bytes=max_envelope_bytes)

    store.write_registration = counting_write  # type: ignore[method-assign]

    base_codec = registry.get_payload_codec(WorkerCapability)

    class BrokenCodec(PayloadCodec[WorkerCapability]):
        def prepare(self, value: WorkerCapability) -> WorkerCapability:
            raise PayloadValidationError("broken codec")

    bad_job = Job(
        "tests.presence.bad.codec",
        version="v1",
        request=Request,
        result=Result,
        capabilities=WorkerCapability,
        capability_codec=BrokenCodec(base_codec.adapter, base_codec.wire),
    )
    jobs = SuperJobs(transport=transport, presence_clock=clock)

    @jobs.handler(bad_job, capabilities=WorkerCapability(locale="x"))
    async def handler(request: Request, context: JobContext[None]) -> Result:
        return Result(value="ok")

    with pytest.raises(PayloadValidationError, match="broken codec"):
        await jobs.start()
    assert writes_before == 0


@pytest.mark.asyncio
async def test_same_job_identity_two_runtimes_distinct_workers() -> None:
    clock = MutableClock(datetime(2026, 2, 7, tzinfo=UTC))
    store = InMemoryDiscoveryBackend(clock=clock)
    transport_a = InMemoryTransport(discovery_store=store, discovery_clock=clock)
    transport_b = InMemoryTransport(discovery_store=store, discovery_clock=clock)
    jobs_a = SuperJobs(transport=transport_a, presence_clock=clock)
    jobs_b = SuperJobs(transport=transport_b, presence_clock=clock)

    @jobs_a.handler(CAP_JOB, capabilities=WorkerCapability(locale="a"))
    async def handler_a(request: Request, context: JobContext[None]) -> Result:
        return Result(value="a")

    @jobs_b.handler(CAP_JOB, capabilities=WorkerCapability(locale="b"))
    async def handler_b(request: Request, context: JobContext[None]) -> Result:
        return Result(value="b")

    await jobs_a.start()
    await jobs_b.start()
    try:
        workers = await jobs_a.client(CAP_JOB).workers()
        assert len(workers) == 2
        assert {worker.worker_id for worker in workers} == {
            jobs_a.worker_id,
            jobs_b.worker_id,
        }
    finally:
        await jobs_b.stop()
        await jobs_a.stop()


@pytest.mark.asyncio
async def test_optional_none_capabilities_publish() -> None:
    clock = MutableClock(datetime(2026, 2, 8, tzinfo=UTC))
    store = InMemoryDiscoveryBackend(clock=clock)
    transport = InMemoryTransport(discovery_store=store, discovery_clock=clock)
    jobs = SuperJobs(transport=transport, presence_clock=clock)

    @jobs.handler(CAP_JOB, capabilities=None)
    async def handler(request: Request, context: JobContext[None]) -> Result:
        return Result(value="ok")

    await jobs.start()
    try:
        workers = await jobs.client(CAP_JOB).workers()
        assert len(workers) == 1
        assert workers[0].capabilities is None
    finally:
        await jobs.stop()


@pytest.mark.asyncio
async def test_initial_partial_publication_rolls_back_all_attempts() -> None:
    clock = MutableClock(datetime(2026, 2, 9, tzinfo=UTC))
    store = InMemoryDiscoveryBackend(clock=clock)
    transport = InMemoryTransport(discovery_store=store, discovery_clock=clock)
    jobs = SuperJobs(transport=transport, presence_clock=clock)
    other = Job(
        "tests.presence.partial",
        version="v1",
        request=Request,
        result=Result,
        capabilities=WorkerCapability,
    )

    @jobs.handler(CAP_JOB, capabilities=WorkerCapability(locale="ok"))
    async def first(request: Request, context: JobContext[None]) -> Result:
        return Result(value="ok")

    def bad_factory() -> WorkerCapability:
        raise ValueError("second handler failed")

    @jobs.handler(other, capabilities=bad_factory)
    async def second(request: Request, context: JobContext[None]) -> Result:
        return Result(value="no")

    with pytest.raises(ValueError, match="second handler failed"):
        await jobs.start()
    assert await jobs.client(CAP_JOB).workers() == []
    assert await jobs.client(other).workers() == []


class _MinimalDiscoveryBackend:
    def __init__(self, clock: MutableClock) -> None:
        self._clock = clock
        self._config = PresenceConfig()
        self._data: dict[tuple[str, str, str | None], bytes] = {}

    @property
    def config(self) -> PresenceConfig:
        return self._config

    async def read_snapshot(self, *, include_stale: bool = False):
        from superjobs.discovery.memory import DiscoverySnapshot

        now = self._clock()
        return DiscoverySnapshot(
            evaluated_at=now,
            entries=tuple(self._data.items()),
        )

    async def write_registration(self, registration, *, max_envelope_bytes: int) -> None:
        from superjobs.discovery.envelope import encode_envelope
        from superjobs.discovery.memory import registration_key

        key = registration_key(registration.worker_id, registration.job)
        self._data[key] = encode_envelope(registration, max_bytes=max_envelope_bytes)

    async def delete_registration(self, worker_id: str, job) -> None:
        from superjobs.discovery.memory import registration_key

        self._data.pop(registration_key(worker_id, job), None)


@pytest.mark.asyncio
async def test_custom_discovery_backend_three_method_protocol() -> None:
    clock = MutableClock(datetime(2026, 2, 10, tzinfo=UTC))
    backend = _MinimalDiscoveryBackend(clock)
    transport = InMemoryTransport(discovery_store=backend, discovery_clock=clock)
    jobs = SuperJobs(transport=transport, presence_clock=clock)

    @jobs.handler(CAP_JOB, capabilities=WorkerCapability(locale="min"))
    async def handler(request: Request, context: JobContext[None]) -> Result:
        return Result(value="ok")

    await jobs.start()
    try:
        assert len(await jobs.client(CAP_JOB).workers()) == 1
    finally:
        await jobs.stop()


@pytest.mark.asyncio
async def test_disabled_discovery_does_not_touch_registry() -> None:
    clock = MutableClock(datetime(2026, 2, 11, tzinfo=UTC))
    store = InMemoryDiscoveryBackend(clock=clock)
    shared_transport = InMemoryTransport(discovery_store=store, discovery_clock=clock)
    disabled_transport = InMemoryTransport(
        discovery_store=InMemoryDiscoveryBackend(clock=clock),
        discovery_clock=clock,
    )
    shared = SuperJobs(transport=shared_transport, presence_clock=clock)
    disabled = SuperJobs(transport=disabled_transport, discovery=False, presence_clock=clock)

    @shared.handler(CAP_JOB, capabilities=WorkerCapability(locale="shared"))
    async def handler(request: Request, context: JobContext[None]) -> Result:
        return Result(value="ok")

    @disabled.handler(PLAIN_JOB)
    async def disabled_handler(request: Request, context: JobContext[None]) -> Result:
        return Result(value="ok")

    await shared.start()
    await disabled.start()
    try:
        assert len(await shared.client(CAP_JOB).workers()) == 1
        assert await disabled.discovery.jobs() == []
        disabled_snapshot = await disabled_transport.discovery_backend.read_snapshot()
        assert disabled_snapshot.entries == ()
    finally:
        await disabled.stop()
        await shared.stop()


