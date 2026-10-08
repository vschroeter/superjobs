from __future__ import annotations

import asyncio
import importlib.util
import json
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import pytest
from pydantic import BaseModel, ConfigDict, ValidationError

from superjobs import Job, JobIdentity, SuperJobs
from superjobs.payload import PayloadValidationError
from superjobs.payload.adapter.protocol import PayloadAdapter, WireValue
from superjobs.payload.codec.implementations.msgpack import MsgpackCodec
from superjobs.discovery import memory as discovery_memory
from superjobs.discovery.capabilities import encode_application_capabilities
from superjobs.discovery.config import DEFAULT_MAX_ENVELOPE_BYTES, PresenceConfig
from superjobs.discovery.envelope import ENVELOPE_VERSION, encode_envelope
from superjobs.discovery.errors import (
    CapabilityDecodeError,
    DiscoveryEnvelopeError,
    DiscoveryEnvelopeSizeError,
    DiscoveryEnvelopeVersionError,
    DiscoveryUnavailableError,
    DiscoveryWriteError,
    UnsupportedDiscoveryBackendError,
)
from superjobs.discovery.memory import InMemoryDiscoveryBackend, registration_key
from superjobs.discovery.models import RawCapabilities, WorkerRegistrationState
from superjobs.discovery.reader import list_offered_jobs, list_worker_registrations
from superjobs.payload.codec.implementations.json import JsonCodec
from superjobs.payload.codec.payloadcodec import PayloadCodec
from superjobs.payload.adapter.implementations.python import PlainPythonPayloadAdapter
from superjobs.transport.in_memory import InMemoryTransport


@dataclass(frozen=True, slots=True, kw_only=True)
class WorkerCapability:
    locale: str


class PydanticCapability(BaseModel):
    model_config = ConfigDict(extra="forbid")
    locale: str


@dataclass(frozen=True, slots=True, kw_only=True)
class NestedMeta:
    region: str


@dataclass(frozen=True, slots=True, kw_only=True)
class NestedCapability:
    locale: str
    meta: NestedMeta


class PydanticNestedMeta(BaseModel):
    model_config = ConfigDict(extra="forbid")
    region: str


class PydanticNestedCapability(BaseModel):
    model_config = ConfigDict(extra="forbid")
    locale: str
    meta: PydanticNestedMeta


class _StrictLocaleCapabilityAdapter(PayloadAdapter[WorkerCapability]):
    def dump(self, value: WorkerCapability) -> WireValue:
        return {"locale": value.locale}

    def load(self, value: WireValue) -> WorkerCapability:
        if not isinstance(value, dict):
            raise PayloadValidationError("capability payload must be an object")
        locale = value.get("locale")
        if not isinstance(locale, str) or len(locale) < 2:
            raise PayloadValidationError("locale must be a string with at least two characters")
        if "unexpected" in value:
            raise PayloadValidationError("capability payload contains unknown fields")
        return WorkerCapability(locale=locale)

    def schema(self) -> None:
        return None


REPO_ROOT = Path(__file__).resolve().parents[1]
DISCOVERY_IMPORT_PROBE = REPO_ROOT / "examples" / "contract_interface" / "discovery_import_probe.py"
CONTRACT_EXAMPLE_SRC = (
    REPO_ROOT / "examples" / "contract_interface" / "superjobs_contract_example" / "src"
)


def _assert_strict_payload_validation_cause(cause: BaseException | None) -> None:
    assert cause is not None
    assert isinstance(cause, (PayloadValidationError, ValidationError))


class MutableClock:
    def __init__(self, start: datetime) -> None:
        if start.tzinfo is None:
            raise ValueError("start must be timezone-aware")
        self._now = start.astimezone(UTC)

    def advance(self, delta: timedelta) -> None:
        self._now += delta

    def __call__(self) -> datetime:
        return self._now


class _InjectingBackend(InMemoryDiscoveryBackend):
    def inject_raw(
        self,
        key: tuple[str, str, str | None],
        data: bytes,
        *,
        last_acknowledged_at: datetime,
    ) -> None:
        self._live[key] = discovery_memory._LiveEntry(
            data=data,
            last_acknowledged_at=last_acknowledged_at.astimezone(UTC),
        )


async def _seed(
    store: InMemoryDiscoveryBackend,
    *,
    worker_id: str,
    job: JobIdentity,
    clock: MutableClock,
    state: WorkerRegistrationState = WorkerRegistrationState.READY,
    expires_at: datetime | None = None,
    capabilities: RawCapabilities | None = None,
    lease_seconds: float = 30,
) -> None:
    now = clock()
    stored = store.build_registration(
        worker_id=worker_id,
        job=job,
        state=state,
        registered_at=now,
        last_seen_at=now,
        expires_at=expires_at or (now + timedelta(seconds=lease_seconds)),
        capabilities=capabilities,
    )
    await store.write_registration(
        stored,
        max_envelope_bytes=store.config.max_envelope_bytes,
    )


def _transport(store: InMemoryDiscoveryBackend, clock: MutableClock) -> InMemoryTransport:
    return InMemoryTransport(discovery_store=store, discovery_clock=clock)


class _SlowDiscoveryBackend:
    def __init__(self, delay: float, config: PresenceConfig) -> None:
        self._delay = delay
        self.config = config

    async def read_snapshot(self, *, include_stale: bool = False) -> Any:
        await asyncio.sleep(self._delay)
        return discovery_memory.DiscoverySnapshot(
            evaluated_at=datetime.now(tz=UTC),
            entries=(),
        )


class _SlowTransport:
    def __init__(self, backend: _SlowDiscoveryBackend) -> None:
        self.discovery_backend = backend


class _BrokenDiscoveryBackend:
    def __init__(self) -> None:
        self.config = PresenceConfig()

    async def read_snapshot(self, *, include_stale: bool = False) -> Any:
        raise PermissionError("registry read denied")


class _ThirdPartyTransportWithoutDiscovery:
    """Minimal stand-in for a foreign backend that does not expose discovery."""

    async def start(self) -> None:
        return

    async def stop(self) -> None:
        return


@pytest.mark.asyncio
async def test_empty_registry_all_public_query_forms() -> None:
    start = datetime(2026, 10, 6, 12, 0, tzinfo=UTC)
    clock = MutableClock(start)
    transport = _transport(InMemoryDiscoveryBackend(clock=clock), clock)
    jobs = SuperJobs(transport=transport)
    job = Job("tests.discovery.empty", version="v1", request=None, result=WorkerCapability)
    cap_job = Job(
        "tests.discovery.empty.cap",
        version="v1",
        request=None,
        result=WorkerCapability,
        capabilities=WorkerCapability,
    )
    async with jobs:
        assert await jobs.discovery.jobs() == []
        assert await jobs.discovery.workers(job) == []
        assert await jobs.discovery.workers(cap_job) == []
        assert await jobs.discovery.workers(job.identity) == []
        assert await jobs.client(job).workers() == []
        assert await jobs.client(cap_job).workers() == []


@pytest.mark.asyncio
async def test_namespace_and_client_equivalence() -> None:
    start = datetime(2026, 10, 6, 12, 0, tzinfo=UTC)
    clock = MutableClock(start)
    store = InMemoryDiscoveryBackend(clock=clock)
    transport = _transport(store, clock)
    job = Job(
        "tests.discovery.equiv",
        version="v1",
        request=None,
        result=WorkerCapability,
        capabilities=WorkerCapability,
    )
    raw = encode_application_capabilities(job, WorkerCapability(locale="de"))
    await _seed(store, worker_id="w-1", job=job.identity, clock=clock, capabilities=raw)
    jobs = SuperJobs(transport=transport)
    async with jobs:
        from_client = await jobs.client(job).workers()
        from_namespace = await jobs.discovery.workers(job)
        assert from_client == from_namespace
        assert from_client[0].capabilities == WorkerCapability(locale="de")
        assert await jobs.discovery.jobs() == [job.identity]


@pytest.mark.asyncio
async def test_late_runtimes_share_discovery_store() -> None:
    start = datetime(2026, 10, 6, 12, 0, tzinfo=UTC)
    clock = MutableClock(start)
    store = InMemoryDiscoveryBackend(clock=clock)
    job = Job("tests.discovery.shared", version="v1", request=None, result=WorkerCapability)
    await _seed(store, worker_id="w-1", job=job.identity, clock=clock)
    first = SuperJobs(transport=_transport(store, clock))
    second = SuperJobs(transport=_transport(store, clock))
    async with first:
        assert await first.discovery.jobs() == [job.identity]
    async with second:
        workers = await second.client(job).workers()
        assert workers[0].worker_id == "w-1"


@pytest.mark.asyncio
async def test_producer_runtime_sees_seeded_workers_without_handlers() -> None:
    start = datetime(2026, 10, 6, 12, 0, tzinfo=UTC)
    clock = MutableClock(start)
    store = InMemoryDiscoveryBackend(clock=clock)
    job = Job("tests.discovery.producer", version="v1", request=None, result=WorkerCapability)
    await _seed(store, worker_id="remote-worker", job=job.identity, clock=clock)
    producer = SuperJobs(transport=_transport(store, clock))
    async with producer:
        workers = await producer.client(job).workers()
        assert len(workers) == 1
        assert workers[0].worker_id == "remote-worker"


@pytest.mark.asyncio
async def test_unavailable_registry_raises_for_all_query_forms() -> None:
    job = Job("tests.discovery.unavailable", version="v1", request=None, result=WorkerCapability)
    transport = InMemoryTransport(discovery_store=_BrokenDiscoveryBackend())
    jobs = SuperJobs(transport=transport)
    async with jobs:
        with pytest.raises(DiscoveryUnavailableError) as unavailable:
            await jobs.discovery.jobs()
        assert isinstance(unavailable.value.__cause__, PermissionError)
        with pytest.raises(DiscoveryUnavailableError):
            await jobs.discovery.workers(job)
        with pytest.raises(DiscoveryUnavailableError):
            await jobs.discovery.workers(job.identity)
        with pytest.raises(DiscoveryUnavailableError):
            await jobs.client(job).workers()


@pytest.mark.asyncio
async def test_bounded_read_timeout_surfaces_unavailable() -> None:
    config = PresenceConfig(read_timeout=timedelta(milliseconds=50))
    transport = _SlowTransport(_SlowDiscoveryBackend(delay=2.0, config=config))
    job = Job("tests.discovery.timeout", version="v1", request=None, result=WorkerCapability)
    with pytest.raises(DiscoveryUnavailableError) as unavailable:
        await list_worker_registrations(transport, job)
    assert isinstance(unavailable.value.__cause__, asyncio.TimeoutError)


@pytest.mark.asyncio
async def test_cancelled_discovery_read_propagates_cancellation() -> None:
    transport = _SlowTransport(_SlowDiscoveryBackend(delay=5.0, config=PresenceConfig()))
    job = Job("tests.discovery.cancel", version="v1", request=None, result=WorkerCapability)
    task = asyncio.create_task(list_worker_registrations(transport, job))
    await asyncio.sleep(0.05)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task


@pytest.mark.asyncio
async def test_multiple_workers_versions_and_unversioned() -> None:
    start = datetime(2026, 10, 6, 12, 0, tzinfo=UTC)
    clock = MutableClock(start)
    store = InMemoryDiscoveryBackend(clock=clock)
    v1 = JobIdentity(name="tests.discovery.multi", version="v1")
    v2 = JobIdentity(name="tests.discovery.multi", version="v2")
    unversioned = JobIdentity(name="tests.discovery.unversioned", version=None)
    await _seed(store, worker_id="a", job=v1, clock=clock)
    await _seed(store, worker_id="b", job=v2, clock=clock)
    await _seed(store, worker_id="c", job=v2, clock=clock)
    await _seed(store, worker_id="d", job=unversioned, clock=clock)
    transport = _transport(store, clock)
    job_v1 = Job(v1.name, version=v1.version, request=None, result=WorkerCapability)
    assert len(await list_worker_registrations(transport, job_v1)) == 1
    assert set(await list_offered_jobs(transport)) == {v1, v2, unversioned}


@pytest.mark.asyncio
async def test_offered_identity_removed_when_last_worker_expires() -> None:
    start = datetime(2026, 10, 6, 12, 0, tzinfo=UTC)
    clock = MutableClock(start)
    store = InMemoryDiscoveryBackend(clock=clock)
    identity = JobIdentity(name="tests.discovery.expire_identity", version="v1")
    await _seed(
        store,
        worker_id="only",
        job=identity,
        clock=clock,
        expires_at=start + timedelta(seconds=30),
    )
    transport = _transport(store, clock)
    assert await list_offered_jobs(transport) == [identity]
    clock.advance(timedelta(seconds=31))
    assert await list_offered_jobs(transport) == []


@pytest.mark.asyncio
async def test_offered_identity_removed_after_explicit_delete() -> None:
    start = datetime(2026, 10, 6, 12, 0, tzinfo=UTC)
    clock = MutableClock(start)
    store = InMemoryDiscoveryBackend(clock=clock)
    identity = JobIdentity(name="tests.discovery.delete_identity", version="v1")
    await _seed(store, worker_id="only", job=identity, clock=clock)
    transport = _transport(store, clock)
    assert await list_offered_jobs(transport) == [identity]
    await store.delete_registration("only", identity)
    assert await list_offered_jobs(transport) == []


@pytest.mark.asyncio
async def test_draining_workers_excluded_from_active_queries() -> None:
    start = datetime(2026, 10, 6, 12, 0, tzinfo=UTC)
    clock = MutableClock(start)
    store = InMemoryDiscoveryBackend(clock=clock)
    identity = JobIdentity(name="tests.discovery.draining", version="v1")
    now = clock()
    draining = store.build_registration(
        worker_id="w-1",
        job=identity,
        state=WorkerRegistrationState.DRAINING,
        registered_at=now,
        last_seen_at=now,
        expires_at=now + timedelta(seconds=30),
        capabilities=None,
    )
    await store.write_registration(draining, max_envelope_bytes=store.config.max_envelope_bytes)
    transport = _transport(store, clock)
    job = Job(identity.name, version=identity.version, request=None, result=WorkerCapability)
    assert await list_worker_registrations(transport, job) == []
    stale = await list_worker_registrations(transport, job, include_stale=True)
    assert len(stale) == 1
    assert stale[0].state == WorkerRegistrationState.DRAINING


@pytest.mark.asyncio
async def test_exact_lease_boundary_excludes_worker() -> None:
    start = datetime(2026, 10, 6, 12, 0, tzinfo=UTC)
    clock = MutableClock(start)
    store = InMemoryDiscoveryBackend(clock=clock)
    identity = JobIdentity(name="tests.discovery.boundary", version="v1")
    expires = start + timedelta(seconds=30)
    await _seed(store, worker_id="w-1", job=identity, clock=clock, expires_at=expires)
    transport = _transport(store, clock)
    job = Job(identity.name, version=identity.version, request=None, result=WorkerCapability)
    assert len(await list_worker_registrations(transport, job)) == 1
    clock.advance(timedelta(seconds=30))
    assert await list_worker_registrations(transport, job) == []


@pytest.mark.asyncio
async def test_stale_inspection_retains_last_seen_without_active_count() -> None:
    start = datetime(2026, 10, 6, 12, 0, tzinfo=UTC)
    clock = MutableClock(start)
    store = InMemoryDiscoveryBackend(clock=clock)
    identity = JobIdentity(name="tests.discovery.stale", version="v1")
    await _seed(
        store,
        worker_id="w-1",
        job=identity,
        clock=clock,
        expires_at=start + timedelta(seconds=30),
    )
    transport = _transport(store, clock)
    job = Job(identity.name, version=identity.version, request=None, result=WorkerCapability)
    assert len(await list_worker_registrations(transport, job)) == 1
    clock.advance(timedelta(seconds=31))
    assert await list_worker_registrations(transport, job) == []
    stale = await list_worker_registrations(transport, job, include_stale=True)
    assert len(stale) == 1
    assert stale[0].last_seen_at == start


@pytest.mark.asyncio
async def test_injected_unknown_envelope_version_fails_on_read() -> None:
    start = datetime(2026, 10, 6, 12, 0, tzinfo=UTC)
    clock = MutableClock(start)
    store = _InjectingBackend(clock=clock)
    identity = JobIdentity(name="tests.discovery.version", version="v1")
    job = Job(identity.name, version=identity.version, request=None, result=WorkerCapability)
    body = {
        "envelope_version": ENVELOPE_VERSION + 1,
        "worker_id": "worker-1",
        "job": {"name": identity.name, "version": identity.version},
        "state": "ready",
        "registered_at": start.isoformat(),
        "last_seen_at": start.isoformat(),
        "expires_at": (start + timedelta(seconds=30)).isoformat(),
        "capabilities": None,
        "metadata": None,
    }
    key = registration_key("worker-1", identity)
    store.inject_raw(key, json.dumps(body).encode(), last_acknowledged_at=start)
    transport = _transport(store, clock)
    with pytest.raises(DiscoveryEnvelopeVersionError):
        await list_worker_registrations(transport, job)


@pytest.mark.asyncio
async def test_injected_key_envelope_mismatch_fails_on_read() -> None:
    start = datetime(2026, 10, 6, 12, 0, tzinfo=UTC)
    clock = MutableClock(start)
    store = _InjectingBackend(clock=clock)
    identity = JobIdentity(name="tests.discovery.key", version="v1")
    other = JobIdentity(name="tests.discovery.other", version="v1")
    reg = store.build_registration(
        worker_id="worker-1",
        job=identity,
        state=WorkerRegistrationState.READY,
        registered_at=start,
        last_seen_at=start,
        expires_at=start + timedelta(seconds=30),
        capabilities=None,
    )
    encoded = encode_envelope(reg, max_bytes=store.config.max_envelope_bytes)
    wrong_key = registration_key("worker-1", other)
    store.inject_raw(wrong_key, encoded, last_acknowledged_at=start)
    transport = _transport(store, clock)
    job = Job(identity.name, version=identity.version, request=None, result=WorkerCapability)
    with pytest.raises(DiscoveryEnvelopeError, match="identity"):
        await list_worker_registrations(transport, job)


@pytest.mark.asyncio
async def test_transport_without_discovery_backend_raises_for_all_query_forms() -> None:
    transport = _ThirdPartyTransportWithoutDiscovery()
    job = Job("tests.discovery.unsupported", version="v1", request=None, result=WorkerCapability)
    cap_job = Job(
        "tests.discovery.unsupported.cap",
        version="v1",
        request=None,
        result=WorkerCapability,
        capabilities=WorkerCapability,
    )
    with pytest.raises(UnsupportedDiscoveryBackendError):
        await list_worker_registrations(transport, job)
    with pytest.raises(UnsupportedDiscoveryBackendError):
        await list_offered_jobs(transport)
    jobs = SuperJobs(transport=transport)
    async with jobs:
        with pytest.raises(UnsupportedDiscoveryBackendError):
            await jobs.discovery.jobs()
        with pytest.raises(UnsupportedDiscoveryBackendError):
            await jobs.discovery.workers(job.identity)
        with pytest.raises(UnsupportedDiscoveryBackendError):
            await jobs.discovery.workers(cap_job)
        with pytest.raises(UnsupportedDiscoveryBackendError):
            await jobs.client(cap_job).workers()


@pytest.mark.asyncio
async def test_capability_typed_job_accepts_none_capabilities() -> None:
    start = datetime(2026, 10, 6, 12, 0, tzinfo=UTC)
    clock = MutableClock(start)
    store = InMemoryDiscoveryBackend(clock=clock)
    job = Job(
        "tests.discovery.cap_none",
        version="v1",
        request=None,
        result=WorkerCapability,
        capabilities=WorkerCapability,
    )
    await _seed(store, worker_id="w-1", job=job.identity, clock=clock, capabilities=None)
    transport = _transport(store, clock)
    workers = await list_worker_registrations(transport, job)
    assert workers[0].capabilities is None


@pytest.mark.asyncio
async def test_discovery_import_probe_from_installed_contract_packages(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    start = datetime(2026, 10, 6, 12, 0, tzinfo=UTC)
    clock = MutableClock(start)
    probe = Path(__file__).with_name("discovery_import_probe.py")
    if not probe.is_file():
        probe = DISCOVERY_IMPORT_PROBE
        monkeypatch.syspath_prepend(str(CONTRACT_EXAMPLE_SRC))
    assert probe.is_file(), "Required discovery import probe is missing"
    spec = importlib.util.spec_from_file_location("discovery_import_probe", probe)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    await module.run_discovery_import_probe(clock=clock)


@pytest.mark.asyncio
async def test_strict_dataclass_round_trip() -> None:
    start = datetime(2026, 10, 6, 12, 0, tzinfo=UTC)
    clock = MutableClock(start)
    store = InMemoryDiscoveryBackend(clock=clock)
    job = Job(
        "tests.discovery.dataclass",
        version="v1",
        request=None,
        result=WorkerCapability,
        capabilities=WorkerCapability,
    )
    raw = encode_application_capabilities(job, WorkerCapability(locale="en"))
    await _seed(store, worker_id="w-1", job=job.identity, clock=clock, capabilities=raw)
    transport = _transport(store, clock)
    workers = await list_worker_registrations(transport, job)
    assert workers[0].capabilities == WorkerCapability(locale="en")


@pytest.mark.asyncio
async def test_strict_pydantic_capability_round_trip() -> None:
    start = datetime(2026, 10, 6, 12, 0, tzinfo=UTC)
    clock = MutableClock(start)
    store = InMemoryDiscoveryBackend(clock=clock)
    job = Job(
        "tests.discovery.pydantic",
        version="v1",
        request=None,
        result=WorkerCapability,
        capabilities=PydanticCapability,
    )
    raw = encode_application_capabilities(job, PydanticCapability(locale="fr"))
    await _seed(store, worker_id="w-1", job=job.identity, clock=clock, capabilities=raw)
    transport = _transport(store, clock)
    workers = await list_worker_registrations(transport, job)
    assert workers[0].capabilities == PydanticCapability(locale="fr")


@pytest.mark.asyncio
async def test_extra_field_wire_payload_fails_strict_typed_decode() -> None:
    start = datetime(2026, 10, 6, 12, 0, tzinfo=UTC)
    clock = MutableClock(start)
    store = InMemoryDiscoveryBackend(clock=clock)
    job = Job(
        "tests.discovery.extra",
        version="v1",
        request=None,
        result=WorkerCapability,
        capabilities=PydanticCapability,
    )
    codec = job.capability_codec
    assert codec is not None
    payload = codec.wire.encode({"locale": "de", "unexpected": True})
    raw = RawCapabilities(
        schema_id="PydanticCapability",
        media_type=codec.media_type,
        payload=payload,
    )
    await _seed(store, worker_id="w-1", job=job.identity, clock=clock, capabilities=raw)
    transport = _transport(store, clock)
    with pytest.raises(CapabilityDecodeError) as error:
        await list_worker_registrations(transport, job)
    _assert_strict_payload_validation_cause(error.value.__cause__)
    raw_workers = await list_worker_registrations(transport, job.identity)
    assert raw_workers[0].capabilities is not None
    assert raw_workers[0].capabilities.payload == payload


@pytest.mark.asyncio
async def test_nested_dataclass_capability_round_trip() -> None:
    start = datetime(2026, 10, 6, 12, 0, tzinfo=UTC)
    clock = MutableClock(start)
    store = InMemoryDiscoveryBackend(clock=clock)
    job = Job(
        "tests.discovery.nested_dataclass",
        version="v1",
        request=None,
        result=WorkerCapability,
        capabilities=NestedCapability,
    )
    value = NestedCapability(locale="en", meta=NestedMeta(region="eu"))
    raw = encode_application_capabilities(job, value)
    await _seed(store, worker_id="w-1", job=job.identity, clock=clock, capabilities=raw)
    transport = _transport(store, clock)
    workers = await list_worker_registrations(transport, job)
    assert workers[0].capabilities == value


@pytest.mark.asyncio
async def test_nested_pydantic_capability_round_trip() -> None:
    start = datetime(2026, 10, 6, 12, 0, tzinfo=UTC)
    clock = MutableClock(start)
    store = InMemoryDiscoveryBackend(clock=clock)
    job = Job(
        "tests.discovery.nested_pydantic",
        version="v1",
        request=None,
        result=WorkerCapability,
        capabilities=PydanticNestedCapability,
    )
    value = PydanticNestedCapability(locale="fr", meta=PydanticNestedMeta(region="eu-west"))
    raw = encode_application_capabilities(job, value)
    await _seed(store, worker_id="w-1", job=job.identity, clock=clock, capabilities=raw)
    transport = _transport(store, clock)
    workers = await list_worker_registrations(transport, job)
    assert workers[0].capabilities == value


@pytest.mark.asyncio
async def test_nested_extra_field_fails_strict_typed_decode() -> None:
    start = datetime(2026, 10, 6, 12, 0, tzinfo=UTC)
    clock = MutableClock(start)
    store = InMemoryDiscoveryBackend(clock=clock)
    job = Job(
        "tests.discovery.nested_extra",
        version="v1",
        request=None,
        result=WorkerCapability,
        capabilities=PydanticNestedCapability,
    )
    codec = job.capability_codec
    assert codec is not None
    payload = codec.wire.encode(
        {"locale": "de", "meta": {"region": "eu", "unexpected": True}},
    )
    raw = RawCapabilities(
        schema_id="PydanticNestedCapability",
        media_type=codec.media_type,
        payload=payload,
    )
    await _seed(store, worker_id="w-1", job=job.identity, clock=clock, capabilities=raw)
    transport = _transport(store, clock)
    with pytest.raises(CapabilityDecodeError) as error:
        await list_worker_registrations(transport, job)
    _assert_strict_payload_validation_cause(error.value.__cause__)


@pytest.mark.asyncio
async def test_nested_wrong_type_fails_strict_typed_decode() -> None:
    start = datetime(2026, 10, 6, 12, 0, tzinfo=UTC)
    clock = MutableClock(start)
    store = InMemoryDiscoveryBackend(clock=clock)
    job = Job(
        "tests.discovery.nested_wrong_type",
        version="v1",
        request=None,
        result=WorkerCapability,
        capabilities=NestedCapability,
    )
    codec = job.capability_codec
    assert codec is not None
    payload = codec.wire.encode({"locale": "de", "meta": {"region": 42}})
    raw = RawCapabilities(
        schema_id="NestedCapability",
        media_type=codec.media_type,
        payload=payload,
    )
    await _seed(store, worker_id="w-1", job=job.identity, clock=clock, capabilities=raw)
    transport = _transport(store, clock)
    with pytest.raises(CapabilityDecodeError) as error:
        await list_worker_registrations(transport, job)
    _assert_strict_payload_validation_cause(error.value.__cause__)


@pytest.mark.asyncio
async def test_differing_schema_id_still_decodes_when_payload_valid() -> None:
    start = datetime(2026, 10, 6, 12, 0, tzinfo=UTC)
    clock = MutableClock(start)
    store = InMemoryDiscoveryBackend(clock=clock)
    job = Job(
        "tests.discovery.schema_id",
        version="v1",
        request=None,
        result=WorkerCapability,
        capabilities=WorkerCapability,
    )
    encoded = encode_application_capabilities(job, WorkerCapability(locale="it"))
    raw = RawCapabilities(
        schema_id="legacy-schema",
        media_type=encoded.media_type,
        payload=encoded.payload,
    )
    await _seed(store, worker_id="w-1", job=job.identity, clock=clock, capabilities=raw)
    transport = _transport(store, clock)
    workers = await list_worker_registrations(transport, job)
    assert workers[0].capabilities == WorkerCapability(locale="it")


@pytest.mark.asyncio
async def test_custom_capability_codec_round_trip() -> None:
    start = datetime(2026, 10, 6, 12, 0, tzinfo=UTC)
    clock = MutableClock(start)
    store = InMemoryDiscoveryBackend(clock=clock)
    codec = PayloadCodec(PlainPythonPayloadAdapter(WorkerCapability), JsonCodec())
    job = Job(
        "tests.discovery.custom_codec",
        version="v1",
        request=None,
        result=WorkerCapability,
        capabilities=WorkerCapability,
        capability_codec=codec,
    )
    raw = encode_application_capabilities(job, WorkerCapability(locale="es"))
    await _seed(store, worker_id="w-1", job=job.identity, clock=clock, capabilities=raw)
    transport = _transport(store, clock)
    workers = await list_worker_registrations(transport, job)
    assert workers[0].capabilities == WorkerCapability(locale="es")


@pytest.mark.asyncio
async def test_custom_capability_adapter_validation_round_trip_and_rejection() -> None:
    start = datetime(2026, 10, 6, 12, 0, tzinfo=UTC)
    clock = MutableClock(start)
    store = InMemoryDiscoveryBackend(clock=clock)
    codec = PayloadCodec(_StrictLocaleCapabilityAdapter(), MsgpackCodec())
    job = Job(
        "tests.discovery.custom_adapter",
        version="v1",
        request=None,
        result=WorkerCapability,
        capabilities=WorkerCapability,
        capability_codec=codec,
    )
    raw = encode_application_capabilities(job, WorkerCapability(locale="es"))
    await _seed(store, worker_id="w-1", job=job.identity, clock=clock, capabilities=raw)
    transport = _transport(store, clock)
    workers = await list_worker_registrations(transport, job)
    assert workers[0].capabilities == WorkerCapability(locale="es")

    bad_payload = codec.wire.encode({"locale": "x", "unexpected": True})
    bad_raw = RawCapabilities(
        schema_id="WorkerCapability",
        media_type=codec.media_type,
        payload=bad_payload,
    )
    await _seed(store, worker_id="w-1", job=job.identity, clock=clock, capabilities=bad_raw)
    with pytest.raises(CapabilityDecodeError) as error:
        await list_worker_registrations(transport, job)
    assert isinstance(error.value.__cause__, PayloadValidationError)


@pytest.mark.asyncio
async def test_raw_identity_query_preserves_unknown_format() -> None:
    start = datetime(2026, 10, 6, 12, 0, tzinfo=UTC)
    clock = MutableClock(start)
    store = InMemoryDiscoveryBackend(clock=clock)
    identity = JobIdentity(name="tests.discovery.raw", version="v1")
    typed_job = Job(
        identity.name,
        version=identity.version,
        request=None,
        result=WorkerCapability,
        capabilities=WorkerCapability,
    )
    payload = b"\x81\xa6locale\xc2de"
    raw = RawCapabilities(
        schema_id="other",
        media_type="application/msgpack",
        payload=payload,
    )
    await _seed(store, worker_id="w-1", job=identity, clock=clock, capabilities=raw)
    transport = _transport(store, clock)
    with pytest.raises(CapabilityDecodeError):
        await list_worker_registrations(transport, typed_job)
    raw_workers = await list_worker_registrations(transport, identity)
    assert raw_workers[0].capabilities is not None
    assert raw_workers[0].capabilities.payload == payload


@pytest.mark.asyncio
async def test_capability_decode_error_includes_worker_context() -> None:
    start = datetime(2026, 10, 6, 12, 0, tzinfo=UTC)
    clock = MutableClock(start)
    store = InMemoryDiscoveryBackend(clock=clock)
    job = Job(
        "tests.discovery.context",
        version="v1",
        request=None,
        result=WorkerCapability,
        capabilities=WorkerCapability,
    )
    raw = RawCapabilities(
        schema_id="WorkerCapability",
        media_type="application/json",
        payload=b"{not-json",
    )
    await _seed(store, worker_id="ctx-worker", job=job.identity, clock=clock, capabilities=raw)
    transport = _transport(store, clock)
    with pytest.raises(CapabilityDecodeError) as error:
        await list_worker_registrations(transport, job)
    assert error.value.worker_id == "ctx-worker"
    assert error.value.job == job.identity


@pytest.mark.asyncio
async def test_untyped_job_rejects_application_capabilities() -> None:
    start = datetime(2026, 10, 6, 12, 0, tzinfo=UTC)
    clock = MutableClock(start)
    store = InMemoryDiscoveryBackend(clock=clock)
    job = Job("tests.discovery.untyped", version="v1", request=None, result=WorkerCapability)
    raw = RawCapabilities(
        schema_id="x",
        media_type="application/msgpack",
        payload=b"\x81\xa6locale\xc2de",
    )
    await _seed(store, worker_id="w-1", job=job.identity, clock=clock, capabilities=raw)
    transport = _transport(store, clock)
    with pytest.raises(CapabilityDecodeError):
        await list_worker_registrations(transport, job)


@pytest.mark.asyncio
async def test_invalid_replace_preserves_previous_snapshot() -> None:
    start = datetime(2026, 10, 6, 12, 0, tzinfo=UTC)
    clock = MutableClock(start)
    store = InMemoryDiscoveryBackend(clock=clock)
    identity = JobIdentity(name="tests.discovery.atomic", version="v1")
    await _seed(store, worker_id="worker-1", job=identity, clock=clock)
    key = registration_key("worker-1", identity)
    with pytest.raises(DiscoveryWriteError):
        await store.replace_registration_bytes(key, b"{not-json", max_envelope_bytes=32768)
    transport = _transport(store, clock)
    job = Job(identity.name, version=identity.version, request=None, result=WorkerCapability)
    assert len(await list_worker_registrations(transport, job)) == 1


@pytest.mark.asyncio
async def test_oversized_write_rejected_and_prior_retained() -> None:
    start = datetime(2026, 10, 6, 12, 0, tzinfo=UTC)
    clock = MutableClock(start)
    store = InMemoryDiscoveryBackend(
        config=PresenceConfig(max_envelope_bytes=512),
        clock=clock,
    )
    identity = JobIdentity(name="tests.discovery.size", version="v1")
    await _seed(store, worker_id="worker-1", job=identity, clock=clock)
    huge = store.build_registration(
        worker_id="worker-1",
        job=identity,
        state=WorkerRegistrationState.READY,
        registered_at=start,
        last_seen_at=start,
        expires_at=start + timedelta(seconds=30),
        capabilities=RawCapabilities(
            schema_id="big",
            media_type="application/octet-stream",
            payload=b"x" * 800,
        ),
    )
    with pytest.raises(DiscoveryWriteError):
        await store.write_registration(huge, max_envelope_bytes=store.config.max_envelope_bytes)
    transport = _transport(store, clock)
    job = Job(identity.name, version=identity.version, request=None, result=WorkerCapability)
    assert len(await list_worker_registrations(transport, job)) == 1


@pytest.mark.asyncio
async def test_reader_respects_configured_max_envelope_not_only_default() -> None:
    start = datetime(2026, 10, 6, 12, 0, tzinfo=UTC)
    clock = MutableClock(start)
    limit = 600
    store = InMemoryDiscoveryBackend(
        config=PresenceConfig(max_envelope_bytes=limit),
        clock=clock,
    )
    identity = JobIdentity(name="tests.discovery.decode_limit", version="v1")
    reg = store.build_registration(
        worker_id="worker-1",
        job=identity,
        state=WorkerRegistrationState.READY,
        registered_at=start,
        last_seen_at=start,
        expires_at=start + timedelta(seconds=30),
        capabilities=RawCapabilities(
            schema_id="x",
            media_type="application/octet-stream",
            payload=b"y" * 500,
        ),
    )
    encoded = encode_envelope(reg, max_bytes=DEFAULT_MAX_ENVELOPE_BYTES)
    key = registration_key("worker-1", identity)
    injecting = _InjectingBackend(config=store.config, clock=clock)
    injecting.inject_raw(key, encoded, last_acknowledged_at=start)
    transport = _transport(injecting, clock)
    job = Job(identity.name, version=identity.version, request=None, result=WorkerCapability)
    with pytest.raises(DiscoveryEnvelopeError):
        await list_worker_registrations(transport, job)


_PUBLIC_DISCOVERY_QUERY_FORMS = (
    "discovery_jobs",
    "discovery_raw_workers",
    "discovery_typed_workers",
    "client_workers",
)

_EARLY_DECODE_PAYLOAD_KINDS = (
    "invalid_json",
    "oversized",
    "unknown_field",
    "bad_version",
)


def _early_decode_payload(
    kind: str,
    *,
    identity: JobIdentity,
    start: datetime,
    max_bytes: int,
) -> tuple[bytes, type[DiscoveryEnvelopeError]]:
    base = {
        "envelope_version": ENVELOPE_VERSION,
        "worker_id": "other-worker",
        "job": {"name": identity.name, "version": identity.version},
        "state": "ready",
        "registered_at": start.isoformat(),
        "last_seen_at": start.isoformat(),
        "expires_at": (start + timedelta(seconds=30)).isoformat(),
        "capabilities": None,
        "metadata": None,
    }
    if kind == "invalid_json":
        return b"{not-json", DiscoveryEnvelopeError
    if kind == "oversized":
        return b"x" * (max_bytes + 1), DiscoveryEnvelopeSizeError
    if kind == "unknown_field":
        body = {**base, "unexpected": True}
        return json.dumps(body).encode(), DiscoveryEnvelopeError
    if kind == "bad_version":
        body = {**base, "envelope_version": 99}
        del body["worker_id"]
        return json.dumps(body).encode(), DiscoveryEnvelopeVersionError
    raise ValueError(f"unknown payload kind: {kind}")


@pytest.mark.asyncio
@pytest.mark.parametrize("query_form", _PUBLIC_DISCOVERY_QUERY_FORMS)
@pytest.mark.parametrize("payload_kind", _EARLY_DECODE_PAYLOAD_KINDS)
async def test_early_decode_errors_retain_registration_key_context(
    query_form: str,
    payload_kind: str,
) -> None:
    start = datetime(2026, 10, 6, 12, 0, tzinfo=UTC)
    clock = MutableClock(start)
    max_bytes = 512
    store = InMemoryDiscoveryBackend(
        config=PresenceConfig(max_envelope_bytes=max_bytes),
        clock=clock,
    )
    identity = JobIdentity(name="tests.discovery.key_context", version="v1")
    key = registration_key("ctx-key-worker", identity)
    payload, expected_type = _early_decode_payload(
        payload_kind,
        identity=identity,
        start=start,
        max_bytes=max_bytes,
    )
    injecting = _InjectingBackend(config=store.config, clock=clock)
    injecting.inject_raw(key, payload, last_acknowledged_at=start)
    transport = _transport(injecting, clock)
    cap_job = Job(
        identity.name,
        version=identity.version,
        request=None,
        result=WorkerCapability,
        capabilities=WorkerCapability,
    )
    jobs = SuperJobs(transport=transport)

    async def _run_query() -> None:
        async with jobs:
            if query_form == "discovery_jobs":
                await jobs.discovery.jobs()
            elif query_form == "discovery_raw_workers":
                await jobs.discovery.workers(identity)
            elif query_form == "discovery_typed_workers":
                await jobs.discovery.workers(cap_job)
            elif query_form == "client_workers":
                await jobs.client(cap_job).workers()
            else:
                raise ValueError(query_form)

    with pytest.raises(expected_type) as error:
        await _run_query()
    exc = error.value
    assert exc.worker_id == "ctx-key-worker"
    assert exc.job == identity
    assert "ctx-key-worker" in str(exc)
    if payload_kind == "oversized":
        assert isinstance(exc, DiscoveryEnvelopeSizeError)
        assert exc.size == max_bytes + 1
        assert exc.limit == max_bytes
    if payload_kind == "bad_version":
        assert isinstance(exc, DiscoveryEnvelopeVersionError)
        assert exc.version == 99
    if payload_kind == "invalid_json":
        assert isinstance(exc.__cause__, json.JSONDecodeError)
