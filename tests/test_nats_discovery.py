from __future__ import annotations

import asyncio
import base64
import json
import uuid
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest
from faststream.nats import NatsBroker
from nats.js.api import KeyValueConfig, StorageType

from superjobs import Job, JobIdentity, SuperJobs
from superjobs.discovery.capabilities import capability_schema_id, encode_application_capabilities
from superjobs.discovery.config import DEFAULT_MAX_ENVELOPE_BYTES, PresenceConfig
from superjobs.discovery.envelope import ENVELOPE_VERSION, encode_envelope
from superjobs.discovery.errors import (
    CapabilityDecodeError,
    DiscoveryConfigurationError,
    DiscoveryEnvelopeError,
    DiscoveryEnvelopeVersionError,
    DiscoveryUnavailableError,
    DiscoveryWriteError,
)
from superjobs.discovery.memory import InMemoryDiscoveryBackend
from superjobs.discovery.models import WorkerRegistrationMetadata, WorkerRegistrationState
from superjobs.discovery.nats import (
    NatsDiscoveryBackend,
    discovery_bucket_name,
    registration_kv_key,
)
from superjobs.discovery.reader import filter_active_entries, list_offered_jobs, list_worker_registrations
from superjobs.transport.nats_backend import NatsJobBackend
from tests.support.nats_harness.server import OwnedNatsServer


@dataclass(frozen=True, slots=True, kw_only=True)
class WorkerCapability:
    locale: str


def _short_presence_config(**overrides: object) -> PresenceConfig:
    base = {
        "renewal_interval": timedelta(seconds=1),
        "lease_timeout": timedelta(seconds=3),
        "stale_retention": timedelta(seconds=4),
        "read_timeout": timedelta(seconds=5),
    }
    base.update(overrides)
    return PresenceConfig(**base)


async def _connect_backend(
    nats_url: str,
    queue_config,
    *,
    presence_config: PresenceConfig | None = None,
    discovery_provision: bool = True,
) -> tuple[NatsBroker, NatsJobBackend]:
    broker = NatsBroker(nats_url, connect_timeout=2, max_reconnect_attempts=0)
    backend = NatsJobBackend(
        broker,
        queue_config,
        presence_config=presence_config or _short_presence_config(),
        discovery_provision=discovery_provision,
    )
    await backend.start()
    return broker, backend


def _build_registration(
    *,
    worker_id: str,
    job: JobIdentity,
    at: datetime,
    lease_seconds: float = 30.0,
    state: WorkerRegistrationState = WorkerRegistrationState.READY,
    capabilities=None,
) -> object:
    helper = InMemoryDiscoveryBackend()
    return helper.build_registration(
        worker_id=worker_id,
        job=job,
        state=state,
        registered_at=at,
        last_seen_at=at,
        expires_at=at + timedelta(seconds=lease_seconds),
        capabilities=capabilities,
    )


def _capability_job(name: str, *, version: str = "v1") -> Job:
    return Job(
        name,
        version=version,
        request=None,
        result=WorkerCapability,
        capabilities=WorkerCapability,
    )


@pytest.mark.nats
@pytest.mark.asyncio
async def test_fresh_bucket_provisions_and_reads_empty(
    nats_url: str,
    nats_queue_config,
) -> None:
    _, backend = await _connect_backend(nats_url, nats_queue_config)
    try:
        snapshot = await backend.discovery_backend.read_snapshot()
        assert snapshot.entries == ()
        assert await list_offered_jobs(backend) == []
    finally:
        await backend.stop()


@pytest.mark.nats
@pytest.mark.asyncio
async def test_existing_compatible_bucket_reused(
    nats_url: str,
    nats_queue_config,
) -> None:
    bucket = discovery_bucket_name(nats_queue_config.subject_prefix)
    config = _short_presence_config()
    broker = NatsBroker(nats_url, connect_timeout=2, max_reconnect_attempts=0)
    await broker.start()
    try:
        js = broker.connection.jetstream()
        await js.create_key_value(
            KeyValueConfig(
                bucket=bucket,
                history=1,
                storage=StorageType.FILE,
                ttl=float(config.stale_retention.total_seconds()),
                max_value_size=config.max_envelope_bytes,
            ),
        )
        backend = NatsJobBackend(
            broker,
            nats_queue_config,
            presence_config=config,
            discovery_provision=False,
        )
        await backend.start()
        identity = JobIdentity(name="tests.nats.existing", version="v1")
        reg = _build_registration(
            worker_id="worker-1",
            job=identity,
            at=datetime.now(tz=UTC),
        )
        await backend.discovery_backend.write_registration(
            reg,
            max_envelope_bytes=config.max_envelope_bytes,
        )
        assert await list_offered_jobs(backend) == [identity]
        await backend.stop()
    finally:
        await broker.stop()


@pytest.mark.nats
@pytest.mark.parametrize(
    ("mismatch", "pattern"),
    [
        ("history", "history"),
        ("storage", "storage"),
        ("ttl", "ttl"),
        ("maxsize", "max_value_size"),
    ],
)
@pytest.mark.asyncio
async def test_incompatible_existing_bucket_rejected(
    nats_url: str,
    nats_queue_config,
    mismatch: str,
    pattern: str,
) -> None:
    bucket = discovery_bucket_name(nats_queue_config.subject_prefix)
    expected = _short_presence_config()
    broker = NatsBroker(nats_url, connect_timeout=2, max_reconnect_attempts=0)
    await broker.start()
    try:
        js = broker.connection.jetstream()
        kwargs = {
            "bucket": bucket,
            "history": 1,
            "storage": StorageType.FILE,
            "ttl": float(expected.stale_retention.total_seconds()),
            "max_value_size": expected.max_envelope_bytes,
        }
        if mismatch == "history":
            kwargs["history"] = 2
        elif mismatch == "storage":
            kwargs["storage"] = StorageType.MEMORY
        elif mismatch == "ttl":
            kwargs["ttl"] = float(expected.stale_retention.total_seconds()) + 60.0
        else:
            kwargs["max_value_size"] = expected.max_envelope_bytes + 1
        await js.create_key_value(KeyValueConfig(**kwargs))
        backend = NatsJobBackend(
            broker,
            nats_queue_config,
            presence_config=expected,
            discovery_provision=False,
        )
        await backend.start()
        with pytest.raises(DiscoveryConfigurationError, match=pattern):
            await backend.discovery_backend.read_snapshot()
        await backend.stop()
    finally:
        await broker.stop()


@pytest.mark.nats
@pytest.mark.asyncio
async def test_long_retention_ttl_validates_in_seconds(
    nats_url: str,
    nats_queue_config,
) -> None:
    config = PresenceConfig(
        renewal_interval=timedelta(seconds=10),
        lease_timeout=timedelta(seconds=30),
        stale_retention=timedelta(hours=25),
        read_timeout=timedelta(seconds=10),
    )
    bucket = discovery_bucket_name(nats_queue_config.subject_prefix)
    broker = NatsBroker(nats_url, connect_timeout=2, max_reconnect_attempts=0)
    await broker.start()
    try:
        js = broker.connection.jetstream()
        await js.create_key_value(
            KeyValueConfig(
                bucket=bucket,
                history=1,
                storage=StorageType.FILE,
                ttl=float(config.stale_retention.total_seconds()),
                max_value_size=config.max_envelope_bytes,
            ),
        )
        backend = NatsJobBackend(
            broker,
            nats_queue_config,
            presence_config=config,
            discovery_provision=False,
        )
        await backend.start()
        assert await backend.discovery_backend.read_snapshot() is not None
        await backend.stop()
    finally:
        await broker.stop()


@pytest.mark.nats
@pytest.mark.asyncio
async def test_late_client_reads_persisted_registration(
    nats_url: str,
    nats_queue_config,
) -> None:
    _, writer = await _connect_backend(nats_url, nats_queue_config)
    presence = writer.discovery_backend.config
    identity = JobIdentity(name="tests.nats.late", version="v1")
    reg = _build_registration(
        worker_id="worker-late",
        job=identity,
        at=datetime.now(tz=UTC),
    )
    await writer.discovery_backend.write_registration(
        reg,
        max_envelope_bytes=presence.max_envelope_bytes,
    )
    await writer.stop()

    _, reader = await _connect_backend(
        nats_url,
        nats_queue_config,
        presence_config=presence,
        discovery_provision=False,
    )
    try:
        workers = await list_worker_registrations(reader, identity)
        assert len(workers) == 1
        assert workers[0].worker_id == "worker-late"
    finally:
        await reader.stop()


@pytest.mark.nats
@pytest.mark.asyncio
async def test_multiple_workers_versions_and_offered_dedup(
    nats_url: str,
    nats_queue_config,
) -> None:
    _, backend = await _connect_backend(nats_url, nats_queue_config)
    try:
        now = datetime.now(tz=UTC)
        identity_v1 = JobIdentity(name="tests.nats.multi", version="v1")
        identity_v2 = JobIdentity(name="tests.nats.multi", version="v2")
        job_v1 = _capability_job(identity_v1.name, version=identity_v1.version or "v1")
        caps = encode_application_capabilities(job_v1, WorkerCapability(locale="de"))
        for worker_id, identity in (
            ("worker-a", identity_v1),
            ("worker-b", identity_v1),
            ("worker-c", identity_v2),
        ):
            reg = _build_registration(
                worker_id=worker_id,
                job=identity,
                at=now,
                capabilities=caps if identity == identity_v1 else None,
            )
            await backend.discovery_backend.write_registration(
                reg,
                max_envelope_bytes=backend.discovery_backend.config.max_envelope_bytes,
            )
        assert await list_offered_jobs(backend) == [identity_v1, identity_v2]
        typed = await list_worker_registrations(backend, job_v1)
        raw = await list_worker_registrations(backend, identity_v1)
        assert len(typed) == 2
        assert len(raw) == 2
        assert typed[0].capabilities.locale == "de"
        assert raw[0].capabilities.payload
    finally:
        await backend.stop()


@pytest.mark.nats
@pytest.mark.asyncio
async def test_superjobs_client_and_discovery_workers_equivalence(
    nats_url: str,
    nats_queue_config,
) -> None:
    jobs = SuperJobs(
        broker=NatsBroker(nats_url, connect_timeout=2, max_reconnect_attempts=0),
        queue_config=nats_queue_config,
    )
    job = _capability_job(f"tests.nats.client.{uuid.uuid4().hex}")
    backend = jobs.transport
    assert isinstance(backend, NatsJobBackend)
    await jobs.start()
    now = datetime.now(tz=UTC)
    caps = encode_application_capabilities(job, WorkerCapability(locale="fr-FR"))
    reg = _build_registration(
        worker_id="worker-client",
        job=job.identity,
        at=now,
        capabilities=caps,
    )
    await backend.discovery_backend.write_registration(
        reg,
        max_envelope_bytes=backend.discovery_backend.config.max_envelope_bytes,
    )
    try:
        from_reader = await list_worker_registrations(backend, job)
        from_namespace = await jobs.discovery.workers(job)
        from_client = await jobs.client(job).workers()
        assert from_reader[0].capabilities.locale == "fr-FR"
        assert from_namespace[0].capabilities.locale == "fr-FR"
        assert from_client[0].capabilities.locale == "fr-FR"
        raw = await jobs.discovery.workers(job.identity)
        assert raw[0].capabilities.payload == caps.payload
    finally:
        await jobs.stop()


@pytest.mark.nats
@pytest.mark.asyncio
async def test_capability_replacement_and_delete(
    nats_url: str,
    nats_queue_config,
) -> None:
    _, backend = await _connect_backend(nats_url, nats_queue_config)
    job = _capability_job("tests.nats.capreplace")
    identity = job.identity
    now = datetime.now(tz=UTC)
    try:
        first_caps = encode_application_capabilities(job, WorkerCapability(locale="de"))
        await backend.discovery_backend.write_registration(
            _build_registration(
                worker_id="worker-1",
                job=identity,
                at=now,
                capabilities=first_caps,
            ),
            max_envelope_bytes=backend.discovery_backend.config.max_envelope_bytes,
        )
        typed_first = await list_worker_registrations(backend, job)
        assert typed_first[0].capabilities.locale == "de"
        second_caps = encode_application_capabilities(job, WorkerCapability(locale="pl"))
        await backend.discovery_backend.write_registration(
            _build_registration(
                worker_id="worker-1",
                job=identity,
                at=now + timedelta(seconds=1),
                capabilities=second_caps,
            ),
            max_envelope_bytes=backend.discovery_backend.config.max_envelope_bytes,
        )
        typed_second = await list_worker_registrations(backend, job)
        assert typed_second[0].capabilities.locale == "pl"
        await backend.discovery_backend.delete_registration("worker-1", identity)
        assert await list_worker_registrations(backend, job) == []
    finally:
        await backend.stop()


@pytest.mark.nats
@pytest.mark.asyncio
async def test_delete_between_enumeration_and_get_is_benign(
    nats_url: str,
    nats_queue_config,
) -> None:
    _, backend = await _connect_backend(nats_url, nats_queue_config)
    identity = JobIdentity(name="tests.nats.race", version="v1")
    now = datetime.now(tz=UTC)
    key = registration_kv_key("worker-race", identity)
    try:
        await backend.discovery_backend.write_registration(
            _build_registration(worker_id="worker-race", job=identity, at=now),
            max_envelope_bytes=backend.discovery_backend.config.max_envelope_bytes,
        )
        discovery = backend.discovery_backend
        original_fetch = discovery._fetch_kv_entry

        async def fetch_and_delete(kv, key_name: str):
            if key_name == key:
                await discovery.delete_registration("worker-race", identity)
            return await original_fetch(kv, key_name)

        discovery._fetch_kv_entry = fetch_and_delete
        snapshot = await discovery.read_snapshot()
        assert snapshot.entries == ()
        assert await list_worker_registrations(backend, identity) == []
    finally:
        await backend.stop()


@pytest.mark.nats
@pytest.mark.asyncio
async def test_serialized_write_and_delete_race(
    nats_url: str,
    nats_queue_config,
) -> None:
    _, backend = await _connect_backend(nats_url, nats_queue_config)
    identity = JobIdentity(name="tests.nats.write_delete", version="v1")
    now = datetime.now(tz=UTC)
    try:
        reg = _build_registration(worker_id="worker-race", job=identity, at=now)
        await asyncio.gather(
            backend.discovery_backend.write_registration(
                reg,
                max_envelope_bytes=backend.discovery_backend.config.max_envelope_bytes,
            ),
            backend.discovery_backend.delete_registration("worker-race", identity),
        )
        assert await list_worker_registrations(backend, identity) == []
    finally:
        await backend.stop()


@pytest.mark.nats
@pytest.mark.asyncio
async def test_deleted_bucket_recreated_when_provisioning_enabled(
    nats_url: str,
    nats_queue_config,
) -> None:
    _, backend = await _connect_backend(nats_url, nats_queue_config)
    identity = JobIdentity(name="tests.nats.recreate", version="v1")
    bucket = backend.discovery_backend.bucket_name
    try:
        await backend.discovery_backend.write_registration(
            _build_registration(worker_id="worker-1", job=identity, at=datetime.now(tz=UTC)),
            max_envelope_bytes=backend.discovery_backend.config.max_envelope_bytes,
        )
        await backend.discovery_backend._ensure_kv()
        js = backend.broker.connection.jetstream()
        await js.delete_key_value(bucket)
        await backend.discovery_backend.write_registration(
            _build_registration(worker_id="worker-2", job=identity, at=datetime.now(tz=UTC)),
            max_envelope_bytes=backend.discovery_backend.config.max_envelope_bytes,
        )
        workers = await list_worker_registrations(backend, identity)
        assert {item.worker_id for item in workers} == {"worker-2"}
    finally:
        await backend.stop()


@pytest.mark.nats
@pytest.mark.asyncio
async def test_deleted_bucket_without_provisioning_raises_configuration_error(
    nats_url: str,
    nats_queue_config,
) -> None:
    _, backend = await _connect_backend(nats_url, nats_queue_config)
    bucket = backend.discovery_backend.bucket_name
    try:
        stale_kv = await backend.discovery_backend._ensure_kv()
        js = backend.broker.connection.jetstream()
        await js.delete_key_value(bucket)
        backend.discovery_backend.invalidate()
        no_provision = NatsJobBackend(
            backend.broker,
            nats_queue_config,
            presence_config=backend.discovery_backend.config,
            discovery_provision=False,
        )
        no_provision.discovery_backend._kv = stale_kv
        with pytest.raises(DiscoveryConfigurationError, match="absent"):
            await no_provision.discovery_backend.write_registration(
                _build_registration(
                    worker_id="worker-x",
                    job=JobIdentity(name="tests.nats.noprovision", version="v1"),
                    at=datetime.now(tz=UTC),
                ),
                max_envelope_bytes=backend.discovery_backend.config.max_envelope_bytes,
            )
    finally:
        await backend.stop()


@pytest.mark.nats
@pytest.mark.asyncio
async def test_incompatible_replacement_bucket_rejected_on_write(
    nats_url: str,
    nats_queue_config,
) -> None:
    _, backend = await _connect_backend(nats_url, nats_queue_config)
    bucket = backend.discovery_backend.bucket_name
    try:
        await backend.discovery_backend._ensure_kv()
        js = backend.broker.connection.jetstream()
        await js.delete_key_value(bucket)
        await js.create_key_value(KeyValueConfig(bucket=bucket, history=2))
        backend.discovery_backend.invalidate()
        with pytest.raises(DiscoveryConfigurationError, match="history"):
            await backend.discovery_backend.write_registration(
                _build_registration(
                    worker_id="worker-x",
                    job=JobIdentity(name="tests.nats.incompatible", version="v1"),
                    at=datetime.now(tz=UTC),
                ),
                max_envelope_bytes=backend.discovery_backend.config.max_envelope_bytes,
            )
    finally:
        await backend.stop()


@pytest.mark.nats
@pytest.mark.asyncio
async def test_lease_expired_retained_until_include_stale(
    nats_url: str,
    nats_queue_config,
) -> None:
    _, backend = await _connect_backend(nats_url, nats_queue_config)
    identity = JobIdentity(name="tests.nats.lease", version="v1")
    now = datetime.now(tz=UTC)
    reg = _build_registration(
        worker_id="worker-lease",
        job=identity,
        at=now,
        lease_seconds=0.5,
    )
    try:
        await backend.discovery_backend.write_registration(
            reg,
            max_envelope_bytes=backend.discovery_backend.config.max_envelope_bytes,
        )
        await asyncio.sleep(0.6)
        assert await list_worker_registrations(backend, identity) == []
        snapshot = await backend.discovery_backend.read_snapshot(include_stale=True)
        active = filter_active_entries(snapshot, job=identity, include_stale=False)
        stale = filter_active_entries(snapshot, job=identity, include_stale=True)
        assert active == []
        assert len(stale) == 1
    finally:
        await backend.stop()


@pytest.mark.nats
@pytest.mark.asyncio
async def test_draining_registration_excluded_from_offered_jobs(
    nats_url: str,
    nats_queue_config,
) -> None:
    _, backend = await _connect_backend(nats_url, nats_queue_config)
    identity = JobIdentity(name="tests.nats.draining", version="v1")
    now = datetime.now(tz=UTC)
    reg = _build_registration(
        worker_id="worker-drain",
        job=identity,
        at=now,
        state=WorkerRegistrationState.DRAINING,
    )
    try:
        await backend.discovery_backend.write_registration(
            reg,
            max_envelope_bytes=backend.discovery_backend.config.max_envelope_bytes,
        )
        assert await list_offered_jobs(backend) == []
        snapshot = await backend.discovery_backend.read_snapshot(include_stale=True)
        stale = filter_active_entries(snapshot, job=identity, include_stale=True)
        assert len(stale) == 1
    finally:
        await backend.stop()


@pytest.mark.nats
@pytest.mark.asyncio
async def test_unversioned_and_literal_none_versions_distinct(
    nats_url: str,
    nats_queue_config,
) -> None:
    _, backend = await _connect_backend(nats_url, nats_queue_config)
    now = datetime.now(tz=UTC)
    unversioned = JobIdentity(name="tests.nats.versioning", version=None)
    literal_none = JobIdentity(name="tests.nats.versioning", version="None")
    dotted = JobIdentity(name="tests.nats.versioning", version="v1.2.3")
    try:
        for worker_id, identity in (
            ("w-uv", unversioned),
            ("w-ln", literal_none),
            ("w-dot", dotted),
        ):
            await backend.discovery_backend.write_registration(
                _build_registration(worker_id=worker_id, job=identity, at=now),
                max_envelope_bytes=backend.discovery_backend.config.max_envelope_bytes,
            )
        snapshot = await backend.discovery_backend.read_snapshot()
        assert len(snapshot.entries) == 3
    finally:
        await backend.stop()


@pytest.mark.nats
@pytest.mark.asyncio
async def test_two_jobs_same_worker_id_occupy_distinct_entries(
    nats_url: str,
    nats_queue_config,
) -> None:
    _, backend = await _connect_backend(nats_url, nats_queue_config)
    now = datetime.now(tz=UTC)
    job_a = JobIdentity(name="tests.nats.same-worker.a", version="v1")
    job_b = JobIdentity(name="tests.nats.same-worker.b", version="v1")
    try:
        for job in (job_a, job_b):
            await backend.discovery_backend.write_registration(
                _build_registration(worker_id="shared-runtime", job=job, at=now),
                max_envelope_bytes=backend.discovery_backend.config.max_envelope_bytes,
            )
        snapshot = await backend.discovery_backend.read_snapshot()
        assert len(snapshot.entries) == 2
    finally:
        await backend.stop()


@pytest.mark.nats
@pytest.mark.asyncio
async def test_bucket_namespace_isolation_between_subject_prefixes(
    nats_url: str,
    nats_queue_config,
) -> None:
    from superjobs.transport.implementations.nats import NatsQueueConfig

    prefix_a = f"{nats_queue_config.subject_prefix}_a"
    prefix_b = f"{nats_queue_config.subject_prefix}_b"
    config_a = NatsQueueConfig(
        stream=f"{prefix_a}_stream",
        observation_stream=f"{prefix_a}_obs",
        subject_prefix=prefix_a,
        completion_bucket=f"{prefix_a}_comp",
        idempotency_bucket=f"{prefix_a}_idem",
    )
    config_b = NatsQueueConfig(
        stream=f"{prefix_b}_stream",
        observation_stream=f"{prefix_b}_obs",
        subject_prefix=prefix_b,
        completion_bucket=f"{prefix_b}_comp",
        idempotency_bucket=f"{prefix_b}_idem",
    )
    _, backend_a = await _connect_backend(nats_url, config_a)
    _, backend_b = await _connect_backend(nats_url, config_b)
    identity = JobIdentity(name="tests.nats.isolation", version="v1")
    now = datetime.now(tz=UTC)
    try:
        await backend_a.discovery_backend.write_registration(
            _build_registration(worker_id="only-a", job=identity, at=now),
            max_envelope_bytes=backend_a.discovery_backend.config.max_envelope_bytes,
        )
        assert await list_worker_registrations(backend_a, identity)
        assert await list_worker_registrations(backend_b, identity) == []
        assert backend_a.discovery_backend.bucket_name != backend_b.discovery_backend.bucket_name
    finally:
        await backend_a.stop()
        await backend_b.stop()


@pytest.mark.nats
@pytest.mark.asyncio
async def test_unknown_kv_key_in_dedicated_bucket_fails_visibly(
    nats_url: str,
    nats_queue_config,
) -> None:
    _, backend = await _connect_backend(nats_url, nats_queue_config)
    try:
        kv = await backend.discovery_backend._ensure_kv()
        await kv.put("foreign.key", b"{}")
        with pytest.raises(DiscoveryEnvelopeError, match="Unexpected key"):
            await backend.discovery_backend.read_snapshot()
    finally:
        await backend.stop()


@pytest.mark.nats
@pytest.mark.asyncio
async def test_oversized_write_rejected_and_prior_snapshot_unchanged(
    nats_url: str,
    nats_queue_config,
) -> None:
    config = _short_presence_config()
    _, backend = await _connect_backend(nats_url, nats_queue_config, presence_config=config)
    identity = JobIdentity(name="tests.nats.size", version="v1")
    now = datetime.now(tz=UTC)
    small = _build_registration(worker_id="worker-1", job=identity, at=now)
    try:
        await backend.discovery_backend.write_registration(
            small,
            max_envelope_bytes=config.max_envelope_bytes,
        )
        big = InMemoryDiscoveryBackend(config=config).build_registration(
            worker_id="worker-1",
            job=identity,
            state=WorkerRegistrationState.READY,
            registered_at=now,
            last_seen_at=now,
            expires_at=now + timedelta(seconds=30),
            capabilities=None,
            metadata=WorkerRegistrationMetadata(
                worker_name="x" * 200,
                host_label="y" * 200,
            ),
        )
        assert len(encode_envelope(big, max_bytes=DEFAULT_MAX_ENVELOPE_BYTES)) > 128
        with pytest.raises(DiscoveryWriteError):
            await backend.discovery_backend.write_registration(
                big,
                max_envelope_bytes=128,
            )
        workers = await list_worker_registrations(backend, identity)
        assert len(workers) == 1
        assert workers[0].worker_id == "worker-1"
    finally:
        await backend.stop()


@pytest.mark.nats
@pytest.mark.asyncio
async def test_concurrent_bucket_creation_and_conflicting_policy_rejected(
    nats_url: str,
    nats_queue_config,
) -> None:
    broker = NatsBroker(nats_url, connect_timeout=2, max_reconnect_attempts=0)
    await broker.start()
    try:
        winner_config = _short_presence_config(stale_retention=timedelta(seconds=4))
        loser_config = _short_presence_config(stale_retention=timedelta(seconds=6))
        winner = NatsDiscoveryBackend(
            broker,
            subject_prefix=nats_queue_config.subject_prefix,
            config=winner_config,
            discovery_provision=True,
        )
        loser = NatsDiscoveryBackend(
            broker,
            subject_prefix=nats_queue_config.subject_prefix,
            config=loser_config,
            discovery_provision=True,
        )
        await asyncio.gather(winner._ensure_kv(), winner._ensure_kv())
        with pytest.raises(DiscoveryConfigurationError, match="ttl"):
            await loser._ensure_kv()
    finally:
        await broker.stop()


@pytest.mark.nats
@pytest.mark.asyncio
async def test_unknown_envelope_version_fails_on_read(
    nats_url: str,
    nats_queue_config,
) -> None:
    _, backend = await _connect_backend(nats_url, nats_queue_config)
    identity = JobIdentity(name="tests.nats.badversion", version="v1")
    now = datetime.now(tz=UTC)
    body = {
        "envelope_version": ENVELOPE_VERSION + 99,
        "worker_id": "worker-bad",
        "job": {"name": identity.name, "version": identity.version},
        "state": "ready",
        "registered_at": now.isoformat(),
        "last_seen_at": now.isoformat(),
        "expires_at": (now + timedelta(seconds=30)).isoformat(),
        "capabilities": None,
        "metadata": None,
    }
    try:
        kv = await backend.discovery_backend._ensure_kv()
        await kv.put(registration_kv_key("worker-bad", identity), json.dumps(body).encode())
        job = _capability_job(identity.name, version=identity.version or "v1")
        with pytest.raises(DiscoveryEnvelopeVersionError):
            await list_worker_registrations(backend, job)
    finally:
        await backend.stop()


@pytest.mark.nats
@pytest.mark.asyncio
async def test_strict_capability_decode_failure_has_context(
    nats_url: str,
    nats_queue_config,
) -> None:
    from pydantic import BaseModel, ConfigDict

    from superjobs.registry import registry

    class StrictCapability(BaseModel):
        model_config = ConfigDict(extra="forbid")
        locale: str

    job = Job(
        "tests.nats.capdecode",
        version="v1",
        request=None,
        result=StrictCapability,
        capabilities=StrictCapability,
    )
    _, backend = await _connect_backend(nats_url, nats_queue_config)
    identity = job.identity
    now = datetime.now(tz=UTC)
    bad_payload = base64.standard_b64encode(json.dumps({}).encode()).decode("ascii")
    body = {
        "envelope_version": ENVELOPE_VERSION,
        "worker_id": "worker-cap",
        "job": {"name": identity.name, "version": identity.version},
        "state": "ready",
        "registered_at": now.isoformat(),
        "last_seen_at": now.isoformat(),
        "expires_at": (now + timedelta(seconds=30)).isoformat(),
        "capabilities": {
            "schema_id": capability_schema_id(job),
            "media_type": registry.get_payload_codec(StrictCapability).media_type,
            "payload_b64": bad_payload,
        },
        "metadata": None,
    }
    try:
        kv = await backend.discovery_backend._ensure_kv()
        await kv.put(registration_kv_key("worker-cap", identity), json.dumps(body).encode())
        with pytest.raises(CapabilityDecodeError) as error:
            await list_worker_registrations(backend, job)
        assert error.value.worker_id == "worker-cap"
        assert error.value.job == identity
    finally:
        await backend.stop()


@pytest.mark.nats
@pytest.mark.asyncio
async def test_missing_bucket_without_provision_raises_configuration_error(
    nats_url: str,
    nats_queue_config,
) -> None:
    _, backend = await _connect_backend(
        nats_url,
        nats_queue_config,
        discovery_provision=False,
    )
    try:
        with pytest.raises(DiscoveryConfigurationError, match="absent"):
            await backend.discovery_backend.read_snapshot()
    finally:
        await backend.stop()


@pytest.mark.nats
@pytest.mark.asyncio
async def test_unavailable_registry_when_broker_stopped(
    nats_url: str,
    nats_queue_config,
) -> None:
    _, backend = await _connect_backend(nats_url, nats_queue_config)
    identity = JobIdentity(name="tests.nats.unavailable", version="v1")
    await backend.stop()
    with pytest.raises(DiscoveryUnavailableError):
        await list_worker_registrations(backend, identity)


@pytest.mark.nats
@pytest.mark.asyncio
async def test_read_denied_stream_info_makes_bucket_configuration_unreadable(
    nats_queue_config,
) -> None:
    bucket = discovery_bucket_name(nats_queue_config.subject_prefix)
    auth = f"""
no_auth_user: admin
authorization {{
  users = [
    {{ user: admin, password: admin, permissions: {{ publish: ">", subscribe: ">" }} }}
    {{
      user: limited
      password: limited
      permissions: {{
        publish: {{
          allow: ">"
          deny: ["$JS.API.STREAM.INFO.KV_{bucket}"]
        }}
        subscribe: "_INBOX.>"
      }}
    }}
  ]
}}
"""
    owner = OwnedNatsServer(extra_config=auth)
    target = await asyncio.to_thread(owner.start)
    admin_url = target.url
    limited_url = admin_url.replace("nats://", "nats://limited:limited@")
    identity = JobIdentity(name="tests.nats.denied", version="v1")
    try:
        admin_broker = NatsBroker(admin_url, connect_timeout=2, max_reconnect_attempts=0)
        admin_backend = NatsJobBackend(
            admin_broker,
            nats_queue_config,
            presence_config=_short_presence_config(),
            discovery_provision=True,
        )
        await admin_backend.start()
        await admin_backend.discovery_backend.write_registration(
            _build_registration(
                worker_id="worker-seed",
                job=identity,
                at=datetime.now(tz=UTC),
            ),
            max_envelope_bytes=admin_backend.discovery_backend.config.max_envelope_bytes,
        )
        await admin_backend.stop()

        limited_broker = NatsBroker(limited_url, connect_timeout=2, max_reconnect_attempts=0)
        limited_backend = NatsJobBackend(
            limited_broker,
            nats_queue_config,
            presence_config=_short_presence_config(read_timeout=timedelta(seconds=2)),
            discovery_provision=False,
        )
        await limited_broker.start()
        with pytest.raises(DiscoveryUnavailableError):
            await list_worker_registrations(limited_backend, identity)
        await limited_broker.stop()
    finally:
        await asyncio.to_thread(owner.stop, target)


@pytest.mark.nats
@pytest.mark.asyncio
async def test_read_denied_consumer_create_blocks_cached_kv_enumeration(
    nats_queue_config,
) -> None:
    bucket = discovery_bucket_name(nats_queue_config.subject_prefix)
    auth = f"""
no_auth_user: admin
authorization {{
  users = [
    {{ user: admin, password: admin, permissions: {{ publish: ">", subscribe: ">" }} }}
    {{
      user: limited
      password: limited
      permissions: {{
        publish: {{
          allow: ">"
          deny: [
            "$JS.API.CONSUMER.CREATE.KV_{bucket}.>",
            "$JS.API.CONSUMER.CREATE.KV_{bucket}"
          ]
        }}
        subscribe: "_INBOX.>"
      }}
    }}
  ]
}}
"""
    owner = OwnedNatsServer(extra_config=auth)
    target = await asyncio.to_thread(owner.start)
    admin_url = target.url
    limited_url = admin_url.replace("nats://", "nats://limited:limited@")
    identity = JobIdentity(name="tests.nats.consumerdeny", version="v1")
    try:
        admin_broker = NatsBroker(admin_url, connect_timeout=2, max_reconnect_attempts=0)
        admin_backend = NatsJobBackend(
            admin_broker,
            nats_queue_config,
            presence_config=_short_presence_config(),
            discovery_provision=True,
        )
        await admin_backend.start()
        await admin_backend.discovery_backend.write_registration(
            _build_registration(
                worker_id="worker-seed",
                job=identity,
                at=datetime.now(tz=UTC),
            ),
            max_envelope_bytes=admin_backend.discovery_backend.config.max_envelope_bytes,
        )
        await admin_backend.stop()

        limited_broker = NatsBroker(limited_url, connect_timeout=2, max_reconnect_attempts=0)
        limited_backend = NatsJobBackend(
            limited_broker,
            nats_queue_config,
            presence_config=_short_presence_config(read_timeout=timedelta(seconds=2)),
            discovery_provision=False,
        )
        await limited_broker.start()
        await limited_backend.discovery_backend._ensure_kv()
        with pytest.raises(DiscoveryUnavailableError):
            await list_worker_registrations(limited_backend, identity)
        await limited_broker.stop()
    finally:
        await asyncio.to_thread(owner.stop, target)


@pytest.mark.nats
@pytest.mark.asyncio
async def test_short_retention_ttl_eventually_clears_kv_entry(
    nats_url: str,
    nats_queue_config,
) -> None:
    config = _short_presence_config(stale_retention=timedelta(seconds=2))
    _, backend = await _connect_backend(
        nats_url,
        nats_queue_config,
        presence_config=config,
    )
    identity = JobIdentity(name="tests.nats.ttl", version="v1")
    try:
        await backend.discovery_backend.write_registration(
            _build_registration(
                worker_id="worker-ttl",
                job=identity,
                at=datetime.now(tz=UTC),
            ),
            max_envelope_bytes=config.max_envelope_bytes,
        )
        await asyncio.sleep(2.5)
        assert await list_worker_registrations(backend, identity) == []
    finally:
        await backend.stop()


def _write_verification_report(path: Path, *, command: str, results: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        f"# Issue #53 verification\n\n## Command\n\n{command}\n\n## Results\n\n{results}\n",
        encoding="utf-8",
    )
