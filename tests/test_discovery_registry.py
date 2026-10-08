from __future__ import annotations

import asyncio
import json
from datetime import UTC, datetime, timedelta

import pytest

from superjobs.discovery.config import (
    DEFAULT_MAX_ENVELOPE_BYTES,
    DEFAULT_STALE_RETENTION,
    PresenceConfig,
)
from superjobs.discovery.envelope import (
    ENVELOPE_VERSION,
    StoredWorkerRegistration,
    decode_envelope_bytes,
    encode_envelope,
    validate_envelope_write,
)
from superjobs.discovery.errors import (
    DiscoveryConfigurationError,
    DiscoveryEnvelopeError,
    DiscoveryEnvelopeVersionError,
    DiscoveryWriteError,
)
from superjobs.discovery import memory as discovery_memory
from superjobs.discovery.memory import InMemoryDiscoveryBackend, registration_key
from superjobs.discovery.models import (
    RawCapabilities,
    WorkerRegistrationMetadata,
    WorkerRegistrationState,
)
from superjobs.job_identity import JobIdentity


class MutableClock:
    def __init__(self, start: datetime) -> None:
        if start.tzinfo is None:
            raise ValueError("start must be timezone-aware")
        self._now = start.astimezone(UTC)

    def advance(self, delta: timedelta) -> None:
        self._now += delta

    def __call__(self) -> datetime:
        return self._now


def _registration_at(
    store: InMemoryDiscoveryBackend,
    *,
    worker_id: str,
    job: JobIdentity,
    registered_at: datetime,
    last_seen_at: datetime,
    expires_at: datetime,
    state: WorkerRegistrationState = WorkerRegistrationState.READY,
):
    stored = store.build_registration(
        worker_id=worker_id,
        job=job,
        state=state,
        registered_at=registered_at,
        last_seen_at=last_seen_at,
        expires_at=expires_at,
        capabilities=None,
    )
    return stored


async def _write(
    store: InMemoryDiscoveryBackend,
    registration,
    *,
    max_envelope_bytes: int | None = None,
) -> None:
    limit = max_envelope_bytes or store.config.max_envelope_bytes
    await store.write_registration(registration, max_envelope_bytes=limit)


class _InjectingBackend(InMemoryDiscoveryBackend):
    """Test-only store that can hold unvalidated bytes like a compromised backend."""

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


@pytest.mark.asyncio
async def test_snapshot_uses_single_evaluation_time_for_pruning() -> None:
    start = datetime(2026, 10, 6, 12, 0, tzinfo=UTC)
    clock = MutableClock(start)
    retention = timedelta(hours=1)
    store = InMemoryDiscoveryBackend(
        config=PresenceConfig(stale_retention=retention),
        clock=clock,
    )
    job = JobIdentity(name="tests.registry.prune", version="v1")
    old_seen = start - retention - timedelta(seconds=1)
    reg = _registration_at(
        store,
        worker_id="worker-1",
        job=job,
        registered_at=old_seen,
        last_seen_at=old_seen,
        expires_at=start + timedelta(seconds=30),
    )
    await _write(store, reg)
    snapshot = await store.read_snapshot()
    assert snapshot.entries == ()
    assert snapshot.evaluated_at == start


@pytest.mark.asyncio
async def test_lease_expired_entry_retained_until_retention_ttl() -> None:
    start = datetime(2026, 10, 6, 12, 0, tzinfo=UTC)
    clock = MutableClock(start)
    store = InMemoryDiscoveryBackend(clock=clock)
    job = JobIdentity(name="tests.registry.lease", version="v1")
    reg = _registration_at(
        store,
        worker_id="worker-1",
        job=job,
        registered_at=start,
        last_seen_at=start,
        expires_at=start + timedelta(seconds=30),
    )
    await _write(store, reg)
    clock.advance(timedelta(seconds=31))
    snapshot = await store.read_snapshot(include_stale=True)
    assert len(snapshot.entries) == 1
    clock.advance(DEFAULT_STALE_RETENTION)
    snapshot = await store.read_snapshot(include_stale=True)
    assert snapshot.entries == ()


@pytest.mark.asyncio
async def test_retention_boundary_prunes_at_exact_ttl_like_kv_expiration() -> None:
    start = datetime(2026, 10, 6, 12, 0, tzinfo=UTC)
    clock = MutableClock(start)
    retention = timedelta(hours=2)
    store = InMemoryDiscoveryBackend(
        config=PresenceConfig(stale_retention=retention),
        clock=clock,
    )
    job = JobIdentity(name="tests.registry.retention", version="v1")
    seen = start
    reg = _registration_at(
        store,
        worker_id="worker-1",
        job=job,
        registered_at=seen,
        last_seen_at=seen,
        expires_at=seen + timedelta(hours=1),
    )
    await _write(store, reg)
    clock.advance(retention - timedelta(microseconds=1))
    snapshot = await store.read_snapshot()
    assert len(snapshot.entries) == 1
    clock.advance(timedelta(microseconds=1))
    snapshot = await store.read_snapshot()
    assert snapshot.entries == ()


@pytest.mark.asyncio
async def test_renewal_preserves_registered_at() -> None:
    start = datetime(2026, 10, 6, 12, 0, tzinfo=UTC)
    clock = MutableClock(start)
    store = InMemoryDiscoveryBackend(clock=clock)
    job = JobIdentity(name="tests.registry.renew", version="v1")
    registered = start - timedelta(minutes=5)
    first = store.build_registration(
        worker_id="worker-1",
        job=job,
        state=WorkerRegistrationState.READY,
        registered_at=registered,
        last_seen_at=start,
        expires_at=start + timedelta(seconds=30),
        capabilities=None,
    )
    await _write(store, first)
    renewed = store.build_registration(
        worker_id="worker-1",
        job=job,
        state=WorkerRegistrationState.READY,
        registered_at=registered,
        last_seen_at=start + timedelta(seconds=10),
        expires_at=start + timedelta(seconds=40),
        capabilities=None,
    )
    await _write(store, renewed)
    snapshot = await store.read_snapshot()
    assert len(snapshot.entries) == 1
    decoded = decode_envelope_bytes(
        snapshot.entries[0][1],
        max_bytes=store.config.max_envelope_bytes,
    )
    assert decoded.registered_at == registered
    assert decoded.last_seen_at == start + timedelta(seconds=10)


@pytest.mark.asyncio
async def test_delete_removes_immediately_without_retained_history() -> None:
    start = datetime(2026, 10, 6, 12, 0, tzinfo=UTC)
    clock = MutableClock(start)
    store = InMemoryDiscoveryBackend(clock=clock)
    job = JobIdentity(name="tests.registry.delete", version="v1")
    reg = _registration_at(
        store,
        worker_id="worker-1",
        job=job,
        registered_at=start,
        last_seen_at=start,
        expires_at=start + timedelta(seconds=30),
    )
    await _write(store, reg)
    await store.delete_registration("worker-1", job)
    snapshot = await store.read_snapshot(include_stale=True)
    assert snapshot.entries == ()
    clock.advance(DEFAULT_STALE_RETENTION * 2)
    snapshot = await store.read_snapshot(include_stale=True)
    assert snapshot.entries == ()


@pytest.mark.asyncio
async def test_registration_keys_distinguish_version_and_unversioned() -> None:
    start = datetime(2026, 10, 6, 12, 0, tzinfo=UTC)
    store = InMemoryDiscoveryBackend(clock=MutableClock(start))
    versioned = JobIdentity(name="tests.registry.keys", version="v1")
    unversioned = JobIdentity(name="tests.registry.keys", version=None)
    for job, worker_id in ((versioned, "worker-v"), (unversioned, "worker-u")):
        reg = _registration_at(
            store,
            worker_id=worker_id,
            job=job,
            registered_at=start,
            last_seen_at=start,
            expires_at=start + timedelta(seconds=30),
        )
        await _write(store, reg)
    snapshot = await store.read_snapshot()
    keys = {entry[0] for entry in snapshot.entries}
    assert keys == {
        registration_key("worker-v", versioned),
        registration_key("worker-u", unversioned),
    }


@pytest.mark.asyncio
async def test_direct_stored_registration_naive_timestamp_rejected_preserves_snapshot() -> None:
    start = datetime(2026, 10, 6, 12, 0, tzinfo=UTC)
    store = InMemoryDiscoveryBackend(clock=MutableClock(start))
    job = JobIdentity(name="tests.registry.naive_direct", version="v1")
    good = _registration_at(
        store,
        worker_id="worker-1",
        job=job,
        registered_at=start,
        last_seen_at=start,
        expires_at=start + timedelta(seconds=30),
    )
    await _write(store, good)
    naive = datetime(2026, 10, 6, 12, 0)
    impostor = StoredWorkerRegistration(
        worker_id="worker-1",
        job=job,
        state=WorkerRegistrationState.READY,
        registered_at=naive,
        last_seen_at=start,
        expires_at=start + timedelta(seconds=30),
        capabilities=None,
        metadata=WorkerRegistrationMetadata(),
    )
    with pytest.raises(DiscoveryWriteError, match="registered_at"):
        await store.write_registration(impostor, max_envelope_bytes=store.config.max_envelope_bytes)
    snapshot = await store.read_snapshot()
    assert len(snapshot.entries) == 1
    decoded = decode_envelope_bytes(snapshot.entries[0][1])
    assert decoded.registered_at == start


def test_build_registration_rejects_naive_timestamps() -> None:
    start = datetime(2026, 10, 6, 12, 0, tzinfo=UTC)
    store = InMemoryDiscoveryBackend(clock=MutableClock(start))
    job = JobIdentity(name="tests.registry.naive_build", version="v1")
    naive = datetime(2026, 10, 6, 12, 0)
    with pytest.raises(ValueError, match="registered_at"):
        store.build_registration(
            worker_id="worker-1",
            job=job,
            state=WorkerRegistrationState.READY,
            registered_at=naive,
            last_seen_at=start,
            expires_at=start + timedelta(seconds=30),
            capabilities=None,
        )


@pytest.mark.asyncio
async def test_write_registration_bytes_rejects_key_envelope_identity_mismatch() -> None:
    start = datetime(2026, 10, 6, 12, 0, tzinfo=UTC)
    store = InMemoryDiscoveryBackend(clock=MutableClock(start))
    job = JobIdentity(name="tests.registry.key_mismatch", version="v1")
    other_job = JobIdentity(name="tests.registry.other", version="v1")
    reg = _registration_at(
        store,
        worker_id="worker-1",
        job=job,
        registered_at=start,
        last_seen_at=start,
        expires_at=start + timedelta(seconds=30),
    )
    encoded = encode_envelope(reg, max_bytes=store.config.max_envelope_bytes)
    wrong_key = registration_key("worker-1", other_job)
    with pytest.raises(DiscoveryWriteError, match="identity"):
        await store.write_registration_bytes(
            wrong_key,
            encoded,
            max_envelope_bytes=store.config.max_envelope_bytes,
        )


@pytest.mark.asyncio
async def test_replace_identity_mismatch_preserves_previous_bytes() -> None:
    start = datetime(2026, 10, 6, 12, 0, tzinfo=UTC)
    store = InMemoryDiscoveryBackend(clock=MutableClock(start))
    job = JobIdentity(name="tests.registry.replace_identity", version="v1")
    other_job = JobIdentity(name="tests.registry.replace_other", version="v1")
    good = _registration_at(
        store,
        worker_id="worker-1",
        job=job,
        registered_at=start,
        last_seen_at=start,
        expires_at=start + timedelta(seconds=30),
    )
    await _write(store, good)
    impostor = _registration_at(
        store,
        worker_id="worker-2",
        job=other_job,
        registered_at=start,
        last_seen_at=start,
        expires_at=start + timedelta(seconds=30),
    )
    encoded = encode_envelope(impostor, max_bytes=store.config.max_envelope_bytes)
    key = registration_key("worker-1", job)
    with pytest.raises(DiscoveryWriteError, match="identity"):
        await store.replace_registration_bytes(
            key,
            encoded,
            max_envelope_bytes=store.config.max_envelope_bytes,
        )
    snapshot = await store.read_snapshot()
    assert len(snapshot.entries) == 1
    decoded = decode_envelope_bytes(snapshot.entries[0][1])
    assert decoded.worker_id == "worker-1"
    assert decoded.job == job


@pytest.mark.asyncio
async def test_write_rejects_bool_max_envelope_bytes_before_effective_limit() -> None:
    start = datetime(2026, 10, 6, 12, 0, tzinfo=UTC)
    store = InMemoryDiscoveryBackend(clock=MutableClock(start))
    job = JobIdentity(name="tests.registry.bool_limit", version="v1")
    reg = _registration_at(
        store,
        worker_id="worker-1",
        job=job,
        registered_at=start,
        last_seen_at=start,
        expires_at=start + timedelta(seconds=30),
    )
    with pytest.raises(DiscoveryWriteError, match="max_envelope_bytes"):
        await store.write_registration(reg, max_envelope_bytes=True)  # type: ignore[arg-type]


@pytest.mark.asyncio
async def test_raw_write_bytes_cannot_bypass_configured_max_envelope_limit() -> None:
    start = datetime(2026, 10, 6, 12, 0, tzinfo=UTC)
    limit = 600
    store = InMemoryDiscoveryBackend(
        config=PresenceConfig(max_envelope_bytes=limit),
        clock=MutableClock(start),
    )
    job = JobIdentity(name="tests.registry.raw_limit", version="v1")
    payload = RawCapabilities(
        schema_id="x",
        media_type="application/octet-stream",
        payload=b"y" * 500,
    )
    reg = store.build_registration(
        worker_id="worker-1",
        job=job,
        state=WorkerRegistrationState.READY,
        registered_at=start,
        last_seen_at=start,
        expires_at=start + timedelta(seconds=30),
        capabilities=payload,
    )
    encoded = encode_envelope(reg, max_bytes=DEFAULT_MAX_ENVELOPE_BYTES)
    key = registration_key("worker-1", job)
    with pytest.raises(DiscoveryWriteError):
        await store.write_registration_bytes(
            key,
            encoded,
            max_envelope_bytes=DEFAULT_MAX_ENVELOPE_BYTES,
        )
    snapshot = await store.read_snapshot()
    assert snapshot.entries == ()


@pytest.mark.asyncio
async def test_malformed_replace_preserves_previous_bytes() -> None:
    start = datetime(2026, 10, 6, 12, 0, tzinfo=UTC)
    store = InMemoryDiscoveryBackend(clock=MutableClock(start))
    job = JobIdentity(name="tests.registry.atomic", version="v1")
    good = _registration_at(
        store,
        worker_id="worker-1",
        job=job,
        registered_at=start,
        last_seen_at=start,
        expires_at=start + timedelta(seconds=30),
    )
    await _write(store, good)
    key = registration_key("worker-1", job)
    with pytest.raises(DiscoveryWriteError):
        await store.replace_registration_bytes(key, b"{not-json", max_envelope_bytes=32768)
    snapshot = await store.read_snapshot()
    assert len(snapshot.entries) == 1
    assert decode_envelope_bytes(snapshot.entries[0][1]).worker_id == "worker-1"


@pytest.mark.asyncio
async def test_oversized_write_preserves_previous_snapshot() -> None:
    start = datetime(2026, 10, 6, 12, 0, tzinfo=UTC)
    store = InMemoryDiscoveryBackend(
        config=PresenceConfig(max_envelope_bytes=512),
        clock=MutableClock(start),
    )
    job = JobIdentity(name="tests.registry.oversize", version="v1")
    good = _registration_at(
        store,
        worker_id="worker-1",
        job=job,
        registered_at=start,
        last_seen_at=start,
        expires_at=start + timedelta(seconds=30),
    )
    await _write(store, good)
    huge = store.build_registration(
        worker_id="worker-1",
        job=job,
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
        await _write(store, huge)
    snapshot = await store.read_snapshot()
    assert len(snapshot.entries) == 1
    decoded = decode_envelope_bytes(snapshot.entries[0][1], max_bytes=512)
    assert decoded.capabilities is None


@pytest.mark.asyncio
async def test_concurrent_replacement_last_write_wins() -> None:
    start = datetime(2026, 10, 6, 12, 0, tzinfo=UTC)
    store = InMemoryDiscoveryBackend(clock=MutableClock(start))
    job = JobIdentity(name="tests.registry.concurrent", version="v1")
    base = _registration_at(
        store,
        worker_id="worker-1",
        job=job,
        registered_at=start,
        last_seen_at=start,
        expires_at=start + timedelta(seconds=30),
    )
    await _write(store, base)
    first = store.build_registration(
        worker_id="worker-1",
        job=job,
        state=WorkerRegistrationState.READY,
        registered_at=start,
        last_seen_at=start + timedelta(seconds=1),
        expires_at=start + timedelta(seconds=31),
        capabilities=None,
    )
    second = store.build_registration(
        worker_id="worker-1",
        job=job,
        state=WorkerRegistrationState.DRAINING,
        registered_at=start,
        last_seen_at=start + timedelta(seconds=2),
        expires_at=start + timedelta(seconds=32),
        capabilities=None,
    )
    await asyncio.gather(
        _write(store, first),
        _write(store, second),
    )
    snapshot = await store.read_snapshot()
    assert len(snapshot.entries) == 1
    decoded = decode_envelope_bytes(snapshot.entries[0][1])
    assert decoded.state in {
        WorkerRegistrationState.READY,
        WorkerRegistrationState.DRAINING,
    }
    assert decoded.last_seen_at in {first.last_seen_at, second.last_seen_at}


@pytest.mark.asyncio
async def test_store_enforces_authoritative_max_even_when_caller_requests_larger() -> None:
    start = datetime(2026, 10, 6, 12, 0, tzinfo=UTC)
    limit = 600
    store = InMemoryDiscoveryBackend(
        config=PresenceConfig(max_envelope_bytes=limit),
        clock=MutableClock(start),
    )
    job = JobIdentity(name="tests.registry.limit", version="v1")
    payload = RawCapabilities(
        schema_id="x",
        media_type="application/octet-stream",
        payload=b"y" * 500,
    )
    reg = store.build_registration(
        worker_id="worker-1",
        job=job,
        state=WorkerRegistrationState.READY,
        registered_at=start,
        last_seen_at=start,
        expires_at=start + timedelta(seconds=30),
        capabilities=payload,
    )
    with pytest.raises(DiscoveryWriteError):
        await store.write_registration(reg, max_envelope_bytes=DEFAULT_MAX_ENVELOPE_BYTES)
    snapshot = await store.read_snapshot()
    assert snapshot.entries == ()


@pytest.mark.asyncio
async def test_decode_respects_configured_max_bytes_not_hardcoded_default() -> None:
    start = datetime(2026, 10, 6, 12, 0, tzinfo=UTC)
    job = JobIdentity(name="tests.registry.decode", version="v1")
    reg = InMemoryDiscoveryBackend(clock=MutableClock(start)).build_registration(
        worker_id="worker-1",
        job=job,
        state=WorkerRegistrationState.READY,
        registered_at=start,
        last_seen_at=start,
        expires_at=start + timedelta(seconds=30),
        capabilities=None,
    )
    encoded = encode_envelope(reg, max_bytes=DEFAULT_MAX_ENVELOPE_BYTES)
    with pytest.raises(DiscoveryEnvelopeError):
        decode_envelope_bytes(encoded, max_bytes=len(encoded) - 1)


def test_config_rejects_bool_max_envelope_bytes() -> None:
    with pytest.raises(DiscoveryConfigurationError):
        PresenceConfig(max_envelope_bytes=True)  # type: ignore[arg-type]


def test_config_rejects_lease_shorter_than_three_renewals() -> None:
    with pytest.raises(DiscoveryConfigurationError):
        PresenceConfig(
            renewal_interval=timedelta(seconds=10),
            lease_timeout=timedelta(seconds=20),
        )


def test_config_rejects_non_positive_read_timeout() -> None:
    with pytest.raises(DiscoveryConfigurationError):
        PresenceConfig(read_timeout=timedelta(0))


def test_naive_clock_rejected_on_read() -> None:
    store = InMemoryDiscoveryBackend(clock=lambda: datetime(2026, 10, 6, 12, 0))
    with pytest.raises(ValueError, match="timezone-aware"):
        asyncio.run(store.read_snapshot())


def test_envelope_version_bool_and_float_rejected() -> None:
    start = datetime(2026, 10, 6, 12, 0, tzinfo=UTC)
    base = {
        "worker_id": "worker-1",
        "job": {"name": "tests.registry.version", "version": "v1"},
        "state": "ready",
        "registered_at": start.isoformat(),
        "last_seen_at": start.isoformat(),
        "expires_at": (start + timedelta(seconds=30)).isoformat(),
        "capabilities": None,
        "metadata": None,
    }
    for version in (True, 1.0):
        body = {**base, "envelope_version": version}
        with pytest.raises(DiscoveryEnvelopeVersionError):
            decode_envelope_object = __import__(
                "superjobs.discovery.envelope",
                fromlist=["decode_envelope_object"],
            ).decode_envelope_object(body)


def test_envelope_invalid_job_identity_and_timestamp_ordering() -> None:
    start = datetime(2026, 10, 6, 12, 0, tzinfo=UTC)
    body = {
        "envelope_version": ENVELOPE_VERSION,
        "worker_id": "worker-1",
        "job": {"name": "INVALID", "version": "v1"},
        "state": "ready",
        "registered_at": start.isoformat(),
        "last_seen_at": start.isoformat(),
        "expires_at": (start + timedelta(seconds=30)).isoformat(),
        "capabilities": None,
        "metadata": None,
    }
    with pytest.raises(DiscoveryWriteError) as error:
        validate_envelope_write(json.dumps(body).encode(), max_bytes=4096)
    assert error.value.worker_id == "worker-1"

    body["job"] = {"name": "tests.registry.bad", "version": "v1"}
    body["registered_at"] = (start + timedelta(seconds=5)).isoformat()
    with pytest.raises(DiscoveryWriteError):
        validate_envelope_write(json.dumps(body).encode(), max_bytes=4096)


def test_envelope_rejects_naive_timestamp_before_timezone_normalization() -> None:
    body = {
        "envelope_version": ENVELOPE_VERSION,
        "worker_id": "worker-1",
        "job": {"name": "tests.registry.naive", "version": "v1"},
        "state": "ready",
        "registered_at": "2026-10-06T12:00:00",
        "last_seen_at": "2026-10-06T12:00:00+00:00",
        "expires_at": "2026-10-06T12:00:30+00:00",
        "capabilities": None,
        "metadata": None,
    }
    with pytest.raises(DiscoveryWriteError):
        validate_envelope_write(json.dumps(body).encode(), max_bytes=4096)


def test_envelope_rejects_non_ascii_base64_capabilities() -> None:
    start = datetime(2026, 10, 6, 12, 0, tzinfo=UTC)
    body = {
        "envelope_version": ENVELOPE_VERSION,
        "worker_id": "worker-1",
        "job": {"name": "tests.registry.b64", "version": "v1"},
        "state": "ready",
        "registered_at": start.isoformat(),
        "last_seen_at": start.isoformat(),
        "expires_at": (start + timedelta(seconds=30)).isoformat(),
        "capabilities": {
            "schema_id": "x",
            "media_type": "application/octet-stream",
            "payload_b64": "café",
        },
        "metadata": None,
    }
    with pytest.raises(DiscoveryWriteError) as error:
        validate_envelope_write(json.dumps(body).encode(), max_bytes=4096)
    assert "worker-1" in str(error.value)


@pytest.mark.asyncio
async def test_injected_malformed_bytes_remain_until_retention_prune() -> None:
    start = datetime(2026, 10, 6, 12, 0, tzinfo=UTC)
    clock = MutableClock(start)
    store = _InjectingBackend(clock=clock)
    job = JobIdentity(name="tests.registry.inject", version="v1")
    key = registration_key("worker-1", job)
    store.inject_raw(key, b"not-json", last_acknowledged_at=start)
    snapshot = await store.read_snapshot()
    assert len(snapshot.entries) == 1
    clock.advance(DEFAULT_STALE_RETENTION + timedelta(seconds=1))
    snapshot = await store.read_snapshot()
    assert snapshot.entries == ()


_RAW_BYTES_WRITE_OPERATIONS = ("write_registration_bytes", "replace_registration_bytes")


@pytest.mark.asyncio
@pytest.mark.parametrize("operation", _RAW_BYTES_WRITE_OPERATIONS)
async def test_malformed_raw_bytes_write_retains_key_context_and_atomic_snapshot(
    operation: str,
) -> None:
    start = datetime(2026, 10, 6, 12, 0, tzinfo=UTC)
    store = InMemoryDiscoveryBackend(clock=MutableClock(start))
    job = JobIdentity(name="tests.registry.key_context", version="v1")
    key = registration_key("worker-1", job)
    if operation == "replace_registration_bytes":
        good = _registration_at(
            store,
            worker_id="worker-1",
            job=job,
            registered_at=start,
            last_seen_at=start,
            expires_at=start + timedelta(seconds=30),
        )
        await _write(store, good)
    malformed = b"{not-json"

    with pytest.raises(DiscoveryWriteError) as error:
        if operation == "write_registration_bytes":
            await store.write_registration_bytes(
                key,
                malformed,
                max_envelope_bytes=store.config.max_envelope_bytes,
            )
        else:
            await store.replace_registration_bytes(
                key,
                malformed,
                max_envelope_bytes=store.config.max_envelope_bytes,
            )

    exc = error.value
    assert exc.worker_id == "worker-1"
    assert exc.job == job
    assert "worker-1" in str(exc)
    envelope_cause = exc.__cause__
    assert isinstance(envelope_cause, DiscoveryEnvelopeError)
    assert isinstance(envelope_cause.__cause__, json.JSONDecodeError)

    snapshot = await store.read_snapshot()
    if operation == "replace_registration_bytes":
        assert len(snapshot.entries) == 1
        assert decode_envelope_bytes(snapshot.entries[0][1]).worker_id == "worker-1"
    else:
        assert snapshot.entries == ()
