from __future__ import annotations

import asyncio
from datetime import timedelta
from unittest.mock import AsyncMock, MagicMock

import pytest
from nats.js.api import KeyValueConfig, StorageType
from nats.js.kv import KeyValue

from datetime import UTC, datetime

from superjobs.discovery.config import PresenceConfig
from superjobs.discovery.errors import DiscoveryEnvelopeError, DiscoveryUnavailableError, DiscoveryWriteError
from superjobs.discovery.memory import InMemoryDiscoveryBackend, registration_key
from superjobs.discovery.models import WorkerRegistrationState
from superjobs.discovery.nats import (
    NatsDiscoveryBackend,
    _validate_existing_bucket,
    discovery_bucket_name,
    parse_registration_kv_key,
    registration_kv_key,
)
from superjobs.job_identity import JobIdentity


def test_registration_kv_key_round_trip_unicode_and_dots() -> None:
    worker_id = "worker-α-🙂"
    job = JobIdentity(name="tests.nats.keys", version="v1.2.beta")
    key = registration_kv_key(worker_id, job)
    assert parse_registration_kv_key(key) == registration_key(worker_id, job)


def test_registration_kv_key_distinguishes_unversioned_from_literal_none() -> None:
    unversioned = JobIdentity(name="tests.nats.unversioned", version=None)
    literal_none = JobIdentity(name="tests.nats.literal", version="None")
    key_uv = registration_kv_key("worker-a", unversioned)
    key_ln = registration_kv_key("worker-a", literal_none)
    assert key_uv != key_ln
    assert parse_registration_kv_key(key_uv)[2] is None
    assert parse_registration_kv_key(key_ln)[2] == "None"


def test_registration_kv_key_rejects_alternate_base64_encoding() -> None:
    job = JobIdentity(name="tests.nats.altb64", version="v1")
    key = registration_kv_key("worker-a", job)
    parts = key.split(".")
    padded_worker = parts[2] + "="
    tampered = ".".join([parts[0], parts[1], padded_worker, parts[3], parts[4]])
    with pytest.raises(DiscoveryEnvelopeError):
        parse_registration_kv_key(tampered)


def test_discovery_bucket_name_normalizes_unsafe_prefix() -> None:
    prefix = "test/bad:chars"
    name = discovery_bucket_name(prefix)
    assert name.startswith("sj-")
    assert name.endswith("-wdisc")


def test_parse_registration_kv_key_rejects_unknown_prefix() -> None:
    with pytest.raises(DiscoveryEnvelopeError):
        parse_registration_kv_key("legacy.worker.key")


def test_validate_existing_bucket_accepts_long_ttl_seconds() -> None:
    config = PresenceConfig(stale_retention=timedelta(hours=25))
    expected = KeyValueConfig(
        bucket="test-bucket",
        history=1,
        storage=StorageType.FILE,
        ttl=float(config.stale_retention.total_seconds()),
        max_value_size=config.max_envelope_bytes,
    )
    stream_config = MagicMock()
    stream_config.storage = StorageType.FILE
    stream_config.max_msg_size = config.max_envelope_bytes
    stream_config.max_age = float(config.stale_retention.total_seconds())
    stream_info = MagicMock()
    stream_info.config = stream_config
    status = MagicMock()
    status.history = 1
    status.stream_info = stream_info
    _validate_existing_bucket(status, expected)


@pytest.mark.asyncio
async def test_list_kv_keys_cancels_during_watchall_init_without_leaking_task() -> None:
    backend = NatsDiscoveryBackend.__new__(NatsDiscoveryBackend)
    backend._config = PresenceConfig(read_timeout=timedelta(seconds=2))
    init_started = asyncio.Event()
    init_release = asyncio.Event()
    watcher = MagicMock()
    watcher.stop = AsyncMock()

    class _EmptyAsyncIter:
        def __aiter__(self):
            return self

        async def __anext__(self):
            raise StopAsyncIteration

    watcher.__aiter__ = lambda self: _EmptyAsyncIter()

    async def watchall(**_kwargs):
        init_started.set()
        await init_release.wait()
        return watcher

    kv = MagicMock()
    kv.watchall = watchall
    task = asyncio.create_task(backend._list_kv_keys(kv))
    await init_started.wait()
    task.cancel()
    init_release.set()
    with pytest.raises(asyncio.CancelledError):
        await task
    watcher.stop.assert_awaited()
    await asyncio.sleep(0)
    assert all(t.get_name() != "superjobs-discovery-kv-watchall-init" for t in asyncio.all_tasks())


@pytest.mark.asyncio
async def test_cancelled_initializer_failure_preserves_cancelled_error() -> None:
    backend = NatsDiscoveryBackend.__new__(NatsDiscoveryBackend)
    backend._config = PresenceConfig(read_timeout=timedelta(seconds=1))
    started = asyncio.Event()
    release = asyncio.Event()

    async def watchall(**_kwargs):
        started.set()
        await release.wait()
        raise RuntimeError("initializer failed while cancellation was draining")

    kv = MagicMock()
    kv.watchall = watchall
    task = asyncio.create_task(backend._list_kv_keys(kv))
    await started.wait()
    task.cancel()
    release.set()
    with pytest.raises(asyncio.CancelledError):
        await task
    await asyncio.sleep(0)
    assert all(t.get_name() != "superjobs-discovery-kv-watchall-init" for t in asyncio.all_tasks())


@pytest.mark.asyncio
async def test_list_kv_keys_watcher_stops_on_iteration_cancellation() -> None:
    backend = NatsDiscoveryBackend.__new__(NatsDiscoveryBackend)
    backend._config = PresenceConfig(read_timeout=timedelta(seconds=2))
    watcher = MagicMock()
    watcher.stop = AsyncMock()

    async def watchall(**_kwargs):
        return watcher

    class _HungIterator:
        def __aiter__(self):
            return self

        async def __anext__(self):
            await asyncio.sleep(10)
            return None

    watcher.__aiter__ = lambda self: _HungIterator()

    kv = MagicMock()
    kv.watchall = watchall
    task = asyncio.create_task(backend._list_kv_keys(kv))
    await asyncio.sleep(0.05)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    watcher.stop.assert_awaited()


@pytest.mark.asyncio
async def test_cancelled_iteration_stop_failure_preserves_cancelled_error() -> None:
    backend = NatsDiscoveryBackend.__new__(NatsDiscoveryBackend)
    backend._config = PresenceConfig(read_timeout=timedelta(seconds=2))
    watcher = MagicMock()
    watcher.stop = AsyncMock(side_effect=RuntimeError("stop failed during cancel cleanup"))

    async def watchall(**_kwargs):
        return watcher

    class _HungIterator:
        def __aiter__(self):
            return self

        async def __anext__(self):
            await asyncio.sleep(10)
            return None

    watcher.__aiter__ = lambda self: _HungIterator()

    kv = MagicMock()
    kv.watchall = watchall
    task = asyncio.create_task(backend._list_kv_keys(kv))
    await asyncio.sleep(0.05)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task


@pytest.mark.asyncio
async def test_successful_read_stop_failure_is_visible() -> None:
    backend = NatsDiscoveryBackend.__new__(NatsDiscoveryBackend)
    backend._config = PresenceConfig(read_timeout=timedelta(seconds=2))
    watcher = MagicMock()
    watcher.stop = AsyncMock(side_effect=RuntimeError("stop failed after successful read"))

    async def watchall(**_kwargs):
        return watcher

    class _SingleMarker:
        def __aiter__(self):
            return self

        async def __anext__(self):
            raise StopAsyncIteration

    watcher.__aiter__ = lambda self: _SingleMarker()

    kv = MagicMock()
    kv.watchall = watchall
    with pytest.raises(RuntimeError, match="stop failed after successful read"):
        await backend._list_kv_keys(kv)


@pytest.mark.asyncio
async def test_write_registration_times_out_on_hanging_put() -> None:
    backend = NatsDiscoveryBackend.__new__(NatsDiscoveryBackend)
    backend._config = PresenceConfig(
        renewal_interval=timedelta(seconds=1),
        lease_timeout=timedelta(seconds=3),
        read_timeout=timedelta(seconds=0.2),
    )
    backend._mutation_lock = asyncio.Lock()
    backend._kv_init_lock = asyncio.Lock()
    backend._bucket_name = "test-bucket"
    backend._discovery_provision = True

    kv = MagicMock()

    async def hanging_put(*_args, **_kwargs):
        await asyncio.sleep(5)

    kv.put = hanging_put
    backend._kv = kv

    async def validate_cached(_kv):
        return None

    backend._validate_cached_kv = validate_cached
    backend._ensure_kv = AsyncMock(return_value=kv)

    helper_job = JobIdentity(name="tests.nats.timeout", version="v1")
    now = datetime.now(tz=UTC)
    reg = InMemoryDiscoveryBackend().build_registration(
        worker_id="worker-1",
        job=helper_job,
        state=WorkerRegistrationState.READY,
        registered_at=now,
        last_seen_at=now,
        expires_at=now + timedelta(seconds=30),
        capabilities=None,
    )

    with pytest.raises(DiscoveryWriteError, match="timed out"):
        await backend.write_registration(reg, max_envelope_bytes=4096)


@pytest.mark.asyncio
async def test_fetch_kv_entry_times_out_on_hanging_get() -> None:
    backend = NatsDiscoveryBackend.__new__(NatsDiscoveryBackend)
    backend._config = PresenceConfig(read_timeout=timedelta(seconds=0.2))
    kv = MagicMock()

    async def hanging_get(*_args, **_kwargs):
        await asyncio.sleep(5)

    kv.get = hanging_get
    backend._kv = kv

    async def validate_cached(_kv):
        return None

    backend._validate_cached_kv = validate_cached
    key = registration_kv_key("worker-1", JobIdentity(name="tests.nats.timeout", version="v1"))
    backend._list_kv_keys = AsyncMock(return_value=[key])
    backend._ensure_kv = AsyncMock(return_value=kv)
    backend._mutation_lock = asyncio.Lock()
    backend._kv_init_lock = asyncio.Lock()
    backend._bucket_name = "test-bucket"
    backend._discovery_provision = True

    with pytest.raises(DiscoveryUnavailableError, match="timed out"):
        await backend.read_snapshot()

@pytest.mark.asyncio
async def test_failed_snapshot_iteration_stops_watcher() -> None:
    backend = NatsDiscoveryBackend.__new__(NatsDiscoveryBackend)
    backend._config = PresenceConfig(read_timeout=timedelta(seconds=1))
    watcher = MagicMock()
    watcher.stop = AsyncMock()

    async def fail_iteration():
        raise RuntimeError("snapshot iteration failed")
        yield None

    watcher.__aiter__ = lambda _self: fail_iteration()
    kv = MagicMock()
    kv.watchall = AsyncMock(return_value=watcher)
    with pytest.raises(RuntimeError, match="snapshot iteration failed"):
        await backend._list_kv_keys(kv)
    watcher.stop.assert_awaited_once()

@pytest.mark.asyncio
async def test_repeated_cancellation_finishes_watchall_initializer_cleanup() -> None:
    backend = NatsDiscoveryBackend.__new__(NatsDiscoveryBackend)
    backend._config = PresenceConfig(read_timeout=timedelta(seconds=1))
    entered = asyncio.Event()
    release = asyncio.Event()
    watcher = MagicMock()
    watcher.stop = AsyncMock()

    async def watchall(**_kwargs):
        entered.set()
        await release.wait()
        return watcher

    kv = MagicMock()
    kv.watchall = watchall
    task = asyncio.create_task(backend._list_kv_keys(kv))
    await entered.wait()
    task.cancel()
    for _ in range(4):
        await asyncio.sleep(0)
    task.cancel()
    await asyncio.sleep(0)
    assert not task.done()
    release.set()
    with pytest.raises(asyncio.CancelledError):
        await task
    watcher.stop.assert_awaited_once()
    assert not any(
        pending.get_name().startswith("superjobs-discovery-kv-watchall-")
        for pending in asyncio.all_tasks()
    )


@pytest.mark.asyncio
async def test_repeated_cancellation_finishes_watcher_stop() -> None:
    backend = NatsDiscoveryBackend.__new__(NatsDiscoveryBackend)
    backend._config = PresenceConfig(read_timeout=timedelta(seconds=1))
    iterating = asyncio.Event()
    stopping = asyncio.Event()
    release = asyncio.Event()
    watcher = MagicMock()

    async def iterate():
        iterating.set()
        await asyncio.Event().wait()
        yield None

    async def stop():
        stopping.set()
        await release.wait()

    watcher.__aiter__ = lambda _self: iterate()
    watcher.stop = AsyncMock(side_effect=stop)
    kv = MagicMock()
    kv.watchall = AsyncMock(return_value=watcher)
    task = asyncio.create_task(backend._list_kv_keys(kv))
    await iterating.wait()
    task.cancel()
    await stopping.wait()
    task.cancel()
    await asyncio.sleep(0)
    assert not task.done()
    release.set()
    with pytest.raises(asyncio.CancelledError):
        await task
    watcher.stop.assert_awaited_once()
    assert not any(
        pending.get_name().startswith("superjobs-discovery-kv-watchall-")
        for pending in asyncio.all_tasks()
    )
