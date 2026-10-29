from __future__ import annotations

import asyncio
from datetime import UTC, datetime
from unittest.mock import AsyncMock, MagicMock
from uuid import uuid4

import pytest
from faststream.nats import NatsBroker
from nats.js.errors import APIError, NoKeysError
from nats.js.kv import KeyValue

from superjobs.resources import (
    NatsResourceAdmissionStore,
    ResourceAdmission,
    ResourceRequirement,
)
from superjobs.resources.errors import ResourceStoreUnavailableError
from superjobs.resources.nats import _KV_KEYS_LIST_RETRY_ATTEMPTS
from tests.test_resource_admission_memory import _intent


def _bucket_status(*, messages: int) -> MagicMock:
    status = MagicMock()
    status.values = messages
    return status


@pytest.mark.asyncio
async def test_read_snapshot_verified_empty_bucket_unit() -> None:
    store = NatsResourceAdmissionStore(MagicMock(), subject_prefix="tests.unit")
    kv = MagicMock()
    store._key_scanner._list_kv_keys = AsyncMock(side_effect=NoKeysError())
    kv.status = AsyncMock(return_value=_bucket_status(messages=0))
    store._ensure_kv = AsyncMock(return_value=kv)

    snapshot = await store.read_snapshot(evaluated_at=datetime.now(UTC))

    assert snapshot.capacities == ()
    assert snapshot.resources == ()
    assert snapshot.execution_claims == ()


@pytest.mark.asyncio
async def test_read_snapshot_false_empty_listing_retries_unit() -> None:
    store = NatsResourceAdmissionStore(MagicMock(), subject_prefix="tests.unit")
    kv = MagicMock()
    store._key_scanner._list_kv_keys = AsyncMock(side_effect=[NoKeysError(), ["key"]])
    kv.status = AsyncMock(return_value=_bucket_status(messages=2))
    store._ensure_kv = AsyncMock(return_value=kv)

    snapshot = await store.read_snapshot(evaluated_at=datetime.now(UTC))

    assert store._key_scanner._list_kv_keys.await_count == 2
    assert snapshot.resources == ()


@pytest.mark.asyncio
async def test_read_snapshot_false_empty_listing_fails_closed_unit() -> None:
    store = NatsResourceAdmissionStore(MagicMock(), subject_prefix="tests.unit")
    kv = MagicMock()
    store._key_scanner._list_kv_keys = AsyncMock(side_effect=NoKeysError())
    kv.status = AsyncMock(return_value=_bucket_status(messages=3))
    store._ensure_kv = AsyncMock(return_value=kv)

    with pytest.raises(ResourceStoreUnavailableError, match="retains JetStream messages"):
        await store.read_snapshot(evaluated_at=datetime.now(UTC))

    assert store._key_scanner._list_kv_keys.await_count == _KV_KEYS_LIST_RETRY_ATTEMPTS


@pytest.mark.asyncio
async def test_read_snapshot_propagates_unrelated_keys_failure() -> None:
    store = NatsResourceAdmissionStore(MagicMock(), subject_prefix="tests.unit")
    kv = MagicMock()
    store._key_scanner._list_kv_keys = AsyncMock(side_effect=APIError())
    store._ensure_kv = AsyncMock(return_value=kv)

    with pytest.raises(ResourceStoreUnavailableError):
        await store.read_snapshot(evaluated_at=datetime.now(UTC))


@pytest.mark.nats
@pytest.mark.asyncio
async def test_read_snapshot_fresh_admission_bucket(nats_url: str) -> None:
    broker = NatsBroker(nats_url, connect_timeout=2, max_reconnect_attempts=0)
    await broker.start()
    try:
        prefix = f"tests.read-snapshot.{uuid4().hex}"
        store = NatsResourceAdmissionStore(broker, subject_prefix=prefix)
        evaluated_at = datetime.now(UTC)
        snapshot = await store.read_snapshot(evaluated_at=evaluated_at)
        assert snapshot.evaluated_at == evaluated_at
        assert snapshot.capacities == ()
        assert snapshot.resources == ()
        assert snapshot.execution_claims == ()
    finally:
        await broker.stop()


@pytest.mark.nats
@pytest.mark.asyncio
async def test_read_snapshot_retries_false_empty_listing_with_retained_owner(
    nats_url: str,
) -> None:
    broker = NatsBroker(nats_url, connect_timeout=2, max_reconnect_attempts=0)
    await broker.start()
    try:
        prefix = f"tests.read-snapshot.{uuid4().hex}"
        store = NatsResourceAdmissionStore(broker, subject_prefix=prefix)
        admission = ResourceAdmission(store)
        key = f"host.{uuid4().hex}"
        await admission.declare_capacity(ResourceRequirement(resource_key=key, units=1))
        acquired = await admission.try_acquire(
            _intent(worker_id="worker-a", execution_id="exec-retained", resource_key=key),
        )
        assert acquired.owner is not None

        kv = await store._ensure_kv()
        real_keys = store._key_scanner._list_kv_keys
        calls = 0

        async def keys_with_transient_false_empty(kv) -> list[str]:
            nonlocal calls
            calls += 1
            if calls == 1:
                raise NoKeysError()
            return await real_keys(kv)

        store._key_scanner._list_kv_keys = keys_with_transient_false_empty

        snapshot = await store.read_snapshot(evaluated_at=datetime.now(UTC))
        resource = next(item for item in snapshot.resources if item.resource_key == key)
        assert resource.owner == acquired.owner
        assert calls == 2
    finally:
        await broker.stop()


@pytest.mark.nats
@pytest.mark.asyncio
async def test_read_snapshot_false_empty_listing_fails_closed_with_retained_owner(
    nats_url: str,
) -> None:
    broker = NatsBroker(nats_url, connect_timeout=2, max_reconnect_attempts=0)
    await broker.start()
    try:
        prefix = f"tests.read-snapshot.{uuid4().hex}"
        store = NatsResourceAdmissionStore(broker, subject_prefix=prefix)
        admission = ResourceAdmission(store)
        key = f"host.{uuid4().hex}"
        await admission.declare_capacity(ResourceRequirement(resource_key=key, units=1))
        acquired = await admission.try_acquire(
            _intent(worker_id="worker-a", execution_id="exec-retained", resource_key=key),
        )
        assert acquired.owner is not None

        kv = await store._ensure_kv()

        async def keys_always_false_empty(kv) -> list[str]:
            raise NoKeysError()

        store._key_scanner._list_kv_keys = keys_always_false_empty

        with pytest.raises(ResourceStoreUnavailableError, match="retains JetStream messages"):
            await store.read_snapshot(evaluated_at=datetime.now(UTC))
    finally:
        await broker.stop()


@pytest.mark.nats
@pytest.mark.asyncio
async def test_snapshot_cleans_own_consumer_without_removing_other_watch(nats_url: str) -> None:
    broker = NatsBroker(nats_url, connect_timeout=2, max_reconnect_attempts=0)
    await broker.start()
    other = None
    try:
        store = NatsResourceAdmissionStore(broker, subject_prefix=f"tests.cleanup.{uuid4().hex}")
        admission = ResourceAdmission(store)
        await admission.declare_capacity(ResourceRequirement(resource_key="fixture.cpu", units=1))
        kv = await store._ensure_kv()
        js = broker.connection.jetstream()
        owner = (await admission.try_acquire(
            _intent(worker_id="writer", execution_id="job", resource_key="fixture.cpu"),
        )).owner
        assert owner is not None
        before = await js.stream_info(kv._stream)
        other = await kv.watchall(meta_only=True)
        baseline = {info.name for info in await js.consumers_info(kv._stream)}
        assert len(baseline) == 1
        for _ in range(5):
            snapshot = await admission.snapshot()
            assert snapshot.capacities[0].declaration.resource_key == "fixture.cpu"
            assert snapshot.resources[0].owner == owner
            after = await js.stream_info(kv._stream)
            assert (after.state.messages, after.state.last_seq) == (
                before.state.messages, before.state.last_seq,
            )
            assert {info.name for info in await js.consumers_info(kv._stream)} == baseline
    finally:
        try:
            if other is not None:
                info = await other._sub.consumer_info()
                await other.stop()
                await broker.connection.jetstream().delete_consumer(kv._stream, info.name)
        finally:
            await broker.stop()


@pytest.mark.nats
@pytest.mark.asyncio
async def test_cancelled_snapshot_cleans_watcher_after_initialization(nats_url: str) -> None:
    broker = NatsBroker(nats_url, connect_timeout=2, max_reconnect_attempts=0)
    await broker.start()
    task = None
    release = asyncio.Event()
    try:
        store = NatsResourceAdmissionStore(broker, subject_prefix=f"tests.cancel.{uuid4().hex}")
        admission = ResourceAdmission(store)
        await admission.declare_capacity(ResourceRequirement(resource_key="fixture.cpu", units=1))
        acquired = await admission.try_acquire(
            _intent(worker_id="writer", execution_id="job", resource_key="fixture.cpu"),
        )
        assert acquired.owner is not None
        kv = await store._ensure_kv()
        js = broker.connection.jetstream()
        baseline = {info.name for info in await js.consumers_info(kv._stream)}
        original = kv.watchall
        ready = asyncio.Event()

        async def delayed_watchall(**kwargs):
            watcher = await original(**kwargs)
            ready.set()
            await release.wait()
            return watcher

        kv.watchall = delayed_watchall
        task = asyncio.create_task(admission.snapshot())
        await asyncio.wait_for(ready.wait(), timeout=3)
        task.cancel()
        release.set()
        with pytest.raises(asyncio.CancelledError):
            await asyncio.wait_for(task, timeout=3)
        assert {info.name for info in await js.consumers_info(kv._stream)} == baseline
        retained = await admission.snapshot()
        assert retained.resources[0].owner == acquired.owner
    finally:
        try:
            release.set()
            if task is not None and not task.done():
                task.cancel()
            if task is not None:
                await asyncio.gather(task, return_exceptions=True)
        finally:
            await broker.stop()


@pytest.mark.nats
@pytest.mark.asyncio
async def test_failed_snapshot_cleans_watcher(nats_url: str, monkeypatch) -> None:
    broker = NatsBroker(nats_url, connect_timeout=2, max_reconnect_attempts=0)
    await broker.start()
    try:
        store = NatsResourceAdmissionStore(broker, subject_prefix=f"tests.failure.{uuid4().hex}")
        admission = ResourceAdmission(store)
        await admission.declare_capacity(ResourceRequirement(resource_key="fixture.cpu", units=1))
        kv = await store._ensure_kv()
        js = broker.connection.jetstream()
        baseline = {info.name for info in await js.consumers_info(kv._stream)}

        async def fail_next(self):
            raise APIError()

        monkeypatch.setattr(KeyValue.KeyWatcher, "__anext__", fail_next)
        with pytest.raises(ResourceStoreUnavailableError):
            await admission.snapshot()
        assert {info.name for info in await js.consumers_info(kv._stream)} == baseline
    finally:
        await broker.stop()


@pytest.mark.nats
@pytest.mark.asyncio
async def test_snapshot_reports_failed_consumer_deletion(nats_url: str, monkeypatch) -> None:
    broker = NatsBroker(nats_url, connect_timeout=2, max_reconnect_attempts=0)
    await broker.start()
    try:
        store = NatsResourceAdmissionStore(broker, subject_prefix=f"tests.deletefail.{uuid4().hex}")
        admission = ResourceAdmission(store)
        await admission.declare_capacity(ResourceRequirement(resource_key="fixture.cpu", units=1))
        acquired = await admission.try_acquire(
            _intent(worker_id="writer", execution_id="job", resource_key="fixture.cpu"),
        )
        kv = await store._ensure_kv()
        js = broker.connection.jetstream()
        baseline = {info.name for info in await js.consumers_info(kv._stream)}
        failing_js = MagicMock()
        failing_js._jsm.delete_consumer = AsyncMock(side_effect=APIError())
        original = store._key_scanner._jetstream
        monkeypatch.setattr(store._key_scanner, "_jetstream", AsyncMock(return_value=failing_js))
        with pytest.raises(ResourceStoreUnavailableError, match="Unable to list admission"):
            await admission.snapshot()
        leaked = {info.name for info in await js.consumers_info(kv._stream)} - baseline
        assert len(leaked) == 1
        for consumer in leaked:
            await js.delete_consumer(kv._stream, consumer)
        monkeypatch.setattr(store._key_scanner, "_jetstream", original)
        assert (await admission.snapshot()).resources[0].owner == acquired.owner
        assert {info.name for info in await js.consumers_info(kv._stream)} == baseline
    finally:
        await broker.stop()
