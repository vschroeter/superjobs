from __future__ import annotations

from datetime import UTC, datetime
from unittest.mock import AsyncMock, MagicMock
from uuid import uuid4

import pytest
from faststream.nats import NatsBroker
from nats.js.errors import APIError, NoKeysError

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
    kv.keys = AsyncMock(side_effect=NoKeysError())
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
    kv.keys = AsyncMock(side_effect=[NoKeysError(), ()])
    kv.status = AsyncMock(return_value=_bucket_status(messages=2))
    store._ensure_kv = AsyncMock(return_value=kv)

    snapshot = await store.read_snapshot(evaluated_at=datetime.now(UTC))

    assert kv.keys.await_count == 2
    assert snapshot.resources == ()


@pytest.mark.asyncio
async def test_read_snapshot_false_empty_listing_fails_closed_unit() -> None:
    store = NatsResourceAdmissionStore(MagicMock(), subject_prefix="tests.unit")
    kv = MagicMock()
    kv.keys = AsyncMock(side_effect=NoKeysError())
    kv.status = AsyncMock(return_value=_bucket_status(messages=3))
    store._ensure_kv = AsyncMock(return_value=kv)

    with pytest.raises(ResourceStoreUnavailableError, match="retains JetStream messages"):
        await store.read_snapshot(evaluated_at=datetime.now(UTC))

    assert kv.keys.await_count == _KV_KEYS_LIST_RETRY_ATTEMPTS


@pytest.mark.asyncio
async def test_read_snapshot_propagates_unrelated_keys_failure() -> None:
    store = NatsResourceAdmissionStore(MagicMock(), subject_prefix="tests.unit")
    kv = MagicMock()
    kv.keys = AsyncMock(side_effect=APIError())
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
        real_keys = kv.keys
        calls = 0

        async def keys_with_transient_false_empty() -> list[str]:
            nonlocal calls
            calls += 1
            if calls == 1:
                raise NoKeysError()
            return await real_keys()

        kv.keys = keys_with_transient_false_empty

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

        async def keys_always_false_empty() -> list[str]:
            raise NoKeysError()

        kv.keys = keys_always_false_empty

        with pytest.raises(ResourceStoreUnavailableError, match="retains JetStream messages"):
            await store.read_snapshot(evaluated_at=datetime.now(UTC))
    finally:
        await broker.stop()
