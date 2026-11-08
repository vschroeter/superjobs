from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest

from superjobs.discovery.errors import DiscoveryUnavailableError
from superjobs.discovery.nats import NatsDiscoveryBackend, registration_kv_key
from superjobs.job_identity import JobIdentity


@pytest.mark.parametrize("early_marker", [True, False])
@pytest.mark.parametrize("operation", [None, "DEL", "PURGE"])
@pytest.mark.parametrize("pending", [0, 1])
async def test_snapshot_waits_for_server_delivery_boundary(early_marker, operation, pending):
    backend = NatsDiscoveryBackend(MagicMock(), subject_prefix="test")
    job = JobIdentity(name="samplegen.tts.qwen", version="1")
    entry = SimpleNamespace(
        key=registration_kv_key("worker-qwen", job),
        value=b"retained-registration",
        operation=operation,
    )
    items = ([None] if early_marker else []) + [entry, None]

    async def updates():
        for item in items:
            yield item

    watcher = MagicMock()
    watcher.__aiter__ = lambda _: updates()
    watcher._sub.consumer_info = AsyncMock(
        return_value=SimpleNamespace(
            delivered=SimpleNamespace(consumer_seq=1 - pending), num_pending=pending
        )
    )
    backend._start_kv_watchall = AsyncMock(return_value=(watcher, None))
    backend._release_kv_watchall = AsyncMock()

    entries = await backend._collect_snapshot_entries(MagicMock())

    expected = [] if operation else [(("worker-qwen", job.name, job.version), entry.value)]
    assert entries == expected
    backend._release_kv_watchall.assert_awaited_once()


async def test_snapshot_accepts_verified_empty_server_boundary():
    backend = NatsDiscoveryBackend(MagicMock(), subject_prefix="test")

    async def updates():
        yield None

    watcher = MagicMock()
    watcher.__aiter__ = lambda _: updates()
    watcher._sub.consumer_info = AsyncMock(
        return_value=SimpleNamespace(delivered=SimpleNamespace(consumer_seq=0), num_pending=0)
    )
    backend._start_kv_watchall = AsyncMock(return_value=(watcher, None))
    backend._release_kv_watchall = AsyncMock()

    assert await backend._collect_snapshot_entries(MagicMock()) == []


async def test_snapshot_rejects_incomplete_server_delivery_boundary():
    backend = NatsDiscoveryBackend(MagicMock(), subject_prefix="test")

    async def updates():
        yield None

    watcher = MagicMock()
    watcher.__aiter__ = lambda _: updates()
    watcher._sub.consumer_info = AsyncMock(
        return_value=SimpleNamespace(delivered=SimpleNamespace(consumer_seq=1), num_pending=0)
    )
    backend._start_kv_watchall = AsyncMock(return_value=(watcher, None))
    backend._release_kv_watchall = AsyncMock()

    with pytest.raises(DiscoveryUnavailableError, match="incomplete"):
        await backend._collect_snapshot_entries(MagicMock())
