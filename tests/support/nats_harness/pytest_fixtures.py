"""Pytest integration for the owned NATS harness."""

from __future__ import annotations

import asyncio
import functools
import os
import uuid

import pytest

from tests.support.nats_harness.pinned import ENV_URL
from tests.support.nats_harness.server import OwnedNatsServer, ensure_external_target


def _session_has_nats_tests(session: pytest.Session) -> bool:
    return any(item.get_closest_marker("nats") is not None for item in session.items)


@pytest.fixture(scope="session")
def nats_server_target(request: pytest.FixtureRequest):
    if not _session_has_nats_tests(request.session):
        yield None
        return

    external_url = os.environ.get(ENV_URL)
    if external_url:
        yield ensure_external_target(external_url)
        return

    owner = OwnedNatsServer()
    target = owner.start()
    try:
        yield target
    finally:
        owner.stop(target)


@pytest.fixture
def nats_owned_server():
    """Function-scoped owned broker with restart/store-reuse controls."""
    owner = OwnedNatsServer()
    target = owner.start()
    try:
        yield owner, target
    finally:
        owner.stop(target)


@pytest.fixture
def nats_url(nats_server_target) -> str:
    if nats_server_target is None:
        pytest.fail(
            "The nats_url fixture is only valid for tests marked @pytest.mark.nats"
        )
    return nats_server_target.url


@pytest.fixture
def nats_queue_config():
    from superjobs.transport.implementations.nats import NatsQueueConfig

    prefix = f"test_{uuid.uuid4().hex}"
    return NatsQueueConfig(
        stream=prefix, observation_stream=f"{prefix}_observations",
        subject_prefix=prefix, completion_bucket=f"{prefix}_completions",
        idempotency_bucket=f"{prefix}_idempotency",
    )


@pytest.fixture
def nats_broker(nats_url: str):
    from faststream.nats import NatsBroker

    return NatsBroker(nats_url, connect_timeout=2, max_reconnect_attempts=0)


@pytest.fixture
def nats_broker_factory(nats_url: str):
    from faststream.nats import NatsBroker

    def factory():
        return NatsBroker(nats_url, connect_timeout=2, max_reconnect_attempts=0)

    return factory


def pytest_collection_modifyitems(items):
    for item in items:
        if item.get_closest_marker("nats") is None or not asyncio.iscoroutinefunction(item.obj):
            continue
        original = item.obj

        @functools.wraps(original)
        async def bounded(*args, __original=original, **kwargs):
            async with asyncio.timeout(25):
                return await __original(*args, **kwargs)

        item.obj = bounded
