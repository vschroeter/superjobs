from __future__ import annotations

import asyncio
import uuid
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from superjobs import HandlerCatalog, JobState, NoRequestJob, SuperJobs
from superjobs.jobs.execution import JobSucceeded
from superjobs.jobs.resource_cleanup import acknowledge_resource_cleanup
from superjobs.transport.backend import SubmissionOptions
from superjobs.transport.nats_backend import NatsJobBackend

RESOURCE_KEY = "device.gpu0"

job_a = NoRequestJob("tests.nats.resource.job_a", version="v1", result=str)
job_b = NoRequestJob("tests.nats.resource.job_b", version="v1", result=str)

pytestmark = pytest.mark.nats


async def _bounded_outcome(handle, *, seconds: float = 15.0):
    return await asyncio.wait_for(handle.outcome(), timeout=seconds)


async def _bounded_status(handle, *, seconds: float = 5.0):
    return await asyncio.wait_for(handle.status(), timeout=seconds)


@pytest.mark.asyncio
async def test_nats_cross_job_resource_exclusion(
    nats_broker_factory,
    nats_queue_config,
) -> None:
    entered = asyncio.Event()
    release = asyncio.Event()
    peak = 0
    active = 0

    catalog_a = HandlerCatalog()
    catalog_b = HandlerCatalog()

    @catalog_a.handler(job_a, resources={RESOURCE_KEY: 1})
    async def handle_a(context) -> str:
        nonlocal peak, active
        active += 1
        peak = max(peak, active)
        entered.set()
        await asyncio.wait_for(release.wait(), timeout=10.0)
        active -= 1
        acknowledge_resource_cleanup(context)
        return "a"

    @catalog_b.handler(job_b, resources={RESOURCE_KEY: 1})
    async def handle_b(context) -> str:
        nonlocal peak, active
        active += 1
        peak = max(peak, active)
        await asyncio.sleep(0.05)
        active -= 1
        acknowledge_resource_cleanup(context)
        return "b"

    runtime_a = SuperJobs(
        broker=nats_broker_factory(),
        queue_config=nats_queue_config,
        handlers=catalog_a,
        resource_capacities={RESOURCE_KEY: 1},
        discovery=False,
    )
    runtime_b = SuperJobs(
        broker=nats_broker_factory(),
        queue_config=nats_queue_config,
        handlers=catalog_b,
        discovery=False,
    )
    await runtime_a.start()
    await runtime_b.start()
    try:
        first = await runtime_a.client(job_a).submit()
        second = await runtime_b.client(job_b).submit()
        await asyncio.wait_for(entered.wait(), timeout=10.0)
        assert peak == 1
        waiting = await _bounded_status(second)
        assert waiting.state in {JobState.WAITING_FOR_RESOURCE, JobState.PENDING}
        release.set()
        outcome_a = await _bounded_outcome(first)
        outcome_b = await _bounded_outcome(second)
        assert isinstance(outcome_a, JobSucceeded)
        assert isinstance(outcome_b, JobSucceeded)
        record_a = await runtime_a.transport.get_execution(job_a.identity, first.id)
        record_b = await runtime_b.transport.get_execution(job_b.identity, second.id)
        assert record_a is not None and record_a.attempt == 1
        assert record_b is not None and record_b.attempt == 1
    finally:
        await runtime_a.stop(graceful=False)
        await runtime_b.stop(graceful=False)


@pytest.mark.asyncio
async def test_resource_job_work_item_uses_wire_attempt_not_redelivery(
    nats_broker,
    nats_queue_config,
) -> None:
    backend = NatsJobBackend(nats_broker, nats_queue_config)
    backend.register_resource_job(job_a.identity)
    await backend.start()
    try:
        execution = await backend.submit(
            job_a.identity,
            request_payload=b"",
            request_media_type=None,
            fingerprint="fp",
            options=SubmissionOptions(
                idempotency_key=f"id-{uuid.uuid4().hex}",
                job_id=f"job-{uuid.uuid4().hex}",
            ),
        )
        await backend.publish(execution, attempt=1)
        subscription = await backend.subscribe_work(job_a.identity)
        subscriber = subscription._subscriber
        message = SimpleNamespace(
            body=__import__("msgpack").packb(
                {"job_id": execution.job_id, "attempt": 1},
                use_bin_type=True,
            ),
            metadata=SimpleNamespace(num_delivered=7),
            reject=AsyncMock(),
            nack=AsyncMock(),
        )
        subscriber.get_one = AsyncMock(return_value=message)
        item = await subscription.__anext__()
        assert item.attempt == 1
        await subscription.close()
    finally:
        await backend.stop()


async def test_nats_resource_retry_releases_the_previous_invocation_claim(
    nats_broker_factory, nats_queue_config,
) -> None:
    from superjobs import FixedBackoff, JobContext, RetryPolicy

    catalog = HandlerCatalog()
    job = NoRequestJob("tests.nats.resource.retry", result=str)
    calls = 0

    @catalog.handler(job, resources={RESOURCE_KEY: 1}, retry=RetryPolicy(max_attempts=2, backoff=FixedBackoff(0)))
    async def run(context: JobContext[None]) -> str:
        nonlocal calls
        calls += 1
        try:
            if calls == 1:
                raise ValueError("inference failed; cleanup acknowledged")
            return "retried safely"
        finally:
            context.acknowledge_resource_cleanup()

    runtime = SuperJobs(broker=nats_broker_factory(), queue_config=nats_queue_config, handlers=catalog, resource_capacities={RESOURCE_KEY: 1}, discovery=False)
    await runtime.start()
    try:
        handle = await runtime.client(job).submit()
        assert await asyncio.wait_for(handle.result(), 10) == "retried safely"
        assert calls == 2
        assert (await handle.status()).attempt == 2
    finally:
        await asyncio.wait_for(runtime.stop(graceful=False), 5)
