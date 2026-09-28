import os
import uuid

import pytest
from pydantic import BaseModel

from superjobs.jobs.events import JobRetryScheduled, JobStarted
from superjobs.jobs.job import Job
from superjobs.jobs.retry_policy import FixedBackoff, RetryPolicy
from superjobs.superjobs import SuperJobs


class Request(BaseModel):
    value: int


class Result(BaseModel):
    value: int


@pytest.mark.nats
@pytest.mark.asyncio
async def test_nats_idempotency_and_retry_preserve_one_execution() -> None:
    from faststream.nats import NatsBroker

    jobs = SuperJobs(
        broker=NatsBroker(
            os.getenv("NATS_URL", "nats://localhost:4222"),
            connect_timeout=1,
        ),
    )
    job = Job(
        f"tests.nats.behavior.{uuid.uuid4().hex}",
        version="v1",
        request=Request,
        result=Result,
    )
    calls = 0

    @jobs.handler(
        job,
        retry=RetryPolicy(max_attempts=2, backoff=FixedBackoff(0.01)),
    )
    async def handler(request: Request, context) -> Result:
        nonlocal calls
        calls += 1
        if calls == 1:
            raise RuntimeError("temporary")
        return Result(value=request.value)

    try:
        await jobs.start()
    except Exception as exception:
        pytest.skip(f"NATS/JetStream is unavailable: {exception}")

    try:
        client = jobs.client(job)
        idempotency_key = f"same-{uuid.uuid4()}"
        first = await client.submit(Request(value=3), idempotency_key=idempotency_key)
        second = await client.submit(Request(value=3), idempotency_key=idempotency_key)
        assert first.id == second.id
        assert await first == Result(value=3)
        events = [event async for event in first.events()]
    finally:
        await jobs.stop()

    assert calls == 2
    assert any(isinstance(event.data, JobRetryScheduled) for event in events)
    assert [event.attempt for event in events if isinstance(event.data, JobStarted)] == [1, 2]
