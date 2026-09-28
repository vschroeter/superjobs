import asyncio
import os

import pytest
from pydantic import BaseModel

from superjobs.jobs.job import Job
from superjobs.superjobs import SuperJobs


class Request(BaseModel):
    value: int


class Result(BaseModel):
    value: int


@pytest.mark.nats
@pytest.mark.asyncio
async def test_nats_redelivers_an_attempt_after_worker_restart() -> None:
    from faststream.nats import NatsBroker

    url = os.getenv("NATS_URL", "nats://localhost:4222")
    first_runtime = SuperJobs(
        broker=NatsBroker(url, connect_timeout=1),
    )
    second_runtime = SuperJobs(
        broker=NatsBroker(url, connect_timeout=1),
    )
    first_job = Job("tests.nats.restart", version="v1", request=Request, result=Result)
    second_job = Job("tests.nats.restart", version="v1", request=Request, result=Result)
    first_started = asyncio.Event()
    calls = {"first": 0, "second": 0}

    @first_runtime.handler(first_job)
    async def first_handler(request: Request, context) -> Result:
        calls["first"] += 1
        first_started.set()
        await asyncio.sleep(60)
        return Result(value=request.value)

    @second_runtime.handler(second_job)
    async def second_handler(request: Request, context) -> Result:
        calls["second"] += 1
        return Result(value=request.value)

    try:
        await first_runtime.start()
        await second_runtime.start()
    except Exception as exception:
        await first_runtime.stop()
        await second_runtime.stop()
        pytest.skip(f"NATS/JetStream is unavailable: {exception}")

    try:
        first_handle = await first_runtime.client(first_job).submit(Request(value=5))
        await asyncio.wait_for(first_started.wait(), timeout=1)
        await first_runtime.stop(graceful=False)

        second_handle = await second_runtime.client(second_job).get(first_handle.id)
        assert await second_handle.result(wait_timeout=2) == Result(value=5)
    finally:
        await first_runtime.stop()
        await second_runtime.stop()

    assert calls == {"first": 1, "second": 1}
