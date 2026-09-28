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
async def test_nats_runtimes_share_the_default_load_balanced_pool() -> None:
    from faststream.nats import NatsBroker

    url = os.getenv("NATS_URL", "nats://localhost:4222")
    first_runtime = SuperJobs(
        broker=NatsBroker(url, connect_timeout=1),
    )
    second_runtime = SuperJobs(
        broker=NatsBroker(url, connect_timeout=1),
    )
    first_job = Job("tests.nats.pool", version="v1", request=Request, result=Result)
    second_job = Job("tests.nats.pool", version="v1", request=Request, result=Result)
    calls = {"first": 0, "second": 0}

    @first_runtime.handler(first_job)
    async def first_handler(request: Request, context) -> Result:
        calls["first"] += 1
        await asyncio.sleep(0.005)
        return Result(value=request.value)

    @second_runtime.handler(second_job)
    async def second_handler(request: Request, context) -> Result:
        calls["second"] += 1
        await asyncio.sleep(0.005)
        return Result(value=request.value)

    try:
        await first_runtime.start()
        await second_runtime.start()
    except Exception as exception:
        await first_runtime.stop()
        await second_runtime.stop()
        pytest.skip(f"NATS/JetStream is unavailable: {exception}")

    try:
        handles = [
            await first_runtime.client(first_job).submit(Request(value=value))
            for value in range(8)
        ]
        await asyncio.gather(*(handle.result() for handle in handles))
    finally:
        await first_runtime.stop()
        await second_runtime.stop()

    assert calls["first"] + calls["second"] == 8
    assert calls["first"] > 0
    assert calls["second"] > 0
