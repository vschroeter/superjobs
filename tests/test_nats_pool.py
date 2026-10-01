import uuid
import asyncio

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
async def test_nats_runtimes_share_the_default_load_balanced_pool(nats_broker_factory, nats_queue_config) -> None:
    test_id = uuid.uuid4().hex
    first_runtime = SuperJobs(broker=nats_broker_factory(), queue_config=nats_queue_config)
    second_runtime = SuperJobs(broker=nats_broker_factory(), queue_config=nats_queue_config)
    first_job = Job(f"tests.nats.pool.{test_id}", version="v1", request=Request, result=Result)
    second_job = Job(f"tests.nats.pool.{test_id}", version="v1", request=Request, result=Result)
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

    await first_runtime.start()
    await second_runtime.start()
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
