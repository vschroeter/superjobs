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
async def test_nats_redelivers_an_attempt_after_worker_restart(nats_broker_factory, nats_queue_config) -> None:
    test_id = uuid.uuid4().hex
    first_runtime = SuperJobs(broker=nats_broker_factory(), queue_config=nats_queue_config)
    second_runtime = SuperJobs(broker=nats_broker_factory(), queue_config=nats_queue_config)
    first_job = Job(f"tests.nats.restart.{test_id}", version="v1", request=Request, result=Result)
    second_job = Job(f"tests.nats.restart.{test_id}", version="v1", request=Request, result=Result)
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

    await first_runtime.start()
    try:
        first_handle = await first_runtime.client(first_job).submit(Request(value=5))
        await asyncio.wait_for(first_started.wait(), timeout=1)
        await first_runtime.stop(graceful=False)
        await second_runtime.start()

        second_handle = await second_runtime.client(second_job).get(first_handle.id)
        assert await second_handle.result(wait_timeout=2) == Result(value=5)
    finally:
        await first_runtime.stop()
        await second_runtime.stop()

    assert calls == {"first": 1, "second": 1}
