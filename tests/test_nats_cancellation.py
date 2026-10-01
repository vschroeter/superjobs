import uuid
import asyncio

import pytest
from pydantic import BaseModel

from superjobs.exceptions.jobs import JobCancelledError
from superjobs.jobs.events import JobCancelled
from superjobs.jobs.job import Job
from superjobs.superjobs import SuperJobs


class Request(BaseModel):
    value: int


class Result(BaseModel):
    value: int


@pytest.mark.nats
@pytest.mark.asyncio
async def test_nats_cancellation_is_cooperative(nats_broker, nats_queue_config) -> None:
    test_id = uuid.uuid4().hex
    jobs = SuperJobs(broker=nats_broker, queue_config=nats_queue_config)
    job = Job(f"tests.nats.cancel.{test_id}", version="v1", request=Request, result=Result)
    started = asyncio.Event()

    @jobs.handler(job)
    async def handler(request: Request, context) -> Result:
        started.set()
        while True:
            await context.check_cancelled()
            await asyncio.sleep(0.01)

    await jobs.start()
    try:
        handle = await jobs.client(job).submit(Request(value=1))
        await started.wait()
        await handle.cancel()
        with pytest.raises(JobCancelledError):
            await handle.result()
        events = [event async for event in handle.events()]
    finally:
        await jobs.stop()

    assert isinstance(events[-1].data, JobCancelled)
