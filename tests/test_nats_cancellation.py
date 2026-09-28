import asyncio
import os

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
async def test_nats_cancellation_is_cooperative() -> None:
    from faststream.nats import NatsBroker

    jobs = SuperJobs(
        broker=NatsBroker(
            os.getenv("NATS_URL", "nats://localhost:4222"),
            connect_timeout=1,
        ),
    )
    job = Job("tests.nats.cancel", version="v1", request=Request, result=Result)
    started = asyncio.Event()

    @jobs.handler(job)
    async def handler(request: Request, context) -> Result:
        started.set()
        while True:
            await context.check_cancelled()
            await asyncio.sleep(0.01)

    try:
        await jobs.start()
    except Exception as exception:
        pytest.skip(f"NATS/JetStream is unavailable: {exception}")

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
