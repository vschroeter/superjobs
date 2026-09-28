import os

import pytest
from pydantic import BaseModel

from superjobs.jobs.events import JobCompleted, JobEvent
from superjobs.jobs.job import Job
from superjobs.superjobs import SuperJobs


class Request(BaseModel):
    value: int


class Result(BaseModel):
    value: int


@pytest.mark.nats
@pytest.mark.asyncio
async def test_nats_submit_result_and_replay() -> None:
    from faststream.nats import NatsBroker

    broker = NatsBroker(
        os.getenv("NATS_URL", "nats://localhost:4222"),
        connect_timeout=1,
    )
    jobs = SuperJobs(broker=broker)
    job = Job(
        "tests.nats.submit",
        version="v1",
        request=Request,
        result=Result,
    )

    @jobs.handler(job)
    async def handler(request: Request, context) -> Result:
        return Result(value=request.value * 2)

    try:
        await jobs.start()
    except Exception as exception:
        pytest.skip(f"NATS/JetStream is unavailable: {exception}")

    try:
        handle = await jobs.client(job).submit(Request(value=21))
        assert await handle == Result(value=42)
        events = [event async for event in handle.events()]
    finally:
        await jobs.stop()

    assert isinstance(events[-1], JobEvent)
    assert isinstance(events[-1].data, JobCompleted)
