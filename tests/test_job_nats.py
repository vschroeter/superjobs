import uuid
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
async def test_nats_submit_result_and_replay(nats_broker, nats_queue_config) -> None:
    test_id = uuid.uuid4().hex
    jobs = SuperJobs(broker=nats_broker, queue_config=nats_queue_config)
    job = Job(
        f"tests.nats.submit.{test_id}",
        version="v1",
        request=Request,
        result=Result,
    )

    @jobs.handler(job)
    async def handler(request: Request, context) -> Result:
        return Result(value=request.value * 2)

    await jobs.start()
    try:
        handle = await jobs.client(job).submit(Request(value=21))
        assert await handle == Result(value=42)
        events = [event async for event in handle.events()]
    finally:
        await jobs.stop()

    assert isinstance(events[-1], JobEvent)
    assert isinstance(events[-1].data, JobCompleted)
