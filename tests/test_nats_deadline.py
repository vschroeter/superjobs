import uuid
from datetime import UTC, datetime, timedelta

import pytest
from pydantic import BaseModel

from superjobs.exceptions.jobs import JobFailedError
from superjobs.jobs.job import Job
from superjobs.superjobs import SuperJobs


class Request(BaseModel):
    value: int


class Result(BaseModel):
    value: int


@pytest.mark.nats
@pytest.mark.asyncio
async def test_nats_producer_only_deadline_is_enforced(nats_broker, nats_queue_config) -> None:
    test_id = uuid.uuid4().hex
    jobs = SuperJobs(broker=nats_broker, queue_config=nats_queue_config)
    job = Job(f"tests.nats.producer-deadline.{test_id}", version="v1", request=Request, result=Result)

    await jobs.start()
    try:
        handle = await jobs.client(job).submit(
            Request(value=1),
            deadline=datetime.now(UTC) + timedelta(milliseconds=20),
        )
        with pytest.raises(JobFailedError) as raised:
            await handle.result(wait_timeout=1)
    finally:
        await jobs.stop()

    assert raised.value.error.code == "deadline_exceeded"
