import os
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
async def test_nats_producer_only_deadline_is_enforced() -> None:
    from faststream.nats import NatsBroker

    jobs = SuperJobs(
        broker=NatsBroker(
            os.getenv("NATS_URL", "nats://localhost:4222"),
            connect_timeout=1,
        ),
    )
    job = Job("tests.nats.producer-deadline", version="v1", request=Request, result=Result)

    try:
        await jobs.start()
    except Exception as exception:
        pytest.skip(f"NATS/JetStream is unavailable: {exception}")

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
