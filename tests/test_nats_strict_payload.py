import asyncio
import os
import uuid

import pytest
from pydantic import BaseModel, ValidationError, field_validator

from superjobs.exceptions.jobs import JobFailedError
from superjobs.jobs.job import Job
from superjobs.jobs.retry_policy import RetryPolicy
from superjobs.superjobs import SuperJobs


class Request(BaseModel):
    value: int


class Result(BaseModel):
    value: int


class Event(BaseModel):
    value: int

    @field_validator("value", mode="before")
    @classmethod
    def convert(cls, raw: object) -> object:
        if isinstance(raw, str) and raw.isdigit():
            return int(raw)
        return raw


@pytest.mark.nats
@pytest.mark.asyncio
async def test_nats_strict_payload_boundaries() -> None:
    from faststream.nats import NatsBroker

    broker = NatsBroker(
        os.getenv("NATS_URL", "nats://localhost:4222"),
        connect_timeout=2,
    )
    jobs = SuperJobs(broker=broker)
    run_id = uuid.uuid4().hex
    ok_job = Job(
        f"tests.nats.strict_payload.ok.{run_id}",
        version="v1",
        request=Request,
        result=Result,
        event=Event,
    )
    invalid_job = Job(
        f"tests.nats.strict_payload.invalid.{run_id}",
        version="v1",
        request=Request,
        result=Result,
    )
    seen: list[int] = []
    invalid_attempts = 0

    @jobs.handler(ok_job)
    async def ok_handler(request: Request, context) -> Result:
        seen.append(request.value)
        with pytest.raises(ValidationError):
            await context.emit(Event.model_construct(value="not-an-int"))
        await context.emit(Event.model_construct(value=str(request.value)))
        return Result(value=request.value + 1)

    @jobs.handler(invalid_job, retry=RetryPolicy(max_attempts=3))
    async def invalid_handler(request: Request, context) -> Result:
        nonlocal invalid_attempts
        invalid_attempts += 1
        return Result.model_construct(value="not-an-int")

    try:
        await asyncio.wait_for(jobs.start(), timeout=10)
        ok_client = jobs.client(ok_job)
        handle = await ok_client.submit(Request(value=7))
        assert (await handle.result(wait_timeout=5)).value == 8
        assert seen == [7]
        events = [event.data async for event in handle.events() if isinstance(event.data, Event)]
        assert events == [Event(value=7)]
        assert type(events[0].value) is int

        with pytest.raises(ValidationError):
            await ok_client.submit(Request.model_construct(value="not-an-int"))
        assert seen == [7]

        invalid_handle = await jobs.client(invalid_job).submit(Request(value=1))
        with pytest.raises(JobFailedError) as failed:
            await invalid_handle.result(wait_timeout=5)
        assert failed.value.error.code == "invalid_result"
        assert invalid_attempts == 1
    finally:
        await asyncio.wait_for(jobs.stop(), timeout=10)
