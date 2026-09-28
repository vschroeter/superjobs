import asyncio
import time
from datetime import UTC, datetime, timedelta

import pytest
from pydantic import BaseModel

from superjobs.exceptions.jobs import JobFailedError
from superjobs.jobs.events import JobFailed, JobRetryScheduled
from superjobs.jobs.execution import JobState
from superjobs.jobs.job import Job
from superjobs.jobs.retry_policy import FixedBackoff, RetryPolicy
from superjobs.superjobs import SuperJobs
from superjobs.transport.in_memory import InMemoryTransport


class Request(BaseModel):
    value: int


class Result(BaseModel):
    value: int


@pytest.mark.asyncio
async def test_attempt_timeout_has_a_stable_failure_code() -> None:
    jobs = SuperJobs(transport=InMemoryTransport())
    job = Job("tests.timeouts.attempt", version="v1", request=Request, result=Result)

    @jobs.handler(job)
    async def handler(request: Request, context) -> Result:
        await asyncio.sleep(0.05)
        return Result(value=request.value)

    async with jobs:
        handle = await jobs.client(job).submit(Request(value=1), timeout=0.005)
        with pytest.raises(JobFailedError) as raised:
            await handle.result()

    assert raised.value.error.code == "timed_out"
    assert (await handle.status()).state is JobState.FAILED


@pytest.mark.asyncio
async def test_attempt_timeout_covers_synchronous_handlers() -> None:
    jobs = SuperJobs(transport=InMemoryTransport())
    job = Job("tests.timeouts.sync", version="v1", request=Request, result=Result)

    @jobs.handler(job)
    def handler(request: Request, context) -> Result:
        time.sleep(0.05)
        return Result(value=request.value)

    async with jobs:
        handle = await jobs.client(job).submit(Request(value=1), timeout=0.005)
        with pytest.raises(JobFailedError) as raised:
            await handle.result()

    assert raised.value.error.code == "timed_out"


@pytest.mark.asyncio
async def test_execution_deadline_applies_across_retry_delay() -> None:
    jobs = SuperJobs(transport=InMemoryTransport())
    job = Job("tests.timeouts.deadline", version="v1", request=Request, result=Result)
    calls = 0

    @jobs.handler(
        job,
        retry=RetryPolicy(max_attempts=3, backoff=FixedBackoff(0.05)),
    )
    async def handler(request: Request, context) -> Result:
        nonlocal calls
        calls += 1
        raise RuntimeError("temporary")

    async with jobs:
        handle = await jobs.client(job).submit(
            Request(value=1),
            deadline=datetime.now(UTC) + timedelta(milliseconds=15),
        )
        with pytest.raises(JobFailedError) as raised:
            await handle.result()
        events = [event async for event in handle.events()]

    assert calls == 1
    assert raised.value.error.code == "deadline_exceeded"
    assert any(isinstance(event.data, JobRetryScheduled) for event in events)
    assert isinstance(events[-1].data, JobFailed)


@pytest.mark.asyncio
async def test_deadline_expires_an_execution_waiting_for_a_worker() -> None:
    transport = InMemoryTransport()
    jobs = SuperJobs(transport=transport)
    job = Job("tests.timeouts.pending", version="v1", request=Request, result=Result)

    async with jobs:
        handle = await jobs.client(job).submit(
            Request(value=1),
            deadline=datetime.now(UTC) + timedelta(milliseconds=5),
        )
        with pytest.raises(JobFailedError) as raised:
            await handle.result(wait_timeout=0.2)

    assert raised.value.error.code == "deadline_exceeded"
