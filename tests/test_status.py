import asyncio

import pytest
from pydantic import BaseModel

from superjobs.jobs.execution import JobState, ProgressSnapshot
from superjobs.jobs.job import Job
from superjobs.superjobs import SuperJobs
from superjobs.transport.in_memory import InMemoryTransport


class Request(BaseModel):
    value: int


class Result(BaseModel):
    value: int


@pytest.mark.asyncio
async def test_status_exposes_attempt_progress_and_terminal_timestamps() -> None:
    jobs = SuperJobs(transport=InMemoryTransport())
    job = Job("tests.status.snapshot", version="v1", request=Request, result=Result)
    started = asyncio.Event()
    release = asyncio.Event()

    @jobs.handler(job)
    async def handler(request: Request, context) -> Result:
        started.set()
        await context.progress(2, 5)
        await release.wait()
        return Result(value=request.value)

    async with jobs:
        handle = await jobs.client(job).submit(Request(value=1))
        await started.wait()
        await asyncio.sleep(0.02)
        running = await handle.status()
        release.set()
        result = await handle.result()
        completed = await handle.status()

    assert running.state is JobState.RUNNING
    assert running.attempt == 1
    assert running.progress == ProgressSnapshot(completed=2, total=5)
    assert running.started_at is not None
    assert result == Result(value=1)
    assert completed.state is JobState.COMPLETED
    assert completed.completed_at is not None
