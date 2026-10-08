"""Regression tests for request-consumer survival after delivery finalization failures."""

from __future__ import annotations

import asyncio

import pytest
from pydantic import BaseModel

from superjobs.exceptions.jobs import JobFailedError
from superjobs.jobs.execution import JobState
from superjobs.jobs.job import Job
from superjobs.jobs.job_identity import JobIdentity
from superjobs.jobs.retry_policy import RetryPolicy
from superjobs.superjobs import SuperJobs
from superjobs.transport.in_memory import InMemoryTransport


class _Request(BaseModel):
    label: str


class _Result(BaseModel):
    label: str


async def _wait_for_terminal(
    transport: InMemoryTransport,
    identity: JobIdentity,
    job_id: str,
    *,
    state: JobState,
    timeout: float = 2.0,
) -> None:
    loop = asyncio.get_running_loop()
    deadline = loop.time() + timeout
    while loop.time() < deadline:
        execution = await transport.get_execution(identity, job_id)
        if execution is not None and execution.state is state:
            return
        await asyncio.sleep(0)
    raise AssertionError(f"timed out waiting for {job_id} to reach {state}")


@pytest.mark.parametrize("concurrency", [1, 2])
@pytest.mark.asyncio
async def test_consumer_survives_write_completion_transport_failure(
    concurrency: int,
) -> None:
    transport = InMemoryTransport()
    jobs = SuperJobs(transport=transport)
    job = Job(
        "tests.finalization.recovery",
        version="v1",
        request=_Request,
        result=_Result,
    )
    write_failures_remaining = 1
    original_write = transport.write_completion

    async def flaky_write_completion(*args, **kwargs):
        nonlocal write_failures_remaining
        if write_failures_remaining > 0:
            write_failures_remaining -= 1
            raise ConnectionError("broker offline during completion write")
        return await original_write(*args, **kwargs)

    transport.write_completion = flaky_write_completion  # type: ignore[method-assign]

    @jobs.handler(job, concurrency=concurrency, retry=RetryPolicy(max_attempts=1))
    async def handler(request: _Request, context) -> _Result:
        return _Result(label=request.label)

    async with jobs:
        client = jobs.client(job)
        first = await client.submit(_Request(label="first"))
        await _wait_for_terminal(
            transport,
            job.identity,
            first.id,
            state=JobState.COMPLETED,
        )

        second = await client.submit(_Request(label="second"))
        await _wait_for_terminal(
            transport,
            job.identity,
            second.id,
            state=JobState.COMPLETED,
        )

        handler_impl = jobs._handlers[job.canonical_name]
        consumer = handler_impl._consumer_task
        assert consumer is not None and not consumer.done()


@pytest.mark.asyncio
async def test_consumer_survives_failure_completion_write_and_redelivers() -> None:
    transport = InMemoryTransport()
    jobs = SuperJobs(transport=transport)
    job = Job(
        "tests.finalization.failure-write",
        version="v1",
        request=_Request,
        result=_Result,
    )
    write_failures_remaining = 1
    original_write = transport.write_completion

    async def flaky_failure_write(*args, **kwargs):
        nonlocal write_failures_remaining
        if write_failures_remaining > 0 and kwargs.get("state") is JobState.FAILED:
            write_failures_remaining -= 1
            raise ConnectionError("broker offline during failure completion write")
        return await original_write(*args, **kwargs)

    transport.write_completion = flaky_failure_write  # type: ignore[method-assign]

    @jobs.handler(job, concurrency=1, retry=RetryPolicy(max_attempts=1))
    async def handler(request: _Request, context) -> _Result:
        if request.label == "boom":
            raise RuntimeError("handler failed")
        return _Result(label=request.label)

    async with jobs:
        client = jobs.client(job)
        handle = await client.submit(_Request(label="boom"))
        await _wait_for_terminal(
            transport,
            job.identity,
            handle.id,
            state=JobState.FAILED,
        )
        with pytest.raises(JobFailedError, match="handler failed"):
            await handle.result()

        follow_up = await client.submit(_Request(label="after"))
        await _wait_for_terminal(
            transport,
            job.identity,
            follow_up.id,
            state=JobState.COMPLETED,
        )
