import asyncio
from dataclasses import dataclass

import pytest
from pydantic import BaseModel

from superjobs.exceptions.jobs import (
    IdempotencyConflictError,
    JobCancelledError,
    JobFailedError,
)
from superjobs.jobs.events import (
    JobCancelled,
    JobCompleted,
    JobFailed,
    JobRetryScheduled,
)
from superjobs.jobs.execution import (
    JobCancelledOutcome,
    JobFailedOutcome,
    JobState,
    JobSucceeded,
)
from superjobs.jobs.job import Job
from superjobs.jobs.job_context import ObservationPolicy
from superjobs.jobs.retry_policy import FixedBackoff, RetryPolicy
from superjobs.superjobs import SuperJobs
from superjobs.transport.in_memory import InMemoryTransport


class Request(BaseModel):
    value: int


class Result(BaseModel):
    value: int


@dataclass(frozen=True)
class Event:
    value: int


async def _wait_for_last_acknowledgement(
    transport: InMemoryTransport,
    job_id: str,
    attempt: int,
    state: JobState,
    *,
    timeout: float = 1.0,
) -> None:
    expected = (job_id, attempt, state)
    loop = asyncio.get_running_loop()
    deadline = loop.time() + timeout
    while loop.time() < deadline:
        if transport.ack_log and transport.ack_log[-1] == expected:
            return
        await asyncio.sleep(0)
    raise AssertionError(
        f"timed out waiting for last ack {expected}, got {transport.ack_log}",
    )


@pytest.mark.asyncio
async def test_same_idempotency_key_reuses_execution() -> None:
    transport = InMemoryTransport()
    jobs = SuperJobs(transport=transport)
    job = Job("tests.semantics.idempotency", version="v1", request=Request, result=Result)
    calls = 0

    @jobs.handler(job)
    async def handler(request: Request, context) -> Result:
        nonlocal calls
        calls += 1
        return Result(value=request.value)

    async with jobs:
        client = jobs.client(job)
        first = await client.submit(Request(value=1), idempotency_key="same")
        second = await client.submit(Request(value=1), idempotency_key="same")
        assert first.id == second.id
        assert await first.result() == Result(value=1)

    assert calls == 1


@pytest.mark.asyncio
async def test_completion_is_written_before_work_is_acknowledged() -> None:
    transport = InMemoryTransport()
    jobs = SuperJobs(transport=transport)
    job = Job("tests.semantics.ack-order", version="v1", request=Request, result=Result)

    @jobs.handler(job)
    async def handler(request: Request, context) -> Result:
        return Result(value=request.value)

    async with jobs:
        handle = await jobs.client(job).submit(Request(value=1))
        await handle.result()

    assert transport.ack_log == [(handle.id, 1, JobState.COMPLETED)]


@pytest.mark.asyncio
async def test_redelivery_of_completed_execution_skips_handler() -> None:
    transport = InMemoryTransport()
    jobs = SuperJobs(transport=transport)
    job = Job("tests.semantics.redelivery", version="v1", request=Request, result=Result)
    calls = 0

    @jobs.handler(job)
    async def handler(request: Request, context) -> Result:
        nonlocal calls
        calls += 1
        return Result(value=request.value)

    async with jobs:
        handle = await jobs.client(job).submit(Request(value=1))
        await handle.result()
        execution = await transport.get_execution(job.identity, handle.id)
        assert execution is not None
        await transport.publish(execution, attempt=2)
        await _wait_for_last_acknowledgement(
            transport,
            handle.id,
            2,
            JobState.COMPLETED,
        )

    assert calls == 1
    assert transport.ack_log[-1] == (handle.id, 2, JobState.COMPLETED)


@pytest.mark.asyncio
async def test_concurrent_duplicate_attempts_emit_one_terminal_event() -> None:
    transport = InMemoryTransport()
    jobs = SuperJobs(transport=transport)
    job = Job("tests.semantics.duplicate", version="v1", request=Request, result=Result)
    started = asyncio.Event()
    release = asyncio.Event()
    calls = 0

    @jobs.handler(job, concurrency=2)
    async def handler(request: Request, context) -> Result:
        nonlocal calls
        calls += 1
        started.set()
        await release.wait()
        return Result(value=request.value)

    async with jobs:
        handle = await jobs.client(job).submit(Request(value=1))
        await started.wait()
        execution = await transport.get_execution(job.identity, handle.id)
        assert execution is not None
        await transport.publish(execution, attempt=2)
        for _ in range(100):
            if calls == 2:
                break
            await asyncio.sleep(0.001)
        release.set()
        await handle.result()
        events = [event async for event in handle.events()]

    assert calls == 2
    assert sum(isinstance(event.data, JobCompleted) for event in events) == 1


@pytest.mark.asyncio
async def test_conflicting_idempotency_submission_is_rejected() -> None:
    transport = InMemoryTransport()
    jobs = SuperJobs(transport=transport)
    job = Job("tests.semantics.idempotency-conflict", version="v1", request=Request, result=Result)

    @jobs.handler(job)
    async def handler(request: Request, context) -> Result:
        return Result(value=request.value)

    async with jobs:
        client = jobs.client(job)
        await client.submit(Request(value=1), idempotency_key="same")
        with pytest.raises(IdempotencyConflictError):
            await client.submit(Request(value=2), idempotency_key="same")


@pytest.mark.asyncio
async def test_failed_result_and_non_throwing_outcome() -> None:
    transport = InMemoryTransport()
    jobs = SuperJobs(transport=transport)
    job = Job("tests.semantics.failure", version="v1", request=Request, result=Result)

    @jobs.handler(job)
    async def handler(request: Request, context) -> Result:
        raise RuntimeError("boom")

    async with jobs:
        handle = await jobs.client(job).submit(Request(value=1))
        with pytest.raises(JobFailedError, match="boom"):
            await handle.result()
        outcome = await handle.outcome()

    assert isinstance(outcome, JobFailedOutcome)
    assert outcome.error.message == "boom"


@pytest.mark.asyncio
async def test_cooperative_cancellation_is_terminal_and_not_retried() -> None:
    transport = InMemoryTransport()
    jobs = SuperJobs(transport=transport)
    job = Job(
        "tests.semantics.cancel",
        version="v1",
        request=Request,
        result=Result,
        event=Event,
    )
    started = asyncio.Event()
    calls = 0

    @jobs.handler(job, retry=RetryPolicy(max_attempts=3))
    async def handler(request: Request, context) -> Result:
        nonlocal calls
        calls += 1
        started.set()
        while True:
            await context.check_cancelled()
            await asyncio.sleep(0.001)

    async with jobs:
        handle = await jobs.client(job).submit(Request(value=1))
        await started.wait()
        await handle.cancel()
        with pytest.raises(JobCancelledError):
            await handle.result()
        events = [event async for event in handle.events()]

    assert calls == 1
    assert isinstance(events[-1].data, JobCancelled)
    assert not any(isinstance(event.data, JobRetryScheduled) for event in events)


@pytest.mark.asyncio
async def test_pending_cancellation_completes_without_a_worker() -> None:
    transport = InMemoryTransport()
    jobs = SuperJobs(transport=transport)
    job = Job("tests.semantics.pending-cancel", version="v1", request=Request, result=Result)

    async with jobs:
        handle = await jobs.client(job).submit(Request(value=1))
        status = await handle.cancel()
        outcome = await handle.outcome()
        events = [event async for event in handle.events()]

    assert status.state is JobState.CANCELLED
    assert isinstance(outcome, JobCancelledOutcome)
    assert isinstance(events[-1].data, JobCancelled)


@pytest.mark.asyncio
async def test_local_wait_timeout_does_not_cancel_the_execution() -> None:
    transport = InMemoryTransport()
    jobs = SuperJobs(transport=transport)
    job = Job("tests.semantics.wait-timeout", version="v1", request=Request, result=Result)

    async with jobs:
        handle = await jobs.client(job).submit(Request(value=1))
        with pytest.raises(asyncio.TimeoutError):
            await handle.result(wait_timeout=0.001)
        assert (await handle.status()).state is JobState.PENDING
        await handle.cancel()


@pytest.mark.asyncio
async def test_retry_succeeds_on_second_attempt() -> None:
    transport = InMemoryTransport()
    jobs = SuperJobs(transport=transport)
    job = Job("tests.semantics.retry", version="v1", request=Request, result=Result)
    calls = 0

    @jobs.handler(
        job,
        retry=RetryPolicy(max_attempts=2),
    )
    async def handler(request: Request, context) -> Result:
        nonlocal calls
        calls += 1
        if calls == 1:
            raise RuntimeError("temporary")
        return Result(value=request.value)

    async with jobs:
        handle = await jobs.client(job).submit(Request(value=2))
        assert await handle.result() == Result(value=2)
        events = [event async for event in handle.events()]

    assert calls == 2
    assert any(isinstance(event.data, JobRetryScheduled) for event in events)
    assert events[-1].data.__class__.__name__ == "JobCompleted"


@pytest.mark.asyncio
async def test_progress_snapshot_is_scoped_to_each_attempt() -> None:
    transport = InMemoryTransport()
    jobs = SuperJobs(transport=transport)
    job = Job(
        "tests.semantics.retry-progress",
        version="v1",
        request=Request,
        result=Result,
    )
    calls = 0

    @jobs.handler(job, retry=RetryPolicy(max_attempts=2))
    async def handler(request: Request, context) -> Result:
        nonlocal calls
        calls += 1
        if calls == 1:
            await context.progress(completed=1, total=2)
            raise RuntimeError("temporary")
        assert context.progress_snapshot is None
        return Result(value=request.value)

    async with jobs:
        handle = await jobs.client(job).submit(Request(value=2))
        assert await handle.result() == Result(value=2)

    assert calls == 2


@pytest.mark.asyncio
async def test_restart_preserves_a_retry_waiting_for_backoff() -> None:
    transport = InMemoryTransport()
    jobs = SuperJobs(transport=transport)
    job = Job("tests.semantics.retry-restart", version="v1", request=Request, result=Result)
    calls = 0

    @jobs.handler(
        job,
        retry=RetryPolicy(max_attempts=2, backoff=FixedBackoff(0.05)),
    )
    async def handler(request: Request, context) -> Result:
        nonlocal calls
        calls += 1
        if calls == 1:
            raise RuntimeError("temporary")
        return Result(value=request.value)

    await jobs.start()
    handle = await jobs.client(job).submit(Request(value=4))
    try:
        for _ in range(100):
            if (await handle.status()).next_attempt_at is not None:
                break
            await asyncio.sleep(0.001)
        assert calls == 1
        await jobs.stop()
        await jobs.start()
        assert await handle.result() == Result(value=4)
    finally:
        await jobs.stop()

    assert calls == 2


@pytest.mark.asyncio
async def test_concurrency_one_serializes_executions() -> None:
    transport = InMemoryTransport()
    jobs = SuperJobs(transport=transport)
    job = Job("tests.semantics.serial", version="v1", request=Request, result=Result)
    active = 0
    maximum = 0

    @jobs.handler(job, concurrency=1)
    async def handler(request: Request, context) -> Result:
        nonlocal active, maximum
        active += 1
        maximum = max(maximum, active)
        await asyncio.sleep(0.01)
        active -= 1
        return Result(value=request.value)

    async with jobs:
        client = jobs.client(job)
        first = await client.submit(Request(value=1))
        second = await client.submit(Request(value=2))
        await asyncio.gather(first.result(), second.result())

    assert maximum == 1


@pytest.mark.asyncio
async def test_multiple_runtimes_share_one_load_balanced_job_pool() -> None:
    transport = InMemoryTransport()
    first_runtime = SuperJobs(transport=transport)
    second_runtime = SuperJobs(transport=transport)
    job = Job(
        "tests.semantics.pool",
        version="v1",
        request=Request,
        result=Result,
    )
    calls = {"first": 0, "second": 0}

    @first_runtime.handler(job)
    async def first_handler(request: Request, context) -> Result:
        calls["first"] += 1
        return Result(value=request.value)

    @second_runtime.handler(job)
    async def second_handler(request: Request, context) -> Result:
        calls["second"] += 1
        return Result(value=request.value)

    await first_runtime.start()
    await second_runtime.start()
    try:
        handles = [
            await first_runtime.client(job).submit(Request(value=value))
            for value in range(8)
        ]
        await asyncio.gather(*(handle.result() for handle in handles))
    finally:
        await first_runtime.stop()
        await second_runtime.stop()

    assert calls["first"] + calls["second"] == 8
    assert calls["first"] > 0
    assert calls["second"] > 0
