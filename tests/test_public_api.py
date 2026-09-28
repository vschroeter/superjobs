import asyncio
from dataclasses import dataclass

import pytest
from pydantic import BaseModel

from superjobs.jobs.events import JobCompleted, JobEvent, JobProgress, JobStarted
from superjobs.jobs.execution import ProgressSnapshot
from superjobs.jobs.job import Job
from superjobs.jobs.job_context import ObservationPolicy
from superjobs.superjobs import SuperJobs
from superjobs.transport.in_memory import InMemoryTransport


class Request(BaseModel):
    value: int


class Result(BaseModel):
    value: int


@dataclass(frozen=True)
class Event:
    value: int


@pytest.mark.asyncio
async def test_async_context_manager_runs_handler_and_returns_typed_result() -> None:
    transport = InMemoryTransport()
    jobs = SuperJobs(transport=transport)
    job = Job("tests.public.result", version="v1", request=Request, result=Result)

    @jobs.handler(job)
    async def handler(request: Request, context) -> Result:
        return Result(value=request.value * 2)

    async with jobs:
        result = await jobs.client(job).run(Request(value=21))

    assert result == Result(value=42)
    assert not jobs.started


@pytest.mark.asyncio
async def test_submit_returns_reusable_handle_and_replays_observations() -> None:
    transport = InMemoryTransport()
    jobs = SuperJobs(transport=transport)
    job = Job(
        "tests.public.events",
        version="v1",
        request=Request,
        result=Result,
        event=Event,
    )

    @jobs.handler(job)
    async def handler(request: Request, context) -> Result:
        await context.emit(Event(value=request.value))
        await context.progress(completed=1, total=1)
        return Result(value=request.value)

    async with jobs:
        handle = await jobs.client(job).submit(Request(value=7))
        result = await handle.result()
        same_handle = await jobs.client(job).get(handle.id)
        events = [event async for event in same_handle.events()]
        second_observer = await jobs.client(job).get(handle.id)
        replayed = [event async for event in second_observer.events()]

    assert result == Result(value=7)
    assert [type(event.data) for event in events] == [
        JobStarted,
        Event,
        JobProgress,
        JobCompleted,
    ]
    assert all(isinstance(event, JobEvent) for event in events)
    assert [event.sequence for event in events] == [1, 2, 3, 4]
    assert [event.sequence for event in replayed] == [1, 2, 3, 4]


@pytest.mark.asyncio
async def test_runtime_can_restart_without_losing_the_backend() -> None:
    transport = InMemoryTransport()
    jobs = SuperJobs(transport=transport)
    job = Job("tests.public.restart", version="v1", request=Request, result=Result)

    @jobs.handler(job)
    async def handler(request: Request, context) -> Result:
        return Result(value=request.value)

    await jobs.start()
    first = await jobs.client(job).run(Request(value=1))
    await jobs.stop()
    await jobs.start()
    second = await jobs.client(job).run(Request(value=2))
    await jobs.stop()

    assert first == Result(value=1)
    assert second == Result(value=2)


@pytest.mark.asyncio
async def test_progress_is_latest_wins_while_logs_and_events_stay_ordered() -> None:
    transport = InMemoryTransport()
    jobs = SuperJobs(
        transport=transport,
        observation_policy=ObservationPolicy(
            batch_interval=0.01,
            progress_interval=1.0,
        ),
    )
    job = Job(
        "tests.public.coalescing",
        version="v1",
        request=Request,
        result=Result,
        event=Event,
    )

    @jobs.handler(job)
    async def handler(request: Request, context) -> Result:
        await context.log("first")
        await context.emit(Event(value=1))
        await context.progress(1, 3)
        await context.progress(2, 3)
        await context.progress(3, 3)
        await context.log("last")
        return Result(value=request.value)

    async with jobs:
        handle = await jobs.client(job).submit(Request(value=4))
        await handle.result()
        events = [event async for event in handle.events()]

    data = [event.data for event in events]
    assert [entry.message for entry in data if hasattr(entry, "message")] == [
        "first",
        "last",
    ]
    assert [entry.value for entry in data if isinstance(entry, Event)] == [1]
    progress = [entry for entry in data if isinstance(entry, JobProgress)]
    assert [(entry.completed, entry.total) for entry in progress] == [(3, 3)]
    assert transport.progress_update_log == [
        (handle.id, ProgressSnapshot(completed=3, total=3)),
    ]


@pytest.mark.asyncio
async def test_none_payload_contracts_support_context_only_handlers() -> None:
    jobs = SuperJobs(transport=InMemoryTransport())
    job = Job("tests.public.noop", version="v1")

    @jobs.handler(job)
    async def handler(context) -> None:
        await context.log("done")
        return None

    async with jobs:
        result = await jobs.client(job).run(None)

    assert result is None


@pytest.mark.asyncio
async def test_handlers_registered_after_start_become_ready_before_submit() -> None:
    jobs = SuperJobs(transport=InMemoryTransport())
    job = Job("tests.public.dynamic", version="v1", request=Request, result=Result)

    async with jobs:

        @jobs.handler(job)
        async def handler(request: Request, context) -> Result:
            return Result(value=request.value)

        result = await jobs.client(job).run(Request(value=9))

    assert result == Result(value=9)


@pytest.mark.asyncio
async def test_graceful_stop_drains_active_attempts() -> None:
    jobs = SuperJobs(transport=InMemoryTransport())
    job = Job("tests.public.drain", version="v1", request=Request, result=Result)
    started = asyncio.Event()
    release = asyncio.Event()

    @jobs.handler(job)
    async def handler(request: Request, context) -> Result:
        started.set()
        await release.wait()
        return Result(value=request.value)

    await jobs.start()
    handle = await jobs.client(job).submit(Request(value=3))
    await started.wait()
    stop_task = asyncio.create_task(jobs.stop(timeout=1))
    await asyncio.sleep(0.01)
    assert not stop_task.done()
    release.set()
    await stop_task

    assert await handle == Result(value=3)
