from __future__ import annotations

import asyncio
from dataclasses import dataclass
from unittest.mock import patch

import pytest

from superjobs import InMemoryTransport, Job, JobContext, SuperJobs
from superjobs.jobs.job_handler import JobHandler


@dataclass
class Request:
    value: int


@dataclass
class Result:
    value: str


class BarrierStartTransport(InMemoryTransport):
    def __init__(self) -> None:
        super().__init__()
        self.start_barrier = asyncio.Event()

    async def start(self) -> None:
        await super().start()
        self.start_barrier.set()


class SlowStopTransport(InMemoryTransport):
    def __init__(self) -> None:
        super().__init__()
        self.stop_entered = asyncio.Event()
        self.stop_finished = asyncio.Event()
        self._release_stop = asyncio.Event()

    def release_stop(self) -> None:
        self._release_stop.set()

    async def stop(self) -> None:
        self.stop_entered.set()
        await self._release_stop.wait()
        await super().stop()
        self.stop_finished.set()


@pytest.mark.asyncio
async def test_serve_starts_waits_and_stops() -> None:
    jobs = SuperJobs(transport=InMemoryTransport())
    job = Job("tests.lifecycle.serve", version="v1", request=Request, result=Result)

    @jobs.handler(job)
    async def handler(request: Request, context: JobContext[None]) -> Result:
        return Result(value=str(request.value))

    serve_task = asyncio.create_task(jobs.serve())
    for _ in range(100):
        if jobs.started:
            break
        await asyncio.sleep(0)
    assert jobs.started
    await jobs.stop()
    await asyncio.wait_for(serve_task, timeout=2)
    assert not jobs.started


@pytest.mark.asyncio
async def test_serve_rejects_already_started_runtime() -> None:
    jobs = SuperJobs(transport=InMemoryTransport())
    await jobs.start()
    try:
        with pytest.raises(RuntimeError, match="already started"):
            await jobs.serve()
    finally:
        await jobs.stop()


@pytest.mark.asyncio
async def test_serve_rejects_concurrent_lifecycle_owner() -> None:
    transport = BarrierStartTransport()
    jobs = SuperJobs(transport=transport)
    active = asyncio.create_task(jobs.serve())
    await asyncio.wait_for(transport.start_barrier.wait(), timeout=2)
    assert jobs.started
    with pytest.raises(RuntimeError, match="lifecycle owner"):
        await jobs.serve()
    await jobs.stop()
    await asyncio.wait_for(active, timeout=2)


@pytest.mark.asyncio
async def test_wait_until_stopped_requires_started_runtime() -> None:
    jobs = SuperJobs(transport=InMemoryTransport())
    with pytest.raises(RuntimeError, match="not started"):
        await jobs.wait_until_stopped()


@pytest.mark.asyncio
async def test_wait_until_stopped_completes_after_transport_shutdown() -> None:
    transport = SlowStopTransport()
    jobs = SuperJobs(transport=transport)
    job = Job("tests.lifecycle.wait-drain", version="v1", request=None, result=None)

    @jobs.handler(job)
    async def handler(context: JobContext[None]) -> None:
        return None

    await jobs.start()
    waiter = asyncio.create_task(jobs.wait_until_stopped())
    await asyncio.sleep(0)
    stop_task = asyncio.create_task(jobs.stop())
    await transport.stop_entered.wait()
    assert not jobs.started
    assert not waiter.done()
    transport.release_stop()
    await stop_task
    await transport.stop_finished.wait()
    await asyncio.wait_for(waiter, timeout=2)


@pytest.mark.asyncio
async def test_multiple_waiters_release_together() -> None:
    jobs = SuperJobs(transport=InMemoryTransport())
    await jobs.start()
    first = asyncio.create_task(jobs.wait_until_stopped())
    second = asyncio.create_task(jobs.wait_until_stopped())
    await asyncio.sleep(0)
    await jobs.stop()
    await asyncio.wait_for(asyncio.gather(first, second), timeout=2)


@pytest.mark.asyncio
async def test_cancelling_one_waiter_does_not_stop_runtime_or_other_waiters() -> None:
    jobs = SuperJobs(transport=InMemoryTransport())
    await jobs.start()
    cancelled = asyncio.create_task(jobs.wait_until_stopped())
    remaining = asyncio.create_task(jobs.wait_until_stopped())
    await asyncio.sleep(0)
    cancelled.cancel()
    with pytest.raises(asyncio.CancelledError):
        await cancelled
    assert jobs.started
    assert not remaining.done()
    await jobs.stop()
    await asyncio.wait_for(remaining, timeout=2)


@pytest.mark.asyncio
async def test_embedded_context_wait_until_stopped_pattern() -> None:
    jobs = SuperJobs(transport=InMemoryTransport())
    job = Job("tests.lifecycle.embedded", version="v1", request=Request, result=Result)
    ready = asyncio.Event()

    @jobs.handler(job)
    async def handler(request: Request, context: JobContext[None]) -> Result:
        return Result(value=str(request.value))

    async def worker() -> None:
        async with jobs:
            ready.set()
            await jobs.wait_until_stopped()

    worker_task = asyncio.create_task(worker())
    await ready.wait()
    assert jobs.started
    assert await jobs.client(job).run(Request(value=3)) == Result(value="3")
    await jobs.stop()
    await asyncio.wait_for(worker_task, timeout=2)


@pytest.mark.asyncio
async def test_restart_opens_new_generation_for_waiters() -> None:
    jobs = SuperJobs(transport=InMemoryTransport())
    await jobs.start()
    first_generation = jobs._active_lifecycle_generation
    await jobs.stop()
    await jobs.start()
    assert jobs._active_lifecycle_generation == first_generation + 1
    waiter = asyncio.create_task(jobs.wait_until_stopped())
    await asyncio.sleep(0)
    await jobs.stop()
    await asyncio.wait_for(waiter, timeout=2)


@pytest.mark.asyncio
async def test_shutdown_failure_releases_waiters() -> None:
    jobs = SuperJobs(transport=InMemoryTransport())
    job = Job("tests.lifecycle.shutdown-failure", version="v1", request=None, result=None)

    @jobs.handler(job)
    async def handler(context: JobContext[None]) -> None:
        return None

    await jobs.start()
    waiter = asyncio.create_task(jobs.wait_until_stopped())
    await asyncio.sleep(0)
    with patch.object(
        JobHandler,
        "stop",
        side_effect=RuntimeError("handler cleanup failed"),
    ):
        with pytest.raises(RuntimeError, match="handler cleanup failed"):
            await jobs.stop()
    with pytest.raises(RuntimeError, match="handler cleanup failed"):
        await waiter


@pytest.mark.asyncio
async def test_stop_raises_shutdown_failure_to_direct_caller() -> None:
    jobs = SuperJobs(transport=InMemoryTransport())
    job = Job("tests.lifecycle.stop-failure", version="v1", request=None, result=None)

    @jobs.handler(job)
    async def handler(context: JobContext[None]) -> None:
        return None

    await jobs.start()
    with patch.object(
        JobHandler,
        "stop",
        side_effect=RuntimeError("handler cleanup failed"),
    ):
        with pytest.raises(RuntimeError, match="handler cleanup failed"):
            await jobs.stop()


@pytest.mark.asyncio
async def test_context_manager_surfaces_shutdown_failure() -> None:
    jobs = SuperJobs(transport=InMemoryTransport())
    job = Job("tests.lifecycle.context-failure", version="v1", request=None, result=None)

    @jobs.handler(job)
    async def handler(context: JobContext[None]) -> None:
        return None

    with patch.object(
        JobHandler,
        "stop",
        side_effect=RuntimeError("handler cleanup failed"),
    ):
        with pytest.raises(RuntimeError, match="handler cleanup failed"):
            async with jobs:
                pass


@pytest.mark.asyncio
async def test_registration_rejected_while_runtime_is_shutting_down() -> None:
    transport = SlowStopTransport()
    jobs = SuperJobs(transport=transport)
    job = Job("tests.lifecycle.register-drain", version="v1", request=None, result=None)

    @jobs.handler(job)
    async def handler(context: JobContext[None]) -> None:
        return None

    await jobs.start()
    stop_task = asyncio.create_task(jobs.stop())
    await transport.stop_entered.wait()
    late = Job("tests.lifecycle.register-late", version="v1", request=None, result=None)
    with pytest.raises(RuntimeError, match="shutting down"):
        jobs.register(late, handler)
    transport.release_stop()
    await stop_task


@pytest.mark.asyncio
async def test_serve_propagates_cancellation_after_cleanup() -> None:
    jobs = SuperJobs(transport=InMemoryTransport())
    job = Job("tests.lifecycle.serve-cancel", version="v1", request=None, result=None)

    @jobs.handler(job)
    async def handler(context: JobContext[None]) -> None:
        await asyncio.Event().wait()

    serve_task = asyncio.create_task(jobs.serve())
    for _ in range(100):
        if jobs.started:
            break
        await asyncio.sleep(0)
    serve_task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await serve_task
    assert not jobs.started
