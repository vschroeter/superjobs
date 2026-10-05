"""Public regression checks from independent lifecycle review."""

import asyncio

import pytest

from superjobs import InMemoryTransport, Job, JobContext, JobIdentity, SuperJobs
from superjobs.transport.backend import WorkSubscription

pytestmark = pytest.mark.asyncio


class PartialStart(InMemoryTransport):
    closed = False

    async def start(self) -> None:
        await super().start()
        raise RuntimeError("partial start")

    async def stop(self) -> None:
        await super().stop()
        self.closed = True


class StopBarrier(InMemoryTransport):
    def __init__(self) -> None:
        super().__init__()
        self.entered = asyncio.Event()
        self.release = asyncio.Event()
        self.closed = False

    async def stop(self) -> None:
        self.entered.set()
        await self.release.wait()
        await super().stop()
        self.closed = True


async def test_partial_transport_start_is_rolled_back() -> None:
    transport = PartialStart()
    jobs = SuperJobs(transport=transport)
    with pytest.raises(RuntimeError, match="partial start"):
        await jobs.start()
    assert transport.closed


async def test_cancelled_stop_completes_cleanup_before_propagating() -> None:
    transport = StopBarrier()
    jobs = SuperJobs(transport=transport)
    await jobs.start()
    waiter = asyncio.create_task(jobs.wait_until_stopped())
    await asyncio.sleep(0)
    stopping = asyncio.create_task(jobs.stop())
    await asyncio.wait_for(transport.entered.wait(), 2)
    stopping.cancel()
    await asyncio.sleep(0)
    transport.release.set()
    with pytest.raises(asyncio.CancelledError):
        await asyncio.wait_for(stopping, 2)
    assert transport.closed
    await asyncio.wait_for(waiter, 2)


async def test_previous_generation_failure_survives_restart() -> None:
    class FailOnce(InMemoryTransport):
        fail = True

        async def stop(self) -> None:
            await super().stop()
            if self.fail:
                self.fail = False
                raise RuntimeError("old generation failure")

    jobs = SuperJobs(transport=FailOnce())
    await jobs.start()
    waiter = asyncio.create_task(jobs.wait_until_stopped())
    await asyncio.sleep(0)
    with pytest.raises(RuntimeError, match="old generation failure"):
        await jobs.stop()
    await jobs.start()
    try:
        with pytest.raises(RuntimeError, match="old generation failure"):
            await asyncio.wait_for(waiter, 2)
    finally:
        await jobs.stop()


async def test_serve_retains_ownership_until_it_returns() -> None:
    class HeldOwnerCleanup(SuperJobs):
        serving: asyncio.Task[None] | None = None

        def __init__(self) -> None:
            super().__init__()
            self.finalizing = asyncio.Event()
            self.release = asyncio.Event()

        async def stop(self, *, graceful: bool = True, timeout: float | None = None) -> None:
            if asyncio.current_task() is self.serving:
                self.finalizing.set()
                await self.release.wait()
            await super().stop(graceful=graceful, timeout=timeout)

    jobs = HeldOwnerCleanup()
    serving = asyncio.create_task(jobs.serve())
    jobs.serving = serving
    for _ in range(100):
        if jobs.started:
            break
        await asyncio.sleep(0)
    assert jobs.started
    await jobs.stop()
    await asyncio.wait_for(jobs.finalizing.wait(), 2)
    try:
        with pytest.raises(RuntimeError, match="owner"):
            await jobs.start()
    finally:
        jobs.release.set()
        await asyncio.wait_for(serving, 2)
        await jobs.stop()


async def test_failed_handler_start_cancels_unfinished_sibling_start() -> None:
    class PartialSubscriptions(InMemoryTransport):
        def __init__(self) -> None:
            super().__init__()
            self.slow_entered = asyncio.Event()
            self.release = asyncio.Event()
            self.slow_cancelled = False

        async def subscribe_work(self, identity: JobIdentity) -> WorkSubscription:
            if identity.name.endswith("slow"):
                self.slow_entered.set()
                try:
                    await self.release.wait()
                except asyncio.CancelledError:
                    self.slow_cancelled = True
                    raise
            else:
                await self.slow_entered.wait()
                raise RuntimeError("subscription start failed")
            return await super().subscribe_work(identity)

    transport = PartialSubscriptions()
    jobs = SuperJobs(transport=transport)
    slow = Job("tests.lifecycle.slow")
    failing = Job("tests.lifecycle.failing")

    async def handler(context: JobContext[None]) -> None:
        pass

    async def failing_handler(context: JobContext[None]) -> None:
        pass

    jobs.register(slow, handler)
    jobs.register(failing, failing_handler)
    try:
        with pytest.raises(RuntimeError, match="subscription start failed"):
            await asyncio.wait_for(jobs.start(), 2)
        assert transport.slow_cancelled
    finally:
        transport.release.set()
        await jobs.stop()


@pytest.mark.asyncio
async def test_provider_catalog_bindings_restart_independently() -> None:
    from contextlib import asynccontextmanager

    from superjobs import HandlerCatalog

    catalog = HandlerCatalog()
    job = Job("tests.lifecycle.provider.restart", result=int)
    generations: list[int] = []

    @asynccontextmanager
    async def provider():
        generation_id = len(generations)
        generations.append(generation_id)

        async def handler(context: JobContext[None]) -> int:
            return generation_id

        yield handler

    catalog.bind(job, provider=provider)
    jobs = SuperJobs(transport=InMemoryTransport(), handlers=catalog)
    await jobs.start()
    first = await jobs.client(job).submit()
    assert (await first.outcome()).result == 0
    await jobs.stop()
    await jobs.start()
    second = await jobs.client(job).submit()
    assert (await second.outcome()).result == 1
    await jobs.stop()
    assert generations == [0, 1]


async def test_startup_failure_preserves_handlers_for_retry() -> None:
    class FailFirstStart(InMemoryTransport):
        fail = True

        async def start(self) -> None:
            await super().start()
            if self.fail:
                self.fail = False
                raise RuntimeError("first startup failed")

    jobs = SuperJobs(transport=FailFirstStart())
    job = Job("tests.lifecycle.retry_start", result=int)

    @jobs.handler(job)
    async def handler(context: JobContext[None]) -> int:
        return 7

    with pytest.raises(RuntimeError, match="first startup failed"):
        await jobs.start()
    async with jobs:
        handle = await asyncio.wait_for(jobs.client(job).submit(), 2)
        outcome = await asyncio.wait_for(handle.outcome(), 2)
        assert outcome.result == 7


async def test_cancelled_subscription_start_never_reports_runtime_ready() -> None:
    from superjobs.jobs.job_identity import JobIdentity
    from superjobs.transport.backend import WorkSubscription

    class CancelledSubscription(InMemoryTransport):
        async def subscribe_work(self, identity: JobIdentity) -> WorkSubscription:
            raise asyncio.CancelledError

    transport = CancelledSubscription()
    jobs = SuperJobs(transport=transport)

    @jobs.handler(Job("tests.lifecycle.cancelled_subscription"))
    async def handler(context: JobContext[None]) -> None:
        pass

    with pytest.raises(asyncio.CancelledError):
        await jobs.start()
    assert not jobs.started
    assert not transport.started
