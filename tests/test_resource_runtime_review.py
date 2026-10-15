"""Independent public-runtime admission acceptance regressions."""

import asyncio

from superjobs import (
    HandlerCatalog,
    InMemoryTransport,
    Job,
    JobContext,
    RetryPolicy,
    SuperJobs,
)
from superjobs.jobs.retry_policy import FixedBackoff
from superjobs import JobState, SubmitOptions
from superjobs.jobs.execution import JobFailedOutcome, JobCancelledOutcome
import threading
import pytest


async def test_success_after_handler_finally_implies_cleanup_acknowledgement() -> None:
    job = Job("review.resource.normal", result=str)
    handlers = HandlerCatalog()
    cleaned = False

    @handlers.handler(job, resources={"review.normal": 1})
    async def run(context: JobContext[None]) -> str:
        nonlocal cleaned
        try:
            return "accepted"
        finally:
            cleaned = True

    runtime = SuperJobs(handlers=handlers, resource_capacities={"review.normal": 1})
    await runtime.start()
    try:
        handle = await runtime.client(job).submit()
        assert await asyncio.wait_for(handle.result(), 2) == "accepted"
        assert cleaned
        snapshot = await runtime.resources.snapshot()
        assert all(record.owner is None for record in snapshot.resources)
    finally:
        await asyncio.wait_for(runtime.stop(graceful=False), 2)


async def test_completion_outage_keeps_the_accepted_result_until_reconciled() -> None:
    class CompletionOutage(InMemoryTransport):
        failures = 8

        async def write_completion(self, *args, **kwargs):
            if self.failures:
                self.failures -= 1
                raise ConnectionError("temporary completion-store outage")
            return await super().write_completion(*args, **kwargs)

    backend = CompletionOutage()
    job = Job("review.resource.persistence", result=str)
    handlers = HandlerCatalog()
    calls = 0

    @handlers.handler(job, resources={"review.persistence": 1})
    async def run(context: JobContext[None]) -> str:
        nonlocal calls
        calls += 1
        context.acknowledge_resource_cleanup()
        return "accepted"

    runtime = SuperJobs(
        transport=backend,
        handlers=handlers,
        resource_capacities={"review.persistence": 1},
    )
    await runtime.start()
    try:
        handle = await runtime.client(job).submit()
        assert await asyncio.wait_for(handle.result(), 3) == "accepted"
        assert calls == 1
        assert backend.failures == 0
    finally:
        await asyncio.wait_for(runtime.stop(graceful=False), 2)


async def test_acknowledged_inference_failure_retries_without_retaining_old_claim() -> (
    None
):
    backend = InMemoryTransport()
    job = Job("review.resource.retry", result=str)
    handlers = HandlerCatalog()
    calls = 0

    @handlers.handler(
        job,
        resources={"review.retry": 1},
        retry=RetryPolicy(max_attempts=2, backoff=FixedBackoff(0)),
    )
    async def run(context: JobContext[None]) -> str:
        nonlocal calls
        calls += 1
        try:
            if calls == 1:
                raise ValueError("inference failed before cleanup")
            return "second attempt"
        finally:
            context.acknowledge_resource_cleanup()

    runtime = SuperJobs(
        transport=backend, handlers=handlers, resource_capacities={"review.retry": 1}
    )
    await runtime.start()
    try:
        handle = await runtime.client(job).submit()
        assert await asyncio.wait_for(handle.result(), 3) == "second attempt"
        assert calls == 2
    finally:
        await asyncio.wait_for(runtime.stop(graceful=False), 2)


async def _until(predicate) -> None:
    async with asyncio.timeout(2):
        while not predicate():
            await asyncio.sleep(0)


async def test_native_thread_timeout_retains_ownership_until_actual_cleanup() -> None:
    backend = InMemoryTransport()
    handlers = HandlerCatalog()
    job = Job("review.resource.native", result=str)
    release = threading.Event()
    entered = asyncio.Event()
    contexts = []
    loop = asyncio.get_running_loop()

    @handlers.handler(job, resources={"review.native": 1})
    def native(context: JobContext[None]) -> str:
        contexts.append(context)
        loop.call_soon_threadsafe(entered.set)
        try:
            assert release.wait(3), "native call was not released by the test"
            return "late result"
        finally:
            context.acknowledge_resource_cleanup()

    runtime = SuperJobs(
        transport=backend, handlers=handlers, resource_capacities={"review.native": 1}
    )
    await runtime.start()
    try:
        handle = await runtime.client(job).submit(options=SubmitOptions(timeout=0.02))
        await asyncio.wait_for(entered.wait(), 2)
        await _until(lambda: contexts[0].cancelled)
        snapshot = await runtime.resources.snapshot()
        assert snapshot.resources[0].owner is not None
        assert (await handle.status()).state is JobState.RUNNING
        release.set()
        outcome = await asyncio.wait_for(handle.outcome(), 2)
        assert (
            isinstance(outcome, JobFailedOutcome) and outcome.error.code == "timed_out"
        )
        assert (await runtime.resources.snapshot()).resources[0].owner is None
    finally:
        release.set()
        await asyncio.wait_for(runtime.stop(graceful=False), 2)


async def test_duplicate_cancellation_cannot_finalize_live_owner() -> None:
    backend = InMemoryTransport()
    handlers = HandlerCatalog()
    job = Job("review.resource.duplicate", result=str)
    entered = asyncio.Event()
    release = asyncio.Event()
    calls = 0
    contexts = []

    @handlers.handler(job, resources={"review.duplicate": 1})
    async def run(context: JobContext[None]) -> str:
        nonlocal calls
        calls += 1
        contexts.append(context)
        entered.set()
        try:
            await release.wait()
            await context.check_cancelled()
            return "ok"
        finally:
            context.acknowledge_resource_cleanup()

    first = SuperJobs(
        transport=backend,
        handlers=handlers,
        resource_capacities={"review.duplicate": 1},
    )
    second = SuperJobs(transport=backend, handlers=handlers)
    await first.start()
    try:
        handle = await first.client(job).submit()
        await asyncio.wait_for(entered.wait(), 2)
        await second.start()
        record = await backend.get_execution(job.identity, handle.id)
        assert record is not None
        await backend.publish(record, attempt=1)
        await _until(lambda: bool(second._handlers[job.canonical_name]._active_tasks))
        await handle.cancel()
        await _until(lambda: contexts[0].cancelled)
        assert (await handle.status()).state is JobState.RUNNING
        assert calls == 1
        assert (await first.resources.snapshot()).resources[0].owner is not None
        release.set()
        assert isinstance(
            await asyncio.wait_for(handle.outcome(), 2), JobCancelledOutcome
        )
        assert calls == 1
        assert backend.retry_log == []
    finally:
        release.set()
        await asyncio.wait_for(first.stop(graceful=False), 2)
        await asyncio.wait_for(second.stop(graceful=False), 2)


async def test_applied_completion_with_lost_acknowledgement_does_not_rerun() -> None:
    class LostAck(InMemoryTransport):
        lost = False

        async def write_completion(self, *args, **kwargs):
            result = await super().write_completion(*args, **kwargs)
            if not self.lost:
                self.lost = True
                raise ConnectionError("completion was stored, acknowledgement lost")
            return result

    backend = LostAck()
    job = Job("review.resource.lostack", result=str)
    handlers = HandlerCatalog()
    calls = 0

    @handlers.handler(job, resources={"review.lostack": 1})
    async def run(context: JobContext[None]) -> str:
        nonlocal calls
        calls += 1
        return "accepted"

    runtime = SuperJobs(
        transport=backend, handlers=handlers, resource_capacities={"review.lostack": 1}
    )
    await runtime.start()
    try:
        handle = await runtime.client(job).submit()
        assert await asyncio.wait_for(handle.result(), 2) == "accepted"
        await _until(lambda: not runtime._handlers[job.canonical_name]._active_tasks)
        assert not (await runtime.resources.snapshot()).execution_claims
        assert calls == 1
    finally:
        await asyncio.wait_for(runtime.stop(graceful=False), 2)


async def test_repeated_shutdown_cancellation_does_not_release_live_native_work() -> None:
    job = Job("review.resource.shutdown", result=str)
    handlers = HandlerCatalog()
    entered = asyncio.Event()
    release = threading.Event()
    contexts = []
    loop = asyncio.get_running_loop()

    @handlers.handler(job, resources={"review.shutdown": 1})
    def native(context: JobContext[None]) -> str:
        contexts.append(context)
        loop.call_soon_threadsafe(entered.set)
        try:
            assert release.wait(3)
            return "finished native call"
        finally:
            context.acknowledge_resource_cleanup()

    runtime = SuperJobs(handlers=handlers, resource_capacities={"review.shutdown": 1})
    await runtime.start()
    try:
        await runtime.client(job).submit()
        await asyncio.wait_for(entered.wait(), 2)
        stopping = asyncio.create_task(runtime.stop(graceful=False))
        await _until(lambda: contexts[0].cancelled)
        stopping.cancel()
        await asyncio.sleep(0)
        stopping.cancel()
        assert (await runtime.resources.snapshot()).resources[0].owner is not None
        release.set()
        with pytest.raises(asyncio.CancelledError):
            await asyncio.wait_for(stopping, 2)
        assert (await runtime.resources.snapshot()).resources[0].owner is None
    finally:
        release.set()
        await asyncio.wait_for(runtime.stop(graceful=False), 2)


async def test_deadline_while_waiting_never_enters_or_releases_the_other_owner() -> None:
    from datetime import UTC, datetime, timedelta

    backend = InMemoryTransport()
    first_job = Job("review.resource.deadline.owner", result=str)
    second_job = Job("review.resource.deadline.waiter", result=str)
    handlers = HandlerCatalog()
    entered = asyncio.Event()
    release = asyncio.Event()
    waiter_calls = 0

    @handlers.handler(first_job, resources={"review.deadline": 1})
    async def hold(context: JobContext[None]) -> str:
        entered.set()
        await release.wait()
        return "owner"

    @handlers.handler(second_job, resources={"review.deadline": 1})
    async def wait(context: JobContext[None]) -> str:
        nonlocal waiter_calls
        waiter_calls += 1
        return "unexpected"

    runtime = SuperJobs(transport=backend, handlers=handlers, resource_capacities={"review.deadline": 1})
    await runtime.start()
    try:
        owner = await runtime.client(first_job).submit()
        await asyncio.wait_for(entered.wait(), 2)
        before = (await runtime.resources.snapshot()).resources[0].owner
        handle = await runtime.client(second_job).submit(options=SubmitOptions(deadline=datetime.now(UTC) + timedelta(seconds=0.1)))
        outcome = await asyncio.wait_for(handle.outcome(), 2)
        assert isinstance(outcome, JobFailedOutcome) and outcome.error.code == "deadline_exceeded"
        assert waiter_calls == 0 and backend.retry_log == []
        assert (await runtime.resources.snapshot()).resources[0].owner == before
        release.set()
        assert await asyncio.wait_for(owner.result(), 2) == "owner"
    finally:
        release.set()
        await asyncio.wait_for(runtime.stop(graceful=False), 2)


async def test_cleanup_failure_readiness_stays_withdrawn_after_renewal() -> None:
    from superjobs import WorkerRegistrationState

    job = Job("review.resource.withdrawn", result=str)
    handlers = HandlerCatalog()

    @handlers.handler(job, resources={"review.withdrawn": 1})
    async def run(context: JobContext[None]) -> str:
        raise RuntimeError("native resource cleanup failed")

    runtime = SuperJobs(handlers=handlers, resource_capacities={"review.withdrawn": 1})
    await runtime.start()
    try:
        assert any(worker.state is WorkerRegistrationState.READY for worker in await runtime.client(job).workers())
        handle = await runtime.client(job).submit()
        assert isinstance(await asyncio.wait_for(handle.outcome(), 2), JobFailedOutcome)
        entry = runtime._presence._entries[job.canonical_name]
        await runtime._presence._renew_entry(entry)
        assert all(worker.state is not WorkerRegistrationState.READY for worker in await runtime.client(job).workers(include_stale=True))
        assert (await runtime.resources.snapshot()).resources[0].availability.value == "unavailable"
    finally:
        await asyncio.wait_for(runtime.stop(graceful=False), 2)


async def test_uncertain_submission_reuses_execution_without_abandoning_live_owner() -> None:
    class LostSubmissionAck(InMemoryTransport):
        lost = False

        async def submit(self, *args, **kwargs):
            record = await super().submit(*args, **kwargs)
            if not self.lost:
                self.lost = True
                raise ConnectionError("accepted submission acknowledgement was lost")
            return record

    backend = LostSubmissionAck()
    job = Job("review.resource.submission", result=str)
    handlers = HandlerCatalog()
    entered = asyncio.Event()
    release = asyncio.Event()
    calls = 0

    @handlers.handler(job, resources={"review.submission": 1})
    async def run(context: JobContext[None]) -> str:
        nonlocal calls
        calls += 1
        entered.set()
        await release.wait()
        return "accepted once"

    runtime = SuperJobs(transport=backend, handlers=handlers, resource_capacities={"review.submission": 1})
    await runtime.start()
    try:
        options = SubmitOptions(job_id="uncertain-resource-execution", idempotency_key="same-submission")
        with pytest.raises(ConnectionError):
            await runtime.client(job).submit(options=options)
        await asyncio.wait_for(entered.wait(), 2)
        owner = (await runtime.resources.snapshot()).resources[0].owner
        handle = await runtime.client(job).submit(options=options)
        assert handle.id == options.job_id and calls == 1
        assert (await runtime.resources.snapshot()).resources[0].owner == owner
        release.set()
        assert await asyncio.wait_for(handle.result(), 2) == "accepted once"
    finally:
        release.set()
        await asyncio.wait_for(runtime.stop(graceful=False), 2)
