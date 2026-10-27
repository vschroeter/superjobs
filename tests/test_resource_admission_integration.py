from __future__ import annotations

import asyncio
from datetime import UTC, datetime, timedelta

import pytest

from superjobs import (
    HandlerCatalog,
    InMemoryTransport,
    JobContext,
    JobState,
    NoRequestJob,
    SuperJobs,
)
from superjobs.jobs.execution import JobCancelledOutcome, JobFailedOutcome, JobSucceeded
from superjobs.jobs.resource_cleanup import acknowledge_resource_cleanup
from superjobs.jobs.submit_options import SubmitOptions as SubmitOptionsType
from superjobs.resources.models import ResourceAvailability

RESOURCE_KEY = "device.gpu0"

job_a = NoRequestJob("tests.resource.job_a", version="v1", result=str)
job_b = NoRequestJob("tests.resource.job_b", version="v1", result=str)


@pytest.fixture
def shared_transport() -> InMemoryTransport:
    return InMemoryTransport()


async def _bounded_outcome(handle, *, seconds: float = 5.0):
    return await asyncio.wait_for(handle.outcome(), timeout=seconds)


async def _bounded_status(handle, *, seconds: float = 2.0):
    return await asyncio.wait_for(handle.status(), timeout=seconds)


@pytest.mark.asyncio
async def test_cross_job_mutual_exclusion_on_shared_resource(
    shared_transport: InMemoryTransport,
) -> None:
    active = 0
    peak = 0
    entered = asyncio.Event()
    release = asyncio.Event()

    catalog_a = HandlerCatalog()
    catalog_b = HandlerCatalog()

    @catalog_a.handler(job_a, resources={RESOURCE_KEY: 1})
    async def handle_a(context: JobContext[None]) -> str:
        nonlocal active, peak
        active += 1
        peak = max(peak, active)
        entered.set()
        await asyncio.wait_for(release.wait(), timeout=3.0)
        active -= 1
        acknowledge_resource_cleanup(context)
        return "a"

    @catalog_b.handler(job_b, resources={RESOURCE_KEY: 1})
    async def handle_b(context: JobContext[None]) -> str:
        nonlocal active, peak
        active += 1
        peak = max(peak, active)
        await asyncio.sleep(0.05)
        active -= 1
        acknowledge_resource_cleanup(context)
        return "b"

    runtime_a = SuperJobs(
        transport=shared_transport,
        handlers=catalog_a,
        resource_capacities={RESOURCE_KEY: 1},
        discovery=False,
    )
    runtime_b = SuperJobs(
        transport=shared_transport,
        handlers=catalog_b,
        discovery=False,
    )
    await runtime_a.start()
    await runtime_b.start()
    try:
        first = await runtime_a.client(job_a).submit()
        second = await runtime_b.client(job_b).submit()
        await asyncio.wait_for(entered.wait(), timeout=3.0)
        assert peak == 1
        release.set()
        outcome_a = await _bounded_outcome(first)
        outcome_b = await _bounded_outcome(second)
        assert isinstance(outcome_a, JobSucceeded)
        assert isinstance(outcome_b, JobSucceeded)
    finally:
        await runtime_a.stop(graceful=False)
        await runtime_b.stop(graceful=False)


@pytest.mark.asyncio
async def test_public_worker_incarnation_matches_resource_admission() -> None:
    entered = asyncio.Event()
    release = asyncio.Event()
    catalog = HandlerCatalog()

    @catalog.handler(job_a, resources={RESOURCE_KEY: 1})
    async def handle(context: JobContext[None]) -> str:
        entered.set()
        await release.wait()
        acknowledge_resource_cleanup(context)
        return "done"

    runtime = SuperJobs(
        transport=InMemoryTransport(),
        handlers=catalog,
        resource_capacities={RESOURCE_KEY: 1},
        discovery=False,
    )
    await runtime.start()
    try:
        incarnation = runtime.worker_incarnation
        handle = await runtime.client(job_a).submit()
        await asyncio.wait_for(entered.wait(), timeout=2)
        snapshot = await runtime.resources.snapshot()
        owner = next(
            record.owner
            for record in snapshot.resources
            if record.resource_key == RESOURCE_KEY and record.owner is not None
        )
        assert owner.incarnation == incarnation
        release.set()
        await _bounded_outcome(handle)
    finally:
        release.set()
        await runtime.stop(graceful=False)


@pytest.mark.asyncio
async def test_cleanup_failure_retains_owner_and_withdraws_readiness(
    shared_transport: InMemoryTransport,
) -> None:
    catalog = HandlerCatalog()

    @catalog.handler(job_a, resources={RESOURCE_KEY: 1})
    async def leaky(context: JobContext[None]) -> str:
        raise RuntimeError("Model cleanup failed")

    runtime = SuperJobs(
        transport=shared_transport,
        handlers=catalog,
        resource_capacities={RESOURCE_KEY: 1},
        discovery=False,
    )
    await runtime.start()
    try:
        handle = await runtime.client(job_a).submit()
        outcome = await _bounded_outcome(handle)
        assert isinstance(outcome, JobFailedOutcome)
        snapshot = await runtime.resources.snapshot()
        resource = next(r for r in snapshot.resources if r.resource_key == RESOURCE_KEY)
        assert resource.availability is ResourceAvailability.UNAVAILABLE
        assert snapshot.execution_claims
        assert runtime._handlers[job_a.canonical_name]._resource_admission_blocked
    finally:
        await runtime.stop(graceful=False)


@pytest.mark.asyncio
async def test_waiting_cancel_does_not_retry(
    shared_transport: InMemoryTransport,
) -> None:
    catalog = HandlerCatalog()
    hold_entered = asyncio.Event()
    hold_release = asyncio.Event()

    @catalog.handler(job_a, resources={RESOURCE_KEY: 1})
    async def hold(context: JobContext[None]) -> str:
        hold_entered.set()
        await asyncio.wait_for(hold_release.wait(), timeout=3.0)
        acknowledge_resource_cleanup(context)
        return "hold"

    @catalog.handler(job_b, resources={RESOURCE_KEY: 1})
    async def waiter(context: JobContext[None]) -> str:
        acknowledge_resource_cleanup(context)
        return "never"

    runtime = SuperJobs(
        transport=shared_transport,
        handlers=catalog,
        resource_capacities={RESOURCE_KEY: 1},
        discovery=False,
    )
    await runtime.start()
    try:
        hold_handle = await runtime.client(job_a).submit()
        await asyncio.wait_for(hold_entered.wait(), timeout=3.0)
        options = SubmitOptionsType(
            deadline=datetime.now(tz=UTC) + timedelta(seconds=2),
        )
        wait_handle = await runtime.client(job_b).submit(options=options)
        status = await _bounded_status(wait_handle)
        assert status.state in {JobState.WAITING_FOR_RESOURCE, JobState.PENDING}
        await wait_handle.cancel()
        outcome = await _bounded_outcome(wait_handle)
        assert isinstance(outcome, JobCancelledOutcome)
        assert shared_transport.retry_log == []
        hold_release.set()
        await _bounded_outcome(hold_handle)
    finally:
        await runtime.stop(graceful=False)


@pytest.mark.asyncio
async def test_acknowledged_cleanup_after_work(
    shared_transport: InMemoryTransport,
) -> None:
    catalog = HandlerCatalog()
    work_done = asyncio.Event()

    @catalog.handler(job_a, resources={RESOURCE_KEY: 1})
    async def work(context: JobContext[None]) -> str:
        await asyncio.sleep(0.05)
        work_done.set()
        acknowledge_resource_cleanup(context)
        return "ok"

    runtime = SuperJobs(
        transport=shared_transport,
        handlers=catalog,
        resource_capacities={RESOURCE_KEY: 1},
        discovery=False,
    )
    await runtime.start()
    try:
        handle = await runtime.client(job_a).submit()
        await asyncio.wait_for(work_done.wait(), timeout=3.0)
        outcome = await _bounded_outcome(handle)
        assert isinstance(outcome, JobSucceeded)
        snapshot = await runtime.resources.snapshot()
        resource = next(r for r in snapshot.resources if r.resource_key == RESOURCE_KEY)
        assert resource.availability is ResourceAvailability.AVAILABLE
        assert not snapshot.execution_claims
    finally:
        await runtime.stop(graceful=False)
