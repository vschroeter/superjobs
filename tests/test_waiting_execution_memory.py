from __future__ import annotations

import asyncio

import pytest

from superjobs import HandlerCatalog, InMemoryTransport, JobState, NoRequestJob, SuperJobs
from superjobs.resources import (
    AdmissionSnapshot,
    UnsupportedWaitingExecutionBackendError,
    WaitingExecutionCompleteness,
    read_waiting_executions,
    supports_waiting_execution_observation,
)
from superjobs.resources.errors import WaitingExecutionUnavailableError
from superjobs.resources.waiting_execution import build_waiting_execution_view
from superjobs.transport.backend import ExecutionRecord
from superjobs.transport.in_memory import InMemoryTransport as InMemoryTransportType

RESOURCE_KEY = "device.gpu0"


def _admission_broker_state(snapshot: AdmissionSnapshot):
    """Compare durable admission contents, not per-read ``evaluated_at`` timestamps."""
    return (snapshot.capacities, snapshot.resources, snapshot.execution_claims)


job_a = NoRequestJob("tests.waiting.job_a", version="v1", result=str)
job_b = NoRequestJob("tests.waiting.job_b", version="v1", result=str)


@pytest.mark.asyncio
async def test_waiting_view_lists_authoritative_execution_state() -> None:
    entered = asyncio.Event()
    release = asyncio.Event()

    catalog_a = HandlerCatalog()
    catalog_b = HandlerCatalog()

    @catalog_a.handler(job_a, resources={RESOURCE_KEY: 1})
    async def handle_a(context) -> str:
        entered.set()
        await asyncio.wait_for(release.wait(), timeout=3.0)
        context.acknowledge_resource_cleanup()
        return "a"

    @catalog_b.handler(job_b, resources={RESOURCE_KEY: 1})
    async def handle_b(context) -> str:
        await asyncio.sleep(0.05)
        context.acknowledge_resource_cleanup()
        return "b"

    transport = InMemoryTransport()
    runtime_a = SuperJobs(
        transport=transport,
        handlers=catalog_a,
        resource_capacities={RESOURCE_KEY: 1},
        discovery=False,
    )
    runtime_b = SuperJobs(
        transport=transport,
        handlers=catalog_b,
        resource_capacities={RESOURCE_KEY: 1},
        discovery=False,
    )
    await runtime_a.start()
    await runtime_b.start()
    try:
        first = await runtime_a.client(job_a).submit()
        second = await runtime_b.client(job_b).submit()
        await asyncio.wait_for(entered.wait(), timeout=3.0)

        view = None
        for _ in range(60):
            view = await runtime_a.resources.waiting_executions()
            if view.entries:
                break
            await asyncio.sleep(0.05)
        assert view is not None

        before = await runtime_a.resources.snapshot()
        after_view = await runtime_a.resources.waiting_executions()
        after = await runtime_a.resources.snapshot()

        assert _admission_broker_state(before) == _admission_broker_state(after)
        assert after_view.completeness is WaitingExecutionCompleteness.COMPLETE
        assert {entry.execution_id for entry in after_view.entries} == {second.id}
        record = await transport.get_execution(job_b.identity, second.id)
        assert record is not None
        assert record.state is JobState.WAITING_FOR_RESOURCE

        release.set()
        await asyncio.wait_for(first.result(), timeout=3.0)
        await asyncio.wait_for(second.result(), timeout=3.0)
        cleared = await runtime_a.resources.waiting_executions()
        assert cleared.entries == ()
    finally:
        await runtime_a.stop(graceful=False)
        await runtime_b.stop(graceful=False)


@pytest.mark.asyncio
async def test_admission_snapshot_is_not_used_as_waiting_authority() -> None:
    transport = InMemoryTransport()
    assert supports_waiting_execution_observation(transport)

    class NoObservationStore:
        resource_admission_store = transport.resource_admission_store

    with pytest.raises(UnsupportedWaitingExecutionBackendError):
        await read_waiting_executions(NoObservationStore())


def test_build_view_truncates_and_marks_completeness() -> None:
    from datetime import UTC, datetime

    from superjobs.jobs.job_identity import JobIdentity

    evaluated_at = datetime.now(tz=UTC)
    records = [
        ExecutionRecord(
            identity=JobIdentity(name=f"tests.waiting.{index}", version="v1"),
            job_id=f"exec-{index}",
            request_payload=b"",
            request_media_type=None,
            fingerprint="fp",
            idempotency_key=f"key-{index}",
            caller_scope="default",
            timeout=None,
            deadline=None,
            created_at=evaluated_at,
            state=JobState.WAITING_FOR_RESOURCE,
        )
        for index in range(3)
    ]
    view = build_waiting_execution_view(
        evaluated_at=evaluated_at,
        records=records,
        max_entries=2,
        partial=False,
    )
    assert view.completeness is WaitingExecutionCompleteness.TRUNCATED
    assert len(view.entries) == 2


def test_build_view_marks_scan_truncated() -> None:
    from datetime import UTC, datetime

    from superjobs.jobs.job_identity import JobIdentity

    evaluated_at = datetime.now(tz=UTC)
    records = [
        ExecutionRecord(
            identity=JobIdentity(name="tests.waiting.one", version="v1"),
            job_id="exec-1",
            request_payload=b"",
            request_media_type=None,
            fingerprint="fp",
            idempotency_key="key-1",
            caller_scope="default",
            timeout=None,
            deadline=None,
            created_at=evaluated_at,
            state=JobState.WAITING_FOR_RESOURCE,
        )
    ]
    view = build_waiting_execution_view(
        evaluated_at=evaluated_at,
        records=records,
        max_entries=10,
        partial=False,
        scan_truncated=True,
    )
    assert view.completeness is WaitingExecutionCompleteness.TRUNCATED


@pytest.mark.asyncio
async def test_waiting_view_unavailable_before_start() -> None:
    transport = InMemoryTransport()
    with pytest.raises(WaitingExecutionUnavailableError, match="started transport"):
        await read_waiting_executions(transport)


@pytest.mark.asyncio
async def test_in_memory_bounded_scan_truncates() -> None:
    transport = InMemoryTransport()
    transport.max_waiting_execution_scan_keys = 1
    await transport.start()
    try:
        from datetime import UTC, datetime

        from superjobs.jobs.job_identity import JobIdentity

        evaluated_at = datetime.now(tz=UTC)
        for index in range(2):
            transport._executions[f"exec-{index}"] = ExecutionRecord(
                identity=JobIdentity(name=f"tests.waiting.{index}", version="v1"),
                job_id=f"exec-{index}",
                request_payload=b"",
                request_media_type=None,
                fingerprint="fp",
                idempotency_key=f"key-{index}",
                caller_scope="default",
                timeout=None,
                deadline=None,
                created_at=evaluated_at,
                state=JobState.WAITING_FOR_RESOURCE,
            )
        view = await read_waiting_executions(transport, max_entries=10)
        assert view.completeness is WaitingExecutionCompleteness.TRUNCATED
        assert len(view.entries) == 1
    finally:
        await transport.stop()


def test_in_memory_transport_exposes_observation_seam() -> None:
    backend = InMemoryTransportType()
    assert supports_waiting_execution_observation(backend)


def test_partial_precedes_truncation_when_both_occur() -> None:
    from datetime import UTC, datetime

    view = build_waiting_execution_view(
        evaluated_at=datetime.now(UTC), records=[], max_entries=1,
        partial=True, scan_truncated=True,
    )
    assert view.completeness is WaitingExecutionCompleteness.PARTIAL


@pytest.mark.asyncio
async def test_waiting_observation_timeout_is_unavailable() -> None:
    from datetime import timedelta

    from superjobs.resources import WaitingExecutionUnavailableError

    class StalledBackend:
        resource_admission_store = object()
        waiting_execution_read_timeout = timedelta(milliseconds=10)

        async def read_waiting_execution_view(self, **kwargs):
            await asyncio.Event().wait()

    with pytest.raises(WaitingExecutionUnavailableError, match="timed out"):
        await read_waiting_executions(StalledBackend())
