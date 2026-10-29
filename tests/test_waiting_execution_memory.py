from __future__ import annotations

import asyncio
import time

import pytest

from superjobs import HandlerCatalog, InMemoryTransport, JobState, NoRequestJob, SuperJobs
from superjobs.resources import (
    AdmissionSnapshot,
    UnsupportedWaitingExecutionBackendError,
    WaitingExecutionCompleteness,
    WaitingExecutionEvidence,
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
        assert after_view.completeness is WaitingExecutionCompleteness.PARTIAL
        assert {entry.execution_id for entry in after_view.entries} == {second.id}
        assert after_view.entries[0].resource_key == RESOURCE_KEY
        assert after_view.entries[0].record_state is JobState.WAITING_FOR_RESOURCE
        assert after_view.entries[0].evidence is WaitingExecutionEvidence.PERSISTED_HISTORICAL
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
    from uuid import uuid4

    from superjobs.jobs.job_identity import JobIdentity

    evaluated_at = datetime.now(tz=UTC)
    pin = uuid4()
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
            attempt=1,
            waiting_resource_key=RESOURCE_KEY,
            waiting_resource_invocation_id=pin,
            waiting_resource_attempt=1,
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
    from uuid import uuid4

    from superjobs.jobs.job_identity import JobIdentity

    evaluated_at = datetime.now(tz=UTC)
    pin = uuid4()
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
            attempt=1,
            waiting_resource_key=RESOURCE_KEY,
            waiting_resource_invocation_id=pin,
            waiting_resource_attempt=1,
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
        from uuid import uuid4

        from superjobs.jobs.job_identity import JobIdentity

        evaluated_at = datetime.now(tz=UTC)
        pin = uuid4()
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
                attempt=1,
                waiting_resource_key=RESOURCE_KEY,
                waiting_resource_invocation_id=pin,
                waiting_resource_attempt=1,
            )
        view = await read_waiting_executions(transport, max_entries=10)
        assert view.completeness is WaitingExecutionCompleteness.PARTIAL
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
async def test_missing_completion_bucket_is_unavailable() -> None:
    from datetime import UTC, datetime
    from unittest.mock import MagicMock

    from superjobs.discovery.nats import NatsDiscoveryBackend
    from superjobs.resources.errors import WaitingExecutionUnavailableError
    from superjobs.resources.waiting_execution import scan_completion_kv_for_waiting_view

    with pytest.raises(WaitingExecutionUnavailableError, match="missing"):
        await scan_completion_kv_for_waiting_view(
            completion_kv=None,
            discovery_backend=MagicMock(spec=NatsDiscoveryBackend),
            evaluated_at=datetime.now(tz=UTC),
            max_entries=10,
            max_scan_keys=100,
            scan_deadline_monotonic=time.monotonic() + 5.0,
            record_from_wire=lambda value: value,
            unpack=lambda value: value,
        )


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


@pytest.mark.asyncio
async def test_waiting_view_evaluated_at_is_scan_start_not_end() -> None:
    from datetime import UTC, datetime, timedelta

    scan_delay = timedelta(milliseconds=80)
    scan_completed_at: datetime | None = None

    class SlowScanBackend:
        resource_admission_store = object()

        async def read_waiting_execution_view(self, *, evaluated_at, **kwargs):
            nonlocal scan_completed_at
            await asyncio.sleep(scan_delay.total_seconds())
            scan_completed_at = datetime.now(tz=UTC)
            return build_waiting_execution_view(
                evaluated_at=evaluated_at,
                records=[],
                max_entries=kwargs["max_entries"],
                partial=False,
            )

    view = await read_waiting_executions(SlowScanBackend())
    assert scan_completed_at is not None
    assert view.evaluated_at < scan_completed_at - scan_delay / 2


@pytest.mark.asyncio
async def test_pending_with_resource_key_marks_partial_without_entry() -> None:
    from datetime import UTC, datetime

    from superjobs.jobs.job_identity import JobIdentity

    transport = InMemoryTransport()
    await transport.start()
    try:
        evaluated_at = datetime.now(tz=UTC)
        transport._executions["exec-pending"] = ExecutionRecord(
            identity=JobIdentity(name="tests.waiting.pending", version="v1"),
            job_id="exec-pending",
            request_payload=b"",
            request_media_type=None,
            fingerprint="fp",
            idempotency_key="key-pending",
            caller_scope="default",
            timeout=None,
            deadline=None,
            created_at=evaluated_at,
            state=JobState.PENDING,
            waiting_resource_key=RESOURCE_KEY,
        )
        view = await read_waiting_executions(transport)
        assert view.completeness is WaitingExecutionCompleteness.PARTIAL
        assert view.entries == ()
    finally:
        await transport.stop()


@pytest.mark.asyncio
async def test_pending_without_resource_key_marks_partial() -> None:
    from datetime import UTC, datetime

    from superjobs.jobs.job_identity import JobIdentity

    transport = InMemoryTransport()
    await transport.start()
    try:
        evaluated_at = datetime.now(tz=UTC)
        transport._executions["exec-pending-plain"] = ExecutionRecord(
            identity=JobIdentity(name="tests.waiting.pending_plain", version="v1"),
            job_id="exec-pending-plain",
            request_payload=b"",
            request_media_type=None,
            fingerprint="fp",
            idempotency_key="key-pending-plain",
            caller_scope="default",
            timeout=None,
            deadline=None,
            created_at=evaluated_at,
            state=JobState.PENDING,
        )
        view = await read_waiting_executions(transport)
        assert view.completeness is WaitingExecutionCompleteness.PARTIAL
        assert view.entries == ()
    finally:
        await transport.stop()


@pytest.mark.asyncio
async def test_writer_gone_same_attempt_waiting_record_is_historical_and_partial() -> None:
    from datetime import UTC, datetime
    from uuid import uuid4

    from superjobs.jobs.job_identity import JobIdentity

    transport = InMemoryTransport()
    await transport.start()
    try:
        invocation = uuid4()
        evaluated_at = datetime.now(tz=UTC)
        transport._executions["exec-dead-writer"] = ExecutionRecord(
            identity=JobIdentity(name="tests.waiting.dead_writer", version="v1"),
            job_id="exec-dead-writer",
            request_payload=b"",
            request_media_type=None,
            fingerprint="fp",
            idempotency_key="key-dead-writer",
            caller_scope="default",
            timeout=None,
            deadline=None,
            created_at=evaluated_at,
            state=JobState.WAITING_FOR_RESOURCE,
            attempt=1,
            waiting_resource_key=RESOURCE_KEY,
            waiting_resource_invocation_id=invocation,
            waiting_resource_attempt=1,
        )
        view = await read_waiting_executions(transport)
        assert len(view.entries) == 1
        assert view.entries[0].evidence is WaitingExecutionEvidence.PERSISTED_HISTORICAL
        assert view.entries[0].waiting_invocation_id == invocation
        assert view.completeness is WaitingExecutionCompleteness.PARTIAL
    finally:
        await transport.stop()


@pytest.mark.asyncio
async def test_free_resource_submit_not_listed_while_pending() -> None:
    acquired = asyncio.Event()
    release = asyncio.Event()

    catalog = HandlerCatalog()

    @catalog.handler(job_a, resources={RESOURCE_KEY: 1})
    async def handle(context) -> str:
        acquired.set()
        await asyncio.wait_for(release.wait(), timeout=3.0)
        context.acknowledge_resource_cleanup()
        return "ok"

    transport = InMemoryTransport()
    runtime = SuperJobs(
        transport=transport,
        handlers=catalog,
        resource_capacities={RESOURCE_KEY: 1},
        discovery=False,
    )
    await runtime.start()
    try:
        handle = await runtime.client(job_a).submit()
        for _ in range(20):
            view = await runtime.resources.waiting_executions()
            assert view.entries == ()
            record = await transport.get_execution(job_a.identity, handle.id)
            assert record is not None
            if record.state is JobState.RUNNING:
                break
            await asyncio.sleep(0.02)
        assert acquired.is_set()
        release.set()
        await asyncio.wait_for(handle.result(), timeout=3.0)
    finally:
        await runtime.stop(graceful=False)


@pytest.mark.asyncio
async def test_waiting_invocation_fence_blocks_conflicting_key_overwrite() -> None:
    from datetime import UTC, datetime
    from uuid import uuid4

    from superjobs.jobs.job_identity import JobIdentity

    transport = InMemoryTransport()
    await transport.start()
    try:
        evaluated_at = datetime.now(tz=UTC)
        record = ExecutionRecord(
            identity=JobIdentity(name="tests.waiting.fence", version="v1"),
            job_id="exec-fence",
            request_payload=b"",
            request_media_type=None,
            fingerprint="fp",
            idempotency_key="key-fence",
            caller_scope="default",
            timeout=None,
            deadline=None,
            created_at=evaluated_at,
            state=JobState.PENDING,
        )
        transport._executions[record.job_id] = record
        first = uuid4()
        second = uuid4()
        other_key = "device.gpu1"
        await transport.mark_started(
            record,
            1,
            target_state=JobState.WAITING_FOR_RESOURCE,
            waiting_resource_key=RESOURCE_KEY,
            waiting_resource_invocation_id=first,
        )
        stale = transport._executions[record.job_id]
        await transport.mark_started(
            stale,
            1,
            target_state=JobState.WAITING_FOR_RESOURCE,
            waiting_resource_key=other_key,
            waiting_resource_invocation_id=second,
        )
        stored = transport._executions[record.job_id]
        assert stored.waiting_resource_key == RESOURCE_KEY
        assert stored.waiting_resource_invocation_id == first
        assert stored.waiting_resource_attempt == 1
        view = await read_waiting_executions(transport)
        assert len(view.entries) == 1
        assert view.entries[0].resource_key == RESOURCE_KEY
        assert view.entries[0].waiting_invocation_id == first
        assert view.completeness is WaitingExecutionCompleteness.PARTIAL
    finally:
        await transport.stop()


@pytest.mark.asyncio
async def test_waiting_observation_drops_on_cancel_before_acquire() -> None:
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
        for _ in range(60):
            view = await runtime_a.resources.waiting_executions()
            if view.entries:
                break
            await asyncio.sleep(0.05)
        assert view.entries
        await second.cancel()
        for _ in range(60):
            view = await runtime_a.resources.waiting_executions()
            if not view.entries:
                break
            await asyncio.sleep(0.05)
        assert view.entries == ()
        release.set()
        await asyncio.wait_for(first.result(), timeout=3.0)
    finally:
        await runtime_a.stop(graceful=False)
        await runtime_b.stop(graceful=False)


@pytest.mark.asyncio
async def test_supersede_stale_waiting_pin_on_newer_attempt() -> None:
    from datetime import UTC, datetime
    from uuid import uuid4

    from superjobs.jobs.job_identity import JobIdentity

    transport = InMemoryTransport()
    await transport.start()
    try:
        evaluated_at = datetime.now(tz=UTC)
        record = ExecutionRecord(
            identity=JobIdentity(name="tests.waiting.supersede", version="v1"),
            job_id="exec-supersede",
            request_payload=b"",
            request_media_type=None,
            fingerprint="fp",
            idempotency_key="key-supersede",
            caller_scope="default",
            timeout=None,
            deadline=None,
            created_at=evaluated_at,
            state=JobState.WAITING_FOR_RESOURCE,
            attempt=1,
            waiting_resource_key=RESOURCE_KEY,
            waiting_resource_invocation_id=uuid4(),
            waiting_resource_attempt=1,
        )
        transport._executions[record.job_id] = record
        successor = uuid4()
        other_key = "device.gpu1"
        await transport.mark_started(
            record,
            2,
            target_state=JobState.WAITING_FOR_RESOURCE,
            waiting_resource_key=other_key,
            waiting_resource_invocation_id=successor,
        )
        stored = transport._executions[record.job_id]
        assert stored.waiting_resource_key == other_key
        assert stored.waiting_resource_invocation_id == successor
        assert stored.waiting_resource_attempt == 2
        view = await read_waiting_executions(transport)
        assert view.completeness is WaitingExecutionCompleteness.PARTIAL
        assert view.entries[0].resource_key == other_key
        assert view.entries[0].waiting_invocation_id == successor
    finally:
        await transport.stop()


@pytest.mark.asyncio
async def test_stale_waiting_pin_lists_historical_entry_and_partial() -> None:
    from datetime import UTC, datetime
    from uuid import uuid4

    from superjobs.jobs.job_identity import JobIdentity

    transport = InMemoryTransport()
    await transport.start()
    try:
        evaluated_at = datetime.now(tz=UTC)
        transport._executions["exec-stale-pin"] = ExecutionRecord(
            identity=JobIdentity(name="tests.waiting.stale", version="v1"),
            job_id="exec-stale-pin",
            request_payload=b"",
            request_media_type=None,
            fingerprint="fp",
            idempotency_key="key-stale",
            caller_scope="default",
            timeout=None,
            deadline=None,
            created_at=evaluated_at,
            state=JobState.WAITING_FOR_RESOURCE,
            attempt=2,
            waiting_resource_key=RESOURCE_KEY,
            waiting_resource_invocation_id=uuid4(),
            waiting_resource_attempt=1,
        )
        view = await read_waiting_executions(transport)
        assert len(view.entries) == 1
        assert view.entries[0].resource_key == RESOURCE_KEY
        assert view.completeness is WaitingExecutionCompleteness.PARTIAL
    finally:
        await transport.stop()


def test_waiting_pin_helpers_block_concurrent_same_attempt() -> None:
    from datetime import UTC, datetime
    from uuid import uuid4

    from superjobs.jobs.job_identity import JobIdentity
    from superjobs.resources.waiting_execution import waiting_pin_blocks_new_publication

    first = uuid4()
    second = uuid4()
    record = ExecutionRecord(
        identity=JobIdentity(name="tests.waiting.helpers", version="v1"),
        job_id="exec-helpers",
        request_payload=b"",
        request_media_type=None,
        fingerprint="fp",
        idempotency_key="key-helpers",
        caller_scope="default",
        timeout=None,
        deadline=None,
        created_at=datetime.now(tz=UTC),
        state=JobState.WAITING_FOR_RESOURCE,
        attempt=1,
        waiting_resource_key=RESOURCE_KEY,
        waiting_resource_invocation_id=first,
        waiting_resource_attempt=1,
    )
    assert waiting_pin_blocks_new_publication(
        record,
        attempt=1,
        waiting_resource_invocation_id=second,
    )
    assert not waiting_pin_blocks_new_publication(
        record,
        attempt=2,
        waiting_resource_invocation_id=second,
    )
