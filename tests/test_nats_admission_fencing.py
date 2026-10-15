from __future__ import annotations

import uuid
from datetime import UTC, datetime

import pytest
from faststream.nats import NatsBroker

from superjobs.jobs.execution import JobState
from superjobs.resources import (
    AcquisitionOutcome,
    NatsResourceAdmissionStore,
    ResourceAdmission,
    ResourceRequirement,
    TerminationConfirmation,
    WorkerIncarnation,
)
from superjobs.resources.errors import ResourceStaleOperationError
from superjobs.transport.backend import SubmissionOptions
from superjobs.transport.nats_backend import NatsJobBackend
from tests.test_resource_admission_memory import _intent

pytestmark = pytest.mark.nats


@pytest.mark.asyncio
async def test_stale_owner_cannot_complete_or_rebind_after_recovery(
    nats_url: str,
    nats_queue_config,
) -> None:
    broker_a = NatsBroker(nats_url, connect_timeout=2, max_reconnect_attempts=0)
    broker_b = NatsBroker(nats_url, connect_timeout=2, max_reconnect_attempts=0)
    await broker_a.start()
    await broker_b.start()
    backend = NatsJobBackend(broker_a, nats_queue_config)
    await backend.start()
    key = "host-nats-fence.gpu0"
    store_a = NatsResourceAdmissionStore(
        broker_a, subject_prefix=nats_queue_config.subject_prefix
    )
    store_b = NatsResourceAdmissionStore(
        broker_b, subject_prefix=nats_queue_config.subject_prefix
    )
    left = ResourceAdmission(store_a)
    right = ResourceAdmission(store_b)
    try:
        await left.declare_capacity(ResourceRequirement(resource_key=key, units=1))
        execution = await backend.submit(
            job_a_identity(),
            request_payload=b"",
            request_media_type=None,
            fingerprint="fp",
            options=SubmissionOptions(
                idempotency_key=f"id-{uuid.uuid4().hex}",
                job_id=f"exec-{uuid.uuid4().hex}",
            ),
            publish=False,
        )
        first = await left.try_acquire(
            _intent(
                worker_id="worker-a", execution_id=execution.job_id, resource_key=key
            ),
        )
        assert first.outcome is AcquisitionOutcome.ACQUIRED
        assert first.owner is not None
        await backend.mark_started(
            execution,
            1,
            target_state=JobState.RUNNING,
            admission_owner=first.owner,
        )
        recovery = await right.resolve_terminated_incarnation(
            TerminationConfirmation(
                incarnation=WorkerIncarnation(
                    worker_id="worker-a", incarnation_id="inc-1"
                ),
                confirmed=True,
                evidence="supervisor:worker-a",
            ),
        )
        assert recovery.outcome.value == "resolved"
        with pytest.raises(ResourceStaleOperationError):
            await backend.write_completion(
                execution,
                state=JobState.COMPLETED,
                result_payload=b"ok",
                expected_admission_owner=first.owner,
            )
        with pytest.raises(ResourceStaleOperationError):
            await backend.mark_retry(
                execution,
                next_attempt_at=datetime.now(tz=UTC),
                expected_admission_owner=first.owner,
            )
        successor = await right.try_acquire(
            _intent(
                worker_id="worker-b", execution_id=execution.job_id, resource_key=key
            ),
        )
        assert successor.outcome is AcquisitionOutcome.ACQUIRED
        assert successor.owner is not None
        refreshed = await backend.get_execution(job_a_identity(), execution.job_id)
        assert refreshed is not None
        await backend.mark_started(refreshed, 1, admission_owner=successor.owner)
        with pytest.raises(ResourceStaleOperationError):
            await backend.mark_started(execution, 1, admission_owner=first.owner)
        with pytest.raises(ResourceStaleOperationError):
            await backend.write_completion(
                execution,
                state=JobState.COMPLETED,
                expected_admission_owner=first.owner,
            )
    finally:
        await backend.stop()
        await broker_a.stop()
        await broker_b.stop()


def job_a_identity():
    from superjobs.jobs.job_identity import JobIdentity

    return JobIdentity("tests.nats.fence.job", version="v1")
