"""Independent regression cases for admission ownership recovery."""

from uuid import uuid4

import pytest

from superjobs.transport.admission_record import (
    admission_owner_from_wire,
    admission_owner_to_wire,
)
from superjobs.resources.models import (
    AdmissionFencing,
    AdmissionOwner,
)

from superjobs.resources import (
    AcquisitionOutcome,
    AdmissionAcquireIntent,
    InMemoryResourceAdmissionStore,
    ResourceAdmission,
    ResourceRequirement,
    TerminationConfirmation,
    WorkerIncarnation,
)


def intent(execution: str, incarnation: str = "first") -> AdmissionAcquireIntent:
    return AdmissionAcquireIntent(
        incarnation=WorkerIncarnation(
            worker_id="review-worker", incarnation_id=incarnation
        ),
        invocation_id=uuid4(),
        execution_id=execution,
        resource_key="review-host.gpu0",
    )


def test_admission_owner_wire_roundtrip_preserves_uuid() -> None:
    owner = AdmissionOwner(
        resource_key="review-host.gpu0",
        incarnation=WorkerIncarnation(worker_id="w1", incarnation_id="inc-1"),
        invocation_id=uuid4(),
        execution_id="exec-wire",
        fencing=AdmissionFencing(generation=2, token="tok"),
    )
    restored = admission_owner_from_wire(admission_owner_to_wire(owner))
    assert restored == owner


async def test_generation_advances_after_acknowledged_release() -> None:
    admission = ResourceAdmission(InMemoryResourceAdmissionStore())
    await admission.declare_capacity(
        ResourceRequirement(resource_key="review-host.gpu0", units=1)
    )
    first = await admission.try_acquire(intent("first"))
    assert first.owner is not None
    await admission.release_acknowledged_cleanup(first.owner)
    second = await admission.try_acquire(intent("second", "second"))
    assert second.owner is not None
    assert second.owner.fencing.generation > first.owner.fencing.generation


async def test_repeated_termination_confirmation_finishes_an_interrupted_sweep() -> (
    None
):
    class InterruptedStore(InMemoryResourceAdmissionStore):
        fail_release = False

        async def compare_and_set_resource(self, record, *, last_revision):
            if self.fail_release and record.owner is None:
                self.fail_release = False
                raise ConnectionError(
                    "broker disconnected after incarnation revocation"
                )
            return await super().compare_and_set_resource(
                record, last_revision=last_revision
            )

    store = InterruptedStore()
    admission = ResourceAdmission(store)
    await admission.declare_capacity(
        ResourceRequirement(resource_key="review-host.gpu0", units=1)
    )
    first = await admission.try_acquire(intent("first"))
    assert first.owner is not None
    confirmation = TerminationConfirmation(
        incarnation=first.owner.incarnation,
        confirmed=True,
        evidence="supervisor observed contained process exit",
    )
    store.fail_release = True
    with pytest.raises(ConnectionError):
        await admission.resolve_terminated_incarnation(confirmation)
    await admission.resolve_terminated_incarnation(confirmation)
    successor = await admission.try_acquire(intent("second", "second"))
    assert successor.outcome is AcquisitionOutcome.ACQUIRED


async def test_claim_revision_does_not_repeat_after_delete_and_recreate() -> None:
    from superjobs.resources.store import CasStatus

    store = InMemoryResourceAdmissionStore()
    admission = ResourceAdmission(store)
    await admission.declare_capacity(
        ResourceRequirement(resource_key="review-host.gpu0", units=1)
    )
    first = await admission.try_acquire(intent("same-execution"))
    assert first.owner is not None
    stale_claim, stale_revision = await store.read_execution_claim("same-execution")
    assert stale_claim is not None
    await admission.release_acknowledged_cleanup(first.owner)
    successor = await admission.try_acquire(intent("same-execution", "second"))
    assert successor.owner is not None
    stale_write = await store.compare_and_set_execution_claim(
        stale_claim,
        last_revision=stale_revision,
    )
    assert stale_write.status is CasStatus.CONFLICT
    current, _ = await store.read_execution_claim("same-execution")
    assert current is not None and current.owner == successor.owner


async def test_expired_heartbeat_is_visible_without_authorizing_takeover() -> None:
    from datetime import UTC, datetime, timedelta
    from superjobs.resources import ResourceAdmissionConfig, ResourceAvailability

    now = [datetime.now(UTC)]
    admission = ResourceAdmission(
        InMemoryResourceAdmissionStore(),
        clock=lambda: now[0],
        config=ResourceAdmissionConfig(heartbeat_timeout=1),
    )
    await admission.declare_capacity(
        ResourceRequirement(resource_key="review-host.gpu0", units=1)
    )
    first = await admission.try_acquire(intent("first"))
    assert first.owner is not None
    now[0] += timedelta(seconds=2)
    assert (await admission.snapshot()).resources[
        0
    ].availability is ResourceAvailability.UNCERTAIN
    assert (
        await admission.try_acquire(intent("second", "second"))
    ).outcome is not AcquisitionOutcome.ACQUIRED
    await admission.reconcile(first.owner)
    assert (await admission.snapshot()).resources[
        0
    ].availability is ResourceAvailability.OWNED
