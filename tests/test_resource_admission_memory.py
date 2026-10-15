from __future__ import annotations

from uuid import uuid4

import pytest

from superjobs.resources import (
    AdmissionAcquireIntent,
    AcquisitionOutcome,
    InMemoryResourceAdmissionStore,
    ReconcileOutcome,
    RecoveryOutcome,
    ReleaseOutcome,
    ResourceAdmission,
    ResourceRequirement,
    TerminationConfirmation,
    WorkerIncarnation,
    parse_exclusive_resource_requirement,
)
from superjobs.resources.errors import ResourceValidationError


def _incarnation(worker_id: str, incarnation_id: str = "inc-1") -> WorkerIncarnation:
    return WorkerIncarnation(worker_id=worker_id, incarnation_id=incarnation_id)


def _intent(
    *,
    worker_id: str,
    execution_id: str,
    resource_key: str,
    invocation_id=None,
    incarnation_id: str = "inc-1",
) -> AdmissionAcquireIntent:
    return AdmissionAcquireIntent(
        incarnation=_incarnation(worker_id, incarnation_id),
        invocation_id=invocation_id or uuid4(),
        execution_id=execution_id,
        resource_key=resource_key,
    )


@pytest.fixture
def store() -> InMemoryResourceAdmissionStore:
    return InMemoryResourceAdmissionStore()


@pytest.fixture
def admission(store: InMemoryResourceAdmissionStore) -> ResourceAdmission:
    return ResourceAdmission(store)


@pytest.mark.asyncio
async def test_parse_exclusive_resource_requirement_rejects_multi_and_non_unit() -> None:
    with pytest.raises(ResourceValidationError):
        parse_exclusive_resource_requirement({"gpu/a": 1, "gpu/b": 1})
    with pytest.raises(ResourceValidationError):
        parse_exclusive_resource_requirement({"gpu/a": 2})


@pytest.mark.asyncio
async def test_capacity_declaration_is_idempotent(admission: ResourceAdmission) -> None:
    requirement = ResourceRequirement(resource_key="device.gpu0", units=1)
    first = await admission.declare_capacity(requirement)
    second = await admission.declare_capacity(requirement)
    assert first == second


@pytest.mark.asyncio
async def test_resource_requirement_rejects_non_unit_capacity() -> None:
    with pytest.raises(Exception):
        ResourceRequirement(resource_key="device.gpu0", units=2)


@pytest.mark.asyncio
async def test_cross_job_resource_contention(store: InMemoryResourceAdmissionStore) -> None:
    shared = store
    left = ResourceAdmission(shared)
    right = ResourceAdmission(shared)
    key = "host-a.gpu0"
    await left.declare_capacity(ResourceRequirement(resource_key=key, units=1))

    first = await left.try_acquire(
        _intent(worker_id="engine-a", execution_id="exec-1", resource_key=key),
    )
    second = await right.try_acquire(
        _intent(worker_id="engine-b", execution_id="exec-2", resource_key=key),
    )

    assert first.outcome is AcquisitionOutcome.ACQUIRED
    assert second.outcome is AcquisitionOutcome.CONTENTION
    snapshot = await left.snapshot()
    assert len(snapshot.execution_claims) == 1
    assert snapshot.execution_claims[0].execution_id == "exec-1"


@pytest.mark.asyncio
async def test_duplicate_invocation_excluded_for_same_execution(
    store: InMemoryResourceAdmissionStore,
) -> None:
    admission = ResourceAdmission(store)
    key = "host-a.gpu0"
    await admission.declare_capacity(ResourceRequirement(resource_key=key, units=1))
    first_intent = _intent(worker_id="w1", execution_id="exec-dup", resource_key=key)
    first = await admission.try_acquire(first_intent)
    assert first.outcome is AcquisitionOutcome.ACQUIRED

    redelivery = _intent(
        worker_id="w1",
        execution_id="exec-dup",
        resource_key=key,
        invocation_id=uuid4(),
    )
    second = await admission.try_acquire(redelivery)
    assert second.outcome is AcquisitionOutcome.CONTENTION


@pytest.mark.asyncio
async def test_partial_acquisition_rolls_back_execution_claim(
    store: InMemoryResourceAdmissionStore,
) -> None:
    left = ResourceAdmission(store)
    right = ResourceAdmission(store)
    key = "host-a.gpu0"
    await left.declare_capacity(ResourceRequirement(resource_key=key, units=1))
    await left.try_acquire(_intent(worker_id="w1", execution_id="exec-held", resource_key=key))

    blocked = await right.try_acquire(
        _intent(worker_id="w2", execution_id="exec-blocked", resource_key=key),
    )
    assert blocked.outcome is AcquisitionOutcome.CONTENTION
    snapshot = await left.snapshot()
    assert {claim.execution_id for claim in snapshot.execution_claims} == {"exec-held"}


@pytest.mark.asyncio
async def test_uncertain_resource_blocks_competing_takeover(
    store: InMemoryResourceAdmissionStore,
) -> None:
    owner_side = ResourceAdmission(store)
    competitor = ResourceAdmission(store)
    key = "host-a.gpu0"
    await owner_side.declare_capacity(ResourceRequirement(resource_key=key, units=1))
    acquired = await owner_side.try_acquire(
        _intent(worker_id="w1", execution_id="exec-uncertain", resource_key=key),
    )
    assert acquired.owner is not None
    marked = await owner_side.mark_uncertain(acquired.owner)
    assert marked.outcome is ReconcileOutcome.RECONCILED

    attempt = await competitor.try_acquire(
        _intent(worker_id="w2", execution_id="exec-other", resource_key=key),
    )
    assert attempt.outcome is AcquisitionOutcome.UNCERTAIN


@pytest.mark.asyncio
async def test_termination_recovery_allows_new_owner(
    store: InMemoryResourceAdmissionStore,
) -> None:
    first = ResourceAdmission(store)
    second = ResourceAdmission(store)
    key = "host-a.gpu0"
    await first.declare_capacity(ResourceRequirement(resource_key=key, units=1))
    acquired = await first.try_acquire(
        _intent(
            worker_id="w1",
            execution_id="exec-old",
            resource_key=key,
            incarnation_id="inc-old",
        ),
    )
    assert acquired.owner is not None

    recovery = await second.resolve_terminated_incarnation(
        TerminationConfirmation(
            incarnation=_incarnation("w1", "inc-old"),
            confirmed=True,
            evidence="supervisor:pid-42",
        ),
    )
    assert recovery.outcome is RecoveryOutcome.RESOLVED

    successor = await second.try_acquire(
        _intent(
            worker_id="w2",
            execution_id="exec-new",
            resource_key=key,
            incarnation_id="inc-new",
        ),
    )
    assert successor.outcome is AcquisitionOutcome.ACQUIRED


@pytest.mark.asyncio
async def test_stale_owner_cannot_release_newer_reservation(
    store: InMemoryResourceAdmissionStore,
) -> None:
    first = ResourceAdmission(store)
    second = ResourceAdmission(store)
    key = "host-a.gpu0"
    await first.declare_capacity(ResourceRequirement(resource_key=key, units=1))
    old = await first.try_acquire(
        _intent(worker_id="w1", execution_id="exec-1", resource_key=key, incarnation_id="inc-1"),
    )
    assert old.owner is not None

    await second.resolve_terminated_incarnation(
        TerminationConfirmation(
            incarnation=_incarnation("w1", "inc-1"),
            confirmed=True,
            evidence="supervisor:pid-99",
        ),
    )
    new = await second.try_acquire(
        _intent(worker_id="w2", execution_id="exec-2", resource_key=key, incarnation_id="inc-2"),
    )
    assert new.owner is not None

    stale_release = await first.release_acknowledged_cleanup(old.owner)
    assert stale_release.outcome is ReleaseOutcome.STALE

    still = await second.snapshot()
    resource = next(record for record in still.resources if record.resource_key == key)
    assert resource.owner == new.owner


@pytest.mark.asyncio
async def test_acknowledged_cleanup_releases_resource(admission: ResourceAdmission) -> None:
    key = "host-a.gpu0"
    await admission.declare_capacity(ResourceRequirement(resource_key=key, units=1))
    acquired = await admission.try_acquire(
        _intent(worker_id="w1", execution_id="exec-done", resource_key=key),
    )
    assert acquired.owner is not None
    released = await admission.release_acknowledged_cleanup(acquired.owner)
    assert released.outcome is ReleaseOutcome.RELEASED
    snapshot = await admission.snapshot()
    resource = next(record for record in snapshot.resources if record.resource_key == key)
    assert resource.availability.value == "available"
    assert not snapshot.execution_claims


@pytest.mark.asyncio
async def test_cleanup_failed_retains_unavailable(admission: ResourceAdmission) -> None:
    key = "host-a.gpu0"
    await admission.declare_capacity(ResourceRequirement(resource_key=key, units=1))
    acquired = await admission.try_acquire(
        _intent(worker_id="w1", execution_id="exec-fail", resource_key=key),
    )
    assert acquired.owner is not None
    retained = await admission.retain_cleanup_failed(acquired.owner, reason_detail="native leak")
    assert retained.outcome is ReconcileOutcome.RECONCILED
    snapshot = await admission.snapshot()
    resource = next(record for record in snapshot.resources if record.resource_key == key)
    assert resource.availability.value == "unavailable"
