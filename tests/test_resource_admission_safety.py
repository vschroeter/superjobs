from __future__ import annotations

import asyncio
from uuid import uuid4

import pytest
from nats.js.errors import APIError

from superjobs.resources import (
    AcquisitionOutcome,
    InMemoryResourceAdmissionStore,
    NatsResourceAdmissionStore,
    ReconcileOutcome,
    RecoveryOutcome,
    ReleaseOutcome,
    ResourceAdmission,
    ResourcePartialAcquisitionError,
    ResourceRequirement,
    ResourceStaleOperationError,
    TerminationConfirmation,
    WorkerIncarnation,
)
from pydantic import ValidationError
from superjobs.resources.store import CasStatus
from tests.test_resource_admission_memory import _incarnation, _intent


class FaultInjectingAdmissionStore(InMemoryResourceAdmissionStore):
    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self.raise_after_resource_apply: BaseException | None = None
        self.raise_after_execution_apply: BaseException | None = None

    async def compare_and_set_resource(self, record, *, last_revision):
        result = await super().compare_and_set_resource(record, last_revision=last_revision)
        if result.status is CasStatus.APPLIED and self.raise_after_resource_apply is not None:
            error = self.raise_after_resource_apply
            self.raise_after_resource_apply = None
            raise error
        return result

    async def compare_and_set_execution_claim(self, record, *, last_revision):
        result = await super().compare_and_set_execution_claim(
            record,
            last_revision=last_revision,
        )
        if result.status is CasStatus.APPLIED and self.raise_after_execution_apply is not None:
            error = self.raise_after_execution_apply
            self.raise_after_execution_apply = None
            raise error
        return result


class PermissionErrorStore(InMemoryResourceAdmissionStore):
    async def compare_and_set_resource(self, record, *, last_revision):
        raise APIError("permission denied")


@pytest.mark.asyncio
async def test_resource_cas_success_then_fault_preserves_partial_state() -> None:
    store = FaultInjectingAdmissionStore()
    admission = ResourceAdmission(store)
    key = "host-a.gpu0"
    await admission.declare_capacity(ResourceRequirement(resource_key=key, units=1))
    store.raise_after_resource_apply = RuntimeError("injected after resource CAS")

    with pytest.raises(ResourcePartialAcquisitionError):
        await admission.try_acquire(_intent(worker_id="w1", execution_id="exec-partial", resource_key=key))

    snapshot = await admission.snapshot()
    assert len(snapshot.execution_claims) == 1
    resource = next(record for record in snapshot.resources if record.resource_key == key)
    assert resource.owner is not None
    assert resource.owner.execution_id == "exec-partial"


@pytest.mark.asyncio
async def test_failed_resource_claim_rolls_back_execution_only_when_resource_unowned() -> None:
    store = InMemoryResourceAdmissionStore()
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
async def test_same_intent_retry_reuses_owner_token() -> None:
    store = InMemoryResourceAdmissionStore()
    admission = ResourceAdmission(store)
    key = "host-a.gpu0"
    await admission.declare_capacity(ResourceRequirement(resource_key=key, units=1))
    invocation = uuid4()
    intent = _intent(
        worker_id="w1",
        execution_id="exec-retry",
        resource_key=key,
        invocation_id=invocation,
    )
    first = await admission.try_acquire(intent)
    assert first.outcome is AcquisitionOutcome.ACQUIRED
    assert first.owner is not None

    second = await admission.try_acquire(intent)
    assert second.outcome is AcquisitionOutcome.ALREADY_HELD
    assert second.owner == first.owner


@pytest.mark.asyncio
async def test_two_stage_release_retains_execution_claim_until_finalize() -> None:
    store = InMemoryResourceAdmissionStore()
    admission = ResourceAdmission(store)
    key = "host-a.gpu0"
    await admission.declare_capacity(ResourceRequirement(resource_key=key, units=1))
    acquired = await admission.try_acquire(
        _intent(worker_id="w1", execution_id="exec-two-stage", resource_key=key),
    )
    assert acquired.owner is not None

    stage_one = await admission.release_resource_after_cleanup(acquired.owner)
    assert stage_one.outcome is ReleaseOutcome.RELEASED
    mid = await admission.snapshot()
    resource = next(record for record in mid.resources if record.resource_key == key)
    assert resource.availability.value == "available"
    assert len(mid.execution_claims) == 1

    impersonator = await admission.try_acquire(
        _intent(worker_id="w2", execution_id="exec-two-stage", resource_key=key),
    )
    assert impersonator.outcome is AcquisitionOutcome.CONTENTION

    successor = await admission.try_acquire(
        _intent(worker_id="w2", execution_id="exec-other", resource_key=key),
    )
    assert successor.outcome is AcquisitionOutcome.ACQUIRED

    finalized = await admission.finalize_execution_claim(acquired.owner)
    assert finalized.outcome is ReleaseOutcome.FINALIZED
    after = await admission.snapshot()
    assert {claim.execution_id for claim in after.execution_claims} == {"exec-other"}


@pytest.mark.asyncio
async def test_mark_uncertain_does_not_downgrade_cleanup_failed() -> None:
    store = InMemoryResourceAdmissionStore()
    admission = ResourceAdmission(store)
    key = "host-a.gpu0"
    await admission.declare_capacity(ResourceRequirement(resource_key=key, units=1))
    acquired = await admission.try_acquire(
        _intent(worker_id="w1", execution_id="exec-fail", resource_key=key),
    )
    assert acquired.owner is not None
    await admission.retain_cleanup_failed(acquired.owner, reason_detail="native leak")

    marked = await admission.mark_uncertain(acquired.owner)
    assert marked.outcome is ReconcileOutcome.UNAVAILABLE
    snapshot = await admission.snapshot()
    resource = next(record for record in snapshot.resources if record.resource_key == key)
    assert resource.unavailable_reason is not None
    assert resource.reason_detail == "native leak"


@pytest.mark.asyncio
async def test_assert_owner_active_rejects_uncertain_and_unavailable() -> None:
    store = InMemoryResourceAdmissionStore()
    admission = ResourceAdmission(store)
    key = "host-a.gpu0"
    await admission.declare_capacity(ResourceRequirement(resource_key=key, units=1))
    acquired = await admission.try_acquire(
        _intent(worker_id="w1", execution_id="exec-assert", resource_key=key),
    )
    assert acquired.owner is not None
    await admission.mark_uncertain(acquired.owner)
    with pytest.raises(ResourceStaleOperationError):
        await admission.assert_owner_active(acquired.owner)

    await admission.retain_cleanup_failed(acquired.owner)
    with pytest.raises(ResourceStaleOperationError):
        await admission.assert_owner_active(acquired.owner)


@pytest.mark.asyncio
async def test_revoked_incarnation_blocks_acquire_and_recovery_is_idempotent() -> None:
    store = InMemoryResourceAdmissionStore()
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

    confirmation = TerminationConfirmation(
        incarnation=_incarnation("w1", "inc-old"),
        confirmed=True,
        evidence="supervisor:pid-1",
    )
    recovery = await second.resolve_terminated_incarnation(confirmation)
    assert recovery.outcome is RecoveryOutcome.RESOLVED

    blocked = await second.try_acquire(
        _intent(
            worker_id="w1",
            execution_id="exec-new",
            resource_key=key,
            incarnation_id="inc-old",
        ),
    )
    assert blocked.outcome is AcquisitionOutcome.UNAVAILABLE

    again = await second.resolve_terminated_incarnation(confirmation)
    assert again.outcome is RecoveryOutcome.RESOLVED


@pytest.mark.asyncio
async def test_termination_confirmation_requires_evidence() -> None:
    with pytest.raises(ValidationError):
        TerminationConfirmation(
            incarnation=WorkerIncarnation(worker_id="w1", incarnation_id="inc-1"),
            confirmed=True,
            evidence="   ",
        )


@pytest.mark.asyncio
async def test_non_cas_store_errors_fail_fast_without_spin() -> None:
    from datetime import UTC, datetime

    from superjobs.resources.config import ResourceAdmissionConfig
    from superjobs.resources.keys import capacity_kv_key
    from superjobs.resources.memory import _RevisionedValue
    from superjobs.resources.models import CapacityDeclaration, StoredCapacityRecord
    from superjobs.resources.wire import encode_capacity

    key = "host-a.gpu0"
    store_with_capacity = PermissionErrorStore()
    config = ResourceAdmissionConfig()
    declaration = CapacityDeclaration(resource_key=key, capacity=1)
    record = StoredCapacityRecord(declaration=declaration, updated_at=datetime.now(tz=UTC))
    encoded = encode_capacity(record, config)
    store_with_capacity._values[capacity_kv_key(key)] = _RevisionedValue(payload=encoded, revision=1)

    admission = ResourceAdmission(store_with_capacity)
    with pytest.raises(APIError):
        await admission.try_acquire(_intent(worker_id="w1", execution_id="exec-x", resource_key=key))


@pytest.mark.asyncio
async def test_cancellation_during_acquire_runs_bounded_cleanup() -> None:
    store = FaultInjectingAdmissionStore()
    admission = ResourceAdmission(store)
    key = "host-a.gpu0"
    await admission.declare_capacity(ResourceRequirement(resource_key=key, units=1))

    async def cancel_mid_acquire() -> None:
        store.raise_after_execution_apply = asyncio.CancelledError()
        with pytest.raises(asyncio.CancelledError):
            await admission.try_acquire(
                _intent(worker_id="w1", execution_id="exec-cancel", resource_key=key),
            )

    await cancel_mid_acquire()
    snapshot = await admission.snapshot()
    assert not snapshot.execution_claims


def test_nats_delete_execution_claim_uses_revision_guard() -> None:
    import inspect

    body = inspect.getsource(NatsResourceAdmissionStore.delete_execution_claim_if_owner)
    assert "last=revision" in body
