"""Fenced ownership handoff without releasing a resident resource."""

from __future__ import annotations

from typing import TYPE_CHECKING, cast

from superjobs.resources.errors import ResourceStaleOperationError, ResourceStoreUnavailableError
from superjobs.resources.models import (
    AcquireResult,
    AcquisitionOutcome,
    AdmissionAcquireIntent,
    AdmissionOwner,
    ReconcileOutcome,
    ReleaseOutcome,
    ReleaseResult,
    ResourceAvailability,
    StoredExecutionClaim,
    StoredResourceRecord,
)
from superjobs.resources.store import CasStatus

if TYPE_CHECKING:
    from superjobs.resources.admission import ResourceAdmission


class RetainedAdmissionMixin:
    async def retain_resource(self, owner: AdmissionOwner, residency_key: str) -> None:
        """Detach device residency from a completed invocation, without claiming cleanup."""
        admission = cast("ResourceAdmission", self)
        if (
            not isinstance(residency_key, str)
            or not residency_key.strip()
            or len(residency_key) > 512
        ):
            raise ValueError("residency_key must contain 1 to 512 characters")
        for _ in range(16):
            await admission.assert_execution_owner(owner)
            record, revision = await admission._store.read_resource(owner.resource_key)
            if (
                record is None
                or record.owner != owner
                or record.availability is not ResourceAvailability.OWNED
            ):
                raise ResourceStaleOperationError("Cannot retain unconfirmed ownership")
            updated = record.model_copy(
                update=dict(
                    retained=True, residency_key=residency_key, updated_at=admission._clock()
                )
            )
            if (
                await admission._store.compare_and_set_resource(updated, last_revision=revision)
            ).status is CasStatus.APPLIED:
                return
        raise ResourceStoreUnavailableError("Concurrent updates prevented residency retention")

    async def request_release(self, resource_key: str) -> bool:
        """Request cooperative cleanup; this never releases or revokes ownership."""
        admission = cast("ResourceAdmission", self)
        for _ in range(16):
            record, revision = await admission._store.read_resource(resource_key)
            if record is None or record.owner is None or not record.residency_key:
                return False
            if record.release_requested:
                return True
            updated = record.model_copy(
                update=dict(release_requested=True, updated_at=admission._clock())
            )
            if (
                await admission._store.compare_and_set_resource(updated, last_revision=revision)
            ).status is CasStatus.APPLIED:
                return True
        raise ResourceStoreUnavailableError("Concurrent updates prevented release request")

    async def reconcile_retained(self, owner: AdmissionOwner) -> StoredResourceRecord:
        """Maintain an idle reservation after its execution claim was finalized."""
        admission = cast("ResourceAdmission", self)
        if await admission._store.read_revoked_incarnation(owner.incarnation) is not None:
            raise ResourceStaleOperationError("Retained incarnation was terminated")
        for _ in range(16):
            record, revision = await admission._store.read_resource(owner.resource_key)
            if record is None or record.owner != owner or not record.retained:
                raise ResourceStaleOperationError("Retained resource token is stale")
            if record.availability is ResourceAvailability.UNAVAILABLE:
                raise ResourceStaleOperationError("Retained resource cleanup failed")
            updated = record.model_copy(
                update=dict(
                    availability=ResourceAvailability.OWNED,
                    heartbeat_at=admission._clock(),
                    updated_at=admission._clock(),
                )
            )
            if (
                await admission._store.compare_and_set_resource(updated, last_revision=revision)
            ).status is CasStatus.APPLIED:
                return updated
        raise ResourceStoreUnavailableError("Concurrent updates prevented residency heartbeat")

    async def acquire_retained(
        self, intent: AdmissionAcquireIntent, previous: AdmissionOwner
    ) -> AcquireResult:
        """CAS-transfer a local idle reservation to a fresh invocation and claim."""
        admission = cast("ResourceAdmission", self)
        if (
            intent.incarnation != previous.incarnation
            or intent.resource_key != previous.resource_key
        ):
            return AcquireResult(outcome=AcquisitionOutcome.STALE)
        if await admission._store.read_revoked_incarnation(intent.incarnation) is not None:
            return AcquireResult(outcome=AcquisitionOutcome.UNAVAILABLE)
        record, revision = await admission._store.read_resource(intent.resource_key)
        if record is None:
            return AcquireResult(outcome=AcquisitionOutcome.STALE)
        existing = await admission.execution_owner(intent.execution_id)
        # Reconcile a successful CAS whose acknowledgement was lost.
        if (
            existing is not None
            and existing.invocation_id == intent.invocation_id
            and record.owner == existing
        ):
            if (await admission.reconcile(existing)).outcome is ReconcileOutcome.RECONCILED:
                return AcquireResult(
                    outcome=AcquisitionOutcome.ACQUIRED, owner=existing, resource=record
                )
        if record.owner != previous or not record.retained or record.release_requested:
            return AcquireResult(outcome=AcquisitionOutcome.CONTENTION, resource=record)
        if record.availability is not ResourceAvailability.OWNED:
            return AcquireResult(outcome=AcquisitionOutcome.UNCERTAIN, resource=record)
        owner = admission._build_owner(intent, record)
        now = admission._clock()
        claim = await admission._claim_execution(
            StoredExecutionClaim(
                execution_id=intent.execution_id, owner=owner, heartbeat_at=now, updated_at=now
            )
        )
        if claim.outcome not in {AcquisitionOutcome.ACQUIRED, AcquisitionOutcome.ALREADY_HELD}:
            return claim
        assert claim.owner is not None
        owner = claim.owner
        updated = record.model_copy(
            update=dict(owner=owner, retained=False, heartbeat_at=now, updated_at=now)
        )
        try:
            write = await admission._store.compare_and_set_resource(updated, last_revision=revision)
            if write.status is CasStatus.APPLIED:
                return AcquireResult(
                    outcome=AcquisitionOutcome.ACQUIRED, owner=owner, resource=updated
                )
            await admission._reconcile_failed_acquire(owner)
            return AcquireResult(outcome=AcquisitionOutcome.CONTENTION, resource=write.value)
        except BaseException:
            # Never free CUDA residency if this transfer may have succeeded.
            await admission._bounded_acquire_cleanup(owner)
            raise

    async def release_retained_after_cleanup(self, owner: AdmissionOwner) -> ReleaseResult:
        """Release an idle device only after the caller's actual cleanup completed."""
        admission = cast("ResourceAdmission", self)
        for _ in range(16):
            record, revision = await admission._store.read_resource(owner.resource_key)
            if record is None or record.owner != owner:
                return ReleaseResult(outcome=ReleaseOutcome.STALE, resource=record)
            if not record.retained:
                return ReleaseResult(outcome=ReleaseOutcome.STALE, resource=record)
            released = StoredResourceRecord(
                resource_key=owner.resource_key,
                availability=ResourceAvailability.AVAILABLE,
                last_owner_generation=owner.fencing.generation,
                updated_at=admission._clock(),
            )
            if (
                await admission._store.compare_and_set_resource(released, last_revision=revision)
            ).status is CasStatus.APPLIED:
                return ReleaseResult(outcome=ReleaseOutcome.RELEASED, resource=released)
        raise ResourceStoreUnavailableError("Concurrent updates prevented residency release")
