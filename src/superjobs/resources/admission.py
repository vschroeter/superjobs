from __future__ import annotations

import asyncio
from collections.abc import Callable
from datetime import UTC, datetime
from typing import TYPE_CHECKING
from uuid import uuid4

from superjobs.resources.errors import (
    ResourceConflictError,
    ResourcePartialAcquisitionError,
    ResourceStaleOperationError,
    ResourceStoreUnavailableError,
)
from superjobs.resources.models import (
    AcquireResult,
    AcquisitionOutcome,
    AdmissionAcquireIntent,
    AdmissionFencing,
    AdmissionOwner,
    AdmissionSnapshot,
    CapacityDeclaration,
    ReconcileOutcome,
    ReconcileResult,
    RecoveryOutcome,
    RecoveryResult,
    ReleaseOutcome,
    ReleaseResult,
    ResourceAvailability,
    ResourceRequirement,
    StoredExecutionClaim,
    StoredResourceRecord,
    StoredRevokedIncarnation,
    TerminationConfirmation,
    UnavailableReason,
)
from superjobs.resources.store import CasStatus, ResourceAdmissionStore

if TYPE_CHECKING:
    from superjobs.resources.config import ResourceAdmissionConfig

Clock = Callable[[], datetime]

_CLEANUP_ATTEMPTS = 3


def _utc_now(clock: Clock | None = None) -> datetime:
    if clock is None:
        return datetime.now(tz=UTC)
    value = clock()
    if value.tzinfo is None:
        raise ValueError("clock must return timezone-aware datetimes")
    return value.astimezone(UTC)


def _owners_match(left: AdmissionOwner, right: AdmissionOwner) -> bool:
    return left == right


def _same_acquire_intent(left: AdmissionOwner, right: AdmissionOwner) -> bool:
    return (
        left.resource_key == right.resource_key
        and left.incarnation == right.incarnation
        and left.invocation_id == right.invocation_id
        and left.execution_id == right.execution_id
    )


class ResourceAdmission:
    """Broker-backed exclusive resource and execution-attempt admission (v1)."""

    def __init__(
        self,
        store: ResourceAdmissionStore,
        *,
        clock: Clock | None = None,
        config: ResourceAdmissionConfig | None = None,
    ) -> None:
        from superjobs.resources.config import ResourceAdmissionConfig

        self._store = store
        self._clock = clock or _utc_now
        self._config = config or ResourceAdmissionConfig()

    async def snapshot(self) -> AdmissionSnapshot:
        snapshot = await self._store.read_snapshot(evaluated_at=self._clock())
        resources = tuple(
            resource.model_copy(
                update={
                    "availability": ResourceAvailability.UNCERTAIN,
                    "reason_detail": "heartbeat_expired",
                }
            )
            if resource.availability is ResourceAvailability.OWNED
            and resource.heartbeat_at is not None
            and (snapshot.evaluated_at - resource.heartbeat_at).total_seconds()
            > self._config.heartbeat_timeout
            else resource
            for resource in snapshot.resources
        )
        return snapshot.model_copy(update={"resources": resources})

    async def declare_capacity(
        self, requirement: ResourceRequirement
    ) -> CapacityDeclaration:
        declaration = CapacityDeclaration(
            resource_key=requirement.resource_key,
            capacity=requirement.units,
        )
        now = self._clock()
        existing = await self._store.read_capacity(declaration.resource_key)
        if existing is not None:
            if existing.declaration != declaration:
                raise ResourceConflictError(
                    "conflicting capacity declaration for resource "
                    f"{declaration.resource_key!r}",
                )
            return declaration
        result = await self._store.compare_and_set_capacity(
            declaration,
            updated_at=now,
            create_only=True,
        )
        if result.status is CasStatus.CONFLICT and result.value is not None:
            if result.value.declaration != declaration:
                raise ResourceConflictError(
                    "conflicting capacity declaration for resource "
                    f"{declaration.resource_key!r}",
                )
            return declaration
        if result.status is not CasStatus.APPLIED:
            raise ResourceConflictError(
                f"could not declare capacity for resource {declaration.resource_key!r}",
            )
        resource, _ = await self._store.read_resource(declaration.resource_key)
        if resource is not None and resource.owner is not None:
            if resource.availability is not ResourceAvailability.AVAILABLE:
                raise ResourceConflictError(
                    "cannot change capacity declaration while resource is occupied",
                )
        return declaration

    def _next_generation(self, current: StoredResourceRecord | None) -> int:
        if current is None:
            return 1
        if current.owner is not None:
            return current.owner.fencing.generation + 1
        return current.last_owner_generation + 1

    def _build_owner(
        self,
        intent: AdmissionAcquireIntent,
        current: StoredResourceRecord | None,
    ) -> AdmissionOwner:
        return AdmissionOwner(
            resource_key=intent.resource_key,
            incarnation=intent.incarnation,
            invocation_id=intent.invocation_id,
            execution_id=intent.execution_id,
            fencing=AdmissionFencing(
                generation=self._next_generation(current),
                token=str(uuid4()),
            ),
        )

    async def try_acquire(self, intent: AdmissionAcquireIntent) -> AcquireResult:
        if await self._store.read_revoked_incarnation(intent.incarnation) is not None:
            return AcquireResult(outcome=AcquisitionOutcome.UNAVAILABLE)

        capacity = await self._store.read_capacity(intent.resource_key)
        if capacity is None:
            return AcquireResult(outcome=AcquisitionOutcome.NOT_DECLARED)

        current_resource, _ = await self._store.read_resource(intent.resource_key)
        if current_resource is not None:
            if current_resource.availability is ResourceAvailability.UNAVAILABLE:
                return AcquireResult(
                    outcome=AcquisitionOutcome.UNAVAILABLE,
                    resource=current_resource,
                )

        now = self._clock()
        provisional_owner = self._build_owner(intent, current_resource)
        claim_record = StoredExecutionClaim(
            execution_id=intent.execution_id,
            owner=provisional_owner,
            heartbeat_at=now,
            updated_at=now,
        )
        owner = provisional_owner
        try:
            claim_result = await self._claim_execution(claim_record)
            if claim_result.outcome is AcquisitionOutcome.ALREADY_HELD:
                held = claim_result.owner
                if held is None or not _same_acquire_intent(held, provisional_owner):
                    return AcquireResult(outcome=AcquisitionOutcome.CONTENTION)
                owner = held
            elif claim_result.outcome is not AcquisitionOutcome.ACQUIRED:
                return claim_result
            elif claim_result.owner is not None:
                owner = claim_result.owner

            resource_result = await self._claim_resource(intent, owner, now=now)
            if resource_result.outcome in {
                AcquisitionOutcome.ACQUIRED,
                AcquisitionOutcome.ALREADY_HELD,
            }:
                if not await self._dual_ownership_confirmed(owner):
                    raise ResourcePartialAcquisitionError(
                        "resource and execution claim tokens diverged after acquisition",
                        owner=owner,
                    )
                return resource_result

            await self._reconcile_failed_acquire(owner)
            return resource_result
        except asyncio.CancelledError:
            await self._bounded_acquire_cleanup(owner)
            raise
        except ResourcePartialAcquisitionError:
            raise
        except Exception:
            await self._bounded_acquire_cleanup(owner)
            raise

    async def _dual_ownership_confirmed(self, owner: AdmissionOwner) -> bool:
        resource, _ = await self._store.read_resource(owner.resource_key)
        claim, _ = await self._store.read_execution_claim(owner.execution_id)
        if resource is None or claim is None or resource.owner is None:
            return False
        return _owners_match(resource.owner, owner) and _owners_match(
            claim.owner, owner
        )

    async def _reconcile_failed_acquire(self, owner: AdmissionOwner) -> None:
        resource, _ = await self._store.read_resource(owner.resource_key)
        claim, claim_revision = await self._store.read_execution_claim(
            owner.execution_id
        )
        if resource is not None and resource.owner is not None:
            if _owners_match(resource.owner, owner):
                raise ResourcePartialAcquisitionError(
                    "exclusive resource was acquired but admission did not complete; "
                    "reconcile before retrying",
                    owner=owner,
                )
            if claim is not None and _owners_match(resource.owner, claim.owner):
                raise ResourcePartialAcquisitionError(
                    "exclusive resource was acquired but admission did not complete; "
                    "reconcile before retrying",
                    owner=claim.owner,
                )
        if claim is None or not _same_acquire_intent(claim.owner, owner):
            return
        await self._store.delete_execution_claim_if_owner(
            owner.execution_id,
            claim.owner,
            last_revision=claim_revision,
        )

    async def _bounded_acquire_cleanup(self, owner: AdmissionOwner) -> None:
        last_error: BaseException | None = None
        for _ in range(_CLEANUP_ATTEMPTS):
            try:
                await asyncio.shield(self._reconcile_failed_acquire(owner))
                return
            except ResourcePartialAcquisitionError:
                raise
            except asyncio.CancelledError:
                raise
            except BaseException as error:
                last_error = error
        if last_error is not None:
            raise ResourcePartialAcquisitionError(
                "failed to reconcile admission state after an interrupted acquire",
                owner=owner,
            ) from last_error

    async def _claim_execution(self, record: StoredExecutionClaim) -> AcquireResult:
        current, revision = await self._store.read_execution_claim(record.execution_id)
        if current is None:
            result = await self._store.compare_and_set_execution_claim(
                record,
                last_revision=None,
            )
            if result.status is CasStatus.APPLIED:
                return AcquireResult(
                    outcome=AcquisitionOutcome.ACQUIRED, owner=record.owner
                )
            if result.status is CasStatus.CONFLICT and result.value is not None:
                if _same_acquire_intent(result.value.owner, record.owner):
                    return AcquireResult(
                        outcome=AcquisitionOutcome.ALREADY_HELD,
                        owner=result.value.owner,
                    )
                return AcquireResult(outcome=AcquisitionOutcome.CONTENTION)
            return AcquireResult(outcome=AcquisitionOutcome.CONTENTION)
        if _same_acquire_intent(current.owner, record.owner):
            updated = current.model_copy(
                update={
                    "heartbeat_at": record.heartbeat_at,
                    "updated_at": record.updated_at,
                },
            )
            applied = await self._store.compare_and_set_execution_claim(
                updated,
                last_revision=revision,
            )
            if applied.status is CasStatus.APPLIED:
                return AcquireResult(
                    outcome=AcquisitionOutcome.ALREADY_HELD, owner=current.owner
                )
            return AcquireResult(outcome=AcquisitionOutcome.CONTENTION)
        return AcquireResult(outcome=AcquisitionOutcome.CONTENTION)

    async def _claim_resource(
        self,
        intent: AdmissionAcquireIntent,
        owner: AdmissionOwner,
        *,
        now: datetime,
    ) -> AcquireResult:
        current, revision = await self._store.read_resource(intent.resource_key)
        if current is not None:
            if current.availability is ResourceAvailability.UNAVAILABLE:
                return AcquireResult(
                    outcome=AcquisitionOutcome.UNAVAILABLE, resource=current
                )
            if current.availability is ResourceAvailability.UNCERTAIN:
                if current.owner is not None and not _same_acquire_intent(
                    current.owner, owner
                ):
                    return AcquireResult(
                        outcome=AcquisitionOutcome.UNCERTAIN, resource=current
                    )
            if current.owner is not None and _owners_match(current.owner, owner):
                updated = current.model_copy(
                    update={
                        "availability": ResourceAvailability.OWNED,
                        "heartbeat_at": now,
                        "updated_at": now,
                    },
                )
                applied = await self._store.compare_and_set_resource(
                    updated,
                    last_revision=revision,
                )
                if applied.status is CasStatus.APPLIED:
                    return AcquireResult(
                        outcome=AcquisitionOutcome.ALREADY_HELD,
                        owner=owner,
                        resource=updated,
                    )
                return AcquireResult(
                    outcome=AcquisitionOutcome.CONTENTION, resource=current
                )
            if current.availability in {
                ResourceAvailability.OWNED,
                ResourceAvailability.UNCERTAIN,
            }:
                return AcquireResult(
                    outcome=AcquisitionOutcome.CONTENTION, resource=current
                )

        record = StoredResourceRecord(
            resource_key=intent.resource_key,
            availability=ResourceAvailability.OWNED,
            owner=owner,
            heartbeat_at=now,
            updated_at=now,
        )
        applied = await self._store.compare_and_set_resource(
            record,
            last_revision=revision if revision > 0 else None,
        )
        if applied.status is CasStatus.APPLIED:
            return AcquireResult(
                outcome=AcquisitionOutcome.ACQUIRED,
                owner=owner,
                resource=record,
            )
        if applied.status is CasStatus.CONFLICT and applied.value is not None:
            return AcquireResult(
                outcome=AcquisitionOutcome.CONTENTION, resource=applied.value
            )
        return AcquireResult(outcome=AcquisitionOutcome.CONTENTION)

    async def heartbeat(self, owner: AdmissionOwner) -> ReconcileResult:
        return await self.reconcile(owner)

    async def execution_owner(self, execution_id: str) -> AdmissionOwner | None:
        claim, _ = await self._store.read_execution_claim(execution_id)
        return claim.owner if claim is not None else None

    async def is_declared(self, resource_key: str) -> bool:
        return await self._store.read_capacity(resource_key) is not None

    async def reconcile(self, owner: AdmissionOwner) -> ReconcileResult:
        if await self._store.read_revoked_incarnation(owner.incarnation) is not None:
            return ReconcileResult(outcome=ReconcileOutcome.NOT_OWNER)
        now = self._clock()
        claim, claim_revision = await self._store.read_execution_claim(
            owner.execution_id
        )
        if claim is None or not _owners_match(claim.owner, owner):
            return ReconcileResult(outcome=ReconcileOutcome.NOT_OWNER)
        updated_claim = claim.model_copy(
            update={"heartbeat_at": now, "updated_at": now},
        )
        claim_write = await self._store.compare_and_set_execution_claim(
            updated_claim,
            last_revision=claim_revision,
        )
        if claim_write.status is not CasStatus.APPLIED:
            return ReconcileResult(outcome=ReconcileOutcome.STALE)

        if claim.resource_released:
            return ReconcileResult(
                outcome=ReconcileOutcome.RECONCILED, execution_claim=updated_claim
            )

        resource, resource_revision = await self._store.read_resource(
            owner.resource_key
        )
        if (
            resource is None
            or resource.owner is None
            or not _owners_match(resource.owner, owner)
        ):
            return ReconcileResult(outcome=ReconcileOutcome.NOT_OWNER)
        if resource.availability is ResourceAvailability.UNAVAILABLE:
            return ReconcileResult(
                outcome=ReconcileOutcome.UNAVAILABLE, resource=resource
            )
        if resource.availability is ResourceAvailability.UNCERTAIN:
            updated_resource = resource.model_copy(
                update={
                    "availability": ResourceAvailability.OWNED,
                    "heartbeat_at": now,
                    "updated_at": now,
                },
            )
            resource_write = await self._store.compare_and_set_resource(
                updated_resource,
                last_revision=resource_revision,
            )
            if resource_write.status is not CasStatus.APPLIED:
                return ReconcileResult(
                    outcome=ReconcileOutcome.STALE, resource=resource
                )
            return ReconcileResult(
                outcome=ReconcileOutcome.RECONCILED,
                resource=updated_resource,
                execution_claim=updated_claim,
            )
        updated_resource = resource.model_copy(
            update={
                "availability": ResourceAvailability.OWNED,
                "heartbeat_at": now,
                "updated_at": now,
            },
        )
        resource_write = await self._store.compare_and_set_resource(
            updated_resource,
            last_revision=resource_revision,
        )
        if resource_write.status is not CasStatus.APPLIED:
            return ReconcileResult(outcome=ReconcileOutcome.STALE)
        return ReconcileResult(
            outcome=ReconcileOutcome.RECONCILED,
            resource=updated_resource,
            execution_claim=updated_claim,
        )

    async def mark_uncertain(self, owner: AdmissionOwner) -> ReconcileResult:
        now = self._clock()
        resource, resource_revision = await self._store.read_resource(
            owner.resource_key
        )
        if resource is None or resource.owner is None:
            return ReconcileResult(outcome=ReconcileOutcome.NOT_OWNER)
        if not _owners_match(resource.owner, owner):
            return ReconcileResult(outcome=ReconcileOutcome.STALE)
        if resource.availability is ResourceAvailability.UNAVAILABLE:
            return ReconcileResult(
                outcome=ReconcileOutcome.UNAVAILABLE, resource=resource
            )
        if resource.availability is ResourceAvailability.UNCERTAIN:
            return ReconcileResult(
                outcome=ReconcileOutcome.RECONCILED, resource=resource
            )
        updated = resource.model_copy(
            update={
                "availability": ResourceAvailability.UNCERTAIN,
                "updated_at": now,
            },
        )
        applied = await self._store.compare_and_set_resource(
            updated,
            last_revision=resource_revision,
        )
        if applied.status is CasStatus.APPLIED:
            return ReconcileResult(
                outcome=ReconcileOutcome.RECONCILED, resource=updated
            )
        return ReconcileResult(outcome=ReconcileOutcome.STALE, resource=resource)

    async def _mark_claim_resource_released(
        self,
        owner: AdmissionOwner,
        *,
        cleanup_acknowledged: bool,
    ) -> bool:
        claim, claim_revision = await self._store.read_execution_claim(
            owner.execution_id
        )
        if claim is None or not _owners_match(claim.owner, owner):
            return False
        updated = claim.model_copy(
            update={
                "resource_released": True,
                "cleanup_acknowledged": cleanup_acknowledged,
                "updated_at": self._clock(),
            },
        )
        applied = await self._store.compare_and_set_execution_claim(
            updated,
            last_revision=claim_revision,
        )
        return applied.status is CasStatus.APPLIED

    async def release_resource_after_cleanup(
        self, owner: AdmissionOwner
    ) -> ReleaseResult:
        for _ in range(8):
            result = await self._release_resource_after_cleanup_once(owner)
            if (
                result.outcome is not ReleaseOutcome.STALE
                or await self.execution_owner(owner.execution_id) != owner
            ):
                return result
        raise ResourceStoreUnavailableError(
            "Concurrent owner updates prevented cleanup release"
        )

    async def _release_resource_after_cleanup_once(
        self, owner: AdmissionOwner
    ) -> ReleaseResult:
        """Release the exclusive device while retaining the execution claim.

        Callers must persist terminal completion and then call
        :meth:`finalize_execution_claim` before another attempt may reuse the execution id.
        """
        now = self._clock()
        resource, resource_revision = await self._store.read_resource(
            owner.resource_key
        )
        claim, claim_revision = await self._store.read_execution_claim(
            owner.execution_id
        )
        if (
            claim is not None
            and _owners_match(claim.owner, owner)
            and claim.resource_released
        ):
            if (
                resource is None
                or resource.owner is None
                or not _owners_match(resource.owner, owner)
            ):
                return ReleaseResult(outcome=ReleaseOutcome.RELEASED, resource=resource)
        if resource is None:
            if claim is not None and _owners_match(claim.owner, owner):
                await self._mark_claim_resource_released(
                    owner, cleanup_acknowledged=True
                )
                released = StoredResourceRecord(
                    resource_key=owner.resource_key,
                    availability=ResourceAvailability.AVAILABLE,
                    owner=None,
                    last_owner_generation=owner.fencing.generation,
                    heartbeat_at=None,
                    updated_at=now,
                )
                return ReleaseResult(outcome=ReleaseOutcome.RELEASED, resource=released)
            return ReleaseResult(outcome=ReleaseOutcome.NOT_OWNER)
        if (
            resource.owner is None
            and resource.availability is ResourceAvailability.AVAILABLE
        ):
            if claim is not None and _owners_match(claim.owner, owner):
                await self._mark_claim_resource_released(
                    owner, cleanup_acknowledged=True
                )
                return ReleaseResult(outcome=ReleaseOutcome.RELEASED, resource=resource)
            return ReleaseResult(outcome=ReleaseOutcome.NOT_OWNER)
        if resource.owner is None or not _owners_match(resource.owner, owner):
            return ReleaseResult(outcome=ReleaseOutcome.STALE, resource=resource)

        if not await self._mark_claim_resource_released(
            owner, cleanup_acknowledged=True
        ):
            return ReleaseResult(outcome=ReleaseOutcome.STALE, resource=resource)

        released = StoredResourceRecord(
            resource_key=owner.resource_key,
            availability=ResourceAvailability.AVAILABLE,
            owner=None,
            last_owner_generation=owner.fencing.generation,
            heartbeat_at=None,
            updated_at=now,
        )
        applied = await self._store.compare_and_set_resource(
            released,
            last_revision=resource_revision,
        )
        if applied.status is not CasStatus.APPLIED:
            return ReleaseResult(outcome=ReleaseOutcome.STALE, resource=resource)
        return ReleaseResult(outcome=ReleaseOutcome.RELEASED, resource=released)

    async def finalize_execution_claim(self, owner: AdmissionOwner) -> ReleaseResult:
        for _ in range(8):
            result = await self._finalize_execution_claim_once(owner)
            if (
                result.outcome is not ReleaseOutcome.STALE
                or await self.execution_owner(owner.execution_id) != owner
            ):
                return result
            resource, _ = await self._store.read_resource(owner.resource_key)
            if resource is not None and resource.owner == owner:
                return result
        raise ResourceStoreUnavailableError(
            "Concurrent owner updates prevented claim finalization"
        )

    async def _finalize_execution_claim_once(
        self, owner: AdmissionOwner
    ) -> ReleaseResult:
        """Drop the execution claim after completion persistence (stage two)."""
        resource, _ = await self._store.read_resource(owner.resource_key)
        if (
            resource is not None
            and resource.owner is not None
            and _owners_match(resource.owner, owner)
        ):
            return ReleaseResult(outcome=ReleaseOutcome.STALE, resource=resource)
        claim, claim_revision = await self._store.read_execution_claim(
            owner.execution_id
        )
        if claim is None:
            return ReleaseResult(outcome=ReleaseOutcome.FINALIZED)
        if not _owners_match(claim.owner, owner):
            return ReleaseResult(outcome=ReleaseOutcome.STALE)
        if not claim.resource_released:
            return ReleaseResult(outcome=ReleaseOutcome.STALE)
        applied = await self._store.delete_execution_claim_if_owner(
            owner.execution_id,
            owner,
            last_revision=claim_revision,
        )
        if applied.status is CasStatus.APPLIED:
            return ReleaseResult(outcome=ReleaseOutcome.FINALIZED)
        if applied.status is CasStatus.MISSING:
            return ReleaseResult(outcome=ReleaseOutcome.FINALIZED)
        return ReleaseResult(outcome=ReleaseOutcome.STALE)

    async def release_acknowledged_cleanup(
        self, owner: AdmissionOwner
    ) -> ReleaseResult:
        """Convenience release for standalone callers that do not need a completion gap.

        Production runtimes should prefer :meth:`release_resource_after_cleanup` followed by
        :meth:`finalize_execution_claim` after durable completion is stored.
        """
        resource_result = await self.release_resource_after_cleanup(owner)
        if resource_result.outcome is not ReleaseOutcome.RELEASED:
            return resource_result
        finalized = await self.finalize_execution_claim(owner)
        if finalized.outcome is ReleaseOutcome.STALE:
            return ReleaseResult(
                outcome=ReleaseOutcome.STALE,
                resource=resource_result.resource,
            )
        return resource_result

    async def retain_cleanup_failed(
        self,
        owner: AdmissionOwner,
        *,
        reason_detail: str | None = None,
    ) -> ReconcileResult:
        now = self._clock()
        resource, resource_revision = await self._store.read_resource(
            owner.resource_key
        )
        if (
            resource is None
            or resource.owner is None
            or not _owners_match(resource.owner, owner)
        ):
            return ReconcileResult(
                outcome=ReconcileOutcome.NOT_OWNER, resource=resource
            )
        updated = resource.model_copy(
            update={
                "availability": ResourceAvailability.UNAVAILABLE,
                "unavailable_reason": UnavailableReason.CLEANUP_FAILED,
                "reason_detail": reason_detail,
                "updated_at": now,
            },
        )
        applied = await self._store.compare_and_set_resource(
            updated,
            last_revision=resource_revision,
        )
        if applied.status is not CasStatus.APPLIED:
            return ReconcileResult(outcome=ReconcileOutcome.STALE, resource=resource)
        return ReconcileResult(outcome=ReconcileOutcome.RECONCILED, resource=updated)

    async def resolve_terminated_incarnation(
        self,
        confirmation: TerminationConfirmation,
    ) -> RecoveryResult:
        now = self._clock()
        revoked = StoredRevokedIncarnation(
            incarnation=confirmation.incarnation,
            evidence=confirmation.evidence,
            revoked_at=now,
        )
        revoke_write = await self._store.compare_and_set_revoked_incarnation(
            revoked,
            create_only=True,
        )
        idempotent_revoke = False
        if revoke_write.status is CasStatus.CONFLICT and revoke_write.value is not None:
            if revoke_write.value.evidence != confirmation.evidence:
                return RecoveryResult(outcome=RecoveryOutcome.STALE)
            idempotent_revoke = True
        elif revoke_write.status is not CasStatus.APPLIED:
            return RecoveryResult(outcome=RecoveryOutcome.STALE)

        snapshot = await self.snapshot()
        released_resources: list[str] = []
        released_executions: list[str] = []
        for resource in snapshot.resources:
            owner = resource.owner
            if owner is None or owner.incarnation != confirmation.incarnation:
                continue
            current, revision = await self._store.read_resource(resource.resource_key)
            if current is None or current.owner is None:
                continue
            if current.owner != owner:
                continue
            released_record = StoredResourceRecord(
                resource_key=resource.resource_key,
                availability=ResourceAvailability.AVAILABLE,
                owner=None,
                last_owner_generation=owner.fencing.generation,
                heartbeat_at=None,
                updated_at=now,
            )
            applied = await self._store.compare_and_set_resource(
                released_record,
                last_revision=revision,
            )
            if applied.status is CasStatus.APPLIED:
                released_resources.append(resource.resource_key)

        for claim in snapshot.execution_claims:
            if claim.owner.incarnation != confirmation.incarnation:
                continue
            current, revision = await self._store.read_execution_claim(
                claim.execution_id
            )
            if current is None:
                continue
            if current.owner != claim.owner:
                continue
            applied = await self._store.delete_execution_claim_if_owner(
                claim.execution_id,
                current.owner,
                last_revision=revision,
            )
            if applied.status is CasStatus.APPLIED:
                released_executions.append(claim.execution_id)

        if not released_resources and not released_executions:
            if idempotent_revoke:
                return RecoveryResult(outcome=RecoveryOutcome.RESOLVED)
            return RecoveryResult(outcome=RecoveryOutcome.NOT_FOUND)
        return RecoveryResult(
            outcome=RecoveryOutcome.RESOLVED,
            released_resources=tuple(released_resources),
            released_executions=tuple(released_executions),
        )

    async def assert_owner_active(self, owner: AdmissionOwner) -> None:
        await self.assert_execution_owner(owner)
        resource, _ = await self._store.read_resource(owner.resource_key)
        if (
            resource is None
            or resource.owner is None
            or not _owners_match(resource.owner, owner)
        ):
            raise ResourceStaleOperationError("resource ownership token is stale")
        if resource.availability in {
            ResourceAvailability.UNCERTAIN,
            ResourceAvailability.UNAVAILABLE,
        }:
            raise ResourceStaleOperationError(
                "resource is not in an active owned state for this operation",
            )
        for _ in range(8):
            reconciled = await self.reconcile(owner)
            if reconciled.outcome is ReconcileOutcome.RECONCILED:
                return
            if await self.execution_owner(owner.execution_id) != owner:
                raise ResourceStaleOperationError(
                    "resource ownership changed during validation"
                )
        raise ResourceStoreUnavailableError(
            "Concurrent owner updates prevented resource validation"
        )

    async def assert_execution_owner(self, owner: AdmissionOwner) -> None:
        """Confirm the current claim with CAS, including after device cleanup."""
        for _ in range(8):
            if (
                await self._store.read_revoked_incarnation(owner.incarnation)
                is not None
            ):
                raise ResourceStaleOperationError("worker incarnation was terminated")
            claim, revision = await self._store.read_execution_claim(owner.execution_id)
            if claim is None or claim.owner != owner:
                raise ResourceStaleOperationError("execution claim token is stale")
            result = await self._store.compare_and_set_execution_claim(
                claim.model_copy(
                    update={"heartbeat_at": self._clock(), "updated_at": self._clock()}
                ),
                last_revision=revision,
            )
            if result.status is CasStatus.APPLIED:
                return
        raise ResourceStoreUnavailableError(
            "Concurrent owner updates prevented execution validation"
        )
