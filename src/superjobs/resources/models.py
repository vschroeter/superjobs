from __future__ import annotations

from collections.abc import Mapping
from datetime import datetime
from enum import StrEnum
from typing import Self
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from superjobs.resources.errors import ResourceValidationError
from superjobs.resources.keys import validate_resource_key
from superjobs.jobs.job_identity import JobIdentity

V1_EXCLUSIVE_CAPACITY = 1
V1_MAX_RESOURCE_REQUIREMENTS = 1


class ResourceAvailability(StrEnum):
    AVAILABLE = "available"
    OWNED = "owned"
    UNCERTAIN = "uncertain"
    UNAVAILABLE = "unavailable"


class UnavailableReason(StrEnum):
    CLEANUP_FAILED = "cleanup_failed"
    ADMINISTRATIVE = "administrative"


class WorkerIncarnation(BaseModel):
    model_config = ConfigDict(frozen=True, strict=True)

    worker_id: str
    incarnation_id: str

    @field_validator("worker_id", "incarnation_id")
    @classmethod
    def _non_empty(cls, value: str) -> str:
        if not value:
            raise ValueError("must be a non-empty string")
        return value


class AdmissionFencing(BaseModel):
    model_config = ConfigDict(frozen=True, strict=True)

    generation: int = Field(ge=1)
    token: str

    @field_validator("token")
    @classmethod
    def _non_empty_token(cls, value: str) -> str:
        if not value:
            raise ValueError("token must be a non-empty string")
        return value


class AdmissionOwner(BaseModel):
    model_config = ConfigDict(frozen=True, strict=True)

    resource_key: str
    incarnation: WorkerIncarnation
    invocation_id: UUID
    execution_id: str
    fencing: AdmissionFencing

    @field_validator("execution_id")
    @classmethod
    def _non_empty_execution(cls, value: str) -> str:
        if not value:
            raise ValueError("execution_id must be a non-empty string")
        return value

    @model_validator(mode="after")
    def _validate_key(self) -> Self:
        validate_resource_key(self.resource_key)
        return self

    def matches_fencing(self, other: AdmissionFencing) -> bool:
        return self.fencing == other

    def same_incarnation(self, other: WorkerIncarnation) -> bool:
        return self.incarnation == other


class CapacityDeclaration(BaseModel):
    model_config = ConfigDict(frozen=True, strict=True)

    resource_key: str
    capacity: int

    @model_validator(mode="after")
    def _validate_v1_capacity(self) -> Self:
        validate_resource_key(self.resource_key)
        if self.capacity != V1_EXCLUSIVE_CAPACITY:
            raise ValueError(
                f"only exclusive capacity {V1_EXCLUSIVE_CAPACITY} is supported in v1",
            )
        return self


class ResourceRequirement(BaseModel):
    model_config = ConfigDict(frozen=True, strict=True)

    resource_key: str
    units: int

    @model_validator(mode="after")
    def _validate_v1_units(self) -> Self:
        validate_resource_key(self.resource_key)
        if self.units != V1_EXCLUSIVE_CAPACITY:
            raise ValueError(
                f"only {V1_EXCLUSIVE_CAPACITY} unit per resource is supported in v1",
            )
        return self


def parse_exclusive_resource_requirement(
    requirements: Mapping[str, int],
) -> ResourceRequirement:
    if len(requirements) != V1_MAX_RESOURCE_REQUIREMENTS:
        raise ResourceValidationError(
            f"v1 admission supports exactly {V1_MAX_RESOURCE_REQUIREMENTS} resource requirement",
        )
    resource_key, units = next(iter(requirements.items()))
    try:
        return ResourceRequirement(resource_key=resource_key, units=units)
    except ValueError as error:
        raise ResourceValidationError(str(error)) from error


class AdmissionAcquireIntent(BaseModel):
    model_config = ConfigDict(frozen=True, strict=True)

    incarnation: WorkerIncarnation
    invocation_id: UUID
    execution_id: str
    resource_key: str

    @model_validator(mode="after")
    def _validate(self) -> Self:
        validate_resource_key(self.resource_key)
        return self


class StoredCapacityRecord(BaseModel):
    model_config = ConfigDict(frozen=True, strict=True)

    declaration: CapacityDeclaration
    updated_at: datetime


class StoredResourceRecord(BaseModel):
    model_config = ConfigDict(frozen=True, strict=True)

    resource_key: str
    availability: ResourceAvailability
    owner: AdmissionOwner | None = None
    last_owner_generation: int = Field(default=0, ge=0)
    unavailable_reason: UnavailableReason | None = None
    reason_detail: str | None = None
    heartbeat_at: datetime | None = None
    updated_at: datetime

    @model_validator(mode="after")
    def _validate_state(self) -> Self:
        validate_resource_key(self.resource_key)
        if self.availability is ResourceAvailability.AVAILABLE:
            if self.owner is not None:
                raise ValueError("available resource records must not carry an owner")
        elif self.owner is None:
            raise ValueError("non-available resource records require an owner")
        if self.availability is ResourceAvailability.UNAVAILABLE:
            if self.unavailable_reason is None:
                raise ValueError(
                    "unavailable resource records require unavailable_reason"
                )
        elif self.unavailable_reason is not None:
            raise ValueError(
                "unavailable_reason is only valid for unavailable resources"
            )
        return self


class StoredExecutionClaim(BaseModel):
    model_config = ConfigDict(frozen=True, strict=True)

    execution_id: str
    owner: AdmissionOwner
    heartbeat_at: datetime
    updated_at: datetime
    resource_released: bool = False
    cleanup_acknowledged: bool = False


class AdmissionSnapshot(BaseModel):
    model_config = ConfigDict(frozen=True, strict=True)

    evaluated_at: datetime
    capacities: tuple[StoredCapacityRecord, ...]
    resources: tuple[StoredResourceRecord, ...]
    execution_claims: tuple[StoredExecutionClaim, ...]


class WaitingExecutionCompleteness(StrEnum):
    """Coverage of persisted ``waiting_for_resource`` records in a bounded scan.

    Values describe how completely the scan observed matching completion records,
    not whether every execution blocked on a resource appears in the view.
    """

    COMPLETE = "complete"
    TRUNCATED = "truncated"
    PARTIAL = "partial"


class WaitingExecutionEntry(BaseModel):
    """One Job execution observed in ``waiting_for_resource`` state."""

    model_config = ConfigDict(frozen=True, strict=True)

    execution_id: str
    job: JobIdentity
    created_at: datetime | None = None


class WaitingExecutionView(BaseModel):
    """Bounded, read-only view of persisted resource-waiting completion records.

    Scan limits do not imply constant peak memory on NATS (full KV key listing) or
    in-memory transports (all execution records copied before truncation).
    """

    model_config = ConfigDict(frozen=True, strict=True)

    evaluated_at: datetime
    completeness: WaitingExecutionCompleteness
    entries: tuple[WaitingExecutionEntry, ...]


class AcquisitionOutcome(StrEnum):
    ACQUIRED = "acquired"
    ALREADY_HELD = "already_held"
    CONTENTION = "contention"
    UNAVAILABLE = "unavailable"
    UNCERTAIN = "uncertain"
    STALE = "stale"
    NOT_DECLARED = "not_declared"


class AcquireResult(BaseModel):
    model_config = ConfigDict(frozen=True, strict=True)

    outcome: AcquisitionOutcome
    owner: AdmissionOwner | None = None
    resource: StoredResourceRecord | None = None


class TerminationConfirmation(BaseModel):
    model_config = ConfigDict(frozen=True, strict=True)

    incarnation: WorkerIncarnation
    confirmed: bool
    evidence: str

    @model_validator(mode="after")
    def _require_confirmation(self) -> Self:
        if not self.confirmed:
            raise ValueError("termination resolution requires explicit confirmation")
        if not self.evidence.strip():
            raise ValueError(
                "termination confirmation requires a non-empty evidence reference"
            )
        return self


class StoredRevokedIncarnation(BaseModel):
    model_config = ConfigDict(frozen=True, strict=True)

    incarnation: WorkerIncarnation
    evidence: str
    revoked_at: datetime


class ReleaseOutcome(StrEnum):
    RELEASED = "released"
    FINALIZED = "finalized"
    STALE = "stale"
    NOT_OWNER = "not_owner"


class ReleaseResult(BaseModel):
    model_config = ConfigDict(frozen=True, strict=True)

    outcome: ReleaseOutcome
    resource: StoredResourceRecord | None = None


class ReconcileOutcome(StrEnum):
    RECONCILED = "reconciled"
    STALE = "stale"
    NOT_OWNER = "not_owner"
    UNAVAILABLE = "unavailable"


class ReconcileResult(BaseModel):
    model_config = ConfigDict(frozen=True, strict=True)

    outcome: ReconcileOutcome
    resource: StoredResourceRecord | None = None
    execution_claim: StoredExecutionClaim | None = None


class RecoveryOutcome(StrEnum):
    RESOLVED = "resolved"
    STALE = "stale"
    NOT_FOUND = "not_found"


class RecoveryResult(BaseModel):
    model_config = ConfigDict(frozen=True, strict=True)

    outcome: RecoveryOutcome
    released_resources: tuple[str, ...] = ()
    released_executions: tuple[str, ...] = ()
