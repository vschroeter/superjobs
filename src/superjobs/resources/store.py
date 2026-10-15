from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from enum import StrEnum
from typing import Protocol

from superjobs.resources.models import (
    AdmissionOwner,
    AdmissionSnapshot,
    CapacityDeclaration,
    StoredCapacityRecord,
    StoredExecutionClaim,
    StoredResourceRecord,
    StoredRevokedIncarnation,
    WorkerIncarnation,
)


class CasStatus(StrEnum):
    APPLIED = "applied"
    CONFLICT = "conflict"
    MISSING = "missing"


@dataclass(frozen=True, slots=True)
class CasResult[T]:
    status: CasStatus
    value: T | None = None


class ResourceAdmissionStore(Protocol):
    async def read_snapshot(self, *, evaluated_at: datetime) -> AdmissionSnapshot: ...

    async def read_capacity(self, resource_key: str) -> StoredCapacityRecord | None: ...

    async def compare_and_set_capacity(
        self,
        declaration: CapacityDeclaration,
        *,
        updated_at: datetime,
        create_only: bool,
    ) -> CasResult[StoredCapacityRecord]: ...

    async def read_resource(
        self, resource_key: str
    ) -> tuple[StoredResourceRecord | None, int]: ...

    async def compare_and_set_resource(
        self,
        record: StoredResourceRecord,
        *,
        last_revision: int | None,
    ) -> CasResult[StoredResourceRecord]: ...

    async def read_execution_claim(
        self,
        execution_id: str,
    ) -> tuple[StoredExecutionClaim | None, int]: ...

    async def compare_and_set_execution_claim(
        self,
        record: StoredExecutionClaim,
        *,
        last_revision: int | None,
    ) -> CasResult[StoredExecutionClaim]: ...

    async def delete_execution_claim_if_owner(
        self,
        execution_id: str,
        owner: AdmissionOwner,
        *,
        last_revision: int,
    ) -> CasResult[None]: ...

    async def read_revoked_incarnation(
        self,
        incarnation: WorkerIncarnation,
    ) -> StoredRevokedIncarnation | None: ...

    async def compare_and_set_revoked_incarnation(
        self,
        record: StoredRevokedIncarnation,
        *,
        create_only: bool,
    ) -> CasResult[StoredRevokedIncarnation]: ...
