from __future__ import annotations

import asyncio
from dataclasses import dataclass
from datetime import datetime

from superjobs.resources.config import ResourceAdmissionConfig
from superjobs.resources.keys import (
    capacity_kv_key,
    execution_kv_key,
    resource_kv_key,
    revoked_incarnation_kv_key,
)
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
from superjobs.resources.store import CasResult, CasStatus
from superjobs.resources.wire import (
    decode_capacity,
    decode_execution_claim,
    decode_resource,
    decode_revoked_incarnation,
    encode_capacity,
    encode_execution_claim,
    encode_resource,
    encode_revoked_incarnation,
)


@dataclass(slots=True)
class _RevisionedValue:
    payload: bytes
    revision: int


class InMemoryResourceAdmissionStore:
    """Deterministic in-memory admission store shared across runtimes in one process."""

    def __init__(self, *, config: ResourceAdmissionConfig | None = None) -> None:
        self._config = config or ResourceAdmissionConfig()
        self._lock = asyncio.Lock()
        self._values: dict[str, _RevisionedValue] = {}
        self._revision = 0

    async def read_snapshot(self, *, evaluated_at: datetime) -> AdmissionSnapshot:
        async with self._lock:
            capacities: list[StoredCapacityRecord] = []
            resources: list[StoredResourceRecord] = []
            claims: list[StoredExecutionClaim] = []
            for key, entry in sorted(self._values.items()):
                if key.startswith("cap.v1."):
                    capacities.append(decode_capacity(entry.payload))
                elif key.startswith("res.v1."):
                    resources.append(decode_resource(entry.payload))
                elif key.startswith("exe.v1."):
                    claims.append(decode_execution_claim(entry.payload))
                elif key.startswith("rev.v1."):
                    continue
        return AdmissionSnapshot(
            evaluated_at=evaluated_at,
            capacities=tuple(capacities),
            resources=tuple(resources),
            execution_claims=tuple(claims),
        )

    async def read_capacity(self, resource_key: str) -> StoredCapacityRecord | None:
        key = capacity_kv_key(resource_key)
        async with self._lock:
            entry = self._values.get(key)
            if entry is None:
                return None
            return decode_capacity(entry.payload)

    async def compare_and_set_capacity(
        self,
        declaration: CapacityDeclaration,
        *,
        updated_at: datetime,
        create_only: bool,
    ) -> CasResult[StoredCapacityRecord]:
        key = capacity_kv_key(declaration.resource_key)
        record = StoredCapacityRecord(declaration=declaration, updated_at=updated_at)
        encoded = encode_capacity(record, self._config)
        async with self._lock:
            current = self._values.get(key)
            if create_only and current is not None:
                return CasResult(CasStatus.CONFLICT, decode_capacity(current.payload))
            if current is not None and not create_only:
                existing = decode_capacity(current.payload)
                if existing.declaration != declaration:
                    return CasResult(CasStatus.CONFLICT, existing)
            self._revision += 1
            revision = self._revision
            self._values[key] = _RevisionedValue(payload=encoded, revision=revision)
            return CasResult(CasStatus.APPLIED, record)

    async def read_resource(
        self, resource_key: str
    ) -> tuple[StoredResourceRecord | None, int]:
        key = resource_kv_key(resource_key)
        async with self._lock:
            entry = self._values.get(key)
            if entry is None:
                return None, 0
            return decode_resource(entry.payload), entry.revision

    async def compare_and_set_resource(
        self,
        record: StoredResourceRecord,
        *,
        last_revision: int | None,
    ) -> CasResult[StoredResourceRecord]:
        key = resource_kv_key(record.resource_key)
        encoded = encode_resource(record, self._config)
        async with self._lock:
            current = self._values.get(key)
            if last_revision is None:
                if current is not None:
                    return CasResult(
                        CasStatus.CONFLICT, decode_resource(current.payload)
                    )
            else:
                if current is None or current.revision != last_revision:
                    if current is None:
                        return CasResult(CasStatus.MISSING, None)
                    return CasResult(
                        CasStatus.CONFLICT, decode_resource(current.payload)
                    )
            self._revision += 1
            revision = self._revision
            self._values[key] = _RevisionedValue(payload=encoded, revision=revision)
            return CasResult(CasStatus.APPLIED, record)

    async def read_execution_claim(
        self,
        execution_id: str,
    ) -> tuple[StoredExecutionClaim | None, int]:
        key = execution_kv_key(execution_id)
        async with self._lock:
            entry = self._values.get(key)
            if entry is None:
                return None, 0
            return decode_execution_claim(entry.payload), entry.revision

    async def compare_and_set_execution_claim(
        self,
        record: StoredExecutionClaim,
        *,
        last_revision: int | None,
    ) -> CasResult[StoredExecutionClaim]:
        key = execution_kv_key(record.execution_id)
        encoded = encode_execution_claim(record, self._config)
        async with self._lock:
            current = self._values.get(key)
            if last_revision is None:
                if current is not None:
                    return CasResult(
                        CasStatus.CONFLICT, decode_execution_claim(current.payload)
                    )
            else:
                if current is None or current.revision != last_revision:
                    if current is None:
                        return CasResult(CasStatus.MISSING, None)
                    return CasResult(
                        CasStatus.CONFLICT,
                        decode_execution_claim(current.payload),
                    )
            self._revision += 1
            revision = self._revision
            self._values[key] = _RevisionedValue(payload=encoded, revision=revision)
            return CasResult(CasStatus.APPLIED, record)

    async def delete_execution_claim_if_owner(
        self,
        execution_id: str,
        owner: AdmissionOwner,
        *,
        last_revision: int,
    ) -> CasResult[None]:
        key = execution_kv_key(execution_id)
        async with self._lock:
            current = self._values.get(key)
            if current is None or current.revision != last_revision:
                if current is None:
                    return CasResult(CasStatus.MISSING, None)
                return CasResult(CasStatus.CONFLICT, None)
            claim = decode_execution_claim(current.payload)
            if claim.owner != owner:
                return CasResult(CasStatus.CONFLICT, None)
            del self._values[key]
            return CasResult(CasStatus.APPLIED, None)

    async def read_revoked_incarnation(
        self,
        incarnation: WorkerIncarnation,
    ) -> StoredRevokedIncarnation | None:
        key = revoked_incarnation_kv_key(
            incarnation.worker_id,
            incarnation.incarnation_id,
        )
        async with self._lock:
            entry = self._values.get(key)
            if entry is None:
                return None
            return decode_revoked_incarnation(entry.payload)

    async def compare_and_set_revoked_incarnation(
        self,
        record: StoredRevokedIncarnation,
        *,
        create_only: bool,
    ) -> CasResult[StoredRevokedIncarnation]:
        key = revoked_incarnation_kv_key(
            record.incarnation.worker_id,
            record.incarnation.incarnation_id,
        )
        encoded = encode_revoked_incarnation(record, self._config)
        async with self._lock:
            current = self._values.get(key)
            if create_only and current is not None:
                return CasResult(
                    CasStatus.CONFLICT, decode_revoked_incarnation(current.payload)
                )
            self._revision += 1
            revision = self._revision
            self._values[key] = _RevisionedValue(payload=encoded, revision=revision)
            return CasResult(CasStatus.APPLIED, record)
