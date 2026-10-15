from __future__ import annotations

import json
from datetime import UTC, datetime
from typing import Any, Mapping
from uuid import UUID

from superjobs.resources.config import ResourceAdmissionConfig
from superjobs.resources.errors import ResourceValidationError
from superjobs.resources.models import (
    AdmissionFencing,
    AdmissionOwner,
    CapacityDeclaration,
    ResourceAvailability,
    StoredCapacityRecord,
    StoredExecutionClaim,
    StoredResourceRecord,
    StoredRevokedIncarnation,
    UnavailableReason,
    WorkerIncarnation,
)

WIRE_VERSION = 1


def _require_utc(value: datetime, *, field: str) -> datetime:
    if value.tzinfo is None:
        raise ResourceValidationError(f"{field} must be timezone-aware")
    return value.astimezone(UTC)


def _encode_datetime(value: datetime) -> str:
    return _require_utc(value, field="timestamp").isoformat()


def _decode_datetime(value: Any, *, field: str) -> datetime:
    if not isinstance(value, str):
        raise ResourceValidationError(f"{field} must be an ISO-8601 string")
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError as error:
        raise ResourceValidationError(
            f"{field} is not a valid ISO-8601 timestamp"
        ) from error
    return _require_utc(parsed, field=field)


def _encode_owner(owner: AdmissionOwner) -> Mapping[str, Any]:
    return {
        "resource_key": owner.resource_key,
        "worker_id": owner.incarnation.worker_id,
        "incarnation_id": owner.incarnation.incarnation_id,
        "invocation_id": str(owner.invocation_id),
        "execution_id": owner.execution_id,
        "generation": owner.fencing.generation,
        "token": owner.fencing.token,
    }


def _decode_invocation_id(value: Any) -> UUID:
    if isinstance(value, UUID):
        return value
    if isinstance(value, str):
        return UUID(value)
    raise TypeError("invocation_id must be a UUID or UUID string")


def _decode_owner(payload: Mapping[str, Any]) -> AdmissionOwner:
    try:
        return AdmissionOwner(
            resource_key=payload["resource_key"],
            incarnation=WorkerIncarnation(
                worker_id=payload["worker_id"],
                incarnation_id=payload["incarnation_id"],
            ),
            invocation_id=_decode_invocation_id(payload["invocation_id"]),
            execution_id=payload["execution_id"],
            fencing=AdmissionFencing(
                generation=payload["generation"],
                token=payload["token"],
            ),
        )
    except (KeyError, TypeError, ValueError) as error:
        raise ResourceValidationError("admission owner record is malformed") from error


def encode_capacity(
    record: StoredCapacityRecord, config: ResourceAdmissionConfig
) -> bytes:
    body = {
        "wire_version": WIRE_VERSION,
        "resource_key": record.declaration.resource_key,
        "capacity": record.declaration.capacity,
        "updated_at": _encode_datetime(record.updated_at),
    }
    encoded = json.dumps(body, separators=(",", ":"), sort_keys=True).encode("utf-8")
    if len(encoded) > config.max_record_bytes:
        raise ResourceValidationError("capacity record exceeds configured size limit")
    return encoded


def decode_capacity(payload: bytes) -> StoredCapacityRecord:
    try:
        body = json.loads(payload.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise ResourceValidationError("capacity record is not valid JSON") from error
    if body.get("wire_version") != WIRE_VERSION:
        raise ResourceValidationError("unsupported capacity record wire version")
    try:
        declaration = CapacityDeclaration(
            resource_key=body["resource_key"],
            capacity=body["capacity"],
        )
        return StoredCapacityRecord(
            declaration=declaration,
            updated_at=_decode_datetime(body["updated_at"], field="updated_at"),
        )
    except (KeyError, TypeError, ValueError) as error:
        raise ResourceValidationError("capacity record is malformed") from error


def encode_resource(
    record: StoredResourceRecord, config: ResourceAdmissionConfig
) -> bytes:
    body: dict[str, Any] = {
        "wire_version": WIRE_VERSION,
        "resource_key": record.resource_key,
        "availability": record.availability.value,
        "last_owner_generation": record.last_owner_generation,
        "updated_at": _encode_datetime(record.updated_at),
    }
    if record.owner is not None:
        body["owner"] = _encode_owner(record.owner)
    if record.heartbeat_at is not None:
        body["heartbeat_at"] = _encode_datetime(record.heartbeat_at)
    if record.unavailable_reason is not None:
        body["unavailable_reason"] = record.unavailable_reason.value
    if record.reason_detail is not None:
        body["reason_detail"] = record.reason_detail
    encoded = json.dumps(body, separators=(",", ":"), sort_keys=True).encode("utf-8")
    if len(encoded) > config.max_record_bytes:
        raise ResourceValidationError("resource record exceeds configured size limit")
    return encoded


def decode_resource(payload: bytes) -> StoredResourceRecord:
    try:
        body = json.loads(payload.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise ResourceValidationError("resource record is not valid JSON") from error
    if body.get("wire_version") != WIRE_VERSION:
        raise ResourceValidationError("unsupported resource record wire version")
    owner_payload = body.get("owner")
    owner = _decode_owner(owner_payload) if owner_payload is not None else None
    heartbeat_raw = body.get("heartbeat_at")
    heartbeat_at = (
        _decode_datetime(heartbeat_raw, field="heartbeat_at")
        if heartbeat_raw is not None
        else None
    )
    availability_raw = body["availability"]
    if isinstance(availability_raw, str):
        availability = ResourceAvailability(availability_raw)
    else:
        availability = availability_raw
    reason_raw = body.get("unavailable_reason")
    unavailable_reason = (
        UnavailableReason(reason_raw) if isinstance(reason_raw, str) else reason_raw
    )
    try:
        return StoredResourceRecord(
            resource_key=body["resource_key"],
            availability=availability,
            owner=owner,
            last_owner_generation=int(body.get("last_owner_generation", 0)),
            unavailable_reason=unavailable_reason,
            reason_detail=body.get("reason_detail"),
            heartbeat_at=heartbeat_at,
            updated_at=_decode_datetime(body["updated_at"], field="updated_at"),
        )
    except (KeyError, TypeError, ValueError) as error:
        raise ResourceValidationError("resource record is malformed") from error


def encode_execution_claim(
    record: StoredExecutionClaim, config: ResourceAdmissionConfig
) -> bytes:
    body = {
        "wire_version": WIRE_VERSION,
        "execution_id": record.execution_id,
        "owner": _encode_owner(record.owner),
        "heartbeat_at": _encode_datetime(record.heartbeat_at),
        "updated_at": _encode_datetime(record.updated_at),
        "resource_released": record.resource_released,
        "cleanup_acknowledged": record.cleanup_acknowledged,
    }
    encoded = json.dumps(body, separators=(",", ":"), sort_keys=True).encode("utf-8")
    if len(encoded) > config.max_record_bytes:
        raise ResourceValidationError(
            "execution claim record exceeds configured size limit"
        )
    return encoded


def encode_revoked_incarnation(
    record: StoredRevokedIncarnation,
    config: ResourceAdmissionConfig,
) -> bytes:
    body = {
        "wire_version": WIRE_VERSION,
        "worker_id": record.incarnation.worker_id,
        "incarnation_id": record.incarnation.incarnation_id,
        "evidence": record.evidence,
        "revoked_at": _encode_datetime(record.revoked_at),
    }
    encoded = json.dumps(body, separators=(",", ":"), sort_keys=True).encode("utf-8")
    if len(encoded) > config.max_record_bytes:
        raise ResourceValidationError(
            "revoked incarnation record exceeds configured size limit"
        )
    return encoded


def decode_revoked_incarnation(payload: bytes) -> StoredRevokedIncarnation:
    try:
        body = json.loads(payload.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise ResourceValidationError(
            "revoked incarnation record is not valid JSON"
        ) from error
    if body.get("wire_version") != WIRE_VERSION:
        raise ResourceValidationError("unsupported revoked incarnation wire version")
    try:
        return StoredRevokedIncarnation(
            incarnation=WorkerIncarnation(
                worker_id=body["worker_id"],
                incarnation_id=body["incarnation_id"],
            ),
            evidence=body["evidence"],
            revoked_at=_decode_datetime(body["revoked_at"], field="revoked_at"),
        )
    except (KeyError, TypeError, ValueError) as error:
        raise ResourceValidationError(
            "revoked incarnation record is malformed"
        ) from error


def decode_execution_claim(payload: bytes) -> StoredExecutionClaim:
    try:
        body = json.loads(payload.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise ResourceValidationError(
            "execution claim record is not valid JSON"
        ) from error
    if body.get("wire_version") != WIRE_VERSION:
        raise ResourceValidationError("unsupported execution claim wire version")
    try:
        return StoredExecutionClaim(
            execution_id=body["execution_id"],
            owner=_decode_owner(body["owner"]),
            heartbeat_at=_decode_datetime(body["heartbeat_at"], field="heartbeat_at"),
            updated_at=_decode_datetime(body["updated_at"], field="updated_at"),
            resource_released=bool(body.get("resource_released", False)),
            cleanup_acknowledged=bool(body.get("cleanup_acknowledged", False)),
        )
    except (KeyError, TypeError, ValueError) as error:
        raise ResourceValidationError("execution claim record is malformed") from error
