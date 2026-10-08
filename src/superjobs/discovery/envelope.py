from __future__ import annotations

import base64
import json
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any, Mapping

from superjobs.discovery.config import DEFAULT_MAX_ENVELOPE_BYTES
from superjobs.discovery.errors import (
    DiscoveryEnvelopeError,
    DiscoveryEnvelopeSizeError,
    DiscoveryEnvelopeVersionError,
    DiscoveryWriteError,
)
from superjobs.discovery.models import (
    RawCapabilities,
    WorkerRegistrationMetadata,
    WorkerRegistrationState,
)
from superjobs.job_identity import JobIdentity

ENVELOPE_VERSION = 1

_TOP_LEVEL_KEYS = frozenset(
    {
        "envelope_version",
        "worker_id",
        "job",
        "state",
        "registered_at",
        "last_seen_at",
        "expires_at",
        "capabilities",
        "metadata",
        "contract_fingerprint",
    },
)
_JOB_KEYS = frozenset({"name", "version"})
_METADATA_KEYS = frozenset(
    {"worker_name", "runtime_version", "application_version", "host_label"},
)
_CAPABILITY_KEYS = frozenset({"schema_id", "media_type", "payload_b64"})


def _require_mapping(
    value: Any,
    *,
    context: str,
    worker_id: str | None = None,
    job: JobIdentity | None = None,
) -> Mapping[str, Any]:
    if not isinstance(value, dict):
        raise DiscoveryEnvelopeError(
            f"{context} must be an object",
            worker_id=worker_id,
            job=job,
        )
    return value


def _reject_unknown_keys(
    mapping: Mapping[str, Any],
    allowed: frozenset[str],
    *,
    context: str,
    worker_id: str | None = None,
    job: JobIdentity | None = None,
) -> None:
    unknown = set(mapping.keys()) - allowed
    if unknown:
        raise DiscoveryEnvelopeError(
            f"{context} contains unknown fields: {', '.join(sorted(unknown))}",
            worker_id=worker_id,
            job=job,
        )


def _parse_utc_timestamp(
    value: Any,
    *,
    field: str,
    worker_id: str | None = None,
    job: JobIdentity | None = None,
) -> datetime:
    if not isinstance(value, str):
        raise DiscoveryEnvelopeError(
            f"{field} must be an ISO-8601 UTC timestamp string",
            worker_id=worker_id,
            job=job,
        )
    normalized = value.replace("Z", "+00:00")
    try:
        parsed = datetime.fromisoformat(normalized)
    except ValueError as error:
        raise DiscoveryEnvelopeError(
            f"{field} is not a valid timestamp",
            worker_id=worker_id,
            job=job,
        ) from error
    if parsed.tzinfo is None:
        raise DiscoveryEnvelopeError(
            f"{field} must be timezone-aware UTC",
            worker_id=worker_id,
            job=job,
        )
    return parsed.astimezone(UTC)


def _parse_job_identity(
    value: Any,
    *,
    worker_id: str | None = None,
) -> JobIdentity:
    mapping = _require_mapping(value, context="job", worker_id=worker_id)
    _reject_unknown_keys(mapping, _JOB_KEYS, context="job", worker_id=worker_id)
    if "name" not in mapping:
        raise DiscoveryEnvelopeError("job.name is required", worker_id=worker_id)
    name = mapping["name"]
    version = mapping.get("version")
    if version is not None and not isinstance(version, str):
        raise DiscoveryEnvelopeError(
            "job.version must be a string or null",
            worker_id=worker_id,
        )
    if not isinstance(name, str):
        raise DiscoveryEnvelopeError("job.name must be a string", worker_id=worker_id)
    try:
        return JobIdentity(name=name, version=version)
    except ValueError as error:
        raise DiscoveryEnvelopeError(
            str(error),
            worker_id=worker_id,
        ) from error


def _parse_metadata(
    value: Any,
    *,
    worker_id: str | None = None,
    job: JobIdentity | None = None,
) -> WorkerRegistrationMetadata:
    if value is None:
        return WorkerRegistrationMetadata()
    mapping = _require_mapping(value, context="metadata", worker_id=worker_id, job=job)
    _reject_unknown_keys(mapping, _METADATA_KEYS, context="metadata", worker_id=worker_id, job=job)
    for key in _METADATA_KEYS:
        item = mapping.get(key)
        if item is not None and not isinstance(item, str):
            raise DiscoveryEnvelopeError(
                f"metadata.{key} must be a string or null",
                worker_id=worker_id,
                job=job,
            )
    return WorkerRegistrationMetadata(
        worker_name=mapping.get("worker_name"),
        runtime_version=mapping.get("runtime_version"),
        application_version=mapping.get("application_version"),
        host_label=mapping.get("host_label"),
    )


def _parse_capabilities(
    value: Any,
    *,
    worker_id: str | None = None,
    job: JobIdentity | None = None,
) -> RawCapabilities | None:
    if value is None:
        return None
    mapping = _require_mapping(
        value,
        context="capabilities",
        worker_id=worker_id,
        job=job,
    )
    _reject_unknown_keys(
        mapping,
        _CAPABILITY_KEYS,
        context="capabilities",
        worker_id=worker_id,
        job=job,
    )
    schema_id = mapping.get("schema_id")
    media_type = mapping.get("media_type")
    payload_b64 = mapping.get("payload_b64")
    if not isinstance(schema_id, str) or not schema_id:
        raise DiscoveryEnvelopeError(
            "capabilities.schema_id must be a non-empty string",
            worker_id=worker_id,
            job=job,
        )
    if not isinstance(media_type, str) or not media_type:
        raise DiscoveryEnvelopeError(
            "capabilities.media_type must be a non-empty string",
            worker_id=worker_id,
            job=job,
        )
    if not isinstance(payload_b64, str):
        raise DiscoveryEnvelopeError(
            "capabilities.payload_b64 must be a base64 string",
            worker_id=worker_id,
            job=job,
        )
    try:
        payload = base64.b64decode(payload_b64.encode("ascii"), validate=True)
    except (ValueError, UnicodeEncodeError) as error:
        raise DiscoveryEnvelopeError(
            "capabilities.payload_b64 is not valid base64",
            worker_id=worker_id,
            job=job,
        ) from error
    return RawCapabilities(schema_id=schema_id, media_type=media_type, payload=payload)


def _parse_state(
    value: Any,
    *,
    worker_id: str | None = None,
    job: JobIdentity | None = None,
) -> WorkerRegistrationState:
    if value == WorkerRegistrationState.READY:
        return WorkerRegistrationState.READY
    if value == WorkerRegistrationState.DRAINING:
        return WorkerRegistrationState.DRAINING
    raise DiscoveryEnvelopeError(
        f"state must be 'ready' or 'draining', not {value!r}",
        worker_id=worker_id,
        job=job,
    )


def _require_timezone_aware_utc_for_write(
    value: datetime,
    *,
    field: str,
    worker_id: str,
    job: JobIdentity,
) -> None:
    if value.tzinfo is None:
        raise DiscoveryWriteError(
            f"{field} must be timezone-aware UTC datetimes",
            worker_id=worker_id,
            job=job,
        )


def _validate_timestamp_ordering(
    *,
    registered_at: datetime,
    last_seen_at: datetime,
    expires_at: datetime,
    worker_id: str,
    job: JobIdentity,
) -> None:
    if not (registered_at <= last_seen_at < expires_at):
        raise DiscoveryEnvelopeError(
            "registered_at must be <= last_seen_at < expires_at",
            worker_id=worker_id,
            job=job,
        )


@dataclass(frozen=True, slots=True)
class StoredWorkerRegistration:
    worker_id: str
    job: JobIdentity
    state: WorkerRegistrationState
    registered_at: datetime
    last_seen_at: datetime
    expires_at: datetime
    capabilities: RawCapabilities | None
    metadata: WorkerRegistrationMetadata
    contract_fingerprint: str | None = None


def decode_envelope_bytes(
    data: bytes,
    *,
    max_bytes: int = DEFAULT_MAX_ENVELOPE_BYTES,
) -> StoredWorkerRegistration:
    if len(data) > max_bytes:
        raise DiscoveryEnvelopeSizeError(len(data), limit=max_bytes)
    try:
        payload = json.loads(data.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise DiscoveryEnvelopeError("Worker registration envelope is not valid UTF-8 JSON") from error
    return decode_envelope_object(payload)


def decode_envelope_object(payload: Any) -> StoredWorkerRegistration:
    mapping = _require_mapping(payload, context="envelope")
    _reject_unknown_keys(mapping, _TOP_LEVEL_KEYS, context="envelope")
    version = mapping.get("envelope_version")
    worker_id = mapping.get("worker_id")
    parsed_worker_id = worker_id if isinstance(worker_id, str) else None
    job_hint: JobIdentity | None = None
    try:
        job_hint = _parse_job_identity(mapping.get("job"), worker_id=parsed_worker_id)
    except DiscoveryEnvelopeError:
        job_hint = None
    if type(version) is not int or version != ENVELOPE_VERSION:
        raise DiscoveryEnvelopeVersionError(
            version,
            worker_id=parsed_worker_id,
            job=job_hint,
        )
    if not isinstance(worker_id, str) or not worker_id:
        raise DiscoveryEnvelopeError(
            "worker_id must be a non-empty string",
            worker_id=parsed_worker_id,
            job=job_hint,
        )
    job = _parse_job_identity(mapping.get("job"), worker_id=worker_id)
    state = _parse_state(mapping.get("state"), worker_id=worker_id, job=job)
    registered_at = _parse_utc_timestamp(
        mapping.get("registered_at"),
        field="registered_at",
        worker_id=worker_id,
        job=job,
    )
    last_seen_at = _parse_utc_timestamp(
        mapping.get("last_seen_at"),
        field="last_seen_at",
        worker_id=worker_id,
        job=job,
    )
    expires_at = _parse_utc_timestamp(
        mapping.get("expires_at"),
        field="expires_at",
        worker_id=worker_id,
        job=job,
    )
    _validate_timestamp_ordering(
        registered_at=registered_at,
        last_seen_at=last_seen_at,
        expires_at=expires_at,
        worker_id=worker_id,
        job=job,
    )
    capabilities = _parse_capabilities(
        mapping.get("capabilities"),
        worker_id=worker_id,
        job=job,
    )
    metadata = _parse_metadata(mapping.get("metadata"), worker_id=worker_id, job=job)
    contract_fingerprint = mapping.get("contract_fingerprint")
    if contract_fingerprint is not None and not isinstance(contract_fingerprint, str):
        raise DiscoveryEnvelopeError(
            "contract_fingerprint must be a string or null",
            worker_id=worker_id,
            job=job,
        )
    return StoredWorkerRegistration(
        worker_id=worker_id,
        job=job,
        state=state,
        registered_at=registered_at,
        last_seen_at=last_seen_at,
        expires_at=expires_at,
        capabilities=capabilities,
        metadata=metadata,
        contract_fingerprint=contract_fingerprint,
    )


def encode_envelope(
    registration: StoredWorkerRegistration,
    *,
    max_bytes: int = DEFAULT_MAX_ENVELOPE_BYTES,
) -> bytes:
    for field_name in ("registered_at", "last_seen_at", "expires_at"):
        _require_timezone_aware_utc_for_write(
            getattr(registration, field_name),
            field=field_name,
            worker_id=registration.worker_id,
            job=registration.job,
        )
    capabilities: dict[str, str] | None = None
    if registration.capabilities is not None:
        capabilities = {
            "schema_id": registration.capabilities.schema_id,
            "media_type": registration.capabilities.media_type,
            "payload_b64": base64.standard_b64encode(registration.capabilities.payload).decode(
                "ascii",
            ),
        }
    metadata: dict[str, str] = {}
    for field in ("worker_name", "runtime_version", "application_version", "host_label"):
        value = getattr(registration.metadata, field)
        if value is not None:
            metadata[field] = value
    body: dict[str, Any] = {
        "envelope_version": ENVELOPE_VERSION,
        "worker_id": registration.worker_id,
        "job": {
            "name": registration.job.name,
            "version": registration.job.version,
        },
        "state": registration.state.value,
        "registered_at": registration.registered_at.astimezone(UTC).isoformat(),
        "last_seen_at": registration.last_seen_at.astimezone(UTC).isoformat(),
        "expires_at": registration.expires_at.astimezone(UTC).isoformat(),
        "capabilities": capabilities,
        "metadata": metadata or None,
    }
    if registration.contract_fingerprint is not None:
        body["contract_fingerprint"] = registration.contract_fingerprint
    encoded = json.dumps(body, separators=(",", ":"), sort_keys=True).encode("utf-8")
    if len(encoded) > max_bytes:
        raise DiscoveryWriteError(
            f"Serialized worker registration exceeds {max_bytes} bytes",
            worker_id=registration.worker_id,
            job=registration.job,
        )
    return encoded


def validate_envelope_write(data: bytes, *, max_bytes: int) -> StoredWorkerRegistration:
    if len(data) > max_bytes:
        raise DiscoveryWriteError(
            f"Worker registration envelope size {len(data)} exceeds limit {max_bytes}",
        )
    try:
        return decode_envelope_bytes(data, max_bytes=max_bytes)
    except DiscoveryEnvelopeError as error:
        raise DiscoveryWriteError(
            str(error),
            worker_id=error.worker_id,
            job=error.job,
        ) from error
