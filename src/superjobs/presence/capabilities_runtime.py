from __future__ import annotations

import inspect
from collections.abc import Awaitable, Callable
from typing import Any

from superjobs.discovery.capabilities import (
    decode_application_capabilities,
    encode_application_capabilities,
)
from superjobs.discovery.envelope import (
    StoredWorkerRegistration,
    decode_envelope_bytes,
    encode_envelope,
)
from superjobs.discovery.models import RawCapabilities
from superjobs.jobs.job import Job
from superjobs.registry import registry


def is_capability_factory(job: Job[Any, Any, Any], spec: Any) -> bool:
    if spec is None:
        return False
    if job.capability_type is not None and isinstance(spec, job.capability_type):
        return False
    return callable(spec)


async def evaluate_capability_spec(job: Job[Any, Any, Any], spec: Any) -> Any | None:
    if spec is None:
        return None
    if not is_capability_factory(job, spec):
        return spec
    result = spec()
    if inspect.isawaitable(result):
        result = await result
    return result


def validate_capability_value(job: Job[Any, Any, Any], value: Any | None) -> Any | None:
    if value is None:
        return None
    if job.capability_type is None:
        raise TypeError(f"Job {job} does not declare application capabilities")
    codec = job.capability_codec
    if codec is None:
        codec = registry.get_payload_codec(job.capability_type)
    codec.prepare(value)
    return value


def encode_validated_capabilities(
    job: Job[Any, Any, Any],
    value: Any | None,
) -> RawCapabilities | None:
    if value is None:
        return None
    return encode_application_capabilities(job, value)


def strict_decode_capabilities(
    job: Job[Any, Any, Any],
    raw: RawCapabilities,
    *,
    worker_id: str,
) -> Any:
    if job.capability_type is None:
        raise TypeError(f"Job {job} does not declare application capabilities")
    return decode_application_capabilities(
        job.capability_type,
        job.capability_codec,
        raw,
        worker_id=worker_id,
        job_identity=job.identity,
    )


def prepare_registration_wire(
    job: Job[Any, Any, Any],
    registration: StoredWorkerRegistration,
    worker_id: str,
    *,
    max_envelope_bytes: int,
) -> bytes:
    if registration.capabilities is not None:
        strict_decode_capabilities(job, registration.capabilities, worker_id=worker_id)
    encoded = encode_envelope(registration, max_bytes=max_envelope_bytes)
    decode_envelope_bytes(encoded, max_bytes=max_envelope_bytes)
    return encoded


async def materialize_capabilities(
    job: Job[Any, Any, Any],
    spec: Any,
) -> tuple[Any | None, RawCapabilities | None]:
    raw_value = await evaluate_capability_spec(job, spec)
    validated = validate_capability_value(job, raw_value)
    encoded = encode_validated_capabilities(job, validated)
    return validated, encoded
