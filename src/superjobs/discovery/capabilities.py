from __future__ import annotations

from typing import Any, TypeVar, cast, overload

from superjobs.discovery.errors import CapabilityDecodeError
from superjobs.discovery.envelope import StoredWorkerRegistration
from superjobs.discovery.models import (
    NoCapability,
    NoCapabilityWorkerRegistration,
    RawCapabilities,
    WorkerRegistration,
)
from superjobs.jobs.job import Job
from superjobs.job_identity import JobIdentity
from superjobs.payload.codec.payloadcodec import PayloadCodec
from superjobs.registry import registry

CapT = TypeVar("CapT")


def capability_schema_id(job: Job[Any, Any, Any]) -> str:
    capability_type = job.capability_type
    if capability_type is None:
        return ""
    codec = job.capability_codec
    if codec is None:
        codec = registry.get_payload_codec(capability_type)
    schema = codec.schema()
    if isinstance(schema, dict) and "title" in schema:
        title = schema.get("title")
        if isinstance(title, str) and title:
            return title
    return capability_type.__name__


def encode_application_capabilities(
    job: Job[Any, Any, Any],
    value: Any,
) -> RawCapabilities:
    if job.capability_type is None:
        raise TypeError(f"Job {job} does not declare application capabilities")
    codec = job.capability_codec
    if codec is None:
        codec = registry.get_payload_codec(job.capability_type)
    prepared = codec.prepare(value)
    payload = codec.encode(prepared)
    return RawCapabilities(
        schema_id=capability_schema_id(job),
        media_type=codec.media_type,
        payload=payload,
    )


def decode_application_capabilities(
    capability_type: type[Any],
    capability_codec: PayloadCodec[Any] | None,
    raw: RawCapabilities,
    *,
    worker_id: str,
    job_identity: JobIdentity,
) -> Any:
    codec = capability_codec
    if codec is None:
        codec = registry.get_payload_codec(capability_type)
    if raw.media_type != codec.media_type:
        raise CapabilityDecodeError(
            f"Capability media type {raw.media_type!r} is incompatible with "
            f"{codec.media_type!r}",
            worker_id=worker_id,
            job=job_identity,
        )
    try:
        return codec.decode(raw.payload)
    except Exception as error:
        raise CapabilityDecodeError(
            f"Capability payload for {job_identity} failed strict decoding",
            worker_id=worker_id,
            job=job_identity,
            cause=error,
        ) from error


@overload
def materialize_registration(
    stored: StoredWorkerRegistration,
    *,
    capability_type: None,
    capability_codec: PayloadCodec[Any] | None,
    decode_typed: bool,
) -> NoCapabilityWorkerRegistration: ...


@overload
def materialize_registration[CapT](
    stored: StoredWorkerRegistration,
    *,
    capability_type: type[CapT],
    capability_codec: PayloadCodec[Any] | None,
    decode_typed: bool,
) -> WorkerRegistration[CapT]: ...


def materialize_registration[CapT](
    stored: StoredWorkerRegistration,
    *,
    capability_type: type[CapT] | type[NoCapability] | None,
    capability_codec: PayloadCodec[Any] | None,
    decode_typed: bool,
) -> (
    NoCapabilityWorkerRegistration
    | WorkerRegistration[CapT]
    | WorkerRegistration[RawCapabilities]
):
    raw = stored.capabilities
    if decode_typed:
        if capability_type is None or capability_type is NoCapability:
            if raw is not None:
                raise CapabilityDecodeError(
                    f"Job {stored.job} does not declare application capabilities",
                    worker_id=stored.worker_id,
                    job=stored.job,
                )
            return NoCapabilityWorkerRegistration(
                worker_id=stored.worker_id,
                job=stored.job,
                state=stored.state,
                registered_at=stored.registered_at,
                last_seen_at=stored.last_seen_at,
                expires_at=stored.expires_at,
                capabilities=None,
                metadata=stored.metadata,
            )
        elif raw is None:
            capabilities = None
        else:
            capabilities = cast(
                CapT,
                decode_application_capabilities(
                    capability_type,
                    capability_codec,
                    raw,
                    worker_id=stored.worker_id,
                    job_identity=stored.job,
                ),
            )
        return WorkerRegistration[CapT](
            worker_id=stored.worker_id,
            job=stored.job,
            state=stored.state,
            registered_at=stored.registered_at,
            last_seen_at=stored.last_seen_at,
            expires_at=stored.expires_at,
            capabilities=capabilities,
            metadata=stored.metadata,
        )
    if raw is None:
        raw_capabilities: RawCapabilities | None = None
    else:
        raw_capabilities = raw
    return WorkerRegistration[RawCapabilities](
        worker_id=stored.worker_id,
        job=stored.job,
        state=stored.state,
        registered_at=stored.registered_at,
        last_seen_at=stored.last_seen_at,
        expires_at=stored.expires_at,
        capabilities=raw_capabilities,
        metadata=stored.metadata,
    )
