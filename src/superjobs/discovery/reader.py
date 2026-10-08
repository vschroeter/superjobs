from __future__ import annotations

import asyncio
from datetime import timedelta
from typing import Any, TypeVar, overload

from superjobs.discovery.capabilities import materialize_registration
from superjobs.discovery.config import DEFAULT_MAX_ENVELOPE_BYTES, PresenceConfig
from superjobs.discovery.envelope import StoredWorkerRegistration, decode_envelope_bytes
from superjobs.discovery.errors import (
    DiscoveryEnvelopeError,
    DiscoveryError,
    DiscoveryUnavailableError,
    UnsupportedDiscoveryBackendError,
)
from superjobs.discovery.memory import (
    DiscoverySnapshot,
    job_identity_from_registration_key,
    registration_key,
)
from superjobs.discovery.models import (
    NoCapability,
    RawCapabilities,
    WorkerRegistration,
    WorkerRegistrationState,
)
from superjobs.jobs.job import (
    CapabilityNoRequestJob,
    CapabilityRequestJob,
    Job,
    NoRequestJob,
    RequestJob,
)
from superjobs.job_identity import JobIdentity

CapT = TypeVar("CapT")
ReqT = TypeVar("ReqT")
FinalT = TypeVar("FinalT")
InterT = TypeVar("InterT")


def resolve_discovery_backend(backend: Any) -> Any:
    discovery = getattr(backend, "discovery_backend", None)
    if discovery is None:
        raise UnsupportedDiscoveryBackendError(
            f"{type(backend).__name__} does not support worker discovery reads",
        )
    return discovery


def _configured_read_timeout(discovery: Any) -> timedelta:
    config = getattr(discovery, "config", None)
    if isinstance(config, PresenceConfig):
        return config.read_timeout
    return PresenceConfig().read_timeout


def _configured_max_envelope_bytes(discovery: Any) -> int:
    config = getattr(discovery, "config", None)
    if isinstance(config, PresenceConfig):
        return config.max_envelope_bytes
    return DEFAULT_MAX_ENVELOPE_BYTES


async def _read_snapshot_bounded(
    discovery: Any,
    *,
    include_stale: bool,
) -> DiscoverySnapshot:
    timeout = _configured_read_timeout(discovery).total_seconds()
    try:
        return await asyncio.wait_for(
            discovery.read_snapshot(include_stale=include_stale),
            timeout=timeout,
        )
    except asyncio.CancelledError:
        raise
    except asyncio.TimeoutError as error:
        raise DiscoveryUnavailableError(
            "Discovery registry read timed out",
        ) from error
    except DiscoveryError:
        raise
    except Exception as error:
        raise DiscoveryUnavailableError(
            "Discovery registry read failed",
        ) from error


def _identity_matches(stored_job: JobIdentity, query: JobIdentity) -> bool:
    return stored_job.name == query.name and stored_job.version == query.version


def _decode_snapshot_entry(
    key: tuple[str, str, str | None],
    data: bytes,
    *,
    max_bytes: int,
) -> StoredWorkerRegistration:
    try:
        stored = decode_envelope_bytes(data, max_bytes=max_bytes)
    except DiscoveryEnvelopeError as error:
        raise error.add_context(
            worker_id=key[0],
            job=job_identity_from_registration_key(key),
        ) from error.__cause__
    expected = registration_key(stored.worker_id, stored.job)
    if key != expected:
        raise DiscoveryEnvelopeError(
            "Registration key does not match envelope identity",
            worker_id=stored.worker_id,
            job=stored.job,
        )
    return stored


def filter_active_entries(
    snapshot: DiscoverySnapshot,
    *,
    job: JobIdentity | None = None,
    include_stale: bool = False,
    max_bytes: int = DEFAULT_MAX_ENVELOPE_BYTES,
) -> list[StoredWorkerRegistration]:
    evaluated_at = snapshot.evaluated_at
    results: list[StoredWorkerRegistration] = []
    for key, data in snapshot.entries:
        stored = _decode_snapshot_entry(key, data, max_bytes=max_bytes)
        if job is not None and not _identity_matches(stored.job, job):
            continue
        if stored.state != WorkerRegistrationState.READY and not include_stale:
            continue
        if not include_stale and stored.expires_at <= evaluated_at:
            continue
        results.append(stored)
    return results


def list_offered_identities(
    snapshot: DiscoverySnapshot,
    *,
    max_bytes: int = DEFAULT_MAX_ENVELOPE_BYTES,
) -> list[JobIdentity]:
    active = filter_active_entries(snapshot, max_bytes=max_bytes)
    seen: set[tuple[str, str | None]] = set()
    identities: list[JobIdentity] = []
    for stored in active:
        token = (stored.job.name, stored.job.version)
        if token in seen:
            continue
        seen.add(token)
        identities.append(stored.job)
    identities.sort(key=lambda identity: (identity.name, identity.version or ""))
    return identities


@overload
async def list_worker_registrations(
    backend: Any,
    job: CapabilityRequestJob[Any, Any, Any, Any, CapT]
    | CapabilityNoRequestJob[Any, Any, CapT],
    *,
    include_stale: bool = False,
) -> list[WorkerRegistration[CapT]]: ...


@overload
async def list_worker_registrations(
    backend: Any,
    job: RequestJob[Any, Any, Any, Any] | NoRequestJob[Any, Any],
    *,
    include_stale: bool = False,
) -> list[WorkerRegistration[object]]: ...


@overload
async def list_worker_registrations(
    backend: Any,
    job: Job[ReqT, FinalT, InterT],
    *,
    include_stale: bool = False,
) -> list[WorkerRegistration[object]]: ...


@overload
async def list_worker_registrations(
    backend: Any,
    job: JobIdentity,
    *,
    include_stale: bool = False,
) -> list[WorkerRegistration[RawCapabilities]]: ...


async def list_worker_registrations(
    backend: Any,
    job: Job[Any, Any, Any] | JobIdentity,
    *,
    include_stale: bool = False,
) -> list[Any]:
    discovery = resolve_discovery_backend(backend)
    max_bytes = _configured_max_envelope_bytes(discovery)
    snapshot = await _read_snapshot_bounded(discovery, include_stale=include_stale)
    if isinstance(job, JobIdentity):
        query = job
        capability_type: type[Any] | type[NoCapability] | None = None
        capability_codec = None
        decode_typed = False
    else:
        query = job.identity
        capability_type = job.capability_type
        capability_codec = job.capability_codec
        decode_typed = True
    stored_entries = filter_active_entries(
        snapshot,
        job=query,
        include_stale=include_stale,
        max_bytes=max_bytes,
    )
    return [
        materialize_registration(
            stored,
            capability_type=capability_type,
            capability_codec=capability_codec if decode_typed else None,
            decode_typed=decode_typed,
        )
        for stored in stored_entries
    ]


async def list_offered_jobs(backend: Any) -> list[JobIdentity]:
    discovery = resolve_discovery_backend(backend)
    max_bytes = _configured_max_envelope_bytes(discovery)
    snapshot = await _read_snapshot_bounded(discovery, include_stale=False)
    return list_offered_identities(snapshot, max_bytes=max_bytes)
