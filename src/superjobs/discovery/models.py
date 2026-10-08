from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from enum import StrEnum
from typing import Generic, TypeVar

from superjobs.job_identity import JobIdentity

CapT = TypeVar("CapT", covariant=True)


class NoCapability:
    """Marker type for Jobs without a declared application capability type."""


class WorkerRegistrationState(StrEnum):
    READY = "ready"
    DRAINING = "draining"


@dataclass(frozen=True, slots=True)
class WorkerRegistrationMetadata:
    worker_name: str | None = None
    runtime_version: str | None = None
    application_version: str | None = None
    host_label: str | None = None


@dataclass(frozen=True, slots=True)
class RawCapabilities:
    """Uninterpreted capability payload with format metadata."""

    schema_id: str
    media_type: str
    payload: bytes


@dataclass(frozen=True, slots=True)
class WorkerRegistration(Generic[CapT]):
    worker_id: str
    job: JobIdentity
    state: WorkerRegistrationState
    registered_at: datetime
    last_seen_at: datetime
    expires_at: datetime
    capabilities: CapT | None
    metadata: WorkerRegistrationMetadata


@dataclass(frozen=True, slots=True)
class NoCapabilityWorkerRegistration(WorkerRegistration[NoCapability]):
    capabilities: None
