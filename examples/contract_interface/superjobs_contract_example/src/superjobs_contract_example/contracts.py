"""Job contracts and payload types shared by producers and workers."""

from __future__ import annotations

from dataclasses import dataclass

from superjobs import Job


@dataclass(frozen=True, slots=True)
class ManifestRequest:
    device_id: str


@dataclass(frozen=True, slots=True)
class ManifestResult:
    revision: str


@dataclass(frozen=True, slots=True)
class ManifestEvent:
    stage: str


MANIFEST_WITH_EVENTS_JOB: Job[ManifestRequest, ManifestResult, ManifestEvent] = Job(
    "examples.contract.manifest.with_events",
    version="v1",
    request=ManifestRequest,
    result=ManifestResult,
    event=ManifestEvent,
)


@dataclass(frozen=True, slots=True)
class ManifestNoEventsRequest:
    bundle_id: str


@dataclass(frozen=True, slots=True)
class ManifestNoEventsResult:
    accepted: bool


MANIFEST_NO_EVENTS_JOB: Job[
    ManifestNoEventsRequest,
    ManifestNoEventsResult,
    None,
] = Job(
    "examples.contract.manifest.no_events",
    version="v1",
    request=ManifestNoEventsRequest,
    result=ManifestNoEventsResult,
    event=None,
)


@dataclass(frozen=True, slots=True)
class TelemetrySample:
    metric: str
    value: float


TELEMETRY_INGEST_JOB: Job[TelemetrySample, None, None] = Job(
    "examples.contract.telemetry.ingest",
    version="v1",
    request=TelemetrySample,
    result=None,
    event=None,
)


@dataclass(frozen=True, slots=True)
class HeartbeatResult:
    ok: bool


HEARTBEAT_JOB: Job[None, HeartbeatResult, None] = Job(
    "examples.contract.heartbeat",
    version="v1",
    request=None,
    result=HeartbeatResult,
    event=None,
)
