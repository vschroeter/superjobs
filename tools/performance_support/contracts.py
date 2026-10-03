"""Performance baseline Job contracts (shared by producer and worker, no handlers)."""

from __future__ import annotations

from dataclasses import dataclass

from superjobs import Job


@dataclass(frozen=True, slots=True, kw_only=True)
class PerformanceTelemetryRequest:
    sequence: int
    metric: str
    value: float
    tags: dict[str, str]
    padding: str


PERFORMANCE_TELEMETRY_JOB = Job(
    "performance.baseline.telemetry",
    version="v1",
    request=PerformanceTelemetryRequest,
    result=None,
    event=None,
)


@dataclass(frozen=True, slots=True, kw_only=True)
class PerformanceManifestRequest:
    device_id: str
    bundle_version: int
    payload: str


@dataclass(frozen=True, slots=True)
class PerformanceManifestResult:
    revision: str


@dataclass(frozen=True, slots=True)
class PerformanceManifestEvent:
    stage: str


PERFORMANCE_MANIFEST_JOB = Job(
    "performance.baseline.manifest",
    version="v1",
    request=PerformanceManifestRequest,
    result=PerformanceManifestResult,
    event=PerformanceManifestEvent,
)
