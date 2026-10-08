"""Job contracts and payload types shared by producers and workers."""

from __future__ import annotations

from dataclasses import dataclass

from superjobs import Job


@dataclass(frozen=True, slots=True, kw_only=True)
class ManifestRequest:
    device_id: str


@dataclass(frozen=True, slots=True)
class ManifestResult:
    revision: str


@dataclass(frozen=True, slots=True)
class ManifestEvent:
    stage: str


MANIFEST_WITH_EVENTS_JOB = Job(
    "examples.contract.manifest.with_events",
    version="v1",
    request=ManifestRequest,
    result=ManifestResult,
    event=ManifestEvent,
)


@dataclass(frozen=True, slots=True, kw_only=True)
class ManifestNoEventsRequest:
    bundle_id: str


@dataclass(frozen=True, slots=True)
class ManifestNoEventsResult:
    accepted: bool


MANIFEST_NO_EVENTS_JOB = Job(
    "examples.contract.manifest.no_events",
    version="v1",
    request=ManifestNoEventsRequest,
    result=ManifestNoEventsResult,
    event=None,
)


@dataclass(frozen=True, slots=True, kw_only=True)
class TelemetrySample:
    metric: str
    value: float


TELEMETRY_INGEST_JOB = Job(
    "examples.contract.telemetry.ingest",
    version="v1",
    request=TelemetrySample,
    result=None,
    event=None,
)


@dataclass(frozen=True, slots=True)
class HeartbeatResult:
    ok: bool


HEARTBEAT_JOB = Job(
    "examples.contract.heartbeat",
    version="v1",
    request=None,
    result=HeartbeatResult,
    event=None,
)


@dataclass(frozen=True, slots=True, kw_only=True)
class CliLocalProbeRequest:
    note: str


@dataclass(frozen=True, slots=True)
class CliLocalProbeResult:
    pid: int


CLI_LOCAL_PROBE_JOB = Job(
    "examples.contract.cli.local_probe",
    version="v1",
    request=CliLocalProbeRequest,
    result=CliLocalProbeResult,
    event=None,
)


@dataclass(frozen=True, slots=True, kw_only=True)
class CliGateRequest:
    gate: str


@dataclass(frozen=True, slots=True)
class CliGateResult:
    gate: str


CLI_GATE_JOB = Job(
    "examples.contract.cli.gate",
    version="v1",
    request=CliGateRequest,
    result=CliGateResult,
    event=None,
)


@dataclass(frozen=True, slots=True, kw_only=True)
class CliFailRequest:
    reason: str


CLI_FAIL_JOB = Job(
    "examples.contract.cli.fail",
    version="v1",
    request=CliFailRequest,
    result=None,
    event=None,
)


@dataclass(frozen=True, slots=True, kw_only=True)
class LocaleCapability:
    locale: str


LOCALE_DISCOVERY_JOB = Job(
    "examples.contract.discovery.locale",
    version="v1",
    request=ManifestRequest,
    result=ManifestResult,
    capabilities=LocaleCapability,
)
