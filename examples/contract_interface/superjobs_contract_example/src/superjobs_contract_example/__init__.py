"""Shared Job contracts for the contract-interface example."""

from superjobs_contract_example.contracts import (
    HEARTBEAT_JOB,
    MANIFEST_NO_EVENTS_JOB,
    MANIFEST_WITH_EVENTS_JOB,
    TELEMETRY_INGEST_JOB,
    HeartbeatResult,
    ManifestEvent,
    ManifestNoEventsRequest,
    ManifestNoEventsResult,
    ManifestRequest,
    ManifestResult,
    TelemetrySample,
)

__all__ = [
    "HEARTBEAT_JOB",
    "MANIFEST_NO_EVENTS_JOB",
    "MANIFEST_WITH_EVENTS_JOB",
    "TELEMETRY_INGEST_JOB",
    "HeartbeatResult",
    "ManifestEvent",
    "ManifestNoEventsRequest",
    "ManifestNoEventsResult",
    "ManifestRequest",
    "ManifestResult",
    "TelemetrySample",
]
