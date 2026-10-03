"""Re-export performance Job identities for worker registration."""

from __future__ import annotations

try:
    from contracts import (
        PERFORMANCE_MANIFEST_JOB,
        PERFORMANCE_TELEMETRY_JOB,
        PerformanceManifestEvent,
        PerformanceManifestRequest,
        PerformanceManifestResult,
        PerformanceTelemetryRequest,
    )
except ModuleNotFoundError:
    from tools.performance_support.contracts import (
        PERFORMANCE_MANIFEST_JOB,
        PERFORMANCE_TELEMETRY_JOB,
        PerformanceManifestEvent,
        PerformanceManifestRequest,
        PerformanceManifestResult,
        PerformanceTelemetryRequest,
    )

__all__ = [
    "PERFORMANCE_MANIFEST_JOB",
    "PERFORMANCE_TELEMETRY_JOB",
    "PerformanceManifestEvent",
    "PerformanceManifestRequest",
    "PerformanceManifestResult",
    "PerformanceTelemetryRequest",
]
