"""Pyright-negative performance baseline consumer checks."""

from __future__ import annotations

from superjobs import SuperJobs

from contracts import (
    PERFORMANCE_MANIFEST_JOB,
    PERFORMANCE_TELEMETRY_JOB,
    PerformanceManifestRequest,
)


async def wrong_telemetry_request(jobs: SuperJobs) -> None:
    client = jobs.client(PERFORMANCE_TELEMETRY_JOB)
    await client.submit("not a telemetry request")  # expect: reportArgumentType


async def incompatible_manifest_result(jobs: SuperJobs) -> None:
    client = jobs.client(PERFORMANCE_MANIFEST_JOB)
    handle = await client.submit(
        PerformanceManifestRequest(
            device_id="perf-device-000000000001",
            bundle_version=1,
            payload="payload",
        ),
    )
    text: str = await handle.result()  # expect: reportAssignmentType
