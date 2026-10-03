"""Pyright-positive performance baseline consumer/worker annotations."""

from __future__ import annotations

from typing import assert_type

from superjobs import JobContext, JobHandle, SuperJobs

from contracts import (
    PERFORMANCE_MANIFEST_JOB,
    PERFORMANCE_TELEMETRY_JOB,
    PerformanceManifestEvent,
    PerformanceManifestRequest,
    PerformanceManifestResult,
    PerformanceTelemetryRequest,
)


async def telemetry_consumer(jobs: SuperJobs) -> None:
    client = jobs.client(PERFORMANCE_TELEMETRY_JOB)
    handle = await client.submit(
        PerformanceTelemetryRequest(
            sequence=1,
            metric="pressure",
            value=1.0,
            tags={"source": "typing", "unit": "kpa", "seq": "000000000001"},
            padding="",
        ),
    )
    assert_type(handle, JobHandle[PerformanceTelemetryRequest, None, None])
    await handle.outcome()


async def manifest_consumer(jobs: SuperJobs) -> None:
    client = jobs.client(PERFORMANCE_MANIFEST_JOB)
    handle = await client.submit(
        PerformanceManifestRequest(
            device_id="perf-device-000000000001",
            bundle_version=1,
            payload="payload",
        ),
    )
    result = await handle.result()
    assert_type(result, PerformanceManifestResult)
    async for event in handle.events():
        if isinstance(event.data, PerformanceManifestEvent):
            assert_type(event.data.stage, str)


def register_handlers(jobs: SuperJobs) -> None:
    @jobs.handler(PERFORMANCE_TELEMETRY_JOB, concurrency=1)
    async def telemetry_handler(
        request: PerformanceTelemetryRequest,
        context: JobContext[None],
    ) -> None:
        assert_type(request.sequence, int)
        return None

    @jobs.handler(PERFORMANCE_MANIFEST_JOB, concurrency=1)
    async def manifest_handler(
        request: PerformanceManifestRequest,
        context: JobContext[PerformanceManifestEvent],
    ) -> PerformanceManifestResult:
        await context.emit(PerformanceManifestEvent(stage="received"))
        return PerformanceManifestResult(revision="typing")
