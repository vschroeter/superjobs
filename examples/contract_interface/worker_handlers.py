"""Worker-side handler registration (not imported by the producer entry points)."""

from __future__ import annotations

from superjobs import JobContext, SuperJobs
from superjobs_contract_example import (
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


def register_contract_handlers(jobs: SuperJobs) -> None:
    @jobs.handler(MANIFEST_WITH_EVENTS_JOB)
    async def manifest_with_events(
        request: ManifestRequest,
        context: JobContext[ManifestEvent],
    ) -> ManifestResult:
        await context.emit(ManifestEvent(stage="validated"))
        await context.emit(ManifestEvent(stage="published"))
        return ManifestResult(revision=f"{request.device_id}-r1")

    async def manifest_no_events_impl(
        request: ManifestNoEventsRequest,
        context: JobContext[None],
    ) -> ManifestNoEventsResult:
        await context.log(f"accepted bundle {request.bundle_id}")
        return ManifestNoEventsResult(accepted=True)

    jobs.register(MANIFEST_NO_EVENTS_JOB, manifest_no_events_impl)

    @TELEMETRY_INGEST_JOB.handler
    async def telemetry_ingest(
        request: TelemetrySample,
        context: JobContext[None],
    ) -> None:
        await context.log(f"telemetry {request.metric}={request.value}")
        return None

    jobs.register(telemetry_ingest)

    @HEARTBEAT_JOB.handler
    async def heartbeat(context: JobContext[None]) -> HeartbeatResult:
        await context.log("heartbeat")
        return HeartbeatResult(ok=True)

    jobs.register(heartbeat)
