"""Pyright-positive consumer checks for shared contracts and public SuperJobs types."""

from __future__ import annotations

from typing import assert_type

from superjobs import (
    Job,
    JobCancelledOutcome,
    JobClient,
    JobContext,
    JobError,
    JobEvent,
    JobFailedOutcome,
    JobHandle,
    JobOutcome,
    JobSucceeded,
    SuperJobs,
)
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

def _as_base_job[Request, Result, Event](
    job: Job[Request, Result, Event],
) -> Job[Request, Result, Event]:
    return job


def _as_base_client[Request, Result, Event](
    client: JobClient[Request, Result, Event],
) -> JobClient[Request, Result, Event]:
    return client


def _contract_types() -> None:
    assert_type(_as_base_job(MANIFEST_WITH_EVENTS_JOB), Job[ManifestRequest, ManifestResult, ManifestEvent])
    assert_type(
        _as_base_job(MANIFEST_NO_EVENTS_JOB),
        Job[ManifestNoEventsRequest, ManifestNoEventsResult, None],
    )
    assert_type(_as_base_job(TELEMETRY_INGEST_JOB), Job[TelemetrySample, None, None])
    assert_type(_as_base_job(HEARTBEAT_JOB), Job[None, HeartbeatResult, None])


async def _client_and_handle_types(jobs: SuperJobs) -> None:
    manifest_client = jobs.client(MANIFEST_WITH_EVENTS_JOB)
    assert_type(
        _as_base_client(manifest_client),
        JobClient[ManifestRequest, ManifestResult, ManifestEvent],
    )
    handle = await manifest_client.submit(ManifestRequest(device_id="d"))
    assert_type(
        handle,
        JobHandle[ManifestRequest, ManifestResult, ManifestEvent],
    )
    assert_type(await handle.result(), ManifestResult)
    manifest_outcome = await handle.outcome()
    assert_type(manifest_outcome, JobOutcome[ManifestResult])
    if isinstance(manifest_outcome, JobSucceeded):
        assert_type(manifest_outcome.result, ManifestResult)
    elif isinstance(manifest_outcome, JobFailedOutcome):
        assert_type(manifest_outcome.error, JobError)
    elif isinstance(manifest_outcome, JobCancelledOutcome):
        assert_type(manifest_outcome.reason, str | None)

    assert_type(await manifest_client.run(ManifestRequest(device_id="d")), ManifestResult)
    assert_type(await handle, ManifestResult)

    reconstructed = await manifest_client.get(handle.id)
    assert_type(
        reconstructed,
        JobHandle[ManifestRequest, ManifestResult, ManifestEvent],
    )
    reconstructed_outcome = await reconstructed.outcome()
    assert_type(reconstructed_outcome, JobOutcome[ManifestResult])

    no_events_client = jobs.client(MANIFEST_NO_EVENTS_JOB)
    no_events_handle = await no_events_client.submit(
        ManifestNoEventsRequest(bundle_id="b"),
    )
    assert_type(
        no_events_handle,
        JobHandle[ManifestNoEventsRequest, ManifestNoEventsResult, None],
    )
    assert_type(await no_events_handle.result(), ManifestNoEventsResult)

    telemetry_client = jobs.client(TELEMETRY_INGEST_JOB)
    telemetry_handle = await telemetry_client.submit(
        TelemetrySample(metric="m", value=1.0),
    )
    assert_type(telemetry_handle, JobHandle[TelemetrySample, None, None])
    assert_type(await telemetry_handle.result(), None)
    telemetry_outcome = await telemetry_handle.outcome()
    assert_type(telemetry_outcome, JobOutcome[None])
    if isinstance(telemetry_outcome, JobSucceeded):
        assert_type(telemetry_outcome.result, None)

    heartbeat_client = jobs.client(HEARTBEAT_JOB)
    heartbeat_handle = await heartbeat_client.submit(None)
    assert_type(heartbeat_handle, JobHandle[None, HeartbeatResult, None])
    assert_type(await heartbeat_handle.result(), HeartbeatResult)


async def _event_stream_types(
    handle: JobHandle[ManifestRequest, ManifestResult, ManifestEvent],
) -> None:
    async for event in handle.events():
        assert_type(event, JobEvent[ManifestEvent])
        if isinstance(event.data, ManifestEvent):
            assert_type(event.data, ManifestEvent)


async def _context_emit_types(context: JobContext[ManifestEvent]) -> None:
    await context.emit(ManifestEvent(stage="ok"))


async def _handler_registration_forms(jobs: SuperJobs) -> None:
    sample = ManifestRequest(device_id="probe")
    manifest_context: JobContext[ManifestEvent] = JobContext(MANIFEST_WITH_EVENTS_JOB)
    quiet_context: JobContext[None] = JobContext(MANIFEST_NO_EVENTS_JOB)
    heartbeat_context: JobContext[None] = JobContext(HEARTBEAT_JOB)
    @jobs.handler(MANIFEST_WITH_EVENTS_JOB)
    async def runtime_decorated_async(
        request: ManifestRequest,
        context: JobContext[ManifestEvent],
    ) -> ManifestResult:
        return ManifestResult(revision=request.device_id)

    assert_type(await runtime_decorated_async(request=sample, context=manifest_context), ManifestResult)

    @jobs.handler(MANIFEST_WITH_EVENTS_JOB)
    def runtime_decorated_sync(
        request: ManifestRequest,
        context: JobContext[ManifestEvent],
    ) -> ManifestResult:
        return ManifestResult(revision=request.device_id)

    assert_type(runtime_decorated_sync(request=sample, context=manifest_context), ManifestResult)

    async def explicit_register_async(
        request: ManifestRequest,
        context: JobContext[ManifestEvent],
    ) -> ManifestResult:
        return ManifestResult(revision=request.device_id)

    jobs.register(MANIFEST_WITH_EVENTS_JOB, explicit_register_async)

    def explicit_register_sync(
        request: ManifestRequest,
        context: JobContext[ManifestEvent],
    ) -> ManifestResult:
        return ManifestResult(revision=request.device_id)

    jobs.register(MANIFEST_WITH_EVENTS_JOB, explicit_register_sync)

    @MANIFEST_NO_EVENTS_JOB.handler
    async def metadata_marked_async(
        request: ManifestNoEventsRequest,
        context: JobContext[None],
    ) -> ManifestNoEventsResult:
        return ManifestNoEventsResult(accepted=True)

    assert_type(
        await metadata_marked_async(
            request=ManifestNoEventsRequest(bundle_id="probe"), context=quiet_context
        ),
        ManifestNoEventsResult,
    )
    jobs.register(metadata_marked_async)

    @MANIFEST_NO_EVENTS_JOB.handler
    def metadata_marked_sync(
        request: ManifestNoEventsRequest,
        context: JobContext[None],
    ) -> ManifestNoEventsResult:
        return ManifestNoEventsResult(accepted=True)

    jobs.register(metadata_marked_sync)

    @HEARTBEAT_JOB.handler
    async def heartbeat_marked_async(context: JobContext[None]) -> HeartbeatResult:
        return HeartbeatResult(ok=True)

    assert_type(await heartbeat_marked_async(context=heartbeat_context), HeartbeatResult)
    jobs.register(heartbeat_marked_async)

    @HEARTBEAT_JOB.handler
    def heartbeat_marked_sync(context: JobContext[None]) -> HeartbeatResult:
        return HeartbeatResult(ok=True)

    assert_type(heartbeat_marked_sync(context=heartbeat_context), HeartbeatResult)
    jobs.register(heartbeat_marked_sync)

    @jobs.handler(HEARTBEAT_JOB)
    async def heartbeat_runtime_async(context: JobContext[None]) -> HeartbeatResult:
        return HeartbeatResult(ok=True)

    assert_type(await heartbeat_runtime_async(context=heartbeat_context), HeartbeatResult)

    @jobs.handler(HEARTBEAT_JOB)
    def heartbeat_runtime_sync(context: JobContext[None]) -> HeartbeatResult:
        return HeartbeatResult(ok=True)

    assert_type(heartbeat_runtime_sync(context=heartbeat_context), HeartbeatResult)

    async def telemetry_async_plain(
        request: TelemetrySample,
        context: JobContext[None],
    ) -> None:
        return None

    jobs.register(TELEMETRY_INGEST_JOB, telemetry_async_plain)
    telemetry_async_result = await telemetry_async_plain(
        TelemetrySample(metric="m", value=1.0),
        JobContext(TELEMETRY_INGEST_JOB),
    )
    assert_type(telemetry_async_result, None)

    def telemetry_sync_plain(
        request: TelemetrySample,
        context: JobContext[None],
    ) -> None:
        return None

    jobs.register(TELEMETRY_INGEST_JOB, telemetry_sync_plain)
    telemetry_sync_result = telemetry_sync_plain(
        TelemetrySample(metric="m", value=1.0),
        JobContext(TELEMETRY_INGEST_JOB),
    )
    assert_type(telemetry_sync_result, None)

    @jobs.handler(MANIFEST_WITH_EVENTS_JOB)
    async def manifest_direct_call(
        request: ManifestRequest,
        context: JobContext[ManifestEvent],
    ) -> ManifestResult:
        return ManifestResult(revision=request.device_id)

    manifest_direct_result = await manifest_direct_call(
        ManifestRequest(device_id="probe"),
        JobContext(MANIFEST_WITH_EVENTS_JOB, backend=jobs.transport),
    )
    assert_type(manifest_direct_result, ManifestResult)

    @HEARTBEAT_JOB.handler
    async def heartbeat_direct_call(context: JobContext[None]) -> HeartbeatResult:
        return HeartbeatResult(ok=True)

    heartbeat_direct_result = await heartbeat_direct_call(
        JobContext(HEARTBEAT_JOB, backend=jobs.transport),
    )
    assert_type(heartbeat_direct_result, HeartbeatResult)


async def _original_handler_signature_types(jobs: SuperJobs) -> None:
    sample = ManifestRequest(device_id="probe")
    context: JobContext[ManifestEvent] = JobContext(MANIFEST_WITH_EVENTS_JOB)
    heartbeat_context: JobContext[None] = JobContext(HEARTBEAT_JOB)

    @jobs.handler(MANIFEST_WITH_EVENTS_JOB)
    def defaults(
        payload: ManifestRequest = sample,
        ctx: JobContext[ManifestEvent] = context,
        *,
        trace: bool = False,
    ) -> ManifestResult:
        return ManifestResult(revision=payload.device_id)

    assert_type(defaults(), ManifestResult)
    assert_type(defaults(payload=sample, ctx=context, trace=True), ManifestResult)

    @MANIFEST_WITH_EVENTS_JOB.handler
    async def extras(
        payload: ManifestRequest,
        ctx: JobContext[ManifestEvent],
        *,
        label: str = "default",
    ) -> ManifestResult:
        return ManifestResult(revision=label)

    jobs.register(extras)
    assert_type(await extras(payload=sample, ctx=context), ManifestResult)
    assert_type(await extras(payload=sample, ctx=context, label="changed"), ManifestResult)

    @HEARTBEAT_JOB.handler
    def optional_context(
        ctx: JobContext[None] = heartbeat_context, *, trace: bool = False
    ) -> HeartbeatResult:
        return HeartbeatResult(ok=True)

    jobs.register(optional_context)
    assert_type(optional_context(), HeartbeatResult)
    assert_type(optional_context(ctx=heartbeat_context, trace=True), HeartbeatResult)

    @jobs.handler(TELEMETRY_INGEST_JOB)
    async def ingest(payload: TelemetrySample, ctx: JobContext[None]) -> None:
        return None

    assert_type(
        await ingest(payload=TelemetrySample(metric="m", value=1), ctx=heartbeat_context),
        None,
    )

    class SpecificResult(ManifestResult):
        pass

    @MANIFEST_WITH_EVENTS_JOB.handler
    def specific(payload: ManifestRequest, ctx: JobContext[ManifestEvent]) -> SpecificResult:
        return SpecificResult(revision=payload.device_id)

    assert_type(specific(payload=sample, ctx=context), SpecificResult)
