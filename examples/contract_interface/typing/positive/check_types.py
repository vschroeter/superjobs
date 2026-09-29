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


def _contract_types() -> None:
    assert_type(MANIFEST_WITH_EVENTS_JOB, Job[ManifestRequest, ManifestResult, ManifestEvent])
    assert_type(
        MANIFEST_NO_EVENTS_JOB,
        Job[ManifestNoEventsRequest, ManifestNoEventsResult, None],
    )
    assert_type(TELEMETRY_INGEST_JOB, Job[TelemetrySample, None, None])
    assert_type(HEARTBEAT_JOB, Job[None, HeartbeatResult, None])


async def _client_and_handle_types(jobs: SuperJobs) -> None:
    manifest_client = jobs.client(MANIFEST_WITH_EVENTS_JOB)
    assert_type(
        manifest_client,
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
