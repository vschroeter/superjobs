"""Deterministic in-memory run of all four contract-interface cases."""

from __future__ import annotations

import asyncio

from superjobs import InMemoryTransport, JobSucceeded, SuperJobs
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

from worker_handlers import register_contract_handlers

DEMO_TIMEOUT = 30.0


async def _run_manifest_with_events(jobs: SuperJobs) -> None:
    client = jobs.client(MANIFEST_WITH_EVENTS_JOB)
    handle = await client.submit(ManifestRequest(device_id="dev-1"))
    result = await handle.result(wait_timeout=DEMO_TIMEOUT)
    assert result == ManifestResult(revision="dev-1-r1")

    reconstructed = await jobs.client(MANIFEST_WITH_EVENTS_JOB).get(handle.id)
    events = [event async for event in reconstructed.events()]
    intermediate = [event.data for event in events if isinstance(event.data, ManifestEvent)]
    assert [event.stage for event in intermediate] == ["validated", "published"]

    outcome = await handle.outcome(wait_timeout=DEMO_TIMEOUT)
    assert isinstance(outcome, JobSucceeded)
    assert outcome.result == ManifestResult(revision="dev-1-r1")


async def _run_manifest_no_events(jobs: SuperJobs) -> None:
    client = jobs.client(MANIFEST_NO_EVENTS_JOB)
    handle = await client.submit(ManifestNoEventsRequest(bundle_id="bundle-a"))
    result = await handle.result(wait_timeout=DEMO_TIMEOUT)
    assert result == ManifestNoEventsResult(accepted=True)
    same = await client.get(handle.id)
    assert await same.result(wait_timeout=DEMO_TIMEOUT) == result


async def _run_telemetry_ingest(jobs: SuperJobs) -> None:
    client = jobs.client(TELEMETRY_INGEST_JOB)
    handle = await client.submit(TelemetrySample(metric="temp", value=21.5))
    result = await handle.result(wait_timeout=DEMO_TIMEOUT)
    assert result is None
    outcome = await handle.outcome(wait_timeout=DEMO_TIMEOUT)
    assert isinstance(outcome, JobSucceeded)
    assert outcome.result is None


async def _run_heartbeat(jobs: SuperJobs) -> None:
    client = jobs.client(HEARTBEAT_JOB)
    handle = await client.submit(None)
    result = await handle.result(wait_timeout=DEMO_TIMEOUT)
    assert result == HeartbeatResult(ok=True)
    reconstructed = await client.get(handle.id)
    assert await reconstructed.result(wait_timeout=DEMO_TIMEOUT) == HeartbeatResult(ok=True)


async def run_demo() -> None:
    async with asyncio.timeout(DEMO_TIMEOUT):
        transport = InMemoryTransport()
        jobs = SuperJobs(transport=transport)
        register_contract_handlers(jobs)
        async with jobs:
            await _run_manifest_with_events(jobs)
            await _run_manifest_no_events(jobs)
            await _run_telemetry_ingest(jobs)
            await _run_heartbeat(jobs)


def main() -> None:
    asyncio.run(run_demo())


if __name__ == "__main__":
    main()
