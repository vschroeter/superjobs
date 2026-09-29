"""NATS producer entry point (imports shared contracts only, not worker code)."""

from __future__ import annotations

import asyncio
import os
import time
from pathlib import Path

from faststream.nats import NatsBroker

from superjobs import JobSucceeded, SuperJobs
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

FLOW_TIMEOUT = 30.0
READY_POLL_INTERVAL = 0.2


def _require_env(name: str) -> str:
    value = os.environ.get(name)
    if not value:
        raise SystemExit(f"{name} must be set")
    return value


async def _wait_for_worker_ready(ready_path: Path, timeout: float) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if ready_path.is_file() and ready_path.read_text(encoding="utf-8") == "ready":
            return
        await asyncio.sleep(READY_POLL_INTERVAL)
    raise TimeoutError(f"worker did not become ready within {timeout}s")


async def _run() -> None:
    async with asyncio.timeout(FLOW_TIMEOUT):
        nats_url = _require_env("NATS_URL")
        ready_path = Path(_require_env("SUPERJOBS_EXAMPLE_READY_FILE"))
        await _wait_for_worker_ready(ready_path, FLOW_TIMEOUT)

        jobs = SuperJobs(
            broker=NatsBroker(nats_url, connect_timeout=5),
        )
        await jobs.start()
        try:
            manifest_client = jobs.client(MANIFEST_WITH_EVENTS_JOB)
            manifest_handle = await manifest_client.submit(
                ManifestRequest(device_id="nats-dev-1"),
            )
            manifest_result = await manifest_handle.result(wait_timeout=FLOW_TIMEOUT)
            if manifest_result != ManifestResult(revision="nats-dev-1-r1"):
                raise AssertionError(f"unexpected manifest result: {manifest_result}")
            events = [
                event.data
                async for event in manifest_handle.events()
                if isinstance(event.data, ManifestEvent)
            ]
            if [event.stage for event in events] != ["validated", "published"]:
                raise AssertionError(f"unexpected manifest events: {events}")

            no_events_client = jobs.client(MANIFEST_NO_EVENTS_JOB)
            no_events_handle = await no_events_client.submit(
                ManifestNoEventsRequest(bundle_id="nats-bundle"),
            )
            no_events_result = await no_events_handle.result(wait_timeout=FLOW_TIMEOUT)
            if no_events_result != ManifestNoEventsResult(accepted=True):
                raise AssertionError(f"unexpected no-events result: {no_events_result}")

            telemetry_client = jobs.client(TELEMETRY_INGEST_JOB)
            telemetry_handle = await telemetry_client.submit(
                TelemetrySample(metric="pressure", value=1.0),
            )
            telemetry_result = await telemetry_handle.result(wait_timeout=FLOW_TIMEOUT)
            if telemetry_result is not None:
                raise AssertionError("telemetry job should not return a final result payload")
            telemetry_outcome = await telemetry_handle.outcome(wait_timeout=FLOW_TIMEOUT)
            if not isinstance(telemetry_outcome, JobSucceeded):
                raise AssertionError(f"unexpected telemetry outcome: {telemetry_outcome}")

            heartbeat_client = jobs.client(HEARTBEAT_JOB)
            heartbeat_handle = await heartbeat_client.submit(None)
            heartbeat_result = await heartbeat_handle.result(wait_timeout=FLOW_TIMEOUT)
            if heartbeat_result != HeartbeatResult(ok=True):
                raise AssertionError(f"unexpected heartbeat result: {heartbeat_result}")

            print("producer completed all contract cases", flush=True)
        finally:
            await asyncio.wait_for(jobs.stop(), timeout=FLOW_TIMEOUT)


def main() -> None:
    asyncio.run(_run())


if __name__ == "__main__":
    main()
