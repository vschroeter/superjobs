"""Installed-wheel producer process for cross-program verification."""

from __future__ import annotations

import asyncio
import os
import sys
import time
from pathlib import Path

from faststream.nats import NatsBroker
from pydantic import ValidationError

from superjobs import (
    JobCancelled,
    JobCancelledOutcome,
    JobCompleted,
    JobFailed,
    JobFailedOutcome,
    JobLog,
    JobProgress,
    JobRetryScheduled,
    JobStarted,
    JobState,
    JobSucceeded,
    PayloadValidationError,
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

from protocol import (
    HANDLER_ENTERED,
    read_ready,
    ready_path,
    wait_for_checkpoint,
)
from runtime_isolation import assert_producer_layout
from expectations import expect_job_cancelled, expect_job_failed
from scenario_jobs import cancel_job, crash_job, failure_job, retry_job

FLOW_TIMEOUT = 30.0
READY_POLL_INTERVAL = 0.05
CRASH_RESULT_WAIT = 5.0
READINESS_CONTROL_TIMEOUT = 1.0

EXIT_OK = 0
EXIT_ASSERTION = 3
EXIT_READINESS = 2
EXIT_MALFORMED = 10
EXIT_WORKER_CRASH_CONTROL = 4

_SYSTEM_EVENT_TYPES = (
    JobStarted,
    JobCompleted,
    JobFailed,
    JobCancelled,
    JobRetryScheduled,
    JobLog,
    JobProgress,
)


class ProducerScenarioExit(BaseException):
    def __init__(self, code: int) -> None:
        self.code = code
        super().__init__(code)


def _require_env(name: str) -> str:
    value = os.environ.get(name)
    if not value:
        print(f"{name} must be set", file=sys.stderr)
        raise SystemExit(11)
    return value


async def _wait_for_worker_ready(state_dir: Path, run_id: str, timeout: float) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        path = ready_path(state_dir)
        if path.is_file():
            marker = read_ready(state_dir)
            if marker.run_id != run_id:
                raise AssertionError(
                    f"ready run_id {marker.run_id!r} != expected {run_id!r}",
                )
            return
        await asyncio.sleep(READY_POLL_INTERVAL)
    raise TimeoutError(f"worker did not become ready within {timeout}s")


def _assert_terminal_completed(events: list[object]) -> None:
    if not events:
        raise AssertionError("missing observation stream for success path")
    if not isinstance(events[-1], JobCompleted):
        raise AssertionError(f"expected JobCompleted terminal event, got {events[-1]!r}")


def _assert_no_application_events(events: list[object]) -> None:
    for event in events:
        if isinstance(event, _SYSTEM_EVENT_TYPES):
            continue
        raise AssertionError(f"unexpected application event on eventless shape: {event!r}")


async def _collect_events(handle) -> list[object]:
    return [event.data async for event in handle.events()]


async def _run_contracts_ok(jobs: SuperJobs) -> None:
    manifest_client = jobs.client(MANIFEST_WITH_EVENTS_JOB)
    manifest_handle = await manifest_client.submit(ManifestRequest(device_id="cross-dev-1"))
    manifest_result = await manifest_handle.result(wait_timeout=FLOW_TIMEOUT)
    if manifest_result != ManifestResult(revision="cross-dev-1-r1"):
        raise AssertionError(f"unexpected manifest result: {manifest_result}")
    manifest_events = await _collect_events(manifest_handle)
    app_events = [event for event in manifest_events if isinstance(event, ManifestEvent)]
    if [event.stage for event in app_events] != ["validated", "published"]:
        raise AssertionError(f"unexpected manifest events: {app_events}")
    _assert_terminal_completed(manifest_events)
    manifest_outcome = await manifest_handle.outcome(wait_timeout=FLOW_TIMEOUT)
    if not isinstance(manifest_outcome, JobSucceeded):
        raise AssertionError(f"unexpected manifest outcome: {manifest_outcome!r}")

    no_events_client = jobs.client(MANIFEST_NO_EVENTS_JOB)
    no_events_handle = await no_events_client.submit(
        ManifestNoEventsRequest(bundle_id="cross-bundle"),
    )
    no_events_result = await no_events_handle.result(wait_timeout=FLOW_TIMEOUT)
    if no_events_result != ManifestNoEventsResult(accepted=True):
        raise AssertionError(f"unexpected no-events result: {no_events_result}")
    no_events_stream = await _collect_events(no_events_handle)
    _assert_no_application_events(no_events_stream)
    _assert_terminal_completed(no_events_stream)

    telemetry_client = jobs.client(TELEMETRY_INGEST_JOB)
    telemetry_handle = await telemetry_client.submit(
        TelemetrySample(metric="pressure", value=1.0),
    )
    telemetry_result = await telemetry_handle.result(wait_timeout=FLOW_TIMEOUT)
    if telemetry_result is not None:
        raise AssertionError("telemetry job should not return a final result payload")
    telemetry_outcome = await telemetry_handle.outcome(wait_timeout=FLOW_TIMEOUT)
    if not isinstance(telemetry_outcome, JobSucceeded):
        raise AssertionError(f"unexpected telemetry outcome: {telemetry_outcome!r}")
    telemetry_stream = await _collect_events(telemetry_handle)
    _assert_no_application_events(telemetry_stream)
    _assert_terminal_completed(telemetry_stream)

    heartbeat_client = jobs.client(HEARTBEAT_JOB)
    heartbeat_handle = await heartbeat_client.submit(None)
    heartbeat_result = await heartbeat_handle.result(wait_timeout=FLOW_TIMEOUT)
    if heartbeat_result != HeartbeatResult(ok=True):
        raise AssertionError(f"unexpected heartbeat result: {heartbeat_result}")
    heartbeat_stream = await _collect_events(heartbeat_handle)
    _assert_no_application_events(heartbeat_stream)
    _assert_terminal_completed(heartbeat_stream)
    heartbeat_outcome = await heartbeat_handle.outcome(wait_timeout=FLOW_TIMEOUT)
    if not isinstance(heartbeat_outcome, JobSucceeded):
        raise AssertionError(f"unexpected heartbeat outcome: {heartbeat_outcome!r}")


async def _run_outcome_failure(jobs: SuperJobs, run_id: str) -> None:
    job = failure_job(run_id)
    handle = await jobs.client(job).submit(ManifestRequest(device_id="fail-1"))
    with expect_job_failed():
        await handle.result(wait_timeout=FLOW_TIMEOUT)
    outcome = await handle.outcome(wait_timeout=FLOW_TIMEOUT)
    if not isinstance(outcome, JobFailedOutcome):
        raise AssertionError(f"expected JobFailedOutcome, got {outcome!r}")
    if outcome.error.code != "invalid_result":
        raise AssertionError(f"unexpected failure code: {outcome.error.code}")
    events = await _collect_events(handle)
    if not isinstance(events[-1], JobFailed):
        raise AssertionError(f"expected JobFailed terminal event, got {events[-1]!r}")
    terminal = events[-1]
    assert isinstance(terminal, JobFailed)
    if terminal.error.code != "invalid_result":
        raise AssertionError(f"unexpected terminal error code: {terminal.error.code}")


async def _run_outcome_cancel(jobs: SuperJobs, run_id: str, state_dir: Path) -> None:
    job = cancel_job(run_id)
    handle = await jobs.client(job).submit(ManifestRequest(device_id="cancel-1"))
    await asyncio.to_thread(
        wait_for_checkpoint,
        state_dir,
        name=HANDLER_ENTERED,
        expected_run_id=run_id,
        expected_execution_id=handle.id,
        deadline=FLOW_TIMEOUT,
    )
    await handle.cancel()
    with expect_job_cancelled():
        await handle.result(wait_timeout=FLOW_TIMEOUT)
    outcome = await handle.outcome(wait_timeout=FLOW_TIMEOUT)
    if not isinstance(outcome, JobCancelledOutcome):
        raise AssertionError(f"expected JobCancelledOutcome, got {outcome!r}")
    events = await _collect_events(handle)
    if not isinstance(events[-1], JobCancelled):
        raise AssertionError(f"expected JobCancelled terminal event, got {events[-1]!r}")


async def _run_outcome_retry(jobs: SuperJobs, run_id: str, state_dir: Path) -> None:
    job = retry_job(run_id)
    handle = await jobs.client(job).submit(ManifestRequest(device_id="retry-1"))
    await asyncio.to_thread(
        wait_for_checkpoint,
        state_dir,
        name=HANDLER_ENTERED,
        expected_run_id=run_id,
        expected_execution_id=handle.id,
        deadline=FLOW_TIMEOUT,
    )
    result = await handle.result(wait_timeout=FLOW_TIMEOUT)
    if result != ManifestResult(revision="retry-retry-1"):
        raise AssertionError(f"unexpected retry result: {result}")
    outcome = await handle.outcome(wait_timeout=FLOW_TIMEOUT)
    if not isinstance(outcome, JobSucceeded):
        raise AssertionError(f"expected JobSucceeded outcome after retry, got {outcome!r}")
    wrapped = [event async for event in handle.events()]
    event_data = [event.data for event in wrapped]
    if not any(isinstance(event, JobRetryScheduled) for event in event_data):
        raise AssertionError("missing JobRetryScheduled event")
    started_attempts = [
        event.attempt for event in wrapped if isinstance(event.data, JobStarted)
    ]
    if started_attempts != [1, 2]:
        raise AssertionError(f"unexpected JobStarted attempts: {started_attempts}")
    _assert_terminal_completed(event_data)


async def _run_control_malformed(jobs: SuperJobs) -> None:
    client = jobs.client(MANIFEST_WITH_EVENTS_JOB)
    try:
        await client.submit(ManifestRequest(device_id=123))  # type: ignore[arg-type]
    except (PayloadValidationError, ValidationError) as exc:
        print(f"expected malformed payload rejection: {exc}", flush=True)
        raise ProducerScenarioExit(EXIT_MALFORMED)
    raise AssertionError("submit accepted malformed request payload")


async def _run_control_worker_crash(jobs: SuperJobs, run_id: str, state_dir: Path) -> None:
    job = crash_job(run_id)
    handle = await jobs.client(job).submit(ManifestRequest(device_id="crash-1"))
    await asyncio.to_thread(
        wait_for_checkpoint,
        state_dir,
        name=HANDLER_ENTERED,
        expected_run_id=run_id,
        expected_execution_id=handle.id,
        deadline=FLOW_TIMEOUT,
    )
    status = await handle.status()
    if status.state not in (JobState.PENDING, JobState.RUNNING):
        raise AssertionError(
            f"expected pending or running status after handler entry, got {status.state}",
        )
    try:
        await handle.result(wait_timeout=CRASH_RESULT_WAIT)
    except TimeoutError:
        print("expected result wait timeout after worker crash", flush=True)
        raise ProducerScenarioExit(EXIT_WORKER_CRASH_CONTROL)
    raise AssertionError("result completed after worker crash")


async def _run() -> None:
    async with asyncio.timeout(FLOW_TIMEOUT):
        assert_producer_layout(Path(__file__).resolve().parent)

        nats_url = _require_env("NATS_URL")
        run_id = _require_env("SUPERJOBS_CROSS_RUN_ID")
        scenario = _require_env("SUPERJOBS_CROSS_SCENARIO")
        state_dir = Path(_require_env("SUPERJOBS_CROSS_STATE_DIR"))

        if scenario == "control_missing_readiness":
            try:
                await _wait_for_worker_ready(
                    state_dir,
                    run_id,
                    READINESS_CONTROL_TIMEOUT,
                )
            except TimeoutError as exc:
                print(f"expected readiness timeout: {exc}", flush=True)
                raise ProducerScenarioExit(EXIT_READINESS) from exc
            raise AssertionError("worker became ready unexpectedly")

        await _wait_for_worker_ready(state_dir, run_id, FLOW_TIMEOUT)

        jobs = SuperJobs(broker=NatsBroker(nats_url, connect_timeout=5))
        await jobs.start()
        try:
            if scenario == "contracts_ok":
                await _run_contracts_ok(jobs)
            elif scenario == "outcome_failure":
                await _run_outcome_failure(jobs, run_id)
            elif scenario == "outcome_cancel":
                await _run_outcome_cancel(jobs, run_id, state_dir)
            elif scenario == "outcome_retry":
                await _run_outcome_retry(jobs, run_id, state_dir)
            elif scenario == "control_malformed_payload":
                await _run_control_malformed(jobs)
            elif scenario == "control_worker_crash":
                await _run_control_worker_crash(jobs, run_id, state_dir)
            else:
                raise AssertionError(f"unknown scenario {scenario!r}")
            print(f"producer ok scenario={scenario} run_id={run_id}", flush=True)
        finally:
            await asyncio.wait_for(jobs.stop(), timeout=FLOW_TIMEOUT)


def main() -> None:
    try:
        asyncio.run(_run())
    except ProducerScenarioExit as exc:
        raise SystemExit(exc.code) from exc
    except AssertionError as exc:
        print(str(exc), file=sys.stderr)
        raise SystemExit(EXIT_ASSERTION) from exc
    except Exception as exc:
        print(f"producer failed: {exc}", file=sys.stderr)
        raise SystemExit(1) from exc


if __name__ == "__main__":
    main()
