"""Worker-side handler registration (never imported by producer entry points)."""

from __future__ import annotations

import asyncio
import os
from pathlib import Path

from superjobs import JobContext, SuperJobs
from superjobs.exceptions.jobs import InvalidResultError
from superjobs.jobs.retry_policy import FixedBackoff, RetryPolicy
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

from protocol import HANDLER_ENTERED, write_checkpoint
from scenario_jobs import cancel_job, crash_job, failure_job, retry_job


def _checkpoint_dir() -> Path:
    return Path(os.environ["SUPERJOBS_CROSS_STATE_DIR"])


def _run_id() -> str:
    return os.environ["SUPERJOBS_CROSS_RUN_ID"]


def _scenario() -> str:
    return os.environ["SUPERJOBS_CROSS_SCENARIO"]


def _signal_handler_entered(job_name: str, context: JobContext[None]) -> None:
    write_checkpoint(
        _checkpoint_dir(),
        run_id=_run_id(),
        checkpoint=HANDLER_ENTERED,
        job=job_name,
        execution_id=context.id,
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


def register_scenario_handlers(jobs: SuperJobs, run_id: str) -> None:
    scenario = _scenario()
    state = {"retry_calls": 0}

    if scenario == "outcome_failure":
        job = failure_job(run_id)

        @jobs.handler(job)
        async def failure_handler(
            request: ManifestRequest,
            context: JobContext[None],
        ) -> ManifestResult:
            _signal_handler_entered(job.name, context)
            raise InvalidResultError()

    if scenario == "outcome_cancel":
        job = cancel_job(run_id)

        @jobs.handler(job)
        async def cancel_handler(
            request: ManifestRequest,
            context: JobContext[None],
        ) -> ManifestResult:
            _signal_handler_entered(job.name, context)
            while True:
                await context.check_cancelled()
                await asyncio.sleep(0.01)

    if scenario == "outcome_retry":
        job = retry_job(run_id)

        @jobs.handler(
            job,
            retry=RetryPolicy(max_attempts=2, backoff=FixedBackoff(0.01)),
        )
        async def retry_handler(
            request: ManifestRequest,
            context: JobContext[None],
        ) -> ManifestResult:
            state["retry_calls"] += 1
            if state["retry_calls"] == 1:
                _signal_handler_entered(job.name, context)
                raise RuntimeError("temporary failure for retry proof")
            return ManifestResult(revision=f"retry-{request.device_id}")

    if scenario == "control_worker_crash":
        job = crash_job(run_id)

        @jobs.handler(job)
        async def crash_handler(
            request: ManifestRequest,
            context: JobContext[None],
        ) -> ManifestResult:
            _signal_handler_entered(job.name, context)
            os._exit(42)
