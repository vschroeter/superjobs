"""Worker-side handlers for performance baselines (never imported by producer)."""

from __future__ import annotations

import asyncio
import os

from superjobs import JobContext, SuperJobs
from superjobs.exceptions.jobs import InvalidResultError

from config import MANIFEST_EVENT_STAGES, MANIFEST_EXPECTED_REVISION, worker_mode_from_env
from contracts import (
    PERFORMANCE_MANIFEST_JOB,
    PERFORMANCE_TELEMETRY_JOB,
    PerformanceManifestEvent,
    PerformanceManifestRequest,
    PerformanceManifestResult,
    PerformanceTelemetryRequest,
)


def _delay_seconds() -> float:
    raw = os.environ.get("SUPERJOBS_PERF_HANDLER_DELAY_SECONDS", "0.05")
    return float(raw)


def register_performance_handlers(jobs: SuperJobs, *, concurrency: int) -> None:
    mode = worker_mode_from_env()

    @jobs.handler(PERFORMANCE_TELEMETRY_JOB, concurrency=concurrency)
    async def telemetry_handler(
        request: PerformanceTelemetryRequest,
        context: JobContext[None],
    ) -> None:
        await _run_mode(mode, context)
        return None

    @jobs.handler(PERFORMANCE_MANIFEST_JOB, concurrency=concurrency)
    async def manifest_handler(
        request: PerformanceManifestRequest,
        context: JobContext[PerformanceManifestEvent],
    ) -> PerformanceManifestResult:
        await _run_mode(mode, context)
        for stage in MANIFEST_EVENT_STAGES:
            await context.emit(PerformanceManifestEvent(stage=stage))
        return PerformanceManifestResult(revision=MANIFEST_EXPECTED_REVISION)


async def _run_mode(mode: str, context: JobContext) -> None:
    if mode == "failure":
        raise InvalidResultError("performance baseline forced failure")
    if mode == "delay":
        await asyncio.sleep(_delay_seconds())
    if mode == "incomplete":
        while True:
            await context.check_cancelled()
            await asyncio.sleep(0.05)
