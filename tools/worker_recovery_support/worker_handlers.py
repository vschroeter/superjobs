"""Worker-side recovery handlers (never imported by producer entry points)."""

from __future__ import annotations

import asyncio
import os
from pathlib import Path

from superjobs import JobContext, SuperJobs
from superjobs_contract_example import ManifestRequest, ManifestResult

from protocol import handler_entered_checkpoint, record_handler_invocation, write_checkpoint
from scenario_jobs import after_completion_job, before_completion_job


def _checkpoint_dir() -> Path:
    return Path(os.environ["SUPERJOBS_CROSS_STATE_DIR"])


def _run_id() -> str:
    return os.environ["SUPERJOBS_CROSS_RUN_ID"]


def _scenario() -> str:
    return os.environ["SUPERJOBS_CROSS_SCENARIO"]


def _worker_generation() -> str:
    return os.environ.get("SUPERJOBS_WORKER_GENERATION", "1")


def _record_entry(job_name: str, context: JobContext[None]) -> None:
    state_dir = _checkpoint_dir()
    run_id = _run_id()
    record_handler_invocation(
        state_dir,
        run_id=run_id,
        worker_generation=_worker_generation(),
        execution_id=context.id,
    )
    generation = _worker_generation()
    write_checkpoint(
        state_dir,
        run_id=run_id,
        checkpoint=handler_entered_checkpoint(generation),
        job=job_name,
        execution_id=context.id,
        pid=os.getpid(),
        worker_generation=generation,
    )


def register_recovery_handlers(jobs: SuperJobs, run_id: str) -> None:
    scenario = _scenario()

    if scenario == "recovery_before_completion":
        job = before_completion_job(run_id)

        @jobs.handler(job)
        async def before_completion_handler(
            request: ManifestRequest,
            context: JobContext[None],
        ) -> ManifestResult:
            _record_entry(job.name, context)
            if _worker_generation() == "1":
                while True:
                    await asyncio.sleep(0.05)
            return ManifestResult(revision=f"recovery-{request.device_id}")

    if scenario == "recovery_after_completion":
        job = after_completion_job(run_id)

        @jobs.handler(job)
        async def after_completion_handler(
            request: ManifestRequest,
            context: JobContext[None],
        ) -> ManifestResult:
            _record_entry(job.name, context)
            return ManifestResult(revision=f"recovery-{request.device_id}")
