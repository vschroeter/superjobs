"""Worker-side broker restart handlers (never imported by producer entry points)."""

from __future__ import annotations

import os
from pathlib import Path

from superjobs import JobContext, SuperJobs
from superjobs_contract_example import ManifestRequest, ManifestResult

from protocol import handler_entered_checkpoint, record_handler_invocation, write_checkpoint
from scenario_jobs import completed_job, pending_job


def _checkpoint_dir() -> Path:
    return Path(os.environ["SUPERJOBS_CROSS_STATE_DIR"])


def _run_id() -> str:
    return os.environ["SUPERJOBS_CROSS_RUN_ID"]


def _worker_generation() -> str:
    return os.environ.get("SUPERJOBS_WORKER_GENERATION", "1")


def _record_entry(job_name: str, context: JobContext[None]) -> None:
    state_dir = _checkpoint_dir()
    run_id = _run_id()
    generation = _worker_generation()
    record_handler_invocation(
        state_dir,
        run_id=run_id,
        worker_generation=generation,
        job=job_name,
        execution_id=context.id,
    )
    write_checkpoint(
        state_dir,
        run_id=run_id,
        checkpoint=handler_entered_checkpoint(generation),
        job=job_name,
        execution_id=context.id,
        pid=os.getpid(),
        worker_generation=generation,
    )


def register_restart_handlers(jobs: SuperJobs, run_id: str) -> None:
    completed = completed_job(run_id)
    pending = pending_job(run_id)

    @jobs.handler(completed)
    async def completed_handler(
        request: ManifestRequest,
        context: JobContext[None],
    ) -> ManifestResult:
        _record_entry(completed.name, context)
        return ManifestResult(revision=f"restart-completed-{request.device_id}")

    @jobs.handler(pending)
    async def pending_handler(
        request: ManifestRequest,
        context: JobContext[None],
    ) -> ManifestResult:
        _record_entry(pending.name, context)
        return ManifestResult(revision=f"restart-pending-{request.device_id}")
