"""Installed-wheel producer for broker restart verification."""

from __future__ import annotations

import asyncio
import os
import sys
import time
from pathlib import Path

from faststream.nats import NatsBroker

from superjobs import JobCompleted, JobState, JobSucceeded, SuperJobs
from superjobs_contract_example import ManifestRequest, ManifestResult

from protocol import (
    COMPLETED_DONE,
    COMPLETED_SUBMITTED,
    PENDING_DONE,
    PENDING_SUBMITTED,
    ready_path,
    read_ready,
    write_checkpoint,
)
from queue_config import queue_config_for_run
from runtime_isolation import assert_producer_layout
from scenario_jobs import completed_job, pending_job

FLOW_TIMEOUT = 30.0
RECOVER_TIMEOUT = 45.0
READY_POLL_INTERVAL = 0.05

EXIT_OK = 0
EXIT_ASSERTION = 3


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
            expected_generation = os.environ.get("SUPERJOBS_EXPECT_WORKER_GENERATION")
            if expected_generation and marker.worker_generation != expected_generation:
                raise AssertionError(
                    f"ready generation {marker.worker_generation!r} "
                    f"!= expected {expected_generation!r}",
                )
            return
        await asyncio.sleep(READY_POLL_INTERVAL)
    raise TimeoutError(f"worker did not become ready within {timeout}s")


async def _phase_run_completed(jobs: SuperJobs, run_id: str, state_dir: Path) -> str:
    job = completed_job(run_id)
    handle = await jobs.client(job).submit(ManifestRequest(device_id="completed-device"))
    write_checkpoint(
        state_dir,
        run_id=run_id,
        checkpoint=COMPLETED_SUBMITTED,
        job=job.name,
        execution_id=handle.id,
    )
    result = await handle.result(wait_timeout=RECOVER_TIMEOUT)
    expected = ManifestResult(revision="restart-completed-completed-device")
    if result != expected:
        raise AssertionError(f"unexpected completed result {result!r}, expected {expected!r}")
    outcome = await handle.outcome(wait_timeout=RECOVER_TIMEOUT)
    if not isinstance(outcome, JobSucceeded):
        raise AssertionError(f"expected JobSucceeded, got {outcome!r}")
    write_checkpoint(
        state_dir,
        run_id=run_id,
        checkpoint=COMPLETED_DONE,
        job=job.name,
        execution_id=handle.id,
    )
    print(f"producer completed execution_id={handle.id}", flush=True)
    return handle.id


async def _phase_submit_pending(jobs: SuperJobs, run_id: str, state_dir: Path) -> str:
    job = pending_job(run_id)
    handle = await jobs.client(job).submit(ManifestRequest(device_id="pending-device"))
    status = await handle.status()
    if status.state in (JobState.COMPLETED, JobState.FAILED, JobState.CANCELLED):
        raise AssertionError(
            f"pending submission must not be terminal before restart, got {status.state!r}",
        )
    write_checkpoint(
        state_dir,
        run_id=run_id,
        checkpoint=PENDING_SUBMITTED,
        job=job.name,
        execution_id=handle.id,
        status_state=status.state.name,
    )
    print(
        f"producer submitted pending execution_id={handle.id} status={status.state.name}",
        flush=True,
    )
    return handle.id


async def _phase_recover_completed(jobs: SuperJobs, run_id: str, execution_id: str) -> None:
    job = completed_job(run_id)
    handle = await jobs.client(job).get(execution_id)
    if handle.id != execution_id:
        raise AssertionError(f"handle id {handle.id!r} != expected {execution_id!r}")
    result = await handle.result(wait_timeout=RECOVER_TIMEOUT)
    expected = ManifestResult(revision="restart-completed-completed-device")
    if result != expected:
        raise AssertionError(f"unexpected recovered result {result!r}")
    outcome = await handle.outcome(wait_timeout=RECOVER_TIMEOUT)
    if not isinstance(outcome, JobSucceeded):
        raise AssertionError(f"expected JobSucceeded outcome, got {outcome!r}")
    events = [event.data async for event in handle.events()]
    if not events or not isinstance(events[-1], JobCompleted):
        raise AssertionError("missing JobCompleted terminal observation after broker restart")
    print(f"producer recovered completed execution_id={execution_id}", flush=True)


async def _phase_await_pending(jobs: SuperJobs, run_id: str, execution_id: str) -> None:
    job = pending_job(run_id)
    handle = await jobs.client(job).get(execution_id)
    if handle.id != execution_id:
        raise AssertionError(f"handle id {handle.id!r} != expected {execution_id!r}")
    result = await handle.result(wait_timeout=RECOVER_TIMEOUT)
    expected = ManifestResult(revision="restart-pending-pending-device")
    if result != expected:
        raise AssertionError(f"unexpected pending result {result!r}, expected {expected!r}")
    outcome = await handle.outcome(wait_timeout=RECOVER_TIMEOUT)
    if not isinstance(outcome, JobSucceeded):
        raise AssertionError(f"expected JobSucceeded outcome, got {outcome!r}")
    print(f"producer observed pending completion execution_id={execution_id}", flush=True)


async def _run() -> None:
    nats_url = _require_env("NATS_URL")
    run_id = _require_env("SUPERJOBS_CROSS_RUN_ID")
    phase = _require_env("SUPERJOBS_BROKER_RESTART_PHASE")
    state_dir = Path(_require_env("SUPERJOBS_CROSS_STATE_DIR"))
    phase_timeout = RECOVER_TIMEOUT + 5.0
    async with asyncio.timeout(phase_timeout):
        assert_producer_layout(Path(__file__).resolve().parent)

        skip_ready = os.environ.get("SUPERJOBS_SKIP_WORKER_READY") == "1"
        if not skip_ready:
            await _wait_for_worker_ready(state_dir, run_id, FLOW_TIMEOUT)

        jobs = SuperJobs(
            broker=NatsBroker(nats_url, connect_timeout=5),
            queue_config=queue_config_for_run(run_id),
        )
        await jobs.start()
        try:
            if phase == "run_completed":
                await _phase_run_completed(jobs, run_id, state_dir)
            elif phase == "submit_pending":
                await _phase_submit_pending(jobs, run_id, state_dir)
            elif phase == "recover_completed":
                execution_id = _require_env("SUPERJOBS_COMPLETED_EXECUTION_ID")
                await _phase_recover_completed(jobs, run_id, execution_id)
            elif phase == "await_pending":
                execution_id = _require_env("SUPERJOBS_PENDING_EXECUTION_ID")
                await _phase_await_pending(jobs, run_id, execution_id)
                write_checkpoint(
                    state_dir,
                    run_id=run_id,
                    checkpoint=PENDING_DONE,
                    job=pending_job(run_id).name,
                    execution_id=execution_id,
                )
            else:
                raise AssertionError(f"unknown broker restart phase {phase!r}")
            print(f"producer ok phase={phase} run_id={run_id}", flush=True)
        finally:
            await asyncio.wait_for(jobs.stop(), timeout=FLOW_TIMEOUT)


def main() -> None:
    try:
        asyncio.run(_run())
    except AssertionError as exc:
        print(str(exc), file=sys.stderr)
        raise SystemExit(EXIT_ASSERTION) from exc
    except Exception as exc:
        print(f"producer failed: {exc!r}", file=sys.stderr)
        raise SystemExit(1) from exc


if __name__ == "__main__":
    main()
