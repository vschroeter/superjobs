"""Installed-wheel producer for worker crash recovery verification."""

from __future__ import annotations

import asyncio
import os
import sys
import time
from pathlib import Path

from faststream.nats import NatsBroker

from superjobs import JobCompleted, JobSucceeded, SuperJobs
from superjobs_contract_example import ManifestRequest, ManifestResult

from protocol import SUBMITTED, read_ready, ready_path, write_checkpoint
from runtime_isolation import assert_producer_layout
from queue_config import queue_config_for_run
from scenario_jobs import after_completion_job, before_completion_job

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
            return
        await asyncio.sleep(READY_POLL_INTERVAL)
    raise TimeoutError(f"worker did not become ready within {timeout}s")


def _job_for_scenario(scenario: str, run_id: str):
    if scenario == "recovery_before_completion":
        return before_completion_job(run_id)
    if scenario == "recovery_after_completion":
        return after_completion_job(run_id)
    raise AssertionError(f"unknown scenario {scenario!r}")


async def _phase_submit(jobs: SuperJobs, scenario: str, run_id: str, state_dir: Path) -> None:
    job = _job_for_scenario(scenario, run_id)
    handle = await jobs.client(job).submit(ManifestRequest(device_id="recovery-device"))
    write_checkpoint(
        state_dir,
        run_id=run_id,
        checkpoint=SUBMITTED,
        job=job.name,
        execution_id=handle.id,
    )
    print(f"producer submitted execution_id={handle.id}", flush=True)


async def _phase_recover(
    jobs: SuperJobs,
    scenario: str,
    run_id: str,
    execution_id: str,
) -> None:
    job = _job_for_scenario(scenario, run_id)
    handle = await jobs.client(job).get(execution_id)
    if handle.id != execution_id:
        raise AssertionError(f"handle id {handle.id!r} != expected {execution_id!r}")
    result = await handle.result(wait_timeout=RECOVER_TIMEOUT)
    expected = ManifestResult(revision="recovery-recovery-device")
    if result != expected:
        raise AssertionError(f"unexpected result {result!r}, expected {expected!r}")
    outcome = await handle.outcome(wait_timeout=RECOVER_TIMEOUT)
    if not isinstance(outcome, JobSucceeded):
        raise AssertionError(f"expected JobSucceeded outcome, got {outcome!r}")
    events = [event.data async for event in handle.events()]
    if not events:
        raise AssertionError("missing observation stream after recovery")
    if not isinstance(events[-1], JobCompleted):
        raise AssertionError(f"expected JobCompleted terminal event, got {events[-1]!r}")
    print(f"producer recovered execution_id={execution_id}", flush=True)


async def _run() -> None:
    nats_url = _require_env("NATS_URL")
    run_id = _require_env("SUPERJOBS_CROSS_RUN_ID")
    scenario = _require_env("SUPERJOBS_CROSS_SCENARIO")
    phase = _require_env("SUPERJOBS_RECOVERY_PHASE")
    phase_timeout = RECOVER_TIMEOUT + 5.0 if phase == "recover" else FLOW_TIMEOUT
    async with asyncio.timeout(phase_timeout):
        assert_producer_layout(Path(__file__).resolve().parent)

        state_dir = Path(_require_env("SUPERJOBS_CROSS_STATE_DIR"))

        await _wait_for_worker_ready(state_dir, run_id, FLOW_TIMEOUT)

        jobs = SuperJobs(
            broker=NatsBroker(nats_url, connect_timeout=5),
            queue_config=queue_config_for_run(run_id),
        )
        await jobs.start()
        try:
            if phase == "submit":
                await _phase_submit(jobs, scenario, run_id, state_dir)
            elif phase == "recover":
                execution_id = _require_env("SUPERJOBS_EXECUTION_ID")
                await _phase_recover(jobs, scenario, run_id, execution_id)
            else:
                raise AssertionError(f"unknown recovery phase {phase!r}")
            print(f"producer ok scenario={scenario} phase={phase} run_id={run_id}", flush=True)
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
