"""Installed-wheel controller for resource admission verification phases."""

from __future__ import annotations

import asyncio
import os
import sys
import time
from pathlib import Path

from superjobs import JobState
from superjobs_contract_example import ManifestResult
from superjobs.resources.models import AdmissionOwner

from admission_helpers import (
    resolve_terminated_owner,
    snapshot_owners,
    stale_release_attempt,
    superjobs_for_admission,
)
from protocol import (
    COMPLETED,
    OWNER_SNAPSHOT_SAVED,
    STALE_RELEASE_REJECTED,
    SUBMITTED,
    TERMINATION_RESOLVED,
    WAITING_OBSERVED,
    write_checkpoint,
)
from runtime_isolation import assert_controller_layout
from scenario_jobs import (
    job_a,
    job_b,
    job_broker_restart,
    job_crash_successor,
    job_redelivery,
)
from stdmsg import emit, fatal

FLOW_TIMEOUT = 45.0
POLL_INTERVAL = 0.05

EXIT_OK = 0
EXIT_ASSERTION = 3


def _require_env(name: str) -> str:
    value = os.environ.get(name)
    if not value:
        fatal(f"{name} must be set", code=11)
    return value


def _state_dir() -> Path:
    return Path(_require_env("SUPERJOBS_CROSS_STATE_DIR"))


def _run_id() -> str:
    return _require_env("SUPERJOBS_CROSS_RUN_ID")


def _job_for_token(token: str, run_id: str):
    mapping = {
        "a": job_a,
        "b": job_b,
        "redelivery": job_redelivery,
        "successor": job_crash_successor,
        "broker_restart": job_broker_restart,
    }
    factory = mapping.get(token)
    if factory is None:
        fatal(f"unknown job token {token!r}")
    return factory(run_id)


async def _submit_job(token: str) -> str:
    from superjobs_contract_example import ManifestRequest

    run_id = _run_id()
    state_dir = _state_dir()
    nats_url = _require_env("NATS_URL")
    jobs = superjobs_for_admission(nats_url, run_id)
    await asyncio.wait_for(jobs.start(), timeout=FLOW_TIMEOUT)
    try:
        job = _job_for_token(token, run_id)
        handle = await jobs.client(job).submit(ManifestRequest(device_id=f"ra-{token}"))
        write_checkpoint(
            state_dir,
            run_id=run_id,
            checkpoint=SUBMITTED,
            job=token,
            execution_id=handle.id,
        )
        emit({"type": "submitted", "job": token, "execution_id": handle.id})
        return handle.id
    finally:
        await asyncio.wait_for(jobs.stop(), timeout=FLOW_TIMEOUT)


async def _observe_waiting(execution_id: str, job_token: str) -> None:
    run_id = _run_id()
    state_dir = _state_dir()
    nats_url = _require_env("NATS_URL")
    jobs = superjobs_for_admission(nats_url, run_id)
    await asyncio.wait_for(jobs.start(), timeout=FLOW_TIMEOUT)
    try:
        job = _job_for_token(job_token, run_id)
        handle = await jobs.client(job).get(execution_id)
        deadline = time.monotonic() + FLOW_TIMEOUT
        observed: str | None = None
        while time.monotonic() < deadline:
            status = await handle.status()
            observed = status.state.value
            if status.state is JobState.WAITING_FOR_RESOURCE:
                break
            await asyncio.sleep(POLL_INTERVAL)
        if observed != JobState.WAITING_FOR_RESOURCE.value:
            fatal(
                f"expected waiting_for_resource for {execution_id}, saw {observed!r}",
            )
        write_checkpoint(
            state_dir,
            run_id=run_id,
            checkpoint=WAITING_OBSERVED,
            execution_id=execution_id,
            job_state=JobState.WAITING_FOR_RESOURCE.value,
        )
        emit({"type": "waiting_observed", "execution_id": execution_id})
    finally:
        await asyncio.wait_for(jobs.stop(), timeout=FLOW_TIMEOUT)


async def _await_completion(execution_id: str, job_token: str) -> None:
    run_id = _run_id()
    state_dir = _state_dir()
    nats_url = _require_env("NATS_URL")
    jobs = superjobs_for_admission(nats_url, run_id)
    await asyncio.wait_for(jobs.start(), timeout=FLOW_TIMEOUT)
    try:
        job = _job_for_token(job_token, run_id)
        handle = await jobs.client(job).get(execution_id)
        async with asyncio.timeout(FLOW_TIMEOUT):
            result = await handle.result()
        if not isinstance(result, ManifestResult):
            fatal(f"execution {execution_id} did not return ManifestResult: {result!r}")
        write_checkpoint(
            state_dir,
            run_id=run_id,
            checkpoint=COMPLETED,
            execution_id=execution_id,
        )
        emit({"type": "completed", "execution_id": execution_id})
    finally:
        await asyncio.wait_for(jobs.stop(), timeout=FLOW_TIMEOUT)


async def _snapshot_owner() -> AdmissionOwner:
    run_id = _run_id()
    state_dir = _state_dir()
    nats_url = _require_env("NATS_URL")
    jobs = superjobs_for_admission(nats_url, run_id)
    await asyncio.wait_for(jobs.start(), timeout=FLOW_TIMEOUT)
    try:
        owners = await snapshot_owners(jobs)
        if len(owners) != 1:
            fatal(f"expected exactly one resource owner, saw {len(owners)}")
        owner = owners[0]
        incarnation = {
            "worker_id": owner.incarnation.worker_id,
            "incarnation_id": owner.incarnation.incarnation_id,
        }
        write_checkpoint(
            state_dir,
            run_id=run_id,
            checkpoint=OWNER_SNAPSHOT_SAVED,
            execution_id=owner.execution_id,
            owner_incarnation=incarnation,
        )
        owner_path = state_dir / "owner_snapshot.json"
        owner_path.write_text(owner.model_dump_json(), encoding="utf-8")
        emit({"type": "owner_snapshot", "execution_id": owner.execution_id})
        return owner
    finally:
        await asyncio.wait_for(jobs.stop(), timeout=FLOW_TIMEOUT)


async def _resolve_termination() -> None:
    run_id = _run_id()
    state_dir = _state_dir()
    owner_path = state_dir / "owner_snapshot.json"
    if not owner_path.is_file():
        fatal("owner_snapshot.json missing before resolve")
    owner = AdmissionOwner.model_validate_json(owner_path.read_text(encoding="utf-8"))
    evidence = os.environ.get(
        "SUPERJOBS_RA_TERMINATION_EVIDENCE",
        "process termination evidence",
    )
    nats_url = _require_env("NATS_URL")
    jobs = superjobs_for_admission(nats_url, run_id)
    await asyncio.wait_for(jobs.start(), timeout=FLOW_TIMEOUT)
    try:
        outcome = await resolve_terminated_owner(jobs, owner, evidence=evidence)
        write_checkpoint(
            state_dir,
            run_id=run_id,
            checkpoint=TERMINATION_RESOLVED,
            execution_id=owner.execution_id,
            release_outcome=outcome,
        )
        emit({"type": "termination_resolved", "outcome": outcome})
    finally:
        await asyncio.wait_for(jobs.stop(), timeout=FLOW_TIMEOUT)


async def _stale_release() -> None:
    run_id = _run_id()
    state_dir = _state_dir()
    owner_path = state_dir / "owner_snapshot.json"
    if not owner_path.is_file():
        fatal("owner_snapshot.json missing before stale release attempt")
    owner = AdmissionOwner.model_validate_json(owner_path.read_text(encoding="utf-8"))
    nats_url = _require_env("NATS_URL")
    jobs = superjobs_for_admission(nats_url, run_id)
    await asyncio.wait_for(jobs.start(), timeout=FLOW_TIMEOUT)
    try:
        outcome = await stale_release_attempt(jobs, owner)
        write_checkpoint(
            state_dir,
            run_id=run_id,
            checkpoint=STALE_RELEASE_REJECTED,
            execution_id=owner.execution_id,
            release_outcome=outcome,
        )
        emit({"type": "stale_release", "outcome": outcome})
    finally:
        await asyncio.wait_for(jobs.stop(), timeout=FLOW_TIMEOUT)


async def _run_phase() -> None:
    role_dir = Path(__file__).resolve().parent
    assert_controller_layout(role_dir)
    phase = _require_env("SUPERJOBS_RA_PHASE")
    if phase == "submit":
        token = _require_env("SUPERJOBS_RA_JOB")
        await _submit_job(token)
    elif phase == "observe_waiting":
        execution_id = _require_env("SUPERJOBS_RA_EXECUTION_ID")
        job_token = _require_env("SUPERJOBS_RA_JOB")
        await _observe_waiting(execution_id, job_token)
    elif phase == "await_completion":
        execution_id = _require_env("SUPERJOBS_RA_EXECUTION_ID")
        job_token = _require_env("SUPERJOBS_RA_JOB")
        await _await_completion(execution_id, job_token)
    elif phase == "snapshot_owner":
        await _snapshot_owner()
    elif phase == "resolve_termination":
        await _resolve_termination()
    elif phase == "stale_release":
        await _stale_release()
    else:
        fatal(f"unknown phase {phase!r}")


def main() -> None:
    try:
        asyncio.run(_run_phase())
    except SystemExit:
        raise
    except Exception as exc:
        print(f"{type(exc).__name__}: {exc}", file=sys.stderr)
        raise SystemExit(EXIT_ASSERTION) from exc
    raise SystemExit(EXIT_OK)


if __name__ == "__main__":
    main()
