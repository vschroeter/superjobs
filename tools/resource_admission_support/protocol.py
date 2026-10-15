"""Readiness, checkpoints, holder ledger, and waits for resource admission verification."""

from __future__ import annotations

import json
import os
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any

READY_FILE_PREFIX = "worker_ready_gen_"
STOP_FILE = "worker_stop.json"

PHASE_READY = "ready"
PHASE_STOP = "stop"

MODEL_LOAD_ENTERED = "model_load_entered"
MODEL_LOAD_RELEASED = "model_load_released"
WAITING_OBSERVED = "waiting_for_resource_observed"
OWNER_SNAPSHOT_SAVED = "owner_snapshot_saved"
TERMINATION_RESOLVED = "termination_resolved"
STALE_RELEASE_REJECTED = "stale_release_rejected"
BROKER_RESTART_HELD = "broker_restart_resource_held"
SUBMITTED = "submitted"
COMPLETED = "completed"


class ProtocolError(Exception):
    """Invalid or stale readiness/checkpoint marker."""


@dataclass(frozen=True, slots=True)
class ReadyMarker:
    run_id: str
    phase: str
    pid: int
    worker_generation: str


@dataclass(frozen=True, slots=True)
class CheckpointMarker:
    run_id: str
    checkpoint: str
    job: str | None = None
    execution_id: str | None = None
    pid: int | None = None
    worker_generation: str | None = None
    job_state: str | None = None
    delivery_attempt: int | None = None
    holder_count: int | None = None
    owner_incarnation: dict[str, Any] | None = None
    release_outcome: str | None = None


@dataclass(frozen=True, slots=True)
class StopMarker:
    run_id: str
    phase: str


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(f".{path.name}.tmp-{os.getpid()}")
    try:
        tmp.write_text(json.dumps(payload, sort_keys=True) + "\n", encoding="utf-8")
        tmp.replace(path)
    finally:
        if tmp.is_file():
            tmp.unlink(missing_ok=True)


def _read_json(path: Path) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        raise ProtocolError(f"malformed marker at {path}") from exc
    if not isinstance(payload, dict):
        raise ProtocolError(f"marker must be a JSON object at {path}")
    return payload


def ready_path(state_dir: Path, *, worker_generation: str) -> Path:
    return state_dir / f"{READY_FILE_PREFIX}{worker_generation}.json"


def assert_ready_absent(state_dir: Path, *, worker_generation: str) -> None:
    path = ready_path(state_dir, worker_generation=worker_generation)
    if path.is_file():
        raise ProtocolError(
            f"{path} must not exist before worker startup "
            "(stale markers can hide a dead worker)",
        )


def stop_path(state_dir: Path) -> Path:
    return state_dir / STOP_FILE


def checkpoint_path(state_dir: Path, name: str, *, job: str | None = None) -> Path:
    if name == SUBMITTED and job is not None:
        return state_dir / f"checkpoint_submitted_{job}.json"
    if job is not None:
        return state_dir / f"checkpoint_{name}_{job}.json"
    return state_dir / f"checkpoint_{name}.json"


def write_ready(
    state_dir: Path,
    run_id: str,
    *,
    pid: int,
    worker_generation: str,
) -> None:
    _write_json(
        ready_path(state_dir, worker_generation=worker_generation),
        {
            "run_id": run_id,
            "phase": PHASE_READY,
            "pid": pid,
            "worker_generation": worker_generation,
        },
    )


def read_ready(state_dir: Path, *, worker_generation: str) -> ReadyMarker:
    payload = _read_json(ready_path(state_dir, worker_generation=worker_generation))
    run_id = payload.get("run_id")
    phase = payload.get("phase")
    pid = payload.get("pid")
    worker_generation = payload.get("worker_generation")
    if not isinstance(run_id, str) or not run_id:
        raise ProtocolError("ready marker missing run_id")
    if phase != PHASE_READY:
        raise ProtocolError(f"ready marker has unexpected phase {phase!r}")
    if not isinstance(pid, int):
        raise ProtocolError("ready marker missing pid")
    if not isinstance(worker_generation, str) or not worker_generation:
        raise ProtocolError("ready marker missing worker_generation")
    return ReadyMarker(
        run_id=run_id,
        phase=phase,
        pid=pid,
        worker_generation=worker_generation,
    )


def write_worker_stop(state_dir: Path, run_id: str) -> None:
    _write_json(stop_path(state_dir), {"run_id": run_id, "phase": PHASE_STOP})


def read_worker_stop(state_dir: Path) -> StopMarker:
    payload = _read_json(stop_path(state_dir))
    run_id = payload.get("run_id")
    phase = payload.get("phase")
    if not isinstance(run_id, str) or not run_id:
        raise ProtocolError("stop marker missing run_id")
    if phase != PHASE_STOP:
        raise ProtocolError(f"stop marker has unexpected phase {phase!r}")
    return StopMarker(run_id=run_id, phase=phase)


def write_checkpoint(
    state_dir: Path,
    *,
    run_id: str,
    checkpoint: str,
    job: str | None = None,
    execution_id: str | None = None,
    pid: int | None = None,
    worker_generation: str | None = None,
    job_state: str | None = None,
    delivery_attempt: int | None = None,
    holder_count: int | None = None,
    owner_incarnation: dict[str, Any] | None = None,
    release_outcome: str | None = None,
) -> None:
    payload: dict[str, Any] = {"run_id": run_id, "checkpoint": checkpoint}
    if job is not None:
        payload["job"] = job
    if execution_id is not None:
        payload["execution_id"] = execution_id
    if pid is not None:
        payload["pid"] = pid
    if worker_generation is not None:
        payload["worker_generation"] = worker_generation
    if job_state is not None:
        payload["job_state"] = job_state
    if delivery_attempt is not None:
        payload["delivery_attempt"] = delivery_attempt
    if holder_count is not None:
        payload["holder_count"] = holder_count
    if owner_incarnation is not None:
        payload["owner_incarnation"] = owner_incarnation
    if release_outcome is not None:
        payload["release_outcome"] = release_outcome
    job_key = job if checkpoint == SUBMITTED else None
    _write_json(checkpoint_path(state_dir, checkpoint, job=job_key), payload)


def read_checkpoint(
    state_dir: Path,
    name: str,
    *,
    job: str | None = None,
    execution_id: str | None = None,
) -> CheckpointMarker:
    if execution_id is not None:
        path = checkpoint_path(state_dir, name, job=execution_id)
    else:
        path = checkpoint_path(state_dir, name, job=job)
    payload = _read_json(path)
    run_id = payload.get("run_id")
    checkpoint = payload.get("checkpoint")
    if not isinstance(run_id, str) or not run_id:
        raise ProtocolError(f"checkpoint {name!r} missing run_id")
    if checkpoint != name:
        raise ProtocolError(
            f"checkpoint file {name!r} contains checkpoint {checkpoint!r}",
        )
    job = payload.get("job")
    execution_id = payload.get("execution_id")
    pid = payload.get("pid")
    worker_generation = payload.get("worker_generation")
    job_state = payload.get("job_state")
    delivery_attempt = payload.get("delivery_attempt")
    holder_count = payload.get("holder_count")
    owner_incarnation = payload.get("owner_incarnation")
    release_outcome = payload.get("release_outcome")
    return CheckpointMarker(
        run_id=run_id,
        checkpoint=checkpoint,
        job=job if isinstance(job, str) else None,
        execution_id=execution_id if isinstance(execution_id, str) else None,
        pid=pid if isinstance(pid, int) else None,
        worker_generation=worker_generation
        if isinstance(worker_generation, str)
        else None,
        job_state=job_state if isinstance(job_state, str) else None,
        delivery_attempt=delivery_attempt
        if isinstance(delivery_attempt, int)
        else None,
        holder_count=holder_count if isinstance(holder_count, int) else None,
        owner_incarnation=owner_incarnation
        if isinstance(owner_incarnation, dict)
        else None,
        release_outcome=release_outcome if isinstance(release_outcome, str) else None,
    )


def write_execution_checkpoint(
    state_dir: Path,
    *,
    run_id: str,
    checkpoint: str,
    execution_id: str,
    job: str | None = None,
    pid: int | None = None,
    worker_generation: str | None = None,
    job_state: str | None = None,
    delivery_attempt: int | None = None,
    holder_count: int | None = None,
    owner_incarnation: dict[str, Any] | None = None,
    release_outcome: str | None = None,
) -> None:
    payload: dict[str, Any] = {
        "run_id": run_id,
        "checkpoint": checkpoint,
        "execution_id": execution_id,
    }
    if job is not None:
        payload["job"] = job
    if pid is not None:
        payload["pid"] = pid
    if worker_generation is not None:
        payload["worker_generation"] = worker_generation
    if job_state is not None:
        payload["job_state"] = job_state
    if delivery_attempt is not None:
        payload["delivery_attempt"] = delivery_attempt
    if holder_count is not None:
        payload["holder_count"] = holder_count
    if owner_incarnation is not None:
        payload["owner_incarnation"] = owner_incarnation
    if release_outcome is not None:
        payload["release_outcome"] = release_outcome
    _write_json(checkpoint_path(state_dir, checkpoint, job=execution_id), payload)


def wait_for_ready(
    state_dir: Path,
    *,
    expected_run_id: str,
    deadline: float,
    poll_interval: float = 0.05,
    child_process: Any = None,
    expected_worker_generation: str,
    forbidden_pid: int | None = None,
) -> ReadyMarker:
    """Wait until deadline (absolute ``time.monotonic()`` limit)."""
    while time.monotonic() < deadline:
        if child_process is not None and child_process.poll() is not None:
            raise ProtocolError(
                f"child exited with code {child_process.poll()} before ready",
            )
        path = ready_path(state_dir, worker_generation=expected_worker_generation)
        if path.is_file():
            marker = read_ready(state_dir, worker_generation=expected_worker_generation)
            if marker.run_id != expected_run_id:
                raise ProtocolError(
                    f"ready run_id {marker.run_id!r} != expected {expected_run_id!r}",
                )
            if marker.worker_generation != expected_worker_generation:
                raise ProtocolError(
                    f"ready generation {marker.worker_generation!r} "
                    f"!= expected {expected_worker_generation!r}",
                )
            if forbidden_pid is not None and marker.pid == forbidden_pid:
                raise ProtocolError(
                    f"ready pid {marker.pid} forbidden (reused dead worker)",
                )
            return marker
        time.sleep(min(poll_interval, max(0.0, deadline - time.monotonic())))
    raise ProtocolError(
        f"worker generation {expected_worker_generation!r} not ready before deadline",
    )


def wait_for_checkpoint(
    state_dir: Path,
    *,
    name: str,
    expected_run_id: str,
    deadline: float,
    poll_interval: float = 0.05,
    child_process: Any = None,
    expected_execution_id: str | None = None,
    job: str | None = None,
) -> CheckpointMarker:
    """Wait until deadline (absolute ``time.monotonic()`` limit)."""
    while time.monotonic() < deadline:
        if child_process is not None and child_process.poll() is not None:
            raise ProtocolError(
                f"child exited with code {child_process.poll()} before checkpoint {name!r}",
            )
        if expected_execution_id is not None:
            path = checkpoint_path(state_dir, name, job=expected_execution_id)
        else:
            path = checkpoint_path(state_dir, name, job=job)
        if path.is_file():
            marker = read_checkpoint(
                state_dir,
                name,
                job=job,
                execution_id=expected_execution_id,
            )
            if marker.run_id != expected_run_id:
                raise ProtocolError(
                    f"checkpoint {name!r} run_id {marker.run_id!r} "
                    f"!= expected {expected_run_id!r}",
                )
            if (
                expected_execution_id is not None
                and marker.execution_id != expected_execution_id
            ):
                time.sleep(min(poll_interval, max(0.0, deadline - time.monotonic())))
                continue
            return marker
        time.sleep(min(poll_interval, max(0.0, deadline - time.monotonic())))
    raise ProtocolError(f"checkpoint {name!r} not observed before deadline")
