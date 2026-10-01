"""Readiness and checkpoint files keyed by a per-run identity."""

from __future__ import annotations

import json
import os
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any


READY_FILE = "worker_ready.json"
STOP_FILE = "worker_stop.json"
PHASE_READY = "ready"
PHASE_STOP = "stop"
HANDLER_ENTERED = "handler_entered"


class ProtocolError(Exception):
    """Invalid or stale readiness/checkpoint marker."""


@dataclass(frozen=True, slots=True)
class ReadyMarker:
    run_id: str
    phase: str


@dataclass(frozen=True, slots=True)
class CheckpointMarker:
    run_id: str
    checkpoint: str
    job: str | None = None
    execution_id: str | None = None


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


def ready_path(state_dir: Path) -> Path:
    return state_dir / READY_FILE


def assert_ready_absent(state_dir: Path) -> None:
    path = ready_path(state_dir)
    if path.is_file():
        raise ProtocolError(
            f"{path} must not exist before worker startup "
            "(stale markers can hide a dead worker)",
        )


def write_ready(state_dir: Path, run_id: str) -> None:
    _write_json(
        ready_path(state_dir),
        {"run_id": run_id, "phase": PHASE_READY},
    )


def read_ready(state_dir: Path) -> ReadyMarker:
    payload = _read_json(ready_path(state_dir))
    run_id = payload.get("run_id")
    phase = payload.get("phase")
    if not isinstance(run_id, str) or not run_id:
        raise ProtocolError("ready marker missing run_id")
    if phase != PHASE_READY:
        raise ProtocolError(f"ready marker has unexpected phase {phase!r}")
    return ReadyMarker(run_id=run_id, phase=phase)


def checkpoint_path(state_dir: Path, name: str) -> Path:
    return state_dir / f"checkpoint_{name}.json"


def stop_path(state_dir: Path) -> Path:
    return state_dir / STOP_FILE


def write_worker_stop(state_dir: Path, run_id: str) -> None:
    _write_json(
        stop_path(state_dir),
        {"run_id": run_id, "phase": PHASE_STOP},
    )


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
) -> None:
    payload: dict[str, Any] = {"run_id": run_id, "checkpoint": checkpoint}
    if job is not None:
        payload["job"] = job
    if execution_id is not None:
        payload["execution_id"] = execution_id
    _write_json(checkpoint_path(state_dir, checkpoint), payload)


def read_checkpoint(state_dir: Path, name: str) -> CheckpointMarker:
    payload = _read_json(checkpoint_path(state_dir, name))
    run_id = payload.get("run_id")
    checkpoint = payload.get("checkpoint")
    job = payload.get("job")
    if not isinstance(run_id, str) or not run_id:
        raise ProtocolError(f"checkpoint {name!r} missing run_id")
    if checkpoint != name:
        raise ProtocolError(
            f"checkpoint file {name!r} contains checkpoint {checkpoint!r}",
        )
    if job is not None and not isinstance(job, str):
        raise ProtocolError(f"checkpoint {name!r} has invalid job field")
    execution_id = payload.get("execution_id")
    if execution_id is not None and not isinstance(execution_id, str):
        raise ProtocolError(f"checkpoint {name!r} has invalid execution_id field")
    return CheckpointMarker(
        run_id=run_id,
        checkpoint=checkpoint,
        job=job if isinstance(job, str) else None,
        execution_id=execution_id if isinstance(execution_id, str) else None,
    )


def wait_for_ready(
    state_dir: Path,
    *,
    expected_run_id: str,
    deadline: float,
    poll_interval: float = 0.05,
    child_process: Any | None = None,
    child_label: str = "worker",
) -> ReadyMarker:
    end = time.monotonic() + deadline
    while time.monotonic() < end:
        if child_process is not None:
            exit_code = child_process.poll()
            if exit_code is not None:
                raise ProtocolError(
                    f"{child_label} exited with code {exit_code} before publishing ready",
                )
        path = ready_path(state_dir)
        if path.is_file():
            marker = read_ready(state_dir)
            if marker.run_id != expected_run_id:
                raise ProtocolError(
                    f"ready marker run_id {marker.run_id!r} != expected {expected_run_id!r}",
                )
            return marker
        time.sleep(poll_interval)
    raise TimeoutError(
        f"worker did not publish ready for run_id {expected_run_id!r} within {deadline}s",
    )


def wait_for_checkpoint(
    state_dir: Path,
    *,
    name: str,
    expected_run_id: str,
    deadline: float,
    poll_interval: float = 0.05,
    expected_execution_id: str | None = None,
) -> CheckpointMarker:
    end = time.monotonic() + deadline
    while time.monotonic() < end:
        path = checkpoint_path(state_dir, name)
        if path.is_file():
            marker = read_checkpoint(state_dir, name)
            if marker.run_id != expected_run_id:
                raise ProtocolError(
                    f"checkpoint {name!r} run_id {marker.run_id!r} != expected {expected_run_id!r}",
                )
            if expected_execution_id is not None and marker.execution_id != expected_execution_id:
                raise ProtocolError(
                    f"checkpoint {name!r} execution_id {marker.execution_id!r} "
                    f"!= expected {expected_execution_id!r}",
                )
            return marker
        time.sleep(poll_interval)
    raise TimeoutError(
        f"checkpoint {name!r} for run_id {expected_run_id!r} not observed within {deadline}s",
    )
