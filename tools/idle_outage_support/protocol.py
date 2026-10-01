"""Readiness, checkpoints, and command protocol for idle outage verification."""

from __future__ import annotations

import json
import os
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any

READY_FILE = "worker_ready.json"
JOB_VERSION = "v1"
STOP_FILE = "worker_stop.json"
INVOCATIONS_FILE = "handler_invocations.json"
PRODUCER_REQUEST_FILE = "producer_request.json"
PHASE_READY = "ready"
PHASE_STOP = "stop"

PRODUCER_RUNTIME_READY = "producer_runtime_ready"
WORKER_RUNTIME_READY = "worker_runtime_ready"
BASELINE_DONE = "baseline_done"
HANDLE_READ_FAILED = "handle_read_failed"
HANDLE_READ_SUCCEEDED = "handle_read_succeeded"
POST_OUTAGE_DONE = "post_outage_done"
BROKER_RECONNECT_SETTINGS = "broker_reconnect_settings"

ALLOWED_OUTAGE_READ_ERROR_TYPES = frozenset(
    {
        "TimeoutError",
        "ConnectionClosedError",
        "NoServersError",
        "ServiceUnavailableError",
    },
)

COMMAND_BASELINE = "baseline"
COMMAND_READ_DURING_OUTAGE = "read_during_outage"
COMMAND_READ_AFTER_RECONNECT = "read_after_reconnect"
COMMAND_POST_OUTAGE = "post_outage"


def broker_event_checkpoint(event: str, role: str) -> str:
    return f"broker_{event}_{role}"


BROKER_DISCONNECTED_PRODUCER = broker_event_checkpoint("disconnected", "producer")
BROKER_RECONNECTED_PRODUCER = broker_event_checkpoint("reconnected", "producer")
BROKER_DISCONNECTED_WORKER = broker_event_checkpoint("disconnected", "worker")
BROKER_RECONNECTED_WORKER = broker_event_checkpoint("reconnected", "worker")


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
    status_state: str | None = None
    request_id: str | None = None
    error_type: str | None = None
    elapsed_seconds: float | None = None
    handle_id: str | None = None
    runtime_object_id: int | None = None
    handle_object_id: int | None = None
    callback_sequence: int | None = None


@dataclass(frozen=True, slots=True)
class StopMarker:
    run_id: str
    phase: str


@dataclass(frozen=True, slots=True)
class HandlerInvocation:
    run_id: str
    worker_generation: str
    job: str
    execution_id: str
    pid: int


@dataclass(frozen=True, slots=True)
class ProducerRequest:
    run_id: str
    request_id: str
    command: str


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


def write_ready(
    state_dir: Path,
    run_id: str,
    *,
    pid: int,
    worker_generation: str,
) -> None:
    _write_json(
        ready_path(state_dir),
        {
            "run_id": run_id,
            "phase": PHASE_READY,
            "pid": pid,
            "worker_generation": worker_generation,
        },
    )


def read_ready(state_dir: Path) -> ReadyMarker:
    payload = _read_json(ready_path(state_dir))
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
    pid: int | None = None,
    worker_generation: str | None = None,
    status_state: str | None = None,
    request_id: str | None = None,
    error_type: str | None = None,
    elapsed_seconds: float | None = None,
    handle_id: str | None = None,
    runtime_object_id: int | None = None,
    handle_object_id: int | None = None,
    callback_sequence: int | None = None,
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
    if status_state is not None:
        payload["status_state"] = status_state
    if request_id is not None:
        payload["request_id"] = request_id
    if error_type is not None:
        payload["error_type"] = error_type
    if elapsed_seconds is not None:
        payload["elapsed_seconds"] = elapsed_seconds
    if handle_id is not None:
        payload["handle_id"] = handle_id
    if runtime_object_id is not None:
        payload["runtime_object_id"] = runtime_object_id
    if handle_object_id is not None:
        payload["handle_object_id"] = handle_object_id
    if callback_sequence is not None:
        payload["callback_sequence"] = callback_sequence
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
    pid = payload.get("pid")
    if pid is not None and not isinstance(pid, int):
        raise ProtocolError(f"checkpoint {name!r} has invalid pid field")
    worker_generation = payload.get("worker_generation")
    if worker_generation is not None and not isinstance(worker_generation, str):
        raise ProtocolError(f"checkpoint {name!r} has invalid worker_generation field")
    status_state = payload.get("status_state")
    if status_state is not None and not isinstance(status_state, str):
        raise ProtocolError(f"checkpoint {name!r} has invalid status_state field")
    request_id = payload.get("request_id")
    if request_id is not None and not isinstance(request_id, str):
        raise ProtocolError(f"checkpoint {name!r} has invalid request_id field")
    error_type = payload.get("error_type")
    if error_type is not None and not isinstance(error_type, str):
        raise ProtocolError(f"checkpoint {name!r} has invalid error_type field")
    elapsed_seconds = payload.get("elapsed_seconds")
    if elapsed_seconds is not None and not isinstance(elapsed_seconds, (int, float)):
        raise ProtocolError(f"checkpoint {name!r} has invalid elapsed_seconds field")
    handle_id = payload.get("handle_id")
    if handle_id is not None and not isinstance(handle_id, str):
        raise ProtocolError(f"checkpoint {name!r} has invalid handle_id field")
    runtime_object_id = payload.get("runtime_object_id")
    if runtime_object_id is not None and not isinstance(runtime_object_id, int):
        raise ProtocolError(f"checkpoint {name!r} has invalid runtime_object_id field")
    handle_object_id = payload.get("handle_object_id")
    if handle_object_id is not None and not isinstance(handle_object_id, int):
        raise ProtocolError(f"checkpoint {name!r} has invalid handle_object_id field")
    callback_sequence = payload.get("callback_sequence")
    if callback_sequence is not None and not isinstance(callback_sequence, int):
        raise ProtocolError(f"checkpoint {name!r} has invalid callback_sequence field")
    return CheckpointMarker(
        run_id=run_id,
        checkpoint=checkpoint,
        job=job if isinstance(job, str) else None,
        execution_id=execution_id if isinstance(execution_id, str) else None,
        pid=pid if isinstance(pid, int) else None,
        worker_generation=worker_generation if isinstance(worker_generation, str) else None,
        status_state=status_state if isinstance(status_state, str) else None,
        request_id=request_id if isinstance(request_id, str) else None,
        error_type=error_type if isinstance(error_type, str) else None,
        elapsed_seconds=(
            float(elapsed_seconds) if isinstance(elapsed_seconds, (int, float)) else None
        ),
        handle_id=handle_id if isinstance(handle_id, str) else None,
        runtime_object_id=runtime_object_id if isinstance(runtime_object_id, int) else None,
        handle_object_id=handle_object_id if isinstance(handle_object_id, int) else None,
        callback_sequence=(
            callback_sequence if isinstance(callback_sequence, int) else None
        ),
    )


def is_allowed_outage_read_error(error_type: str) -> bool:
    return error_type in ALLOWED_OUTAGE_READ_ERROR_TYPES


def assert_allowed_outage_read_error(error_type: str | None) -> None:
    if error_type is None or not is_allowed_outage_read_error(error_type):
        allowed = ", ".join(sorted(ALLOWED_OUTAGE_READ_ERROR_TYPES))
        raise ProtocolError(
            f"outage read error type {error_type!r} is not in allowlist ({allowed})",
        )


def producer_request_path(state_dir: Path) -> Path:
    return state_dir / PRODUCER_REQUEST_FILE


def write_producer_request(
    state_dir: Path,
    *,
    run_id: str,
    request_id: str,
    command: str,
) -> None:
    _write_json(
        producer_request_path(state_dir),
        {
            "run_id": run_id,
            "request_id": request_id,
            "command": command,
        },
    )


def read_producer_request(state_dir: Path) -> ProducerRequest | None:
    path = producer_request_path(state_dir)
    if not path.is_file():
        return None
    payload = _read_json(path)
    run_id = payload.get("run_id")
    request_id = payload.get("request_id")
    command = payload.get("command")
    if not isinstance(run_id, str) or not isinstance(request_id, str) or not isinstance(command, str):
        raise ProtocolError(f"invalid producer request at {path}")
    return ProducerRequest(run_id=run_id, request_id=request_id, command=command)


def invocations_path(state_dir: Path) -> Path:
    return state_dir / INVOCATIONS_FILE


def read_invocations(state_dir: Path) -> list[HandlerInvocation]:
    path = invocations_path(state_dir)
    if not path.is_file():
        return []
    payload = _read_json(path)
    raw = payload.get("invocations")
    if not isinstance(raw, list):
        raise ProtocolError(f"{path} must contain an invocations list")
    items: list[HandlerInvocation] = []
    for entry in raw:
        if not isinstance(entry, dict):
            raise ProtocolError(f"invalid invocation entry in {path}")
        run_id = entry.get("run_id")
        generation = entry.get("worker_generation")
        job = entry.get("job")
        execution_id = entry.get("execution_id")
        pid = entry.get("pid")
        if not isinstance(run_id, str) or not isinstance(generation, str):
            raise ProtocolError(f"invocation missing run_id or worker_generation in {path}")
        if not isinstance(job, str):
            raise ProtocolError(f"invocation missing job in {path}")
        if not isinstance(execution_id, str):
            raise ProtocolError(f"invocation missing execution_id in {path}")
        if not isinstance(pid, int):
            raise ProtocolError(f"invocation missing pid in {path}")
        items.append(
            HandlerInvocation(
                run_id=run_id,
                worker_generation=generation,
                job=job,
                execution_id=execution_id,
                pid=pid,
            ),
        )
    return items


def record_handler_invocation(
    state_dir: Path,
    *,
    run_id: str,
    worker_generation: str,
    job: str,
    execution_id: str,
) -> int:
    path = invocations_path(state_dir)
    existing = read_invocations(state_dir)
    existing.append(
        HandlerInvocation(
            run_id=run_id,
            worker_generation=worker_generation,
            job=job,
            execution_id=execution_id,
            pid=os.getpid(),
        ),
    )
    payload = {
        "invocations": [
            {
                "run_id": item.run_id,
                "worker_generation": item.worker_generation,
                "job": item.job,
                "execution_id": item.execution_id,
                "pid": item.pid,
            }
            for item in existing
        ],
    }
    _write_json(path, payload)
    return len(existing)


def wait_for_ready(
    state_dir: Path,
    *,
    expected_run_id: str,
    deadline: float,
    poll_interval: float = 0.05,
    child_process: Any | None = None,
    child_label: str = "worker",
    expected_worker_generation: str | None = None,
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
            if (
                expected_worker_generation is not None
                and marker.worker_generation != expected_worker_generation
            ):
                raise ProtocolError(
                    f"ready marker generation {marker.worker_generation!r} "
                    f"!= expected {expected_worker_generation!r}",
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
    expected_request_id: str | None = None,
    child_process: Any | None = None,
    child_label: str = "child",
) -> CheckpointMarker:
    end = time.monotonic() + deadline
    while time.monotonic() < end:
        if child_process is not None:
            exit_code = child_process.poll()
            if exit_code is not None:
                raise ProtocolError(
                    f"{child_label} exited with code {exit_code} before checkpoint {name!r}",
                )
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
            if expected_request_id is not None and marker.request_id != expected_request_id:
                raise ProtocolError(
                    f"checkpoint {name!r} request_id {marker.request_id!r} "
                    f"!= expected {expected_request_id!r}",
                )
            return marker
        time.sleep(poll_interval)
    raise TimeoutError(
        f"checkpoint {name!r} for run_id {expected_run_id!r} not observed within {deadline}s",
    )


def wait_for_invocation_count(
    state_dir: Path,
    *,
    minimum: int,
    deadline: float,
    poll_interval: float = 0.05,
) -> list[HandlerInvocation]:
    end = time.monotonic() + deadline
    while time.monotonic() < end:
        invocations = read_invocations(state_dir)
        if len(invocations) >= minimum:
            return invocations
        time.sleep(poll_interval)
    raise TimeoutError(
        f"expected at least {minimum} handler invocations within {deadline}s",
    )


def assert_idle_outage_invocations(
    invocations: list[HandlerInvocation],
    *,
    run_id: str,
    baseline_execution_id: str,
    post_outage_execution_id: str,
    worker_pid: int,
    expected_jobs_by_execution: dict[str, str] | None = None,
) -> None:
    if len(invocations) != 2:
        raise ProtocolError(
            f"expected exactly two handler invocations, saw {len(invocations)}",
        )
    baseline = [item for item in invocations if item.execution_id == baseline_execution_id]
    post = [item for item in invocations if item.execution_id == post_outage_execution_id]
    if len(baseline) != 1 or len(post) != 1:
        raise ProtocolError("expected one invocation per execution identity")
    for item in invocations:
        if item.run_id != run_id:
            raise ProtocolError(f"invocation run_id mismatch {item.run_id!r}")
        if item.worker_generation != "1":
            raise ProtocolError("idle outage requires a single surviving worker generation")
        if item.pid != worker_pid:
            raise ProtocolError(
                f"invocation pid {item.pid} != surviving worker pid {worker_pid}",
            )
        if expected_jobs_by_execution is not None:
            if item.job != expected_jobs_by_execution.get(item.execution_id):
                raise ProtocolError("invocation Job name does not match its execution")
