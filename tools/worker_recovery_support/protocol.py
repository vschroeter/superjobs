"""Readiness, checkpoints, and handler-invocation evidence for worker recovery verification."""

from __future__ import annotations

import json
import os
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any

READY_FILE = "worker_ready.json"
STOP_FILE = "worker_stop.json"
INVOCATIONS_FILE = "handler_invocations.json"
PHASE_READY = "ready"
PHASE_STOP = "stop"
HANDLER_ENTERED = "handler_entered"
SUBMITTED = "submitted"
COMPLETION_SAVED = "completion_saved"
RETRY_PUBLISHED = "retry_published"
REPLACEMENT_ACK = "replacement_ack"


def handler_entered_checkpoint(worker_generation: str) -> str:
    return f"{HANDLER_ENTERED}_g{worker_generation}"


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
    terminal_state: str | None = None
    terminal_event_published: bool | None = None
    completion_state: str | None = None
    delivery_attempt: int | None = None
    retry_target_attempt: int | None = None


@dataclass(frozen=True, slots=True)
class StopMarker:
    run_id: str
    phase: str


@dataclass(frozen=True, slots=True)
class HandlerInvocation:
    run_id: str
    worker_generation: str
    execution_id: str
    pid: int


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
    terminal_state: str | None = None,
    terminal_event_published: bool | None = None,
    completion_state: str | None = None,
    delivery_attempt: int | None = None,
    retry_target_attempt: int | None = None,
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
    if terminal_state is not None:
        payload["terminal_state"] = terminal_state
    if terminal_event_published is not None:
        payload["terminal_event_published"] = terminal_event_published
    if completion_state is not None:
        payload["completion_state"] = completion_state
    if delivery_attempt is not None:
        payload["delivery_attempt"] = delivery_attempt
    if retry_target_attempt is not None:
        payload["retry_target_attempt"] = retry_target_attempt
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
    terminal_state = payload.get("terminal_state")
    if terminal_state is not None and not isinstance(terminal_state, str):
        raise ProtocolError(f"checkpoint {name!r} has invalid terminal_state field")
    terminal_event_published = payload.get("terminal_event_published")
    if terminal_event_published is not None and not isinstance(
        terminal_event_published,
        bool,
    ):
        raise ProtocolError(
            f"checkpoint {name!r} has invalid terminal_event_published field",
        )
    completion_state = payload.get("completion_state")
    if completion_state is not None and not isinstance(completion_state, str):
        raise ProtocolError(f"checkpoint {name!r} has invalid completion_state field")
    delivery_attempt = payload.get("delivery_attempt")
    if delivery_attempt is not None and not isinstance(delivery_attempt, int):
        raise ProtocolError(f"checkpoint {name!r} has invalid delivery_attempt field")
    retry_target_attempt = payload.get("retry_target_attempt")
    if retry_target_attempt is not None and not isinstance(retry_target_attempt, int):
        raise ProtocolError(f"checkpoint {name!r} has invalid retry_target_attempt field")
    return CheckpointMarker(
        run_id=run_id,
        checkpoint=checkpoint,
        job=job if isinstance(job, str) else None,
        execution_id=execution_id if isinstance(execution_id, str) else None,
        pid=pid if isinstance(pid, int) else None,
        worker_generation=worker_generation if isinstance(worker_generation, str) else None,
        terminal_state=terminal_state if isinstance(terminal_state, str) else None,
        terminal_event_published=(
            terminal_event_published if isinstance(terminal_event_published, bool) else None
        ),
        completion_state=completion_state if isinstance(completion_state, str) else None,
        delivery_attempt=delivery_attempt if isinstance(delivery_attempt, int) else None,
        retry_target_attempt=(
            retry_target_attempt if isinstance(retry_target_attempt, int) else None
        ),
    )


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
        execution_id = entry.get("execution_id")
        pid = entry.get("pid")
        if not isinstance(run_id, str) or not isinstance(generation, str):
            raise ProtocolError(f"invocation missing run_id or worker_generation in {path}")
        if not isinstance(execution_id, str):
            raise ProtocolError(f"invocation missing execution_id in {path}")
        if not isinstance(pid, int):
            raise ProtocolError(f"invocation missing pid in {path}")
        items.append(
            HandlerInvocation(
                run_id=run_id,
                worker_generation=generation,
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
    execution_id: str,
) -> int:
    path = invocations_path(state_dir)
    existing = read_invocations(state_dir)
    existing.append(
        HandlerInvocation(
            run_id=run_id,
            worker_generation=worker_generation,
            execution_id=execution_id,
            pid=os.getpid(),
        ),
    )
    payload = {
        "invocations": [
            {
                "run_id": item.run_id,
                "worker_generation": item.worker_generation,
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
    forbidden_pid: int | None = None,
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
            if forbidden_pid is not None and marker.pid == forbidden_pid:
                raise ProtocolError(
                    f"ready marker pid {marker.pid} matches forbidden replaced worker pid",
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
    child_process: Any | None = None,
    child_label: str = "worker",
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


def wait_for_replacement_ack(
    state_dir: Path,
    *,
    expected_run_id: str,
    expected_execution_id: str,
    deadline: float,
    poll_interval: float = 0.05,
    child_process: Any | None = None,
    child_label: str = "worker",
) -> CheckpointMarker:
    return wait_for_checkpoint(
        state_dir,
        name=REPLACEMENT_ACK,
        expected_run_id=expected_run_id,
        expected_execution_id=expected_execution_id,
        deadline=deadline,
        poll_interval=poll_interval,
        child_process=child_process,
        child_label=child_label,
    )


def assert_invocations_match_workers(
    invocations: list[HandlerInvocation],
    *,
    run_id: str,
    execution_id: str,
    allowed_pids_by_generation: dict[str, int],
) -> None:
    if not invocations:
        raise ProtocolError("missing handler invocation evidence")
    for item in invocations:
        if item.run_id != run_id:
            raise ProtocolError(
                f"invocation run_id {item.run_id!r} != expected {run_id!r}",
            )
        if item.execution_id != execution_id:
            raise ProtocolError(
                f"invocation execution_id {item.execution_id!r} "
                f"!= expected {execution_id!r}",
            )
        expected_pid = allowed_pids_by_generation.get(item.worker_generation)
        if expected_pid is None:
            raise ProtocolError(
                f"unexpected worker generation {item.worker_generation!r} in invocations",
            )
        if item.pid != expected_pid:
            raise ProtocolError(
                f"invocation pid {item.pid} != worker pid {expected_pid} "
                f"for generation {item.worker_generation!r}",
            )


def wait_for_worker_generation_invocation(
    state_dir: Path,
    *,
    worker_generation: str,
    deadline: float,
    poll_interval: float = 0.05,
) -> HandlerInvocation:
    end = time.monotonic() + deadline
    while time.monotonic() < end:
        for item in read_invocations(state_dir):
            if item.worker_generation == worker_generation:
                return item
        time.sleep(poll_interval)
    raise TimeoutError(
        f"expected handler invocation from worker generation {worker_generation!r} "
        f"within {deadline}s",
    )
