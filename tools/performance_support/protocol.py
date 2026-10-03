"""Readiness and cooperative stop markers for performance runs."""

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
RESULTS_FILE = "producer_results.json"


class ProtocolError(Exception):
    """Invalid or stale readiness marker."""


@dataclass(frozen=True, slots=True)
class ReadyMarker:
    run_id: str
    phase: str


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
    _write_json(ready_path(state_dir), {"run_id": run_id, "phase": PHASE_READY})


def read_ready(state_dir: Path) -> ReadyMarker:
    payload = _read_json(ready_path(state_dir))
    run_id = payload.get("run_id")
    phase = payload.get("phase")
    if not isinstance(run_id, str) or not run_id:
        raise ProtocolError("ready marker missing run_id")
    if phase != PHASE_READY:
        raise ProtocolError(f"ready marker has unexpected phase {phase!r}")
    return ReadyMarker(run_id=run_id, phase=phase)


def stop_path(state_dir: Path) -> Path:
    return state_dir / STOP_FILE


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


def results_path(state_dir: Path) -> Path:
    return state_dir / RESULTS_FILE


def write_producer_results(state_dir: Path, payload: dict[str, Any]) -> None:
    _write_json(results_path(state_dir), payload)


def read_producer_results(state_dir: Path) -> dict[str, Any]:
    return _read_json(results_path(state_dir))


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
