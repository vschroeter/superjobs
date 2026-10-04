"""Readiness markers for CLI process verification."""

from __future__ import annotations

import json
import os
import time
from pathlib import Path
from typing import Any


class ProtocolError(Exception):
    """Invalid readiness or checkpoint marker."""


def sync_dir_from_env() -> Path:
    raw = os.environ.get("SUPERJOBS_CLI_SYNC_DIR") or os.environ.get("SUPERJOBS_CLI_STATE_DIR")
    if not raw:
        raise ProtocolError("SUPERJOBS_CLI_SYNC_DIR or SUPERJOBS_CLI_STATE_DIR must be set")
    return Path(raw)


def write_json_atomic(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(path.name + ".tmp")
    tmp.write_text(json.dumps(payload, sort_keys=True) + "\n", encoding="utf-8")
    tmp.replace(path)


def read_json(path: Path) -> dict[str, Any]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ProtocolError(f"marker must be object at {path}")
    return payload


def checkpoint_path(state_dir: Path, name: str, *, gate: str | None = None) -> Path:
    if gate:
        return state_dir / f"checkpoint_{name}_{gate}.json"
    return state_dir / f"checkpoint_{name}.json"


def release_path(state_dir: Path, gate: str) -> Path:
    return state_dir / f"release_{gate}.json"


def assert_checkpoint_absent(
    state_dir: Path,
    name: str,
    *,
    gate: str | None = None,
) -> None:
    path = checkpoint_path(state_dir, name, gate=gate)
    if path.is_file():
        raise ProtocolError(f"checkpoint {name!r} must not exist at {path}")


def _validate_checkpoint(
    payload: dict[str, Any],
    *,
    run_id: str | None,
    gate: str | None,
    execution_id: str | None,
) -> None:
    if run_id is not None and payload.get("run_id") != run_id:
        raise ProtocolError("checkpoint run_id mismatch")
    if gate is not None and payload.get("gate") != gate:
        raise ProtocolError("checkpoint gate mismatch")
    if execution_id is not None and payload.get("execution_id") != execution_id:
        raise ProtocolError("checkpoint execution_id mismatch")


def wait_for_worker_ready(
    state_dir: Path,
    *,
    run_id: str,
    deadline: float,
    poll_interval: float = 0.05,
    child_process: Any | None = None,
) -> None:
    ready = state_dir / "worker_ready.json"
    end = time.monotonic() + deadline
    while time.monotonic() < end:
        if child_process is not None and child_process.poll() is not None:
            raise ProtocolError(
                f"worker exited {child_process.returncode} before ready",
            )
        if ready.is_file():
            payload = read_json(ready)
            if payload.get("run_id") != run_id:
                raise ProtocolError("ready run_id mismatch")
            return
        time.sleep(poll_interval)
    raise TimeoutError(f"worker not ready within {deadline}s")


def wait_for_checkpoint(
    state_dir: Path,
    name: str,
    *,
    deadline: float,
    poll_interval: float = 0.05,
    gate: str | None = None,
    run_id: str | None = None,
    expected_execution_id: str | None = None,
) -> dict[str, Any]:
    path = checkpoint_path(state_dir, name, gate=gate)
    end = time.monotonic() + deadline
    while time.monotonic() < end:
        if path.is_file():
            payload = read_json(path)
            _validate_checkpoint(
                payload,
                run_id=run_id,
                gate=gate,
                execution_id=expected_execution_id,
            )
            return payload
        time.sleep(poll_interval)
    raise TimeoutError(f"checkpoint {name!r} not observed within {deadline}s")


def write_worker_stop(state_dir: Path, run_id: str) -> None:
    write_json_atomic(state_dir / "worker_stop.json", {"run_id": run_id, "phase": "stop"})


def write_gate_release(state_dir: Path, gate: str, *, run_id: str | None = None) -> None:
    payload: dict[str, Any] = {"released": True, "gate": gate}
    if run_id is not None:
        payload["run_id"] = run_id
    write_json_atomic(release_path(state_dir, gate), payload)
