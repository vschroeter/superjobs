"""Scenario orchestration helpers for the resource admission verifier parent."""

from __future__ import annotations

import asyncio
import json
import subprocess
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, TextIO

from tools.cross_program_support.child_env import isolated_child_env
from tools.resource_admission_support import protocol as ra_protocol
from tools.resource_admission_support.evidence import (
    read_events,
    summarize_holder_overlap,
)
from tools.resource_admission_support.nats_control import publish_control
from tools.resource_admission_support.queue_config import (
    control_subject,
    queue_config_for_run,
)
from tools.verify_cross_program import (
    ChildRecord,
    CrossProgramError,
    _close_child_streams_safe,
    _finalize_child_after_kill,
    _spawn_child,
    _wait_for_producer_exit,
)


class ResourceAdmissionError(Exception):
    """Raised when resource admission verification fails."""


CONTROLLER_EXIT_OK = 0


@dataclass
class _TrackedChild:
    process: subprocess.Popen[str]
    record: ChildRecord
    streams: tuple[TextIO, TextIO]


def phase_deadline(scenario_deadline: float, kill_reap: float) -> float:
    """Absolute monotonic deadline for a scenario phase."""
    return scenario_deadline - kill_reap


def ready_deadline(
    scenario_deadline: float,
    kill_reap: float,
    *,
    cap_seconds: float,
) -> float:
    """Absolute monotonic deadline for worker readiness."""
    return min(
        phase_deadline(scenario_deadline, kill_reap), time.monotonic() + cap_seconds
    )


def spawn_worker(
    *,
    worker_python: Path,
    worker_dir: Path,
    env: dict[str, str],
    log_dir: Path,
    label: str,
) -> tuple[subprocess.Popen[str], ChildRecord, TextIO, TextIO]:
    return _spawn_child(
        name=label,
        python=worker_python,
        role_dir=worker_dir,
        script="worker_app.py",
        cwd=worker_dir,
        env=env,
        log_dir=log_dir,
    )


def spawn_controller(
    *,
    controller_python: Path,
    controller_dir: Path,
    env: dict[str, str],
    log_dir: Path,
    label: str,
) -> tuple[subprocess.Popen[str], ChildRecord, TextIO, TextIO]:
    return _spawn_child(
        name=label,
        python=controller_python,
        role_dir=controller_dir,
        script="controller_app.py",
        cwd=controller_dir,
        env=env,
        log_dir=log_dir,
    )


def run_controller_phase(
    *,
    controller_python: Path,
    controller_dir: Path,
    base_env: dict[str, str],
    phase_env: dict[str, str],
    log_dir: Path,
    label: str,
    scenario_deadline: float,
    kill_reap: float,
    worker_process: subprocess.Popen[str] | None = None,
) -> ChildRecord:
    env = isolated_child_env({**base_env, **phase_env})
    process, record, out, err = spawn_controller(
        controller_python=controller_python,
        controller_dir=controller_dir,
        env=env,
        log_dir=log_dir,
        label=label,
    )
    exit_code: int | None = None
    failure: BaseException | None = None
    cleanup_deadline = scenario_deadline
    try:
        exit_code = _wait_for_producer_exit(
            process,
            label=label,
            deadline=cleanup_deadline,
            worker_process=worker_process,
            expected_worker_code=None,
        )
    except CrossProgramError as exc:
        failure = exc
    except BaseException as exc:
        failure = exc
    finally:
        if process.poll() is None:
            try:
                _finalize_child_after_kill(process, record, deadline=cleanup_deadline)
            except Exception as kill_exc:
                if failure is None:
                    failure = kill_exc
        _close_child_streams_safe(out, err)
        record.ended_at = time.monotonic()
        if exit_code is not None:
            record.exit_code = exit_code

    if failure is not None:
        stdout = (
            record.stdout_path.read_text(encoding="utf-8") if record.stdout_path else ""
        )
        stderr = (
            record.stderr_path.read_text(encoding="utf-8") if record.stderr_path else ""
        )
        raise ResourceAdmissionError(
            f"{label} failed: {failure}; stdout={stdout!r}; stderr={stderr!r}",
        ) from failure
    if exit_code != CONTROLLER_EXIT_OK:
        stdout = (
            record.stdout_path.read_text(encoding="utf-8") if record.stdout_path else ""
        )
        stderr = (
            record.stderr_path.read_text(encoding="utf-8") if record.stderr_path else ""
        )
        raise ResourceAdmissionError(
            f"{label} exit {exit_code} != {CONTROLLER_EXIT_OK}; stdout={stdout!r}; stderr={stderr!r}",
        )
    return record


def publish_release(
    nats_url: str,
    run_id: str,
    *,
    execution_id: str,
    ack_wait: float = 3.0,
) -> None:
    subject = control_subject(queue_config_for_run(run_id, ack_wait=ack_wait), run_id)
    asyncio.run(
        publish_control(
            nats_url,
            subject,
            {
                "type": "release_holder",
                "run_id": run_id,
                "execution_id": execution_id,
            },
        ),
    )


def execution_id_from_submitted(state_dir: Path, run_id: str, job: str) -> str:
    marker = ra_protocol.read_checkpoint(state_dir, ra_protocol.SUBMITTED, job=job)
    if marker.run_id != run_id or not marker.execution_id:
        raise ResourceAdmissionError(
            f"submitted checkpoint missing execution_id for job {job!r}"
        )
    return marker.execution_id


def assert_holder_evidence_from_state(
    state_dir: Path,
    *,
    maximum: int = 1,
    minimum_enters: int = 1,
) -> int:
    summary = summarize_holder_overlap(read_events(state_dir))
    if summary.max_concurrent > maximum:
        raise ResourceAdmissionError(
            f"resource holder overlap {summary.max_concurrent} exceeds maximum {maximum}",
        )
    if summary.enter_count < minimum_enters:
        raise ResourceAdmissionError(
            f"expected at least {minimum_enters} holder enter events, "
            f"saw {summary.enter_count}",
        )
    if summary.exit_count < minimum_enters:
        raise ResourceAdmissionError(
            f"expected at least {minimum_enters} holder exit events, "
            f"saw {summary.exit_count}",
        )
    return summary.max_concurrent


def finalize_tracked(
    tracked: list[_TrackedChild],
    *,
    deadline: float,
) -> None:
    errors: list[str] = []
    for item in tracked:
        if item.process.poll() is None:
            try:
                _finalize_child_after_kill(item.process, item.record, deadline=deadline)
            except Exception as exc:
                errors.append(f"{item.record.name}: {exc}")
        try:
            _close_child_streams_safe(*item.streams)
        except Exception as exc:
            errors.append(f"{item.record.name} streams: {exc}")
    if errors:
        raise ResourceAdmissionError("cleanup failed: " + "; ".join(errors))


def load_owner_snapshot(state_dir: Path) -> dict[str, Any]:
    path = state_dir / "owner_snapshot.json"
    if not path.is_file():
        raise ResourceAdmissionError("owner_snapshot.json missing")
    return json.loads(path.read_text(encoding="utf-8"))
