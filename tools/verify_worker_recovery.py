#!/usr/bin/env python3
"""Verify worker crash recovery before and after durable completion."""

from __future__ import annotations

import argparse
import hashlib
import json
import shutil
import signal
import subprocess
import sys
import tempfile
import time
import uuid
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, TextIO

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))
SUPPORT_ROOT = Path(__file__).resolve().parent / "worker_recovery_support"
ORIGIN_PROBE = Path(__file__).resolve().parent / "wheel_origin_probe.py"

from tests.support.nats_harness.server import (  # noqa: E402
    NatsServerTarget,
    OwnedNatsServer,
)
from tests.support.nats_harness.pinned import SHUTDOWN_TIMEOUT_SECONDS  # noqa: E402
from tests.support.nats_harness.provision import resolve_executable  # noqa: E402

from tools.cross_program_support.child_env import (  # noqa: E402
    isolated_child_env,
    python_command,
    subprocess_creationflags,
)
from tools.cross_program_support.protocol import write_worker_stop  # noqa: E402
from tools.verify_contract_typing import (  # noqa: E402
    DEFAULT_RUNTIMES,
    REPO_ROOT as TYPING_REPO_ROOT,
    VerificationError,
    assert_final_runtime_python,
    assert_work_parent_outside_repo,
    build_wheels,
    create_wheel_venv,
    run_cmd,
    runtime_evidence_tag,
    venv_python,
)
from tools.verify_cross_program import (  # noqa: E402
    ChildRecord,
    CrossProgramError,
    _close_child_streams_safe,
    _collect_checkpoint_snapshot,
    _finalize_child_after_kill,
    _reap_child,
    _snapshot_json_file,
    _spawn_child,
    _shutdown_worker_cooperatively,
    _wait_for_producer_exit,
    _write_evidence,
)
from tools.worker_recovery_support import protocol as wr_protocol  # noqa: E402
from tools.worker_recovery_support.spawn_pid import marker_pid_in_spawn_tree  # noqa: E402

assert TYPING_REPO_ROOT == REPO_ROOT

CHILD_TIMEOUT_SECONDS = 30
SCENARIO_TIMEOUT_SECONDS = 90
KILL_REAP_SECONDS = 5
POLL_INTERVAL_SECONDS = 0.05
WORKER_EXIT_OK = 0
PRODUCER_EXIT_OK = 0

ROLE_SHARED_FILES = (
    "protocol.py",
    "scenario_jobs.py",
    "runtime_isolation.py",
)
WORKER_SUPPORT_FILES = ROLE_SHARED_FILES + (
    "worker_handlers.py",
    "worker_app.py",
    "blocking_backend.py",
    "delivery_seam.py",
    "queue_config.py",
)
PRODUCER_SUPPORT_FILES = ROLE_SHARED_FILES + ("producer_app.py", "queue_config.py")

SCENARIO_BEFORE = "recovery_before_completion"
SCENARIO_AFTER = "recovery_after_completion"
RECOVERY_SCENARIOS = (SCENARIO_BEFORE, SCENARIO_AFTER)


class WorkerRecoveryError(Exception):
    """Raised when worker recovery verification fails."""


@dataclass
class RecoveryScenarioRecord:
    name: str
    run_id: str
    state_dir: Path
    broker_store_dir: Path
    broker_store_fingerprint_before: str
    broker_store_fingerprint_after: str
    started_at: float
    ended_at: float | None = None
    execution_id: str | None = None
    invocations: list[dict[str, Any]] | None = None
    workers: list[ChildRecord] = field(default_factory=list)
    producers: list[ChildRecord] = field(default_factory=list)
    checkpoints: dict[str, Any] | None = None
    broker_pid: int | None = None
    broker_config_path: str | None = None
    broker_config_fingerprint: str | None = None
    generation_snapshots: dict[str, Any] | None = None
    error: str | None = None


@dataclass
class PythonPassState:
    python_version: str
    tag: str
    producer_py: Path
    worker_py: Path
    producer_dir: Path
    worker_dir: Path
    scenario_records: list[RecoveryScenarioRecord] = field(default_factory=list)
    origins: dict[str, Any] | None = None
    runtime_version: str | None = None
    error: str | None = None


def materialize_role_dir(work_dir: Path, role: str, files: tuple[str, ...]) -> Path:
    dest = work_dir / role
    dest.mkdir(parents=True, exist_ok=True)
    for name in files:
        shutil.copy2(SUPPORT_ROOT / name, dest / name)
    return dest


def probe_recovery_origins(
    python: Path,
    role_dir: Path,
    *,
    role: str,
    evidence_dir: Path | None,
    python_version: str,
) -> dict[str, Any]:
    probe_dir = role_dir / f"origin_probe_{role}"
    probe_dir.mkdir(parents=True, exist_ok=True)
    files = PRODUCER_SUPPORT_FILES if role == "producer" else WORKER_SUPPORT_FILES
    for name in files:
        shutil.copy2(role_dir / name, probe_dir / name)
    if role == "producer":
        shutil.copy2(probe_dir / "producer_app.py", probe_dir / "producer.py")
    shutil.copy2(ORIGIN_PROBE, probe_dir / "wheel_origin_probe.py")
    command = python_command(
        str(python),
        probe_dir,
        "wheel_origin_probe.py",
        str(probe_dir),
        str(REPO_ROOT),
        "--role",
        role,
    )
    completed = run_cmd(command, cwd=probe_dir, env=isolated_child_env())
    evidence = json.loads(completed.stdout)
    if evidence_dir is not None:
        evidence_dir.mkdir(parents=True, exist_ok=True)
        tag = runtime_evidence_tag(python_version)
        (evidence_dir / f"{tag}-{role}-origin.json").write_text(
            json.dumps(evidence, indent=2),
            encoding="utf-8",
        )
    return evidence


def _broker_store_dir(target: NatsServerTarget) -> Path:
    if target.work_dir is None:
        raise WorkerRecoveryError("owned broker missing work_dir")
    return target.work_dir / "store"


def _format_pass_error(exc: BaseException) -> str:
    message = str(exc)
    if message:
        return message
    return f"{type(exc).__name__}"


def _wait_budget_seconds(scenario_deadline: float) -> float:
    return max(0.0, scenario_deadline - time.monotonic() - KILL_REAP_SECONDS)


def _broker_config_fingerprint(config_path: Path | None) -> str:
    if config_path is None or not config_path.is_file():
        return "missing"
    digest = hashlib.sha256(config_path.read_bytes()).hexdigest()
    return f"sha256:{digest}"


def _assert_broker_identity_unchanged(
    broker_target: NatsServerTarget,
    *,
    expected_pid: int,
    expected_config_path: Path,
    expected_config_fingerprint: str,
) -> None:
    process = broker_target.process
    if process is None:
        raise WorkerRecoveryError("broker process handle missing during scenario")
    if process.poll() is not None:
        raise WorkerRecoveryError(
            f"broker process exited with code {process.poll()} during scenario",
        )
    if process.pid != expected_pid:
        raise WorkerRecoveryError(
            f"broker pid changed from {expected_pid} to {process.pid}",
        )
    if not expected_config_path.is_file():
        raise WorkerRecoveryError("broker config path missing during scenario")
    fingerprint = _broker_config_fingerprint(expected_config_path)
    if fingerprint != expected_config_fingerprint:
        raise WorkerRecoveryError(
            "broker config changed during worker replacement "
            f"({expected_config_fingerprint} -> {fingerprint})",
        )


def _assert_ready_pid_matches_spawn(
    process: subprocess.Popen[str],
    marker: wr_protocol.ReadyMarker,
    *,
    label: str,
) -> None:
    spawn_pid = process.pid
    if spawn_pid is None:
        raise WorkerRecoveryError(f"{label} spawn pid missing")
    if not marker_pid_in_spawn_tree(spawn_pid, marker.pid):
        raise WorkerRecoveryError(
            f"{label} ready pid {marker.pid} not in spawn tree rooted at {spawn_pid}",
        )


def _capture_generation_snapshot(state_dir: Path, generation: str) -> dict[str, Any]:
    snapshot: dict[str, Any] = {"worker_generation": generation}
    ready_file = wr_protocol.ready_path(state_dir)
    if ready_file.is_file():
        snapshot["worker_ready.json"] = _snapshot_json_file(ready_file)
    for name in (wr_protocol.SUBMITTED, wr_protocol.COMPLETION_SAVED):
        path = wr_protocol.checkpoint_path(state_dir, name)
        if path.is_file():
            snapshot[path.name] = _snapshot_json_file(path)
    return snapshot


def _broker_store_fingerprint(store_dir: Path) -> str:
    if not store_dir.is_dir():
        return "missing"
    total_size = 0
    file_count = 0
    for path in store_dir.rglob("*"):
        if path.is_file():
            file_count += 1
            total_size += path.stat().st_size
    return f"files={file_count}:bytes={total_size}"


def _hard_kill_worker(
    process: subprocess.Popen[str],
    record: ChildRecord,
    *,
    deadline: float,
) -> int:
    pid = record.pid
    if process.poll() is not None:
        raise WorkerRecoveryError(
            f"worker pid {pid} already exited with code {process.poll()} before hard kill",
        )
    if pid is None or not marker_pid_in_spawn_tree(process.pid, pid):
        raise WorkerRecoveryError("worker interpreter does not belong to the owned spawn tree")
    interpreter = None
    kill_deadline = min(deadline, time.monotonic() + KILL_REAP_SECONDS)
    if sys.platform == "win32":
        from tools.worker_recovery_support.windows_process import OwnedWindowsProcess

        interpreter = OwnedWindowsProcess(pid)
    try:
        if interpreter is not None and interpreter.exited(deadline=time.monotonic()):
            raise WorkerRecoveryError("worker interpreter exited before the controlled kill")
        killed_at = time.monotonic()
        process.kill()
        _finalize_child_after_kill(process, record, deadline=kill_deadline)
        exit_code = process.poll()
        expected_code = 1 if sys.platform == "win32" else -signal.SIGKILL
        if exit_code != expected_code:
            raise WorkerRecoveryError(f"controlled kill exit {exit_code} != expected {expected_code}")
        if interpreter is not None and not interpreter.exited(deadline=kill_deadline - 1):
            interpreter.terminate()
            interpreter.exited(deadline=kill_deadline)
            raise WorkerRecoveryError("worker interpreter survived launcher kill; forcibly cleaned up")
        record.termination = {
            "launcher_pid": process.pid,
            "interpreter_pid": pid,
            "kill_started_at_monotonic": killed_at,
            "confirmed_at_monotonic": time.monotonic(),
            "interpreter_terminated": True,
            "launcher_exit": exit_code,
        }
        record.exit_code = exit_code
        return exit_code
    finally:
        if interpreter is not None:
            try:
                if not interpreter.exited(deadline=time.monotonic()):
                    interpreter.terminate()
                    interpreter.exited(deadline=kill_deadline)
            finally:
                interpreter.close()


def _validate_completion_saved_checkpoint(
    marker: wr_protocol.CheckpointMarker,
    *,
    worker_record: ChildRecord,
    execution_id: str,
) -> None:
    if marker.pid != worker_record.pid:
        raise WorkerRecoveryError(
            f"completion_saved pid {marker.pid} != worker pid {worker_record.pid}",
        )
    if marker.worker_generation != "1":
        raise WorkerRecoveryError(
            f"completion_saved generation {marker.worker_generation!r} != '1'",
        )
    if marker.execution_id != execution_id:
        raise WorkerRecoveryError("completion_saved execution_id mismatch")
    if marker.completion_state != "COMPLETED":
        raise WorkerRecoveryError(
            f"completion_saved state {marker.completion_state!r} != COMPLETED",
        )
    if marker.terminal_event_published is not False:
        raise WorkerRecoveryError(
            "completion_saved requires terminal_event_published false at checkpoint",
        )


def _validate_replacement_ack(
    marker: wr_protocol.CheckpointMarker,
    *,
    worker_record: ChildRecord,
    execution_id: str,
) -> None:
    if marker.pid != worker_record.pid:
        raise WorkerRecoveryError(
            f"replacement_ack pid {marker.pid} != worker pid {worker_record.pid}",
        )
    if marker.worker_generation != "2":
        raise WorkerRecoveryError("replacement_ack must be from worker generation 2")
    if marker.execution_id != execution_id:
        raise WorkerRecoveryError("replacement_ack execution_id mismatch")
    if marker.terminal_state != "COMPLETED":
        raise WorkerRecoveryError(
            f"replacement_ack terminal_state {marker.terminal_state!r} != COMPLETED",
        )
    if marker.terminal_event_published is not True:
        raise WorkerRecoveryError(
            "replacement_ack requires terminal_event_published true after recovery",
        )


def _validate_handler_entered_checkpoint(
    marker: wr_protocol.CheckpointMarker,
    *,
    worker_record: ChildRecord,
    execution_id: str,
    worker_generation: str,
) -> None:
    if marker.worker_generation != worker_generation:
        raise WorkerRecoveryError(
            f"handler_entered generation {marker.worker_generation!r} "
            f"!= expected {worker_generation!r}",
        )
    if marker.pid != worker_record.pid:
        raise WorkerRecoveryError(
            f"handler_entered pid {marker.pid} != worker pid {worker_record.pid}",
        )
    if marker.execution_id != execution_id:
        raise WorkerRecoveryError("handler_entered execution_id mismatch")


def _collect_recovery_snapshot(state_dir: Path) -> dict[str, Any]:
    snapshot = _collect_checkpoint_snapshot(state_dir)
    invocations_path = wr_protocol.invocations_path(state_dir)
    if invocations_path.is_file():
        snapshot["handler_invocations.json"] = _snapshot_json_file(invocations_path)
    return snapshot


@dataclass
class _TrackedChild:
    process: subprocess.Popen[str]
    record: ChildRecord
    streams: tuple[TextIO, TextIO]


def _finalize_tracked_children(
    tracked: list[_TrackedChild],
    *,
    deadline: float,
) -> None:
    errors: list[str] = []
    for item in tracked:
        existing = item.process.poll()
        if existing is not None:
            item.record.exit_code = existing
            if item.record.ended_at is None:
                item.record.ended_at = time.monotonic()
        elif item.process.poll() is None:
            try:
                _finalize_child_after_kill(item.process, item.record, deadline=deadline)
            except BaseException as exc:
                errors.append(f"{item.record.name}: {exc}")
        try:
            _close_child_streams_safe(*item.streams)
        except BaseException as exc:
            errors.append(f"{item.record.name} streams: {exc}")
    if errors:
        raise WorkerRecoveryError("child cleanup failed: " + "; ".join(errors))
    for item in tracked:
        if item.process.poll() is None:
            raise WorkerRecoveryError(
                f"{item.record.name} pid {item.record.pid} still alive after cleanup",
            )


def _clear_worker_ready(state_dir: Path) -> None:
    wr_protocol.ready_path(state_dir).unlink(missing_ok=True)


def _spawn_worker(
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


def _spawn_producer(
    *,
    producer_python: Path,
    producer_dir: Path,
    env: dict[str, str],
    log_dir: Path,
    label: str,
) -> tuple[subprocess.Popen[str], ChildRecord, TextIO, TextIO]:
    return _spawn_child(
        name=label,
        python=producer_python,
        role_dir=producer_dir,
        script="producer_app.py",
        cwd=producer_dir,
        env=env,
        log_dir=log_dir,
    )


def _wait_producer_ok(
    process: subprocess.Popen[str],
    record: ChildRecord,
    streams: tuple[TextIO, TextIO],
    *,
    deadline: float,
    worker_process: subprocess.Popen[str] | None = None,
) -> None:
    try:
        code = _wait_for_producer_exit(
            process,
            label=record.name,
            deadline=deadline,
            worker_process=worker_process,
            expected_worker_code=None,
        )
    except CrossProgramError as exc:
        stdout = record.stdout_path.read_text(encoding="utf-8") if record.stdout_path else ""
        stderr = record.stderr_path.read_text(encoding="utf-8") if record.stderr_path else ""
        raise WorkerRecoveryError(
            f"{exc}; producer stdout={stdout!r}; stderr={stderr!r}",
        ) from exc
    if code != PRODUCER_EXIT_OK:
        stdout = record.stdout_path.read_text(encoding="utf-8") if record.stdout_path else ""
        stderr = record.stderr_path.read_text(encoding="utf-8") if record.stderr_path else ""
        raise WorkerRecoveryError(
            f"{record.name} exit {code} != expected {PRODUCER_EXIT_OK}; "
            f"stdout={stdout!r}; stderr={stderr!r}",
        )
    record.exit_code = code
    record.ended_at = time.monotonic()
    _close_child_streams_safe(*streams)


def _child_record_json(child: ChildRecord) -> dict[str, Any]:
    ended = child.ended_at if child.ended_at is not None else child.started_at
    return {
        "name": child.name,
        "command": child.command,
        "cwd": str(child.cwd),
        "pid": child.pid,
        "started_at_monotonic": child.started_at,
        "ended_at_monotonic": child.ended_at,
        "duration_seconds": ended - child.started_at,
        "exit_code": child.exit_code,
        "termination": getattr(child, "termination", None),
        "stdout_path": str(child.stdout_path) if child.stdout_path else None,
        "stderr_path": str(child.stderr_path) if child.stderr_path else None,
    }


def _recovery_record_to_json(item: RecoveryScenarioRecord) -> dict[str, Any]:
    return {
        "name": item.name,
        "run_id": item.run_id,
        "execution_id": item.execution_id,
        "broker_store_dir": str(item.broker_store_dir),
        "broker_store_fingerprint_before": item.broker_store_fingerprint_before,
        "broker_store_fingerprint_after": item.broker_store_fingerprint_after,
        "broker_pid": item.broker_pid,
        "broker_config_path": item.broker_config_path,
        "broker_config_fingerprint": item.broker_config_fingerprint,
        "generation_snapshots": item.generation_snapshots,
        "started_at_monotonic": item.started_at,
        "ended_at_monotonic": item.ended_at,
        "duration_seconds": (item.ended_at or item.started_at) - item.started_at,
        "error": item.error,
        "checkpoints": item.checkpoints,
        "invocations": item.invocations,
        "workers": [_child_record_json(worker) for worker in item.workers],
        "producers": [_child_record_json(producer) for producer in item.producers],
    }


def _write_recovery_artifact(path: Path, record: RecoveryScenarioRecord) -> None:
    _write_evidence(path / "scenario.json", _recovery_record_to_json(record))
    for child in (*record.workers, *record.producers):
        for stream_path in (child.stdout_path, child.stderr_path):
            if stream_path is None or not stream_path.is_file():
                continue
            destination = path / stream_path.name
            if stream_path.resolve() == destination.resolve():
                continue
            shutil.copy2(stream_path, destination)


def _run_recovery_scenario(
    *,
    scenario: str,
    nats_url: str,
    broker_target: NatsServerTarget,
    producer_python: Path,
    worker_python: Path,
    producer_dir: Path,
    worker_dir: Path,
    artifact_scenario_dir: Path | None,
    scenario_deadline: float,
) -> RecoveryScenarioRecord:
    run_id = uuid.uuid4().hex
    state_dir = producer_dir / "state" / scenario / run_id
    state_dir.mkdir(parents=True, exist_ok=True)
    log_dir = (
        artifact_scenario_dir / scenario / run_id
        if artifact_scenario_dir is not None
        else state_dir / "logs"
    )
    store_dir = _broker_store_dir(broker_target)
    broker_config = (
        broker_target.work_dir / "server.conf"
        if broker_target.work_dir is not None
        else None
    )
    broker_config_fp = _broker_config_fingerprint(broker_config)
    record = RecoveryScenarioRecord(
        name=scenario,
        run_id=run_id,
        state_dir=state_dir,
        broker_store_dir=store_dir,
        broker_store_fingerprint_before=_broker_store_fingerprint(store_dir),
        broker_store_fingerprint_after="",
        started_at=time.monotonic(),
        broker_pid=(
            broker_target.process.pid if broker_target.process is not None else None
        ),
        broker_config_path=str(broker_config) if broker_config is not None else None,
        broker_config_fingerprint=broker_config_fp,
    )
    base_env = {
        "NATS_URL": nats_url,
        "SUPERJOBS_CROSS_RUN_ID": run_id,
        "SUPERJOBS_CROSS_SCENARIO": scenario,
        "SUPERJOBS_CROSS_STATE_DIR": str(state_dir),
    }
    worker_process: subprocess.Popen[str] | None = None
    worker_record: ChildRecord | None = None
    worker_record2: ChildRecord | None = None
    worker_streams: tuple[TextIO, TextIO] | None = None
    tracked: list[_TrackedChild] = []
    try:
        worker_env = isolated_child_env(
            {**base_env, "SUPERJOBS_WORKER_GENERATION": "1"},
        )
        worker_process, worker_record, w_out, w_err = _spawn_worker(
            worker_python=worker_python,
            worker_dir=worker_dir,
            env=worker_env,
            log_dir=log_dir,
            label="worker-1",
        )
        record.workers.append(worker_record)
        worker_streams = (w_out, w_err)
        tracked.append(_TrackedChild(worker_process, worker_record, worker_streams))
        ready_g1 = wr_protocol.wait_for_ready(
            state_dir,
            expected_run_id=run_id,
            deadline=max(
                0.0,
                min(CHILD_TIMEOUT_SECONDS, _wait_budget_seconds(scenario_deadline)),
            ),
            child_process=worker_process,
            expected_worker_generation="1",
        )
        _assert_ready_pid_matches_spawn(worker_process, ready_g1, label="worker-1")
        worker_record.pid = ready_g1.pid

        submit_env = isolated_child_env(
            {**base_env, "SUPERJOBS_RECOVERY_PHASE": "submit"},
        )
        producer_process, producer_record, p_out, p_err = _spawn_producer(
            producer_python=producer_python,
            producer_dir=producer_dir,
            env=submit_env,
            log_dir=log_dir,
            label="producer-submit",
        )
        record.producers.append(producer_record)
        tracked.append(_TrackedChild(producer_process, producer_record, (p_out, p_err)))
        _wait_producer_ok(
            producer_process,
            producer_record,
            (p_out, p_err),
            deadline=scenario_deadline,
            worker_process=worker_process,
        )

        submitted = wr_protocol.wait_for_checkpoint(
            state_dir,
            name=wr_protocol.SUBMITTED,
            expected_run_id=run_id,
            deadline=_wait_budget_seconds(scenario_deadline),
            child_process=worker_process,
        )
        execution_id = submitted.execution_id
        if not execution_id:
            raise WorkerRecoveryError("submit checkpoint missing execution_id")
        record.execution_id = execution_id

        handler_g1 = wr_protocol.handler_entered_checkpoint("1")
        if scenario == SCENARIO_BEFORE:
            handler_marker = wr_protocol.wait_for_checkpoint(
                state_dir,
                name=handler_g1,
                expected_run_id=run_id,
                expected_execution_id=execution_id,
                deadline=_wait_budget_seconds(scenario_deadline),
                child_process=worker_process,
            )
            _validate_handler_entered_checkpoint(
                handler_marker,
                worker_record=worker_record,
                execution_id=execution_id,
                worker_generation="1",
            )
            wr_protocol.wait_for_invocation_count(
                state_dir,
                minimum=1,
                deadline=_wait_budget_seconds(scenario_deadline),
            )
        else:
            completion_saved = wr_protocol.wait_for_checkpoint(
                state_dir,
                name=wr_protocol.COMPLETION_SAVED,
                expected_run_id=run_id,
                expected_execution_id=execution_id,
                deadline=_wait_budget_seconds(scenario_deadline),
                child_process=worker_process,
            )
            _validate_completion_saved_checkpoint(
                completion_saved,
                worker_record=worker_record,
                execution_id=execution_id,
            )
            handler_marker = wr_protocol.wait_for_checkpoint(
                state_dir,
                name=handler_g1,
                expected_run_id=run_id,
                expected_execution_id=execution_id,
                deadline=_wait_budget_seconds(scenario_deadline),
                child_process=worker_process,
            )
            _validate_handler_entered_checkpoint(
                handler_marker,
                worker_record=worker_record,
                execution_id=execution_id,
                worker_generation="1",
            )
            invocations = wr_protocol.wait_for_invocation_count(
                state_dir,
                minimum=1,
                deadline=_wait_budget_seconds(scenario_deadline),
            )
            if len(invocations) != 1:
                raise WorkerRecoveryError(
                    f"expected exactly one handler invocation before kill, saw {len(invocations)}",
                )

        kill_exit = _hard_kill_worker(
            worker_process,
            worker_record,
            deadline=scenario_deadline,
        )
        worker_record.exit_code = kill_exit
        worker_record.ended_at = time.monotonic()
        _close_child_streams_safe(*worker_streams)
        worker_process = None
        worker_streams = None

        if not store_dir.is_dir():
            raise WorkerRecoveryError("broker store missing after first worker kill")

        record.generation_snapshots = {
            "1": _capture_generation_snapshot(state_dir, "1"),
        }
        _clear_worker_ready(state_dir)
        replacement_env = isolated_child_env(
            {**base_env, "SUPERJOBS_WORKER_GENERATION": "2"},
        )
        worker_process, worker_record2, w_out2, w_err2 = _spawn_worker(
            worker_python=worker_python,
            worker_dir=worker_dir,
            env=replacement_env,
            log_dir=log_dir,
            label="worker-2",
        )
        record.workers.append(worker_record2)
        worker_streams = (w_out2, w_err2)
        tracked.append(_TrackedChild(worker_process, worker_record2, worker_streams))
        ready_g2 = wr_protocol.wait_for_ready(
            state_dir,
            expected_run_id=run_id,
            deadline=max(
                0.0,
                min(CHILD_TIMEOUT_SECONDS, _wait_budget_seconds(scenario_deadline)),
            ),
            child_process=worker_process,
            expected_worker_generation="2",
            forbidden_pid=worker_record.pid,
        )
        _assert_ready_pid_matches_spawn(worker_process, ready_g2, label="worker-2")
        worker_record2.pid = ready_g2.pid

        assert broker_target.process is not None
        assert broker_config is not None
        _assert_broker_identity_unchanged(
            broker_target,
            expected_pid=record.broker_pid or broker_target.process.pid,
            expected_config_path=broker_config,
            expected_config_fingerprint=broker_config_fp,
        )

        recover_env = isolated_child_env(
            {
                **base_env,
                "SUPERJOBS_RECOVERY_PHASE": "recover",
                "SUPERJOBS_EXECUTION_ID": execution_id,
            },
        )
        producer_process2, producer_record2, p_out2, p_err2 = _spawn_producer(
            producer_python=producer_python,
            producer_dir=producer_dir,
            env=recover_env,
            log_dir=log_dir,
            label="producer-recover",
        )
        record.producers.append(producer_record2)
        tracked.append(
            _TrackedChild(producer_process2, producer_record2, (p_out2, p_err2)),
        )
        _wait_producer_ok(
            producer_process2,
            producer_record2,
            (p_out2, p_err2),
            deadline=scenario_deadline,
            worker_process=worker_process,
        )

        replacement_ack = wr_protocol.wait_for_replacement_ack(
            state_dir,
            expected_run_id=run_id,
            expected_execution_id=execution_id,
            deadline=_wait_budget_seconds(scenario_deadline),
            child_process=worker_process,
        )
        assert worker_record2 is not None
        _validate_replacement_ack(
            replacement_ack,
            worker_record=worker_record2,
            execution_id=execution_id,
        )

        _shutdown_worker_cooperatively(
            worker_process=worker_process,
            record=worker_record2,
            state_dir=state_dir,
            run_id=run_id,
            stdout_io=w_out2,
            stderr_io=w_err2,
            scenario_deadline=scenario_deadline,
        )
        worker_process = None
        worker_streams = None

        _assert_broker_identity_unchanged(
            broker_target,
            expected_pid=record.broker_pid or broker_target.process.pid,
            expected_config_path=broker_config,
            expected_config_fingerprint=broker_config_fp,
        )

        record.broker_store_fingerprint_after = _broker_store_fingerprint(store_dir)
        if record.broker_store_fingerprint_after == "missing":
            raise WorkerRecoveryError("broker store missing after recovery")
        if store_dir.resolve() != record.broker_store_dir.resolve():
            raise WorkerRecoveryError("broker store path changed across worker replacement")

        invocations = wr_protocol.read_invocations(state_dir)
        record.invocations = [
            {
                "run_id": item.run_id,
                "worker_generation": item.worker_generation,
                "execution_id": item.execution_id,
                "pid": item.pid,
            }
            for item in invocations
        ]
        assert worker_record is not None and worker_record.pid is not None
        assert worker_record2.pid is not None
        wr_protocol.assert_invocations_match_workers(
            invocations,
            run_id=run_id,
            execution_id=execution_id,
            allowed_pids_by_generation={
                "1": worker_record.pid,
                "2": worker_record2.pid,
            },
        )
        gen1_count = sum(1 for item in invocations if item.worker_generation == "1")
        gen2_count = sum(1 for item in invocations if item.worker_generation == "2")
        if scenario == SCENARIO_BEFORE:
            if gen1_count < 1 or gen2_count < 1:
                raise WorkerRecoveryError(
                    f"scenario A requires gen1 and gen2 invocations, saw "
                    f"gen1={gen1_count} gen2={gen2_count}",
                )
        else:
            if gen2_count != 0:
                raise WorkerRecoveryError(
                    f"scenario B forbids gen2 handler invocations after replacement ack, "
                    f"saw {gen2_count}",
                )
            if gen1_count != 1:
                raise WorkerRecoveryError(
                    f"scenario B requires exactly one gen1 invocation, saw {gen1_count}",
                )
    except BaseException as exc:
        record.error = f"{type(exc).__name__}: {exc}"
    finally:
        cleanup_deadline = min(
            scenario_deadline,
            time.monotonic() + KILL_REAP_SECONDS,
        )
        try:
            _finalize_tracked_children(tracked, deadline=cleanup_deadline)
        except BaseException as cleanup_exc:
            if record.error is None:
                record.error = f"cleanup: {cleanup_exc}"
            else:
                record.error = f"{record.error}; cleanup: {cleanup_exc}"
        record.ended_at = time.monotonic()
        record.checkpoints = _collect_recovery_snapshot(state_dir)
        if artifact_scenario_dir is not None:
            _write_recovery_artifact(artifact_scenario_dir / scenario / run_id, record)
    return record


def _pass_payload(state: PythonPassState, broker_log: str) -> dict[str, Any]:
    return {
        "python_version": state.python_version,
        "runtime_version": state.runtime_version,
        "producer_interpreter": str(state.producer_py),
        "worker_interpreter": str(state.worker_py),
        "origins": state.origins,
        "error": state.error,
        "broker_log": broker_log,
        "scenarios": [_recovery_record_to_json(item) for item in state.scenario_records],
    }


def verify_python_version(
    *,
    python_version: str,
    work_dir: Path,
    library: Path,
    contract: Path,
    artifact_dir: Path,
) -> PythonPassState:
    tag = runtime_evidence_tag(python_version)
    state = PythonPassState(
        python_version=python_version,
        tag=tag,
        producer_py=Path(),
        worker_py=Path(),
        producer_dir=Path(),
        worker_dir=Path(),
    )
    evidence_dir = artifact_dir / tag
    evidence_dir.mkdir(parents=True, exist_ok=True)
    artifact_scenarios = evidence_dir / "scenarios"
    broker_logs: list[str] = []
    pass_error: BaseException | None = None
    try:
        producer_venv = create_wheel_venv(
            work_dir,
            python_version,
            library,
            contract,
            name=f"venv-producer-{tag}",
        )
        worker_venv = create_wheel_venv(
            work_dir,
            python_version,
            library,
            contract,
            name=f"venv-worker-{tag}",
        )
        state.producer_py = venv_python(producer_venv)
        state.worker_py = venv_python(worker_venv)
        state.runtime_version = assert_final_runtime_python(state.producer_py)
        assert_final_runtime_python(state.worker_py)

        state.producer_dir = materialize_role_dir(
            work_dir / tag,
            "producer",
            PRODUCER_SUPPORT_FILES,
        )
        state.worker_dir = materialize_role_dir(
            work_dir / tag,
            "worker",
            WORKER_SUPPORT_FILES,
        )
        state.origins = {
            "producer": probe_recovery_origins(
                state.producer_py,
                state.producer_dir,
                role="producer",
                evidence_dir=evidence_dir,
                python_version=python_version,
            ),
            "worker": probe_recovery_origins(
                state.worker_py,
                state.worker_dir,
                role="worker",
                evidence_dir=evidence_dir,
                python_version=python_version,
            ),
        }

        for scenario in RECOVERY_SCENARIOS:
            # Keep the owned broker's bounded stop inside the overall scenario cap.
            scenario_deadline = time.monotonic() + SCENARIO_TIMEOUT_SECONDS - SHUTDOWN_TIMEOUT_SECONDS
            owner = OwnedNatsServer()
            broker_log = ""
            cleanup_error: BaseException | None = None
            scenario_error: str | None = None
            record: RecoveryScenarioRecord | None = None
            try:
                if _wait_budget_seconds(scenario_deadline) <= 0:
                    raise WorkerRecoveryError(
                        f"scenario {scenario}: deadline exhausted before broker startup",
                    )
                target = owner.start()
                record = _run_recovery_scenario(
                    scenario=scenario,
                    nats_url=target.url,
                    broker_target=target,
                    producer_python=state.producer_py,
                    worker_python=state.worker_py,
                    producer_dir=state.producer_dir,
                    worker_dir=state.worker_dir,
                    artifact_scenario_dir=artifact_scenarios,
                    scenario_deadline=scenario_deadline,
                )
                state.scenario_records.append(record)
                if record.error:
                    raise WorkerRecoveryError(record.error)
            except BaseException as exc:
                scenario_error = f"{type(exc).__name__}: {exc}"
                if record is None:
                    state.error = scenario_error
                raise WorkerRecoveryError(scenario_error) from exc
            finally:
                if owner.target is not None:
                    try:
                        owner.stop(owner.target)
                    except BaseException as exc:
                        cleanup_error = exc
                        cleanup_message = _format_pass_error(exc)
                        if state.error is None:
                            state.error = f"Broker cleanup failed: {cleanup_message}"
                        elif cleanup_message not in state.error:
                            state.error = f"{state.error}; Broker cleanup failed: {cleanup_message}"
                    log_path = owner.target.log_path
                    if log_path is not None and log_path.is_file():
                        broker_log = log_path.read_text(encoding="utf-8")
                broker_logs.append(broker_log)
                scenario_payload = _pass_payload(state, broker_log)
                scenario_payload["scenario"] = scenario
                if scenario_error is not None:
                    scenario_payload["scenario_error"] = scenario_error
                if record is not None and record.error and scenario_payload.get("error") is None:
                    scenario_payload["error"] = record.error
                _write_evidence(
                    evidence_dir / f"summary-{scenario}.json",
                    scenario_payload,
                )
                if cleanup_error is not None and (record is None or record.error is None):
                    raise WorkerRecoveryError(state.error) from cleanup_error
    except BaseException as exc:
        pass_error = exc
        state.error = _format_pass_error(exc)
    finally:
        _write_evidence(
            evidence_dir / "summary.json",
            _pass_payload(state, "\n".join(broker_logs)),
        )
    if pass_error is not None:
        raise pass_error
    return state


def collect_python_versions(args: argparse.Namespace) -> tuple[str, ...]:
    versions: list[str] = list(args.python_versions or [])
    if not versions:
        return DEFAULT_RUNTIMES
    return tuple(dict.fromkeys(versions))


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--python",
        action="append",
        dest="python_versions",
        help=f"Python runtime matrix (default: {', '.join(DEFAULT_RUNTIMES)}).",
    )
    parser.add_argument(
        "--artifact-dir",
        type=Path,
        help="Directory for per-run JSON evidence (origins, scenario logs, broker log).",
    )
    parser.add_argument(
        "--keep-work",
        action="store_true",
        help="Retain temporary work directories after a successful run.",
    )
    parser.add_argument(
        "--work-dir",
        type=Path,
        help="Parent directory for temporary verification files (must be outside the repo).",
    )
    args = parser.parse_args(argv)
    python_versions = collect_python_versions(args)
    parent = args.work_dir or Path(tempfile.gettempdir())
    assert_work_parent_outside_repo(parent)
    work_dir = Path(tempfile.mkdtemp(prefix="superjobs-worker-recovery-", dir=parent))
    artifact_dir = args.artifact_dir or Path(
        tempfile.mkdtemp(prefix="superjobs-worker-recovery-artifacts-", dir=parent),
    )
    artifact_dir.mkdir(parents=True, exist_ok=True)
    owns_default_artifacts = args.artifact_dir is None

    exit_code = 0
    run_error: BaseException | None = None
    try:
        library, contract = build_wheels(work_dir)
        # Initial binary provisioning is setup, outside native scenario lifecycle budgets.
        resolve_executable()
        for py_version in python_versions:
            verify_python_version(
                python_version=py_version,
                work_dir=work_dir,
                library=library,
                contract=contract,
                artifact_dir=artifact_dir,
            )
    except (WorkerRecoveryError, VerificationError, KeyboardInterrupt) as exc:
        exit_code = 1
        run_error = exc
        if isinstance(exc, KeyboardInterrupt):
            print("verify_worker_recovery: interrupted", file=sys.stderr)
        else:
            print(f"verify_worker_recovery: {exc}", file=sys.stderr)
    except BaseException as exc:
        exit_code = 1
        run_error = exc
        print(f"verify_worker_recovery: {exc}", file=sys.stderr)
    finally:
        print(f"Artifact directory: {artifact_dir}", file=sys.stderr)
        if run_error is not None:
            from tools.verify_cross_program import _parse_verification_error, _write_run_error

            if isinstance(run_error, VerificationError):
                _write_run_error(artifact_dir, run_error)
            else:
                message = str(run_error) if str(run_error) else type(run_error).__name__
                _write_evidence(
                    artifact_dir / "run_error.json",
                    {"error": message, "exception_type": type(run_error).__name__},
                )
        if args.keep_work:
            print(f"Work directory (inspection): {work_dir}", file=sys.stderr)
        elif work_dir.exists():
            shutil.rmtree(work_dir, ignore_errors=True)
        if owns_default_artifacts and exit_code == 0:
            shutil.rmtree(artifact_dir, ignore_errors=True)

    if exit_code != 0:
        return exit_code

    print("verify_worker_recovery: OK")
    for py_version in python_versions:
        tag = runtime_evidence_tag(py_version)
        print(f"Worker recovery {tag}: scenarios {', '.join(RECOVERY_SCENARIOS)}")
    if not owns_default_artifacts:
        print(f"Artifacts written to {artifact_dir}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
