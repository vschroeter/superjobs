#!/usr/bin/env python3
"""Verify execution and result persistence across a NATS broker restart."""

from __future__ import annotations

import argparse
import hashlib
import json
import shutil
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
SUPPORT_ROOT = Path(__file__).resolve().parent / "broker_restart_support"
ORIGIN_PROBE = Path(__file__).resolve().parent / "wheel_origin_probe.py"

from tests.support.nats_harness.pinned import (  # noqa: E402
    SHUTDOWN_TIMEOUT_SECONDS,
    STARTUP_TIMEOUT_SECONDS,
)
from tests.support.nats_harness.provision import resolve_executable  # noqa: E402
from tests.support.nats_harness.server import (  # noqa: E402
    NatsServerTarget,
    OwnedNatsServer,
)

from tools.broker_restart_support import broker_resources as br_resources  # noqa: E402
from tools.broker_restart_support.queue_config import queue_config_for_run  # noqa: E402
from tools.cross_program_support.child_env import (  # noqa: E402
    isolated_child_env,
    python_command,
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
    _wait_for_producer_exit,
    _write_evidence,
)
from tools.worker_recovery_support.spawn_pid import marker_pid_in_spawn_tree  # noqa: E402
from tools.verify_worker_recovery import (  # noqa: E402
    WorkerRecoveryError,
    _hard_kill_worker,
)
from tools.broker_restart_support import protocol as br_protocol  # noqa: E402

assert TYPING_REPO_ROOT == REPO_ROOT

CHILD_TIMEOUT_SECONDS = 30
SCENARIO_TIMEOUT_SECONDS = 90
RESTART_PHASE_BUDGET_SECONDS = (
    SHUTDOWN_TIMEOUT_SECONDS + STARTUP_TIMEOUT_SECONDS
)
KILL_REAP_SECONDS = 5
WORKER_EXIT_OK = 0
PRODUCER_EXIT_OK = 0

SCENARIO_NAME = "broker_restart_persistence"

ROLE_SHARED_FILES = (
    "protocol.py",
    "scenario_jobs.py",
    "runtime_isolation.py",
    "queue_config.py",
)
WORKER_SUPPORT_FILES = ROLE_SHARED_FILES + (
    "worker_handlers.py",
    "worker_app.py",
)
PRODUCER_SUPPORT_FILES = ROLE_SHARED_FILES + ("producer_app.py",)


class BrokerRestartError(Exception):
    """Raised when broker restart verification fails."""


@dataclass
class BrokerRestartScenarioRecord:
    name: str
    run_id: str
    state_dir: Path
    broker_store_dir: Path
    broker_store_fingerprint_before: str
    broker_store_fingerprint_after_restart: str
    broker_store_fingerprint_final: str
    started_at: float
    ended_at: float | None = None
    completed_execution_id: str | None = None
    pending_execution_id: str | None = None
    invocations: list[dict[str, Any]] | None = None
    workers: list[ChildRecord] = field(default_factory=list)
    producers: list[ChildRecord] = field(default_factory=list)
    checkpoints: dict[str, Any] | None = None
    broker_pid_before: int | None = None
    broker_pid_after_restart: int | None = None
    broker_port: int | None = None
    broker_config_path: str | None = None
    broker_config_fingerprint: str | None = None
    broker_config_fingerprint_after_restart: str | None = None
    broker_config_content_before: str | None = None
    broker_config_content_after_restart: str | None = None
    jetstream_store_line_before: str | None = None
    jetstream_store_line_after: str | None = None
    broker_resources_before_restart: dict[str, Any] | None = None
    broker_resources_after_restart: dict[str, Any] | None = None
    pending_work_evidence_before_restart: dict[str, Any] | None = None
    pending_work_evidence_after_restart: dict[str, Any] | None = None
    empty_store_control_detected: bool = False
    control_oracle_error: str | None = None
    error: str | None = None


@dataclass
class PythonPassState:
    python_version: str
    tag: str
    producer_py: Path
    worker_py: Path
    producer_dir: Path
    worker_dir: Path
    scenario_record: BrokerRestartScenarioRecord | None = None
    origins: dict[str, Any] | None = None
    runtime_version: str | None = None
    error: str | None = None


def materialize_role_dir(work_dir: Path, role: str, files: tuple[str, ...]) -> Path:
    dest = work_dir / role
    dest.mkdir(parents=True, exist_ok=True)
    for name in files:
        shutil.copy2(SUPPORT_ROOT / name, dest / name)
    return dest


def probe_restart_origins(
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
        raise BrokerRestartError("owned broker missing work_dir")
    return target.work_dir / "store"


def _format_pass_error(exc: BaseException) -> str:
    message = str(exc)
    if message:
        return message
    return f"{type(exc).__name__}"


def _wait_budget_seconds(scenario_deadline: float) -> float:
    return max(0.0, scenario_deadline - time.monotonic() - KILL_REAP_SECONDS)


def _work_deadline(scenario_deadline: float) -> float:
    """Absolute deadline for scenario work (reserves KILL_REAP_SECONDS for child cleanup)."""
    return scenario_deadline - KILL_REAP_SECONDS


def _child_phase_deadline(scenario_deadline: float) -> float:
    return min(_work_deadline(scenario_deadline), time.monotonic() + CHILD_TIMEOUT_SECONDS)


def _restart_phase_deadline(scenario_deadline: float) -> float:
    return min(_work_deadline(scenario_deadline), time.monotonic() + RESTART_PHASE_BUDGET_SECONDS)


def _assert_path_within_owned_root(path: Path, owned_root: Path) -> None:
    try:
        path.resolve().relative_to(owned_root.resolve())
    except ValueError as exc:
        raise BrokerRestartError(
            f"path {path!r} escapes owned broker root {owned_root!r}",
        ) from exc


def _read_broker_config_content(config_path: Path | None) -> str | None:
    if config_path is None or not config_path.is_file():
        return None
    return config_path.read_text(encoding="utf-8")


def _assert_no_live_application_processes(tracked: list[_TrackedChild]) -> None:
    live = [
        item.record.name
        for item in tracked
        if item.process.poll() is None
    ]
    if live:
        raise BrokerRestartError(
            f"application processes still running before broker restart: {', '.join(live)}",
        )


def _isolate_and_empty_owned_store(broker_target: NatsServerTarget) -> Path:
    if broker_target.work_dir is None:
        raise BrokerRestartError("owned broker missing work_dir")
    owned_root = broker_target.work_dir
    store_dir = owned_root / "store"
    if not store_dir.is_dir():
        raise BrokerRestartError("broker store missing before empty-store substitution")
    _assert_path_within_owned_root(store_dir, owned_root)
    backup = owned_root / f"store.snapshot.{uuid.uuid4().hex[:8]}"
    _assert_path_within_owned_root(backup, owned_root)
    if backup.exists():
        shutil.rmtree(backup)
    store_dir.rename(backup)
    store_dir.mkdir(parents=True)
    return backup


def _restore_isolated_owned_store(broker_target: NatsServerTarget, backup: Path) -> None:
    if broker_target.work_dir is None:
        return
    owned_root = broker_target.work_dir
    store_dir = owned_root / "store"
    _assert_path_within_owned_root(store_dir, owned_root)
    _assert_path_within_owned_root(backup, owned_root)
    if store_dir.exists():
        shutil.rmtree(store_dir)
    if backup.is_dir():
        backup.rename(store_dir)


def _broker_config_fingerprint(config_path: Path | None) -> str:
    if config_path is None or not config_path.is_file():
        return "missing"
    digest = hashlib.sha256(config_path.read_bytes()).hexdigest()
    return f"sha256:{digest}"


def _jetstream_store_line(config_path: Path | None) -> str:
    if config_path is None or not config_path.is_file():
        return "missing"
    for line in config_path.read_text(encoding="utf-8").splitlines():
        if "store_dir" in line:
            return line.strip()
    return "missing"


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


def _assert_nonempty_store_fingerprint(fingerprint: str, *, label: str) -> None:
    if fingerprint == "missing" or fingerprint == "files=0:bytes=0":
        raise BrokerRestartError(
            f"{label} broker store fingerprint {fingerprint!r} indicates empty or missing store",
        )


def _assert_ready_pid_matches_spawn(
    process: subprocess.Popen[str],
    marker: br_protocol.ReadyMarker,
    *,
    label: str,
) -> None:
    spawn_pid = process.pid
    if spawn_pid is None:
        raise BrokerRestartError(f"{label} spawn pid missing")
    if not marker_pid_in_spawn_tree(spawn_pid, marker.pid):
        raise BrokerRestartError(
            f"{label} ready pid {marker.pid} not in spawn tree rooted at {spawn_pid}",
        )


@dataclass
class _TrackedChild:
    process: subprocess.Popen[str]
    record: ChildRecord
    streams: tuple[TextIO, TextIO]
    started_path: Path


def _owned_interpreter_pid(
    process: subprocess.Popen[str],
    record: ChildRecord,
    started_path: Path,
) -> int:
    launcher_pid = process.pid
    if launcher_pid is None:
        raise BrokerRestartError(f"{record.name} launcher pid missing")
    if started_path.is_file():
        interpreter_pid = int(started_path.read_text(encoding="utf-8").strip())
        if process.poll() is None and not marker_pid_in_spawn_tree(launcher_pid, interpreter_pid):
            raise BrokerRestartError(
                f"{record.name} started marker pid {interpreter_pid} "
                f"not in spawn tree rooted at {launcher_pid}",
            )
        return interpreter_pid
    if record.pid is not None and (
        process.poll() is not None or marker_pid_in_spawn_tree(launcher_pid, record.pid)
    ):
        return record.pid
    if process.poll() is not None and launcher_pid is not None:
        return launcher_pid
    raise BrokerRestartError(
        f"{record.name} interpreter pid unknown "
        f"(launcher={launcher_pid}, record.pid={record.pid}, started={started_path})",
    )


def _assert_launcher_and_interpreter_dead(
    process: subprocess.Popen[str],
    interpreter_pid: int,
    *,
    record: ChildRecord,
    deadline: float,
) -> None:
    if process.poll() is None:
        raise BrokerRestartError(
            f"{record.name} launcher pid {process.pid} still alive after cleanup",
        )
    if sys.platform != "win32" or interpreter_pid == process.pid:
        return
    from tools.worker_recovery_support.windows_process import OwnedWindowsProcess

    try:
        interpreter = OwnedWindowsProcess(interpreter_pid)
    except OSError:
        return
    try:
        if not interpreter.exited(deadline=deadline):
            raise BrokerRestartError(
                f"{record.name} interpreter pid {interpreter_pid} still alive after cleanup",
            )
    finally:
        interpreter.close()


def _finalize_application_child(
    item: _TrackedChild,
    *,
    deadline: float,
) -> None:
    process = item.process
    record = item.record
    interpreter_pid = _owned_interpreter_pid(process, record, item.started_path)
    existing = process.poll()
    if existing is not None:
        record.exit_code = existing
        if record.ended_at is None:
            record.ended_at = time.monotonic()
        _assert_launcher_and_interpreter_dead(
            process,
            interpreter_pid,
            record=record,
            deadline=deadline,
        )
        return
    kill_record = ChildRecord(
        name=record.name,
        command=record.command,
        cwd=record.cwd,
        started_at=record.started_at,
        pid=interpreter_pid,
        stdout_path=record.stdout_path,
        stderr_path=record.stderr_path,
    )
    try:
        if sys.platform == "win32" and interpreter_pid != process.pid:
            _hard_kill_worker(process, kill_record, deadline=deadline)
            record.exit_code = kill_record.exit_code
            record.termination = kill_record.termination
        else:
            _finalize_child_after_kill(process, record, deadline=deadline)
    except WorkerRecoveryError as exc:
        raise BrokerRestartError(str(exc)) from exc
    if record.ended_at is None:
        record.ended_at = time.monotonic()
    _assert_launcher_and_interpreter_dead(
        process,
        interpreter_pid,
        record=record,
        deadline=deadline,
    )


def _finalize_tracked_children(
    tracked: list[_TrackedChild],
    *,
    deadline: float,
) -> None:
    errors: list[str] = []
    for item in tracked:
        try:
            _finalize_application_child(item, deadline=deadline)
        except BaseException as exc:
            errors.append(f"{item.record.name}: {exc}")
        try:
            _close_child_streams_safe(*item.streams)
        except BaseException as exc:
            errors.append(f"{item.record.name} streams: {exc}")
    if errors:
        raise BrokerRestartError("child cleanup failed: " + "; ".join(errors))
    for item in tracked:
        if item.process.poll() is None:
            raise BrokerRestartError(
                f"{item.record.name} pid {item.record.pid} still alive after cleanup",
            )


def _child_env_with_started(
    base_env: dict[str, str],
    log_dir: Path,
    label: str,
) -> tuple[dict[str, str], Path]:
    started_path = (log_dir / f"{label}.interpreter.pid").resolve()
    env = isolated_child_env(
        {**base_env, "SUPERJOBS_PROCESS_STARTED_PATH": str(started_path)},
    )
    return env, started_path


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
    scenario_deadline: float,
    worker_process: subprocess.Popen[str] | None = None,
) -> None:
    deadline = _child_phase_deadline(scenario_deadline)
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
        raise BrokerRestartError(
            f"{exc}; producer stdout={stdout!r}; stderr={stderr!r}",
        ) from exc
    if code != PRODUCER_EXIT_OK:
        stdout = record.stdout_path.read_text(encoding="utf-8") if record.stdout_path else ""
        stderr = record.stderr_path.read_text(encoding="utf-8") if record.stderr_path else ""
        raise BrokerRestartError(
            f"{record.name} exit {code} != expected {PRODUCER_EXIT_OK}; "
            f"stdout={stdout!r}; stderr={stderr!r}",
        )
    record.exit_code = code
    record.ended_at = time.monotonic()
    _close_child_streams_safe(*streams)


def _shutdown_worker_cooperatively(
    *,
    worker_process: subprocess.Popen[str],
    record: ChildRecord,
    state_dir: Path,
    run_id: str,
    stdout_io: TextIO,
    stderr_io: TextIO,
    scenario_deadline: float,
) -> None:
    shutdown_deadline = _child_phase_deadline(scenario_deadline)
    existing = worker_process.poll()
    if existing is not None:
        record.exit_code = existing
        record.ended_at = time.monotonic()
        _close_child_streams_safe(stdout_io, stderr_io)
        if existing != WORKER_EXIT_OK:
            raise BrokerRestartError(f"worker exit {existing} != expected {WORKER_EXIT_OK}")
        return
    write_worker_stop(state_dir, run_id)
    record.exit_code = _reap_child(
        worker_process,
        label="worker",
        deadline=shutdown_deadline,
        expected_code=WORKER_EXIT_OK,
    )
    record.ended_at = time.monotonic()
    _close_child_streams_safe(stdout_io, stderr_io)


def _collect_restart_snapshot(state_dir: Path) -> dict[str, Any]:
    snapshot = _collect_checkpoint_snapshot(state_dir)
    invocations_path = br_protocol.invocations_path(state_dir)
    if invocations_path.is_file():
        snapshot["handler_invocations.json"] = _snapshot_json_file(invocations_path)
    return snapshot


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
        "stdout_path": str(child.stdout_path) if child.stdout_path else None,
        "stderr_path": str(child.stderr_path) if child.stderr_path else None,
    }


def _scenario_record_to_json(item: BrokerRestartScenarioRecord) -> dict[str, Any]:
    return {
        "name": item.name,
        "run_id": item.run_id,
        "completed_execution_id": item.completed_execution_id,
        "pending_execution_id": item.pending_execution_id,
        "broker_store_dir": str(item.broker_store_dir),
        "broker_store_fingerprint_before": item.broker_store_fingerprint_before,
        "broker_store_fingerprint_after_restart": item.broker_store_fingerprint_after_restart,
        "broker_store_fingerprint_final": item.broker_store_fingerprint_final,
        "broker_pid_before": item.broker_pid_before,
        "broker_pid_after_restart": item.broker_pid_after_restart,
        "broker_port": item.broker_port,
        "broker_config_path": item.broker_config_path,
        "broker_config_fingerprint": item.broker_config_fingerprint,
        "broker_config_fingerprint_after_restart": item.broker_config_fingerprint_after_restart,
        "broker_config_content_before": item.broker_config_content_before,
        "broker_config_content_after_restart": item.broker_config_content_after_restart,
        "jetstream_store_line_before": item.jetstream_store_line_before,
        "jetstream_store_line_after": item.jetstream_store_line_after,
        "broker_resources_before_restart": item.broker_resources_before_restart,
        "broker_resources_after_restart": item.broker_resources_after_restart,
        "pending_work_evidence_before_restart": item.pending_work_evidence_before_restart,
        "pending_work_evidence_after_restart": item.pending_work_evidence_after_restart,
        "empty_store_control_detected": item.empty_store_control_detected,
        "control_oracle_error": item.control_oracle_error,
        "started_at_monotonic": item.started_at,
        "ended_at_monotonic": item.ended_at,
        "duration_seconds": (item.ended_at or item.started_at) - item.started_at,
        "error": item.error,
        "checkpoints": item.checkpoints,
        "invocations": item.invocations,
        "workers": [_child_record_json(worker) for worker in item.workers],
        "producers": [_child_record_json(producer) for producer in item.producers],
    }


def _write_restart_artifact(path: Path, record: BrokerRestartScenarioRecord) -> None:
    _write_evidence(path / "scenario.json", _scenario_record_to_json(record))
    for child in (*record.workers, *record.producers):
        for stream_path in (child.stdout_path, child.stderr_path):
            if stream_path is None or not stream_path.is_file():
                continue
            destination = path / stream_path.name
            if stream_path.resolve() == destination.resolve():
                continue
            shutil.copy2(stream_path, destination)


def _run_broker_restart_scenario(
    *,
    owner: OwnedNatsServer,
    nats_url: str,
    broker_target: NatsServerTarget,
    producer_python: Path,
    worker_python: Path,
    producer_dir: Path,
    worker_dir: Path,
    artifact_scenario_dir: Path | None,
    scenario_deadline: float,
    empty_store_control: bool = False,
) -> BrokerRestartScenarioRecord:
    run_id = uuid.uuid4().hex
    # Keep atomic checkpoint paths short under Windows temporary test roots.
    state_dir = producer_dir.parent.parent / "state" / run_id
    state_dir.mkdir(parents=True, exist_ok=True)
    log_dir = (
        artifact_scenario_dir / SCENARIO_NAME / run_id
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
    queue_config = queue_config_for_run(run_id)
    record = BrokerRestartScenarioRecord(
        name=SCENARIO_NAME,
        run_id=run_id,
        state_dir=state_dir,
        broker_store_dir=store_dir,
        broker_store_fingerprint_before=_broker_store_fingerprint(store_dir),
        broker_store_fingerprint_after_restart="",
        broker_store_fingerprint_final="",
        started_at=time.monotonic(),
        broker_pid_before=(
            broker_target.process.pid if broker_target.process is not None else None
        ),
        broker_port=int(nats_url.rsplit(":", 1)[1]),
        broker_config_path=str(broker_config) if broker_config is not None else None,
        broker_config_fingerprint=broker_config_fp,
        broker_config_content_before=_read_broker_config_content(broker_config),
        jetstream_store_line_before=_jetstream_store_line(broker_config),
    )
    base_env = {
        "NATS_URL": nats_url,
        "SUPERJOBS_CROSS_RUN_ID": run_id,
        "SUPERJOBS_CROSS_STATE_DIR": str(state_dir),
    }
    tracked: list[_TrackedChild] = []
    worker_process: subprocess.Popen[str] | None = None
    worker_record: ChildRecord | None = None
    worker_streams: tuple[TextIO, TextIO] | None = None
    try:
        worker_env, worker_started = _child_env_with_started(
            {**base_env, "SUPERJOBS_WORKER_GENERATION": "1"},
            log_dir,
            "worker-gen1",
        )
        worker_process, worker_record, w_out, w_err = _spawn_worker(
            worker_python=worker_python,
            worker_dir=worker_dir,
            env=worker_env,
            log_dir=log_dir,
            label="worker-gen1",
        )
        record.workers.append(worker_record)
        worker_streams = (w_out, w_err)
        tracked.append(_TrackedChild(worker_process, worker_record, worker_streams, worker_started))
        ready_g1 = br_protocol.wait_for_ready(
            state_dir,
            expected_run_id=run_id,
            deadline=max(
                0.0,
                min(CHILD_TIMEOUT_SECONDS, _wait_budget_seconds(scenario_deadline)),
            ),
            child_process=worker_process,
            expected_worker_generation="1",
        )
        _assert_ready_pid_matches_spawn(worker_process, ready_g1, label="worker-gen1")
        worker_record.pid = ready_g1.pid

        complete_env, complete_started = _child_env_with_started(
            {**base_env, "SUPERJOBS_BROKER_RESTART_PHASE": "run_completed"},
            log_dir,
            "producer-run-completed",
        )
        producer_process, producer_record, p_out, p_err = _spawn_producer(
            producer_python=producer_python,
            producer_dir=producer_dir,
            env=complete_env,
            log_dir=log_dir,
            label="producer-run-completed",
        )
        record.producers.append(producer_record)
        tracked.append(
            _TrackedChild(producer_process, producer_record, (p_out, p_err), complete_started),
        )
        _wait_producer_ok(
            producer_process,
            producer_record,
            (p_out, p_err),
            scenario_deadline=scenario_deadline,
            worker_process=worker_process,
        )
        completed_done = br_protocol.wait_for_checkpoint(
            state_dir,
            name=br_protocol.COMPLETED_DONE,
            expected_run_id=run_id,
            deadline=_wait_budget_seconds(scenario_deadline),
        )
        completed_execution_id = completed_done.execution_id
        if not completed_execution_id:
            raise BrokerRestartError("completed_done checkpoint missing execution_id")
        record.completed_execution_id = completed_execution_id
        br_protocol.wait_for_invocation_count(
            state_dir,
            minimum=1,
            deadline=_wait_budget_seconds(scenario_deadline),
        )
        invocations_pre = br_protocol.read_invocations(state_dir)
        if len(invocations_pre) != 1:
            raise BrokerRestartError(
                f"expected exactly one handler invocation before shutdown, saw {len(invocations_pre)}",
            )

        _shutdown_worker_cooperatively(
            worker_process=worker_process,
            record=worker_record,
            state_dir=state_dir,
            run_id=run_id,
            stdout_io=w_out,
            stderr_io=w_err,
            scenario_deadline=scenario_deadline,
        )
        worker_process = None
        worker_streams = None

        pending_env, pending_started = _child_env_with_started(
            {
                **base_env,
                "SUPERJOBS_BROKER_RESTART_PHASE": "submit_pending",
                "SUPERJOBS_SKIP_WORKER_READY": "1",
            },
            log_dir,
            "producer-submit-pending",
        )
        producer_process2, producer_record2, p_out2, p_err2 = _spawn_producer(
            producer_python=producer_python,
            producer_dir=producer_dir,
            env=pending_env,
            log_dir=log_dir,
            label="producer-submit-pending",
        )
        record.producers.append(producer_record2)
        tracked.append(
            _TrackedChild(producer_process2, producer_record2, (p_out2, p_err2), pending_started),
        )
        _wait_producer_ok(
            producer_process2,
            producer_record2,
            (p_out2, p_err2),
            scenario_deadline=scenario_deadline,
            worker_process=None,
        )
        pending_submitted = br_protocol.wait_for_checkpoint(
            state_dir,
            name=br_protocol.PENDING_SUBMITTED,
            expected_run_id=run_id,
            deadline=_wait_budget_seconds(scenario_deadline),
        )
        pending_execution_id = pending_submitted.execution_id
        if not pending_execution_id:
            raise BrokerRestartError("pending_submitted checkpoint missing execution_id")
        record.pending_execution_id = pending_execution_id
        pending_work_subject = (
            f"{queue_config.subject_prefix}.work."
            f"{pending_submitted.job}:{br_protocol.JOB_VERSION}"
        )
        if len(br_protocol.read_invocations(state_dir)) != 1:
            raise BrokerRestartError("pending submission must not invoke handlers before restart")

        record.broker_store_fingerprint_before = _broker_store_fingerprint(store_dir)
        _assert_nonempty_store_fingerprint(
            record.broker_store_fingerprint_before,
            label="pre-restart",
        )
        record.broker_resources_before_restart = br_resources.collect_broker_resource_evidence(
            nats_url,
            queue_config,
            deadline=_work_deadline(scenario_deadline),
        )
        br_resources.assert_persisted_file_storage(record.broker_resources_before_restart)
        br_resources.assert_resources_nonempty(record.broker_resources_before_restart)
        br_resources.assert_pending_work_retained(
            record.broker_resources_before_restart,
            pending_execution_id=pending_execution_id,
            pending_work_subject=pending_work_subject,
        )
        record.pending_work_evidence_before_restart = {
            "pending_execution_id": pending_execution_id,
            "pending_job": pending_submitted.job,
            "work_messages": record.broker_resources_before_restart.get("work_messages"),
        }

        _assert_no_live_application_processes(tracked)

        if broker_target.process is None:
            raise BrokerRestartError("broker process missing before restart")
        restart_deadline = _restart_phase_deadline(scenario_deadline)
        if empty_store_control:
            owner.pause(broker_target, deadline=restart_deadline)
            _isolate_and_empty_owned_store(broker_target)
            restarted = owner._launch(
                port=int(nats_url.rsplit(":", 1)[1]),
                deadline=restart_deadline,
            )
        else:
            restarted = owner.restart(broker_target, deadline=restart_deadline)
        record.broker_pid_after_restart = (
            restarted.process.pid if restarted.process is not None else None
        )
        if record.broker_pid_before == record.broker_pid_after_restart:
            raise BrokerRestartError("broker pid must change across restart")
        if restarted.url != nats_url:
            raise BrokerRestartError("broker client URL must be preserved across restart")
        record.broker_config_fingerprint_after_restart = _broker_config_fingerprint(broker_config)
        record.broker_config_content_after_restart = _read_broker_config_content(broker_config)
        record.jetstream_store_line_after = _jetstream_store_line(broker_config)
        if record.jetstream_store_line_before != record.jetstream_store_line_after:
            raise BrokerRestartError(
                "jetstream store_dir line changed across restart "
                f"({record.jetstream_store_line_before!r} -> {record.jetstream_store_line_after!r})",
            )
        if "store_dir" not in (record.jetstream_store_line_after or ""):
            raise BrokerRestartError("broker config missing jetstream store_dir after restart")

        if empty_store_control:
            try:
                record.broker_resources_after_restart = (
                    br_resources.collect_broker_resource_evidence(
                        nats_url,
                        queue_config,
                        deadline=_work_deadline(scenario_deadline),
                    )
                )
                br_resources.assert_resources_survive_restart(
                    record.broker_resources_before_restart,
                    record.broker_resources_after_restart,
                )
                raise BrokerRestartError(
                    "empty-store negative control failed: "
                    "missing persisted resources not detected",
                )
            except br_resources.BrokerResourceError as exc:
                record.empty_store_control_detected = True
                record.control_oracle_error = str(exc)
            return record

        record.broker_store_fingerprint_after_restart = _broker_store_fingerprint(store_dir)
        _assert_nonempty_store_fingerprint(
            record.broker_store_fingerprint_after_restart,
            label="post-restart",
        )
        if store_dir.resolve() != record.broker_store_dir.resolve():
            raise BrokerRestartError("broker store path changed across restart")

        record.broker_resources_after_restart = br_resources.collect_broker_resource_evidence(
            nats_url,
            queue_config,
            deadline=_work_deadline(scenario_deadline),
        )
        br_resources.assert_resources_survive_restart(
            record.broker_resources_before_restart,
            record.broker_resources_after_restart,
        )
        br_resources.assert_pending_work_retained(
            record.broker_resources_after_restart,
            pending_execution_id=pending_execution_id,
            pending_work_subject=pending_work_subject,
        )
        record.pending_work_evidence_after_restart = {
            "pending_execution_id": pending_execution_id,
            "pending_job": pending_submitted.job,
            "work_messages": record.broker_resources_after_restart.get("work_messages"),
        }

        recover_env, recover_started = _child_env_with_started(
            {
                **base_env,
                "SUPERJOBS_BROKER_RESTART_PHASE": "recover_completed",
                "SUPERJOBS_COMPLETED_EXECUTION_ID": completed_execution_id,
                "SUPERJOBS_SKIP_WORKER_READY": "1",
            },
            log_dir,
            "producer-recover-completed",
        )
        producer_process3, producer_record3, p_out3, p_err3 = _spawn_producer(
            producer_python=producer_python,
            producer_dir=producer_dir,
            env=recover_env,
            log_dir=log_dir,
            label="producer-recover-completed",
        )
        record.producers.append(producer_record3)
        tracked.append(
            _TrackedChild(
                producer_process3,
                producer_record3,
                (p_out3, p_err3),
                recover_started,
            ),
        )
        _wait_producer_ok(
            producer_process3,
            producer_record3,
            (p_out3, p_err3),
            scenario_deadline=scenario_deadline,
            worker_process=None,
        )
        if len(br_protocol.read_invocations(state_dir)) != 1:
            raise BrokerRestartError(
                "recovering completed result must not invoke handlers again",
            )

        br_protocol.ready_path(state_dir).unlink(missing_ok=True)
        br_protocol.stop_path(state_dir).unlink(missing_ok=True)
        worker_env2, worker2_started = _child_env_with_started(
            {**base_env, "SUPERJOBS_WORKER_GENERATION": "2"},
            log_dir,
            "worker-gen2",
        )
        worker_process, worker_record2, w_out2, w_err2 = _spawn_worker(
            worker_python=worker_python,
            worker_dir=worker_dir,
            env=worker_env2,
            log_dir=log_dir,
            label="worker-gen2",
        )
        record.workers.append(worker_record2)
        worker_streams = (w_out2, w_err2)
        tracked.append(
            _TrackedChild(worker_process, worker_record2, worker_streams, worker2_started),
        )
        ready_g2 = br_protocol.wait_for_ready(
            state_dir,
            expected_run_id=run_id,
            deadline=max(
                0.0,
                min(CHILD_TIMEOUT_SECONDS, _wait_budget_seconds(scenario_deadline)),
            ),
            child_process=worker_process,
            expected_worker_generation="2",
            forbidden_pid=worker_record.pid if worker_record else None,
        )
        _assert_ready_pid_matches_spawn(worker_process, ready_g2, label="worker-gen2")
        worker_record2.pid = ready_g2.pid

        await_env, await_started = _child_env_with_started(
            {
                **base_env,
                "SUPERJOBS_BROKER_RESTART_PHASE": "await_pending",
                "SUPERJOBS_PENDING_EXECUTION_ID": pending_execution_id,
                "SUPERJOBS_EXPECT_WORKER_GENERATION": "2",
            },
            log_dir,
            "producer-await-pending",
        )
        producer_process4, producer_record4, p_out4, p_err4 = _spawn_producer(
            producer_python=producer_python,
            producer_dir=producer_dir,
            env=await_env,
            log_dir=log_dir,
            label="producer-await-pending",
        )
        record.producers.append(producer_record4)
        tracked.append(
            _TrackedChild(
                producer_process4,
                producer_record4,
                (p_out4, p_err4),
                await_started,
            ),
        )
        _wait_producer_ok(
            producer_process4,
            producer_record4,
            (p_out4, p_err4),
            scenario_deadline=scenario_deadline,
            worker_process=worker_process,
        )
        br_protocol.wait_for_checkpoint(
            state_dir,
            name=br_protocol.PENDING_DONE,
            expected_run_id=run_id,
            expected_execution_id=pending_execution_id,
            deadline=_wait_budget_seconds(scenario_deadline),
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

        invocations = br_protocol.read_invocations(state_dir)
        assert worker_record is not None and worker_record.pid is not None
        assert worker_record2.pid is not None
        try:
            br_protocol.assert_broker_restart_invocations(
                invocations,
                run_id=run_id,
                completed_execution_id=completed_execution_id,
                pending_execution_id=pending_execution_id,
                allowed_pids_by_generation={
                    "1": worker_record.pid,
                    "2": worker_record2.pid,
                },
                expected_jobs_by_execution={
                    completed_execution_id: completed_done.job,
                    pending_execution_id: pending_submitted.job,
                },
            )
        except br_protocol.ProtocolError as exc:
            raise BrokerRestartError(str(exc)) from exc
        if worker_record2.exit_code != WORKER_EXIT_OK:
            raise BrokerRestartError(
                f"worker-gen2 exit {worker_record2.exit_code} != expected {WORKER_EXIT_OK}",
            )
        record.invocations = [
            {
                "run_id": item.run_id,
                "worker_generation": item.worker_generation,
                "job": item.job,
                "execution_id": item.execution_id,
                "pid": item.pid,
            }
            for item in invocations
        ]
        record.broker_store_fingerprint_final = _broker_store_fingerprint(store_dir)
        _assert_nonempty_store_fingerprint(record.broker_store_fingerprint_final, label="final")
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
        record.checkpoints = _collect_restart_snapshot(state_dir)
        if artifact_scenario_dir is not None:
            _write_restart_artifact(
                artifact_scenario_dir / SCENARIO_NAME / run_id,
                record,
            )
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
        "scenario": (
            _scenario_record_to_json(state.scenario_record)
            if state.scenario_record is not None
            else None
        ),
    }


def verify_python_version(
    *,
    python_version: str,
    work_dir: Path,
    library: Path,
    contract: Path,
    artifact_dir: Path,
    empty_store_control: bool = False,
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
    broker_log = ""
    pass_error: BaseException | None = None
    scenario_error: str | None = None
    record: BrokerRestartScenarioRecord | None = None
    owner = OwnedNatsServer()
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
            "producer": probe_restart_origins(
                state.producer_py,
                state.producer_dir,
                role="producer",
                evidence_dir=evidence_dir,
                python_version=python_version,
            ),
            "worker": probe_restart_origins(
                state.worker_py,
                state.worker_dir,
                role="worker",
                evidence_dir=evidence_dir,
                python_version=python_version,
            ),
        }

        scenario_deadline = time.monotonic() + SCENARIO_TIMEOUT_SECONDS - SHUTDOWN_TIMEOUT_SECONDS
        if _wait_budget_seconds(scenario_deadline) <= 0:
            raise BrokerRestartError("deadline exhausted before broker startup")

        target = owner.start(deadline=_work_deadline(scenario_deadline))
        record = _run_broker_restart_scenario(
            owner=owner,
            nats_url=target.url,
            broker_target=target,
            producer_python=state.producer_py,
            worker_python=state.worker_py,
            producer_dir=state.producer_dir,
            worker_dir=state.worker_dir,
            artifact_scenario_dir=evidence_dir / "scenarios",
            scenario_deadline=scenario_deadline,
            empty_store_control=empty_store_control,
        )
        state.scenario_record = record
        if empty_store_control:
            if record.error:
                raise BrokerRestartError(record.error)
            if not record.empty_store_control_detected:
                raise BrokerRestartError(
                    "empty-store negative control failed: "
                    "missing persisted resources not detected",
                )
            return state
        if record.error:
            raise BrokerRestartError(record.error)
    except BaseException as exc:
        pass_error = exc
        scenario_error = f"{type(exc).__name__}: {exc}"
        state.error = _format_pass_error(exc)
    finally:
        cleanup_error: BaseException | None = None
        if owner.target is not None:
            try:
                owner.stop(
                    owner.target,
                    deadline=time.monotonic() + SHUTDOWN_TIMEOUT_SECONDS,
                )
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
        payload = _pass_payload(state, broker_log)
        if scenario_error is not None:
            payload["scenario_error"] = scenario_error
        if record is not None and record.error and payload.get("error") is None:
            payload["error"] = record.error
        _write_evidence(evidence_dir / "summary.json", payload)
        if cleanup_error is not None and (record is None or record.error is None):
            raise BrokerRestartError(state.error) from cleanup_error
    if pass_error is not None:
        if isinstance(pass_error, BrokerRestartError):
            raise pass_error
        raise BrokerRestartError(state.error or _format_pass_error(pass_error)) from pass_error
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
    parser.add_argument(
        "--empty-store-control",
        action="store_true",
        help=(
            "Run the empty-store negative control (expects missing persisted resources "
            "after an isolated owned-store substitution)."
        ),
    )
    args = parser.parse_args(argv)
    python_versions = collect_python_versions(args)
    parent = args.work_dir or Path(tempfile.gettempdir())
    assert_work_parent_outside_repo(parent)
    parent.mkdir(parents=True, exist_ok=True)
    work_dir = Path(tempfile.mkdtemp(prefix="superjobs-broker-restart-", dir=parent))
    artifact_dir = args.artifact_dir or Path(
        tempfile.mkdtemp(prefix="superjobs-broker-restart-artifacts-", dir=parent),
    )
    artifact_dir.mkdir(parents=True, exist_ok=True)
    owns_default_artifacts = args.artifact_dir is None

    exit_code = 0
    run_error: BaseException | None = None
    try:
        library, contract = build_wheels(work_dir)
        resolve_executable()
        for py_version in python_versions:
            verify_python_version(
                python_version=py_version,
                work_dir=work_dir,
                library=library,
                contract=contract,
                artifact_dir=artifact_dir,
                empty_store_control=args.empty_store_control,
            )
    except (BrokerRestartError, VerificationError, KeyboardInterrupt) as exc:
        exit_code = 1
        run_error = exc
        if isinstance(exc, KeyboardInterrupt):
            print("verify_broker_restart: interrupted", file=sys.stderr)
        else:
            print(f"verify_broker_restart: {exc}", file=sys.stderr)
    except BaseException as exc:
        exit_code = 1
        run_error = exc
        print(f"verify_broker_restart: {exc}", file=sys.stderr)
    finally:
        print(f"Artifact directory: {artifact_dir}", file=sys.stderr)
        if run_error is not None:
            from tools.verify_cross_program import _write_run_error

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

    if args.empty_store_control:
        print("verify_broker_restart: empty-store control OK")
    else:
        print("verify_broker_restart: OK")
    for py_version in python_versions:
        tag = runtime_evidence_tag(py_version)
        label = (
            f"empty-store control {tag}"
            if args.empty_store_control
            else f"Broker restart {tag}: scenario {SCENARIO_NAME}"
        )
        print(label)
    if not owns_default_artifacts:
        print(f"Artifacts written to {artifact_dir}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
