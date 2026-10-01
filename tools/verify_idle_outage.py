#!/usr/bin/env python3
"""Verify idle producer and worker recovery after a short NATS broker outage."""

from __future__ import annotations

import argparse
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
SUPPORT_ROOT = Path(__file__).resolve().parent / "idle_outage_support"
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
    _close_child_streams_safe,
    _collect_checkpoint_snapshot,
    _finalize_child_after_kill,
    _reap_child,
    _spawn_child,
    _write_evidence,
)
from tools.verify_broker_restart import (  # noqa: E402
    _assert_ready_pid_matches_spawn,
    _broker_store_dir,
    _broker_store_fingerprint,
    _child_env_with_started,
    _finalize_tracked_children,
    _format_pass_error,
    _restart_phase_deadline,
    _spawn_producer,
    _spawn_worker,
    _TrackedChild,
    _wait_budget_seconds,
    _work_deadline,
)
from tools.idle_outage_support import protocol as io_protocol  # noqa: E402

assert TYPING_REPO_ROOT == REPO_ROOT

CHILD_TIMEOUT_SECONDS = 30
SCENARIO_TIMEOUT_SECONDS = 90
RESTART_PHASE_BUDGET_SECONDS = SHUTDOWN_TIMEOUT_SECONDS + STARTUP_TIMEOUT_SECONDS
KILL_REAP_SECONDS = 5
WORKER_EXIT_OK = 0
PRODUCER_EXIT_OK = 0

SCENARIO_NAME = "idle_outage_recovery"

ROLE_SHARED_FILES = (
    "protocol.py",
    "scenario_jobs.py",
    "runtime_isolation.py",
    "queue_config.py",
    "broker_callbacks.py",
    "broker_connect.py",
)
WORKER_SUPPORT_FILES = ROLE_SHARED_FILES + (
    "worker_handlers.py",
    "worker_app.py",
)
PRODUCER_SUPPORT_FILES = ROLE_SHARED_FILES + ("producer_app.py",)


class IdleOutageError(Exception):
    """Raised when idle outage verification fails."""


@dataclass
class IdleOutageScenarioRecord:
    name: str
    run_id: str
    state_dir: Path
    broker_store_dir: Path
    broker_store_fingerprint_before: str
    broker_store_fingerprint_after_restart: str
    started_at: float
    ended_at: float | None = None
    baseline_execution_id: str | None = None
    post_outage_execution_id: str | None = None
    producer_pid: int | None = None
    worker_pid: int | None = None
    broker_pid_before: int | None = None
    broker_pid_after_restart: int | None = None
    workers: list[ChildRecord] = field(default_factory=list)
    producers: list[ChildRecord] = field(default_factory=list)
    checkpoints: dict[str, Any] | None = None
    invocations: list[dict[str, Any]] | None = None
    outage_read_error_type: str | None = None
    producer_runtime_object_id: int | None = None
    worker_runtime_object_id: int | None = None
    baseline_handle_object_id: int | None = None
    error: str | None = None


@dataclass
class PythonPassState:
    python_version: str
    tag: str
    producer_py: Path
    worker_py: Path
    producer_dir: Path
    worker_dir: Path
    scenario_record: IdleOutageScenarioRecord | None = None
    origins: dict[str, Any] | None = None
    runtime_version: str | None = None
    error: str | None = None


def materialize_role_dir(work_dir: Path, role: str, files: tuple[str, ...]) -> Path:
    dest = work_dir / role
    dest.mkdir(parents=True, exist_ok=True)
    for name in files:
        shutil.copy2(SUPPORT_ROOT / name, dest / name)
    return dest


def probe_idle_origins(
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


def _issue_producer_command(
    state_dir: Path,
    *,
    run_id: str,
    command: str,
) -> str:
    request_id = uuid.uuid4().hex
    io_protocol.write_producer_request(
        state_dir,
        run_id=run_id,
        request_id=request_id,
        command=command,
    )
    return request_id


def _shutdown_long_running_children(
    *,
    worker_process: subprocess.Popen[str],
    worker_record: ChildRecord,
    producer_process: subprocess.Popen[str],
    producer_record: ChildRecord,
    state_dir: Path,
    run_id: str,
    worker_streams: tuple[TextIO, TextIO],
    producer_streams: tuple[TextIO, TextIO],
    scenario_deadline: float,
) -> None:
    deadline = min(
        scenario_deadline,
        time.monotonic() + CHILD_TIMEOUT_SECONDS,
    )
    write_worker_stop(state_dir, run_id)
    producer_record.exit_code = _reap_child(
        producer_process,
        label="producer",
        deadline=deadline,
        expected_code=PRODUCER_EXIT_OK,
    )
    producer_record.ended_at = time.monotonic()
    worker_record.exit_code = _reap_child(
        worker_process,
        label="worker",
        deadline=deadline,
        expected_code=WORKER_EXIT_OK,
    )
    worker_record.ended_at = time.monotonic()
    _close_child_streams_safe(*producer_streams)
    _close_child_streams_safe(*worker_streams)


def _assert_callback_marker(
    marker: io_protocol.CheckpointMarker,
    *,
    expected_pid: int,
    expected_runtime_object_id: int,
    expected_callback_sequence: int,
    label: str,
) -> None:
    if marker.pid != expected_pid:
        raise IdleOutageError(
            f"{label} callback pid {marker.pid} != live process pid {expected_pid}",
        )
    if marker.runtime_object_id != expected_runtime_object_id:
        raise IdleOutageError(
            f"{label} callback runtime_object_id {marker.runtime_object_id} "
            f"!= {expected_runtime_object_id}",
        )
    if marker.callback_sequence != expected_callback_sequence:
        raise IdleOutageError(
            f"{label} callback_sequence {marker.callback_sequence} "
            f"!= expected {expected_callback_sequence}",
        )


def _assert_process_alive(process: subprocess.Popen[str], *, label: str) -> None:
    if process.poll() is not None:
        raise IdleOutageError(f"{label} exited with code {process.poll()} during outage")


def _scenario_record_to_json(item: IdleOutageScenarioRecord) -> dict[str, Any]:
    ended = item.ended_at if item.ended_at is not None else item.started_at
    return {
        "name": item.name,
        "run_id": item.run_id,
        "baseline_execution_id": item.baseline_execution_id,
        "post_outage_execution_id": item.post_outage_execution_id,
        "producer_pid": item.producer_pid,
        "worker_pid": item.worker_pid,
        "broker_store_dir": str(item.broker_store_dir),
        "broker_store_fingerprint_before": item.broker_store_fingerprint_before,
        "broker_store_fingerprint_after_restart": item.broker_store_fingerprint_after_restart,
        "broker_pid_before": item.broker_pid_before,
        "broker_pid_after_restart": item.broker_pid_after_restart,
        "outage_read_error_type": item.outage_read_error_type,
        "started_at_monotonic": item.started_at,
        "ended_at_monotonic": item.ended_at,
        "duration_seconds": ended - item.started_at,
        "error": item.error,
        "checkpoints": item.checkpoints,
        "invocations": item.invocations,
        "workers": [
            {
                "name": child.name,
                "pid": child.pid,
                "exit_code": child.exit_code,
            }
            for child in item.workers
        ],
        "producers": [
            {
                "name": child.name,
                "pid": child.pid,
                "exit_code": child.exit_code,
            }
            for child in item.producers
        ],
    }


def _write_idle_artifact(path: Path, record: IdleOutageScenarioRecord) -> None:
    _write_evidence(path / "scenario.json", _scenario_record_to_json(record))


def _run_idle_outage_scenario(
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
) -> IdleOutageScenarioRecord:
    run_id = uuid.uuid4().hex
    state_dir = producer_dir.parent.parent / "state" / run_id
    state_dir.mkdir(parents=True, exist_ok=True)
    log_dir = (
        artifact_scenario_dir / SCENARIO_NAME / run_id
        if artifact_scenario_dir is not None
        else state_dir / "logs"
    )
    store_dir = _broker_store_dir(broker_target)
    record = IdleOutageScenarioRecord(
        name=SCENARIO_NAME,
        run_id=run_id,
        state_dir=state_dir,
        broker_store_dir=store_dir,
        broker_store_fingerprint_before=_broker_store_fingerprint(store_dir),
        broker_store_fingerprint_after_restart="",
        started_at=time.monotonic(),
        broker_pid_before=(
            broker_target.process.pid if broker_target.process is not None else None
        ),
    )
    base_env = {
        "NATS_URL": nats_url,
        "SUPERJOBS_CROSS_RUN_ID": run_id,
        "SUPERJOBS_CROSS_STATE_DIR": str(state_dir),
    }
    tracked: list[_TrackedChild] = []
    worker_process: subprocess.Popen[str] | None = None
    producer_process: subprocess.Popen[str] | None = None
    worker_streams: tuple[TextIO, TextIO] | None = None
    producer_streams: tuple[TextIO, TextIO] | None = None
    worker_record: ChildRecord | None = None
    producer_record: ChildRecord | None = None
    try:
        worker_env, worker_started = _child_env_with_started(
            {**base_env, "SUPERJOBS_WORKER_GENERATION": "1"},
            log_dir,
            "worker",
        )
        worker_process, worker_record, w_out, w_err = _spawn_worker(
            worker_python=worker_python,
            worker_dir=worker_dir,
            env=worker_env,
            log_dir=log_dir,
            label="worker",
        )
        record.workers.append(worker_record)
        worker_streams = (w_out, w_err)
        tracked.append(_TrackedChild(worker_process, worker_record, worker_streams, worker_started))

        ready = io_protocol.wait_for_ready(
            state_dir,
            expected_run_id=run_id,
            deadline=max(
                0.0,
                min(CHILD_TIMEOUT_SECONDS, _wait_budget_seconds(scenario_deadline)),
            ),
            child_process=worker_process,
            expected_worker_generation="1",
        )
        _assert_ready_pid_matches_spawn(worker_process, ready, label="worker")
        worker_record.pid = ready.pid
        record.worker_pid = ready.pid
        worker_runtime = io_protocol.wait_for_checkpoint(
            state_dir,
            name=io_protocol.WORKER_RUNTIME_READY,
            expected_run_id=run_id,
            deadline=_wait_budget_seconds(scenario_deadline),
            child_process=worker_process,
            child_label="worker",
        )
        if worker_runtime.runtime_object_id is None:
            raise IdleOutageError("worker_runtime_ready missing runtime_object_id")
        record.worker_runtime_object_id = worker_runtime.runtime_object_id

        producer_env, producer_started = _child_env_with_started(
            base_env,
            log_dir,
            "producer",
        )
        producer_process, producer_record, p_out, p_err = _spawn_producer(
            producer_python=producer_python,
            producer_dir=producer_dir,
            env=producer_env,
            log_dir=log_dir,
            label="producer",
        )
        record.producers.append(producer_record)
        producer_streams = (p_out, p_err)
        tracked.append(
            _TrackedChild(producer_process, producer_record, producer_streams, producer_started),
        )

        runtime_ready = io_protocol.wait_for_checkpoint(
            state_dir,
            name=io_protocol.PRODUCER_RUNTIME_READY,
            expected_run_id=run_id,
            deadline=_wait_budget_seconds(scenario_deadline),
            child_process=producer_process,
            child_label="producer",
        )
        if runtime_ready.pid is None:
            raise IdleOutageError("producer runtime ready checkpoint missing pid")
        producer_record.pid = runtime_ready.pid
        record.producer_pid = runtime_ready.pid
        if runtime_ready.runtime_object_id is None:
            raise IdleOutageError("producer_runtime_ready missing runtime_object_id")
        record.producer_runtime_object_id = runtime_ready.runtime_object_id
        _assert_process_alive(worker_process, label="worker")
        _assert_process_alive(producer_process, label="producer")

        baseline_request = _issue_producer_command(
            state_dir,
            run_id=run_id,
            command=io_protocol.COMMAND_BASELINE,
        )
        baseline_done = io_protocol.wait_for_checkpoint(
            state_dir,
            name=io_protocol.BASELINE_DONE,
            expected_run_id=run_id,
            expected_request_id=baseline_request,
            deadline=_wait_budget_seconds(scenario_deadline),
            child_process=producer_process,
            child_label="producer",
        )
        if not baseline_done.execution_id:
            raise IdleOutageError("baseline_done checkpoint missing execution_id")
        record.baseline_execution_id = baseline_done.execution_id
        if baseline_done.handle_object_id is None:
            raise IdleOutageError("baseline_done missing handle_object_id")
        record.baseline_handle_object_id = baseline_done.handle_object_id
        if baseline_done.runtime_object_id != record.producer_runtime_object_id:
            raise IdleOutageError("baseline_done runtime_object_id mismatch")
        io_protocol.wait_for_invocation_count(
            state_dir,
            minimum=1,
            deadline=_wait_budget_seconds(scenario_deadline),
        )
        record.broker_store_fingerprint_before = _broker_store_fingerprint(store_dir)

        if broker_target.process is None:
            raise IdleOutageError("broker process missing before outage")
        pause_deadline = _restart_phase_deadline(scenario_deadline)
        owner.pause(broker_target, deadline=pause_deadline)
        _assert_process_alive(worker_process, label="worker")
        _assert_process_alive(producer_process, label="producer")

        disconnect_deadline = _wait_budget_seconds(scenario_deadline)
        io_protocol.wait_for_checkpoint(
            state_dir,
            name=io_protocol.BROKER_DISCONNECTED_PRODUCER,
            expected_run_id=run_id,
            deadline=disconnect_deadline,
            child_process=producer_process,
            child_label="producer",
        )
        io_protocol.wait_for_checkpoint(
            state_dir,
            name=io_protocol.BROKER_DISCONNECTED_WORKER,
            expected_run_id=run_id,
            deadline=disconnect_deadline,
            child_process=worker_process,
            child_label="worker",
        )
        assert record.producer_pid is not None
        assert record.producer_runtime_object_id is not None
        assert record.worker_runtime_object_id is not None
        _assert_callback_marker(
            io_protocol.read_checkpoint(state_dir, io_protocol.BROKER_DISCONNECTED_PRODUCER),
            expected_pid=record.producer_pid,
            expected_runtime_object_id=record.producer_runtime_object_id,
            expected_callback_sequence=1,
            label="producer disconnect",
        )
        _assert_callback_marker(
            io_protocol.read_checkpoint(state_dir, io_protocol.BROKER_DISCONNECTED_WORKER),
            expected_pid=record.worker_pid,
            expected_runtime_object_id=record.worker_runtime_object_id,
            expected_callback_sequence=1,
            label="worker disconnect",
        )

        outage_read_request = _issue_producer_command(
            state_dir,
            run_id=run_id,
            command=io_protocol.COMMAND_READ_DURING_OUTAGE,
        )
        read_failed = io_protocol.wait_for_checkpoint(
            state_dir,
            name=io_protocol.HANDLE_READ_FAILED,
            expected_run_id=run_id,
            expected_request_id=outage_read_request,
            deadline=_wait_budget_seconds(scenario_deadline),
            child_process=producer_process,
            child_label="producer",
        )
        try:
            io_protocol.assert_allowed_outage_read_error(read_failed.error_type)
        except io_protocol.ProtocolError as exc:
            raise IdleOutageError(str(exc)) from exc
        if read_failed.handle_id != record.baseline_execution_id:
            raise IdleOutageError(
                "outage read checkpoint handle_id does not match baseline execution",
            )
        if read_failed.elapsed_seconds is None or read_failed.elapsed_seconds > 5.0:
            raise IdleOutageError(
                "outage read must be bounded (missing or excessive elapsed_seconds)",
            )
        if read_failed.runtime_object_id != record.producer_runtime_object_id:
            raise IdleOutageError("outage read runtime_object_id changed")
        if read_failed.handle_object_id != record.baseline_handle_object_id:
            raise IdleOutageError("outage read handle_object_id changed")
        record.outage_read_error_type = read_failed.error_type

        restarted = owner.restart(broker_target, deadline=_restart_phase_deadline(scenario_deadline))
        record.broker_pid_after_restart = (
            restarted.process.pid if restarted.process is not None else None
        )
        if record.broker_pid_before == record.broker_pid_after_restart:
            raise IdleOutageError("broker pid must change across restart")
        if restarted.url != nats_url:
            raise IdleOutageError("broker client URL must be preserved across restart")
        record.broker_store_fingerprint_after_restart = _broker_store_fingerprint(store_dir)

        reconnect_deadline = _wait_budget_seconds(scenario_deadline)
        io_protocol.wait_for_checkpoint(
            state_dir,
            name=io_protocol.BROKER_RECONNECTED_PRODUCER,
            expected_run_id=run_id,
            deadline=reconnect_deadline,
            child_process=producer_process,
            child_label="producer",
        )
        io_protocol.wait_for_checkpoint(
            state_dir,
            name=io_protocol.BROKER_RECONNECTED_WORKER,
            expected_run_id=run_id,
            deadline=reconnect_deadline,
            child_process=worker_process,
            child_label="worker",
        )
        _assert_callback_marker(
            io_protocol.read_checkpoint(state_dir, io_protocol.BROKER_RECONNECTED_PRODUCER),
            expected_pid=record.producer_pid,
            expected_runtime_object_id=record.producer_runtime_object_id,
            expected_callback_sequence=2,
            label="producer reconnect",
        )
        _assert_callback_marker(
            io_protocol.read_checkpoint(state_dir, io_protocol.BROKER_RECONNECTED_WORKER),
            expected_pid=record.worker_pid,
            expected_runtime_object_id=record.worker_runtime_object_id,
            expected_callback_sequence=2,
            label="worker reconnect",
        )
        _assert_process_alive(worker_process, label="worker")
        _assert_process_alive(producer_process, label="producer")
        if io_protocol.read_ready(state_dir).pid != record.worker_pid:
            raise IdleOutageError("worker pid changed across outage")
        producer_runtime = io_protocol.read_checkpoint(
            state_dir,
            io_protocol.PRODUCER_RUNTIME_READY,
        )
        if producer_runtime.pid != record.producer_pid:
            raise IdleOutageError("producer pid changed across outage")

        reconnect_read_request = _issue_producer_command(
            state_dir,
            run_id=run_id,
            command=io_protocol.COMMAND_READ_AFTER_RECONNECT,
        )
        read_succeeded = io_protocol.wait_for_checkpoint(
            state_dir,
            name=io_protocol.HANDLE_READ_SUCCEEDED,
            expected_run_id=run_id,
            expected_request_id=reconnect_read_request,
            deadline=_wait_budget_seconds(scenario_deadline),
            child_process=producer_process,
            child_label="producer",
        )
        if read_succeeded.runtime_object_id != record.producer_runtime_object_id:
            raise IdleOutageError("reconnect read runtime_object_id changed")
        if read_succeeded.handle_object_id != record.baseline_handle_object_id:
            raise IdleOutageError("reconnect read handle_object_id changed")

        post_request = _issue_producer_command(
            state_dir,
            run_id=run_id,
            command=io_protocol.COMMAND_POST_OUTAGE,
        )
        post_done = io_protocol.wait_for_checkpoint(
            state_dir,
            name=io_protocol.POST_OUTAGE_DONE,
            expected_run_id=run_id,
            expected_request_id=post_request,
            deadline=_wait_budget_seconds(scenario_deadline),
            child_process=producer_process,
            child_label="producer",
        )
        if not post_done.execution_id:
            raise IdleOutageError("post_outage_done checkpoint missing execution_id")
        record.post_outage_execution_id = post_done.execution_id

        invocations = io_protocol.wait_for_invocation_count(
            state_dir,
            minimum=2,
            deadline=_wait_budget_seconds(scenario_deadline),
        )
        assert record.baseline_execution_id is not None
        assert record.worker_pid is not None
        io_protocol.assert_idle_outage_invocations(
            invocations,
            run_id=run_id,
            baseline_execution_id=record.baseline_execution_id,
            post_outage_execution_id=record.post_outage_execution_id,
            worker_pid=record.worker_pid,
            expected_jobs_by_execution={
                record.baseline_execution_id: baseline_done.job,
                record.post_outage_execution_id: post_done.job,
            },
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

        assert (
            worker_process is not None
            and producer_process is not None
            and worker_record is not None
            and producer_record is not None
            and worker_streams is not None
            and producer_streams is not None
        )
        _shutdown_long_running_children(
            worker_process=worker_process,
            worker_record=worker_record,
            producer_process=producer_process,
            producer_record=producer_record,
            state_dir=state_dir,
            run_id=run_id,
            worker_streams=worker_streams,
            producer_streams=producer_streams,
            scenario_deadline=scenario_deadline,
        )
        worker_process = None
        producer_process = None
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
        record.checkpoints = _collect_checkpoint_snapshot(state_dir)
        invocations_path = io_protocol.invocations_path(state_dir)
        if invocations_path.is_file():
            from tools.verify_cross_program import _snapshot_json_file

            record.checkpoints = record.checkpoints or {}
            record.checkpoints["handler_invocations.json"] = _snapshot_json_file(invocations_path)
        if artifact_scenario_dir is not None:
            _write_idle_artifact(
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
    record: IdleOutageScenarioRecord | None = None
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
            "producer": probe_idle_origins(
                state.producer_py,
                state.producer_dir,
                role="producer",
                evidence_dir=evidence_dir,
                python_version=python_version,
            ),
            "worker": probe_idle_origins(
                state.worker_py,
                state.worker_dir,
                role="worker",
                evidence_dir=evidence_dir,
                python_version=python_version,
            ),
        }

        scenario_deadline = time.monotonic() + SCENARIO_TIMEOUT_SECONDS - SHUTDOWN_TIMEOUT_SECONDS
        if _wait_budget_seconds(scenario_deadline) <= 0:
            raise IdleOutageError("deadline exhausted before broker startup")

        target = owner.start(deadline=_work_deadline(scenario_deadline))
        record = _run_idle_outage_scenario(
            owner=owner,
            nats_url=target.url,
            broker_target=target,
            producer_python=state.producer_py,
            worker_python=state.worker_py,
            producer_dir=state.producer_dir,
            worker_dir=state.worker_dir,
            artifact_scenario_dir=evidence_dir / "scenarios",
            scenario_deadline=scenario_deadline,
        )
        state.scenario_record = record
        if record.error:
            raise IdleOutageError(record.error)
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
            raise IdleOutageError(state.error) from cleanup_error
    if pass_error is not None:
        if isinstance(pass_error, IdleOutageError):
            raise pass_error
        raise IdleOutageError(state.error or _format_pass_error(pass_error)) from pass_error
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
    parent.mkdir(parents=True, exist_ok=True)
    work_dir = Path(tempfile.mkdtemp(prefix="superjobs-idle-outage-", dir=parent))
    artifact_dir = args.artifact_dir or Path(
        tempfile.mkdtemp(prefix="superjobs-idle-outage-artifacts-", dir=parent),
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
            )
    except (IdleOutageError, VerificationError, KeyboardInterrupt) as exc:
        exit_code = 1
        run_error = exc
        if isinstance(exc, KeyboardInterrupt):
            print("verify_idle_outage: interrupted", file=sys.stderr)
        else:
            print(f"verify_idle_outage: {exc}", file=sys.stderr)
    except BaseException as exc:
        exit_code = 1
        run_error = exc
        print(f"verify_idle_outage: {exc}", file=sys.stderr)
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

    print("verify_idle_outage: OK")
    for py_version in python_versions:
        tag = runtime_evidence_tag(py_version)
        print(f"Idle outage {tag}: scenario {SCENARIO_NAME}")
    if not owns_default_artifacts:
        print(f"Artifacts written to {artifact_dir}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
