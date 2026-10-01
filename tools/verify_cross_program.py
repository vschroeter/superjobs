#!/usr/bin/env python3
"""Verify installed producer and worker contracts in separate NATS OS processes."""

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
SUPPORT_ROOT = Path(__file__).resolve().parent / "cross_program_support"
ORIGIN_PROBE = Path(__file__).resolve().parent / "wheel_origin_probe.py"

from tests.support.nats_harness.server import OwnedNatsServer  # noqa: E402

from tools.cross_program_support.child_env import (  # noqa: E402
    isolated_child_env,
    python_command,
    subprocess_creationflags,
)
from tools.cross_program_support.protocol import wait_for_ready, write_worker_stop  # noqa: E402
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

assert TYPING_REPO_ROOT == REPO_ROOT

CHILD_TIMEOUT_SECONDS = 30
SCENARIO_TIMEOUT_SECONDS = 90
KILL_REAP_SECONDS = 5
POLL_INTERVAL_SECONDS = 0.05
WORKER_INTENTIONAL_CRASH_CODE = 42
WORKER_EXIT_OK = 0

PRODUCER_EXIT_OK = 0
PRODUCER_EXIT_READINESS = 2
PRODUCER_EXIT_MALFORMED = 10
PRODUCER_EXIT_WORKER_CRASH = 4

ROLE_SHARED_FILES = (
    "protocol.py",
    "scenario_jobs.py",
    "runtime_isolation.py",
    "expectations.py",
)
WORKER_SUPPORT_FILES = ROLE_SHARED_FILES + (
    "worker_handlers.py",
    "worker_app.py",
)
PRODUCER_SUPPORT_FILES = ROLE_SHARED_FILES + ("producer_app.py",)


class CrossProgramError(Exception):
    """Raised when cross-program verification fails."""


@dataclass(frozen=True)
class ScenarioSpec:
    name: str
    start_worker: bool
    start_producer: bool
    expected_producer_code: int | None
    expected_worker_code: int | None


SCENARIOS: tuple[ScenarioSpec, ...] = (
    ScenarioSpec("contracts_ok", True, True, PRODUCER_EXIT_OK, None),
    ScenarioSpec("outcome_failure", True, True, PRODUCER_EXIT_OK, None),
    ScenarioSpec("outcome_cancel", True, True, PRODUCER_EXIT_OK, None),
    ScenarioSpec("outcome_retry", True, True, PRODUCER_EXIT_OK, None),
    ScenarioSpec(
        "control_missing_readiness",
        False,
        True,
        PRODUCER_EXIT_READINESS,
        None,
    ),
    ScenarioSpec(
        "control_worker_crash",
        True,
        True,
        PRODUCER_EXIT_WORKER_CRASH,
        WORKER_INTENTIONAL_CRASH_CODE,
    ),
    ScenarioSpec(
        "control_malformed_payload",
        True,
        True,
        PRODUCER_EXIT_MALFORMED,
        None,
    ),
)


@dataclass
class ChildRecord:
    name: str
    command: list[str]
    cwd: Path
    started_at: float
    pid: int | None = None
    ended_at: float | None = None
    exit_code: int | None = None
    stdout_path: Path | None = None
    stderr_path: Path | None = None

    def stream_text(self) -> tuple[str, str]:
        stdout = (
            self.stdout_path.read_text(encoding="utf-8")
            if self.stdout_path and self.stdout_path.is_file()
            else ""
        )
        stderr = (
            self.stderr_path.read_text(encoding="utf-8")
            if self.stderr_path and self.stderr_path.is_file()
            else ""
        )
        return stdout, stderr


@dataclass
class ScenarioRecord:
    name: str
    run_id: str
    state_dir: Path
    started_at: float
    ended_at: float | None = None
    producer: ChildRecord | None = None
    worker: ChildRecord | None = None
    checkpoints: dict[str, Any] | None = None
    error: str | None = None


@dataclass
class PythonPassState:
    python_version: str
    tag: str
    producer_py: Path
    worker_py: Path
    producer_dir: Path
    worker_dir: Path
    scenario_records: list[ScenarioRecord] = field(default_factory=list)
    origins: dict[str, Any] | None = None
    runtime_version: str | None = None
    broker_log: str = ""
    error: str | None = None


def materialize_role_dir(work_dir: Path, role: str, files: tuple[str, ...]) -> Path:
    dest = work_dir / role
    dest.mkdir(parents=True, exist_ok=True)
    for name in files:
        shutil.copy2(SUPPORT_ROOT / name, dest / name)
    return dest


def probe_role_origins(
    python: Path,
    role_dir: Path,
    *,
    role: str,
    evidence_dir: Path | None,
    python_version: str,
) -> dict[str, Any]:
    probe_dir = role_dir / f"origin_probe_{role}"
    probe_dir.mkdir(parents=True, exist_ok=True)
    for name in (PRODUCER_SUPPORT_FILES if role == "producer" else WORKER_SUPPORT_FILES):
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


def _snapshot_json_file(path: Path) -> Any:
    raw = path.read_text(encoding="utf-8")
    try:
        return json.loads(raw)
    except json.JSONDecodeError as exc:
        return {"error": str(exc), "raw": raw}


def _collect_checkpoint_snapshot(state_dir: Path) -> dict[str, Any]:
    snapshot: dict[str, Any] = {}
    ready_file = state_dir / "worker_ready.json"
    if ready_file.is_file():
        snapshot["ready"] = _snapshot_json_file(ready_file)
    for path in sorted(state_dir.glob("checkpoint_*.json")):
        snapshot[path.name] = _snapshot_json_file(path)
    stop_file = state_dir / "worker_stop.json"
    if stop_file.is_file():
        snapshot["worker_stop"] = _snapshot_json_file(stop_file)
    return snapshot


def _spawn_child(
    *,
    name: str,
    python: Path,
    role_dir: Path,
    script: str,
    cwd: Path,
    env: dict[str, str],
    log_dir: Path,
) -> tuple[subprocess.Popen[str], ChildRecord, TextIO, TextIO]:
    log_dir.mkdir(parents=True, exist_ok=True)
    stdout_path = log_dir / f"{name}.stdout.log"
    stderr_path = log_dir / f"{name}.stderr.log"
    stdout_io = stdout_path.open("w", encoding="utf-8")
    stderr_io = stderr_path.open("w", encoding="utf-8")
    command = python_command(str(python), role_dir, script)
    try:
        process = subprocess.Popen(
            command,
            cwd=cwd,
            env=env,
            stdout=stdout_io,
            stderr=stderr_io,
            text=True,
            creationflags=subprocess_creationflags(),
        )
    except BaseException:
        stdout_io.close()
        stderr_io.close()
        raise
    record = ChildRecord(
        name=name,
        command=command,
        cwd=cwd,
        started_at=time.monotonic(),
        pid=process.pid,
        stdout_path=stdout_path,
        stderr_path=stderr_path,
    )
    return process, record, stdout_io, stderr_io



def _close_child_streams_safe(stdout_io: TextIO | None, stderr_io: TextIO | None) -> None:
    for stream in (stdout_io, stderr_io):
        if stream is None:
            continue
        try:
            stream.close()
        except OSError:
            pass


def _reap_child(
    process: subprocess.Popen[str],
    *,
    label: str,
    deadline: float,
    expected_code: int | None = None,
) -> int:
    wait_deadline = deadline - KILL_REAP_SECONDS
    while time.monotonic() < wait_deadline:
        code = process.poll()
        if code is not None:
            if expected_code is not None and code != expected_code:
                raise CrossProgramError(f"{label} exit {code} != expected {expected_code}")
            return code
        remaining = wait_deadline - time.monotonic()
        if remaining <= 0:
            break
        time.sleep(min(POLL_INTERVAL_SECONDS, remaining))
    if process.poll() is None:
        process.kill()
    while time.monotonic() < deadline:
        try:
            process.wait(timeout=max(0.0, deadline - time.monotonic()))
            break
        except subprocess.TimeoutExpired:
            if process.poll() is not None:
                break
    if process.poll() is None:
        raise CrossProgramError(f"{label} did not exit before scenario deadline")
    code = process.returncode
    if code is None:
        raise CrossProgramError(f"{label} exited without a return code")
    if expected_code is not None and code != expected_code:
        raise CrossProgramError(f"{label} exit {code} != expected {expected_code}")
    return code


def _finalize_child_after_kill(
    process: subprocess.Popen[str],
    record: ChildRecord,
    *,
    deadline: float,
) -> None:
    if process.poll() is None:
        process.kill()
    kill_deadline = min(deadline, time.monotonic() + KILL_REAP_SECONDS)
    while process.poll() is None and time.monotonic() < kill_deadline:
        try:
            process.wait(timeout=max(0.0, kill_deadline - time.monotonic()))
        except subprocess.TimeoutExpired:
            break
    record.exit_code = process.poll()
    record.ended_at = time.monotonic()


def _worker_exit_allowed_during_producer_wait(
    *,
    worker_code: int,
    expected_worker_code: int | None,
) -> bool:
    if expected_worker_code is not None:
        return worker_code == expected_worker_code
    return False


def _wait_for_producer_exit(
    producer_process: subprocess.Popen[str],
    *,
    label: str,
    deadline: float,
    worker_process: subprocess.Popen[str] | None,
    expected_worker_code: int | None,
) -> int:
    while True:
        remaining = deadline - time.monotonic()
        if remaining <= KILL_REAP_SECONDS:
            break
        producer_code = producer_process.poll()
        if producer_code is not None:
            return producer_code
        if worker_process is not None:
            worker_code = worker_process.poll()
            if worker_code is not None and not _worker_exit_allowed_during_producer_wait(
                worker_code=worker_code,
                expected_worker_code=expected_worker_code,
            ):
                raise CrossProgramError(
                    f"worker exited with code {worker_code} while waiting for {label}",
                )
        time.sleep(min(POLL_INTERVAL_SECONDS, max(0.0, remaining - KILL_REAP_SECONDS)))
    timed_out = producer_process.poll() is None
    if timed_out:
        producer_process.kill()
    code = _reap_child(
        producer_process,
        label=label,
        deadline=deadline,
        expected_code=None,
    )
    if timed_out:
        raise CrossProgramError(f"{label} timed out before scenario deadline")
    return code


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
    shutdown_deadline = min(scenario_deadline, time.monotonic() + CHILD_TIMEOUT_SECONDS)
    existing = worker_process.poll()
    if existing is not None:
        record.exit_code = existing
        record.ended_at = time.monotonic()
        _close_child_streams_safe(stdout_io, stderr_io)
        if existing != WORKER_EXIT_OK:
            raise CrossProgramError(f"worker exit {existing} != expected {WORKER_EXIT_OK}")
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


def _run_scenario(
    *,
    spec: ScenarioSpec,
    nats_url: str,
    producer_python: Path,
    worker_python: Path,
    producer_dir: Path,
    worker_dir: Path,
    artifact_scenario_dir: Path | None,
) -> ScenarioRecord:
    run_id = uuid.uuid4().hex
    state_dir = producer_dir / "state" / spec.name / run_id
    state_dir.mkdir(parents=True, exist_ok=True)
    log_dir = (
        artifact_scenario_dir / spec.name / run_id
        if artifact_scenario_dir is not None
        else state_dir / "logs"
    )
    record = ScenarioRecord(
        name=spec.name,
        run_id=run_id,
        state_dir=state_dir,
        started_at=time.monotonic(),
    )
    base_env = {
        "NATS_URL": nats_url,
        "SUPERJOBS_CROSS_RUN_ID": run_id,
        "SUPERJOBS_CROSS_SCENARIO": spec.name,
        "SUPERJOBS_CROSS_STATE_DIR": str(state_dir),
    }
    worker_process: subprocess.Popen[str] | None = None
    producer_process: subprocess.Popen[str] | None = None
    worker_streams: tuple[TextIO, TextIO] | None = None
    producer_streams: tuple[TextIO, TextIO] | None = None
    scenario_deadline = time.monotonic() + SCENARIO_TIMEOUT_SECONDS
    try:
        if spec.start_worker:
            worker_env = isolated_child_env(base_env)
            worker_process, worker_record, w_out, w_err = _spawn_child(
                name="worker",
                python=worker_python,
                role_dir=worker_dir,
                script="worker_app.py",
                cwd=worker_dir,
                env=worker_env,
                log_dir=log_dir,
            )
            record.worker = worker_record
            worker_streams = (w_out, w_err)
            wait_for_ready(
                state_dir,
                expected_run_id=run_id,
                deadline=max(0.0, min(CHILD_TIMEOUT_SECONDS, scenario_deadline - time.monotonic())),
                child_process=worker_process,
            )

        if spec.start_producer:
            producer_env = isolated_child_env(base_env)
            producer_process, producer_record, p_out, p_err = _spawn_child(
                name="producer",
                python=producer_python,
                role_dir=producer_dir,
                script="producer_app.py",
                cwd=producer_dir,
                env=producer_env,
                log_dir=log_dir,
            )
            record.producer = producer_record
            producer_streams = (p_out, p_err)
            producer_code = _wait_for_producer_exit(
                producer_process,
                label="producer",
                deadline=scenario_deadline,
                worker_process=worker_process,
                expected_worker_code=spec.expected_worker_code,
            )
            producer_record.exit_code = producer_code
            producer_record.ended_at = time.monotonic()
            _close_child_streams_safe(p_out, p_err)
            if spec.expected_producer_code is not None:
                if producer_code != spec.expected_producer_code:
                    raise CrossProgramError(
                        f"scenario {spec.name}: producer exit "
                        f"{producer_code} != expected "
                        f"{spec.expected_producer_code}",
                    )

        if worker_process is not None and record.worker is not None:
            w_out, w_err = worker_streams or (None, None)
            assert w_out is not None and w_err is not None
            if spec.expected_worker_code is not None:
                remaining_deadline = scenario_deadline
                existing = worker_process.poll()
                if existing is None:
                    record.worker.exit_code = _reap_child(
                        worker_process,
                        label=f"worker ({spec.name})",
                        deadline=remaining_deadline,
                        expected_code=spec.expected_worker_code,
                    )
                else:
                    record.worker.exit_code = existing
                    if existing != spec.expected_worker_code:
                        raise CrossProgramError(
                            f"worker ({spec.name}) exit {existing} != expected "
                            f"{spec.expected_worker_code}",
                        )
                record.worker.ended_at = time.monotonic()
                _close_child_streams_safe(w_out, w_err)
            else:
                _shutdown_worker_cooperatively(
                    worker_process=worker_process,
                    record=record.worker,
                    state_dir=state_dir,
                    run_id=run_id,
                    stdout_io=w_out,
                    stderr_io=w_err,
                    scenario_deadline=scenario_deadline,
                )
    except BaseException as exc:
        record.error = f"{type(exc).__name__}: {exc}"
        if producer_process is not None and record.producer is not None:
            try:
                _finalize_child_after_kill(
                    producer_process,
                    record.producer,
                    deadline=scenario_deadline,
                )
            except BaseException:
                pass
            try:
                if producer_streams is not None:
                    _close_child_streams_safe(*producer_streams)
            except BaseException:
                pass
        if worker_process is not None and record.worker is not None:
            try:
                _finalize_child_after_kill(
                    worker_process,
                    record.worker,
                    deadline=scenario_deadline,
                )
            except BaseException:
                pass
            try:
                if worker_streams is not None:
                    _close_child_streams_safe(*worker_streams)
            except BaseException:
                pass
    finally:
        record.ended_at = time.monotonic()
        record.checkpoints = _collect_checkpoint_snapshot(state_dir)
        if artifact_scenario_dir is not None:
            _write_scenario_artifact(artifact_scenario_dir / spec.name / run_id, record)
    return record


def _child_to_json(child: ChildRecord | None) -> dict[str, Any] | None:
    if child is None:
        return None
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
        "stdout_path": None if child.stdout_path is None else str(child.stdout_path),
        "stderr_path": None if child.stderr_path is None else str(child.stderr_path),
    }


def _scenario_to_json(item: ScenarioRecord) -> dict[str, Any]:
    producer_stdout, producer_stderr = (
        item.producer.stream_text() if item.producer is not None else ("", "")
    )
    worker_stdout, worker_stderr = (
        item.worker.stream_text() if item.worker is not None else ("", "")
    )
    return {
        "name": item.name,
        "run_id": item.run_id,
        "started_at_monotonic": item.started_at,
        "ended_at_monotonic": item.ended_at,
        "duration_seconds": (item.ended_at or item.started_at) - item.started_at,
        "error": item.error,
        "checkpoints": item.checkpoints,
        "producer": _child_to_json(item.producer),
        "worker": _child_to_json(item.worker),
        "producer_exit": None if item.producer is None else item.producer.exit_code,
        "worker_exit": None if item.worker is None else item.worker.exit_code,
        "producer_stdout": producer_stdout,
        "producer_stderr": producer_stderr,
        "worker_stdout": worker_stdout,
        "worker_stderr": worker_stderr,
    }


def _write_scenario_artifact(path: Path, record: ScenarioRecord) -> None:
    _write_evidence(path / "scenario.json", _scenario_to_json(record))


def _write_evidence(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2), encoding="utf-8")


def _pass_payload(state: PythonPassState) -> dict[str, Any]:
    return {
        "python_version": state.python_version,
        "runtime_version": state.runtime_version,
        "producer_interpreter": str(state.producer_py),
        "worker_interpreter": str(state.worker_py),
        "origins": state.origins,
        "error": state.error,
        "broker_log": state.broker_log,
        "scenarios": [_scenario_to_json(item) for item in state.scenario_records],
    }


def _parse_verification_error(exc: VerificationError) -> dict[str, Any]:
    message = str(exc)
    payload: dict[str, Any] = {
        "error": message,
        "exception_type": type(exc).__name__,
    }
    stdout_marker = "\nstdout:\n"
    stderr_marker = "\nstderr:\n"
    if stdout_marker in message and stderr_marker in message:
        head, rest = message.split(stdout_marker, 1)
        stdout_part, stderr_part = rest.split(stderr_marker, 1)
        payload["summary"] = head
        payload["subprocess_stdout"] = stdout_part
        payload["subprocess_stderr"] = stderr_part
    return payload


def _write_run_error(artifact_dir: Path, exc: BaseException) -> None:
    if isinstance(exc, VerificationError):
        payload = _parse_verification_error(exc)
    else:
        payload = {"error": str(exc), "exception_type": type(exc).__name__}
    _write_evidence(artifact_dir / "run_error.json", payload)


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
    owner: OwnedNatsServer | None = None
    artifact_scenarios = evidence_dir / "scenarios"
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
            "producer": probe_role_origins(
                state.producer_py,
                state.producer_dir,
                role="producer",
                evidence_dir=evidence_dir,
                python_version=python_version,
            ),
            "worker": probe_role_origins(
                state.worker_py,
                state.worker_dir,
                role="worker",
                evidence_dir=evidence_dir,
                python_version=python_version,
            ),
        }

        owner = OwnedNatsServer()
        target = owner.start()
        for spec in SCENARIOS:
            record = _run_scenario(
                spec=spec,
                nats_url=target.url,
                producer_python=state.producer_py,
                worker_python=state.worker_py,
                producer_dir=state.producer_dir,
                worker_dir=state.worker_dir,
                artifact_scenario_dir=artifact_scenarios,
            )
            state.scenario_records.append(record)
            if record.error:
                raise CrossProgramError(record.error)
    except BaseException as exc:
        state.error = str(exc)
        raise
    finally:
        broker_log = ""
        cleanup_error: BaseException | None = None
        if owner is not None and owner.target is not None:
            try:
                owner.stop(owner.target)
            except BaseException as exc:
                cleanup_error = exc
                if state.error is None:
                    state.error = f"Broker cleanup failed: {type(exc).__name__}: {exc}"
            finally:
                log_path = owner.target.log_path
                if log_path is not None and log_path.is_file():
                    broker_log = log_path.read_text(encoding="utf-8")
        state.broker_log = broker_log
        _write_evidence(evidence_dir / "summary.json", _pass_payload(state))
        if cleanup_error is not None and sys.exc_info()[0] is None:
            raise CrossProgramError(state.error) from cleanup_error
    return state


def collect_python_versions(args: argparse.Namespace) -> tuple[str, ...]:
    versions: list[str] = list(args.python_versions or [])
    if not versions:
        return DEFAULT_RUNTIMES
    return tuple(dict.fromkeys(versions))


def print_summary(records: dict[str, dict[str, Any]]) -> None:
    for py_version, payload in records.items():
        tag = runtime_evidence_tag(py_version)
        scenarios = payload.get("scenarios") or []
        print(
            f"Cross-program {tag}: {len(scenarios)} scenarios, "
            f"runtime {payload.get('runtime_version')}, origins OK"
        )
        for item in scenarios:
            print(
                f"  - {item['name']}: producer={item.get('producer_exit')} "
                f"worker={item.get('worker_exit')}"
            )


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
    work_dir = Path(tempfile.mkdtemp(prefix="superjobs-cross-program-", dir=parent))
    artifact_dir = args.artifact_dir or Path(
        tempfile.mkdtemp(prefix="superjobs-cross-artifacts-", dir=parent),
    )
    artifact_dir.mkdir(parents=True, exist_ok=True)
    owns_default_artifacts = args.artifact_dir is None

    records: dict[str, dict[str, Any]] = {}
    exit_code = 0
    run_error: BaseException | None = None
    try:
        library, contract = build_wheels(work_dir)
        for py_version in python_versions:
            pass_state = verify_python_version(
                python_version=py_version,
                work_dir=work_dir,
                library=library,
                contract=contract,
                artifact_dir=artifact_dir,
            )
            records[py_version] = _pass_payload(pass_state)
    except (CrossProgramError, VerificationError, KeyboardInterrupt) as exc:
        exit_code = 1
        run_error = exc
        if isinstance(exc, KeyboardInterrupt):
            print("verify_cross_program: interrupted", file=sys.stderr)
        else:
            print(f"verify_cross_program: {exc}", file=sys.stderr)
    except BaseException as exc:
        exit_code = 1
        run_error = exc
        print(f"verify_cross_program: {exc}", file=sys.stderr)
    finally:
        print(f"Artifact directory: {artifact_dir}", file=sys.stderr)
        if run_error is not None:
            _write_run_error(artifact_dir, run_error)
        if args.keep_work:
            print(f"Work directory (inspection): {work_dir}", file=sys.stderr)
        elif work_dir.exists():
            shutil.rmtree(work_dir, ignore_errors=True)
        if owns_default_artifacts and exit_code == 0:
            shutil.rmtree(artifact_dir, ignore_errors=True)

    if exit_code != 0:
        return exit_code

    print("verify_cross_program: OK")
    print_summary(records)
    if not owns_default_artifacts:
        print(f"Artifacts written to {artifact_dir}")
    if args.keep_work:
        print(f"Work directory preserved at {work_dir}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
