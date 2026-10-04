#!/usr/bin/env python3
"""Verify installed contract-interface CLI modes in isolated OS processes (issue #42)."""

from __future__ import annotations

import argparse
import json
import os
import re
import shutil
import signal
import subprocess
import sys
import tempfile
import time
import uuid
from dataclasses import dataclass, field
from pathlib import Path
from collections.abc import Callable
from typing import Any, TextIO

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

EXAMPLE_ROOT = REPO_ROOT / "examples" / "contract_interface"
CLI_EXAMPLE_SRC = EXAMPLE_ROOT / "superjobs_contract_cli_example"
WORKER_EXAMPLE_SRC = EXAMPLE_ROOT / "superjobs_contract_worker_example"
WORKER_RESOURCES_SRC = EXAMPLE_ROOT / "superjobs_contract_worker_resources"
SUPPORT = Path(__file__).resolve().parent / "cli_process_support"
ORIGIN_PROBE = Path(__file__).resolve().parent / "cli_origin_probe.py"
WHEEL_ORIGIN_PROBE = Path(__file__).resolve().parent / "wheel_origin_probe.py"
OBSERVE_SCRIPT = SUPPORT / "observe_execution.py"
WIN_CTRL_C = SUPPORT / "win_ctrl_c.py"

from tests.support.nats_harness.server import OwnedNatsServer  # noqa: E402

from scripts.dev_check.process_tree import kill_process_tree  # noqa: E402

from tools.cli_process_support.child_owner import (  # noqa: E402
    ChildOwner,
    OwnedChild,
    current_child_owner,
    reap_with_owner,
)
from tools.cli_process_support.observations import (  # noqa: E402
    assert_manifest_with_events_stream,
    parse_observation_lines,
)
from tools.cli_process_support.protocol import (  # noqa: E402
    ProtocolError,
    assert_checkpoint_absent,
    checkpoint_path,
    wait_for_checkpoint,
    wait_for_worker_ready,
    write_gate_release,
    write_worker_stop,
)
from tools.cross_program_support.child_env import isolated_child_env, subprocess_creationflags  # noqa: E402
from tools.verify_contract_typing import (  # noqa: E402
    DEFAULT_RUNTIMES,
    REPO_ROOT as TYPING_REPO_ROOT,
    VerificationError,
    assert_final_runtime_python,
    assert_work_parent_outside_repo,
    build_wheels,
    run_cmd,
    runtime_evidence_tag,
    strip_contract_sources,
    venv_python,
)

assert TYPING_REPO_ROOT == REPO_ROOT

EXIT_SUCCESS = 0
EXIT_RUNTIME_FAILURE = 1
EXIT_USAGE = 2
EXIT_INTERRUPTED = 130

CHILD_TIMEOUT_SECONDS = 120
SCENARIO_TIMEOUT_SECONDS = 180
KILL_REAP_SECONDS = 5
POLL_INTERVAL_SECONDS = 0.05
WORKER_STARTUP_DEADLINE = 30.0
CHECKPOINT_DEADLINE = 60.0
INTERRUPT_DEADLINE = 45.0

CLI_ENTRY = "superjobs-contract-cli"
WORKER_ENTRY = "superjobs-contract-worker"

def _exit_code_matches(code: int, expected: int) -> bool:
    return code == expected


EXECUTION_REF_RE = re.compile(r"execution reference:\s*(\{.*\})", re.DOTALL)


class CliProcessError(Exception):
    """Raised when installed CLI process verification fails."""


@dataclass(frozen=True)
class WheelBundle:
    library: Path
    contract: Path
    cli_example: Path
    worker_example: Path
    worker_resources: Path


@dataclass(frozen=True)
class ScenarioSpec:
    name: str
    needs_broker: bool
    needs_worker: bool


@dataclass
class CommandRecord:
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
    commands: list[CommandRecord] = field(default_factory=list)
    evidence: dict[str, Any] = field(default_factory=dict)
    error: str | None = None


@dataclass
class PythonPassState:
    python_version: str
    tag: str
    cli_py: Path
    worker_py: Path
    cli_entry: Path
    worker_entry: Path
    scenario_records: list[ScenarioRecord] = field(default_factory=list)
    origins: dict[str, Any] | None = None
    runtime_version: str | None = None
    broker_log: str = ""
    error: str | None = None


LOCAL_SCENARIOS: tuple[str, ...] = (
    "local_probe_pid",
    "local_observe_streams",
    "local_bundle_positional",
    "local_input_json_file_stdin",
    "local_fail_handler",
    "local_invalid_before_submit",
    "local_interrupt_gate",
)

REMOTE_SCENARIOS: tuple[str, ...] = (
    "remote_bundle_submit_wait",
    "remote_observe_streams",
    "remote_gate_wait",
    "remote_no_wait_observe",
    "remote_wait_timeout_uncancelled",
    "remote_interrupt_uncancelled",
    "remote_fail_terminal",
    "remote_unavailable_broker",
    "remote_invalid_before_submit",
    "control_worker_readiness_timeout",
    "control_broken_worker_launch",
)


def _append_command(target: list[CommandRecord], cmd: CommandRecord) -> None:
    if cmd not in target:
        target.append(cmd)


def _copy_build_example(src: Path, work_dir: Path, folder: str) -> Path:
    dest = work_dir / folder
    shutil.copytree(
        src,
        dest,
        dirs_exist_ok=True,
        ignore=shutil.ignore_patterns(".venv", "__pycache__"),
    )
    strip_contract_sources(dest / "pyproject.toml")
    return dest


def build_example_wheels(work_dir: Path) -> WheelBundle:
    library, contract = build_wheels(work_dir)
    dist = work_dir / "dist"
    cli_build = _copy_build_example(CLI_EXAMPLE_SRC, work_dir, "cli_pkg")
    worker_build = _copy_build_example(WORKER_EXAMPLE_SRC, work_dir, "worker_pkg")
    resources_build = _copy_build_example(WORKER_RESOURCES_SRC, work_dir, "worker_resources_pkg")
    run_cmd(["uv", "build", "--project", str(cli_build), "--out-dir", str(dist)])
    run_cmd(["uv", "build", "--project", str(worker_build), "--out-dir", str(dist)])
    run_cmd(["uv", "build", "--project", str(resources_build), "--out-dir", str(dist)])
    wheels = sorted(dist.glob("*.whl"))
    cli_wheel = next((w for w in wheels if w.name.startswith("superjobs_contract_cli")), None)
    worker_wheel = next((w for w in wheels if w.name.startswith("superjobs_contract_worker")), None)
    resources_wheel = next(
        (w for w in wheels if w.name.startswith("superjobs_contract_worker_resources")),
        None,
    )
    if cli_wheel is None or worker_wheel is None or resources_wheel is None:
        raise CliProcessError(
            f"Missing example wheels in {dist}: {[w.name for w in wheels]}",
        )
    return WheelBundle(
        library=library,
        contract=contract,
        cli_example=cli_wheel,
        worker_example=worker_wheel,
        worker_resources=resources_wheel,
    )


def create_layout_venv(
    work_dir: Path,
    python: str,
    bundle: WheelBundle,
    *,
    role: str,
) -> Path:
    tag = f"venv-{role}-py{python.replace('.', '')}"
    venv_dir = work_dir / tag
    if venv_dir.exists():
        shutil.rmtree(venv_dir)
    run_cmd(["uv", "venv", "--python", python, str(venv_dir)])
    py = venv_python(venv_dir)
    assert_final_runtime_python(py)
    if role == "cli":
        install = [
            "uv",
            "pip",
            "install",
            "--python",
            str(py),
            f"{bundle.library}[cli]",
            str(bundle.contract),
            str(bundle.cli_example),
            "pytest>=9.1.1",
        ]
    elif role == "worker":
        install = [
            "uv",
            "pip",
            "install",
            "--python",
            str(py),
            str(bundle.library),
            str(bundle.contract),
            str(bundle.worker_resources),
            str(bundle.worker_example),
        ]
    else:
        raise CliProcessError(f"unknown layout role {role!r}")
    run_cmd(install, env={"PYTHONNOUSERSITE": "1"})
    return venv_dir


def console_script_path(venv_dir: Path, name: str) -> Path:
    if sys.platform == "win32":
        path = venv_dir / "Scripts" / f"{name}.exe"
    else:
        path = venv_dir / "bin" / name
    if not path.is_file():
        raise CliProcessError(f"missing console script {path}")
    return path


def probe_layout_origins(python: Path, *, role: str, evidence_dir: Path | None, python_version: str) -> dict[str, Any]:
    probe_dir = Path(tempfile.mkdtemp(prefix=f"superjobs-cli-origin-{role}-"))
    try:
        shutil.copy2(WHEEL_ORIGIN_PROBE, probe_dir / "wheel_origin_probe.py")
        shutil.copy2(ORIGIN_PROBE, probe_dir / "cli_origin_probe.py")
        completed = run_cmd(
            [
                str(python),
                str(probe_dir / "cli_origin_probe.py"),
                str(REPO_ROOT),
                "--role",
                role,
            ],
            cwd=probe_dir,
            env=isolated_child_env(),
        )
        evidence = json.loads(completed.stdout)
    finally:
        shutil.rmtree(probe_dir, ignore_errors=True)
    if evidence_dir is not None:
        evidence_dir.mkdir(parents=True, exist_ok=True)
        tag = runtime_evidence_tag(python_version)
        (evidence_dir / f"{tag}-{role}-origin.json").write_text(
            json.dumps(evidence, indent=2),
            encoding="utf-8",
        )
    return evidence


def _windows_hidden_console_popen_kwargs() -> dict[str, Any]:
    si = subprocess.STARTUPINFO()
    si.dwFlags |= subprocess.STARTF_USESHOWWINDOW
    si.wShowWindow = subprocess.SW_HIDE
    return {
        "creationflags": subprocess.CREATE_NEW_CONSOLE,
        "startupinfo": si,
    }


def _spawn_logged(
    *,
    name: str,
    command: list[str],
    cwd: Path,
    env: dict[str, str],
    log_dir: Path,
    interruptible: bool = False,
    commands: list[CommandRecord] | None = None,
    stdin: int | None = None,
) -> tuple[subprocess.Popen[str], CommandRecord, TextIO, TextIO]:
    log_dir.mkdir(parents=True, exist_ok=True)
    stdout_path = log_dir / f"{name}.stdout.log"
    stderr_path = log_dir / f"{name}.stderr.log"
    stdout_io = stdout_path.open("w", encoding="utf-8")
    stderr_io = stderr_path.open("w", encoding="utf-8")
    record = CommandRecord(
        name=name,
        command=command,
        cwd=cwd,
        started_at=time.monotonic(),
        stdout_path=stdout_path,
        stderr_path=stderr_path,
    )
    if commands is not None:
        _append_command(commands, record)
    popen_kwargs: dict[str, Any] = {}
    if interruptible and sys.platform == "win32":
        popen_kwargs.update(_windows_hidden_console_popen_kwargs())
        command = [
            str(sys.executable), "-I", str(SUPPORT / "win_console_entry.py"), *command,
        ]
        record.command = command
    elif not interruptible:
        popen_kwargs["creationflags"] = subprocess_creationflags()
    try:
        process = subprocess.Popen(
            command,
            cwd=cwd,
            env=env,
            stdout=stdout_io,
            stderr=stderr_io,
            text=True,
            stdin=stdin,
            start_new_session=interruptible and sys.platform != "win32",
            **popen_kwargs,
        )
    except BaseException:
        record.ended_at = time.monotonic()
        stdout_io.close()
        stderr_io.close()
        raise
    record.pid = process.pid
    owner = current_child_owner()
    if owner is not None:
        owner.register(
            OwnedChild(
                label=name,
                process=process,
                stdout_io=stdout_io,
                stderr_io=stderr_io,
                record=record,
            ),
        )
    return process, record, stdout_io, stderr_io


def _close_streams(*streams: TextIO | None) -> None:
    for stream in streams:
        if stream is None:
            continue
        try:
            stream.close()
        except OSError:
            pass


def _reap_process(
    process: subprocess.Popen[str],
    *,
    label: str,
    deadline: float,
    expected_code: int | None = None,
    owner: ChildOwner | None = None,
) -> int:
    forced = False
    wait_deadline = deadline - KILL_REAP_SECONDS
    while time.monotonic() < wait_deadline:
        code = process.poll()
        if code is not None:
            if expected_code is not None and not _exit_code_matches(code, expected_code):
                raise CliProcessError(f"{label} exit {code} != expected {expected_code}")
            return code
        time.sleep(min(POLL_INTERVAL_SECONDS, max(0.0, wait_deadline - time.monotonic())))
    if process.poll() is None:
        forced = True
        active_owner = owner or current_child_owner()
        if active_owner is not None:
            owned = active_owner.owned_for_process(process)
            if owned is None:
                owned = OwnedChild(label=label, process=process)
                active_owner.register(owned)
            active_owner.force_kill(owned, reason="reap timeout")
        else:
            kill_process_tree(process.pid or 0)
    while time.monotonic() < deadline:
        try:
            process.wait(timeout=max(0.0, deadline - time.monotonic()))
            break
        except subprocess.TimeoutExpired:
            if process.poll() is not None:
                break
    code = process.poll()
    if code is None:
        raise CliProcessError(f"{label} did not exit before deadline")
    if forced:
        raise CliProcessError(f"{label} required forced process-tree kill")
    if expected_code is not None and not _exit_code_matches(code, expected_code):
        raise CliProcessError(f"{label} exit {code} != expected {expected_code}")
    return code


def send_process_interrupt(process: subprocess.Popen[str]) -> None:
    if process.poll() is not None:
        return
    if sys.platform == "win32":
        completed = subprocess.run(
            [str(sys.executable), str(WIN_CTRL_C), str(process.pid)],
            capture_output=True,
            text=True,
            timeout=INTERRUPT_DEADLINE,
            creationflags=subprocess_creationflags(),
        )
        if completed.returncode != 0:
            raise CliProcessError(
                f"win_ctrl_c failed: {completed.stderr.strip() or completed.stdout}",
            )
    else:
        os.kill(process.pid, signal.SIGINT)


def _run_cli(
    *,
    entry: Path,
    args: list[str],
    env: dict[str, str],
    log_dir: Path,
    name: str,
    cwd: Path | None = None,
    deadline: float | None = None,
    expected_code: int | None = None,
    interruptible: bool = False,
    commands: list[CommandRecord] | None = None,
    input_text: str | None = None,
) -> CommandRecord:
    command = [str(entry), *args]
    process, record, stdout_io, stderr_io = _spawn_logged(
        name=name,
        command=command,
        cwd=cwd or entry.parent,
        env=env,
        log_dir=log_dir,
        interruptible=interruptible,
        commands=commands,
        stdin=subprocess.PIPE if input_text is not None else None,
    )
    owner = current_child_owner()
    if owner is not None:
        owned = owner.owned_for_process(process)
        if owned is not None and expected_code is not None:
            owned.expected_exit = expected_code
    end = time.monotonic() + (deadline or CHILD_TIMEOUT_SECONDS)
    code: int | None = None
    try:
        if input_text is not None:
            process.communicate(input=input_text, timeout=max(0.0, end - time.monotonic()))
        code = _reap_process(
            process,
            label=name,
            deadline=end,
            expected_code=expected_code,
            owner=owner,
        )
    finally:
        if code is None and process.poll() is not None:
            code = process.poll()
        if code is not None:
            record.exit_code = code
        record.ended_at = time.monotonic()
        if owner is not None:
            owned = owner.owned_for_process(process)
            if owned is not None:
                owned.finalize_record()
        _close_streams(stdout_io, stderr_io)
    if code is None:
        raise CliProcessError(f"{name} did not produce an exit code")
    return record


def _parse_execution_reference(stderr: str) -> dict[str, Any] | None:
    for line in stderr.splitlines():
        match = EXECUTION_REF_RE.search(line)
        if match:
            return json.loads(match.group(1))
    return None


def _parse_stdout_json(stdout: str) -> Any:
    text = stdout.strip()
    if not text:
        raise CliProcessError("expected JSON stdout")
    return json.loads(text)


def _validate_gate_reference(reference: dict[str, Any]) -> str:
    if reference.get("job_name") != "examples.contract.cli.gate" or reference.get("job_version") != "v1":
        raise CliProcessError("execution reference has wrong contract identity")
    execution_id = reference.get("job_id")
    if not isinstance(execution_id, str) or not execution_id:
        raise CliProcessError("execution reference has no execution identity")
    return execution_id


def _scenario_env(
    state_dir: Path,
    *,
    sync_dir: Path | None = None,
    nats_url: str | None = None,
    run_id: str | None = None,
    extra: dict[str, str] | None = None,
) -> dict[str, str]:
    sync_root = sync_dir or state_dir
    payload = {
        "SUPERJOBS_CLI_STATE_DIR": str(state_dir),
        "SUPERJOBS_CLI_SYNC_DIR": str(sync_root),
        "SUPERJOBS_CLI_EMIT_PID": "1",
        "SUPERJOBS_CLI_EMIT_FACTORY": "1",
    }
    if run_id is not None:
        payload["SUPERJOBS_CLI_RUN_ID"] = run_id
    if nats_url is not None:
        payload["SUPERJOBS_NATS_URL"] = nats_url
    if extra:
        payload.update(extra)
    return isolated_child_env(payload)


def _start_worker(
    *,
    worker_entry: Path,
    run_id: str,
    state_dir: Path,
    nats_url: str,
    log_dir: Path,
    sync_dir: Path,
) -> tuple[subprocess.Popen[str], CommandRecord, TextIO, TextIO]:
    env = _scenario_env(state_dir, sync_dir=sync_dir, nats_url=nats_url, run_id=run_id)
    process, record, stdout_io, stderr_io = _spawn_logged(
        name="worker",
        command=[str(worker_entry)],
        cwd=worker_entry.parent,
        env=env,
        log_dir=log_dir,
    )
    owner = current_child_owner()
    if owner is not None:
        owned = owner.owned_for_process(process)
        if owned is not None:
            owned.expected_exit = 0
    try:
        wait_for_worker_ready(
            state_dir,
            run_id=run_id,
            deadline=WORKER_STARTUP_DEADLINE,
            child_process=process,
        )
    except BaseException:
        if process.poll() is None:
            kill_process_tree(process.pid or 0)
            deadline = time.monotonic() + KILL_REAP_SECONDS
            while time.monotonic() < deadline and process.poll() is None:
                time.sleep(POLL_INTERVAL_SECONDS)
            if owner is not None:
                owned = owner.owned_for_process(process)
                if owned is not None:
                    owned.finalize_record()
                    owned.close_streams()
        raise
    return process, record, stdout_io, stderr_io


def _stop_worker(process: subprocess.Popen[str], state_dir: Path, run_id: str, record: CommandRecord) -> None:
    write_worker_stop(state_dir, run_id)
    deadline = time.monotonic() + CHILD_TIMEOUT_SECONDS
    code = _reap_process(process, label="worker", deadline=deadline, expected_code=0)
    record.exit_code = code
    record.ended_at = time.monotonic()


def _prepare_observer_bundle(work_parent: Path) -> Path:
    bundle_dir = Path(tempfile.mkdtemp(prefix="superjobs-cli-observe-", dir=work_parent))
    shutil.copy2(OBSERVE_SCRIPT, bundle_dir / "observe_execution.py")
    runner = bundle_dir / "run_observe.py"
    runner.write_text(
        "import runpy\nrunpy.run_path('observe_execution.py', run_name='__main__')\n",
        encoding="utf-8",
    )
    return bundle_dir


def _observe_execution(
    cli_py: Path,
    execution_id: str,
    *,
    gate: str,
    nats_url: str,
    log_dir: Path,
    observer_bundle: Path,
    commands: list[CommandRecord] | None = None,
) -> CommandRecord:
    env = isolated_child_env(
        {
            "SUPERJOBS_CLI_EXECUTION_ID": execution_id,
            "SUPERJOBS_CLI_GATE": gate,
            "SUPERJOBS_NATS_URL": nats_url,
        },
    )
    command = [str(cli_py), "-I", str(observer_bundle / "run_observe.py")]
    process, record, stdout_io, stderr_io = _spawn_logged(
        name="observe",
        command=command,
        cwd=observer_bundle,
        env=env,
        log_dir=log_dir,
        commands=commands,
    )
    deadline = time.monotonic() + CHILD_TIMEOUT_SECONDS
    try:
        code = _reap_process(
            process,
            label="observe",
            deadline=deadline,
            expected_code=0,
            owner=current_child_owner(),
        )
        record.exit_code = code
        record.ended_at = time.monotonic()
    finally:
        _close_streams(stdout_io, stderr_io)
    evidence = _parse_stdout_json(record.stream_text()[0])
    if evidence.get("execution_id") != execution_id or evidence.get("gate") != gate:
        raise CliProcessError("fresh observer recovered the wrong execution")
    if evidence.get("cancellation_requested") is not False:
        raise CliProcessError("fresh observer found a cancellation request")
    return record


def _run_local_scenarios(
    *,
    cli_entry: Path,
    state_root: Path,
    log_root: Path,
    on_record: Callable[[ScenarioRecord], None] | None = None,
) -> list[ScenarioRecord]:
    records: list[ScenarioRecord] = []
    with ChildOwner():
        for name in LOCAL_SCENARIOS:
            run_id = uuid.uuid4().hex
            state_dir = state_root / name
            state_dir.mkdir(parents=True, exist_ok=True)
            log_dir = log_root / name
            record = ScenarioRecord(name=name, run_id=run_id, state_dir=state_dir, started_at=time.monotonic())
            commands_done = False
            try:
                if name == "local_probe_pid":
                    cmd = _run_cli(
                        entry=cli_entry,
                        args=["run", "probe-local", "pid-check"],
                        env=_scenario_env(state_dir),
                        log_dir=log_dir,
                        name="cli",
                        expected_code=EXIT_SUCCESS,
                        commands=record.commands,
                    )
                    stdout, stderr = cmd.stream_text()
                    cli_pid = int(
                        next(line for line in stderr.splitlines() if line.startswith("cli-pid:")).split(":", 1)[1],
                    )
                    result = _parse_stdout_json(stdout)
                    if result.get("pid") != cli_pid:
                        raise CliProcessError("handler PID != CLI PID")
                    record.evidence["cli_pid"] = cli_pid

                elif name == "local_observe_streams":
                    cmd = _run_cli(
                        entry=cli_entry,
                        args=["run", "observe-local", "--device-id", "dev-1"],
                        env=_scenario_env(state_dir, run_id=run_id),
                        log_dir=log_dir,
                        name="cli",
                        expected_code=EXIT_SUCCESS,
                        commands=record.commands,
                    )
                    stdout, stderr = cmd.stream_text()
                    result = _parse_stdout_json(stdout)
                    if result.get("revision") != "dev-1":
                        raise CliProcessError("stdout result mismatch for observe-local")
                    envelopes = parse_observation_lines(stderr)
                    assert_manifest_with_events_stream(
                        envelopes,
                        job_id=envelopes[0]["job_id"],
                        device_id="dev-1",
                    )
                    record.evidence["observations"] = len(envelopes)

                elif name == "local_bundle_positional":
                    cmd = _run_cli(
                        entry=cli_entry,
                        args=["run", "bundle", "pos-bundle-1"],
                        env=_scenario_env(state_dir),
                        log_dir=log_dir,
                        name="cli",
                        expected_code=EXIT_SUCCESS,
                        commands=record.commands,
                    )
                    stdout, _ = cmd.stream_text()
                    payload = _parse_stdout_json(stdout)
                    if payload != {"accepted": True}:
                        raise CliProcessError(f"unexpected bundle result {payload!r}")

                elif name == "local_input_json_file_stdin":
                    json_cmd = _run_cli(
                        entry=cli_entry,
                        args=["run", "observe-local", "--json", '{"device_id":"json-dev"}'],
                        env=_scenario_env(state_dir / "json"),
                        log_dir=log_dir / "json",
                        name="cli-json",
                        expected_code=EXIT_SUCCESS,
                        commands=record.commands,
                    )
                    file_dir = state_dir / "file"
                    file_dir.mkdir(parents=True, exist_ok=True)
                    input_file = file_dir / "request.json"
                    input_file.write_text('{"device_id":"file-dev"}', encoding="utf-8")
                    file_cmd = _run_cli(
                        entry=cli_entry,
                        args=["run", "observe-local", "--input", str(input_file)],
                        env=_scenario_env(file_dir),
                        log_dir=log_dir / "file",
                        name="cli-file",
                        expected_code=EXIT_SUCCESS,
                        commands=record.commands,
                    )
                    stdin_dir = state_dir / "stdin"
                    stdin_dir.mkdir(parents=True, exist_ok=True)
                    stdin_process, stdin_cmd, stdin_out, stdin_err = _spawn_logged(
                        name="cli-stdin",
                        command=[str(cli_entry), "run", "observe-local", "--input", "-"],
                        env=_scenario_env(stdin_dir),
                        cwd=cli_entry.parent,
                        log_dir=log_dir / "stdin",
                        commands=record.commands,
                        stdin=subprocess.PIPE,
                    )
                    try:
                        stdin_process.communicate(input='{"device_id":"stdin-dev"}', timeout=CHILD_TIMEOUT_SECONDS)
                        stdin_cmd.exit_code = _reap_process(
                            stdin_process, label="cli-stdin", expected_code=EXIT_SUCCESS,
                            deadline=time.monotonic() + KILL_REAP_SECONDS,
                            owner=current_child_owner(),
                        )
                        stdin_cmd.ended_at = time.monotonic()
                    finally:
                        _close_streams(stdin_out, stdin_err)
                    record.evidence["modes"] = {
                        "json": json.loads(json_cmd.stream_text()[0]),
                        "file": json.loads(file_cmd.stream_text()[0]),
                        "stdin": json.loads(stdin_cmd.stream_text()[0]),
                    }
                    for mode, device in (("json", "json-dev"), ("file", "file-dev"), ("stdin", "stdin-dev")):
                        if record.evidence["modes"][mode] != {"revision": device}:
                            raise CliProcessError(f"{mode} input returned the wrong result")
                    commands_done = True

                elif name == "local_fail_handler":
                    cmd = _run_cli(
                        entry=cli_entry,
                        args=["run", "fail-local", "--reason", "boom"],
                        env=_scenario_env(state_dir),
                        log_dir=log_dir,
                        name="cli",
                        expected_code=EXIT_RUNTIME_FAILURE,
                        commands=record.commands,
                    )
                    stdout, stderr = cmd.stream_text()
                    if stdout.strip():
                        raise CliProcessError("failure path must not write stdout")
                    if "boom" not in stderr:
                        raise CliProcessError("expected failure diagnostics on stderr")

                elif name == "local_invalid_before_submit":
                    if list(state_dir.glob("checkpoint_handler_entered*.json")):
                        raise CliProcessError("handler checkpoint must not exist before submit")
                    assert_checkpoint_absent(state_dir, "runtime_factory_entered")
                    cmd = _run_cli(
                        entry=cli_entry,
                        args=["run", "gate-local", "--json", '{"gate": 1}'],
                        env=_scenario_env(state_dir, run_id=run_id),
                        log_dir=log_dir,
                        name="cli",
                        expected_code=EXIT_USAGE,
                        commands=record.commands,
                    )
                    if list(state_dir.glob("checkpoint_handler_entered*.json")):
                        raise CliProcessError("handler checkpoint must not exist after validation failure")
                    assert_checkpoint_absent(state_dir, "runtime_factory_entered")
                    record.evidence["stderr"] = cmd.stream_text()[1]

                elif name == "local_interrupt_gate":
                    gate = "local-hold"
                    env = _scenario_env(state_dir, run_id=run_id)
                    process, cmd, stdout_io, stderr_io = _spawn_logged(
                        name="cli",
                        command=[
                            str(cli_entry),
                            "run",
                            "gate-local",
                            "--gate",
                            gate,
                        ],
                        cwd=cli_entry.parent,
                        env=env,
                        log_dir=log_dir,
                        interruptible=True,
                    )
                    _append_command(record.commands, cmd)
                    try:
                        entered = wait_for_checkpoint(
                            state_dir,
                            "handler_entered",
                            deadline=CHECKPOINT_DEADLINE,
                            gate=gate,
                            run_id=run_id,
                        )
                        send_process_interrupt(process)
                        code = _reap_process(
                            process,
                            label="cli-interrupt",
                            deadline=time.monotonic() + INTERRUPT_DEADLINE,
                            expected_code=EXIT_INTERRUPTED,
                            owner=current_child_owner(),
                        )
                        cmd.exit_code = code
                        cmd.ended_at = time.monotonic()
                        write_gate_release(state_dir, gate, run_id=run_id)
                        if not checkpoint_path(state_dir, "handler_entered", gate=gate).is_file():
                            raise CliProcessError("interrupt cleanup missing handler_entered marker")
                        closed = wait_for_checkpoint(state_dir, "runtime_factory_closed", deadline=1)
                        if closed.get("mode") != "local":
                            raise CliProcessError("local factory did not finish cleanup")
                        if cmd.stream_text()[0].strip():
                            raise CliProcessError("interrupted local command wrote result stdout")
                        record.evidence["execution_id"] = entered.get("execution_id")
                        record.evidence["interrupt_exit"] = code
                    finally:
                        _close_streams(stdout_io, stderr_io)
                    commands_done = True

                else:
                    raise CliProcessError(f"unknown local scenario {name!r}")

                if not commands_done:
                    _append_command(record.commands, cmd)
            except BaseException as exc:
                record.error = str(exc)
                raise
            finally:
                record.ended_at = time.monotonic()
                records.append(record)
                if on_record is not None:
                    on_record(record)
    return records


def _run_remote_scenarios(
    *,
    cli_entry: Path,
    cli_py: Path,
    worker_entry: Path,
    nats_url: str,
    state_root: Path,
    log_root: Path,
    sync_root: Path,
    observer_bundle: Path,
    on_record: Callable[[ScenarioRecord], None] | None = None,
) -> list[ScenarioRecord]:
    records: list[ScenarioRecord] = []
    worker_process: subprocess.Popen[str] | None = None
    worker_record: CommandRecord | None = None
    worker_streams: tuple[TextIO, TextIO] | None = None
    worker_run_id = uuid.uuid4().hex
    worker_state = state_root / "_worker"
    worker_state.mkdir(parents=True, exist_ok=True)
    sync_root.mkdir(parents=True, exist_ok=True)
    worker_log = log_root / "_worker"

    def ensure_worker(record: ScenarioRecord | None = None) -> None:
        nonlocal worker_process, worker_record, worker_streams
        if worker_process is not None:
            if worker_process.poll() is not None:
                code = worker_process.poll()
                raise CliProcessError(f"worker exited {code} before scenario end")
            return
        worker_process, worker_record, out_io, err_io = _start_worker(
            worker_entry=worker_entry,
            run_id=worker_run_id,
            state_dir=worker_state,
            nats_url=nats_url,
            log_dir=worker_log,
            sync_dir=sync_root,
        )
        worker_streams = (out_io, err_io)
        active_owner = current_child_owner()
        if active_owner is not None:
            child = active_owner.owned_for_process(worker_process)
            if child is not None:
                child.expected_exit = 0
        if record is not None:
            record.evidence["worker"] = {
                "pid": worker_process.pid,
                "run_id": worker_run_id,
                "expected_exit": 0,
                "ready": True,
            }

    with ChildOwner() as owner:
        try:
            for name in REMOTE_SCENARIOS:
                run_id = uuid.uuid4().hex
                state_dir = state_root / name
                state_dir.mkdir(parents=True, exist_ok=True)
                log_dir = log_root / name
                record = ScenarioRecord(name=name, run_id=run_id, state_dir=state_dir, started_at=time.monotonic())
                try:
                    if name == "control_broken_worker_launch":
                        broken = worker_entry.parent / "missing-worker.exe"
                        cmd_rec: CommandRecord | None = None
                        with ChildOwner() as owner_launch:
                            try:
                                proc, cmd_rec, out_io, err_io = _spawn_logged(
                                    name="broken-worker",
                                    command=[str(broken)],
                                    cwd=broken.parent,
                                    env=_scenario_env(
                                        worker_state,
                                        sync_dir=sync_root,
                                        nats_url=nats_url,
                                        run_id=worker_run_id,
                                    ),
                                    log_dir=worker_log / "broken",
                                )
                                _append_command(record.commands, cmd_rec)
                                child = owner_launch.owned_for_process(proc)
                                if child is None:
                                    raise CliProcessError("broken worker missing ownership")
                                reap_with_owner(
                                    child,
                                    owner=owner_launch,
                                    deadline=time.monotonic() + 5.0,
                                )
                                raise CliProcessError("broken worker launch should not run")
                            except OSError:
                                record.evidence["broken_launch"] = "spawn_failed"
                            except (CliProcessError, ProtocolError) as exc:
                                if "broken worker launch should not run" in str(exc):
                                    raise
                                record.evidence["broken_launch"] = str(exc)
                        continue

                    if name == "control_worker_readiness_timeout":
                        stall_state = state_dir / "stall-worker-state"
                        stall_state.mkdir(parents=True, exist_ok=True)
                        stall_run_id = uuid.uuid4().hex
                        with ChildOwner(allow_forced_cleanup=True) as stall_owner:
                            proc, cmd_rec, out_io, err_io = _spawn_logged(
                                name="stall-worker",
                                command=[sys.executable, "-c", "import time; time.sleep(3600)"],
                                cwd=stall_state,
                                env=_scenario_env(
                                    stall_state,
                                    sync_dir=sync_root,
                                    nats_url=nats_url,
                                    run_id=stall_run_id,
                                ),
                                log_dir=log_dir,
                                commands=record.commands,
                            )
                            try:
                                wait_for_worker_ready(
                                    stall_state,
                                    run_id=stall_run_id,
                                    deadline=2.0,
                                    child_process=proc,
                                )
                                raise CliProcessError("stall worker should not become ready")
                            except TimeoutError as exc:
                                record.evidence["readiness_failure"] = str(exc)
                                record.evidence["stall_worker_pid"] = proc.pid
                            finally:
                                _close_streams(out_io, err_io)
                        if proc.poll() is None or stall_owner.cleanup_errors:
                            raise CliProcessError("readiness control failed to reap its child")
                        record.evidence["expected_forced_cleanup"] = stall_owner.forced_kills
                        continue

                    if name == "remote_unavailable_broker":
                        cmd = _run_cli(
                            entry=cli_entry,
                            args=["submit", "gate-remote", "--gate", "down", "--wait"],
                            env=_scenario_env(
                                state_dir,
                                sync_dir=sync_root,
                                nats_url="nats://127.0.0.1:1",
                            ),
                            log_dir=log_dir,
                            name="cli",
                            expected_code=EXIT_RUNTIME_FAILURE,
                            deadline=90.0,
                            commands=record.commands,
                        )
                        continue

                    ensure_worker(record)

                    if name == "remote_bundle_submit_wait":
                        cmd = _run_cli(
                            entry=cli_entry,
                            args=["submit", "bundle", "remote-bundle", "--wait"],
                            env=_scenario_env(state_dir, sync_dir=sync_root, nats_url=nats_url),
                            log_dir=log_dir,
                            name="cli",
                            expected_code=EXIT_SUCCESS,
                            commands=record.commands,
                        )
                        stdout, _ = cmd.stream_text()
                        if _parse_stdout_json(stdout) != {"accepted": True}:
                            raise CliProcessError("remote bundle wait result mismatch")

                    elif name == "remote_observe_streams":
                        payload = '{"device_id":"remote-dev"}'
                        request_file = state_dir / "request.json"
                        request_file.write_text(payload, encoding="utf-8")
                        inputs = (
                            ("fields", ["--device-id", "remote-dev"], None),
                            ("json", ["--json", payload], None),
                            ("file", ["--input", str(request_file)], None),
                            ("stdin", ["--input", "-"], payload),
                        )
                        record.evidence["input_modes"] = {}
                        for mode, input_args, input_text in inputs:
                            cmd = _run_cli(
                                entry=cli_entry,
                                args=["submit", "observe-local", *input_args, "--wait"],
                                env=_scenario_env(state_dir, sync_dir=sync_root, nats_url=nats_url),
                                log_dir=log_dir, name=f"cli-{mode}", expected_code=EXIT_SUCCESS,
                                commands=record.commands, input_text=input_text,
                            )
                            stdout, stderr = cmd.stream_text()
                            if _parse_stdout_json(stdout) != {"revision": "remote-dev-r1"}:
                                raise CliProcessError(f"remote {mode} result mismatch")
                            envelopes = parse_observation_lines(stderr)
                            if not envelopes:
                                raise CliProcessError("missing remote observations")
                            assert_manifest_with_events_stream(
                                envelopes, job_id=envelopes[0]["job_id"], device_id="remote-dev",
                            )
                            cli_pid = int(next(line for line in stderr.splitlines() if line.startswith("cli-pid:")).split(":", 1)[1])
                            worker_pid = envelopes[1]["observation"]["extra"].get("worker_pid")
                            ready = json.loads((worker_state / "worker_ready.json").read_text(encoding="utf-8"))
                            if worker_pid != ready.get("pid") or worker_pid == cli_pid:
                                raise CliProcessError("remote handler did not run in the separate worker")
                            record.evidence["input_modes"][mode] = dict(cli_pid=cli_pid, worker_pid=worker_pid, observations=envelopes)

                    elif name == "remote_gate_wait":
                        gate = "wait-gate"
                        env = _scenario_env(state_dir, sync_dir=sync_root, nats_url=nats_url)
                        process, cmd, stdout_io, stderr_io = _spawn_logged(
                            name="cli",
                            command=[
                                str(cli_entry),
                                "submit",
                                "gate-remote",
                                "--gate",
                                gate,
                                "--wait",
                            ],
                            cwd=cli_entry.parent,
                            env=env,
                            log_dir=log_dir,
                        )
                        _append_command(record.commands, cmd)
                        try:
                            wait_for_checkpoint(
                                sync_root,
                                "handler_entered",
                                deadline=CHECKPOINT_DEADLINE,
                                gate=gate,
                            )
                            write_gate_release(sync_root, gate, run_id=run_id)
                            code = _reap_process(
                                process,
                                label="cli",
                                deadline=time.monotonic() + CHILD_TIMEOUT_SECONDS,
                                expected_code=EXIT_SUCCESS,
                                owner=owner,
                            )
                            cmd.exit_code = code
                            cmd.ended_at = time.monotonic()
                            stdout, _ = cmd.stream_text()
                            if _parse_stdout_json(stdout) != {"gate": gate}:
                                raise CliProcessError("gate wait result mismatch")
                        finally:
                            _close_streams(stdout_io, stderr_io)

                    elif name == "remote_no_wait_observe":
                        gate = "observe-gate"
                        cmd = _run_cli(
                            entry=cli_entry,
                            args=["submit", "gate-remote", "--gate", gate],
                            env=_scenario_env(state_dir, sync_dir=sync_root, nats_url=nats_url),
                            log_dir=log_dir,
                            name="cli",
                            expected_code=EXIT_SUCCESS,
                            commands=record.commands,
                        )
                        stdout, stderr = cmd.stream_text()
                        if not stdout.strip():
                            raise CliProcessError("no-wait submit must print execution reference on stdout")
                        ref = _parse_stdout_json(stdout)
                        execution_id = _validate_gate_reference(ref)
                        entered = wait_for_checkpoint(
                            sync_root,
                            "handler_entered",
                            deadline=CHECKPOINT_DEADLINE,
                            gate=gate,
                            expected_execution_id=execution_id,
                        )
                        record.evidence["worker_checkpoint"] = entered
                        write_gate_release(sync_root, gate, run_id=run_id)
                        observe = _observe_execution(
                            cli_py,
                            execution_id,
                            gate=gate,
                            nats_url=nats_url,
                            log_dir=log_dir,
                            observer_bundle=observer_bundle,
                            commands=record.commands,
                        )
                        _append_command(record.commands, observe)
                        record.evidence["execution_id"] = execution_id

                    elif name == "remote_wait_timeout_uncancelled":
                        gate = "timeout-gate"
                        cmd = _run_cli(
                            entry=cli_entry,
                            args=[
                                "submit",
                                "gate-remote",
                                "--gate",
                                gate,
                                "--wait",
                                "--wait-timeout",
                                "1",
                            ],
                            env=_scenario_env(state_dir, sync_dir=sync_root, nats_url=nats_url),
                            log_dir=log_dir,
                            name="cli",
                            expected_code=EXIT_RUNTIME_FAILURE,
                            deadline=90.0,
                            commands=record.commands,
                        )
                        _, stderr = cmd.stream_text()
                        ref = _parse_execution_reference(stderr)
                        if ref is None:
                            raise CliProcessError("timeout must preserve execution reference")
                        execution_id = _validate_gate_reference(ref)
                        write_gate_release(sync_root, gate, run_id=run_id)
                        observe = _observe_execution(
                            cli_py,
                            execution_id,
                            gate=gate,
                            nats_url=nats_url,
                            log_dir=log_dir,
                            observer_bundle=observer_bundle,
                            commands=record.commands,
                        )
                        _append_command(record.commands, observe)
                        record.evidence["execution_id"] = execution_id

                    elif name == "remote_interrupt_uncancelled":
                        gate = "interrupt-gate"
                        env = _scenario_env(state_dir, sync_dir=sync_root, nats_url=nats_url)
                        process, cmd, stdout_io, stderr_io = _spawn_logged(
                            name="cli",
                            command=[
                                str(cli_entry),
                                "submit",
                                "gate-remote",
                                "--gate",
                                gate,
                                "--wait",
                            ],
                            cwd=cli_entry.parent,
                            env=env,
                            log_dir=log_dir,
                            interruptible=True,
                        )
                        _append_command(record.commands, cmd)
                        try:
                            entered = wait_for_checkpoint(
                                sync_root,
                                "handler_entered",
                                deadline=CHECKPOINT_DEADLINE,
                                gate=gate,
                            )
                            execution_id = entered.get("execution_id")
                            if not isinstance(execution_id, str):
                                raise CliProcessError("handler_entered missing execution_id")
                            send_process_interrupt(process)
                            code = _reap_process(
                                process,
                                label="cli-interrupt",
                                deadline=time.monotonic() + INTERRUPT_DEADLINE,
                                expected_code=EXIT_INTERRUPTED,
                                owner=owner,
                            )
                            cmd.exit_code = code
                            cmd.ended_at = time.monotonic()
                            _, stderr = cmd.stream_text()
                            ref = _parse_execution_reference(stderr)
                            if ref is None or ref.get("job_id") != execution_id:
                                raise CliProcessError("interrupt must preserve execution reference")
                            _validate_gate_reference(ref)
                            if cmd.stream_text()[0].strip():
                                raise CliProcessError("interrupted remote command wrote result stdout")
                            write_gate_release(sync_root, gate, run_id=run_id)
                            observe = _observe_execution(
                                cli_py,
                                execution_id,
                                gate=gate,
                                nats_url=nats_url,
                                log_dir=log_dir,
                                observer_bundle=observer_bundle,
                                commands=record.commands,
                            )
                            _append_command(record.commands, observe)
                            record.evidence["execution_id"] = execution_id
                        finally:
                            _close_streams(stdout_io, stderr_io)

                    elif name == "remote_invalid_before_submit":
                        isolated_sync = state_dir / "isolated-sync"
                        isolated_sync.mkdir(parents=True, exist_ok=True)
                        assert_checkpoint_absent(isolated_sync, "runtime_factory_entered")
                        cmd = _run_cli(
                            entry=cli_entry,
                            args=["submit", "gate-remote", "--json", '{"gate": 1}', "--wait"],
                            env=_scenario_env(state_dir, sync_dir=isolated_sync, nats_url=nats_url),
                            log_dir=log_dir,
                            name="cli",
                            expected_code=EXIT_USAGE,
                            commands=record.commands,
                        )
                        assert_checkpoint_absent(isolated_sync, "runtime_factory_entered")
                        continue

                    elif name == "remote_fail_terminal":
                        cmd = _run_cli(
                            entry=cli_entry,
                            args=["submit", "fail-remote", "--reason", "remote-boom", "--wait"],
                            env=_scenario_env(state_dir, sync_dir=sync_root, nats_url=nats_url),
                            log_dir=log_dir,
                            name="cli",
                            expected_code=EXIT_RUNTIME_FAILURE,
                            commands=record.commands,
                        )
                        _, stderr = cmd.stream_text()
                        if "remote-boom" not in stderr:
                            raise CliProcessError("expected terminal failure diagnostics")

                    else:
                        raise CliProcessError(f"unknown remote scenario {name!r}")
                except BaseException as exc:
                    record.error = str(exc)
                    raise
                finally:
                    record.ended_at = time.monotonic()
                    records.append(record)
                    if on_record is not None:
                        on_record(record)
        finally:
            shutdown_error: BaseException | None = None
            if worker_process is not None and worker_record is not None:
                try:
                    if worker_process.poll() is None:
                        write_worker_stop(worker_state, worker_run_id)
                        code = _reap_process(
                            worker_process,
                            label="worker",
                            deadline=time.monotonic() + CHILD_TIMEOUT_SECONDS,
                            expected_code=0,
                            owner=owner,
                        )
                        worker_record.exit_code = code
                        worker_record.ended_at = time.monotonic()
                    elif worker_process.poll() != 0:
                        raise CliProcessError(
                            f"worker exited {worker_process.poll()} before graceful shutdown",
                        )
                except BaseException as exc:
                    shutdown_error = exc
                finally:
                    if worker_streams is not None:
                        _close_streams(*worker_streams)
            if shutdown_error is not None:
                raise shutdown_error
        owner.assert_no_forced_kills()
    return records


def _scenario_to_json(record: ScenarioRecord) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "name": record.name,
        "run_id": record.run_id,
        "state_dir": str(record.state_dir),
        "started_at": record.started_at,
        "ended_at": record.ended_at,
        "error": record.error,
        "evidence": record.evidence,
        "commands": [],
    }
    for cmd in record.commands:
        stdout, stderr = cmd.stream_text()
        payload["commands"].append(
            {
                "name": cmd.name,
                "command": cmd.command,
                "pid": cmd.pid,
                "cwd": str(cmd.cwd),
                "started_at": cmd.started_at,
                "ended_at": cmd.ended_at,
                "exit_code": cmd.exit_code,
                "stdout": stdout,
                "stderr": stderr,
            },
        )
    return payload


def _write_evidence(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def _pass_payload(state: PythonPassState) -> dict[str, Any]:
    return {
        "python_version": state.python_version,
        "runtime_version": state.runtime_version,
        "origins": state.origins,
        "broker_log": state.broker_log,
        "error": state.error,
        "scenarios": [_scenario_to_json(item) for item in state.scenario_records],
    }


def verify_python_version(
    *,
    python_version: str,
    work_dir: Path,
    bundle: WheelBundle,
    artifact_dir: Path,
) -> PythonPassState:
    work_dir.mkdir(parents=True, exist_ok=True)
    tag = runtime_evidence_tag(python_version)
    evidence_dir = artifact_dir / tag
    evidence_dir.mkdir(parents=True, exist_ok=True)
    state = PythonPassState(
        python_version=python_version,
        tag=tag,
        cli_py=Path(),
        worker_py=Path(),
        cli_entry=Path(),
        worker_entry=Path(),
    )
    owner: OwnedNatsServer | None = None
    try:
        cli_venv = create_layout_venv(work_dir, python_version, bundle, role="cli")
        worker_venv = create_layout_venv(work_dir, python_version, bundle, role="worker")
        state.cli_py = venv_python(cli_venv)
        state.worker_py = venv_python(worker_venv)
        state.runtime_version = assert_final_runtime_python(state.cli_py)
        assert_final_runtime_python(state.worker_py)
        state.cli_entry = console_script_path(cli_venv, CLI_ENTRY)
        state.worker_entry = console_script_path(worker_venv, WORKER_ENTRY)
        state.origins = {
            "cli": probe_layout_origins(
                state.cli_py,
                role="cli",
                evidence_dir=evidence_dir,
                python_version=python_version,
            ),
            "worker": probe_layout_origins(
                state.worker_py,
                role="worker",
                evidence_dir=evidence_dir,
                python_version=python_version,
            ),
        }
        state_root = evidence_dir / "state"
        log_root = evidence_dir / "logs"
        sync_root = state_root / "sync"
        observer_bundle = _prepare_observer_bundle(work_dir)

        def persist_record(record: ScenarioRecord) -> None:
            if record not in state.scenario_records:
                state.scenario_records.append(record)
            _write_evidence(
                evidence_dir / "scenarios.partial.json",
                {"scenarios": [_scenario_to_json(item) for item in state.scenario_records]},
            )

        for item in _run_local_scenarios(
            cli_entry=state.cli_entry,
            state_root=state_root / "local",
            log_root=log_root / "local",
            on_record=persist_record,
        ):
            persist_record(item)
        owner = OwnedNatsServer()
        target = owner.start()
        _run_remote_scenarios(
            cli_entry=state.cli_entry,
            cli_py=state.cli_py,
            worker_entry=state.worker_entry,
            nats_url=target.url,
            state_root=state_root / "remote",
            log_root=log_root / "remote",
            sync_root=sync_root,
            observer_bundle=observer_bundle,
            on_record=persist_record,
        )
    except BaseException as exc:
        notes = getattr(exc, "__notes__", [])
        state.error = "\n".join([str(exc), *notes])
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
            raise CliProcessError(state.error) from cleanup_error
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
            f"CLI process {tag}: {len(scenarios)} scenarios, "
            f"runtime {payload.get('runtime_version')}, origins OK",
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
        help="Directory for per-run JSON evidence (default under dist/verification/cli-process).",
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
    work_dir = Path(tempfile.mkdtemp(prefix="superjobs-cli-process-", dir=parent))
    default_artifact = REPO_ROOT / "dist" / "verification" / "cli-process"
    artifact_parent = (args.artifact_dir or default_artifact).resolve()
    artifact_dir = artifact_parent / f"run-{uuid.uuid4().hex[:12]}"
    artifact_dir.mkdir(parents=True, exist_ok=True)

    records: dict[str, dict[str, Any]] = {}
    exit_code = 0
    try:
        bundle = build_example_wheels(work_dir)
        for py_version in python_versions:
            pass_state = verify_python_version(
                python_version=py_version,
                work_dir=work_dir,
                bundle=bundle,
                artifact_dir=artifact_dir,
            )
            records[py_version] = _pass_payload(pass_state)
    except (Exception, KeyboardInterrupt) as exc:
        exit_code = 1
        if isinstance(exc, KeyboardInterrupt):
            print("verify_cli_process: interrupted", file=sys.stderr)
        else:
            print(f"verify_cli_process: {exc}", file=sys.stderr)
        _write_evidence(artifact_dir / "run_error.json", {"error": str(exc)})
    else:
        print_summary(records)
        _write_evidence(artifact_dir / "summary.json", records)
    finally:
        if not args.keep_work:
            shutil.rmtree(work_dir, ignore_errors=True)
    return exit_code


if __name__ == "__main__":
    raise SystemExit(main())
