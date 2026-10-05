"""Deterministic checks for tools/verify_cli_process.py (no full NATS gate by default)."""

from __future__ import annotations

import json
import os
import subprocess
import sys
import time
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]
RUNNER = REPO_ROOT / "tools" / "verify_cli_process.py"

from tools.cli_process_support import protocol as cli_protocol  # noqa: E402
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
from tools.cross_program_support.child_env import isolated_child_env  # noqa: E402
from tools import verify_cli_process as vcp  # noqa: E402


def test_assert_checkpoint_absent(tmp_path: Path) -> None:
    cli_protocol.assert_checkpoint_absent(tmp_path, "handler_entered")
    cli_protocol.checkpoint_path(tmp_path, "handler_entered").write_text("{}", encoding="utf-8")
    with pytest.raises(cli_protocol.ProtocolError, match="must not exist"):
        cli_protocol.assert_checkpoint_absent(tmp_path, "handler_entered")


def test_wait_for_worker_ready_rejects_early_exit(tmp_path: Path) -> None:
    child = MagicMock()
    child.poll.return_value = 4
    with pytest.raises(cli_protocol.ProtocolError, match="before ready"):
        cli_protocol.wait_for_worker_ready(
            tmp_path,
            run_id="run",
            deadline=0.2,
            poll_interval=0.01,
            child_process=child,
        )


def test_wait_for_worker_ready_times_out(tmp_path: Path) -> None:
    child = MagicMock()
    child.poll.return_value = None
    with pytest.raises(TimeoutError, match="not ready"):
        cli_protocol.wait_for_worker_ready(
            tmp_path,
            run_id="run",
            deadline=0.05,
            poll_interval=0.01,
            child_process=child,
        )


def test_wait_for_checkpoint_times_out(tmp_path: Path) -> None:
    with pytest.raises(TimeoutError, match="checkpoint"):
        cli_protocol.wait_for_checkpoint(
            tmp_path,
            "handler_entered",
            deadline=0.05,
            poll_interval=0.01,
            gate="hold",
        )


def test_parse_execution_reference() -> None:
    stderr = 'diagnostic\nexecution reference: {"job_id":"abc","job_name":"x","job_version":"v1"}\n'
    ref = vcp._parse_execution_reference(stderr)
    assert ref is not None and ref["job_id"] == "abc"


def test_scenario_json_retains_command_streams(tmp_path: Path) -> None:
    stdout_path = tmp_path / "out.log"
    stderr_path = tmp_path / "err.log"
    stdout_path.write_text("stdout-body", encoding="utf-8")
    stderr_path.write_text("stderr-body", encoding="utf-8")
    record = vcp.ScenarioRecord(
        name="demo",
        run_id="r1",
        state_dir=tmp_path,
        started_at=0.0,
    )
    record.commands.append(
        vcp.CommandRecord(
            name="cli",
            command=["cli"],
            cwd=tmp_path,
            started_at=0.0,
            exit_code=0,
            stdout_path=stdout_path,
            stderr_path=stderr_path,
        ),
    )
    payload = vcp._scenario_to_json(record)
    assert payload["commands"][0]["stdout"] == "stdout-body"
    assert payload["commands"][0]["stderr"] == "stderr-body"


def test_runner_help_exits_zero() -> None:
    completed = subprocess.run(
        [sys.executable, str(RUNNER), "--help"],
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
        check=False,
        creationflags=subprocess.CREATE_NO_WINDOW if sys.platform == "win32" else 0,
    )
    assert completed.returncode == 0
    assert "--artifact-dir" in completed.stdout


def test_broker_cleanup_failure_fails_pass_and_retains_evidence(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from types import SimpleNamespace

    log = tmp_path / "broker.log"
    log.write_text("broker diagnostics", encoding="utf-8")

    class BrokenCleanupOwner:
        def __init__(self):
            self.target = SimpleNamespace(log_path=log)

        def start(self):
            return SimpleNamespace(log_path=log, url="nats://127.0.0.1:4222")

        def stop(self, target):
            raise RuntimeError("cleanup broke")

    bundle = vcp.WheelBundle(
        library=tmp_path / "lib.whl",
        contract=tmp_path / "contract.whl",
        handlers=tmp_path / "handlers.whl",
        cli_example=tmp_path / "cli.whl",
        worker_example=tmp_path / "worker.whl",
        worker_resources=tmp_path / "worker_resources.whl",
    )

    monkeypatch.setattr(vcp, "OwnedNatsServer", BrokenCleanupOwner)
    monkeypatch.setattr(vcp, "create_layout_venv", lambda *args, **kwargs: tmp_path)
    monkeypatch.setattr(vcp, "console_script_path", lambda *args, **kwargs: tmp_path / "cli.exe")
    monkeypatch.setattr(vcp, "venv_python", lambda *args: Path(sys.executable))
    monkeypatch.setattr(vcp, "assert_final_runtime_python", lambda *args: "3.12")
    monkeypatch.setattr(vcp, "probe_layout_origins", lambda *args, **kwargs: {})
    monkeypatch.setattr(vcp, "_run_local_scenarios", lambda **kwargs: [])
    monkeypatch.setattr(vcp, "_run_remote_scenarios", lambda **kwargs: [])

    evidence = tmp_path / "evidence"
    with pytest.raises(vcp.CliProcessError, match="cleanup broke"):
        vcp.verify_python_version(
            python_version="3.12",
            work_dir=tmp_path / "work",
            bundle=bundle,
            artifact_dir=evidence,
        )
    payload = json.loads((evidence / "py312" / "summary.json").read_text(encoding="utf-8"))
    assert "cleanup broke" in payload["error"]
    assert payload["broker_log"] == "broker diagnostics"


def test_spawn_logged_registers_with_child_owner(tmp_path: Path) -> None:
    with ChildOwner() as owner:
        assert current_child_owner() is owner
        process, record, stdout_io, stderr_io = vcp._spawn_logged(
            name="sleep",
            command=[sys.executable, "-c", "import time; time.sleep(0.2)"],
            cwd=tmp_path,
            env=isolated_child_env(),
            log_dir=tmp_path / "logs",
        )
        assert owner.owned_for_process(process) is not None
        assert owner.owned_for_process(process).record is record
        vcp._reap_process(process, label="sleep", deadline=time.monotonic() + 15.0, owner=owner)
        vcp._close_streams(stdout_io, stderr_io)
    assert current_child_owner() is None


def test_reap_forced_timeout_fails(tmp_path: Path) -> None:
    owner = ChildOwner()
    process = subprocess.Popen(
        [sys.executable, "-c", "import time; time.sleep(30)"],
        cwd=tmp_path,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    child = owner.register(OwnedChild(label="slow", process=process))
    with pytest.raises(cli_protocol.ProtocolError, match="forced child termination"):
        reap_with_owner(child, owner=owner, deadline=time.monotonic() + 0.2, poll_interval=0.01)
    assert process.poll() is not None


def test_child_owner_reaps_descendant(tmp_path: Path) -> None:
    from tests.test_dev_check import _descendant_pid_alive

    marker = tmp_path / "child-pid.txt"
    script = tmp_path / "tree.py"
    script.write_text(
        "import pathlib, subprocess, sys, time\n"
        "child = subprocess.Popen([sys.executable, '-c', 'import time; time.sleep(30)'])\n"
        "pathlib.Path(sys.argv[1]).write_text(str(child.pid))\n"
        "time.sleep(30)\n",
        encoding="utf-8",
    )
    with ChildOwner(allow_forced_cleanup=True) as owner:
        process = subprocess.Popen(
            [sys.executable, str(script), str(marker)],
            cwd=tmp_path,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        owner.register(OwnedChild(label="tree", process=process))
        deadline = time.monotonic() + 5
        while not marker.is_file() and time.monotonic() < deadline:
            time.sleep(0.01)
        assert marker.is_file(), "descendant never reached readiness"
        child_pid = int(marker.read_text())
        assert _descendant_pid_alive(child_pid)
        owner.force_kill(owner.owned_for_process(process), reason="test")
    assert process.poll() is not None
    assert not _descendant_pid_alive(child_pid), "owned descendant survived cleanup"


def test_run_cli_retains_command_record_on_failure(tmp_path: Path) -> None:
    missing = tmp_path / "missing-cli.exe"
    commands: list[vcp.CommandRecord] = []
    with ChildOwner():
        with pytest.raises((OSError, vcp.CliProcessError)):
            vcp._run_cli(
                entry=missing,
                args=[],
                env=isolated_child_env(),
                log_dir=tmp_path / "logs",
                name="cli",
                commands=commands,
            )
    assert len(commands) == 1
    assert commands[0].name == "cli"


def test_observation_verdict_rejects_wrong_order() -> None:
    envelopes = [
        {"job_id": "j1", "sequence": 1, "observation": {"kind": "started", "payload": {}}},
        {"job_id": "j1", "sequence": 2, "observation": {"kind": "log", "payload": {}}},
        {"job_id": "j1", "sequence": 3, "observation": {"kind": "progress", "payload": {}}},
        {
            "job_id": "j1",
            "sequence": 4,
            "observation": {"kind": "application", "payload": {"stage": "mid"}},
        },
        {"job_id": "j1", "sequence": 5, "observation": {"kind": "failed", "payload": {}}},
    ]
    with pytest.raises(cli_protocol.ProtocolError, match="unexpected observation order"):
        assert_manifest_with_events_stream(envelopes, job_id="j1", device_id="dev")


def test_parse_observation_lines_rejects_malformed_json() -> None:
    stderr = 'cli-pid: 1\n{broken\n{"observation":{"kind":"started"},"sequence":1}\n'
    with pytest.raises(cli_protocol.ProtocolError, match="malformed JSON"):
        parse_observation_lines(stderr)


def test_child_owner_records_cleanup_errors(tmp_path: Path) -> None:
    owner = ChildOwner()
    with patch(
        "tools.cli_process_support.child_owner.kill_process_tree",
        return_value=["taskkill warning"],
    ):
        process = subprocess.Popen(
            [sys.executable, "-c", "import time; time.sleep(30)"],
            cwd=tmp_path,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        child = owner.register(OwnedChild(label="x", process=process))
        owner.force_kill(child, reason="test")
    assert owner.cleanup_errors
    if process.poll() is None:
        process.kill()
    process.wait(timeout=5)


@pytest.mark.skipif(sys.platform != "win32", reason="Windows console inheritance regression")
def test_hidden_console_delivers_real_ctrl_c(tmp_path: Path) -> None:
    ready = tmp_path / "signal-ready.txt"
    target = tmp_path / "signal_target.py"
    target.write_text(
        "import pathlib, signal, sys, time\n"
        "signal.signal(signal.SIGINT, lambda *_args: sys.exit(130))\n"
        "pathlib.Path(sys.argv[1]).write_text(str(__import__('os').getpid()))\n"
        "while True: time.sleep(0.05)\n",
        encoding="utf-8",
    )
    with ChildOwner() as owner:
        process, record, out_io, err_io = vcp._spawn_logged(
            name="interrupt", command=[sys.executable, "-I", str(target), str(ready)],
            cwd=tmp_path, env=isolated_child_env(), log_dir=tmp_path / "logs",
            interruptible=True,
        )
        deadline = time.monotonic() + 10
        while not ready.is_file() and time.monotonic() < deadline:
            time.sleep(0.01)
        assert ready.is_file(), record.stream_text()
        vcp.send_process_interrupt(process)
        assert vcp._reap_process(
            process, label="interrupt", deadline=time.monotonic() + 15,
            expected_code=130, owner=owner,
        ) == 130
        assert not owner.forced_kills
        vcp._close_streams(out_io, err_io)
