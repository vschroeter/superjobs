"""Deterministic checks for tools/verify_idle_outage.py."""

from __future__ import annotations

import asyncio
import importlib
import json
import os
import shutil
import subprocess
import sys
import uuid
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]
RUNNER = REPO_ROOT / "tools" / "verify_idle_outage.py"

from tools.idle_outage_support import protocol as io_protocol  # noqa: E402
from tools import verify_idle_outage as vio  # noqa: E402


def test_assert_idle_outage_invocations_require_single_worker_pid() -> None:
    run_id = "run"
    baseline_id = "baseline"
    post_id = "post"
    invocations = [
        io_protocol.HandlerInvocation(
            run_id=run_id,
            worker_generation="1",
            job="j1",
            execution_id=baseline_id,
            pid=42,
        ),
        io_protocol.HandlerInvocation(
            run_id=run_id,
            worker_generation="1",
            job="j2",
            execution_id=post_id,
            pid=42,
        ),
    ]
    io_protocol.assert_idle_outage_invocations(
        invocations,
        run_id=run_id,
        baseline_execution_id=baseline_id,
        post_outage_execution_id=post_id,
        worker_pid=42,
    )
    bad_generation = [
        io_protocol.HandlerInvocation(
            run_id=run_id,
            worker_generation="2",
            job="j1",
            execution_id=baseline_id,
            pid=42,
        ),
        io_protocol.HandlerInvocation(
            run_id=run_id,
            worker_generation="1",
            job="j2",
            execution_id=post_id,
            pid=42,
        ),
    ]
    with pytest.raises(io_protocol.ProtocolError, match="single surviving worker generation"):
        io_protocol.assert_idle_outage_invocations(
            bad_generation,
            run_id=run_id,
            baseline_execution_id=baseline_id,
            post_outage_execution_id=post_id,
            worker_pid=42,
        )


def test_producer_request_round_trip(tmp_path: Path) -> None:
    run_id = uuid.uuid4().hex
    io_protocol.write_producer_request(
        tmp_path,
        run_id=run_id,
        request_id="req-1",
        command=io_protocol.COMMAND_BASELINE,
    )
    request = io_protocol.read_producer_request(tmp_path)
    assert request is not None
    assert request.command == io_protocol.COMMAND_BASELINE
    assert request.request_id == "req-1"


def test_broker_event_checkpoint_names() -> None:
    assert (
        io_protocol.broker_event_checkpoint("disconnected", "producer")
        == io_protocol.BROKER_DISCONNECTED_PRODUCER
    )


def test_materialize_role_dir_includes_callbacks(tmp_path: Path) -> None:
    worker_dir = vio.materialize_role_dir(tmp_path, "worker", vio.WORKER_SUPPORT_FILES)
    assert (worker_dir / "broker_callbacks.py").is_file()
    assert (worker_dir / "broker_connect.py").is_file()


def test_outage_read_error_oracle_rejects_missing_job() -> None:
    assert not io_protocol.is_allowed_outage_read_error("JobNotFoundError")
    with pytest.raises(io_protocol.ProtocolError, match="allowlist"):
        io_protocol.assert_allowed_outage_read_error("ValueError")


def test_outage_read_error_oracle_accepts_timeout() -> None:
    io_protocol.assert_allowed_outage_read_error("TimeoutError")


@pytest.mark.parametrize("error_type", ["AssertionError", "RuntimeError", None])
def test_outage_read_error_oracle_rejects_arbitrary(error_type: str | None) -> None:
    with pytest.raises(io_protocol.ProtocolError):
        io_protocol.assert_allowed_outage_read_error(error_type)


def test_broker_start_failure_writes_summary(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    class FailStartOwner:
        target = None

        def start(self, *, deadline=None):
            raise RuntimeError("broker start failed")

        def stop(self, target, *, deadline=None):
            return None

    monkeypatch.setattr(vio, "OwnedNatsServer", FailStartOwner)
    monkeypatch.setattr(vio, "create_wheel_venv", lambda *args, **kwargs: tmp_path)
    monkeypatch.setattr(vio, "assert_final_runtime_python", lambda *args: "3.12")
    monkeypatch.setattr(vio, "probe_idle_origins", lambda *args, **kwargs: {})
    evidence = tmp_path / "evidence"
    with pytest.raises(vio.IdleOutageError, match="broker start failed"):
        vio.verify_python_version(
            python_version="3.12",
            work_dir=tmp_path / "work",
            library=tmp_path / "library.whl",
            contract=tmp_path / "contract.whl",
            artifact_dir=evidence,
        )
    summary = json.loads((evidence / "py312" / "summary.json").read_text(encoding="utf-8"))
    assert summary["scenario_error"]
    assert "broker start failed" in summary["scenario_error"]


def test_missing_nats_executable_cli_writes_run_error(tmp_path: Path) -> None:
    missing = tmp_path / "missing-nats-server"
    artifact_dir = tmp_path / "artifacts"
    env = {**os.environ, "NATS_EXECUTABLE": str(missing)}
    completed = subprocess.run(
        [
            sys.executable,
            str(RUNNER),
            "--python",
            "3.12",
            "--artifact-dir",
            str(artifact_dir),
            "--work-dir",
            str(tmp_path / "work-parent"),
        ],
        cwd=REPO_ROOT,
        env=env,
        capture_output=True,
        text=True,
        check=False,
        creationflags=subprocess.CREATE_NO_WINDOW if sys.platform == "win32" else 0,
    )
    assert completed.returncode == 1
    run_error = json.loads((artifact_dir / "run_error.json").read_text(encoding="utf-8"))
    assert run_error.get("error")


@pytest.mark.parametrize("events", [("disconnected", "reconnected"), ("reconnected", "disconnected")])
def test_broker_callbacks_record_first_event_only(tmp_path: Path, monkeypatch: pytest.MonkeyPatch, events) -> None:
    import asyncio
    import importlib.util

    monkeypatch.setitem(sys.modules, "protocol", io_protocol)
    spec = importlib.util.spec_from_file_location("idle_outage_callbacks_under_test", REPO_ROOT / "tools/idle_outage_support/broker_callbacks.py")
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(module)
    disconnected, reconnected = module.make_broker_callbacks(tmp_path, "run", role="producer", runtime_object_id=lambda: 99)
    callbacks = {"disconnected": disconnected, "reconnected": reconnected}

    async def run_callbacks():
        for event in events:
            await callbacks[event]()
            await callbacks[event]()

    asyncio.run(run_callbacks())
    for index, event in enumerate(events, start=1):
        marker = io_protocol.read_checkpoint(tmp_path, io_protocol.broker_event_checkpoint(event, "producer"))
        assert marker.callback_sequence == index
        assert marker.runtime_object_id == 99



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
