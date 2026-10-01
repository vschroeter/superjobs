"""Deterministic checks for tools/verify_cross_program.py (no full NATS gate by default)."""

from __future__ import annotations

import json
import shutil
import subprocess
import sys
import time
import uuid
from pathlib import Path
from unittest.mock import MagicMock

import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]
RUNNER = REPO_ROOT / "tools" / "verify_cross_program.py"
SUPPORT = REPO_ROOT / "tools" / "cross_program_support"
REGRESSION = REPO_ROOT / "tests" / "support" / "cross_program_regression"

from tools.cross_program_support import protocol as cp_protocol  # noqa: E402
from tools.cross_program_support.child_env import isolated_child_env, python_command  # noqa: E402
from tools.cross_program_support.expectations import expect_job_cancelled, expect_job_failed  # noqa: E402
from tools import verify_cross_program as vcp  # noqa: E402


def _materialize_regression_role(
    base: Path,
    role: str,
    *,
    script_src: Path,
) -> Path:
    role_dir = base / role
    role_dir.mkdir(parents=True, exist_ok=True)
    shutil.copy2(SUPPORT / "protocol.py", role_dir / "protocol.py")
    script_name = "worker_app.py" if role == "worker" else "producer_app.py"
    shutil.copy2(script_src, role_dir / script_name)
    return role_dir


def _run_regression_scenario(
    tmp_path: Path,
    *,
    spec: vcp.ScenarioSpec,
    worker_script: Path | None,
    producer_script: Path | None,
    artifact_dir: Path | None = None,
) -> vcp.ScenarioRecord:
    roles_root = tmp_path / "roles"
    producer_dir = _materialize_regression_role(
        roles_root,
        "producer",
        script_src=producer_script or REGRESSION / "producer_exit0.py",
    )
    worker_dir = _materialize_regression_role(
        roles_root,
        "worker",
        script_src=worker_script or REGRESSION / "worker_normal.py",
    )
    return vcp._run_scenario(
        spec=spec,
        nats_url="nats://127.0.0.1:4222",
        producer_python=Path(sys.executable),
        worker_python=Path(sys.executable),
        producer_dir=producer_dir,
        worker_dir=worker_dir,
        artifact_scenario_dir=artifact_dir,
    )


def test_ready_marker_requires_matching_run_id(tmp_path: Path) -> None:
    cp_protocol.assert_ready_absent(tmp_path)
    run_id = uuid.uuid4().hex
    cp_protocol.write_ready(tmp_path, run_id)
    marker = cp_protocol.read_ready(tmp_path)
    assert marker.run_id == run_id
    cp_protocol.write_ready(tmp_path, "other-id")
    with pytest.raises(cp_protocol.ProtocolError, match="run_id"):
        cp_protocol.wait_for_ready(tmp_path, expected_run_id=run_id, deadline=0.2, poll_interval=0.01)


def test_checkpoint_includes_execution_id(tmp_path: Path) -> None:
    run_id = uuid.uuid4().hex
    execution_id = "exec-123"
    cp_protocol.write_checkpoint(
        tmp_path,
        run_id=run_id,
        checkpoint=cp_protocol.HANDLER_ENTERED,
        job="cross_program.verify.cancel",
        execution_id=execution_id,
    )
    marker = cp_protocol.read_checkpoint(tmp_path, cp_protocol.HANDLER_ENTERED)
    assert marker.execution_id == execution_id
    with pytest.raises(cp_protocol.ProtocolError, match="execution_id"):
        cp_protocol.wait_for_checkpoint(
            tmp_path,
            name=cp_protocol.HANDLER_ENTERED,
            expected_run_id=run_id,
            expected_execution_id="other",
            deadline=0.05,
            poll_interval=0.01,
        )


def test_protocol_writes_are_atomic(tmp_path: Path) -> None:
    run_id = uuid.uuid4().hex
    cp_protocol.write_ready(tmp_path, run_id)
    assert not list(tmp_path.glob(".*.tmp-*"))
    payload = json.loads((tmp_path / cp_protocol.READY_FILE).read_text(encoding="utf-8"))
    assert payload["run_id"] == run_id


def test_wait_for_ready_times_out(tmp_path: Path) -> None:
    with pytest.raises(TimeoutError):
        cp_protocol.wait_for_ready(
            tmp_path,
            expected_run_id="missing",
            deadline=0.05,
            poll_interval=0.01,
        )


def test_wait_for_ready_rejects_early_child_exit(tmp_path: Path) -> None:
    child = MagicMock()
    child.poll.return_value = 3
    with pytest.raises(cp_protocol.ProtocolError, match="exited with code 3"):
        cp_protocol.wait_for_ready(
            tmp_path,
            expected_run_id="run",
            deadline=0.2,
            poll_interval=0.01,
            child_process=child,
        )


def test_isolated_child_env_does_not_set_pythonpath() -> None:
    env = isolated_child_env({"NATS_URL": "nats://127.0.0.1:4222"})
    assert "PYTHONPATH" not in env
    assert env["PYTHONNOUSERSITE"] == "1"


def test_python_command_uses_isolated_bootstrap(tmp_path: Path) -> None:
    role_dir = tmp_path / "producer"
    role_dir.mkdir()
    args = python_command(sys.executable, role_dir, "producer_app.py")
    assert args[1] == "-I"
    assert args[2].endswith("isolated_bootstrap.py")
    assert args[3] == str(role_dir.resolve())
    assert args[4] == "producer_app.py"


def test_expect_job_failed_requires_exception() -> None:
    with pytest.raises(AssertionError, match="JobFailedError"):
        with expect_job_failed():
            pass


def test_expect_job_cancelled_requires_exception() -> None:
    with pytest.raises(AssertionError, match="JobCancelledError"):
        with expect_job_cancelled():
            pass


def test_materialize_role_trees(tmp_path: Path) -> None:
    producer_dir = vcp.materialize_role_dir(tmp_path, "producer", vcp.PRODUCER_SUPPORT_FILES)
    worker_dir = vcp.materialize_role_dir(tmp_path, "worker", vcp.WORKER_SUPPORT_FILES)
    assert (producer_dir / "runtime_isolation.py").is_file()
    assert (worker_dir / "worker_handlers.py").is_file()
    assert not (producer_dir / "worker_handlers.py").exists()


def test_scenario_record_retains_streams_on_failure(tmp_path: Path) -> None:
    record = vcp.ScenarioRecord(
        name="failed",
        run_id="abc",
        state_dir=tmp_path,
        started_at=0.0,
        error="boom",
    )
    stdout_path = tmp_path / "producer.stdout.log"
    stderr_path = tmp_path / "producer.stderr.log"
    stdout_path.write_text("hello-out", encoding="utf-8")
    stderr_path.write_text("hello-err", encoding="utf-8")
    record.producer = vcp.ChildRecord(
        name="producer",
        command=["python"],
        cwd=tmp_path,
        started_at=0.0,
        exit_code=3,
        stdout_path=stdout_path,
        stderr_path=stderr_path,
    )
    payload = vcp._scenario_to_json(record)
    assert payload["error"] == "boom"
    assert payload["producer_stdout"] == "hello-out"
    assert payload["producer_stderr"] == "hello-err"
    assert payload["producer"]["exit_code"] == 3
    assert payload["producer"]["command"] == ["python"]


def test_checkpoint_snapshot_tolerates_corrupt_ready(tmp_path: Path) -> None:
    ready = tmp_path / "worker_ready.json"
    ready.write_text("{bad", encoding="utf-8")
    snapshot = vcp._collect_checkpoint_snapshot(tmp_path)
    assert "error" in snapshot["ready"]
    assert snapshot["ready"]["raw"] == "{bad"


def test_run_scenario_normal_worker_stop_and_producer_ok(tmp_path: Path) -> None:
    spec = vcp.ScenarioSpec(
        "regression_ok",
        start_worker=True,
        start_producer=True,
        expected_producer_code=vcp.PRODUCER_EXIT_OK,
        expected_worker_code=None,
    )
    record = _run_regression_scenario(tmp_path, spec=spec, worker_script=None, producer_script=None)
    assert record.error is None
    assert record.producer is not None and record.producer.exit_code == 0
    assert record.worker is not None and record.worker.exit_code == 0
    assert record.checkpoints is not None
    assert record.checkpoints.get("worker_stop") is not None


def test_run_scenario_worker_crash_before_ready(tmp_path: Path) -> None:
    spec = vcp.ScenarioSpec(
        "regression_worker_crash",
        start_worker=True,
        start_producer=False,
        expected_producer_code=None,
        expected_worker_code=None,
    )
    record = _run_regression_scenario(
        tmp_path,
        spec=spec,
        worker_script=REGRESSION / "worker_crash_before_ready.py",
        producer_script=None,
    )
    assert record.error is not None
    assert record.worker is not None
    assert record.worker.exit_code == 42
    assert "crash before ready" in record.worker.stream_text()[1]


def test_run_scenario_producer_failure_cleans_worker(tmp_path: Path) -> None:
    spec = vcp.ScenarioSpec(
        "regression_producer_fail",
        start_worker=True,
        start_producer=True,
        expected_producer_code=vcp.PRODUCER_EXIT_OK,
        expected_worker_code=None,
    )
    record = _run_regression_scenario(
        tmp_path,
        spec=spec,
        worker_script=None,
        producer_script=REGRESSION / "producer_exit1.py",
    )
    assert record.error is not None
    assert record.producer is not None and record.producer.exit_code == 1
    assert record.worker is not None and record.worker.exit_code is not None
    assert record.worker.stdout_path is not None and record.worker.stdout_path.is_file()


def test_run_scenario_corrupt_ready_retains_diagnostic(tmp_path: Path) -> None:
    spec = vcp.ScenarioSpec(
        "regression_corrupt_ready",
        start_worker=True,
        start_producer=False,
        expected_producer_code=None,
        expected_worker_code=None,
    )
    record = _run_regression_scenario(
        tmp_path,
        spec=spec,
        worker_script=REGRESSION / "worker_corrupt_ready.py",
        producer_script=None,
    )
    assert record.error is not None
    assert record.checkpoints is not None
    ready = record.checkpoints.get("ready")
    assert isinstance(ready, dict) and "error" in ready and ready.get("raw")


def test_run_scenario_timeout_cleanup(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(vcp, "SCENARIO_TIMEOUT_SECONDS", 1.5)
    monkeypatch.setattr(vcp, "CHILD_TIMEOUT_SECONDS", 1.5)
    monkeypatch.setattr(vcp, "KILL_REAP_SECONDS", 0.3)
    spec = vcp.ScenarioSpec(
        "regression_timeout",
        start_worker=True,
        start_producer=True,
        expected_producer_code=vcp.PRODUCER_EXIT_OK,
        expected_worker_code=None,
    )
    started = time.monotonic()
    record = _run_regression_scenario(
        tmp_path,
        spec=spec,
        worker_script=None,
        producer_script=REGRESSION / "producer_hang.py",
    )
    elapsed = time.monotonic() - started
    assert record.error is not None
    assert elapsed < 5.0
    assert record.producer is not None and record.producer.exit_code is not None
    assert record.worker is not None and record.worker.exit_code is not None


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


def test_interrupt_records_failure_and_reaps_worker(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    def interrupted(*args, **kwargs):
        raise KeyboardInterrupt

    monkeypatch.setattr(vcp, "wait_for_ready", interrupted)
    spec = vcp.ScenarioSpec("interrupted", True, False, None, None)
    record = _run_regression_scenario(
        tmp_path, spec=spec, worker_script=None, producer_script=None,
        artifact_dir=tmp_path / "evidence",
    )
    assert record.error and "KeyboardInterrupt" in record.error
    assert record.worker is not None and record.worker.exit_code is not None
    evidence = next((tmp_path / "evidence").rglob("scenario.json"))
    assert json.loads(evidence.read_text(encoding="utf-8"))["error"]


def test_broker_cleanup_failure_fails_pass_and_retains_evidence(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    from types import SimpleNamespace

    log = tmp_path / "broker.log"
    log.write_text("broker diagnostics", encoding="utf-8")

    class BrokenCleanupOwner:
        def __init__(self):
            self.target = SimpleNamespace(log_path=log)

        def start(self):
            return self.target

        def stop(self, target):
            raise RuntimeError("cleanup broke")

    monkeypatch.setattr(vcp, "OwnedNatsServer", BrokenCleanupOwner)
    monkeypatch.setattr(vcp, "create_wheel_venv", lambda *args, **kwargs: tmp_path)
    monkeypatch.setattr(vcp, "assert_final_runtime_python", lambda *args: "3.12")
    monkeypatch.setattr(vcp, "probe_role_origins", lambda *args, **kwargs: {})
    monkeypatch.setattr(vcp, "SCENARIOS", ())
    evidence = tmp_path / "evidence"
    with pytest.raises(vcp.CrossProgramError, match="cleanup broke"):
        vcp.verify_python_version(
            python_version="3.12", work_dir=tmp_path / "work",
            library=tmp_path / "library.whl", contract=tmp_path / "contract.whl",
            artifact_dir=evidence,
        )
    payload = json.loads((evidence / "py312" / "summary.json").read_text(encoding="utf-8"))
    assert "cleanup broke" in payload["error"]
    assert payload["broker_log"] == "broker diagnostics"
