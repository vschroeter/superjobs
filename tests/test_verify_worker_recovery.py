"""Deterministic checks for tools/verify_worker_recovery.py."""

from __future__ import annotations

import json
import shutil
import subprocess
import sys
import time
import uuid
from pathlib import Path
from types import SimpleNamespace
import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]
RUNNER = REPO_ROOT / "tools" / "verify_worker_recovery.py"
SUPPORT = REPO_ROOT / "tools" / "worker_recovery_support"
REGRESSION = REPO_ROOT / "tests" / "support" / "worker_recovery_regression"

from tools.worker_recovery_support import protocol as wr_protocol  # noqa: E402
from tools import verify_worker_recovery as vwr  # noqa: E402
from tools.verify_cross_program import ChildRecord  # noqa: E402


def _materialize_regression_worker(base: Path, script_src: Path) -> Path:
    role_dir = base / "worker"
    role_dir.mkdir(parents=True, exist_ok=True)
    for name in vwr.WORKER_SUPPORT_FILES:
        shutil.copy2(SUPPORT / name, role_dir / name)
    shutil.copy2(script_src, role_dir / "worker_app.py")
    return role_dir


def test_submitted_checkpoint_requires_matching_run_id(tmp_path: Path) -> None:
    run_id = uuid.uuid4().hex
    wr_protocol.write_checkpoint(
        tmp_path,
        run_id=run_id,
        checkpoint=wr_protocol.SUBMITTED,
        execution_id="exec-1",
    )
    with pytest.raises(wr_protocol.ProtocolError, match="run_id"):
        wr_protocol.wait_for_checkpoint(
            tmp_path,
            name=wr_protocol.SUBMITTED,
            expected_run_id="other",
            deadline=0.05,
            poll_interval=0.01,
        )


def test_invocation_evidence_is_atomic_and_counted(tmp_path: Path) -> None:
    run_id = uuid.uuid4().hex
    count = wr_protocol.record_handler_invocation(
        tmp_path,
        run_id=run_id,
        worker_generation="1",
        execution_id="exec-1",
    )
    assert count == 1
    assert not list(tmp_path.glob(".*.tmp-*"))
    second = wr_protocol.record_handler_invocation(
        tmp_path,
        run_id=run_id,
        worker_generation="2",
        execution_id="exec-1",
    )
    assert second == 2
    items = wr_protocol.read_invocations(tmp_path)
    assert [item.worker_generation for item in items] == ["1", "2"]


def test_hard_kill_records_nontrivial_exit(tmp_path: Path) -> None:
    pid_file = tmp_path / "interpreter.pid"
    cmd = [sys.executable, "-c",
           "import os, sys, time; from pathlib import Path; "
           "Path(sys.argv[1]).write_text(str(os.getpid())); time.sleep(30)", str(pid_file)]
    process = subprocess.Popen(
        cmd,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        creationflags=subprocess.CREATE_NO_WINDOW if sys.platform == "win32" else 0,
    )
    try:
        startup_deadline = time.monotonic() + 5
        while not pid_file.is_file() and time.monotonic() < startup_deadline:
            time.sleep(0.01)
        assert pid_file.is_file()
        record = ChildRecord(
            name="worker", command=cmd, cwd=tmp_path,
            started_at=time.monotonic(), pid=int(pid_file.read_text()),
        )
        exit_code = vwr._hard_kill_worker(process, record, deadline=time.monotonic() + 5)
        assert exit_code != 0
        assert process.poll() is not None
        assert record.exit_code == exit_code
        assert record.termination["interpreter_terminated"] is True
        assert record.termination["launcher_pid"] == process.pid
        assert record.termination["interpreter_pid"] == int(pid_file.read_text())
    finally:
        if process.poll() is None:
            process.kill()
        process.wait(timeout=5)


def test_hard_kill_rejects_already_exited_process(tmp_path: Path) -> None:
    process = subprocess.Popen(
        [sys.executable, "-c", "pass"],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        creationflags=subprocess.CREATE_NO_WINDOW if sys.platform == "win32" else 0,
    )
    process.wait(timeout=10)
    record = ChildRecord(
        name="worker",
        command=[],
        cwd=tmp_path,
        started_at=time.monotonic(),
        pid=process.pid,
    )
    with pytest.raises(vwr.WorkerRecoveryError, match="already exited"):
        vwr._hard_kill_worker(process, record, deadline=time.monotonic() + 2)


def test_wait_for_ready_rejects_stale_run_id(tmp_path: Path) -> None:
    wr_protocol.write_ready(tmp_path, "stale", pid=1, worker_generation="1")
    with pytest.raises(wr_protocol.ProtocolError, match="run_id"):
        wr_protocol.wait_for_ready(
            tmp_path,
            expected_run_id="expected",
            deadline=0.1,
            poll_interval=0.01,
        )


def test_wait_for_ready_rejects_replaced_worker_pid(tmp_path: Path) -> None:
    run_id = uuid.uuid4().hex
    wr_protocol.write_ready(tmp_path, run_id, pid=111, worker_generation="2")
    with pytest.raises(wr_protocol.ProtocolError, match="forbidden"):
        wr_protocol.wait_for_ready(
            tmp_path,
            expected_run_id=run_id,
            expected_worker_generation="2",
            forbidden_pid=111,
            deadline=0.1,
            poll_interval=0.01,
        )


def test_checkpoint_wait_fails_when_child_dies_first(tmp_path: Path) -> None:
    process = subprocess.Popen(
        [sys.executable, "-c", "pass"],
        creationflags=subprocess.CREATE_NO_WINDOW if sys.platform == "win32" else 0,
    )
    process.wait(timeout=10)
    with pytest.raises(wr_protocol.ProtocolError, match="exited"):
        wr_protocol.wait_for_checkpoint(
            tmp_path,
            name=wr_protocol.SUBMITTED,
            expected_run_id="run",
            deadline=0.2,
            poll_interval=0.01,
            child_process=process,
        )
    run_id = uuid.uuid4().hex
    wr_protocol.write_ready(tmp_path, run_id, pid=111, worker_generation="2")
    with pytest.raises(wr_protocol.ProtocolError, match="forbidden"):
        wr_protocol.wait_for_ready(
            tmp_path,
            expected_run_id=run_id,
            expected_worker_generation="2",
            forbidden_pid=111,
            deadline=0.1,
            poll_interval=0.01,
        )


def test_recovery_scenario_worker_crash_before_ready(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(vwr, "SCENARIO_TIMEOUT_SECONDS", 3.0)
    monkeypatch.setattr(vwr, "CHILD_TIMEOUT_SECONDS", 2.0)
    monkeypatch.setattr(vwr, "KILL_REAP_SECONDS", 0.3)
    worker_dir = _materialize_regression_worker(tmp_path, REGRESSION / "worker_never_ready.py")
    producer_dir = vwr.materialize_role_dir(tmp_path / "roles", "producer", vwr.PRODUCER_SUPPORT_FILES)
    record = vwr._run_recovery_scenario(
        scenario=vwr.SCENARIO_BEFORE,
        nats_url="nats://127.0.0.1:4222",
        broker_target=SimpleNamespace(work_dir=tmp_path / "broker", process=None),
        producer_python=Path(sys.executable),
        worker_python=Path(sys.executable),
        producer_dir=producer_dir,
        worker_dir=worker_dir,
        artifact_scenario_dir=tmp_path / "artifacts",
        scenario_deadline=time.monotonic() + 3.0,
    )
    assert record.error is not None
    assert record.workers
    assert record.workers[0].exit_code == 42
    assert "before publishing ready" in record.error or "exited with code 42" in record.error


def test_broker_cleanup_failure_fails_pass(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    log = tmp_path / "broker.log"
    log.write_text("broker diagnostics", encoding="utf-8")

    class BrokenCleanupOwner:
        def __init__(self):
            self.target = SimpleNamespace(log_path=log, work_dir=tmp_path / "store", url="nats://x")

        def start(self):
            (tmp_path / "store").mkdir()
            return self.target

        def stop(self, target):
            raise RuntimeError("cleanup broke")

    monkeypatch.setattr(vwr, "OwnedNatsServer", BrokenCleanupOwner)
    monkeypatch.setattr(vwr, "create_wheel_venv", lambda *args, **kwargs: tmp_path)
    monkeypatch.setattr(vwr, "assert_final_runtime_python", lambda *args: "3.12")
    monkeypatch.setattr(vwr, "probe_recovery_origins", lambda *args, **kwargs: {})
    monkeypatch.setattr(vwr, "_run_recovery_scenario", lambda **kwargs: vwr.RecoveryScenarioRecord(
        name="recovery_before_completion",
        run_id="r",
        state_dir=tmp_path,
        broker_store_dir=tmp_path / "store",
        broker_store_fingerprint_before="files=0:bytes=0",
        broker_store_fingerprint_after="files=0:bytes=0",
        started_at=0.0,
    ))
    evidence = tmp_path / "evidence"
    with pytest.raises(vwr.WorkerRecoveryError, match="cleanup broke"):
        vwr.verify_python_version(
            python_version="3.12",
            work_dir=tmp_path / "work",
            library=tmp_path / "library.whl",
            contract=tmp_path / "contract.whl",
            artifact_dir=evidence,
        )
    summary = json.loads((evidence / "py312" / "summary-recovery_before_completion.json").read_text())
    assert "cleanup broke" in summary["error"]
    assert "broker diagnostics" in summary["broker_log"]


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
    assert "--scenario" in completed.stdout


def test_recovery_scenario_rejects_stale_ready_pid(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(vwr, "SCENARIO_TIMEOUT_SECONDS", 3.0)
    monkeypatch.setattr(vwr, "CHILD_TIMEOUT_SECONDS", 2.0)
    monkeypatch.setattr(vwr, "KILL_REAP_SECONDS", 0.3)
    worker_dir = _materialize_regression_worker(tmp_path, REGRESSION / "stale_ready_worker.py")
    producer_dir = vwr.materialize_role_dir(
        tmp_path / "roles",
        "producer",
        vwr.PRODUCER_SUPPORT_FILES,
    )
    record = vwr._run_recovery_scenario(
        scenario=vwr.SCENARIO_BEFORE,
        nats_url="nats://127.0.0.1:4222",
        broker_target=SimpleNamespace(work_dir=tmp_path / "broker", process=None),
        producer_python=Path(sys.executable),
        worker_python=Path(sys.executable),
        producer_dir=producer_dir,
        worker_dir=worker_dir,
        artifact_scenario_dir=tmp_path / "artifacts",
        scenario_deadline=time.monotonic() + 3.0,
    )
    assert record.error is not None
    assert "generation" in record.error.lower()


def test_recovery_scenario_producer_hang_is_reaped(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(vwr, "SCENARIO_TIMEOUT_SECONDS", 4.0)
    monkeypatch.setattr(vwr, "CHILD_TIMEOUT_SECONDS", 2.0)
    monkeypatch.setattr(vwr, "KILL_REAP_SECONDS", 0.3)
    worker_dir = _materialize_regression_worker(tmp_path, REGRESSION / "worker_ready_only.py")
    producer_dir = vwr.materialize_role_dir(tmp_path / "roles", "producer", vwr.PRODUCER_SUPPORT_FILES)
    shutil.copy2(REGRESSION / "producer_hang.py", producer_dir / "producer_app.py")
    record = vwr._run_recovery_scenario(
        scenario=vwr.SCENARIO_BEFORE,
        nats_url="nats://127.0.0.1:4222",
        broker_target=SimpleNamespace(work_dir=tmp_path / "broker", process=None),
        producer_python=Path(sys.executable),
        worker_python=Path(sys.executable),
        producer_dir=producer_dir,
        worker_dir=worker_dir,
        artifact_scenario_dir=tmp_path / "artifacts",
        scenario_deadline=time.monotonic() + 4.0,
    )
    assert record.error is not None
    assert record.producers
    assert record.producers[0].exit_code is not None


def test_recovery_scenario_producer_crash_is_reaped(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(vwr, "SCENARIO_TIMEOUT_SECONDS", 4.0)
    monkeypatch.setattr(vwr, "CHILD_TIMEOUT_SECONDS", 2.0)
    monkeypatch.setattr(vwr, "KILL_REAP_SECONDS", 0.3)
    worker_dir = _materialize_regression_worker(tmp_path, REGRESSION / "worker_ready_only.py")
    producer_dir = vwr.materialize_role_dir(tmp_path / "roles", "producer", vwr.PRODUCER_SUPPORT_FILES)
    shutil.copy2(REGRESSION / "producer_crash.py", producer_dir / "producer_app.py")
    record = vwr._run_recovery_scenario(
        scenario=vwr.SCENARIO_BEFORE,
        nats_url="nats://127.0.0.1:4222",
        broker_target=SimpleNamespace(work_dir=tmp_path / "broker", process=None),
        producer_python=Path(sys.executable),
        worker_python=Path(sys.executable),
        producer_dir=producer_dir,
        worker_dir=worker_dir,
        artifact_scenario_dir=tmp_path / "artifacts",
        scenario_deadline=time.monotonic() + 4.0,
    )
    assert record.error is not None
    assert record.producers
    assert record.producers[0].exit_code not in (None, 0)


def test_interrupt_records_nonempty_error(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    def interrupted(*args, **kwargs):
        raise KeyboardInterrupt

    monkeypatch.setattr(wr_protocol, "wait_for_ready", interrupted)
    worker_dir = vwr.materialize_role_dir(tmp_path, "worker", vwr.WORKER_SUPPORT_FILES)
    producer_dir = vwr.materialize_role_dir(tmp_path, "producer", vwr.PRODUCER_SUPPORT_FILES)
    record = vwr._run_recovery_scenario(
        scenario=vwr.SCENARIO_BEFORE,
        nats_url="nats://127.0.0.1:4222",
        broker_target=SimpleNamespace(work_dir=tmp_path / "broker", process=None),
        producer_python=Path(sys.executable),
        worker_python=Path(sys.executable),
        producer_dir=producer_dir,
        worker_dir=worker_dir,
        artifact_scenario_dir=tmp_path / "evidence",
        scenario_deadline=time.monotonic() + 3.0,
    )
    assert record.error
    assert "KeyboardInterrupt" in record.error
    evidence = next((tmp_path / "evidence").rglob("scenario.json"))
    payload = json.loads(evidence.read_text(encoding="utf-8"))
    assert payload["error"]


def _replacement_ack_marker(
    *,
    terminal_state: str = "COMPLETED",
    terminal_event_published: bool = True,
) -> wr_protocol.CheckpointMarker:
    return wr_protocol.CheckpointMarker(
        run_id="run",
        checkpoint=wr_protocol.REPLACEMENT_ACK,
        execution_id="exec-1",
        pid=2,
        worker_generation="2",
        terminal_state=terminal_state,
        terminal_event_published=terminal_event_published,
    )


@pytest.mark.parametrize(
    ("terminal_state", "terminal_event_published"),
    [
        ("FAILED", True),
        ("CANCELLED", True),
        ("COMPLETED", False),
    ],
)
def test_validate_replacement_ack_rejects_non_success_terminal(
    terminal_state: str,
    terminal_event_published: bool,
) -> None:
    worker = ChildRecord(name="worker-2", command=[], cwd=Path("."), started_at=0.0, pid=2)
    with pytest.raises(vwr.WorkerRecoveryError):
        vwr._validate_replacement_ack(
            _replacement_ack_marker(
                terminal_state=terminal_state,
                terminal_event_published=terminal_event_published,
            ),
            worker_record=worker,
            execution_id="exec-1",
        )


def test_validate_replacement_ack_accepts_completed_terminal() -> None:
    worker = ChildRecord(name="worker-2", command=[], cwd=Path("."), started_at=0.0, pid=2)
    vwr._validate_replacement_ack(
        _replacement_ack_marker(),
        worker_record=worker,
        execution_id="exec-1",
    )


def test_validate_handler_entered_rejects_pid_mismatch() -> None:
    worker = ChildRecord(name="worker-1", command=[], cwd=Path("."), started_at=0.0, pid=10)
    marker = wr_protocol.CheckpointMarker(
        run_id="run",
        checkpoint=wr_protocol.handler_entered_checkpoint("1"),
        execution_id="exec-1",
        pid=11,
        worker_generation="1",
    )
    with pytest.raises(vwr.WorkerRecoveryError, match="pid"):
        vwr._validate_handler_entered_checkpoint(
            marker,
            worker_record=worker,
            execution_id="exec-1",
            worker_generation="1",
        )


def test_validate_retry_published_rejects_missing_attempt_evidence() -> None:
    worker = ChildRecord(name="worker-1", command=[], cwd=Path("."), started_at=0.0, pid=10)
    marker = wr_protocol.CheckpointMarker(
        run_id="run",
        checkpoint=wr_protocol.RETRY_PUBLISHED,
        execution_id="exec-1",
        pid=10,
        worker_generation="1",
    )
    with pytest.raises(vwr.WorkerRecoveryError, match="delivery_attempt"):
        vwr._validate_retry_published_checkpoint(
            marker,
            worker_record=worker,
            execution_id="exec-1",
        )


def test_validate_retry_published_accepts_delivery_and_retry_evidence() -> None:
    worker = ChildRecord(name="worker-1", command=[], cwd=Path("."), started_at=0.0, pid=10)
    marker = wr_protocol.CheckpointMarker(
        run_id="run",
        checkpoint=wr_protocol.RETRY_PUBLISHED,
        execution_id="exec-1",
        pid=10,
        worker_generation="1",
        delivery_attempt=1,
        retry_target_attempt=2,
    )
    vwr._validate_retry_published_checkpoint(
        marker,
        worker_record=worker,
        execution_id="exec-1",
    )


def test_broker_start_failure_writes_scenario_summary(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    class FailStartOwner:
        target = None

        def start(self):
            raise RuntimeError("broker start failed")

        def stop(self, target):
            return None

    monkeypatch.setattr(vwr, "OwnedNatsServer", FailStartOwner)
    monkeypatch.setattr(vwr, "create_wheel_venv", lambda *args, **kwargs: tmp_path)
    monkeypatch.setattr(vwr, "assert_final_runtime_python", lambda *args: "3.12")
    monkeypatch.setattr(vwr, "probe_recovery_origins", lambda *args, **kwargs: {})
    evidence = tmp_path / "evidence"
    with pytest.raises(vwr.WorkerRecoveryError, match="broker start failed"):
        vwr.verify_python_version(
            python_version="3.12",
            work_dir=tmp_path / "work",
            library=tmp_path / "library.whl",
            contract=tmp_path / "contract.whl",
            artifact_dir=evidence,
        )
    summary = json.loads((evidence / "py312" / "summary-recovery_before_completion.json").read_text())
    assert summary["scenario_error"]
    assert "broker start failed" in summary["scenario_error"]
    final_summary = json.loads((evidence / "py312" / "summary.json").read_text())
    assert final_summary["error"]


def test_format_pass_error_uses_exception_type_when_message_empty() -> None:
    class EmptyError(Exception):
        pass

    assert vwr._format_pass_error(EmptyError()) == "EmptyError"


def test_wait_budget_seconds_reserves_kill_reap(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(vwr.time, "monotonic", lambda: 100.0)
    assert vwr._wait_budget_seconds(110.0) == 5.0
