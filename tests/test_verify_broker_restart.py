"""Deterministic checks for tools/verify_broker_restart.py."""

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
RUNNER = REPO_ROOT / "tools" / "verify_broker_restart.py"
SUPPORT = REPO_ROOT / "tools" / "broker_restart_support"
REGRESSION = REPO_ROOT / "tests" / "support" / "broker_restart_regression"

from tools.broker_restart_support import broker_resources as br_resources  # noqa: E402
from tools.broker_restart_support import protocol as br_protocol  # noqa: E402
from tools.cross_program_support.child_env import isolated_child_env, python_command  # noqa: E402
from tools.verify_cross_program import ChildRecord  # noqa: E402
from tools import verify_broker_restart as vbr  # noqa: E402


def _materialize_regression_worker(base: Path, script_src: Path) -> Path:
    role_dir = base / "worker"
    role_dir.mkdir(parents=True, exist_ok=True)
    for name in vbr.WORKER_SUPPORT_FILES:
        shutil.copy2(SUPPORT / name, role_dir / name)
    shutil.copy2(script_src, role_dir / "worker_app.py")
    return role_dir


def test_invocation_evidence_includes_job_name(tmp_path: Path) -> None:
    run_id = uuid.uuid4().hex
    count = br_protocol.record_handler_invocation(
        tmp_path,
        run_id=run_id,
        worker_generation="1",
        job="broker_restart.completed.example",
        execution_id="exec-1",
    )
    assert count == 1
    items = br_protocol.read_invocations(tmp_path)
    assert items[0].job == "broker_restart.completed.example"


def test_assert_resources_nonempty_rejects_empty_work_stream() -> None:
    evidence = {
        "work_stream": {"storage": "file", "messages": 0},
        "observation_stream": {"storage": "file"},
        "completion_bucket": {"values": 1},
    }
    with pytest.raises(br_resources.BrokerResourceError, match="no persisted messages"):
        br_resources.assert_resources_nonempty(evidence)


@pytest.mark.parametrize(
    ("resource_key", "storage"),
    [
        ("work_stream", "memory"),
        ("observation_stream", "memory"),
        ("completion_bucket", "memory"),
        ("idempotency_bucket", "memory"),
    ],
)
def test_assert_persisted_file_storage_rejects_memory(resource_key: str, storage: str) -> None:
    evidence = {
        key: {"storage": "file"}
        for key in br_resources.PERSISTED_FILE_RESOURCE_KEYS
    }
    evidence[resource_key] = {"storage": storage}
    with pytest.raises(br_resources.BrokerResourceError, match="memory-only"):
        br_resources.assert_persisted_file_storage(evidence)


@pytest.mark.parametrize("resource_key", br_resources.PERSISTED_FILE_RESOURCE_KEYS)
def test_assert_persisted_file_storage_rejects_missing_section(resource_key: str) -> None:
    evidence = {
        key: {"storage": "file"}
        for key in br_resources.PERSISTED_FILE_RESOURCE_KEYS
        if key != resource_key
    }
    with pytest.raises(br_resources.BrokerResourceError, match=f"missing {resource_key}"):
        br_resources.assert_persisted_file_storage(evidence)


def test_empty_store_fingerprint_rejected() -> None:
    with pytest.raises(vbr.BrokerRestartError, match="empty or missing store"):
        vbr._assert_nonempty_store_fingerprint("files=0:bytes=0", label="negative-control")


def test_broker_restart_invocations_require_single_gen_per_execution() -> None:
    run_id = "run"
    completed_id = "completed"
    pending_id = "pending"
    invocations = [
        br_protocol.HandlerInvocation(
            run_id=run_id,
            worker_generation="1",
            job="j1",
            execution_id=completed_id,
            pid=10,
        ),
        br_protocol.HandlerInvocation(
            run_id=run_id,
            worker_generation="2",
            job="j2",
            execution_id=pending_id,
            pid=20,
        ),
    ]
    br_protocol.assert_broker_restart_invocations(
        invocations,
        run_id=run_id,
        completed_execution_id=completed_id,
        pending_execution_id=pending_id,
        allowed_pids_by_generation={"1": 10, "2": 20},
    )
    with pytest.raises(br_protocol.ProtocolError, match="expected exactly two"):
        br_protocol.assert_broker_restart_invocations(
            invocations + [
                br_protocol.HandlerInvocation(
                    run_id=run_id,
                    worker_generation="2",
                    job="j1",
                    execution_id=completed_id,
                    pid=99,
                ),
            ],
            run_id=run_id,
            completed_execution_id=completed_id,
            pending_execution_id=pending_id,
            allowed_pids_by_generation={"1": 10, "2": 20},
        )


def test_scenario_worker_crash_before_ready(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(vbr, "SCENARIO_TIMEOUT_SECONDS", 4.0)
    monkeypatch.setattr(vbr, "CHILD_TIMEOUT_SECONDS", 2.0)
    monkeypatch.setattr(vbr, "KILL_REAP_SECONDS", 0.3)
    worker_dir = _materialize_regression_worker(tmp_path, REGRESSION / "worker_never_ready.py")
    producer_dir = vbr.materialize_role_dir(tmp_path / "roles", "producer", vbr.PRODUCER_SUPPORT_FILES)
    owner = SimpleNamespace(
        restart=lambda target: target,
    )
    record = vbr._run_broker_restart_scenario(
        owner=owner,
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
    assert record.workers
    assert record.workers[0].exit_code == 42


def test_scenario_producer_hang_is_reaped(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(vbr, "SCENARIO_TIMEOUT_SECONDS", 5.0)
    monkeypatch.setattr(vbr, "CHILD_TIMEOUT_SECONDS", 2.0)
    monkeypatch.setattr(vbr, "KILL_REAP_SECONDS", 0.3)
    worker_dir = _materialize_regression_worker(
        tmp_path,
        REPO_ROOT / "tests" / "support" / "worker_recovery_regression" / "worker_ready_only.py",
    )
    producer_dir = vbr.materialize_role_dir(tmp_path / "roles", "producer", vbr.PRODUCER_SUPPORT_FILES)
    shutil.copy2(REGRESSION / "producer_hang.py", producer_dir / "producer_app.py")
    owner = SimpleNamespace(restart=lambda target: target)
    record = vbr._run_broker_restart_scenario(
        owner=owner,
        nats_url="nats://127.0.0.1:4222",
        broker_target=SimpleNamespace(work_dir=tmp_path / "broker", process=None),
        producer_python=Path(sys.executable),
        worker_python=Path(sys.executable),
        producer_dir=producer_dir,
        worker_dir=worker_dir,
        artifact_scenario_dir=tmp_path / "artifacts",
        scenario_deadline=time.monotonic() + 5.0,
    )
    assert record.error is not None
    assert record.producers
    assert record.producers[0].exit_code is not None


def test_broker_start_failure_writes_summary(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    class FailStartOwner:
        target = None

        def start(self, *, deadline=None):
            raise RuntimeError("broker start failed")

        def stop(self, target, *, deadline=None):
            return None

    monkeypatch.setattr(vbr, "OwnedNatsServer", FailStartOwner)
    monkeypatch.setattr(vbr, "create_wheel_venv", lambda *args, **kwargs: tmp_path)
    monkeypatch.setattr(vbr, "assert_final_runtime_python", lambda *args: "3.12")
    monkeypatch.setattr(vbr, "probe_restart_origins", lambda *args, **kwargs: {})
    evidence = tmp_path / "evidence"
    with pytest.raises(vbr.BrokerRestartError, match="broker start failed"):
        vbr.verify_python_version(
            python_version="3.12",
            work_dir=tmp_path / "work",
            library=tmp_path / "library.whl",
            contract=tmp_path / "contract.whl",
            artifact_dir=evidence,
        )
    summary = json.loads((evidence / "py312" / "summary.json").read_text(encoding="utf-8"))
    assert summary["scenario_error"]
    assert "broker start failed" in summary["scenario_error"]


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
    assert "--empty-store-control" in completed.stdout


def test_scenario_rejects_stale_ready_pid(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(vbr, "SCENARIO_TIMEOUT_SECONDS", 4.0)
    monkeypatch.setattr(vbr, "CHILD_TIMEOUT_SECONDS", 2.0)
    monkeypatch.setattr(vbr, "KILL_REAP_SECONDS", 0.3)
    worker_dir = _materialize_regression_worker(tmp_path, REGRESSION / "stale_ready_worker.py")
    producer_dir = vbr.materialize_role_dir(tmp_path / "roles", "producer", vbr.PRODUCER_SUPPORT_FILES)
    owner = SimpleNamespace(restart=lambda target: target)
    record = vbr._run_broker_restart_scenario(
        owner=owner,
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
    assert "generation" in record.error.lower()


def test_scenario_producer_crash_is_reaped(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(vbr, "SCENARIO_TIMEOUT_SECONDS", 5.0)
    monkeypatch.setattr(vbr, "CHILD_TIMEOUT_SECONDS", 2.0)
    monkeypatch.setattr(vbr, "KILL_REAP_SECONDS", 0.3)
    worker_dir = _materialize_regression_worker(
        tmp_path,
        REPO_ROOT / "tests" / "support" / "worker_recovery_regression" / "worker_ready_only.py",
    )
    producer_dir = vbr.materialize_role_dir(tmp_path / "roles", "producer", vbr.PRODUCER_SUPPORT_FILES)
    shutil.copy2(
        REPO_ROOT / "tests" / "support" / "worker_recovery_regression" / "producer_crash.py",
        producer_dir / "producer_app.py",
    )
    owner = SimpleNamespace(restart=lambda target: target)
    record = vbr._run_broker_restart_scenario(
        owner=owner,
        nats_url="nats://127.0.0.1:4222",
        broker_target=SimpleNamespace(work_dir=tmp_path / "broker", process=None),
        producer_python=Path(sys.executable),
        worker_python=Path(sys.executable),
        producer_dir=producer_dir,
        worker_dir=worker_dir,
        artifact_scenario_dir=tmp_path / "artifacts",
        scenario_deadline=time.monotonic() + 5.0,
    )
    assert record.error is not None
    assert record.producers
    assert record.producers[0].exit_code not in (None, 0)


def test_broker_cleanup_failure_fails_pass(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    log = tmp_path / "broker.log"
    log.write_text("broker diagnostics", encoding="utf-8")

    class BrokenCleanupOwner:
        def __init__(self):
            self.target = SimpleNamespace(log_path=log, work_dir=tmp_path / "store", url="nats://x")

        def start(self, *, deadline=None):
            (tmp_path / "store").mkdir()
            return self.target

        def stop(self, target, *, deadline=None):
            raise RuntimeError("cleanup broke")

    monkeypatch.setattr(vbr, "OwnedNatsServer", BrokenCleanupOwner)
    monkeypatch.setattr(vbr, "create_wheel_venv", lambda *args, **kwargs: tmp_path)
    monkeypatch.setattr(vbr, "assert_final_runtime_python", lambda *args: "3.12")
    monkeypatch.setattr(vbr, "probe_restart_origins", lambda *args, **kwargs: {})
    evidence = tmp_path / "evidence"
    with pytest.raises(vbr.BrokerRestartError, match="cleanup broke"):
        vbr.verify_python_version(
            python_version="3.12",
            work_dir=tmp_path / "work",
            library=tmp_path / "library.whl",
            contract=tmp_path / "contract.whl",
            artifact_dir=evidence,
        )
    summary = json.loads((evidence / "py312" / "summary.json").read_text(encoding="utf-8"))
    assert "cleanup broke" in summary["error"]
    assert "broker diagnostics" in summary["broker_log"]


def test_origin_probe_failure_writes_summary(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    class NoopOwner:
        target = None

        def start(self, *, deadline=None):
            raise RuntimeError("should not start")

        def stop(self, target, *, deadline=None):
            return None

    monkeypatch.setattr(vbr, "OwnedNatsServer", NoopOwner)
    monkeypatch.setattr(vbr, "create_wheel_venv", lambda *args, **kwargs: tmp_path)
    monkeypatch.setattr(vbr, "assert_final_runtime_python", lambda *args: "3.12")

    def fail_probe(*args, **kwargs):
        raise RuntimeError("origin probe failed")

    monkeypatch.setattr(vbr, "probe_restart_origins", fail_probe)
    evidence = tmp_path / "evidence"
    with pytest.raises(vbr.BrokerRestartError, match="origin probe failed"):
        vbr.verify_python_version(
            python_version="3.12",
            work_dir=tmp_path / "work",
            library=tmp_path / "library.whl",
            contract=tmp_path / "contract.whl",
            artifact_dir=evidence,
        )
    summary = json.loads((evidence / "py312" / "summary.json").read_text(encoding="utf-8"))
    assert "origin probe failed" in summary["scenario_error"]


def test_assert_pending_work_retained_requires_exact_work_subject() -> None:
    execution_id = "exec-pending"
    subject = "br_run.work.broker_restart.pending.run"
    evidence = {
        "work_messages": [
            {
                "seq": 2,
                "subject": "br_run.work.other.job",
                "job_id": execution_id,
            },
        ],
    }
    with pytest.raises(br_resources.BrokerResourceError, match="work subject"):
        br_resources.assert_pending_work_retained(
            evidence,
            pending_execution_id=execution_id,
            pending_work_subject=subject,
        )
    br_resources.assert_pending_work_retained(
        evidence | {
            "work_messages": [
                {"seq": 2, "subject": subject, "job_id": execution_id, "version": "v1"},
            ],
        },
        pending_execution_id=execution_id,
        pending_work_subject=subject,
    )


def test_empty_store_control_operational_failure_fails_pass(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class NoopOwner:
        target = SimpleNamespace(
            log_path=tmp_path / "broker.log",
            work_dir=tmp_path / "broker",
            url="nats://127.0.0.1:4222",
            process=SimpleNamespace(pid=1, poll=lambda: 0),
            owned=True,
        )

        def start(self, *, deadline=None):
            (tmp_path / "broker").mkdir(parents=True, exist_ok=True)
            self.target.log_path.write_text("", encoding="utf-8")
            return self.target

        def stop(self, target, *, deadline=None):
            raise RuntimeError("cleanup broke")

    def fake_scenario(**kwargs):
        return vbr.BrokerRestartScenarioRecord(
            name=vbr.SCENARIO_NAME,
            run_id="run",
            state_dir=tmp_path,
            broker_store_dir=tmp_path / "store",
            broker_store_fingerprint_before="files=1:bytes=1",
            broker_store_fingerprint_after_restart="",
            broker_store_fingerprint_final="",
            started_at=0.0,
            empty_store_control_detected=True,
            control_oracle_error="work stream missing",
            error="cleanup: child cleanup failed",
        )

    monkeypatch.setattr(vbr, "OwnedNatsServer", NoopOwner)
    monkeypatch.setattr(vbr, "create_wheel_venv", lambda *args, **kwargs: tmp_path)
    monkeypatch.setattr(vbr, "assert_final_runtime_python", lambda *args: "3.12")
    monkeypatch.setattr(vbr, "probe_restart_origins", lambda *args, **kwargs: {})
    monkeypatch.setattr(vbr, "_run_broker_restart_scenario", fake_scenario)
    evidence = tmp_path / "evidence"
    with pytest.raises(vbr.BrokerRestartError, match="child cleanup failed"):
        vbr.verify_python_version(
            python_version="3.12",
            work_dir=tmp_path / "work",
            library=tmp_path / "library.whl",
            contract=tmp_path / "contract.whl",
            artifact_dir=evidence,
            empty_store_control=True,
        )


def test_scenario_timeout_retains_summary(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    class SlowStartOwner:
        target = None

        def start(self, *, deadline=None):
            raise TimeoutError("broker startup exceeded budget")

        def stop(self, target, *, deadline=None):
            return None

    monkeypatch.setattr(vbr, "OwnedNatsServer", SlowStartOwner)
    monkeypatch.setattr(vbr, "create_wheel_venv", lambda *args, **kwargs: tmp_path)
    monkeypatch.setattr(vbr, "assert_final_runtime_python", lambda *args: "3.12")
    monkeypatch.setattr(vbr, "probe_restart_origins", lambda *args, **kwargs: {})
    evidence = tmp_path / "evidence"
    with pytest.raises(vbr.BrokerRestartError, match="broker startup exceeded"):
        vbr.verify_python_version(
            python_version="3.12",
            work_dir=tmp_path / "work",
            library=tmp_path / "library.whl",
            contract=tmp_path / "contract.whl",
            artifact_dir=evidence,
        )
    summary = json.loads((evidence / "py312" / "summary.json").read_text(encoding="utf-8"))
    assert summary.get("scenario_error")
    assert "broker startup exceeded" in summary["scenario_error"]


@pytest.mark.skipif(sys.platform != "win32", reason="Windows venv launcher semantics")
def test_pre_ready_worker_cleanup_uses_started_marker(tmp_path: Path) -> None:
    import venv

    venv_dir = tmp_path / "venv"
    venv.EnvBuilder(with_pip=False).create(venv_dir)
    venv_python = venv_dir / "Scripts" / "python.exe"
    started_path = tmp_path / "worker-gen1.interpreter.pid"
    hang_script = tmp_path / "hang.py"
    hang_script.write_text("import time\n\ntime.sleep(120)\n", encoding="utf-8")
    env = isolated_child_env(
        {"SUPERJOBS_PROCESS_STARTED_PATH": str(started_path)},
    )
    command = python_command(str(venv_python), tmp_path, "hang.py")
    stdout_path = tmp_path / "out.log"
    stderr_path = tmp_path / "err.log"
    stdout_io = stdout_path.open("w", encoding="utf-8")
    stderr_io = stderr_path.open("w", encoding="utf-8")
    process = subprocess.Popen(
        command,
        cwd=tmp_path,
        env=env,
        stdout=stdout_io,
        stderr=stderr_io,
        creationflags=subprocess.CREATE_NO_WINDOW,
    )
    record = ChildRecord(
        name="worker-gen1",
        command=command,
        cwd=tmp_path,
        started_at=time.monotonic(),
        pid=process.pid,
    )
    tracked = vbr._TrackedChild(process, record, (stdout_io, stderr_io), started_path)
    deadline = time.monotonic() + 5
    try:
        startup_deadline = time.monotonic() + 5
        while not started_path.is_file() and time.monotonic() < startup_deadline:
            time.sleep(0.05)
        assert started_path.is_file()
        vbr._finalize_tracked_children([tracked], deadline=deadline)
        assert process.poll() is not None
        assert record.exit_code is not None
    finally:
        if process.poll() is None:
            process.kill()
            process.wait(timeout=5)


@pytest.mark.nats
def test_empty_store_control_native(tmp_path: Path) -> None:
    from tests.support.nats_harness.provision import resolve_executable

    resolve_executable()
    work_parent = tmp_path / "work"
    work_parent.mkdir()
    artifact_dir = tmp_path / "artifacts"
    completed = subprocess.run(
        [
            sys.executable,
            str(RUNNER),
            "--empty-store-control",
            "--python",
            "3.12",
            "--artifact-dir",
            str(artifact_dir),
            "--work-dir",
            str(work_parent),
        ],
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
        timeout=180,
        check=False,
        creationflags=subprocess.CREATE_NO_WINDOW if sys.platform == "win32" else 0,
    )
    assert completed.returncode == 0, completed.stderr
    summary = json.loads((artifact_dir / "py312" / "summary.json").read_text(encoding="utf-8"))
    scenario = summary["scenario"]
    assert scenario["empty_store_control_detected"] is True
    assert scenario.get("control_oracle_error")
    assert scenario["error"] is None
