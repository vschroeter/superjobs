"""Deterministic orchestration tests for scripts/dev_check (no full integration gate)."""

from __future__ import annotations

import ctypes
import json
import sys
from pathlib import Path
from unittest.mock import patch

import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]

from scripts.dev_check import process_tree  # noqa: E402
from scripts.dev_check.constants import INTEGRATION_STAGE_BUDGET_SECONDS  # noqa: E402
from scripts.dev_check.orchestrator import run_stages  # noqa: E402
from scripts.dev_check.stages import (  # noqa: E402
    INTEGRATION_STAGES,
    StageOutcome,
    StageSpec,
    assess_required_pytest_junit,
    broker_restart_argv,
    idle_outage_argv,
    contract_typing_argv,
    cross_program_argv,
    evaluate_pytest_stage,
    evaluate_tool_stage,
    integration_pytest_argv,
    nats_junit_path,
    parse_pytest_junit_skips,
    worker_recovery_argv,
)

def _descendant_pid_alive(child_pid: int) -> bool:
    if sys.platform == "win32":
        kernel = ctypes.WinDLL("kernel32", use_last_error=True)
        kernel.OpenProcess.restype = ctypes.c_void_p
        kernel.OpenProcess.argtypes = [ctypes.c_ulong, ctypes.c_int, ctypes.c_ulong]
        kernel.WaitForSingleObject.argtypes = [ctypes.c_void_p, ctypes.c_ulong]
        kernel.CloseHandle.argtypes = [ctypes.c_void_p]
        handle = kernel.OpenProcess(0x00100000, False, child_pid)
        alive = bool(handle) and kernel.WaitForSingleObject(handle, 0) == 258
        if handle:
            kernel.CloseHandle(handle)
        return alive
    status = Path(f"/proc/{child_pid}/status")
    if not status.is_file():
        return False
    return "State:\tZ" not in status.read_text(encoding="utf-8", errors="replace")


def _reap_leaked_descendant(marker: Path, child_pid: int | None) -> None:
    pid = child_pid
    if pid is None and marker.is_file():
        try:
            pid = int(json.loads(marker.read_text(encoding="utf-8"))["pid"])
        except (KeyError, TypeError, ValueError, json.JSONDecodeError):
            return
    if pid is not None and _descendant_pid_alive(pid):
        process_tree.kill_process_tree(pid)


def test_assess_required_pytest_junit_missing(tmp_path: Path) -> None:
    ok, details = assess_required_pytest_junit(tmp_path / "missing.xml")
    assert ok is False
    assert details["reason"] == "required junit report missing"


def test_assess_required_pytest_junit_malformed(tmp_path: Path) -> None:
    bad = tmp_path / "bad.xml"
    bad.write_text("not xml", encoding="utf-8")
    ok, details = assess_required_pytest_junit(bad)
    assert ok is False
    assert details["reason"] == "junit parse error"


def test_assess_required_pytest_junit_zero_cases(tmp_path: Path) -> None:
    junit = tmp_path / "empty.xml"
    junit.write_text(
        '<?xml version="1.0"?><testsuite></testsuite>',
        encoding="utf-8",
    )
    ok, details = assess_required_pytest_junit(junit)
    assert ok is False
    assert details["reason"] == "junit has zero testcases"


def test_parse_pytest_junit_skips_counts_skipped(tmp_path: Path) -> None:
    junit = tmp_path / "junit.xml"
    junit.write_text(
        """<?xml version="1.0" encoding="utf-8"?>
<testsuite>
  <testcase classname="x" name="ok"/>
  <testcase classname="x" name="skip"><skipped/></testcase>
</testsuite>""",
        encoding="utf-8",
    )
    assert parse_pytest_junit_skips(junit) == 1


def test_evaluate_pytest_stage_fails_on_skips(tmp_path: Path) -> None:
    junit = tmp_path / "nats-junit.xml"
    junit.write_text(
        """<?xml version="1.0" encoding="utf-8"?>
<testsuite><testcase name="x"><skipped/></testcase></testsuite>""",
        encoding="utf-8",
    )
    stage = StageSpec("nats-pytest", "pytest", 30.0, lambda *_: [], "")
    outcome = evaluate_pytest_stage(
        stage=stage,
        result_returncode=0,
        timed_out=False,
        interrupted=False,
        stdout="",
        stderr="",
        duration_seconds=1.0,
        junit_path=junit,
        cleanup_errors=(),
    )
    assert outcome.status == "failed"
    assert outcome.details.get("skipped_tests") == 1


def test_evaluate_pytest_stage_fails_on_cleanup_errors() -> None:
    stage = StageSpec("deterministic-pytest", "pytest", 1.0, lambda *_: [], "")
    outcome = evaluate_pytest_stage(
        stage=stage,
        result_returncode=0,
        timed_out=False,
        interrupted=False,
        stdout="",
        stderr="",
        duration_seconds=0.1,
        junit_path=None,
        cleanup_errors=("communicate after kill timed out",),
    )
    assert outcome.status == "failed"
    assert outcome.details.get("reason") == "subprocess cleanup errors"


def test_evaluate_tool_stage_timeout() -> None:
    stage = StageSpec("tool", "tool", 1.0, lambda *_: [], "")
    outcome = evaluate_tool_stage(
        stage=stage,
        result_returncode=None,
        timed_out=True,
        interrupted=False,
        stdout="",
        stderr="",
        duration_seconds=2.0,
        cleanup_errors=("cleanup",),
    )
    assert outcome.status == "timeout"
    assert outcome.cleanup_errors == ("cleanup",)


def test_evaluate_tool_stage_fails_on_cleanup_errors() -> None:
    stage = StageSpec("tool", "tool", 1.0, lambda *_: [], "")
    outcome = evaluate_tool_stage(
        stage=stage,
        result_returncode=0,
        timed_out=False,
        interrupted=False,
        stdout="",
        stderr="",
        duration_seconds=0.1,
        cleanup_errors=("taskkill timed out",),
    )
    assert outcome.status == "failed"


def test_run_stages_stops_after_failure(tmp_path: Path) -> None:
    failing = StageOutcome(
        name="deterministic-pytest",
        status="failed",
        returncode=1,
        duration_seconds=0.1,
        stdout="",
        stderr="boom",
    )
    passed = StageOutcome(
        name="should-not-run",
        status="passed",
        returncode=0,
        duration_seconds=0.0,
        stdout="",
        stderr="",
    )

    def fake_bounded(argv, *, cwd, env, timeout_seconds):  # type: ignore[no-untyped-def]
        from scripts.dev_check.process_tree import StageProcessResult

        return StageProcessResult(
            returncode=1,
            timed_out=False,
            stdout="",
            stderr="boom",
            duration_seconds=0.1,
            pid=1234,
        )

    with patch("scripts.dev_check.orchestrator.run_bounded", fake_bounded):
        with patch(
            "scripts.dev_check.orchestrator.evaluate_pytest_stage",
            return_value=failing,
        ):
            with patch(
                "scripts.dev_check.orchestrator.evaluate_tool_stage",
                return_value=passed,
            ):
                summary = run_stages("fast", artifact_dir=tmp_path / "run")
    assert not summary.ok
    assert len(summary.outcomes) == 1
    summary_path = tmp_path / "run" / "run-summary.json"
    assert summary_path.is_file()
    payload = json.loads(summary_path.read_text(encoding="utf-8"))
    assert payload["ok"] is False


def test_run_bounded_kills_detached_descendants(tmp_path: Path) -> None:
    marker = tmp_path / "child.json"
    child_code = (
        "import json,os,sys,time; from pathlib import Path; "
        "Path(sys.argv[1]).write_text(json.dumps({'pid':os.getpid()})); "
        "time.sleep(120)"
    )
    parent_code = (
        "import subprocess,sys,time; "
        "subprocess.Popen([sys.executable,'-c',sys.argv[1],sys.argv[2]],"
        "start_new_session=True); time.sleep(120)"
    )
    child_pid: int | None = None
    try:
        result = process_tree.run_bounded(
            [sys.executable, "-c", parent_code, child_code, str(marker)],
            cwd=tmp_path,
            env=None,
            timeout_seconds=2.0,
        )
        assert result.timed_out is True
        assert marker.is_file(), "Child never reached readiness checkpoint"
        child_pid = int(json.loads(marker.read_text(encoding="utf-8"))["pid"])
        alive = _descendant_pid_alive(child_pid)
        assert not alive, f"Detached descendant {child_pid} survived timeout cleanup"
        assert not result.cleanup_errors, result.cleanup_errors
    finally:
        _reap_leaked_descendant(marker, child_pid)


def test_run_bounded_missing_command_returns_evidence(tmp_path: Path) -> None:
    result = process_tree.run_bounded(
        ["definitely-not-a-real-command-xyz"],
        cwd=tmp_path,
        env=None,
        timeout_seconds=1.0,
    )
    assert result.returncode == 127
    assert result.timed_out is False
    assert result.stderr


def test_integration_stages_include_idle_outage() -> None:
    names = [stage.name for stage in INTEGRATION_STAGES]
    assert names == [
        "contract-typing",
        "nats-pytest",
        "cross-program",
        "worker-recovery",
        "broker-restart",
        "idle-outage",
    ]


def test_integration_argv_uses_custom_artifact_dir(tmp_path: Path) -> None:
    run_dir = tmp_path / "artifacts"
    junit = integration_pytest_argv(REPO_ROOT, "3.12", run_dir)
    assert str(nats_junit_path(run_dir)) in " ".join(junit)
    cross = cross_program_argv(REPO_ROOT, "3.12", run_dir)
    assert str(run_dir / "cross-program") in " ".join(cross)
    typing = contract_typing_argv(REPO_ROOT, "3.12", run_dir)
    assert str(run_dir / "contract-typing") in " ".join(typing)
    recovery = worker_recovery_argv(REPO_ROOT, "3.12", run_dir)
    assert str(run_dir / "worker-recovery") in " ".join(recovery)
    restart = broker_restart_argv(REPO_ROOT, "3.12", run_dir)
    assert str(run_dir / "broker-restart") in " ".join(restart)
    idle = idle_outage_argv(REPO_ROOT, "3.12", run_dir)
    assert str(run_dir / "idle-outage") in " ".join(idle)


def test_run_stages_sets_nats_log_dir(tmp_path: Path) -> None:
    captured_env: dict[str, str] = {}

    def fake_bounded(argv, *, cwd, env, timeout_seconds):  # type: ignore[no-untyped-def]
        captured_env.update(env or {})
        from scripts.dev_check.process_tree import StageProcessResult

        return StageProcessResult(
            returncode=0,
            timed_out=False,
            stdout="",
            stderr="",
            duration_seconds=0.01,
            pid=1,
        )

    with patch("scripts.dev_check.orchestrator.run_bounded", fake_bounded):
        with patch(
            "scripts.dev_check.orchestrator.evaluate_pytest_stage",
            return_value=StageOutcome(
                name="deterministic-pytest",
                status="passed",
                returncode=0,
                duration_seconds=0.0,
                stdout="",
                stderr="",
            ),
        ):
            run_dir = tmp_path / "env-run"
            run_stages("fast", artifact_dir=run_dir)
    assert captured_env.get("SUPERJOBS_NATS_LOG_DIR") == str(run_dir / "nats-logs")


def test_run_stages_unlinks_stale_nats_junit(tmp_path: Path) -> None:
    run_dir = tmp_path / "integration-run"
    run_dir.mkdir()
    stale = nats_junit_path(run_dir)
    stale.write_text("<stale/>", encoding="utf-8")

    def fake_bounded(argv, *, cwd, env, timeout_seconds):  # type: ignore[no-untyped-def]
        from scripts.dev_check.process_tree import StageProcessResult

        return StageProcessResult(
            returncode=0,
            timed_out=False,
            stdout="",
            stderr="",
            duration_seconds=0.01,
            pid=1,
        )

    with patch("scripts.dev_check.orchestrator.run_bounded", fake_bounded):
        with patch(
            "scripts.dev_check.orchestrator.evaluate_pytest_stage",
            return_value=StageOutcome(
                name="nats-pytest",
                status="passed",
                returncode=0,
                duration_seconds=0.0,
                stdout="",
                stderr="",
            ),
        ):
            with patch(
                "scripts.dev_check.orchestrator.evaluate_tool_stage",
                return_value=StageOutcome(
                    name="contract-typing",
                    status="passed",
                    returncode=0,
                    duration_seconds=0.0,
                    stdout="",
                    stderr="",
                ),
            ):
                run_stages("integration", artifact_dir=run_dir)
    assert not stale.is_file()


def test_integration_budget_uses_monotonic_not_wall_clock(tmp_path: Path) -> None:
    timeouts: list[float] = []
    mono_sequence = [
        0.0,
        0.0,
        0.0,
        INTEGRATION_STAGE_BUDGET_SECONDS + 5.0,
    ]
    mono_iter = iter(mono_sequence)

    def fake_bounded(argv, *, cwd, env, timeout_seconds):  # type: ignore[no-untyped-def]
        timeouts.append(timeout_seconds)
        from scripts.dev_check.process_tree import StageProcessResult

        return StageProcessResult(
            returncode=0,
            timed_out=False,
            stdout="",
            stderr="",
            duration_seconds=0.01,
            pid=1,
        )

    def fake_monotonic() -> float:
        try:
            return next(mono_iter)
        except StopIteration:
            return mono_sequence[-1]

    passed = StageOutcome(
        name="x",
        status="passed",
        returncode=0,
        duration_seconds=0.0,
        stdout="",
        stderr="",
    )

    with patch("scripts.dev_check.orchestrator.run_bounded", fake_bounded):
        with patch("scripts.dev_check.orchestrator.evaluate_pytest_stage", return_value=passed):
            with patch("scripts.dev_check.orchestrator.evaluate_tool_stage", return_value=passed):
                with patch("scripts.dev_check.orchestrator.time.monotonic", fake_monotonic):
                    with patch("scripts.dev_check.orchestrator.time.time", return_value=9999999999.0):
                        summary = run_stages("integration", artifact_dir=tmp_path / "budget")
    assert len(timeouts) == 1
    assert summary.outcomes[-1].status == "failed"
    assert "budget exhausted" in str(summary.outcomes[-1].details.get("reason", ""))


def test_cli_fast_invokes_pytest_marker(tmp_path: Path) -> None:
    calls: list[list[str]] = []

    def fake_bounded(argv, *, cwd, env, timeout_seconds):  # type: ignore[no-untyped-def]
        calls.append(list(argv))
        from scripts.dev_check.process_tree import StageProcessResult

        return StageProcessResult(
            returncode=0,
            timed_out=False,
            stdout="1 passed",
            stderr="",
            duration_seconds=0.01,
            pid=1,
        )

    from scripts.dev_check.cli import main as dev_check_main

    with patch("scripts.dev_check.orchestrator.run_bounded", fake_bounded):
        exit_code = dev_check_main(
            ["fast", "--artifact-dir", str(tmp_path / "cli-fast")],
        )
    assert exit_code == 0
    assert calls
    assert "-m" in calls[0]
    assert "not nats and not contract_typing" in calls[0]


def test_integration_argv_uses_contract_typing_runner_not_pytest_marker() -> None:
    argv = contract_typing_argv(REPO_ROOT, "3.12", REPO_ROOT / "dist" / "custom")
    assert "verify_contract_typing.py" in " ".join(argv)
    assert "-m" not in argv


def test_run_bounded_launch_oserror_returns_evidence(tmp_path: Path) -> None:
    with patch(
        "scripts.dev_check.process_tree.subprocess.Popen",
        side_effect=PermissionError("access denied"),
    ):
        result = process_tree.run_bounded(
            [sys.executable, "-c", "pass"],
            cwd=tmp_path,
            env=None,
            timeout_seconds=1.0,
        )
    assert result.returncode == 127
    assert "access denied" in result.stderr


def test_run_bounded_interrupt_returns_evidence(tmp_path: Path) -> None:
    class _InterruptedProcess:
        pid = 4242

        def communicate(self, timeout: float | None = None) -> tuple[str, str]:
            raise KeyboardInterrupt()

        def kill(self) -> None:
            return None

    with patch(
        "scripts.dev_check.process_tree.subprocess.Popen",
        return_value=_InterruptedProcess(),
    ):
        with patch("scripts.dev_check.process_tree.kill_process_tree", return_value=[]):
            result = process_tree.run_bounded(
                [sys.executable, "-c", "pass"],
                cwd=tmp_path,
                env=None,
                timeout_seconds=30.0,
            )
    assert result.interrupted is True
    assert result.stderr


def test_run_stages_persists_summary_on_interrupt(tmp_path: Path) -> None:
    def fake_bounded(argv, *, cwd, env, timeout_seconds):  # type: ignore[no-untyped-def]
        from scripts.dev_check.process_tree import StageProcessResult

        return StageProcessResult(
            returncode=None,
            timed_out=False,
            stdout="",
            stderr="stopped",
            duration_seconds=0.1,
            pid=1,
            interrupted=True,
        )

    with patch("scripts.dev_check.orchestrator.run_bounded", fake_bounded):
        summary = run_stages("fast", artifact_dir=tmp_path / "interrupt-run")
    assert not summary.ok
    assert summary.outcomes[0].details.get("reason") == "interrupted"
    assert (tmp_path / "interrupt-run" / "run-summary.json").is_file()


def test_require_gate_python_rejects_prerelease() -> None:
    from scripts.dev_check.orchestrator import require_gate_python

    fake_info = type(
        "VI",
        (),
        {"major": 3, "minor": 12, "releaselevel": "beta", "serial": 0},
    )()
    with patch("scripts.dev_check.orchestrator.sys.version_info", fake_info):
        with patch(
            "scripts.dev_check.orchestrator.platform.python_implementation",
            return_value="CPython",
        ):
            with pytest.raises(SystemExit):
                require_gate_python()


def test_require_gate_python_rejects_non_cpython() -> None:
    from scripts.dev_check.orchestrator import require_gate_python

    with patch(
        "scripts.dev_check.orchestrator.platform.python_implementation",
        return_value="PyPy",
    ):
        with pytest.raises(SystemExit):
            require_gate_python()


def test_persist_summary_written_on_timeout(tmp_path: Path) -> None:
    def slow_bounded(argv, *, cwd, env, timeout_seconds):  # type: ignore[no-untyped-def]
        from scripts.dev_check.process_tree import StageProcessResult

        return StageProcessResult(
            returncode=None,
            timed_out=True,
            stdout="",
            stderr="",
            duration_seconds=timeout_seconds,
            pid=99,
            cleanup_errors=(),
        )

    with patch("scripts.dev_check.orchestrator.run_bounded", slow_bounded):
        summary = run_stages("fast", artifact_dir=tmp_path / "timeout-run")
    assert summary.outcomes[0].status == "timeout"
    assert (tmp_path / "timeout-run" / "run-summary.json").is_file()
