"""Stage definitions for developer check orchestration."""

from __future__ import annotations

import json
import sys
import xml.etree.ElementTree as ET
from dataclasses import dataclass, field
from pathlib import Path
from typing import Callable, Literal

from scripts.dev_check.constants import (
    FAST_PYTEST_MARKER,
    INTEGRATION_JOB_BUDGET_SECONDS,
    INTEGRATION_PYTEST_MARKER,
)

StageKind = Literal["pytest", "tool"]


@dataclass(frozen=True)
class StageSpec:
    name: str
    kind: StageKind
    timeout_seconds: float
    build_argv: Callable[[Path, str, Path], list[str]]
    description: str


def nats_junit_path(run_dir: Path) -> Path:
    return run_dir / "nats-junit.xml"


def _python_tool_argv(repo_root: Path, rel_script: str, *extra: str) -> list[str]:
    script = repo_root / rel_script
    return [sys.executable, str(script), *extra]


def fast_pytest_argv(repo_root: Path, _python_version: str, _run_dir: Path) -> list[str]:
    return [
        sys.executable,
        "-m",
        "pytest",
        "-m",
        FAST_PYTEST_MARKER,
        "-q",
        "--strict-markers",
    ]


def integration_pytest_argv(repo_root: Path, _python_version: str, run_dir: Path) -> list[str]:
    junit = nats_junit_path(run_dir)
    junit.parent.mkdir(parents=True, exist_ok=True)
    return [
        sys.executable,
        "-m",
        "pytest",
        "-m",
        INTEGRATION_PYTEST_MARKER,
        "-q",
        "--strict-markers",
        f"--junitxml={junit}",
    ]


def contract_typing_argv(repo_root: Path, python_version: str, run_dir: Path) -> list[str]:
    evidence_parent = run_dir / "contract-typing"
    evidence_parent.mkdir(parents=True, exist_ok=True)
    return _python_tool_argv(
        repo_root,
        "tools/verify_contract_typing.py",
        "--mode",
        "both",
        "--python",
        python_version,
        "--evidence",
        str(evidence_parent),
    )


def cross_program_argv(repo_root: Path, python_version: str, run_dir: Path) -> list[str]:
    artifact = run_dir / "cross-program"
    artifact.mkdir(parents=True, exist_ok=True)
    return _python_tool_argv(
        repo_root,
        "tools/verify_cross_program.py",
        "--python",
        python_version,
        "--artifact-dir",
        str(artifact),
    )


def worker_recovery_argv(repo_root: Path, python_version: str, run_dir: Path) -> list[str]:
    artifact = run_dir / "worker-recovery"
    artifact.mkdir(parents=True, exist_ok=True)
    return _python_tool_argv(
        repo_root,
        "tools/verify_worker_recovery.py",
        "--python",
        python_version,
        "--artifact-dir",
        str(artifact),
    )


def broker_restart_argv(repo_root: Path, python_version: str, run_dir: Path) -> list[str]:
    artifact = run_dir / "broker-restart"
    artifact.mkdir(parents=True, exist_ok=True)
    return _python_tool_argv(
        repo_root,
        "tools/verify_broker_restart.py",
        "--python",
        python_version,
        "--artifact-dir",
        str(artifact),
    )


def idle_outage_argv(repo_root: Path, python_version: str, run_dir: Path) -> list[str]:
    artifact = run_dir / "idle-outage"
    artifact.mkdir(parents=True, exist_ok=True)
    return _python_tool_argv(
        repo_root,
        "tools/verify_idle_outage.py",
        "--python",
        python_version,
        "--artifact-dir",
        str(artifact),
    )


def cli_process_argv(repo_root: Path, python_version: str, run_dir: Path) -> list[str]:
    artifact = run_dir / "cli-process"
    artifact.mkdir(parents=True, exist_ok=True)
    return _python_tool_argv(
        repo_root,
        "tools/verify_cli_process.py",
        "--python",
        python_version,
        "--artifact-dir",
        str(artifact),
    )


FAST_STAGES: tuple[StageSpec, ...] = (
    StageSpec(
        name="deterministic-pytest",
        kind="pytest",
        timeout_seconds=600.0,
        build_argv=fast_pytest_argv,
        description="Routine deterministic tests without NATS or contract_typing pytest gate.",
    ),
)

_TOOL_STAGE_TIMEOUT = float(INTEGRATION_JOB_BUDGET_SECONDS)

INTEGRATION_STAGES: tuple[StageSpec, ...] = (
    StageSpec(
        name="contract-typing",
        kind="tool",
        timeout_seconds=_TOOL_STAGE_TIMEOUT,
        build_argv=contract_typing_argv,
        description="Pinned Pyright source/wheel consumers and installed runtime checks.",
    ),
    StageSpec(
        name="nats-pytest",
        kind="pytest",
        timeout_seconds=600.0,
        build_argv=integration_pytest_argv,
        description="Real NATS JetStream tests via the owned harness (no skips).",
    ),
    StageSpec(
        name="cross-program",
        kind="tool",
        timeout_seconds=_TOOL_STAGE_TIMEOUT,
        build_argv=cross_program_argv,
        description="Installed producer/worker verification in separate OS processes.",
    ),
    StageSpec(
        name="worker-recovery",
        kind="tool",
        timeout_seconds=_TOOL_STAGE_TIMEOUT,
        build_argv=worker_recovery_argv,
        description="Worker crash recovery before and after durable completion.",
    ),
    StageSpec(
        name="broker-restart",
        kind="tool",
        timeout_seconds=_TOOL_STAGE_TIMEOUT,
        build_argv=broker_restart_argv,
        description="Persistent-store broker restart with fresh installed applications.",
    ),
    StageSpec(
        name="idle-outage",
        kind="tool",
        timeout_seconds=_TOOL_STAGE_TIMEOUT,
        build_argv=idle_outage_argv,
        description="Idle installed producer/worker survive a short broker outage.",
    ),
    StageSpec(
        name="cli-process",
        kind="tool",
        timeout_seconds=_TOOL_STAGE_TIMEOUT,
        build_argv=cli_process_argv,
        description="Installed contract-interface CLI local run and NATS submit verification.",
    ),
)


@dataclass
class StageOutcome:
    name: str
    status: Literal["passed", "failed", "skipped", "timeout"]
    returncode: int | None
    duration_seconds: float
    stdout: str
    stderr: str
    details: dict[str, object] = field(default_factory=dict)
    cleanup_errors: tuple[str, ...] = ()


def assess_required_pytest_junit(junit_path: Path) -> tuple[bool, dict[str, object]]:
    if not junit_path.is_file():
        return False, {"reason": "required junit report missing", "junit_path": str(junit_path)}
    try:
        root = ET.parse(junit_path).getroot()
    except ET.ParseError as exc:
        return False, {"reason": "junit parse error", "error": str(exc), "junit_path": str(junit_path)}

    cases = list(root.iter("testcase"))
    if not cases:
        return False, {"reason": "junit has zero testcases", "junit_path": str(junit_path)}

    skipped = sum(1 for case in cases if case.find("skipped") is not None)
    errors = sum(1 for case in cases if case.find("error") is not None)
    failures = sum(1 for case in cases if case.find("failure") is not None)
    if skipped or errors or failures:
        return False, {
            "reason": "junit reports skipped, error, or failure nodes",
            "skipped_tests": skipped,
            "errors": errors,
            "failures": failures,
            "junit_path": str(junit_path),
        }
    return True, {"testcase_count": len(cases), "junit_path": str(junit_path)}


def parse_pytest_junit_skips(junit_path: Path) -> int:
    ok, details = assess_required_pytest_junit(junit_path)
    if ok:
        return 0
    return int(details.get("skipped_tests") or 0)


def _fail_on_cleanup_errors(outcome: StageOutcome) -> StageOutcome:
    if not outcome.cleanup_errors or outcome.status != "passed":
        return outcome
    details = dict(outcome.details)
    details["reason"] = "subprocess cleanup errors"
    return StageOutcome(
        name=outcome.name,
        status="failed",
        returncode=outcome.returncode,
        duration_seconds=outcome.duration_seconds,
        stdout=outcome.stdout,
        stderr=outcome.stderr,
        details=details,
        cleanup_errors=outcome.cleanup_errors,
    )


def evaluate_pytest_stage(
    *,
    stage: StageSpec,
    result_returncode: int | None,
    timed_out: bool,
    interrupted: bool,
    stdout: str,
    stderr: str,
    duration_seconds: float,
    junit_path: Path | None,
    cleanup_errors: tuple[str, ...],
) -> StageOutcome:
    if interrupted:
        return StageOutcome(
            name=stage.name,
            status="failed",
            returncode=result_returncode,
            duration_seconds=duration_seconds,
            stdout=stdout,
            stderr=stderr,
            cleanup_errors=cleanup_errors,
            details={"reason": "interrupted"},
        )
    if timed_out:
        return StageOutcome(
            name=stage.name,
            status="timeout",
            returncode=None,
            duration_seconds=duration_seconds,
            stdout=stdout,
            stderr=stderr,
            cleanup_errors=cleanup_errors,
            details={"reason": "stage wall-clock budget exceeded"},
        )
    if stage.name == "nats-pytest" and junit_path is not None:
        ok, details = assess_required_pytest_junit(junit_path)
        if not ok:
            return StageOutcome(
                name=stage.name,
                status="failed",
                returncode=result_returncode,
                duration_seconds=duration_seconds,
                stdout=stdout,
                stderr=stderr,
                cleanup_errors=cleanup_errors,
                details=details,
            )
    if result_returncode != 0:
        return StageOutcome(
            name=stage.name,
            status="failed",
            returncode=result_returncode,
            duration_seconds=duration_seconds,
            stdout=stdout,
            stderr=stderr,
            cleanup_errors=cleanup_errors,
        )
    return _fail_on_cleanup_errors(
        StageOutcome(
            name=stage.name,
            status="passed",
            returncode=result_returncode,
            duration_seconds=duration_seconds,
            stdout=stdout,
            stderr=stderr,
            cleanup_errors=cleanup_errors,
        )
    )


def evaluate_tool_stage(
    *,
    stage: StageSpec,
    result_returncode: int | None,
    timed_out: bool,
    interrupted: bool,
    stdout: str,
    stderr: str,
    duration_seconds: float,
    cleanup_errors: tuple[str, ...],
) -> StageOutcome:
    if interrupted:
        return StageOutcome(
            name=stage.name,
            status="failed",
            returncode=result_returncode,
            duration_seconds=duration_seconds,
            stdout=stdout,
            stderr=stderr,
            cleanup_errors=cleanup_errors,
            details={"reason": "interrupted"},
        )
    if timed_out:
        return StageOutcome(
            name=stage.name,
            status="timeout",
            returncode=None,
            duration_seconds=duration_seconds,
            stdout=stdout,
            stderr=stderr,
            cleanup_errors=cleanup_errors,
            details={"reason": "stage wall-clock budget exceeded"},
        )
    if result_returncode != 0:
        return StageOutcome(
            name=stage.name,
            status="failed",
            returncode=result_returncode,
            duration_seconds=duration_seconds,
            stdout=stdout,
            stderr=stderr,
            cleanup_errors=cleanup_errors,
        )
    return _fail_on_cleanup_errors(
        StageOutcome(
            name=stage.name,
            status="passed",
            returncode=result_returncode,
            duration_seconds=duration_seconds,
            stdout=stdout,
            stderr=stderr,
            cleanup_errors=cleanup_errors,
        )
    )


def outcome_to_json(outcome: StageOutcome) -> dict[str, object]:
    return {
        "name": outcome.name,
        "status": outcome.status,
        "returncode": outcome.returncode,
        "duration_seconds": outcome.duration_seconds,
        "details": outcome.details,
        "cleanup_errors": list(outcome.cleanup_errors),
        "stdout_tail": outcome.stdout[-4000:],
        "stderr_tail": outcome.stderr[-4000:],
    }


def write_stage_streams(stage_dir: Path, outcome: StageOutcome) -> None:
    stage_dir.mkdir(parents=True, exist_ok=True)
    (stage_dir / "stdout.txt").write_text(outcome.stdout, encoding="utf-8")
    (stage_dir / "stderr.txt").write_text(outcome.stderr, encoding="utf-8")
    (stage_dir / "stage.json").write_text(
        json.dumps(outcome_to_json(outcome), indent=2),
        encoding="utf-8",
    )
