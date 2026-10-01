"""Run developer check stages and persist summaries."""

from __future__ import annotations

import json
import platform
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Literal

from scripts.dev_check.constants import (
    DEFAULT_ARTIFACT_ROOT,
    INTEGRATION_STAGE_BUDGET_SECONDS,
)
from scripts.dev_check.process_tree import run_bounded
from scripts.dev_check.stages import (
    FAST_STAGES,
    INTEGRATION_STAGES,
    StageOutcome,
    StageSpec,
    evaluate_pytest_stage,
    evaluate_tool_stage,
    nats_junit_path,
    outcome_to_json,
    write_stage_streams,
)

CheckMode = Literal["fast", "integration", "full"]


@dataclass(frozen=True)
class RunSummary:
    mode: CheckMode
    python_version: str
    platform: str
    started_at: float
    finished_at: float
    outcomes: tuple[StageOutcome, ...]
    artifact_dir: Path

    @property
    def ok(self) -> bool:
        return all(item.status == "passed" for item in self.outcomes)

    def to_dict(self) -> dict[str, object]:
        return {
            "mode": self.mode,
            "python_version": self.python_version,
            "platform": self.platform,
            "started_at": self.started_at,
            "finished_at": self.finished_at,
            "ok": self.ok,
            "stages": [outcome_to_json(item) for item in self.outcomes],
        }


def repo_root() -> Path:
    return Path(__file__).resolve().parents[2]


def require_gate_python() -> str:
    """Return the active CPython minor for gates; exit if the interpreter is unsupported."""
    if platform.python_implementation() != "CPython":
        raise SystemExit(
            "Developer checks require CPython; "
            f"got {platform.python_implementation()} ({sys.version})"
        )
    info = sys.version_info
    if info.releaselevel != "final":
        raise SystemExit(
            f"Prerelease interpreters are excluded from required gates: {sys.version}"
        )
    return f"{info.major}.{info.minor}"


def current_python_minor() -> str:
    """Alias for require_gate_python (active interpreter minor)."""
    return require_gate_python()


def stages_for_mode(mode: CheckMode) -> tuple[StageSpec, ...]:
    if mode == "fast":
        return FAST_STAGES
    if mode == "integration":
        return INTEGRATION_STAGES
    return FAST_STAGES + INTEGRATION_STAGES


def persist_summary(summary: RunSummary) -> Path:
    summary.artifact_dir.mkdir(parents=True, exist_ok=True)
    path = summary.artifact_dir / "run-summary.json"
    path.write_text(json.dumps(summary.to_dict(), indent=2), encoding="utf-8")
    return path


def _stage_env(run_dir: Path) -> dict[str, str]:
    log_dir = run_dir / "nats-logs"
    log_dir.mkdir(parents=True, exist_ok=True)
    return {
        "PYTHONNOUSERSITE": "1",
        "SUPERJOBS_NATS_LOG_DIR": str(log_dir),
    }


def run_stages(
    mode: CheckMode,
    *,
    artifact_dir: Path | None = None,
) -> RunSummary:
    root = repo_root()
    py_version = require_gate_python()

    run_dir = artifact_dir or (root / DEFAULT_ARTIFACT_ROOT / mode)
    run_dir.mkdir(parents=True, exist_ok=True)
    started_wall = time.time()
    started_mono = time.monotonic()
    deadline_mono = (
        started_mono + INTEGRATION_STAGE_BUDGET_SECONDS
        if mode in ("integration", "full")
        else None
    )
    outcomes: list[StageOutcome] = []

    for stage in stages_for_mode(mode):
        if deadline_mono is not None and time.monotonic() >= deadline_mono:
            outcomes.append(
                StageOutcome(
                    name=stage.name,
                    status="failed",
                    returncode=None,
                    duration_seconds=0.0,
                    stdout="",
                    stderr="",
                    details={"reason": "integration job budget exhausted before stage start"},
                )
            )
            break

        remaining = None
        if deadline_mono is not None:
            remaining = max(1.0, deadline_mono - time.monotonic())
        timeout = min(stage.timeout_seconds, remaining or stage.timeout_seconds)

        if stage.name == "nats-pytest":
            stale = nats_junit_path(run_dir)
            if stale.is_file():
                stale.unlink()

        argv = stage.build_argv(root, py_version, run_dir)
        stage_dir = run_dir / "stages" / stage.name
        result = run_bounded(argv, cwd=root, env=_stage_env(run_dir), timeout_seconds=timeout)

        junit_path = nats_junit_path(run_dir) if stage.name == "nats-pytest" else None
        if stage.kind == "pytest":
            outcome = evaluate_pytest_stage(
                stage=stage,
                result_returncode=result.returncode,
                timed_out=result.timed_out,
                interrupted=result.interrupted,
                stdout=result.stdout,
                stderr=result.stderr,
                duration_seconds=result.duration_seconds,
                junit_path=junit_path,
                cleanup_errors=result.cleanup_errors,
            )
        else:
            outcome = evaluate_tool_stage(
                stage=stage,
                result_returncode=result.returncode,
                timed_out=result.timed_out,
                interrupted=result.interrupted,
                stdout=result.stdout,
                stderr=result.stderr,
                duration_seconds=result.duration_seconds,
                cleanup_errors=result.cleanup_errors,
            )
        write_stage_streams(stage_dir, outcome)
        outcomes.append(outcome)
        if outcome.status != "passed":
            break

    finished_wall = time.time()
    summary = RunSummary(
        mode=mode,
        python_version=py_version,
        platform=sys.platform,
        started_at=started_wall,
        finished_at=finished_wall,
        outcomes=tuple(outcomes),
        artifact_dir=run_dir,
    )
    persist_summary(summary)
    return summary


def print_summary(summary: RunSummary) -> None:
    for item in summary.outcomes:
        print(
            f"[{item.status}] {item.name} "
            f"({item.duration_seconds:.1f}s, exit={item.returncode})"
        )
    print(f"Artifacts: {summary.artifact_dir}")
