#!/usr/bin/env python3
"""Bounded real-NATS loop reproducing manifest observation replay failures (issue #27)."""

from __future__ import annotations

import argparse
import json
import math
import os
import platform
import sys
import tempfile
import time
import uuid
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from scripts.dev_check.constants import INTEGRATION_JOB_BUDGET_SECONDS  # noqa: E402
from scripts.dev_check.process_tree import StageProcessResult, run_bounded  # noqa: E402
from tools.manifest_replay_repro_support.extract import (  # noqa: E402
    count_validation_event_failures,
    failure_rows_from_report,
)
from tools.manifest_replay_repro_support.report_paths import (  # noqa: E402
    list_run_dirs,
    resolve_fresh_diagnostic_report,
    write_attempt_streams,
)

CAPTURE_ENV = "SUPERJOBS_PERF_CAPTURE_REPLAY_EVIDENCE"
DEFAULT_ARTIFACT_ROOT = REPO_ROOT / "dist" / "verification" / "manifest-replay-repro"
PERFORMANCE_RUNNER = REPO_ROOT / "tools" / "verify_performance.py"
ISSUE_URL = "https://github.com/vschroeter/superjobs/issues/27"
EXIT_REPRODUCED = 2
EXIT_INFRA = 1
EXIT_HARNESS = 3


class ManifestReplayReproError(Exception):
    """Raised when the reproduction runner cannot complete."""


@dataclass
class AttemptOutcome:
    index: int
    status: str
    returncode: int | None
    duration_seconds: float
    diagnostic_report: Path | None = None
    validation_event_failures: int = 0
    failure_rows: list[dict[str, Any]] = field(default_factory=list)
    error: str | None = None
    timed_out: bool = False
    cleanup_errors: tuple[str, ...] = ()

    def to_dict(self) -> dict[str, Any]:
        return {
            "index": self.index,
            "status": self.status,
            "returncode": self.returncode,
            "duration_seconds": self.duration_seconds,
            "diagnostic_report": (
                str(self.diagnostic_report) if self.diagnostic_report is not None else None
            ),
            "validation_event_failures": self.validation_event_failures,
            "failure_rows": self.failure_rows,
            "error": self.error,
            "timed_out": self.timed_out,
            "cleanup_errors": list(self.cleanup_errors),
        }


def _load_report(path: Path) -> dict[str, Any]:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        raise ManifestReplayReproError(f"malformed diagnostic report at {path}: {exc}") from exc


def _performance_argv(
    *,
    python_version: str,
    artifact_dir: Path,
    skip_contract_typing: bool,
    work_parent: Path,
    report_date: str,
) -> list[str]:
    command = [
        "uv",
        "run",
        "--no-project",
        "--python",
        python_version,
        "--with",
        ".",
        "python",
        str(PERFORMANCE_RUNNER),
        "--python",
        python_version,
        "--diagnostic",
        "--diagnostic-full-order",
        "--artifact-dir",
        str(artifact_dir),
        "--work-dir",
        str(work_parent),
        "--report-date",
        report_date,
    ]
    if skip_contract_typing:
        command.append("--skip-contract-typing")
    return command


def _run_performance_diagnostic(
    *,
    python_version: str,
    artifact_dir: Path,
    skip_contract_typing: bool,
    work_parent: Path,
    timeout_seconds: float,
    report_date: str,
) -> StageProcessResult:
    env = os.environ.copy()
    env[CAPTURE_ENV] = "1"
    return run_bounded(
        _performance_argv(
            python_version=python_version,
            artifact_dir=artifact_dir,
            skip_contract_typing=skip_contract_typing,
            work_parent=work_parent,
            report_date=report_date,
        ),
        cwd=REPO_ROOT,
        env=env,
        timeout_seconds=timeout_seconds,
    )


def _classify_attempt(
    *,
    index: int,
    completed: StageProcessResult,
    duration: float,
    report_path: Path | None,
    report_error: str | None,
) -> AttemptOutcome:
    base = {
        "index": index,
        "returncode": completed.returncode,
        "duration_seconds": duration,
        "timed_out": completed.timed_out,
        "cleanup_errors": completed.cleanup_errors,
    }
    if completed.cleanup_errors:
        return AttemptOutcome(
            status="error",
            error="; ".join(completed.cleanup_errors),
            **base,
        )
    if completed.pid is None:
        return AttemptOutcome(
            status="error",
            error=completed.stderr or "verify_performance subprocess spawn failed",
            **base,
        )
    if completed.timed_out:
        return AttemptOutcome(
            status="error",
            error="verify_performance timed out",
            **base,
        )
    if report_error is not None:
        return AttemptOutcome(
            status="error",
            error=report_error,
            **base,
        )
    if report_path is None or not report_path.is_file():
        tail = (completed.stderr or completed.stdout)[-2000:]
        return AttemptOutcome(
            status="harness_error",
            error=tail or "diagnostic-report.json missing",
            **base,
        )
    try:
        report = _load_report(report_path)
    except ManifestReplayReproError as exc:
        return AttemptOutcome(
            status="error",
            error=str(exc),
            diagnostic_report=report_path,
            **base,
        )
    failures = count_validation_event_failures(report)
    rows = [
        row
        for row in failure_rows_from_report(report)
        if row.get("phase") == "validation_events"
    ]
    if failures > 0:
        status = "reproduced"
    elif completed.returncode == 0:
        status = "clean"
    else:
        status = "harness_error"
    return AttemptOutcome(
        status=status,
        diagnostic_report=report_path,
        validation_event_failures=failures,
        failure_rows=rows,
        error=None if status != "harness_error" else (completed.stderr or completed.stdout)[-2000:],
        **base,
    )


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--python", default="3.12", help="Child runtime (default 3.12).")
    parser.add_argument(
        "--max-attempts",
        type=int,
        default=5,
        help="Maximum full-order diagnostic runs (default 5).",
    )
    parser.add_argument(
        "--continue-after-repro",
        action="store_true",
        help="Keep running until max-attempts even after a reproduced failure.",
    )
    parser.add_argument(
        "--artifact-dir",
        type=Path,
        help=f"Root for attempt artifacts (default {DEFAULT_ARTIFACT_ROOT}).",
    )
    parser.add_argument(
        "--skip-contract-typing",
        action="store_true",
        help="Pass --skip-contract-typing to verify_performance.",
    )
    parser.add_argument(
        "--work-dir",
        type=Path,
        help="Parent directory for ephemeral performance work dirs (must be outside the repo).",
    )
    parser.add_argument(
        "--attempt-timeout-seconds",
        type=float,
        default=INTEGRATION_JOB_BUDGET_SECONDS,
        help="Wall-clock bound per verify_performance invocation.",
    )
    args = parser.parse_args(argv)

    if args.max_attempts < 1:
        parser.error("--max-attempts must be at least 1")
    if not math.isfinite(args.attempt_timeout_seconds) or args.attempt_timeout_seconds <= 0:
        parser.error("--attempt-timeout-seconds must be a finite positive number")

    invocation = uuid.uuid4().hex[:12]
    artifact_parent = args.artifact_dir or DEFAULT_ARTIFACT_ROOT
    artifact_root = artifact_parent / f"run-{invocation}"
    artifact_root.mkdir(parents=True, exist_ok=True)
    print(f"Artifact run directory: {artifact_root}", file=sys.stderr)
    work_parent = args.work_dir or Path(tempfile.gettempdir())
    work_parent.mkdir(parents=True, exist_ok=True)
    report_date = time.strftime("%Y-%m-%d")

    outcomes: list[AttemptOutcome] = []
    reproduced = False
    for index in range(1, args.max_attempts + 1):
        attempt_dir = artifact_root / f"attempt-{index:02d}"
        attempt_dir.mkdir(parents=True, exist_ok=True)
        run_dirs_before = set(list_run_dirs(attempt_dir))
        started = time.monotonic()
        completed = _run_performance_diagnostic(
            python_version=args.python,
            artifact_dir=attempt_dir,
            skip_contract_typing=args.skip_contract_typing,
            work_parent=work_parent,
            timeout_seconds=args.attempt_timeout_seconds,
            report_date=report_date,
        )
        duration = time.monotonic() - started
        report_path, report_error = resolve_fresh_diagnostic_report(
            attempt_dir,
            python_version=args.python,
            run_dirs_before=run_dirs_before,
        )
        write_attempt_streams(
            attempt_dir,
            stdout=completed.stdout,
            stderr=completed.stderr,
            meta={
                "returncode": completed.returncode,
                "timed_out": completed.timed_out,
                "duration_seconds": duration,
                "diagnostic_report": str(report_path) if report_path else None,
                "report_resolution_error": report_error,
                "cleanup_errors": list(completed.cleanup_errors),
            },
        )
        outcome = _classify_attempt(
            index=index,
            completed=completed,
            duration=duration,
            report_path=report_path,
            report_error=report_error,
        )
        outcomes.append(outcome)
        print(
            f"attempt {index}: status={outcome.status} "
            f"validation_event_failures={outcome.validation_event_failures} "
            f"returncode={outcome.returncode} duration={outcome.duration_seconds:.1f}s",
            file=sys.stderr,
        )
        if outcome.status == "reproduced":
            reproduced = True
            for row in outcome.failure_rows:
                print(
                    "  failure "
                    f"execution_id={row.get('execution_id')} "
                    f"exception_type={row.get('exception_type')} "
                    f"sample={row.get('sample_index')} "
                    f"sequence={row.get('sequence')}",
                    file=sys.stderr,
                )
            if not args.continue_after_repro:
                break
        if outcome.status in ("error", "harness_error"):
            if outcome.error:
                print(outcome.error, file=sys.stderr)
            break

    fatal_errors = [
        item.error
        for item in outcomes
        if item.status == "error" and item.error
    ]
    summary = {
        "schema": "superjobs-manifest-replay-repro/v1",
        "issue": ISSUE_URL,
        "invocation": invocation,
        "artifact_run_dir": str(artifact_root),
        "platform": {
            "system": platform.system(),
            "release": platform.release(),
            "machine": platform.machine(),
        },
        "python": args.python,
        "max_attempts": args.max_attempts,
        "attempts_run": len(outcomes),
        "reproduced": reproduced,
        "outcomes": [item.to_dict() for item in outcomes],
        "fatal_errors": fatal_errors,
    }
    summary_path = artifact_root / "reproduction-summary.json"
    summary_path.write_text(json.dumps(summary, indent=2), encoding="utf-8")
    print(f"Reproduction summary: {summary_path}")
    if fatal_errors:
        print("Fatal errors:", "; ".join(fatal_errors), file=sys.stderr)
    if reproduced:
        return EXIT_REPRODUCED
    if any(item.status == "harness_error" for item in outcomes):
        return EXIT_HARNESS
    if any(item.status == "error" for item in outcomes):
        return EXIT_INFRA
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
