#!/usr/bin/env python3
"""Repeat selected NATS reliability verification families with bounded wall time."""

from __future__ import annotations

import argparse
import json
import platform
import secrets
import sys
import time
import uuid
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from scripts.dev_check.constants import (  # noqa: E402
    INTEGRATION_JOB_BUDGET_SECONDS,
    JOB_CLEANUP_RESERVE_SECONDS,
)
from scripts.dev_check.orchestrator import repo_root, require_gate_python  # noqa: E402
from scripts.dev_check.process_tree import StageProcessResult, run_bounded  # noqa: E402
from tools.reliability_repetition_support.evidence import (  # noqa: E402
    ReliabilityEvidenceError,
    assess_child_outcome,
)
from tools.reliability_repetition_support.plan import (  # noqa: E402
    REPETITIONS_PER_FAMILY,
    ReliabilityRunItem,
    build_child_argv,
    planned_items,
    shuffle_items,
)
from tools.verify_broker_restart import SCENARIO_NAME as BROKER_RESTART_SCENARIO  # noqa: E402
from tools.verify_contract_typing import runtime_evidence_tag  # noqa: E402
from tools.verify_cross_program import SCENARIOS as CROSS_PROGRAM_SCENARIOS  # noqa: E402
from tools.verify_worker_recovery import SCENARIO_AFTER  # noqa: E402

DEFAULT_ARTIFACT_ROOT = REPO_ROOT / "dist" / "verification" / "reliability-repetition"
ITEMS_DIRNAME = "items"
CHILD_EVIDENCE_DIRNAME = "e"
INVOCATION_TOKEN_HEX_LEN = 12
WINDOWS_MAX_PATH = 260


class ReliabilityRepetitionError(Exception):
    """Raised when the repetition runner cannot complete successfully."""


def new_invocation_token() -> str:
    """Return a short collision-resistant directory name for one repetition run."""
    return uuid.uuid4().hex[:INVOCATION_TOKEN_HEX_LEN]


def item_artifact_dir(invocation_dir: Path, sequence_index: int) -> Path:
    return invocation_dir / ITEMS_DIRNAME / f"{sequence_index:02d}"


def child_evidence_dir(item_dir: Path) -> Path:
    return item_dir / CHILD_EVIDENCE_DIRNAME


def worst_case_native_evidence_relpath(python_version: str) -> Path:
    """Longest relative path under a child ``--artifact-dir`` from native verify tools."""
    tag = runtime_evidence_tag(python_version)
    run_id = "f" * 32
    scenario_names = (
        BROKER_RESTART_SCENARIO,
        SCENARIO_AFTER,
        *(spec.name for spec in CROSS_PROGRAM_SCENARIOS),
    )
    scenario = max(scenario_names, key=len)
    log_names = (
        "producer-recover-completed.stderr.log",
        "producer-recover.stderr.log",
        "producer-await-pending.stderr.log",
        "worker-1.stderr.log",
        "producer.stderr.log",
    )
    log_name = max(log_names, key=len)
    return Path(tag) / "scenarios" / scenario / run_id / log_name


def deepest_default_artifact_file(
    *,
    repo_root: Path,
    python_version: str,
    invocation_token: str | None = None,
) -> Path:
    """Absolute path to the longest native evidence file for the default artifact layout."""
    token = invocation_token or ("f" * INVOCATION_TOKEN_HEX_LEN)
    artifact_root = repo_root / "dist" / "verification" / "reliability-repetition"
    last_sequence = len(planned_items()) - 1
    item_dir = item_artifact_dir(artifact_root / token, last_sequence)
    evidence = child_evidence_dir(item_dir)
    return evidence / worst_case_native_evidence_relpath(python_version)


@dataclass
class ItemOutcome:
    item: ReliabilityRunItem
    status: str
    returncode: int | None
    duration_seconds: float
    error: str | None = None
    timed_out: bool = False
    interrupted: bool = False
    cleanup_errors: tuple[str, ...] = ()

    def to_dict(self) -> dict[str, Any]:
        return {
            "family": self.item.family,
            "repetition": self.item.repetition,
            "sequence_index": self.item.sequence_index,
            "status": self.status,
            "returncode": self.returncode,
            "duration_seconds": self.duration_seconds,
            "error": self.error,
            "timed_out": self.timed_out,
            "interrupted": self.interrupted,
            "cleanup_errors": list(self.cleanup_errors),
        }


@dataclass
class RunState:
    seed: int
    python_version: str
    environment: dict[str, str]
    artifact_dir: Path
    invocation_id: str
    planned: tuple[ReliabilityRunItem, ...]
    started_at: float
    started_mono: float
    outcomes: list[ItemOutcome] = field(default_factory=list)
    finished_at: float | None = None
    finished_mono: float | None = None
    run_error: str | None = None

    @property
    def completed_count(self) -> int:
        return sum(1 for item in self.outcomes if item.status == "passed")

    def _elapsed_monotonic_seconds(self) -> float | None:
        if self.finished_mono is None:
            return None
        return self.finished_mono - self.started_mono

    def is_run_ok(self) -> bool:
        if self.run_error is not None:
            return False
        if self.completed_count != len(self.planned):
            return False
        if self.finished_at is None or self.finished_mono is None:
            return False
        elapsed_mono = self._elapsed_monotonic_seconds()
        if elapsed_mono is None or elapsed_mono > INTEGRATION_JOB_BUDGET_SECONDS:
            return False
        return True

    def to_summary(self) -> dict[str, Any]:
        finished_wall = self.finished_at
        finished_mono = self.finished_mono
        elapsed_mono = self._elapsed_monotonic_seconds()
        elapsed_wall = (
            (finished_wall - self.started_at)
            if finished_wall is not None
            else None
        )
        return {
            "seed": self.seed,
            "invocation_id": self.invocation_id,
            "python_version": self.python_version,
            "environment": self.environment,
            "repetitions_per_family": REPETITIONS_PER_FAMILY,
            "whole_run_budget_seconds": INTEGRATION_JOB_BUDGET_SECONDS,
            "cleanup_reserve_seconds": JOB_CLEANUP_RESERVE_SECONDS,
            "started_at": self.started_at,
            "finished_at": finished_wall,
            "elapsed_seconds": elapsed_mono,
            "elapsed_wall_seconds": elapsed_wall,
            "planned_items": len(self.planned),
            "completed_items": self.completed_count,
            "ok": self.is_run_ok(),
            "run_error": self.run_error,
            "planned_order": [
                {
                    "family": item.family,
                    "repetition": item.repetition,
                    "sequence_index": item.sequence_index,
                }
                for item in self.planned
            ],
            "outcomes": [item.to_dict() for item in self.outcomes],
        }


def build_run_environment() -> dict[str, str]:
    return {
        "platform": platform.platform(),
        "system": platform.system(),
        "release": platform.release(),
        "sys_platform": sys.platform,
        "python_version": sys.version,
        "python_executable": sys.executable,
    }


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2), encoding="utf-8")


def _summary_path(artifact_dir: Path) -> Path:
    return artifact_dir / "run-summary.json"


def persist_run_summary(state: RunState) -> Path:
    path = _summary_path(state.artifact_dir)
    _write_json(path, state.to_summary())
    return path


def _persist_run_summary_safe(state: RunState) -> None:
    try:
        persist_run_summary(state)
    except OSError as exc:
        message = f"failed to persist run summary: {exc}"
        if state.run_error:
            state.run_error = f"{state.run_error}; {message}"
        else:
            state.run_error = message
        try:
            persist_run_summary(state)
        except OSError:
            pass


def _write_item_streams(item_dir: Path, result: StageProcessResult) -> None:
    item_dir.mkdir(parents=True, exist_ok=True)
    (item_dir / "stdout.txt").write_text(result.stdout, encoding="utf-8")
    (item_dir / "stderr.txt").write_text(result.stderr, encoding="utf-8")


def _remaining_run_seconds(deadline_mono: float) -> float:
    return max(0.0, deadline_mono - JOB_CLEANUP_RESERVE_SECONDS - time.monotonic())


def _finalize_run_state(state: RunState) -> None:
    state.finished_at = time.time()
    state.finished_mono = time.monotonic()


def run_repetition(
    *,
    seed: int,
    artifact_dir: Path,
    python_version: str,
    invocation_id: str | None = None,
) -> RunState:
    started_wall = time.time()
    started_mono = time.monotonic()
    deadline_mono = started_mono + INTEGRATION_JOB_BUDGET_SECONDS
    root = repo_root()
    ordered = shuffle_items(seed, planned_items())
    run_id = invocation_id or new_invocation_token()
    state = RunState(
        seed=seed,
        python_version=python_version,
        environment=build_run_environment(),
        artifact_dir=artifact_dir,
        invocation_id=run_id,
        planned=ordered,
        started_at=started_wall,
        started_mono=started_mono,
    )
    artifact_dir.mkdir(parents=True, exist_ok=True)
    _persist_run_summary_safe(state)
    env = {"PYTHONNOUSERSITE": "1"}

    try:
        for item in ordered:
            if state.run_error is not None:
                break
            remaining = _remaining_run_seconds(deadline_mono)
            if remaining <= 0:
                state.run_error = "whole-run budget exhausted before next item"
                break

            try:
                item_dir = item_artifact_dir(artifact_dir, item.sequence_index)
                child_artifacts = child_evidence_dir(item_dir)
                child_artifacts.mkdir(parents=True, exist_ok=True)
                argv = build_child_argv(
                    repo_root=root,
                    python_version=python_version,
                    family=item.family,
                    artifact_dir=child_artifacts,
                )
                (item_dir / "command.json").write_text(
                    json.dumps({"argv": argv}, indent=2),
                    encoding="utf-8",
                )
                result = run_bounded(
                    argv,
                    cwd=root,
                    env=env,
                    timeout_seconds=remaining,
                )
                _write_item_streams(item_dir, result)
                combined = result.stdout + "\n" + result.stderr
                outcome_error: str | None = None
                status = "failed"
                try:
                    if time.monotonic() >= deadline_mono - JOB_CLEANUP_RESERVE_SECONDS:
                        raise ReliabilityEvidenceError("whole-run budget exhausted during item")
                    assess_child_outcome(
                        family=item.family,
                        artifact_dir=child_artifacts,
                        python_version=python_version,
                        result=result,
                        combined_output=combined,
                    )
                    status = "passed"
                except ReliabilityEvidenceError as exc:
                    outcome_error = str(exc)
                    state.run_error = f"{item.label} failed: {outcome_error}"
                outcome = ItemOutcome(
                    item=item,
                    status=status,
                    returncode=result.returncode,
                    duration_seconds=result.duration_seconds,
                    error=outcome_error,
                    timed_out=result.timed_out,
                    interrupted=result.interrupted,
                    cleanup_errors=result.cleanup_errors,
                )
                state.outcomes.append(outcome)
                _persist_run_summary_safe(state)
                if state.run_error is not None:
                    break
            except BaseException as exc:
                state.run_error = f"{item.label} launch failed: {type(exc).__name__}: {exc}"
                _persist_run_summary_safe(state)
                break
    finally:
        _finalize_run_state(state)
        elapsed_mono = state._elapsed_monotonic_seconds()
        if (
            elapsed_mono is not None
            and elapsed_mono > INTEGRATION_JOB_BUDGET_SECONDS
            and state.run_error is None
        ):
            state.run_error = (
                f"whole-run monotonic budget exceeded "
                f"({elapsed_mono:.1f}s > {INTEGRATION_JOB_BUDGET_SECONDS}s)"
            )
        _persist_run_summary_safe(state)

    return state


def _load_summary(path: Path) -> dict[str, Any]:
    if not path.is_file():
        raise ReliabilityRepetitionError(f"missing run summary at {path}")
    return json.loads(path.read_text(encoding="utf-8"))


def _assert_summary_complete(summary: dict[str, Any], *, planned_items: int) -> None:
    required_keys = (
        "seed",
        "invocation_id",
        "environment",
        "planned_items",
        "completed_items",
        "elapsed_seconds",
        "elapsed_wall_seconds",
        "planned_order",
        "outcomes",
        "ok",
    )
    missing = [key for key in required_keys if key not in summary]
    if missing:
        raise ReliabilityRepetitionError(f"run summary missing keys: {missing}")
    if summary["planned_items"] != planned_items:
        raise ReliabilityRepetitionError("run summary planned_items mismatch")
    if not isinstance(summary["environment"], dict):
        raise ReliabilityRepetitionError("run summary environment is not an object")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--seed",
        type=int,
        help="Deterministic shuffle seed (ordering only). Random when omitted.",
    )
    parser.add_argument(
        "--artifact-dir",
        type=Path,
        default=DEFAULT_ARTIFACT_ROOT,
        help=f"Evidence root (default: {DEFAULT_ARTIFACT_ROOT.relative_to(REPO_ROOT)}).",
    )
    args = parser.parse_args(argv)
    seed = args.seed if args.seed is not None else secrets.randbits(63)
    python_version = require_gate_python()
    artifact_root = args.artifact_dir
    if not artifact_root.is_absolute():
        artifact_root = REPO_ROOT / artifact_root
    invocation_id = new_invocation_token()
    artifact_dir = artifact_root / invocation_id

    print(
        f"verify_reliability_repetition: seed={seed} python={python_version} "
        f"platform={platform.platform()} invocation={invocation_id}",
        file=sys.stderr,
    )

    state: RunState | None = None
    try:
        state = run_repetition(
            seed=seed,
            artifact_dir=artifact_dir,
            python_version=python_version,
            invocation_id=invocation_id,
        )
    except BaseException as exc:
        if state is None:
            state = RunState(
                seed=seed,
                python_version=python_version,
                environment=build_run_environment(),
                artifact_dir=artifact_dir,
                invocation_id=invocation_id,
                planned=shuffle_items(seed, planned_items()),
                started_at=time.time(),
                started_mono=time.monotonic(),
                run_error=f"{type(exc).__name__}: {exc}",
            )
            _finalize_run_state(state)
        else:
            state.run_error = f"{type(exc).__name__}: {exc}"
            _finalize_run_state(state)
        persist_run_summary(state)
        print(f"verify_reliability_repetition: {exc}", file=sys.stderr)
        return 1

    summary_path = persist_run_summary(state)
    summary = _load_summary(summary_path)
    _assert_summary_complete(summary, planned_items=len(state.planned))

    print(f"Artifact directory: {artifact_dir}", file=sys.stderr)
    print(f"Run summary: {summary_path}", file=sys.stderr)
    if state.run_error is not None:
        print(f"verify_reliability_repetition: {state.run_error}", file=sys.stderr)
        print(
            f"Completed {state.completed_count}/{len(state.planned)} planned items "
            f"(seed={seed})",
            file=sys.stderr,
        )
        return 1

    if not summary.get("ok"):
        print("verify_reliability_repetition: run summary not ok after completion", file=sys.stderr)
        return 1

    print("verify_reliability_repetition: OK")
    print(
        f"Completed {state.completed_count}/{len(state.planned)} items "
        f"in {summary['elapsed_seconds']:.1f}s (seed={seed})",
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
