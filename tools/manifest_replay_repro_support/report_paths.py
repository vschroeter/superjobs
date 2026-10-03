"""Locate fresh verify_performance diagnostic reports under run-* artifact dirs."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from tools.verify_contract_typing import runtime_evidence_tag

RUN_DIR_PREFIX = "run-"
DIAGNOSTIC_REPORT_NAME = "diagnostic-report.json"


def list_run_dirs(artifact_parent: Path) -> list[Path]:
    if not artifact_parent.is_dir():
        return []
    return sorted(
        path
        for path in artifact_parent.iterdir()
        if path.is_dir() and path.name.startswith(RUN_DIR_PREFIX)
    )


def diagnostic_report_in_run_dir(run_dir: Path) -> Path:
    return run_dir / DIAGNOSTIC_REPORT_NAME


def legacy_runtime_report_path(artifact_parent: Path, python_version: str) -> Path:
    return (
        artifact_parent
        / runtime_evidence_tag(python_version)
        / DIAGNOSTIC_REPORT_NAME
    )


def resolve_fresh_diagnostic_report(
    artifact_parent: Path,
    *,
    python_version: str,
    run_dirs_before: set[Path],
) -> tuple[Path | None, str | None]:
    """Return the diagnostic report from exactly one new run-* dir under artifact_parent."""
    after = [path for path in list_run_dirs(artifact_parent) if path not in run_dirs_before]
    legacy = legacy_runtime_report_path(artifact_parent, python_version)
    if legacy.is_file() and not after:
        return None, (
            f"stale evidence: found {legacy} but no new {RUN_DIR_PREFIX}<token>/ "
            f"{DIAGNOSTIC_REPORT_NAME} from this attempt"
        )
    if not after:
        return None, f"no new {RUN_DIR_PREFIX}<token> directory under {artifact_parent}"
    if len(after) > 1:
        names = ", ".join(path.name for path in after)
        return None, f"ambiguous fresh run directories: {names}"
    report_path = diagnostic_report_in_run_dir(after[0])
    if not report_path.is_file():
        return None, f"{report_path} missing"
    if legacy.is_file():
        return None, (
            f"stale evidence: legacy runtime report {legacy} must not be used when "
            f"fresh report is {report_path}"
        )
    return report_path, None


def attempt_subprocess_paths(attempt_dir: Path) -> dict[str, Path]:
    return {
        "stdout": attempt_dir / "verify_performance.stdout.txt",
        "stderr": attempt_dir / "verify_performance.stderr.txt",
        "meta": attempt_dir / "attempt-meta.json",
    }


def write_attempt_streams(
    attempt_dir: Path,
    *,
    stdout: str,
    stderr: str,
    meta: dict[str, Any],
) -> None:
    paths = attempt_subprocess_paths(attempt_dir)
    attempt_dir.mkdir(parents=True, exist_ok=True)
    paths["stdout"].write_text(stdout, encoding="utf-8")
    paths["stderr"].write_text(stderr, encoding="utf-8")
    paths["meta"].write_text(json.dumps(meta, indent=2), encoding="utf-8")
