"""CLI entry for developer check orchestration."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

from scripts.dev_check.constants import (
    FAST_PYTHON_MINORS,
    GATE_FAMILY_FAST,
    GATE_FAMILY_INTEGRATION,
    INTEGRATION_PYTHON_MINORS,
)
from scripts.dev_check.orchestrator import print_summary, require_gate_python, run_stages


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Cross-platform developer checks: fast deterministic gates and full integration.",
    )
    parser.add_argument(
        "mode",
        choices=("fast", "integration", "full"),
        help="fast: deterministic pytest only; integration: typing/NATS/process gates; full: both.",
    )
    parser.add_argument(
        "--artifact-dir",
        type=Path,
        help="Directory for run-summary.json and per-stage logs (default: dist/verification/dev-check/<mode>).",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    py_version = require_gate_python()
    summary = run_stages(args.mode, artifact_dir=args.artifact_dir)
    print_summary(summary)
    if summary.ok:
        print(f"dev_check {args.mode}: OK (python {py_version})")
        return 0
    print(f"dev_check {args.mode}: FAILED (python {py_version})", file=sys.stderr)
    print(
        f"Required CI families: {GATE_FAMILY_FAST} ({', '.join(FAST_PYTHON_MINORS)}), "
        f"{GATE_FAMILY_INTEGRATION} ({', '.join(INTEGRATION_PYTHON_MINORS)})",
        file=sys.stderr,
    )
    return 1


def ensure_repo_on_path() -> None:
    repo_root = Path(__file__).resolve().parents[2]
    if str(repo_root) not in sys.path:
        sys.path.insert(0, str(repo_root))
