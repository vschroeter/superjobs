"""Parent-side merge of worker stdout RA_MSG lines and append-only evidence files."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

try:
    from stdmsg import parse_line
except ModuleNotFoundError:  # imported from repository parent
    from tools.resource_admission_support.stdmsg import parse_line

try:
    from evidence import HolderOverlapSummary, read_events, summarize_holder_overlap
except ModuleNotFoundError:
    from tools.resource_admission_support.evidence import (
        HolderOverlapSummary,
        read_events,
        summarize_holder_overlap,
    )


def parse_worker_log(path: Path) -> list[dict[str, Any]]:
    if not path.is_file():
        return []
    messages: list[dict[str, Any]] = []
    for line in path.read_text(encoding="utf-8").splitlines():
        try:
            payload = parse_line(line)
        except json.JSONDecodeError:
            continue
        if payload is not None:
            messages.append(payload)
    return messages


def collect_scenario_observations(
    state_dir: Path,
    *,
    worker_logs: list[Path] | None = None,
) -> dict[str, Any]:
    events = read_events(state_dir)
    overlap = summarize_holder_overlap(events)
    stdout_messages: list[dict[str, Any]] = []
    for log_path in worker_logs or []:
        stdout_messages.extend(parse_worker_log(log_path))
    return {
        "events": events,
        "holder_overlap": {
            "max_concurrent": overlap.max_concurrent,
            "enter_count": overlap.enter_count,
            "exit_count": overlap.exit_count,
        },
        "stdout_messages": stdout_messages,
    }


def assert_holder_evidence(
    summary: HolderOverlapSummary,
    *,
    maximum: int = 1,
    minimum_enters: int = 1,
) -> None:
    if summary.max_concurrent > maximum:
        raise ValueError(
            f"holder overlap {summary.max_concurrent} exceeds maximum {maximum}",
        )
    if summary.enter_count < minimum_enters:
        raise ValueError(
            f"expected at least {minimum_enters} holder enter events, saw {summary.enter_count}",
        )
    if summary.exit_count < minimum_enters:
        raise ValueError(
            f"expected at least {minimum_enters} holder exit events, saw {summary.exit_count}",
        )
