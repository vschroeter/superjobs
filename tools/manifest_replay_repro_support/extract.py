"""Parse performance diagnostic reports for replay failure rows."""

from __future__ import annotations

from typing import Any

REPLAY_CAPTURE_PHASE = "validation_events"


def failure_rows_from_combinations(
    combinations: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for combo in combinations:
        workload = combo.get("workload")
        concurrency = combo.get("concurrency")
        for sample_index, sample in enumerate(combo.get("samples") or [], start=1):
            for row in sample.get("failure_diagnostics") or []:
                enriched = dict(row)
                enriched["workload"] = workload
                enriched["concurrency"] = concurrency
                enriched["sample_index"] = sample_index
                rows.append(enriched)
    return rows


def failure_rows_from_report(report: dict[str, Any]) -> list[dict[str, Any]]:
    return failure_rows_from_combinations(list(report.get("combinations") or []))


def count_validation_event_failures(report: dict[str, Any]) -> int:
    total = 0
    for row in failure_rows_from_report(report):
        if row.get("phase") == REPLAY_CAPTURE_PHASE:
            total += 1
    return total


def unique_execution_ids_for_replay_failures(
    combinations: list[dict[str, Any]],
) -> list[str]:
    seen: list[str] = []
    for row in failure_rows_from_combinations(combinations):
        if row.get("phase") != REPLAY_CAPTURE_PHASE:
            continue
        execution_id = row.get("execution_id")
        if not isinstance(execution_id, str) or not execution_id:
            continue
        if execution_id not in seen:
            seen.append(execution_id)
    return seen
