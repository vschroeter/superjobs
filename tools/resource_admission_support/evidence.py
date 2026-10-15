"""Append-only scenario evidence (no shared read/modify/write aggregates)."""

from __future__ import annotations

import json
import os
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Literal

EvidenceKind = Literal[
    "work_pull",
    "holder_enter",
    "holder_exit",
    "handler_callback",
]

EVENTS_FILE = "events.jsonl"


def evidence_dir(state_dir: Path) -> Path:
    return state_dir / "evidence"


def events_path(state_dir: Path) -> Path:
    return evidence_dir(state_dir) / EVENTS_FILE


def append_event(
    state_dir: Path,
    *,
    kind: EvidenceKind,
    run_id: str,
    execution_id: str | None = None,
    job: str | None = None,
    attempt: int | None = None,
    worker_generation: str | None = None,
    extra: dict[str, Any] | None = None,
) -> None:
    path = events_path(state_dir)
    path.parent.mkdir(parents=True, exist_ok=True)
    payload: dict[str, Any] = {
        "kind": kind,
        "run_id": run_id,
        "pid": os.getpid(),
        "t": time.monotonic(),
    }
    if execution_id is not None:
        payload["execution_id"] = execution_id
    if job is not None:
        payload["job"] = job
    if attempt is not None:
        payload["attempt"] = attempt
    if worker_generation is not None:
        payload["worker_generation"] = worker_generation
    if extra:
        payload.update(extra)
    with path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(payload, sort_keys=True) + "\n")


def read_events(state_dir: Path) -> list[dict[str, Any]]:
    path = events_path(state_dir)
    if not path.is_file():
        return []
    events: list[dict[str, Any]] = []
    for line in path.read_text(encoding="utf-8").splitlines():
        if not line.strip():
            continue
        item = json.loads(line)
        if isinstance(item, dict):
            events.append(item)
    return events


@dataclass(frozen=True, slots=True)
class HolderOverlapSummary:
    max_concurrent: int
    enter_count: int
    exit_count: int


def summarize_holder_overlap(events: list[dict[str, Any]]) -> HolderOverlapSummary:
    holder_events = [
        item for item in events if item.get("kind") in {"holder_enter", "holder_exit"}
    ]
    holder_events.sort(
        key=lambda item: (float(item.get("t", 0.0)), int(item.get("pid", 0)))
    )
    active = 0
    peak = 0
    enters = 0
    exits = 0
    for item in holder_events:
        kind = item.get("kind")
        if kind == "holder_enter":
            active += 1
            enters += 1
        elif kind == "holder_exit":
            active -= 1
            exits += 1
        peak = max(peak, active)
    return HolderOverlapSummary(
        max_concurrent=peak,
        enter_count=enters,
        exit_count=exits,
    )


def count_handler_callbacks(
    events: list[dict[str, Any]],
    *,
    execution_id: str,
) -> int:
    return sum(
        1
        for item in events
        if item.get("kind") == "handler_callback"
        and item.get("execution_id") == execution_id
    )


def count_work_pulls(
    events: list[dict[str, Any]],
    *,
    execution_id: str,
) -> int:
    return sum(
        1
        for item in events
        if item.get("kind") == "work_pull" and item.get("execution_id") == execution_id
    )


def wait_for_work_pulls(
    state_dir: Path,
    *,
    execution_id: str,
    minimum: int,
    deadline: float,
    poll_interval: float = 0.05,
) -> int:
    while time.monotonic() < deadline:
        count = count_work_pulls(read_events(state_dir), execution_id=execution_id)
        if count >= minimum:
            return count
        time.sleep(min(poll_interval, max(0.0, deadline - time.monotonic())))
    raise TimeoutError(
        f"expected at least {minimum} work_pull for {execution_id}, "
        f"saw {count_work_pulls(read_events(state_dir), execution_id=execution_id)}",
    )
