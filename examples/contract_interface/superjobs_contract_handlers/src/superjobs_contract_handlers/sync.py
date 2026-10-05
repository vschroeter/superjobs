"""File-based coordination for CLI process verification (issue #42 / #46)."""

from __future__ import annotations

import asyncio
import json
import os
from collections.abc import Awaitable, Callable
from pathlib import Path


def _sync_root() -> Path:
    raw = os.environ.get("SUPERJOBS_CLI_SYNC_DIR") or os.environ.get("SUPERJOBS_CLI_STATE_DIR")
    if not raw:
        raise RuntimeError(
            "SUPERJOBS_CLI_SYNC_DIR or SUPERJOBS_CLI_STATE_DIR must be set for gated handlers",
        )
    return Path(raw)


def _write_json_atomic(path: Path, payload: dict[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(path.name + ".tmp")
    tmp.write_text(json.dumps(payload, sort_keys=True) + "\n", encoding="utf-8")
    tmp.replace(path)


def write_handler_entered(
    gate: str,
    *,
    execution_id: str | None = None,
    run_id: str | None = None,
) -> None:
    payload: dict[str, object] = {"checkpoint": "handler_entered", "gate": gate}
    if execution_id is not None:
        payload["execution_id"] = execution_id
    if run_id is not None:
        payload["run_id"] = run_id
    path = _sync_root() / f"checkpoint_handler_entered_{gate}.json"
    _write_json_atomic(path, payload)


def write_provider_entered(command: str) -> None:
    if os.environ.get("SUPERJOBS_CLI_EMIT_FACTORY") != "1":
        return
    _write_json_atomic(
        _sync_root() / f"checkpoint_provider_entered_{command}.json",
        {"checkpoint": "provider_entered", "command": command, "gate": command},
    )


def write_provider_closed(command: str) -> None:
    if os.environ.get("SUPERJOBS_CLI_EMIT_FACTORY") != "1":
        return
    _write_json_atomic(
        _sync_root() / f"checkpoint_provider_closed_{command}.json",
        {"checkpoint": "provider_closed", "command": command, "gate": command},
    )


def gate_release_path(gate: str) -> Path:
    return _sync_root() / f"release_{gate}.json"


def write_gate_release(gate: str) -> None:
    _write_json_atomic(gate_release_path(gate), {"released": True, "gate": gate})


async def wait_for_gate_release(
    gate: str,
    *,
    poll_interval: float = 0.05,
    check_cancelled: Callable[[], Awaitable[None]] | None = None,
) -> None:
    path = gate_release_path(gate)
    while not path.is_file():
        if check_cancelled is not None:
            await check_cancelled()
        await asyncio.sleep(poll_interval)
