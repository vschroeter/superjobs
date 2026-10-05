"""CLI-local sync helpers; gate coordination lives in ``superjobs_contract_handlers.sync``."""

from __future__ import annotations

import os

from superjobs_contract_handlers.sync import (
    gate_release_path,
    wait_for_gate_release,
    write_gate_release,
    write_handler_entered,
    write_provider_closed,
    write_provider_entered,
)

__all__ = [
    "gate_release_path",
    "wait_for_gate_release",
    "write_gate_release",
    "write_handler_entered",
    "write_runtime_factory_closed",
    "write_runtime_factory_entered",
    "write_provider_closed",
    "write_provider_entered",
]


def write_runtime_factory_entered(mode: str) -> None:
    if os.environ.get("SUPERJOBS_CLI_EMIT_FACTORY") != "1":
        return
    from superjobs_contract_handlers.sync import _sync_root, _write_json_atomic

    _write_json_atomic(
        _sync_root() / "checkpoint_runtime_factory_entered.json",
        {"checkpoint": "runtime_factory_entered", "mode": mode},
    )


def write_runtime_factory_closed(mode: str) -> None:
    if os.environ.get("SUPERJOBS_CLI_EMIT_FACTORY") != "1":
        return
    from superjobs_contract_handlers.sync import _sync_root, _write_json_atomic

    _write_json_atomic(
        _sync_root() / "checkpoint_runtime_factory_closed.json",
        {"checkpoint": "runtime_factory_closed", "mode": mode},
    )
