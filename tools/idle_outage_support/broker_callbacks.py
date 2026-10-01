"""NATS broker disconnect/reconnect markers for idle outage verification."""

from __future__ import annotations

import os
from collections.abc import Callable
from pathlib import Path

from protocol import broker_event_checkpoint, checkpoint_path, write_checkpoint

def make_broker_callbacks(
    state_dir: Path,
    run_id: str,
    *,
    role: str,
    runtime_object_id: Callable[[], int],
) -> tuple[object, object]:
    """Return ``disconnected_cb`` and ``reconnected_cb`` for ``NatsBroker``."""

    callback_sequence = 0

    def _record_first(event: str) -> None:
        nonlocal callback_sequence
        checkpoint = broker_event_checkpoint(event, role)
        if checkpoint_path(state_dir, checkpoint).is_file():
            return
        callback_sequence += 1
        write_checkpoint(
            state_dir,
            run_id=run_id,
            checkpoint=checkpoint,
            pid=os.getpid(),
            runtime_object_id=runtime_object_id(),
            callback_sequence=callback_sequence,
        )

    async def disconnected_cb() -> None:
        _record_first("disconnected")

    async def reconnected_cb() -> None:
        _record_first("reconnected")

    return disconnected_cb, reconnected_cb
