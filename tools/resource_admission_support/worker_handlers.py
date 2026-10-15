"""Worker-side resource admission handlers (never imported by controller)."""

from __future__ import annotations

import asyncio
import json
import os
from pathlib import Path

from catalog_factory import register_scenario_handlers
from constants import RESOURCE_KEY
from evidence import append_event
from protocol import (
    MODEL_LOAD_ENTERED,
    MODEL_LOAD_RELEASED,
    write_execution_checkpoint,
)
from queue_config import control_subject, queue_config_for_run
from stdmsg import emit


def _state_dir() -> Path:
    return Path(os.environ["SUPERJOBS_CROSS_STATE_DIR"])


def _run_id() -> str:
    return os.environ["SUPERJOBS_CROSS_RUN_ID"]


def _scenario() -> str:
    return os.environ["SUPERJOBS_CROSS_SCENARIO"]


def _worker_generation() -> str:
    return os.environ.get("SUPERJOBS_WORKER_GENERATION", "1")


class ControlWaiter:
    def __init__(self, nats_url: str, subject: str) -> None:
        self._nats_url = nats_url
        self._subject = subject
        self._releases: dict[str, asyncio.Event] = {}
        self._subscription = None
        self._connection = None
        self._listen_task: asyncio.Task[None] | None = None
        self._closed = False

    async def start(self) -> None:
        import nats

        self._connection = await nats.connect(self._nats_url, connect_timeout=5.0)
        self._subscription = await self._connection.subscribe(self._subject)
        self._listen_task = asyncio.create_task(
            self._listen(), name="ra-control-waiter"
        )

    async def _listen(self) -> None:
        assert self._subscription is not None
        try:
            while not self._closed:
                message = await self._subscription.next_msg(timeout=None)
                payload = json.loads(message.data.decode("utf-8"))
                if not isinstance(payload, dict):
                    continue
                if payload.get("type") != "release_holder":
                    continue
                execution_id = payload.get("execution_id")
                if not isinstance(execution_id, str) or not execution_id:
                    continue
                event = self._releases.get(execution_id)
                if event is None:
                    continue
                event.set()
                if message.reply:
                    await message.respond(json.dumps({"released": execution_id}).encode("utf-8"))
        except asyncio.CancelledError:
            raise
        except Exception:
            if not self._closed:
                raise

    async def wait_release(self, execution_id: str) -> None:
        event = self._releases.setdefault(execution_id, asyncio.Event())
        await event.wait()

    async def close(self) -> None:
        self._closed = True
        if self._listen_task is not None:
            self._listen_task.cancel()
            try:
                await self._listen_task
            except asyncio.CancelledError:
                pass
            self._listen_task = None
        if self._connection is not None:
            await self._connection.drain()
            self._connection = None


def register_worker_handlers(catalog, jobs) -> ControlWaiter:
    nats_url = os.environ["NATS_URL"]
    run_id = _run_id()
    scenario = _scenario()
    queue_config = queue_config_for_run(
        run_id,
        ack_wait=float(os.environ.get("SUPERJOBS_RA_ACK_WAIT", "3.0")),
    )
    subject = control_subject(queue_config, run_id)
    waiter = ControlWaiter(nats_url, subject)

    async def on_enter(label: str, context, delivery_attempt: int | None) -> None:
        state_dir = _state_dir()
        run = _run_id()
        generation = _worker_generation()
        execution_id = context.id
        append_event(
            state_dir,
            kind="handler_callback",
            run_id=run,
            execution_id=execution_id,
            job=label,
            attempt=delivery_attempt,
            worker_generation=generation,
        )
        append_event(
            state_dir,
            kind="holder_enter",
            run_id=run,
            execution_id=execution_id,
            job=label,
            worker_generation=generation,
        )
        write_execution_checkpoint(
            state_dir,
            run_id=run,
            checkpoint=MODEL_LOAD_ENTERED,
            execution_id=execution_id,
            job=label,
            pid=os.getpid(),
            worker_generation=generation,
            delivery_attempt=delivery_attempt,
        )
        emit(
            {
                "type": "model_load_entered",
                "job": label,
                "execution_id": execution_id,
                "resource_key": RESOURCE_KEY,
            },
        )

    async def on_exit(label: str, context) -> None:
        state_dir = _state_dir()
        run = _run_id()
        execution_id = context.id
        append_event(
            state_dir,
            kind="holder_exit",
            run_id=run,
            execution_id=execution_id,
            job=label,
            worker_generation=_worker_generation(),
        )
        write_execution_checkpoint(
            state_dir,
            run_id=run,
            checkpoint=MODEL_LOAD_RELEASED,
            execution_id=execution_id,
            job=label,
            pid=os.getpid(),
            worker_generation=_worker_generation(),
        )
        emit(
            {"type": "model_load_released", "job": label, "execution_id": execution_id}
        )

    register_scenario_handlers(
        catalog,
        scenario=scenario,
        run_id=run_id,
        hold_until_control=waiter.wait_release,
        on_enter=on_enter,
        on_exit=on_exit,
    )
    return waiter
