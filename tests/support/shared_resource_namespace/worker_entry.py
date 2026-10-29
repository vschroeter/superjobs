"""Subprocess worker for shared resource-admission namespace tests."""

from __future__ import annotations

import asyncio
import json
import os
import sys
from pathlib import Path

import nats
from faststream.nats import NatsBroker

from superjobs import HandlerCatalog, SuperJobs
from superjobs.jobs.resource_cleanup import acknowledge_resource_cleanup
from superjobs.resources import V1_EXCLUSIVE_CAPACITY

from constants import HOLD_JOB, RESOURCE_KEY
from queue_config import control_subject, queue_config_for_work_prefix

STARTUP_TIMEOUT = 30.0


def _require_env(name: str) -> str:
    value = os.environ.get(name)
    if not value:
        print(f"{name} must be set", file=sys.stderr)
        raise SystemExit(11)
    return value


def _ready_path(state_dir: Path, worker_label: str) -> Path:
    return state_dir / f"worker_ready_{worker_label}.json"


def _write_ready(state_dir: Path, *, worker_label: str, jobs: SuperJobs) -> None:
    incarnation = jobs.worker_incarnation
    payload = {
        "worker_label": worker_label,
        "pid": os.getpid(),
        "worker_id": incarnation.worker_id,
        "incarnation_id": incarnation.incarnation_id,
    }
    _ready_path(state_dir, worker_label).write_text(json.dumps(payload), encoding="utf-8")


class ReleaseWaiter:
    def __init__(self, nats_url: str, subject: str) -> None:
        self._nats_url = nats_url
        self._subject = subject
        self._releases: dict[str, asyncio.Event] = {}
        self._connection = None
        self._subscription = None
        self._task: asyncio.Task[None] | None = None
        self._closed = False

    async def start(self) -> None:
        self._connection = await nats.connect(self._nats_url, connect_timeout=5.0)
        self._subscription = await self._connection.subscribe(self._subject)
        self._task = asyncio.create_task(self._listen(), name="shared-ns-release-waiter")

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
                event = self._releases.setdefault(execution_id, asyncio.Event())
                event.set()
                if message.reply:
                    await message.respond(
                        json.dumps({"released": execution_id}).encode("utf-8"),
                    )
        except asyncio.CancelledError:
            raise

    async def wait_release(self, execution_id: str) -> None:
        event = self._releases.setdefault(execution_id, asyncio.Event())
        await event.wait()

    async def close(self) -> None:
        self._closed = True
        if self._task is not None:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
        if self._connection is not None:
            await self._connection.drain()


async def _run() -> None:
    nats_url = _require_env("NATS_URL")
    work_prefix = _require_env("SJ_WORK_PREFIX")
    worker_label = _require_env("SJ_WORKER_LABEL")
    state_dir = Path(_require_env("SJ_STATE_DIR"))
    handler_mode = os.environ.get("SJ_HANDLER_MODE", "hold")
    state_dir.mkdir(parents=True, exist_ok=True)
    resource_namespace = os.environ.get("SJ_RESOURCE_NAMESPACE")
    ns = resource_namespace if resource_namespace else None

    queue_config = queue_config_for_work_prefix(work_prefix, resource_namespace=ns)
    release_waiter = ReleaseWaiter(nats_url, control_subject(queue_config))
    await release_waiter.start()

    catalog = HandlerCatalog()
    jobs: SuperJobs | None = None

    async def _write_entered(context) -> None:
        assert jobs is not None
        incarnation = jobs.worker_incarnation
        entered_path = state_dir / f"entered_{worker_label}.json"
        entered_path.write_text(
            json.dumps(
                {
                    "execution_id": context.id,
                    "worker_label": worker_label,
                    "pid": os.getpid(),
                    "worker_id": incarnation.worker_id,
                    "incarnation_id": incarnation.incarnation_id,
                },
            ),
            encoding="utf-8",
        )

    if handler_mode == "cleanup_failed":

        @catalog.handler(HOLD_JOB, resources={RESOURCE_KEY: V1_EXCLUSIVE_CAPACITY})
        async def cleanup_failed_handler(context) -> str:
            await _write_entered(context)
            context.resource_cleanup_failed("deliberate test cleanup failure")
            raise RuntimeError("deliberate cleanup failure without acknowledgement")

    else:

        @catalog.handler(HOLD_JOB, resources={RESOURCE_KEY: V1_EXCLUSIVE_CAPACITY})
        async def hold_handler(context) -> str:
            await _write_entered(context)
            await release_waiter.wait_release(context.id)
            acknowledge_resource_cleanup(context)
            return worker_label

    broker = NatsBroker(nats_url, connect_timeout=5)
    jobs = SuperJobs(
        broker=broker,
        queue_config=queue_config,
        handlers=catalog,
        resource_capacities={RESOURCE_KEY: V1_EXCLUSIVE_CAPACITY},
        discovery=False,
    )
    await asyncio.wait_for(jobs.start(), timeout=STARTUP_TIMEOUT)
    try:
        _write_ready(state_dir, worker_label=worker_label, jobs=jobs)
        stop_path = state_dir / f"stop_{worker_label}"
        while not stop_path.is_file():
            await asyncio.sleep(0.05)
    finally:
        await release_waiter.close()
        await asyncio.wait_for(jobs.stop(), timeout=STARTUP_TIMEOUT)


def main() -> None:
    try:
        asyncio.run(_run())
    except TimeoutError as exc:
        print(str(exc), file=sys.stderr)
        raise SystemExit(12) from exc
    except Exception as exc:
        print(f"{type(exc).__name__}: {exc}", file=sys.stderr)
        raise SystemExit(3) from exc


if __name__ == "__main__":
    main()
