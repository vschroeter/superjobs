"""Installed-wheel worker entry point for CLI process verification."""

from __future__ import annotations

import asyncio
import json
import os
import sys
from pathlib import Path

from faststream.nats import NatsBroker

from superjobs import SuperJobs
from superjobs_contract_worker_resources import WORKER_RESOURCE_TOKEN

from superjobs_contract_worker_example.handlers import register_contract_handlers

STARTUP_TIMEOUT = 30.0
STOP_POLL_INTERVAL = 0.05


def _require_env(name: str) -> str:
    value = os.environ.get(name)
    if not value:
        print(f"{name} must be set", file=sys.stderr)
        raise SystemExit(11)
    return value


def _ready_path(state_dir: Path) -> Path:
    return state_dir / "worker_ready.json"


def _stop_path(state_dir: Path) -> Path:
    return state_dir / "worker_stop.json"


def _write_ready(state_dir: Path, run_id: str) -> None:
    _ready_path(state_dir).write_text(
        json.dumps({"run_id": run_id, "phase": "ready", "pid": os.getpid()}) + "\n",
        encoding="utf-8",
    )


async def _wait_for_stop(state_dir: Path, run_id: str) -> None:
    while True:
        path = _stop_path(state_dir)
        if path.is_file():
            payload = json.loads(path.read_text(encoding="utf-8"))
            if payload.get("run_id") != run_id:
                raise AssertionError("stop run_id mismatch")
            return
        await asyncio.sleep(STOP_POLL_INTERVAL)


async def _run() -> None:
    nats_url = _require_env("SUPERJOBS_NATS_URL")
    run_id = _require_env("SUPERJOBS_CLI_RUN_ID")
    state_dir = Path(_require_env("SUPERJOBS_CLI_STATE_DIR"))
    ready = _ready_path(state_dir)
    if ready.is_file():
        raise RuntimeError(f"{ready} must not exist before worker startup")

    if not WORKER_RESOURCE_TOKEN:
        raise RuntimeError("worker resource token missing")
    jobs = SuperJobs(broker=NatsBroker(nats_url, connect_timeout=5))
    register_contract_handlers(jobs)
    await asyncio.wait_for(jobs.start(), timeout=STARTUP_TIMEOUT)
    try:
        _write_ready(state_dir, run_id)
        print(f"worker ready run_id={run_id}", flush=True)
        await _wait_for_stop(state_dir, run_id)
    finally:
        await asyncio.wait_for(jobs.stop(graceful=False), timeout=STARTUP_TIMEOUT)


def main() -> None:
    try:
        asyncio.run(_run())
    except TimeoutError as exc:
        print(str(exc), file=sys.stderr)
        raise SystemExit(12) from exc
    except Exception as exc:
        print(f"worker failed: {exc}", file=sys.stderr)
        raise SystemExit(1) from exc


if __name__ == "__main__":
    raise SystemExit(main())
