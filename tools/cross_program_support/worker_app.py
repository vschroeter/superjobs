"""Installed-wheel worker process for cross-program verification."""

from __future__ import annotations

import asyncio
import os
import sys
from pathlib import Path

from faststream.nats import NatsBroker

from superjobs import SuperJobs

from protocol import (
    assert_ready_absent,
    read_worker_stop,
    stop_path,
    write_ready,
)
from runtime_isolation import assert_worker_layout
from worker_handlers import register_contract_handlers, register_scenario_handlers

STARTUP_TIMEOUT = 30.0
STOP_POLL_INTERVAL = 0.05


def _require_env(name: str) -> str:
    value = os.environ.get(name)
    if not value:
        print(f"{name} must be set", file=sys.stderr)
        raise SystemExit(11)
    return value


async def _wait_for_stop(state_dir: Path, run_id: str) -> None:
    while True:
        path = stop_path(state_dir)
        if path.is_file():
            marker = read_worker_stop(state_dir)
            if marker.run_id != run_id:
                raise AssertionError(
                    f"stop run_id {marker.run_id!r} != expected {run_id!r}",
                )
            return
        await asyncio.sleep(STOP_POLL_INTERVAL)


async def _run() -> None:
    role_dir = Path(__file__).resolve().parent
    assert_worker_layout(role_dir)

    nats_url = _require_env("NATS_URL")
    run_id = _require_env("SUPERJOBS_CROSS_RUN_ID")
    scenario = _require_env("SUPERJOBS_CROSS_SCENARIO")
    state_dir = Path(_require_env("SUPERJOBS_CROSS_STATE_DIR"))
    assert_ready_absent(state_dir)

    jobs = SuperJobs(broker=NatsBroker(nats_url, connect_timeout=5))
    register_contract_handlers(jobs)
    if scenario != "control_missing_readiness":
        register_scenario_handlers(jobs, run_id)
    await asyncio.wait_for(jobs.start(), timeout=STARTUP_TIMEOUT)
    try:
        write_ready(state_dir, run_id)
        print(f"worker ready run_id={run_id} scenario={scenario}", flush=True)
        await _wait_for_stop(state_dir, run_id)
        print(f"worker stop acknowledged run_id={run_id}", flush=True)
    finally:
        await asyncio.wait_for(jobs.stop(), timeout=STARTUP_TIMEOUT)


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
    main()
