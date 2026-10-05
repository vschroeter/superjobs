"""NATS worker entry point for the contract-interface example."""

from __future__ import annotations

import asyncio
import os
from pathlib import Path

from faststream.nats import NatsBroker

from superjobs import SuperJobs
from catalog_handlers import CONTRACT_CATALOG

STARTUP_TIMEOUT = 30.0


def _require_env(name: str) -> str:
    value = os.environ.get(name)
    if not value:
        raise SystemExit(f"{name} must be set")
    return value


async def _run() -> None:
    nats_url = _require_env("NATS_URL")
    ready_path = Path(_require_env("SUPERJOBS_EXAMPLE_READY_FILE"))
    if ready_path.exists():
        raise SystemExit(
            f"{ready_path} must not exist before worker startup; "
            "choose a unique path per worker run (a stale marker can hide a dead worker)",
        )

    jobs = SuperJobs(
        broker=NatsBroker(nats_url, connect_timeout=5),
        handlers=CONTRACT_CATALOG,
    )
    try:
        await asyncio.wait_for(jobs.start(), timeout=STARTUP_TIMEOUT)
    except TimeoutError:
        raise SystemExit(f"worker did not start within {STARTUP_TIMEOUT}s") from None
    try:
        ready_path.write_text("ready", encoding="utf-8")
        print("worker ready", flush=True)
        try:
            await jobs.wait_until_stopped()
        finally:
            ready_path.unlink(missing_ok=True)
    finally:
        await jobs.stop()


def main() -> None:
    asyncio.run(_run())


if __name__ == "__main__":
    main()
