"""NATS worker declaring capacity and serving resource-bound handlers."""

from __future__ import annotations

import asyncio
import os

from faststream.nats import NatsBroker

from superjobs import SuperJobs
from superjobs.resources import V1_EXCLUSIVE_CAPACITY

from contract import DEVICE_RESOURCE_KEY
from handlers import catalog


async def main() -> None:
    nats_url = os.environ.get("SUPERJOBS_NATS_URL", "nats://localhost:4222")
    broker = NatsBroker(nats_url)
    jobs = SuperJobs(
        broker=broker,
        handlers=catalog,
        discovery=False,
        resource_capacities={DEVICE_RESOURCE_KEY: V1_EXCLUSIVE_CAPACITY},
    )
    await jobs.serve()


if __name__ == "__main__":
    asyncio.run(main())
