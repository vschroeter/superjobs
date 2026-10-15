"""Controller: remote submit, diagnostic ownership snapshot, confirmed recovery."""

from __future__ import annotations

import asyncio
import os

from faststream.nats import NatsBroker

from superjobs import SuperJobs
from superjobs.resources import TerminationConfirmation, V1_EXCLUSIVE_CAPACITY

from contract import DEVICE_RESOURCE_KEY, DEVICE_WORK_JOB, DeviceWorkRequest


async def main() -> None:
    nats_url = os.environ.get("SUPERJOBS_NATS_URL", "nats://localhost:4222")
    broker = NatsBroker(nats_url)
    jobs = SuperJobs(
        broker=broker,
        discovery=False,
        resource_capacities={DEVICE_RESOURCE_KEY: V1_EXCLUSIVE_CAPACITY},
    )
    await jobs.start()
    try:
        client = jobs.client(DEVICE_WORK_JOB)
        handle = await client.submit(DeviceWorkRequest(device_id="sensor-17"))
        result = await handle.result()
        print(result.revision)

        snapshot = await jobs.resources.snapshot()
        stale_owner = None
        for record in snapshot.resources:
            if record.owner is not None:
                stale_owner = record.owner
                print(record.resource_key, record.availability, record.owner.execution_id)

        # After the application supervisor confirms the owning processes exited:
        if stale_owner is not None:
            confirmation = TerminationConfirmation(
                incarnation=stale_owner.incarnation,
                confirmed=True,
                evidence="supervisor:pid=4242 cgroup=engine-a scope exited",
            )
            recovery = await jobs.resources.resolve_terminated_incarnation(confirmation)
            print(recovery.outcome.value)
    finally:
        await jobs.stop()


if __name__ == "__main__":
    asyncio.run(main())
