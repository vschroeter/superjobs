"""Runnable import proof: shared contract Job + discovery reads (no worker code)."""

from __future__ import annotations

import asyncio
from collections.abc import Callable
from datetime import UTC, datetime, timedelta

from superjobs import InMemoryDiscoveryBackend, InMemoryTransport, SuperJobs
from superjobs.discovery.capabilities import encode_application_capabilities
from superjobs.discovery.models import WorkerRegistrationState
from superjobs_contract_example import LOCALE_DISCOVERY_JOB, LocaleCapability

Clock = Callable[[], datetime]


async def run_discovery_import_probe(clock: Clock | None = None) -> None:
    clock_fn: Clock = clock or (lambda: datetime.now(tz=UTC))
    now = clock_fn()
    if now.tzinfo is None:
        raise ValueError("clock must return timezone-aware UTC datetimes")
    start = now.astimezone(UTC)
    store = InMemoryDiscoveryBackend(clock=clock_fn)
    raw = encode_application_capabilities(
        LOCALE_DISCOVERY_JOB,
        LocaleCapability(locale="probe"),
    )
    stored = store.build_registration(
        worker_id="probe-worker",
        job=LOCALE_DISCOVERY_JOB.identity,
        state=WorkerRegistrationState.READY,
        registered_at=start,
        last_seen_at=start,
        expires_at=start + timedelta(seconds=30),
        capabilities=raw,
    )
    await store.write_registration(stored, max_envelope_bytes=store.config.max_envelope_bytes)
    jobs = SuperJobs(transport=InMemoryTransport(discovery_store=store))
    async with jobs:
        workers = await jobs.client(LOCALE_DISCOVERY_JOB).workers()
        assert workers[0].capabilities == LocaleCapability(locale="probe")


async def main() -> None:
    await run_discovery_import_probe()
    print("discovery_import_probe: OK")


if __name__ == "__main__":
    asyncio.run(main())
