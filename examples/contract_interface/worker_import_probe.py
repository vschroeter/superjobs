"""Runnable import proof: contract Job + HandlerCatalog worker (no worker package import)."""

from __future__ import annotations

import asyncio

from superjobs import HandlerCatalog, InMemoryDiscoveryBackend, InMemoryTransport, JobContext, SuperJobs
from superjobs_contract_example import (
    LOCALE_DISCOVERY_JOB,
    LocaleCapability,
    ManifestRequest,
    ManifestResult,
)


class _LocaleFactory:
    def __init__(self) -> None:
        self.calls = 0

    def __call__(self) -> LocaleCapability:
        self.calls += 1
        return LocaleCapability(locale=f"factory-{self.calls}")


async def run_worker_import_probe() -> None:
    discovery_store = InMemoryDiscoveryBackend()
    worker_transport = InMemoryTransport(discovery_store=discovery_store)
    producer_transport = InMemoryTransport(discovery_store=discovery_store)

    catalog = HandlerCatalog()
    locale_factory = _LocaleFactory()

    @catalog.handler(LOCALE_DISCOVERY_JOB, capabilities=locale_factory)
    async def handle_manifest(
        request: ManifestRequest,
        context: JobContext[None],
    ) -> ManifestResult:
        return ManifestResult(revision=f"{request.device_id}-r1")

    worker_jobs = SuperJobs(transport=worker_transport, handlers=catalog)
    producer_jobs = SuperJobs(transport=producer_transport)

    async with worker_jobs:
        assert locale_factory.calls == 1
        typed = await producer_jobs.discovery.workers(LOCALE_DISCOVERY_JOB)
        assert len(typed) == 1
        assert typed[0].capabilities == LocaleCapability(locale="factory-1")
        assert typed[0].job == LOCALE_DISCOVERY_JOB.identity

        raw_workers = await producer_jobs.discovery.workers(LOCALE_DISCOVERY_JOB.identity)
        assert len(raw_workers) == 1
        assert raw_workers[0].worker_id == worker_jobs.worker_id
        assert raw_workers[0].capabilities is not None

        handle = worker_jobs.worker(LOCALE_DISCOVERY_JOB)
        await handle.update_capabilities(LocaleCapability(locale="manual"))
        refreshed = await producer_jobs.client(LOCALE_DISCOVERY_JOB).workers()
        assert refreshed[0].capabilities == LocaleCapability(locale="manual")

        await handle.refresh_capabilities()
        assert locale_factory.calls == 2
        after_refresh = await producer_jobs.discovery.workers(LOCALE_DISCOVERY_JOB)
        assert after_refresh[0].capabilities == LocaleCapability(locale="factory-2")

        await handle.update_capabilities(None)
        none_caps = await producer_jobs.discovery.workers(LOCALE_DISCOVERY_JOB)
        assert none_caps[0].capabilities is None

        result = await worker_jobs.client(LOCALE_DISCOVERY_JOB).run(
            ManifestRequest(device_id="probe-device"),
        )
        assert result == ManifestResult(revision="probe-device-r1")


async def main() -> None:
    await run_worker_import_probe()
    print("worker_import_probe: OK")


if __name__ == "__main__":
    asyncio.run(main())
