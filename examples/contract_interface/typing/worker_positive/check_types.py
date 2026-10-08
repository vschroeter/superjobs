"""Positive worker presence typing checks."""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from contextlib import asynccontextmanager
from typing import assert_type

from superjobs import HandlerCatalog, JobContext, LocalWorkerHandle, RequestJob, SuperJobs
from superjobs_contract_example import (
    HEARTBEAT_JOB,
    HeartbeatResult,
    LOCALE_DISCOVERY_JOB,
    LocaleCapability,
    ManifestRequest,
    ManifestResult,
)

jobs = SuperJobs()
catalog = HandlerCatalog()


def _static_runtime_capabilities() -> None:
    @jobs.handler(LOCALE_DISCOVERY_JOB, capabilities=LocaleCapability(locale="de"))
    async def handler(
        request: ManifestRequest, context: JobContext[None]
    ) -> ManifestResult:
        return ManifestResult(revision=request.device_id)

    handle = jobs.worker(LOCALE_DISCOVERY_JOB)
    assert_type(handle, LocalWorkerHandle[LocaleCapability])


def _factory_runtime_capabilities() -> None:
    def factory() -> LocaleCapability:
        return LocaleCapability(locale="fr")

    async def async_factory() -> LocaleCapability:
        return LocaleCapability(locale="it")

    @jobs.handler(LOCALE_DISCOVERY_JOB, capabilities=factory)
    async def sync_factory_handler(
        request: ManifestRequest, context: JobContext[None]
    ) -> ManifestResult:
        return ManifestResult(revision=request.device_id)

    @jobs.handler(LOCALE_DISCOVERY_JOB, capabilities=async_factory)
    async def async_factory_handler(
        request: ManifestRequest, context: JobContext[None]
    ) -> ManifestResult:
        return ManifestResult(revision=request.device_id)

    assert_type(jobs.worker(LOCALE_DISCOVERY_JOB), LocalWorkerHandle[LocaleCapability])


def _optional_none_capability() -> None:
    @jobs.handler(LOCALE_DISCOVERY_JOB, capabilities=None)
    async def handler(
        request: ManifestRequest, context: JobContext[None]
    ) -> ManifestResult:
        return ManifestResult(revision=request.device_id)


def _catalog_provider_capabilities() -> None:
    def provider_caps() -> LocaleCapability:
        return LocaleCapability(locale="es")

    @asynccontextmanager
    async def provider():
        async def inner(
            request: ManifestRequest, context: JobContext[None]
        ) -> ManifestResult:
            return ManifestResult(revision=request.device_id)

        yield inner

    catalog.bind_provider(
        LOCALE_DISCOVERY_JOB,
        provider,
        capabilities=provider_caps,
    )


def _catalog_bind_capabilities() -> None:
    @asynccontextmanager
    async def provider():
        async def inner(
            request: ManifestRequest, context: JobContext[None]
        ) -> ManifestResult:
            return ManifestResult(revision=request.device_id)

        yield inner

    catalog.bind(
        LOCALE_DISCOVERY_JOB,
        provider=provider,
        capabilities=lambda: LocaleCapability(locale="pt"),
    )


def _register_with_capabilities() -> None:
    async def handler(
        request: ManifestRequest, context: JobContext[None]
    ) -> ManifestResult:
        return ManifestResult(revision=request.device_id)

    jobs.register(
        LOCALE_DISCOVERY_JOB,
        handler,
        capabilities=LocaleCapability(locale="nl"),
    )


def _legacy_no_cap_none_only() -> None:
    @jobs.handler(HEARTBEAT_JOB, capabilities=None)
    async def handler(context: JobContext[None]) -> HeartbeatResult:
        return HeartbeatResult(ok=True)

    assert_type(jobs.worker(HEARTBEAT_JOB), LocalWorkerHandle[None])


def _optional_capability_value() -> None:
    cap: LocaleCapability | None = None
    factory: Callable[[], LocaleCapability | None | Awaitable[LocaleCapability | None]] = (
        lambda: cap
    )

    @jobs.handler(LOCALE_DISCOVERY_JOB, capabilities=factory)
    async def handler(
        request: ManifestRequest, context: JobContext[None]
    ) -> ManifestResult:
        return ManifestResult(revision=request.device_id)


def _explicit_widened_none_update() -> None:
    async def _use() -> None:
        handle = jobs.worker(LOCALE_DISCOVERY_JOB)
        none_cap: LocaleCapability | None = None
        await handle.update_capabilities(none_cap)

    _ = _use


def _constructor_capability_types() -> None:
    cap = LocaleCapability(locale="de")
    assert_type(cap, LocaleCapability)

async def _typed_updates_and_widened_none() -> None:
    from superjobs import RequestJob
    typed = jobs.worker(LOCALE_DISCOVERY_JOB)
    await typed.update_capabilities(LocaleCapability(locale="de"))
    await typed.update_capabilities(None)
    await typed.refresh_capabilities()
    widened: RequestJob[ManifestRequest, ManifestResult, None, ...] = LOCALE_DISCOVERY_JOB
    await jobs.worker(widened).update_capabilities(None)


async def _parameter_with_erased_capability(widened: RequestJob[ManifestRequest, ManifestResult, None, ...]) -> None:
    assert_type(jobs.worker(widened), LocalWorkerHandle[None])
    await jobs.worker(widened).update_capabilities(None)


def _catalog_optional_factories_and_plain_request_none() -> None:
    from superjobs_contract_example import MANIFEST_WITH_EVENTS_JOB, ManifestEvent

    async def optional_factory() -> LocaleCapability | None:
        return None

    async def handler(request: ManifestRequest, context: JobContext[None]) -> ManifestResult:
        return ManifestResult(revision=request.device_id)

    catalog.register(LOCALE_DISCOVERY_JOB, handler, capabilities=optional_factory)

    @catalog.handler(LOCALE_DISCOVERY_JOB, capabilities=optional_factory)
    async def decorated(request: ManifestRequest, context: JobContext[None]) -> ManifestResult:
        return ManifestResult(revision=request.device_id)

    @asynccontextmanager
    async def provider():
        async def callback(request: ManifestRequest, context: JobContext[ManifestEvent]) -> ManifestResult:
            return ManifestResult(revision=request.device_id)
        yield callback

    catalog.bind(MANIFEST_WITH_EVENTS_JOB, provider=provider, capabilities=None)


def _no_request_capability_forms() -> None:
    from superjobs import Job
    declared = Job("typing.local.capability", capabilities=LocaleCapability)

    def handler(context: JobContext[None]) -> None:
        return None

    jobs.register(declared, handler, capabilities=LocaleCapability(locale="de"))
    catalog.register(declared, handler, capabilities=lambda: None)
    assert_type(jobs.worker(declared), LocalWorkerHandle[LocaleCapability])

    @asynccontextmanager
    async def provider():
        yield handler

    catalog.bind_provider(declared, provider, capabilities=lambda: LocaleCapability(locale="de"))
