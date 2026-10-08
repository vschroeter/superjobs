"""Negative worker presence typing checks."""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from contextlib import asynccontextmanager

from superjobs import HandlerCatalog, JobContext, SuperJobs
from superjobs.jobs.job import CapabilityRequestJob
from superjobs_contract_example import (
    HEARTBEAT_JOB,
    LOCALE_DISCOVERY_JOB,
    MANIFEST_WITH_EVENTS_JOB,
    HeartbeatResult,
    LocaleCapability,
    ManifestEvent,
    ManifestRequest,
    ManifestResult,
)

jobs = SuperJobs()
catalog = HandlerCatalog()

_LOCALE_JOB: CapabilityRequestJob[
    ManifestRequest,
    ManifestResult,
    None,
    ...,
    LocaleCapability,
] = LOCALE_DISCOVERY_JOB


def _wrong_static_runtime_capabilities() -> None:
    @jobs.handler(LOCALE_DISCOVERY_JOB, capabilities="bad")  # expect: reportCallIssue, reportArgumentType
    async def handler(
        request: ManifestRequest, context: JobContext[None]
    ) -> ManifestResult:
        return ManifestResult(revision=request.device_id)


def _wrong_runtime_sync_factory() -> None:
    def factory() -> str:
        return "bad"

    @jobs.handler(_LOCALE_JOB, capabilities=factory)  # expect: reportCallIssue, reportArgumentType
    async def handler(
        request: ManifestRequest, context: JobContext[None]
    ) -> ManifestResult:
        return ManifestResult(revision=request.device_id)


def _wrong_runtime_async_factory() -> None:
    async def factory() -> str:
        return "bad"

    @jobs.handler(_LOCALE_JOB, capabilities=factory)  # expect: reportCallIssue, reportArgumentType
    async def handler(
        request: ManifestRequest, context: JobContext[None]
    ) -> ManifestResult:
        return ManifestResult(revision=request.device_id)


def _wrong_catalog_provider_capabilities() -> None:
    def factory() -> str:
        return "bad"

    @asynccontextmanager
    async def provider():
        async def inner(
            request: ManifestRequest, context: JobContext[None]
        ) -> ManifestResult:
            return ManifestResult(revision=request.device_id)

        yield inner

    catalog.bind_provider(  # expect: reportCallIssue
        _LOCALE_JOB,
        provider,
        capabilities=factory,  # expect: reportArgumentType
    )


def _wrong_catalog_bind_capabilities() -> None:
    @asynccontextmanager
    async def provider():
        async def inner(
            request: ManifestRequest, context: JobContext[None]
        ) -> ManifestResult:
            return ManifestResult(revision=request.device_id)

        yield inner

    def bad_caps() -> str:
        return "bad"

    catalog.bind(  # expect: reportCallIssue
        _LOCALE_JOB,
        provider=provider,
        capabilities=bad_caps,  # expect: reportArgumentType
    )


def _wrong_register_capabilities() -> None:
    async def handler(
        request: ManifestRequest, context: JobContext[None]
    ) -> ManifestResult:
        return ManifestResult(revision=request.device_id)

    jobs.register(  # expect: reportCallIssue
        _LOCALE_JOB,
        handler,
        capabilities="bad",  # expect: reportArgumentType
    )


def _no_cap_job_non_none_capabilities() -> None:
    @jobs.handler(HEARTBEAT_JOB, capabilities=LocaleCapability(locale="de"))  # expect: reportCallIssue, reportArgumentType
    async def handler(context: JobContext[None]) -> HeartbeatResult:
        return HeartbeatResult(ok=True)


def _widened_update_bad() -> None:
    async def _use() -> None:
        from superjobs import RequestJob
        widened: RequestJob[ManifestRequest, ManifestResult, None, ...] = LOCALE_DISCOVERY_JOB
        handle = jobs.worker(widened)
        await handle.update_capabilities("bad")  # expect: reportArgumentType

    _ = _use


def _wrong_handler_request() -> None:
    @jobs.handler(_LOCALE_JOB, capabilities=LocaleCapability(locale="de"))  # expect: reportArgumentType
    async def handler(
        request: str,
        context: JobContext[None],
    ) -> ManifestResult:
        return ManifestResult(revision=request)


def _wrong_handler_result() -> None:
    @jobs.handler(_LOCALE_JOB, capabilities=LocaleCapability(locale="de"))  # expect: reportArgumentType
    async def handler(
        request: ManifestRequest,
        context: JobContext[None],
    ) -> str:
        return "wrong"


def _wrong_handler_context() -> None:
    @jobs.handler(_LOCALE_JOB, capabilities=LocaleCapability(locale="de"))  # expect: reportArgumentType
    async def handler(
        request: ManifestRequest,
        context: JobContext[str],
    ) -> ManifestResult:
        return ManifestResult(revision=request.device_id)


def _wrong_catalog_register() -> None:
    async def handler(
        request: str,
        context: JobContext[None],
    ) -> ManifestResult:
        return ManifestResult(revision=request)

    catalog.register(  # expect: reportCallIssue
        _LOCALE_JOB,
        handler,  # expect: reportArgumentType
        capabilities=LocaleCapability(locale="de"),
    )


def _legacy_job_with_capability_value() -> None:
    @jobs.handler(MANIFEST_WITH_EVENTS_JOB, capabilities=LocaleCapability(locale="de"))  # expect: reportCallIssue, reportArgumentType
    async def handler(
        request: ManifestRequest,
        context: JobContext[ManifestEvent],
    ) -> ManifestResult:
        return ManifestResult(revision=request.device_id)


def _marked_no_cap_with_capabilities() -> None:
    @HEARTBEAT_JOB.handler
    async def marked(context: JobContext[None]) -> HeartbeatResult:
        return HeartbeatResult(ok=True)

    jobs.register(marked, capabilities=LocaleCapability(locale="de"))  # expect: reportCallIssue


def _wrong_update_numeric() -> None:
    async def _use() -> None:
        handle = jobs.worker(LOCALE_DISCOVERY_JOB)
        await handle.update_capabilities(1)  # expect: reportArgumentType

    _ = _use


def _wrong_catalog_static_capability() -> None:
    @catalog.handler(LOCALE_DISCOVERY_JOB, capabilities="bad")  # expect: reportCallIssue, reportArgumentType
    async def handler(request: ManifestRequest, context: JobContext[None]) -> ManifestResult:
        return ManifestResult(revision=request.device_id)


def _wrong_no_request_static_capability() -> None:
    from superjobs import Job
    declared = Job("typing.bad.capability", capabilities=LocaleCapability)
    @jobs.handler(declared, capabilities=42)  # expect: reportCallIssue, reportArgumentType
    async def handler(context: JobContext[None]) -> None:
        return None


def _wrong_catalog_async_factory() -> None:
    async def wrong_factory() -> str:
        return "bad"

    async def handler(request: ManifestRequest, context: JobContext[None]) -> ManifestResult:
        return ManifestResult(revision=request.device_id)

    catalog.register(  # expect: reportCallIssue
        LOCALE_DISCOVERY_JOB,
        handler,
        capabilities=wrong_factory,  # expect: reportArgumentType
    )
