"""Pyright-negative checks: deliberate HandlerCatalog type errors."""

from __future__ import annotations

from contextlib import asynccontextmanager

from superjobs import HandlerCatalog, JobContext
from superjobs_contract_example import (
    HEARTBEAT_JOB,
    MANIFEST_WITH_EVENTS_JOB,
    HeartbeatResult,
    ManifestEvent,
    ManifestRequest,
    ManifestResult,
)


def _wrong_catalog_decorator_request(catalog: HandlerCatalog) -> None:
    @catalog.handler(MANIFEST_WITH_EVENTS_JOB)  # expect: reportArgumentType
    async def incompatible_request(
        request: str,
        context: JobContext[ManifestEvent],
    ) -> ManifestResult:
        return ManifestResult(revision=request)


def _wrong_catalog_decorator_context(catalog: HandlerCatalog) -> None:
    @catalog.handler(MANIFEST_WITH_EVENTS_JOB)  # expect: reportArgumentType
    async def incompatible_context(
        request: ManifestRequest,
        context: JobContext[str],
    ) -> ManifestResult:
        return ManifestResult(revision=request.device_id)


def _wrong_catalog_decorator_result(catalog: HandlerCatalog) -> None:
    @catalog.handler(MANIFEST_WITH_EVENTS_JOB)  # expect: reportArgumentType
    async def incompatible_result(
        request: ManifestRequest,
        context: JobContext[ManifestEvent],
    ) -> str:
        return "wrong"


def _wrong_catalog_register(catalog: HandlerCatalog) -> None:
    async def incompatible_request(
        request: str,
        context: JobContext[ManifestEvent],
    ) -> ManifestResult:
        return ManifestResult(revision=request)

    catalog.register(MANIFEST_WITH_EVENTS_JOB, incompatible_request)  # expect: reportCallIssue, reportArgumentType


def _wrong_catalog_provider_request(catalog: HandlerCatalog) -> None:
    @asynccontextmanager
    async def incompatible_provider():
        async def handler(
            request: str,
            context: JobContext[ManifestEvent],
        ) -> ManifestResult:
            return ManifestResult(revision=request)

        yield handler

    catalog.bind(MANIFEST_WITH_EVENTS_JOB, provider=incompatible_provider)  # expect: reportCallIssue, reportArgumentType


def _wrong_catalog_provider_no_request(catalog: HandlerCatalog) -> None:
    @asynccontextmanager
    async def incompatible_provider():
        async def handler(
            request: ManifestRequest,
            context: JobContext[None],
        ) -> HeartbeatResult:
            return HeartbeatResult(ok=True)

        yield handler

    catalog.bind(HEARTBEAT_JOB, provider=incompatible_provider)  # expect: reportCallIssue, reportArgumentType
