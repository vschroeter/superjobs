"""Pyright-positive checks for public HandlerCatalog registration typing."""

from __future__ import annotations

from contextlib import asynccontextmanager
from superjobs import (
    CLIField,
    Command,
    HandlerCatalog,
    JobContext,
    NoRequestJob,
    RequestJob,
    SuperJobs,
)
from superjobs_contract_example import (
    HEARTBEAT_JOB,
    MANIFEST_WITH_EVENTS_JOB,
    HeartbeatResult,
    ManifestEvent,
    ManifestRequest,
    ManifestResult,
)


def _catalog_decorators(catalog: HandlerCatalog) -> None:
    @catalog.handler(MANIFEST_WITH_EVENTS_JOB)
    async def request_handler(
        request: ManifestRequest, context: JobContext[ManifestEvent]
    ) -> ManifestResult:
        return ManifestResult(revision=request.device_id)

    @catalog.handler(HEARTBEAT_JOB)
    async def no_request_handler(context: JobContext[None]) -> HeartbeatResult:
        return HeartbeatResult(ok=True)

def _catalog_explicit_register(catalog: HandlerCatalog) -> None:
    async def request_handler(
        request: ManifestRequest, context: JobContext[ManifestEvent]
    ) -> ManifestResult:
        return ManifestResult(revision=request.device_id)

    async def no_request_handler(context: JobContext[None]) -> HeartbeatResult:
        return HeartbeatResult(ok=True)

    catalog.register(MANIFEST_WITH_EVENTS_JOB, request_handler)
    catalog.register(HEARTBEAT_JOB, no_request_handler)


def _catalog_marked_register(catalog: HandlerCatalog) -> None:
    @MANIFEST_WITH_EVENTS_JOB.handler
    async def request_marked(
        request: ManifestRequest, context: JobContext[ManifestEvent]
    ) -> ManifestResult:
        return ManifestResult(revision=request.device_id)

    catalog.register(request_marked)


def _catalog_provider_bind(catalog: HandlerCatalog) -> None:
    @asynccontextmanager
    async def request_provider():
        async def handler(
            request: ManifestRequest, context: JobContext[ManifestEvent]
        ) -> ManifestResult:
            return ManifestResult(revision=request.device_id)

        yield handler

    @asynccontextmanager
    async def no_request_provider():
        def handler(context: JobContext[None]) -> HeartbeatResult:
            return HeartbeatResult(ok=True)

        yield handler

    catalog.bind(MANIFEST_WITH_EVENTS_JOB, provider=request_provider)
    catalog.bind(HEARTBEAT_JOB, provider=no_request_provider)


def _catalog_cli_metadata(catalog: HandlerCatalog) -> None:
    command = Command(name="manifest", positional_fields=("device_id",))

    @catalog.handler(MANIFEST_WITH_EVENTS_JOB, cli=command)
    async def request_handler(
        request: ManifestRequest, context: JobContext[ManifestEvent]
    ) -> ManifestResult:
        return ManifestResult(revision=request.device_id)

def _presence_aware_jobs(
    request_job: RequestJob[ManifestRequest, ManifestResult, ManifestEvent, ...],
    no_request_job: NoRequestJob[HeartbeatResult, None],
) -> None:
    catalog = HandlerCatalog()

    @catalog.handler(request_job)
    async def request_handler(
        request: ManifestRequest, context: JobContext[ManifestEvent]
    ) -> ManifestResult:
        return ManifestResult(revision=request.device_id)

    @catalog.handler(no_request_job)
    async def no_request_handler(context: JobContext[None]) -> HeartbeatResult:
        return HeartbeatResult(ok=True)

    catalog.register(request_job, request_handler)
    catalog.register(no_request_job, no_request_handler)


def _runtime_catalog_cli_shortcut(jobs: SuperJobs) -> None:
    from superjobs.cli import CLIField as CliCLIField

    assert CliCLIField is CLIField

    @jobs.handler(MANIFEST_WITH_EVENTS_JOB, cli="manifest-cli")
    async def request_handler(
        request: ManifestRequest, context: JobContext[ManifestEvent]
    ) -> ManifestResult:
        return ManifestResult(revision=request.device_id)


def _catalog_bind_provider_alias(catalog: HandlerCatalog) -> None:
    @asynccontextmanager
    async def provider():
        async def handler(
            request: ManifestRequest, context: JobContext[ManifestEvent]
        ) -> ManifestResult:
            return ManifestResult(revision=request.device_id)

        yield handler

    catalog.bind_provider(MANIFEST_WITH_EVENTS_JOB, provider)
