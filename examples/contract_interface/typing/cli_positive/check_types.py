"""Pyright-positive JobCLI handler registration checks."""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from typing import Any, assert_type, cast

from superjobs import Job, JobContext
from superjobs.cli import CLIField, JobCLI
from superjobs_contract_example import (
    HEARTBEAT_JOB,
    MANIFEST_NO_EVENTS_JOB,
    MANIFEST_WITH_EVENTS_JOB,
    TELEMETRY_INGEST_JOB,
    HeartbeatResult,
    ManifestEvent,
    ManifestNoEventsRequest,
    ManifestNoEventsResult,
    ManifestRequest,
    ManifestResult,
    TelemetrySample,
)


def _async_request_handler(cli: JobCLI) -> None:
    async def handler(
        request: ManifestRequest,
        context: JobContext[ManifestEvent],
    ) -> ManifestResult:
        return ManifestResult(revision=request.device_id)

    cli.add("manifest-async", MANIFEST_WITH_EVENTS_JOB, handler=handler)


def _sync_request_handler(cli: JobCLI) -> None:
    def handler(
        request: ManifestNoEventsRequest,
        context: JobContext[None],
    ) -> ManifestNoEventsResult:
        return ManifestNoEventsResult(accepted=True)

    cli.add("manifest-sync", MANIFEST_NO_EVENTS_JOB, handler=handler)


def _no_request_handler(cli: JobCLI) -> None:
    async def heartbeat(context: JobContext[None]) -> HeartbeatResult:
        return HeartbeatResult(ok=True)

    cli.add("heartbeat", HEARTBEAT_JOB, handler=heartbeat)


def _no_result_handler(cli: JobCLI) -> None:
    async def ingest(
        request: TelemetrySample,
        context: JobContext[None],
    ) -> None:
        await context.log(request.metric)

    cli.add("ingest", TELEMETRY_INGEST_JOB, handler=ingest)


def _marked_handler(cli: JobCLI) -> None:
    @MANIFEST_NO_EVENTS_JOB.handler
    async def marked(
        request: ManifestNoEventsRequest,
        context: JobContext[None],
    ) -> ManifestNoEventsResult:
        return ManifestNoEventsResult(accepted=True)

    cli.add("manifest-marked", MANIFEST_NO_EVENTS_JOB, handler=marked)


def _remote_only_widened(cli: JobCLI) -> None:
    job: Job[ManifestRequest, ManifestResult, ManifestEvent] = MANIFEST_WITH_EVENTS_JOB
    cli.add("remote-manifest", job, remote_only=True)


def _lazy_async_request_factory(cli: JobCLI) -> None:
    def factory() -> (
        Callable[
            [ManifestRequest, JobContext[ManifestEvent]],
            Awaitable[ManifestResult],
        ]
    ):
        async def lazy(
            request: ManifestRequest,
            context: JobContext[ManifestEvent],
        ) -> ManifestResult:
            return ManifestResult(revision=request.device_id)

        return lazy

    cli.add("lazy-async", MANIFEST_WITH_EVENTS_JOB, handler_factory=factory)


def _lazy_sync_request_factory(cli: JobCLI) -> None:
    def factory() -> Callable[
        [ManifestNoEventsRequest, JobContext[None]],
        ManifestNoEventsResult,
    ]:
        def lazy(
            request: ManifestNoEventsRequest,
            context: JobContext[None],
        ) -> ManifestNoEventsResult:
            return ManifestNoEventsResult(accepted=True)

        return lazy

    cli.add("lazy-sync", MANIFEST_NO_EVENTS_JOB, handler_factory=factory)


def _lazy_async_no_request_factory(cli: JobCLI) -> None:
    def factory() -> Callable[[JobContext[None]], Awaitable[HeartbeatResult]]:
        async def lazy(context: JobContext[None]) -> HeartbeatResult:
            return HeartbeatResult(ok=True)

        return lazy

    cli.add("lazy-heartbeat-async", HEARTBEAT_JOB, handler_factory=factory)


def _lazy_sync_no_request_factory(cli: JobCLI) -> None:
    def factory() -> Callable[[JobContext[None]], HeartbeatResult]:
        def lazy(context: JobContext[None]) -> HeartbeatResult:
            return HeartbeatResult(ok=True)

        return lazy

    cli.add("lazy-heartbeat-sync", HEARTBEAT_JOB, handler_factory=factory)


def _field_customization_kwargs(cli: JobCLI) -> None:
    async def handler(
        request: ManifestNoEventsRequest,
        context: JobContext[None],
    ) -> ManifestNoEventsResult:
        return ManifestNoEventsResult(accepted=True)

    cli.add(
        "manifest-fields",
        MANIFEST_NO_EVENTS_JOB,
        handler=handler,
        positional_fields=("bundle_id",),
        field_options={"bundle_id": CLIField(help="Bundle identifier.")},
    )


def _typed_runtime_factories(cli: JobCLI) -> None:
    from collections.abc import AsyncIterator
    from contextlib import asynccontextmanager

    from faststream.nats import NatsBroker

    from superjobs import InMemoryTransport, SuperJobs
    from superjobs.cli import RemoteRuntimeFactory

    @asynccontextmanager
    async def local_runtime() -> AsyncIterator[SuperJobs]:
        jobs = SuperJobs(transport=InMemoryTransport())
        async with jobs:
            yield jobs

    @asynccontextmanager
    async def remote_runtime() -> AsyncIterator[SuperJobs]:
        jobs = SuperJobs(broker=NatsBroker("nats://127.0.0.1:4222"))
        async with jobs:
            yield jobs

    remote_factory = cast(RemoteRuntimeFactory, remote_runtime)
    assert_type(remote_factory, RemoteRuntimeFactory)

    typed = JobCLI(
        local_runtime_factory=local_runtime,
        remote_runtime_factory=remote_factory,
    )
    assert_type(typed, JobCLI)


def _contract_cli_example_registration() -> None:
    from superjobs_contract_cli_example import build_cli as example_build

    assert_type(example_build(), JobCLI)
