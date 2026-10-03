"""Intentional misuse of the public CLI registration interface."""
from __future__ import annotations

from collections.abc import Awaitable, Callable
from superjobs import Job, JobContext
from superjobs.cli import JobCLI
from superjobs_contract_example import (
    HEARTBEAT_JOB,
    MANIFEST_NO_EVENTS_JOB,
    MANIFEST_WITH_EVENTS_JOB,
    ManifestEvent,
    ManifestNoEventsRequest,
    ManifestNoEventsResult,
    ManifestRequest,
    ManifestResult,
)


def wrong_request(cli: JobCLI) -> None:
    async def handler(request: ManifestNoEventsRequest, context: JobContext[ManifestEvent]) -> ManifestResult:
        return ManifestResult(revision=request.bundle_id)
    cli.add("wrong-request", MANIFEST_WITH_EVENTS_JOB, handler=handler)  # expect: reportCallIssue, reportArgumentType


def wrong_context(cli: JobCLI) -> None:
    async def handler(request: ManifestRequest, context: JobContext[None]) -> ManifestResult:
        return ManifestResult(revision=request.device_id)
    cli.add("wrong-context", MANIFEST_WITH_EVENTS_JOB, handler=handler)  # expect: reportCallIssue, reportArgumentType


def wrong_result(cli: JobCLI) -> None:
    async def handler(request: ManifestRequest, context: JobContext[ManifestEvent]) -> str:
        return request.device_id
    cli.add("wrong-result", MANIFEST_WITH_EVENTS_JOB, handler=handler)  # expect: reportCallIssue, reportArgumentType


def wrong_request_factory(cli: JobCLI) -> None:
    def factory() -> Callable[[ManifestNoEventsRequest, JobContext[ManifestEvent]], Awaitable[ManifestResult]]:
        async def handler(request: ManifestNoEventsRequest, context: JobContext[ManifestEvent]) -> ManifestResult:
            return ManifestResult(revision=request.bundle_id)
        return handler
    cli.add("wrong-request-factory", MANIFEST_WITH_EVENTS_JOB, handler_factory=factory)  # expect: reportCallIssue, reportArgumentType


def wrong_context_factory(cli: JobCLI) -> None:
    def factory() -> Callable[[ManifestRequest, JobContext[None]], Awaitable[ManifestResult]]:
        async def handler(request: ManifestRequest, context: JobContext[None]) -> ManifestResult:
            return ManifestResult(revision=request.device_id)
        return handler
    cli.add("wrong-context-factory", MANIFEST_WITH_EVENTS_JOB, handler_factory=factory)  # expect: reportCallIssue, reportArgumentType


def wrong_result_factory(cli: JobCLI) -> None:
    def factory() -> Callable[[ManifestRequest, JobContext[ManifestEvent]], Awaitable[str]]:
        async def handler(request: ManifestRequest, context: JobContext[ManifestEvent]) -> str:
            return request.device_id
        return handler
    cli.add("wrong-result-factory", MANIFEST_WITH_EVENTS_JOB, handler_factory=factory)  # expect: reportCallIssue, reportArgumentType


def wrong_no_request_factory(cli: JobCLI) -> None:
    def factory() -> Callable[[JobContext[None]], Awaitable[str]]:
        async def handler(context: JobContext[None]) -> str:
            return "wrong"
        return handler
    cli.add("wrong-heartbeat-factory", HEARTBEAT_JOB, handler_factory=factory)  # expect: reportCallIssue, reportArgumentType


def widened_local(cli: JobCLI, job: Job[ManifestRequest, ManifestResult, ManifestEvent]) -> None:
    async def handler(request: ManifestRequest, context: JobContext[ManifestEvent]) -> ManifestResult:
        return ManifestResult(revision=request.device_id)
    cli.add("wide", job, handler=handler)  # expect: reportCallIssue, reportArgumentType


def conflicting_modes(cli: JobCLI) -> None:
    async def handler(request: ManifestRequest, context: JobContext[ManifestEvent]) -> ManifestResult:
        return ManifestResult(revision=request.device_id)
    cli.add("remote-with-handler", MANIFEST_WITH_EVENTS_JOB, handler=handler, remote_only=True)  # expect: reportCallIssue, reportArgumentType


def missing_local_handler(cli: JobCLI) -> None:
    cli.add("missing", HEARTBEAT_JOB)  # expect: reportCallIssue


def invalid_field_options_value(cli: JobCLI) -> None:
    async def handler(
        request: ManifestNoEventsRequest,
        context: JobContext[None],
    ) -> ManifestNoEventsResult:
        return ManifestNoEventsResult(accepted=True)

    cli.add(  # expect: reportCallIssue
        "bad-field-options",
        MANIFEST_NO_EVENTS_JOB,
        handler=handler,
        field_options={"bundle_id": "not-a-cli-field"},  # expect: reportArgumentType
    )


def invalid_positional_fields_type(cli: JobCLI) -> None:
    async def handler(
        request: ManifestNoEventsRequest,
        context: JobContext[None],
    ) -> ManifestNoEventsResult:
        return ManifestNoEventsResult(accepted=True)

    cli.add(  # expect: reportCallIssue
        "bad-positional",
        MANIFEST_NO_EVENTS_JOB,
        handler=handler,
        positional_fields=["bundle_id"],  # expect: reportArgumentType
    )
