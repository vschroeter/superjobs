"""Pyright-negative checks: deliberate type errors (not part of the passing suite)."""

from __future__ import annotations

from superjobs import JobContext, NoRequestJob, SuperJobs
from superjobs_contract_example import (
    HEARTBEAT_JOB,
    MANIFEST_WITH_EVENTS_JOB,
    HeartbeatResult,
    ManifestEvent,
    ManifestRequest,
    ManifestResult,
)


async def _wrong_request_submit(jobs: SuperJobs) -> None:
    client = jobs.client(MANIFEST_WITH_EVENTS_JOB)
    await client.submit("not a manifest request")  # reportArgumentType


async def _wrong_event_emit(context: JobContext[ManifestEvent]) -> None:
    await context.emit("not an event")  # reportArgumentType


def _wrong_runtime_decorator_request(jobs: SuperJobs) -> None:
    @jobs.handler(MANIFEST_WITH_EVENTS_JOB)
    async def incompatible_request(
        request: str,
        context: JobContext[ManifestEvent],
    ) -> ManifestResult:
        return ManifestResult(revision=request)


def _wrong_runtime_decorator_context(jobs: SuperJobs) -> None:
    @jobs.handler(MANIFEST_WITH_EVENTS_JOB)
    async def incompatible_context(
        request: ManifestRequest,
        context: JobContext[str],
    ) -> ManifestResult:
        return ManifestResult(revision=request.device_id)


def _wrong_runtime_decorator_result(jobs: SuperJobs) -> None:
    @jobs.handler(MANIFEST_WITH_EVENTS_JOB)
    async def incompatible_result(
        request: ManifestRequest,
        context: JobContext[ManifestEvent],
    ) -> str:
        return "wrong"


def _wrong_explicit_register_request(jobs: SuperJobs) -> None:
    async def incompatible_request(
        request: str,
        context: JobContext[ManifestEvent],
    ) -> ManifestResult:
        return ManifestResult(revision=request)

    jobs.register(MANIFEST_WITH_EVENTS_JOB, incompatible_request)  # reportArgumentType


def _wrong_explicit_register_context(jobs: SuperJobs) -> None:
    async def incompatible_context(
        request: ManifestRequest,
        context: JobContext[str],
    ) -> ManifestResult:
        return ManifestResult(revision=request.device_id)

    jobs.register(MANIFEST_WITH_EVENTS_JOB, incompatible_context)  # reportArgumentType


def _wrong_explicit_register_result(jobs: SuperJobs) -> None:
    async def incompatible_result(
        request: ManifestRequest,
        context: JobContext[ManifestEvent],
    ) -> str:
        return "wrong"

    jobs.register(MANIFEST_WITH_EVENTS_JOB, incompatible_result)  # reportArgumentType


def _wrong_metadata_decorator_request() -> None:
    @MANIFEST_WITH_EVENTS_JOB.handler
    async def incompatible_request(
        request: str,
        context: JobContext[ManifestEvent],
    ) -> ManifestResult:
        return ManifestResult(revision=request)


def _wrong_metadata_decorator_context() -> None:
    @MANIFEST_WITH_EVENTS_JOB.handler
    async def incompatible_context(
        request: ManifestRequest,
        context: JobContext[str],
    ) -> ManifestResult:
        return ManifestResult(revision=request.device_id)


def _wrong_metadata_decorator_result() -> None:
    @MANIFEST_WITH_EVENTS_JOB.handler
    async def incompatible_result(
        request: ManifestRequest,
        context: JobContext[ManifestEvent],
    ) -> str:
        return "wrong"


def _missing_context_runtime_decorator(jobs: SuperJobs) -> None:
    @jobs.handler(MANIFEST_WITH_EVENTS_JOB)
    async def missing_context(request: ManifestRequest) -> ManifestResult:
        return ManifestResult(revision=request.device_id)


def _wrong_no_request_explicit_register(jobs: SuperJobs) -> None:
    async def needs_context_only(
        request: None,
        context: JobContext[None],
    ) -> HeartbeatResult:
        return HeartbeatResult(ok=True)

    jobs.register(HEARTBEAT_JOB, needs_context_only)  # reportArgumentType


def _wrong_explicitly_annotated_no_request_register(
    jobs: SuperJobs, job: NoRequestJob[HeartbeatResult, None]
) -> None:
    async def needs_context_only(
        request: None,
        context: JobContext[None],
    ) -> HeartbeatResult:
        return HeartbeatResult(ok=True)

    jobs.register(job, needs_context_only)  # reportArgumentType


def _wrong_no_request_runtime_decorator(jobs: SuperJobs) -> None:
    @jobs.handler(HEARTBEAT_JOB)
    async def needs_context_only(
        request: None,
        context: JobContext[None],
    ) -> HeartbeatResult:
        return HeartbeatResult(ok=True)


def _wrong_no_request_metadata_decorator() -> None:
    @HEARTBEAT_JOB.handler
    async def needs_context_only(
        request: None,
        context: JobContext[None],
    ) -> HeartbeatResult:
        return HeartbeatResult(ok=True)


def _wrong_marked_register(jobs: SuperJobs) -> None:
    @MANIFEST_WITH_EVENTS_JOB.handler
    async def marked(
        request: str,
        context: JobContext[ManifestEvent],
    ) -> str:
        return "wrong"

    jobs.register(marked)  # reportArgumentType


def _extra_required_runtime_parameter(jobs: SuperJobs) -> None:
    @jobs.handler(MANIFEST_WITH_EVENTS_JOB)
    async def extra(
        request: ManifestRequest, context: JobContext[ManifestEvent], *, required: bool
    ) -> ManifestResult:
        return ManifestResult(revision=request.device_id)


def _extra_required_metadata_parameter() -> None:
    @MANIFEST_WITH_EVENTS_JOB.handler
    def extra(
        payload: ManifestRequest, ctx: JobContext[ManifestEvent], required: bool
    ) -> ManifestResult:
        return ManifestResult(revision=payload.device_id)


def _wrong_preserved_parameter(jobs: SuperJobs) -> None:
    @jobs.handler(MANIFEST_WITH_EVENTS_JOB)
    def checked(
        payload: ManifestRequest, ctx: JobContext[ManifestEvent], *, trace: bool = False
    ) -> ManifestResult:
        return ManifestResult(revision=payload.device_id)

    request = ManifestRequest(device_id="probe")
    context: JobContext[ManifestEvent] = JobContext(MANIFEST_WITH_EVENTS_JOB)
    checked(payload=request, ctx=context, trace="wrong")  # reportArgumentType
    checked(payload="wrong", ctx=context)  # reportArgumentType
    checked(payload=request, ctx=context, unknown=True)  # reportCallIssue
