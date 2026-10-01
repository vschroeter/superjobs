"""Pyright-negative checks: deliberate type errors (not part of the passing suite)."""

from __future__ import annotations

from superjobs import Job, JobContext, NoRequestJob, SuperJobs
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
    await client.submit("not a manifest request")  # expect: reportArgumentType


async def _wrong_event_emit(context: JobContext[ManifestEvent]) -> None:
    await context.emit("not an event")  # expect: reportArgumentType


def _wrong_runtime_decorator_request(jobs: SuperJobs) -> None:
    @jobs.handler(MANIFEST_WITH_EVENTS_JOB)  # expect: reportArgumentType
    async def incompatible_request(
        request: str,
        context: JobContext[ManifestEvent],
    ) -> ManifestResult:
        return ManifestResult(revision=request)


def _wrong_runtime_decorator_context(jobs: SuperJobs) -> None:
    @jobs.handler(MANIFEST_WITH_EVENTS_JOB)  # expect: reportArgumentType
    async def incompatible_context(
        request: ManifestRequest,
        context: JobContext[str],
    ) -> ManifestResult:
        return ManifestResult(revision=request.device_id)


def _wrong_runtime_decorator_result(jobs: SuperJobs) -> None:
    @jobs.handler(MANIFEST_WITH_EVENTS_JOB)  # expect: reportArgumentType
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

    jobs.register(MANIFEST_WITH_EVENTS_JOB, incompatible_request)  # expect: reportCallIssue, reportArgumentType


def _wrong_explicit_register_context(jobs: SuperJobs) -> None:
    async def incompatible_context(
        request: ManifestRequest,
        context: JobContext[str],
    ) -> ManifestResult:
        return ManifestResult(revision=request.device_id)

    jobs.register(MANIFEST_WITH_EVENTS_JOB, incompatible_context)  # expect: reportCallIssue, reportArgumentType


def _wrong_explicit_register_result(jobs: SuperJobs) -> None:
    async def incompatible_result(
        request: ManifestRequest,
        context: JobContext[ManifestEvent],
    ) -> str:
        return "wrong"

    jobs.register(MANIFEST_WITH_EVENTS_JOB, incompatible_result)  # expect: reportCallIssue, reportArgumentType


def _wrong_metadata_decorator_request() -> None:
    @MANIFEST_WITH_EVENTS_JOB.handler  # expect: reportArgumentType
    async def incompatible_request(
        request: str,
        context: JobContext[ManifestEvent],
    ) -> ManifestResult:
        return ManifestResult(revision=request)


def _wrong_metadata_decorator_context() -> None:
    @MANIFEST_WITH_EVENTS_JOB.handler  # expect: reportArgumentType
    async def incompatible_context(
        request: ManifestRequest,
        context: JobContext[str],
    ) -> ManifestResult:
        return ManifestResult(revision=request.device_id)


def _wrong_metadata_decorator_result() -> None:
    @MANIFEST_WITH_EVENTS_JOB.handler  # expect: reportArgumentType
    async def incompatible_result(
        request: ManifestRequest,
        context: JobContext[ManifestEvent],
    ) -> str:
        return "wrong"


def _missing_context_runtime_decorator(jobs: SuperJobs) -> None:
    @jobs.handler(MANIFEST_WITH_EVENTS_JOB)  # expect: reportArgumentType
    async def missing_context(request: ManifestRequest) -> ManifestResult:
        return ManifestResult(revision=request.device_id)


def _missing_context_explicit_register(jobs: SuperJobs) -> None:
    async def missing_context(request: ManifestRequest) -> ManifestResult:
        return ManifestResult(revision=request.device_id)

    jobs.register(MANIFEST_WITH_EVENTS_JOB, missing_context)  # expect: reportCallIssue, reportArgumentType


def _missing_context_metadata_decorator() -> None:
    @MANIFEST_WITH_EVENTS_JOB.handler  # expect: reportArgumentType
    async def missing_context(request: ManifestRequest) -> ManifestResult:
        return ManifestResult(revision=request.device_id)


def _wrong_no_request_explicit_register(jobs: SuperJobs) -> None:
    async def needs_context_only(
        request: None,
        context: JobContext[None],
    ) -> HeartbeatResult:
        return HeartbeatResult(ok=True)

    jobs.register(HEARTBEAT_JOB, needs_context_only)  # expect: reportCallIssue, reportArgumentType


def _wrong_explicitly_annotated_no_request_register(
    jobs: SuperJobs, job: NoRequestJob[HeartbeatResult, None]
) -> None:
    async def needs_context_only(
        request: None,
        context: JobContext[None],
    ) -> HeartbeatResult:
        return HeartbeatResult(ok=True)

    jobs.register(job, needs_context_only)  # expect: reportCallIssue, reportArgumentType


def _wrong_no_request_runtime_decorator(jobs: SuperJobs) -> None:
    @jobs.handler(HEARTBEAT_JOB)  # expect: reportArgumentType
    async def needs_context_only(
        request: None,
        context: JobContext[None],
    ) -> HeartbeatResult:
        return HeartbeatResult(ok=True)


def _wrong_no_request_metadata_decorator() -> None:
    @HEARTBEAT_JOB.handler  # expect: reportArgumentType
    async def needs_context_only(
        request: None,
        context: JobContext[None],
    ) -> HeartbeatResult:
        return HeartbeatResult(ok=True)


def _zero_argument_no_request_runtime_decorator(jobs: SuperJobs) -> None:
    @jobs.handler(HEARTBEAT_JOB)  # expect: reportArgumentType
    async def missing_context() -> HeartbeatResult:
        return HeartbeatResult(ok=True)


def _zero_argument_no_request_explicit_register(jobs: SuperJobs) -> None:
    async def missing_context() -> HeartbeatResult:
        return HeartbeatResult(ok=True)

    jobs.register(HEARTBEAT_JOB, missing_context)  # expect: reportCallIssue, reportArgumentType


def _zero_argument_no_request_metadata_decorator() -> None:
    @HEARTBEAT_JOB.handler  # expect: reportArgumentType
    async def missing_context() -> HeartbeatResult:
        return HeartbeatResult(ok=True)


def _wrong_marked_register(jobs: SuperJobs) -> None:
    @MANIFEST_WITH_EVENTS_JOB.handler  # expect: reportArgumentType
    async def marked(
        request: str,
        context: JobContext[ManifestEvent],
    ) -> str:
        return "wrong"

    jobs.register(marked)


def _extra_required_runtime_parameter(jobs: SuperJobs) -> None:
    @jobs.handler(MANIFEST_WITH_EVENTS_JOB)  # expect: reportArgumentType
    async def extra(
        request: ManifestRequest, context: JobContext[ManifestEvent], *, required: bool
    ) -> ManifestResult:
        return ManifestResult(revision=request.device_id)


def _extra_required_metadata_parameter() -> None:
    @MANIFEST_WITH_EVENTS_JOB.handler  # expect: reportArgumentType
    def extra(
        payload: ManifestRequest, ctx: JobContext[ManifestEvent], required: bool
    ) -> ManifestResult:
        return ManifestResult(revision=payload.device_id)


def _widened_request_job_runtime_handler(jobs: SuperJobs) -> None:
    def accept(job: Job[ManifestRequest, ManifestResult, ManifestEvent]) -> None:
        @jobs.handler(job)  # expect: reportCallIssue, reportArgumentType
        async def compatible_handler(
            request: ManifestRequest,
            context: JobContext[ManifestEvent],
        ) -> ManifestResult:
            return ManifestResult(revision=request.device_id)

    accept(MANIFEST_WITH_EVENTS_JOB)


def _widened_request_job_explicit_register(jobs: SuperJobs) -> None:
    def accept(job: Job[ManifestRequest, ManifestResult, ManifestEvent]) -> None:
        async def compatible_handler(
            request: ManifestRequest,
            context: JobContext[ManifestEvent],
        ) -> ManifestResult:
            return ManifestResult(revision=request.device_id)

        jobs.register(job, compatible_handler)  # expect: reportCallIssue, reportArgumentType

    accept(MANIFEST_WITH_EVENTS_JOB)


def _widened_request_job_metadata_decorator() -> None:
    def accept(job: Job[ManifestRequest, ManifestResult, ManifestEvent]) -> None:
        @job.handler  # expect: reportAttributeAccessIssue
        async def compatible_handler(
            request: ManifestRequest,
            context: JobContext[ManifestEvent],
        ) -> ManifestResult:
            return ManifestResult(revision=request.device_id)

    accept(MANIFEST_WITH_EVENTS_JOB)


def _widened_no_request_job_runtime_handler(jobs: SuperJobs) -> None:
    def accept(job: Job[None, HeartbeatResult, None]) -> None:
        @jobs.handler(job)  # expect: reportCallIssue, reportArgumentType
        async def compatible_handler(context: JobContext[None]) -> HeartbeatResult:
            return HeartbeatResult(ok=True)

    accept(HEARTBEAT_JOB)


def _widened_no_request_job_explicit_register(jobs: SuperJobs) -> None:
    def accept(job: Job[None, HeartbeatResult, None]) -> None:
        async def compatible_handler(context: JobContext[None]) -> HeartbeatResult:
            return HeartbeatResult(ok=True)

        jobs.register(job, compatible_handler)  # expect: reportCallIssue, reportArgumentType

    accept(HEARTBEAT_JOB)


def _widened_no_request_job_metadata_decorator() -> None:
    def accept(job: Job[None, HeartbeatResult, None]) -> None:
        @job.handler  # expect: reportAttributeAccessIssue
        async def compatible_handler(context: JobContext[None]) -> HeartbeatResult:
            return HeartbeatResult(ok=True)

    accept(HEARTBEAT_JOB)


def _extra_required_explicit_register(jobs: SuperJobs) -> None:
    async def extra(
        request: ManifestRequest, context: JobContext[ManifestEvent], *, required: bool
    ) -> ManifestResult:
        return ManifestResult(revision=request.device_id)

    jobs.register(MANIFEST_WITH_EVENTS_JOB, extra)  # expect: reportCallIssue, reportArgumentType


def _wrong_preserved_parameter(jobs: SuperJobs) -> None:
    @jobs.handler(MANIFEST_WITH_EVENTS_JOB)
    def checked(
        payload: ManifestRequest, ctx: JobContext[ManifestEvent], *, trace: bool = False
    ) -> ManifestResult:
        return ManifestResult(revision=payload.device_id)

    request = ManifestRequest(device_id="probe")
    context: JobContext[ManifestEvent] = JobContext(MANIFEST_WITH_EVENTS_JOB)
    checked(payload=request, ctx=context, trace="wrong")  # expect: reportArgumentType
    checked(payload="wrong", ctx=context)  # expect: reportArgumentType
    checked(payload=request, ctx=context, unknown=True)  # expect: reportCallIssue
