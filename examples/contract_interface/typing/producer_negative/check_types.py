"""Pyright-negative checks for producer submit and no-request registration."""

from __future__ import annotations

from pydantic import BaseModel

from superjobs import Job, JobContext, SubmitOptions, SuperJobs
from superjobs_contract_example import (
    HEARTBEAT_JOB,
    MANIFEST_WITH_EVENTS_JOB,
    HeartbeatResult,
    ManifestEvent,
    ManifestRequest,
    ManifestResult,
)


class PydanticRequest(BaseModel):
    metric: str
    count: int = 1


async def _pydantic_constructor_rejections(jobs: SuperJobs) -> None:
    client = jobs.client(Job("probe.negative.pydantic", request=PydanticRequest))
    await client.submit()  # expect: reportCallIssue
    await client.submit(metric=17)  # expect: reportArgumentType
    await client.submit(metric="cpu", unknown=True)  # expect: reportCallIssue


async def _missing_required_keyword(jobs: SuperJobs) -> None:
    client = jobs.client(MANIFEST_WITH_EVENTS_JOB)
    await client.submit()  # expect: reportCallIssue


async def _wrong_keyword_type(jobs: SuperJobs) -> None:
    client = jobs.client(MANIFEST_WITH_EVENTS_JOB)
    await client.submit(device_id=17)  # expect: reportArgumentType


async def _unknown_keyword(jobs: SuperJobs) -> None:
    client = jobs.client(MANIFEST_WITH_EVENTS_JOB)
    await client.submit(device_id="ok", unknown=True)  # expect: reportCallIssue


async def _mixed_object_and_keywords(jobs: SuperJobs) -> None:
    client = jobs.client(MANIFEST_WITH_EVENTS_JOB)
    await client.submit(ManifestRequest(device_id="ok"), device_id="dup")  # expect: reportArgumentType


async def _wrong_explicit_object(jobs: SuperJobs) -> None:
    client = jobs.client(MANIFEST_WITH_EVENTS_JOB)
    await client.submit("not-a-request")  # expect: reportArgumentType


async def _bare_positional_constructor_string(jobs: SuperJobs) -> None:
    client = jobs.client(MANIFEST_WITH_EVENTS_JOB)
    await client.submit("device-id")  # expect: reportArgumentType


async def _request_keywords_on_no_request_job(jobs: SuperJobs) -> None:
    client = jobs.client(HEARTBEAT_JOB)
    await client.submit(device_id="nope")  # expect: reportCallIssue


async def _wrong_submit_options_value(jobs: SuperJobs) -> None:
    client = jobs.client(MANIFEST_WITH_EVENTS_JOB)
    await client.submit(
        ManifestRequest(device_id="ok"),
        options=SubmitOptions(timeout="bad"),  # expect: reportArgumentType
    )


async def _keyword_submit_options_in_constructor_form(jobs: SuperJobs) -> None:
    client = jobs.client(MANIFEST_WITH_EVENTS_JOB)
    await client.submit(  # expect: reportCallIssue
        device_id="ok",
        options=SubmitOptions(timeout=1),
    )


async def _execution_option_as_request_keyword(jobs: SuperJobs) -> None:
    client = jobs.client(MANIFEST_WITH_EVENTS_JOB)
    await client.submit(device_id="ok", timeout=5)  # expect: reportCallIssue


def _constructor_keywords_after_job_erasure(jobs: SuperJobs) -> None:
    def widen(job: Job[ManifestRequest, ManifestResult, ManifestEvent]) -> None:
        client = jobs.client(job)
        client.submit(device_id="erased")  # expect: reportCallIssue

    widen(MANIFEST_WITH_EVENTS_JOB)


def _wrong_no_request_explicit_register(jobs: SuperJobs) -> None:
    async def needs_context_only(
        request: None,
        context: JobContext[None],
    ) -> HeartbeatResult:
        return HeartbeatResult(ok=True)

    jobs.register(HEARTBEAT_JOB, needs_context_only)  # expect: reportCallIssue, reportArgumentType


def _wrong_no_request_runtime_decorator(jobs: SuperJobs) -> None:
    @jobs.handler(HEARTBEAT_JOB)  # expect: reportArgumentType
    async def needs_context_only(
        request: None,
        context: JobContext[None],
    ) -> HeartbeatResult:
        return HeartbeatResult(ok=True)
