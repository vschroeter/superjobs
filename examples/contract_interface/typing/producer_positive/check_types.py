"""Pyright-positive checks for keyword submit and SubmitOptions."""

from __future__ import annotations

from dataclasses import dataclass
from typing import assert_type

from pydantic import BaseModel

from superjobs import Job, SubmitOptions, SuperJobs
from superjobs_contract_example import (
    HEARTBEAT_JOB,
    MANIFEST_NO_EVENTS_JOB,
    MANIFEST_WITH_EVENTS_JOB,
    TELEMETRY_INGEST_JOB,
    ManifestEvent,
    ManifestNoEventsRequest,
    ManifestNoEventsResult,
    ManifestRequest,
    ManifestResult,
    TelemetrySample,
)


@dataclass(kw_only=True, frozen=True, slots=True)
class KeywordOnlyRequest:
    bundle_id: str
    options: str = "business"


@dataclass(kw_only=True, frozen=True, slots=True)
class DefaultOnlyRequest:
    count: int = 7


@dataclass(kw_only=True, frozen=True, slots=True)
class BusinessTimeoutRequest:
    timeout: int


class PydanticRequest(BaseModel):
    metric: str
    count: int = 1


async def _keyword_submit_with_options(jobs: SuperJobs) -> None:
    manifest_client = jobs.client(MANIFEST_WITH_EVENTS_JOB)
    handle = await manifest_client.submit(device_id="sensor-17")
    manifest_result = await handle.result()
    assert_type(manifest_result, ManifestResult)
    await manifest_client.submit(SubmitOptions(timeout=5), device_id="sensor-17")
    await manifest_client.submit(
        ManifestRequest(device_id="sensor-17"),
        options=SubmitOptions(timeout=5),
    )
    await manifest_client.submit(ManifestRequest(device_id="sensor-17"))

    no_events_client = jobs.client(MANIFEST_NO_EVENTS_JOB)
    await no_events_client.submit(bundle_id="bundle-1")

    telemetry_client = jobs.client(TELEMETRY_INGEST_JOB)
    await telemetry_client.submit(metric="cpu", value=1.0)

    heartbeat_client = jobs.client(HEARTBEAT_JOB)
    await heartbeat_client.submit()
    await heartbeat_client.submit(SubmitOptions(timeout=1))
    await heartbeat_client.submit(None)

    business_options_job = Job(
        "probe.business_options",
        request=KeywordOnlyRequest,
        result=ManifestResult,
    )
    business_client = jobs.client(business_options_job)
    await business_client.submit(bundle_id="b", options="trace")

    defaults = Job("probe.defaults", request=DefaultOnlyRequest, result=ManifestResult)
    assert_type(await (await jobs.client(defaults).submit()).result(), ManifestResult)

    metrics = Job("probe.metrics", request=PydanticRequest, result=ManifestResult)
    assert_type(await (await jobs.client(metrics).submit(metric="cpu")).result(), ManifestResult)
    await jobs.client(metrics).submit(metric="cpu", count=2)

    business_timeout = Job("probe.business_timeout", request=BusinessTimeoutRequest, result=ManifestResult)
    await jobs.client(business_timeout).submit(timeout=3)
    await jobs.client(business_timeout).submit(BusinessTimeoutRequest(timeout=3), timeout=5)


def _omitted_event_inference() -> None:
    job = Job(
        "probe.noevent",
        request=ManifestRequest,
        result=ManifestResult,
    )
    def accepts_no_event(value: Job[ManifestRequest, ManifestResult, None]) -> None:
        pass

    accepts_no_event(job)

    request_and_event = Job("probe.request_event", request=ManifestRequest, event=ManifestEvent)
    def accepts_request_event(value: Job[ManifestRequest, None, ManifestEvent]) -> None:
        pass

    accepts_request_event(request_and_event)

    event_only = Job("probe.event_only", event=ManifestEvent)
    def accepts_event_only(value: Job[None, None, ManifestEvent]) -> None:
        pass

    accepts_event_only(event_only)
