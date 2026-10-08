"""Positive discovery typing checks for job-bound capability inference."""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from typing import assert_type

from pydantic import BaseModel

from superjobs import Job, JobIdentity, JobClient, RawCapabilities, RequestJob, SuperJobs, WorkerRegistration
from superjobs_contract_example import (
    LOCALE_DISCOVERY_JOB,
    MANIFEST_WITH_EVENTS_JOB,
    LocaleCapability,
    ManifestEvent,
    ManifestRequest,
    ManifestResult,
)


@dataclass(kw_only=True, frozen=True, slots=True)
class ProbeRequest:
    device_id: str


class PydanticProbeRequest(BaseModel):
    device_id: str


@dataclass(kw_only=True, frozen=True, slots=True)
class OptionalLocaleCapability:
    locale: str | None = None


REQUEST_ONLY_DATACLASS_JOB = Job(
    "examples.contract.discovery.request_only_dataclass",
    request=ProbeRequest,
)

REQUEST_ONLY_PYDANTIC_JOB = Job(
    "examples.contract.discovery.request_only_pydantic",
    request=PydanticProbeRequest,
    result=None,
    event=None,
)

REQUEST_ONLY_EXPLICIT_NONE_JOB = Job(
    "examples.contract.discovery.request_only_explicit_none",
    request=PydanticProbeRequest,
    result=None,
)

REQUEST_ONLY_WITH_CAPABILITY_JOB = Job(
    "examples.contract.discovery.request_only_capability",
    request=ProbeRequest,
    capabilities=OptionalLocaleCapability,
)

LOCALE_JOB = LOCALE_DISCOVERY_JOB

EXPLICIT_BASE_MANIFEST_JOB: Job[ManifestRequest, ManifestResult, ManifestEvent] = (
    MANIFEST_WITH_EVENTS_JOB
)


def _request_only_job_shapes() -> None:
    def accepts_dataclass(job: RequestJob[ProbeRequest, None, None, ...]) -> None:
        pass

    accepts_dataclass(REQUEST_ONLY_DATACLASS_JOB)
    accepts_dataclass(REQUEST_ONLY_WITH_CAPABILITY_JOB)


def _accepts_legacy_three_type_job(job: Job[ManifestRequest, ManifestResult, None]) -> None:
    pass


def _accepts_legacy_three_type_client(
    client: JobClient[ManifestRequest, ManifestResult, None],
) -> None:
    pass


async def request_only_keyword_submit(jobs: SuperJobs) -> None:
    await jobs.client(REQUEST_ONLY_DATACLASS_JOB).submit(device_id="sensor-1")
    await jobs.client(REQUEST_ONLY_PYDANTIC_JOB).submit(device_id="sensor-2")
    await jobs.client(REQUEST_ONLY_EXPLICIT_NONE_JOB).submit(device_id="sensor-3")
    cap_client = jobs.client(REQUEST_ONLY_WITH_CAPABILITY_JOB)
    await cap_client.submit(device_id="sensor-4")
    workers = await cap_client.workers()
    assert_type(workers, list[WorkerRegistration[OptionalLocaleCapability]])


async def discovery_inference(jobs: SuperJobs) -> None:
    client = jobs.client(LOCALE_JOB)
    workers = await client.workers()
    assert_type(workers, list[WorkerRegistration[LocaleCapability]])
    first = workers[0]
    if first.capabilities is not None:
        assert_type(first.capabilities.locale, str)

    namespace_workers = await jobs.discovery.workers(LOCALE_JOB)
    assert_type(namespace_workers, list[WorkerRegistration[LocaleCapability]])

    offered = await jobs.discovery.jobs()
    assert_type(offered, list[JobIdentity])

    identity = JobIdentity(name=LOCALE_JOB.name, version=LOCALE_JOB.version)
    raw_workers = await jobs.discovery.workers(identity)
    assert_type(raw_workers, list[WorkerRegistration[RawCapabilities]])

    plain_client = jobs.client(MANIFEST_WITH_EVENTS_JOB)
    plain_workers = await plain_client.workers()
    assert_type(plain_workers, Sequence[WorkerRegistration[object]])
    assert_type(plain_workers[0].capabilities, object | None)

    explicit_job: Job[ManifestRequest, ManifestResult, ManifestEvent] = EXPLICIT_BASE_MANIFEST_JOB
    explicit_namespace_workers = await jobs.discovery.workers(explicit_job)
    assert_type(explicit_namespace_workers, list[WorkerRegistration[object]])

    explicit_client: JobClient[ManifestRequest, ManifestResult, ManifestEvent] = jobs.client(
        explicit_job,
    )
    explicit_client_workers = await explicit_client.workers()
    assert_type(explicit_client_workers, Sequence[WorkerRegistration[object]])


async def _legacy_namespace_workers(
    jobs: SuperJobs,
    job: Job[ManifestRequest, ManifestResult, None],
) -> list[WorkerRegistration[object]]:
    return await jobs.discovery.workers(job)


async def _legacy_client_workers(
    client: JobClient[ManifestRequest, ManifestResult, None],
) -> Sequence[WorkerRegistration[object]]:
    return await client.workers()


async def legacy_job_and_client_widening(jobs: SuperJobs) -> None:
    _accepts_legacy_three_type_job(LOCALE_JOB)
    locale_client = jobs.client(LOCALE_JOB)
    _accepts_legacy_three_type_client(locale_client)

    legacy_namespace_workers = await _legacy_namespace_workers(jobs, LOCALE_JOB)
    assert_type(legacy_namespace_workers, list[WorkerRegistration[object]])
    if legacy_namespace_workers[0].capabilities is not None:
        capability: object = legacy_namespace_workers[0].capabilities

    legacy_client_workers = await _legacy_client_workers(locale_client)
    assert_type(legacy_client_workers, Sequence[WorkerRegistration[object]])
    if legacy_client_workers[0].capabilities is not None:
        client_capability: object = legacy_client_workers[0].capabilities
