"""Negative discovery typing checks."""

from __future__ import annotations

from dataclasses import dataclass
from typing import assert_type

from pydantic import BaseModel

from superjobs import Job, JobIdentity, RawCapabilities, SuperJobs, WorkerRegistration
from superjobs_contract_example import (
    LOCALE_DISCOVERY_JOB,
    LocaleCapability,
    ManifestRequest,
    ManifestResult,
)


@dataclass(kw_only=True, frozen=True, slots=True)
class ProbeRequest:
    device_id: str


class PydanticProbeRequest(BaseModel):
    device_id: str


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

LOCALE_JOB = LOCALE_DISCOVERY_JOB


async def request_only_constructor_rejections(jobs: SuperJobs) -> None:
    await jobs.client(REQUEST_ONLY_DATACLASS_JOB).submit()  # expect: reportCallIssue
    await jobs.client(REQUEST_ONLY_PYDANTIC_JOB).submit(device_id=17)  # expect: reportArgumentType
    await jobs.client(REQUEST_ONLY_PYDANTIC_JOB).submit(  # expect: reportCallIssue
        device_id="ok",
        unknown=True,
    )
    await jobs.client(REQUEST_ONLY_DATACLASS_JOB).submit("not-a-request")  # expect: reportArgumentType


def wrong_capability_value_type() -> None:
    LocaleCapability(locale=17)  # expect: reportArgumentType


def capability_declaration_requires_a_type() -> None:
    Job("examples.contract.discovery.invalid", capabilities=LocaleCapability(locale="de"))  # expect: reportArgumentType, reportArgumentType


async def raw_capabilities_are_not_application_dtos(jobs: SuperJobs) -> None:
    identity = JobIdentity(name=LOCALE_JOB.name, version=LOCALE_JOB.version)
    raw_workers = await jobs.discovery.workers(identity)
    assert_type(raw_workers, list[WorkerRegistration[RawCapabilities]])
    assert_type(raw_workers[0].capabilities, LocaleCapability)  # expect: reportAssertTypeFailure


async def _legacy_namespace_workers(
    jobs: SuperJobs,
    job: Job[ManifestRequest, ManifestResult, None],
) -> list[WorkerRegistration[object]]:
    return await jobs.discovery.workers(job)


async def widened_capabilities_are_not_statically_none(jobs: SuperJobs) -> None:
    legacy_workers = await _legacy_namespace_workers(jobs, LOCALE_JOB)
    assert_type(legacy_workers, list[WorkerRegistration[object]])
    assert_type(legacy_workers[0].capabilities, None)  # expect: reportAssertTypeFailure
