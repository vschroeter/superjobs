from dataclasses import dataclass

import pytest
from pydantic import BaseModel

from superjobs.jobs.job import Job
from superjobs.jobs.job_identity import JobIdentity


class Request(BaseModel):
    value: int


class Result(BaseModel):
    value: int


@dataclass(frozen=True)
class Event:
    value: int


def test_job_keeps_hierarchical_name_and_explicit_version() -> None:
    job = Job(
        "namespace.root.app.device.task",
        version="v1",
        request=Request,
        result=Result,
        event=Event,
    )

    assert job.name == "namespace.root.app.device.task"
    assert job.version == "v1"
    assert job.canonical_name == "namespace.root.app.device.task:v1"


def test_job_without_version_is_unversioned_not_latest() -> None:
    identity = JobIdentity("namespace.task")

    assert identity.version is None
    assert identity.canonical_name == "namespace.task"


@pytest.mark.parametrize(
    "name",
    [
        "",
        "Namespace.task",
        "namespace..task",
        "namespace.task:v1",
        "namespace.task*",
        "namespace.task>",
        "namespace.task name",
    ],
)
def test_job_rejects_invalid_hierarchical_names(name: str) -> None:
    with pytest.raises(ValueError):
        JobIdentity(name)


def test_job_supports_none_payload_contracts() -> None:
    job = Job("namespace.noop", version="v1", request=None, result=None, event=None)

    assert job.request_codec is None
    assert job.result_codec is None
    assert job.event_codec is None


def test_job_definition_is_immutable() -> None:
    job = Job("namespace.immutable", version="v1", request=Request, result=Result)

    with pytest.raises(AttributeError):
        job.request_codec = None
