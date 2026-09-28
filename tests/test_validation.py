import pytest
from pydantic import BaseModel

from superjobs.exceptions.jobs import JobFailedError
from superjobs.jobs.job import Job
from superjobs.superjobs import SuperJobs
from superjobs.transport.in_memory import InMemoryTransport


class Request(BaseModel):
    value: int


class Result(BaseModel):
    value: str


@pytest.mark.asyncio
async def test_invalid_client_request_is_rejected_before_submission() -> None:
    jobs = SuperJobs(transport=InMemoryTransport())
    job = Job("tests.validation.request", version="v1", request=Request, result=Result)

    @jobs.handler(job)
    async def handler(request: Request, context) -> Result:
        return Result(value=str(request.value))

    async with jobs:
        with pytest.raises(ValueError):
            await jobs.client(job).submit(Request(value="not-an-int"))


@pytest.mark.asyncio
async def test_oversized_result_is_a_permanent_typed_failure() -> None:
    jobs = SuperJobs(
        transport=InMemoryTransport(max_result_bytes=8),
    )
    job = Job("tests.validation.result", version="v1", request=Request, result=Result)

    @jobs.handler(job)
    async def handler(request: Request, context) -> Result:
        return Result(value="too much data")

    async with jobs:
        handle = await jobs.client(job).submit(Request(value=1))
        with pytest.raises(JobFailedError) as raised:
            await handle.result()

    assert raised.value.error.code == "result_too_large"


@pytest.mark.asyncio
async def test_plain_python_payloads_are_validated_on_submission_and_completion() -> None:
    jobs = SuperJobs(transport=InMemoryTransport())
    job = Job(
        "tests.validation.primitive",
        version="v1",
        request=int,
        result=int,
    )

    @jobs.handler(job)
    async def handler(request: int, context) -> int:
        return "not an integer"  # type: ignore[return-value]

    async with jobs:
        client = jobs.client(job)
        with pytest.raises(ValueError):
            await client.submit("not an integer")  # type: ignore[arg-type]
        handle = await client.submit(1)
        with pytest.raises(JobFailedError) as raised:
            await handle.result()

    assert raised.value.error.code == "invalid_result"
