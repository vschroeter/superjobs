import asyncio
import os
import uuid

import pytest
from faststream.nats import NatsBroker
from pydantic import BaseModel

from superjobs.jobs.events import (
    JobCancelled,
    JobCompleted,
    JobFailed,
    JobLog,
    JobProgress,
    JobRetryScheduled,
    JobStarted,
)
from superjobs.jobs.job import Job
from superjobs.jobs.job_context import JobContext
from superjobs.superjobs import SuperJobs


class GenerateRequest(BaseModel):
    count: int


class GenerateResult(BaseModel):
    generated: str


class GenerateEvent(BaseModel):
    value: int


@pytest.mark.nats
@pytest.mark.asyncio
async def test_job_generation() -> None:
    generate_job = Job(
        "superjobs.generate",
        version="v1",
        request=GenerateRequest,
        result=GenerateResult,
        event=GenerateEvent,
    )
    jobs = SuperJobs(
        broker=NatsBroker(
            os.getenv("NATS_URL", "nats://localhost:4222"),
            connect_timeout=1,
        ),
    )

    @jobs.handler(generate_job)
    async def generate(
        request: GenerateRequest,
        context: JobContext[GenerateEvent],
    ) -> GenerateResult:
        await context.log("Starting generation")
        print(f"##### Starting generation for {request.count} items")
        for index in range(request.count):
            await context.check_cancelled()
            await asyncio.sleep(0.01)
            await context.emit(GenerateEvent(value=index))
            await context.progress(
                completed=index + 1,
                total=request.count,
            )
        print(f"##### Generated {request.count} items")

        return GenerateResult(generated=f"Generated {request.count} items")

    try:
        await jobs.start()
    except Exception as exception:
        pytest.skip(f"NATS/JetStream is unavailable: {exception}")

    try:
        async with jobs:
            client = jobs.client(generate_job)
            print("Running client")
            result = await client.run(GenerateRequest(count=20))
            print("Submitted client")
            handle = await client.submit(
                GenerateRequest(count=20),
                idempotency_key=f"generation-{uuid.uuid4().hex}",
            )
            print("Submitting handle")
            events = [event async for event in handle.events()]
            print("Getting events")
            observed_result = await handle
            print("Getting observed result:", observed_result)
            # print("Events:", events)
            print("Result:", result)
    finally:
        if jobs.started:
            await jobs.stop()

    assert result.generated == "Generated 20 items"
    assert observed_result.generated == "Generated 20 items"
    assert any(isinstance(event.data, JobStarted) for event in events)
    assert any(isinstance(event.data, JobProgress) for event in events)
    assert any(isinstance(event.data, GenerateEvent) for event in events)
    assert isinstance(events[-1].data, JobCompleted)


if __name__ == "__main__":
    asyncio.run(test_job_generation())
