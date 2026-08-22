import asyncio

from faststream.nats import NatsBroker
from pydantic import BaseModel
from superjobs.jobs.job import Job
from superjobs.jobs.job_context import JobContext
from superjobs.superjobs import SuperJobs


class GenerateRequest(BaseModel):
    count: int


class GenerateResult(BaseModel):
    test_result: str


class GenerateEvent(BaseModel):
    value: int


def test_job_generation():
    asyncio.run(_test_job_generation())


async def _test_job_generation():
    GenerateTest = Job[
        GenerateRequest,
        GenerateResult,
        GenerateEvent,
    ](
        "superjobs.generate",
        request=GenerateRequest,
        result=GenerateResult,
        event=GenerateEvent,
    )

    jobs = SuperJobs(broker=NatsBroker("nats://localhost:4222"))
    received_requests: list[GenerateRequest] = []
    attempts: dict[int, int] = {}
    received = asyncio.Event()
    retried = asyncio.Event()

    @jobs.handle(GenerateTest)
    async def generate_test(request: GenerateRequest, context: JobContext[GenerateRequest, GenerateResult, GenerateEvent]) -> GenerateResult:

        print("GENERATE TEST", request, context)

        received_requests.append(request)
        attempts[request.count] = attempts.get(request.count, 0) + 1
        print("ATTEMPTS", attempts)
        if request.count == 12 and len(attempts) == 1:
            raise RuntimeError("temporary failure")
        if request.count == 11:
            received.set()
        if request.count == 12 and len(attempts) == 2:
            retried.set()
        return GenerateResult(test_result=str(request.count))

    try:
        await jobs.start()
        client = jobs.client(GenerateTest)
        await client.start()
        await client.submit(GenerateRequest(count=11))
        await asyncio.wait_for(received.wait(), timeout=5)
        assert any(request.count == 11 for request in received_requests)

        await client.submit(GenerateRequest(count=12))
        await asyncio.wait_for(retried.wait(), timeout=5)
        assert attempts[11] == 1
        assert attempts[12] == 1
    finally:
        await jobs.stop()


if __name__ == "__main__":
    test_job_generation()
