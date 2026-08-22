import asyncio

from faststream.nats import NatsBroker
from pydantic import BaseModel
from superjobs.exceptions.jobs import JobCancelledError
from superjobs.jobs.events import JobCancelled, JobCompleted, JobFailed, JobProgress
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

    # Job definition
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

    # SuperJobs instance
    jobs = SuperJobs(broker=NatsBroker("nats://localhost:4222"))

    # Job handler
    @jobs.handle(GenerateTest)
    async def job_handler(request: GenerateRequest, context: JobContext[GenerateRequest, GenerateResult, GenerateEvent]) -> GenerateResult:

        try:
            for i in range(request.count):
                await context.check_cancelled()

                # Simulate work
                await asyncio.sleep(1)

                # Emit intermediate events
                await context.emit_event(GenerateEvent(value=i))

                # This should internally automatically call "extend_lease" to make sure, that the job is in NATS in progress and not
                # re-transmitted after an ACK timeout
                await context.progress(i, request.count)
        except JobCancelledError:
            return GenerateResult(test_result=f"Cancelled at {i} tasks")

        if context.cancelled:
            # Tidy up any resources
            pass

        # This should internally call "ACK" so that the job is completed in NATS
        return GenerateResult(test_result=f"Completed {request.count} tasks")

    try:
        # Start the jobs connection, the handlers, etc.
        await jobs.start()

        # Create a client for the job
        client = await jobs.client(GenerateTest)

        # Method A) Submit and wait for the final result to be received
        await client.run(GenerateRequest(count=11))

        # Method B) Submit and work on the handle...
        handle = await client.submit(GenerateRequest(count=11))

        async for event in handle.events():
            match event:
                case JobCompleted():
                    print("Job completed")
                case JobCancelled():
                    print("Job cancelled")
                case JobFailed():
                    print("Job failed")
                case JobProgress():
                    print("Job progress")
                case GenerateEvent():
                    print("Generate event")

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
