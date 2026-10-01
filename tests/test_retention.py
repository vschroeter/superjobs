from datetime import UTC, datetime, timedelta

import pytest
import superjobs.transport.in_memory as in_memory_transport
from pydantic import BaseModel

from superjobs.exceptions.jobs import ObservationExpiredError, ResultExpiredError
from superjobs.jobs.events import JobLog
from superjobs.jobs.execution import JobState, ObservationCursor
from superjobs.jobs.job import Job
from superjobs.jobs.retention import ObservationRetention, ResultRetention
from superjobs.superjobs import SuperJobs
from superjobs.transport.in_memory import InMemoryTransport


class Request(BaseModel):
    value: int


class Result(BaseModel):
    value: int


@pytest.mark.asyncio
async def test_observation_cursor_reports_expired_history() -> None:
    transport = InMemoryTransport(
        observation_retention=ObservationRetention(max_events=2),
    )
    jobs = SuperJobs(transport=transport)
    job = Job("tests.retention.observations", version="v1", request=Request, result=Result)

    @jobs.handler(job)
    async def handler(request: Request, context) -> Result:
        await context.log("one")
        await context.log("two")
        return Result(value=request.value)

    async with jobs:
        handle = await jobs.client(job).submit(Request(value=1))
        await handle.result()
        with pytest.raises(ObservationExpiredError):
            _ = [event async for event in handle.events(after=0)]
        events = [
            event async for event in handle.events(after=ObservationCursor(2))
        ]

    assert isinstance(events[-1].data, JobLog) or events[-1].data.__class__.__name__ == "JobCompleted"


@pytest.mark.asyncio
async def test_result_retention_is_independent_from_completion_status(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class ControlledDateTime(datetime):
        _now = datetime(2020, 1, 1, 12, 0, 0, tzinfo=UTC)

        @classmethod
        def now(cls, tz=None):
            if tz is UTC:
                return cls._now
            return datetime.now(tz)

        @classmethod
        def advance(cls, amount: timedelta) -> None:
            cls._now = cls._now + amount

    monkeypatch.setattr(in_memory_transport, "datetime", ControlledDateTime)

    transport = InMemoryTransport(
        result_retention=ResultRetention(max_age=timedelta(milliseconds=1)),
    )
    jobs = SuperJobs(transport=transport)
    job = Job("tests.retention.result", version="v1", request=Request, result=Result)

    @jobs.handler(job)
    async def handler(request: Request, context) -> Result:
        return Result(value=request.value)

    async with jobs:
        handle = await jobs.client(job).submit(Request(value=1))
        assert await handle.result(wait_timeout=1.0) == Result(value=1)
        assert (await handle.status()).state is JobState.COMPLETED
        ControlledDateTime.advance(timedelta(milliseconds=2))
        with pytest.raises(ResultExpiredError):
            await handle.result(wait_timeout=1.0)
        assert (await handle.status()).state is JobState.COMPLETED
