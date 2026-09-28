from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from typing import Any

from superjobs.exceptions.jobs import (
    JobCancelledError,
    JobFailedError,
    JobNotFoundError,
    ResultExpiredError,
)
from superjobs.jobs.events import JobEvent
from superjobs.jobs.execution import (
    JobCancelledOutcome,
    JobFailedOutcome,
    JobState,
    JobSucceeded,
    JobOutcome,
    ObservationCursor,
    JobStatus,
)
from superjobs.jobs.job import Job
from superjobs.transport.backend import JobBackend


class JobHandle[ReqT: Any | None, FinalT: Any | None, InterT: Any | None]:
    def __init__(
        self,
        job: Job[ReqT, FinalT, InterT],
        backend: JobBackend,
        job_id: str,
    ):
        self.job = job
        self.backend = backend
        self.id = job_id
        register_job = getattr(backend, "register_job", None)
        if register_job is not None:
            register_job(job)

    @property
    def job_id(self) -> str:
        return self.id

    async def status(self) -> JobStatus:
        record = await self._get_record()
        return record.status()

    async def result(self, *, wait_timeout: float | None = None) -> FinalT:
        _validate_wait_timeout(wait_timeout)
        record = await self._get_record()
        if record.state not in _TERMINAL_STATES:
            wait = self.backend.wait_for_status(self.job.identity, self.id)
            if wait_timeout is None:
                await wait
            else:
                await asyncio.wait_for(wait, timeout=wait_timeout)
            record = await self._get_record()

        if record.state is JobState.COMPLETED:
            if record.result_expired:
                raise ResultExpiredError(self.id)
            return self.job.decode_result(record.result_payload)
        if record.state is JobState.FAILED:
            raise JobFailedError(
                record.error
                or _fallback_error("Job failed without an error record"),
            )
        if record.state is JobState.CANCELLED:
            raise JobCancelledError(
                record.error.message if record.error is not None else None,
            )
        raise RuntimeError(f"Job {self.id} did not reach a terminal state")

    async def outcome(self, *, wait_timeout: float | None = None) -> JobOutcome:
        _validate_wait_timeout(wait_timeout)
        record = await self._get_record()
        if record.state not in _TERMINAL_STATES:
            wait = self.backend.wait_for_status(self.job.identity, self.id)
            if wait_timeout is None:
                await wait
            else:
                await asyncio.wait_for(wait, timeout=wait_timeout)
            record = await self._get_record()

        if record.state is JobState.COMPLETED:
            if record.result_expired:
                raise ResultExpiredError(self.id)
            return JobSucceeded(self.job.decode_result(record.result_payload))
        if record.state is JobState.FAILED:
            return JobFailedOutcome(
                record.error
                or _fallback_error("Job failed without an error record"),
            )
        if record.state is JobState.CANCELLED:
            return JobCancelledOutcome(
                record.error.message if record.error is not None else None,
            )
        raise RuntimeError(f"Job {self.id} did not reach a terminal state")

    def events(
        self,
        *,
        after: int | ObservationCursor = 0,
    ) -> AsyncIterator[JobEvent[InterT]]:
        after_sequence = after.sequence if isinstance(after, ObservationCursor) else after
        if after_sequence < 0:
            raise ValueError("observation cursor must be non-negative")

        async def iterator() -> AsyncIterator[JobEvent[InterT]]:
            try:
                stream = await self.backend.observations(
                    self.job.identity,
                    self.id,
                    after=after_sequence,
                )
                async for event in stream:
                    yield event
            except KeyError as exception:
                raise JobNotFoundError(self.id) from exception
            finally:
                close = locals().get("stream")
                if close is not None:
                    close_method = getattr(close, "aclose", None)
                    if close_method is not None:
                        await close_method()

        return iterator()

    async def cancel(self) -> JobStatus:
        try:
            return await self.backend.request_cancel(self.job.identity, self.id)
        except KeyError as exception:
            raise JobNotFoundError(self.id) from exception

    def __await__(self):
        return self.result().__await__()

    async def _get_record(self):
        record = await self.backend.get_execution(self.job.identity, self.id)
        if record is None:
            raise JobNotFoundError(self.id)
        return record


_TERMINAL_STATES = {
    JobState.COMPLETED,
    JobState.FAILED,
    JobState.CANCELLED,
}


def _fallback_error(message: str):
    from superjobs.jobs.execution import JobError

    return JobError(code="failed", message=message)


def _validate_wait_timeout(wait_timeout: float | None) -> None:
    if wait_timeout is not None and wait_timeout <= 0:
        raise ValueError("wait_timeout must be positive")
