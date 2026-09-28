from __future__ import annotations

from collections.abc import AsyncIterator
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Protocol

from superjobs.jobs.events import JobEvent
from superjobs.jobs.execution import (
    JobError,
    JobState,
    JobStatus,
    ProgressSnapshot,
)
from superjobs.jobs.job_identity import JobIdentity
from superjobs.transport.delivery import Delivery


@dataclass(frozen=True, slots=True)
class SubmissionOptions:
    idempotency_key: str
    job_id: str
    caller_scope: str = "default"
    timeout: float | None = None
    deadline: datetime | None = None


@dataclass
class ExecutionRecord:
    identity: JobIdentity
    job_id: str
    request_payload: bytes
    request_media_type: str | None
    fingerprint: str
    idempotency_key: str
    caller_scope: str
    timeout: float | None
    deadline: datetime | None
    created_at: datetime
    state: JobState = JobState.PENDING
    attempt: int = 0
    progress: ProgressSnapshot | None = None
    started_at: datetime | None = None
    completed_at: datetime | None = None
    cancellation_requested: bool = False
    next_attempt_at: datetime | None = None
    result_payload: bytes = b""
    result_media_type: str | None = None
    error: JobError | None = None
    result_expired: bool = False
    terminal_event_published: bool = False
    published: bool = False
    observation_lock_token: str | None = None
    observation_lock_expires_at: datetime | None = None

    def status(self) -> JobStatus:
        return JobStatus(
            state=self.state,
            progress=self.progress,
            attempt=self.attempt,
            created_at=self.created_at,
            started_at=self.started_at,
            completed_at=self.completed_at,
            cancellation_requested=self.cancellation_requested,
            next_attempt_at=self.next_attempt_at,
            error=self.error,
        )


@dataclass
class WorkItem:
    execution: ExecutionRecord
    attempt: int
    delivery: Delivery


class WorkSubscription(Protocol):
    def __aiter__(self) -> AsyncIterator[WorkItem]: ...

    async def close(self) -> None: ...


class JobTransport(Protocol):
    async def start(self) -> None: ...

    async def stop(self) -> None: ...

    async def publish(
        self,
        execution: ExecutionRecord,
        *,
        attempt: int = 1,
        not_before: datetime | None = None,
    ) -> None: ...

    async def subscribe_work(self, identity: JobIdentity) -> WorkSubscription: ...


class ExecutionStore(Protocol):
    async def submit(
        self,
        identity: JobIdentity,
        *,
        request_payload: bytes,
        request_media_type: str | None,
        fingerprint: str,
        options: SubmissionOptions,
        publish: bool = True,
    ) -> ExecutionRecord: ...

    async def get_execution(
        self,
        identity: JobIdentity,
        job_id: str,
    ) -> ExecutionRecord | None: ...

    async def write_completion(
        self,
        execution: ExecutionRecord,
        *,
        state: JobState,
        result_payload: bytes = b"",
        result_media_type: str | None = None,
        error: JobError | None = None,
    ) -> ExecutionRecord: ...

    async def request_cancel(self, identity: JobIdentity, job_id: str) -> JobStatus: ...

    async def is_cancel_requested(self, job_id: str) -> bool: ...

    async def wait_for_cancellation(self, job_id: str) -> None: ...

    async def mark_started(self, execution: ExecutionRecord, attempt: int) -> None: ...

    async def update_progress(
        self,
        execution: ExecutionRecord,
        progress: ProgressSnapshot,
    ) -> None: ...

    async def mark_retry(
        self,
        execution: ExecutionRecord,
        *,
        next_attempt_at: datetime | None,
    ) -> None: ...

    async def wait_for_status(
        self,
        identity: JobIdentity,
        job_id: str,
    ) -> JobStatus: ...

    async def signal_status(self, job_id: str) -> None: ...

    async def claim_terminal_event(
        self,
        identity: JobIdentity,
        job_id: str,
    ) -> bool: ...

    async def release_terminal_event(
        self,
        identity: JobIdentity,
        job_id: str,
    ) -> None: ...


class ObservationSink(Protocol):
    async def publish_observations(
        self,
        identity: JobIdentity,
        job_id: str,
        *,
        attempt: int,
        events: list[Any],
        progress: ProgressSnapshot | None = None,
    ) -> None: ...

    async def publish_terminal_observation(
        self,
        identity: JobIdentity,
        job_id: str,
        *,
        attempt: int,
        event: Any,
    ) -> bool: ...

    async def append_observations(
        self,
        identity: JobIdentity,
        job_id: str,
        events: list[JobEvent[Any]],
    ) -> None: ...

    async def allocate_observation_sequences(
        self,
        identity: JobIdentity,
        job_id: str,
        count: int,
    ) -> list[int]: ...

    async def observations(
        self,
        identity: JobIdentity,
        job_id: str,
        *,
        after: int = 0,
    ) -> AsyncIterator[JobEvent[Any]]: ...


class JobBackend(ExecutionStore, ObservationSink, JobTransport, Protocol):
    """Combined seam used by the runtime; each concern is independently mockable."""
