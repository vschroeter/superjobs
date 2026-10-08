from __future__ import annotations

import asyncio
from collections import defaultdict
from collections.abc import AsyncIterator
from datetime import UTC, datetime
from typing import Any, Callable

from superjobs.exceptions.jobs import (
    IdempotencyConflictError,
    ObservationExpiredError,
    ResultTooLargeError,
)
from superjobs.jobs.events import JobCancelled, JobCompleted, JobFailed, JobEvent
from superjobs.jobs.execution import JobError, JobState, JobStatus, ProgressSnapshot
from superjobs.jobs.job_identity import JobIdentity
from superjobs.jobs.retention import ObservationRetention, ResultRetention
from superjobs.transport.backend import (
    ExecutionRecord,
    JobBackend,
    SubmissionOptions,
    WorkItem,
    WorkSubscription,
)
from superjobs.transport.delivery import Delivery


class _InMemoryDelivery(Delivery):
    def __init__(self, backend: InMemoryTransport, execution: ExecutionRecord, attempt: int):
        self._backend = backend
        self._execution = execution
        self._attempt = attempt
        self._done = False
        self._lock = asyncio.Lock()

    @property
    def attempt(self) -> int:
        return self._attempt

    async def ack(self) -> None:
        async with self._lock:
            if self._done:
                return
            self._done = True
            self._backend.ack_log.append(
                (self._execution.job_id, self._attempt, self._execution.state),
            )

    async def retry(self, *, delay=None, attempt=None) -> None:
        async with self._lock:
            if self._done:
                return
            self._done = True
        self._backend.retry_log.append((self._execution.job_id, self._attempt))
        await self._backend._retry(
            self._execution,
            attempt=attempt if attempt is not None else self._attempt + 1,
            delay=delay.total_seconds() if delay is not None else 0.0,
        )

    async def reject(self, *, reason=None) -> None:
        async with self._lock:
            if self._done:
                return
            self._done = True
            self._backend.reject_log.append(
                (self._execution.job_id, self._attempt, reason),
            )

    async def extend_lease(self) -> None:
        return


class _InMemoryWorkSubscription(WorkSubscription):
    def __init__(
        self,
        backend: InMemoryTransport,
        identity: JobIdentity,
        queue: asyncio.Queue[WorkItem | None],
    ):
        self._backend = backend
        self._identity = identity
        self._queue = queue
        self._closed = False

    def __aiter__(self) -> _InMemoryWorkSubscription:
        return self

    async def __anext__(self) -> WorkItem:
        if self._closed:
            raise StopAsyncIteration
        item = await self._queue.get()
        if item is None:
            self._closed = True
            raise StopAsyncIteration
        return item

    async def close(self) -> None:
        if not self._closed:
            self._closed = True
            self._backend._work_subscriptions[self._identity.canonical_name].discard(
                self,
            )


class _InMemoryObservationStream:
    def __init__(
        self,
        backend: InMemoryTransport,
        job_id: str,
        initial: list[JobEvent[Any]],
        queue: asyncio.Queue[JobEvent[Any] | None] | None,
    ):
        self._backend = backend
        self._job_id = job_id
        self._initial = iter(initial)
        self._queue = queue
        self._closed = False

    def __aiter__(self) -> _InMemoryObservationStream:
        return self

    async def __anext__(self) -> JobEvent[Any]:
        if self._closed:
            raise StopAsyncIteration

        try:
            return next(self._initial)
        except StopIteration:
            pass

        if self._queue is None:
            await self.aclose()
            raise StopAsyncIteration

        event = await self._queue.get()
        if event is None:
            await self.aclose()
            raise StopAsyncIteration
        return event

    async def aclose(self) -> None:
        if self._closed:
            return
        self._closed = True
        if self._queue is not None:
            self._backend._observation_subscribers[self._job_id].discard(self._queue)


class InMemoryTransport(JobBackend):
    """A deterministic backend used by the public API tests and local callers."""

    def __init__(
        self,
        *,
        observation_retention: ObservationRetention | None = None,
        result_retention: ResultRetention | None = None,
        max_result_bytes: int | None = None,
        discovery_store: Any | None = None,
        presence_config: Any | None = None,
        discovery_clock: Callable[[], datetime] | None = None,
    ) -> None:
        self.started = False
        self._owners = 0
        self.observation_retention = observation_retention or ObservationRetention()
        self.result_retention = result_retention or ResultRetention()
        if max_result_bytes is not None and max_result_bytes < 1:
            raise ValueError("max_result_bytes must be positive")
        self.max_result_bytes = max_result_bytes
        if discovery_store is None:
            from superjobs.discovery.memory import InMemoryDiscoveryBackend

            discovery_store = InMemoryDiscoveryBackend(
                config=presence_config,
                clock=discovery_clock,
            )
        self.discovery_backend = discovery_store
        self.ack_log: list[tuple[str, int, JobState]] = []
        self.retry_log: list[tuple[str, int]] = []
        self.reject_log: list[tuple[str, int, str | None]] = []
        self.progress_update_log: list[tuple[str, ProgressSnapshot]] = []
        self._executions: dict[str, ExecutionRecord] = {}
        self._idempotency: dict[tuple[str, str, str, str], str] = {}
        self._work_queues: dict[str, asyncio.Queue[WorkItem | None]] = {}
        self._work_subscriptions: defaultdict[
            str,
            set[_InMemoryWorkSubscription],
        ] = defaultdict(set)
        self._observations: defaultdict[str, list[JobEvent[Any]]] = defaultdict(list)
        self._observation_subscribers: defaultdict[
            str,
            set[asyncio.Queue[JobEvent[Any] | None]],
        ] = defaultdict(set)
        self._status_events: defaultdict[str, asyncio.Event] = defaultdict(asyncio.Event)
        self._execution_locks: defaultdict[str, asyncio.Lock] = defaultdict(asyncio.Lock)
        self._observation_locks: defaultdict[str, asyncio.Lock] = defaultdict(asyncio.Lock)
        self._observation_sequences: defaultdict[str, int] = defaultdict(int)
        self._retry_tasks: set[asyncio.Task[None]] = set()
        self._deadline_tasks: dict[str, asyncio.Task[None]] = {}
        self._cancel_events: defaultdict[str, asyncio.Event] = defaultdict(asyncio.Event)

    async def start(self) -> None:
        self._owners += 1
        self.started = True
        for execution in self._executions.values():
            if execution.deadline is not None and execution.state is JobState.PENDING:
                self._schedule_deadline(execution)
            if (
                execution.state is JobState.PENDING
                and execution.next_attempt_at is not None
            ):
                self._schedule_retry(
                    execution,
                    attempt=max(1, execution.attempt + 1),
                )

    async def stop(self) -> None:
        if self._owners:
            self._owners -= 1
        if self._owners:
            return
        self.started = False
        for canonical_name, queue in self._work_queues.items():
            for subscription in tuple(self._work_subscriptions[canonical_name]):
                await subscription.close()
        tasks = list(self._retry_tasks)
        for task in tasks:
            if not task.done():
                task.cancel()
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
        deadline_tasks = tuple(self._deadline_tasks.values())
        for task in deadline_tasks:
            if not task.done():
                task.cancel()
        if deadline_tasks:
            await asyncio.gather(*deadline_tasks, return_exceptions=True)
        self._deadline_tasks.clear()
        for subscribers in self._observation_subscribers.values():
            for queue in tuple(subscribers):
                await queue.put(None)
            subscribers.clear()

    async def submit(
        self,
        identity: JobIdentity,
        *,
        request_payload: bytes,
        request_media_type: str | None,
        fingerprint: str,
        options: SubmissionOptions,
        publish: bool = True,
    ) -> ExecutionRecord:
        if not self.started:
            raise RuntimeError("Transport must be started before submitting jobs")

        idempotency_scope = (
            identity.canonical_name,
            options.caller_scope,
            options.idempotency_key,
            fingerprint,
        )
        existing_id = self._idempotency.get(idempotency_scope)
        if existing_id is not None:
            execution = self._executions[existing_id]
            if (
                publish
                and not execution.published
                and execution.state is JobState.PENDING
            ):
                await self.publish(execution)
            return execution

        for key, existing_execution_id in self._idempotency.items():
            if key[:3] == (
                identity.canonical_name,
                options.caller_scope,
                options.idempotency_key,
            ):
                raise IdempotencyConflictError(
                    "Idempotency key was already used with a different submission",
                )

        existing = self._executions.get(options.job_id)
        if existing is not None:
            if existing.identity != identity or existing.fingerprint != fingerprint:
                raise IdempotencyConflictError(
                    f"Job ID {options.job_id!r} was already used for another submission",
                )
            if (
                publish
                and not existing.published
                and existing.state is JobState.PENDING
            ):
                await self.publish(existing)
            return existing

        execution = ExecutionRecord(
            identity=identity,
            job_id=options.job_id,
            request_payload=request_payload,
            request_media_type=request_media_type,
            fingerprint=fingerprint,
            idempotency_key=options.idempotency_key,
            caller_scope=options.caller_scope,
            timeout=options.timeout,
            deadline=options.deadline,
            created_at=datetime.now(UTC),
        )
        self._executions[execution.job_id] = execution
        self._idempotency[idempotency_scope] = execution.job_id
        if execution.deadline is not None:
            self._schedule_deadline(execution)

        if publish:
            await self.publish(execution)
        return execution

    def _schedule_deadline(self, execution: ExecutionRecord) -> None:
        existing = self._deadline_tasks.get(execution.job_id)
        if existing is not None and not existing.done():
            return

        async def enforce() -> None:
            deadline = execution.deadline
            if deadline is None:
                return
            delay = (deadline - datetime.now(UTC)).total_seconds()
            if delay > 0:
                await asyncio.sleep(delay)
            async with self._execution_locks[execution.job_id]:
                if execution.state is not JobState.PENDING:
                    return
                execution.state = JobState.FAILED
                execution.completed_at = datetime.now(UTC)
                execution.error = JobError(
                    code="deadline_exceeded",
                    message="The execution deadline was exceeded before starting",
                )
            await self._append_terminal(
                execution,
                JobFailed(error=execution.error),
            )

        task = asyncio.create_task(
            enforce(),
            name=f"superjobs-deadline-{execution.job_id}",
        )
        self._deadline_tasks[execution.job_id] = task

    async def publish(
        self,
        execution: ExecutionRecord,
        *,
        attempt: int = 1,
        not_before: datetime | None = None,
    ) -> None:
        if not_before is not None:
            delay = (not_before - datetime.now(UTC)).total_seconds()
            if delay > 0:
                await asyncio.sleep(delay)
        queue = self._work_queues.setdefault(
            execution.identity.canonical_name,
            asyncio.Queue(),
        )
        delivery = _InMemoryDelivery(self, execution, attempt)
        await queue.put(WorkItem(execution=execution, attempt=attempt, delivery=delivery))
        execution.published = True

    async def _retry(
        self,
        execution: ExecutionRecord,
        *,
        attempt: int,
        delay: float,
    ) -> None:
        execution.next_attempt_at = datetime.now(UTC)
        if delay:
            execution.next_attempt_at = datetime.fromtimestamp(
                execution.next_attempt_at.timestamp() + delay,
                UTC,
            )

        if not delay:
            execution.next_attempt_at = None
            await self.publish(execution, attempt=attempt)
            return
        self._schedule_retry(execution, attempt=attempt, delay=delay)

    def _schedule_retry(
        self,
        execution: ExecutionRecord,
        *,
        attempt: int,
        delay: float | None = None,
    ) -> None:
        if delay is None:
            if execution.next_attempt_at is None:
                return
            delay = max(
                0.0,
                (execution.next_attempt_at - datetime.now(UTC)).total_seconds(),
            )

        async def enqueue() -> None:
            if delay:
                await asyncio.sleep(delay)
            execution.next_attempt_at = None
            await self.publish(execution, attempt=attempt)

        task = asyncio.create_task(
            enqueue(),
            name=f"superjobs-retry-{execution.job_id}",
        )
        self._retry_tasks.add(task)
        task.add_done_callback(self._retry_tasks.discard)

    async def get_execution(
        self,
        identity: JobIdentity,
        job_id: str,
    ) -> ExecutionRecord | None:
        execution = self._executions.get(job_id)
        if execution is None or execution.identity != identity:
            return None
        self._expire_result(execution)
        return execution

    def _expire_result(self, execution: ExecutionRecord) -> None:
        if execution.result_expired or execution.completed_at is None:
            return
        retention = self.result_retention
        too_old = (
            retention.max_age is not None
            and datetime.now(UTC) - execution.completed_at > retention.max_age
        )
        too_large = (
            retention.max_bytes is not None
            and len(execution.result_payload) > retention.max_bytes
        )
        if too_old or too_large:
            execution.result_payload = b""
            execution.result_expired = True

    async def write_completion(
        self,
        execution: ExecutionRecord,
        *,
        state: JobState,
        result_payload: bytes = b"",
        result_media_type: str | None = None,
        error: JobError | None = None,
    ) -> ExecutionRecord:
        if (
            state is JobState.COMPLETED
            and self.max_result_bytes is not None
            and len(result_payload) > self.max_result_bytes
        ):
            raise ResultTooLargeError(
                f"Result is larger than the configured {self.max_result_bytes}-byte limit",
            )
        async with self._execution_locks[execution.job_id]:
            if execution.state in {
                JobState.COMPLETED,
                JobState.FAILED,
                JobState.CANCELLED,
            }:
                return execution
            execution.state = state
            execution.completed_at = datetime.now(UTC)
            execution.result_payload = result_payload
            execution.result_media_type = result_media_type
            execution.error = error
            execution.next_attempt_at = None
            self._status_events[execution.job_id].set()
        return execution

    async def request_cancel(self, identity: JobIdentity, job_id: str) -> JobStatus:
        execution = await self.get_execution(identity, job_id)
        if execution is None:
            raise KeyError(job_id)
        pending = False
        async with self._execution_locks[job_id]:
            if execution.state not in {
                JobState.COMPLETED,
                JobState.FAILED,
                JobState.CANCELLED,
            }:
                execution.cancellation_requested = True
                pending = execution.state is JobState.PENDING
                if pending:
                    execution.state = JobState.CANCELLED
                    execution.completed_at = datetime.now(UTC)
                    execution.error = JobError(
                        code="cancelled",
                        message="Cancellation requested before the Attempt started",
                    )
                if not pending:
                    self._status_events[job_id].set()
                if not pending:
                    self._cancel_events[job_id].set()
        if pending:
            error = execution.error
            if error is None:
                raise RuntimeError("Pending cancellation has no error record")
            await self._append_terminal(
                execution,
                JobCancelled(reason=error.message),
            )
        return execution.status()

    async def _append_terminal(self, execution: ExecutionRecord, data: Any) -> None:
        try:
            await self.publish_terminal_observation(
                execution.identity,
                execution.job_id,
                attempt=execution.attempt,
                event=data,
            )
        finally:
            self._status_events[execution.job_id].set()

    async def is_cancel_requested(self, job_id: str) -> bool:
        execution = self._executions.get(job_id)
        return execution.cancellation_requested if execution is not None else False

    async def wait_for_cancellation(self, job_id: str) -> None:
        if await self.is_cancel_requested(job_id):
            return
        await self._cancel_events[job_id].wait()

    async def publish_observations(
        self,
        identity: JobIdentity,
        job_id: str,
        *,
        attempt: int,
        events: list[Any],
        progress: ProgressSnapshot | None = None,
    ) -> None:
        if not events:
            return
        execution = await self.get_execution(identity, job_id)
        if execution is None:
            raise KeyError(job_id)
        async with self._observation_locks[job_id]:
            if execution.terminal_event_published and not any(
                isinstance(event, (JobCompleted, JobFailed, JobCancelled))
                for event in events
            ):
                return
            if progress is not None and execution.state not in {
                JobState.COMPLETED,
                JobState.FAILED,
                JobState.CANCELLED,
            }:
                execution.progress = progress
                self.progress_update_log.append((execution.job_id, progress))
            first = self._observation_sequences[job_id] + 1
            self._observation_sequences[job_id] += len(events)
            wrapped = [
                JobEvent(
                    job_id=job_id,
                    sequence=sequence,
                    timestamp=datetime.now(UTC),
                    attempt=attempt,
                    data=event,
                )
                for sequence, event in enumerate(
                    events,
                    start=first,
                )
            ]
            await self._append_observations_locked(job_id, wrapped)

    async def publish_terminal_observation(
        self,
        identity: JobIdentity,
        job_id: str,
        *,
        attempt: int,
        event: Any,
    ) -> bool:
        execution = await self.get_execution(identity, job_id)
        if execution is None:
            raise KeyError(job_id)
        async with self._observation_locks[job_id]:
            if execution.terminal_event_published:
                return False
            sequence = self._observation_sequences[job_id] + 1
            self._observation_sequences[job_id] = sequence
            wrapped = JobEvent(
                job_id=job_id,
                sequence=sequence,
                timestamp=datetime.now(UTC),
                attempt=attempt,
                data=event,
            )
            try:
                await self._append_observations_locked(job_id, [wrapped])
            except Exception:
                if self._observation_sequences[job_id] == sequence:
                    self._observation_sequences[job_id] -= 1
                raise
            execution.terminal_event_published = True
            self._status_events[job_id].set()
            return True

    async def append_observations(
        self,
        identity: JobIdentity,
        job_id: str,
        events: list[JobEvent[Any]],
    ) -> None:
        execution = await self.get_execution(identity, job_id)
        if execution is None:
            raise KeyError(job_id)
        if execution.terminal_event_published and not any(
            isinstance(event.data, (JobCompleted, JobFailed, JobCancelled))
            for event in events
        ):
            return
        async with self._observation_locks[job_id]:
            await self._append_observations_locked(job_id, events)

    async def _append_observations_locked(
        self,
        job_id: str,
        events: list[JobEvent[Any]],
    ) -> None:
        self._observations[job_id].extend(events)
        self._prune_observations(job_id)
        subscribers = tuple(self._observation_subscribers[job_id])
        for event in events:
            for queue in subscribers:
                await queue.put(event)
            if isinstance(event.data, (JobCompleted, JobFailed, JobCancelled)):
                for queue in subscribers:
                    await queue.put(None)

    async def allocate_observation_sequences(
        self,
        identity: JobIdentity,
        job_id: str,
        count: int,
    ) -> list[int]:
        if count < 0:
            raise ValueError("count must be non-negative")
        execution = await self.get_execution(identity, job_id)
        if execution is None:
            raise KeyError(job_id)
        async with self._observation_locks[job_id]:
            first = self._observation_sequences[job_id] + 1
            self._observation_sequences[job_id] += count
            return list(range(first, first + count))

    async def observations(
        self,
        identity: JobIdentity,
        job_id: str,
        *,
        after: int = 0,
    ) -> AsyncIterator[JobEvent[Any]]:
        execution = await self.get_execution(identity, job_id)
        if execution is None:
            raise KeyError(job_id)

        async with self._observation_locks[job_id]:
            all_history = self._observations[job_id]
            self._prune_observations(job_id)
            all_history = self._observations[job_id]
            if all_history and after < all_history[0].sequence - 1:
                raise ObservationExpiredError(
                    f"Observations before sequence {all_history[0].sequence} expired",
                )
            if (
                not all_history
                and after < self._observation_sequences[job_id]
            ):
                raise ObservationExpiredError(
                    "The requested observation cursor has expired",
                )
            history = [
                event for event in all_history if event.sequence > after
            ]
            terminal_seen = any(
                isinstance(event.data, (JobCompleted, JobFailed, JobCancelled))
                for event in all_history
            ) or execution.state in {
                JobState.COMPLETED,
                JobState.FAILED,
                JobState.CANCELLED,
            }
            queue: asyncio.Queue[JobEvent[Any] | None] | None
            if terminal_seen:
                queue = None
            else:
                queue = asyncio.Queue()
                self._observation_subscribers[job_id].add(queue)

        return _InMemoryObservationStream(self, job_id, history, queue)

    def _prune_observations(self, job_id: str) -> None:
        history = self._observations[job_id]
        retention = self.observation_retention
        if retention.max_age is not None:
            cutoff = datetime.now(UTC) - retention.max_age
            history[:] = [event for event in history if event.timestamp >= cutoff]
        if retention.max_events is not None:
            del history[:-retention.max_events]
        if retention.max_bytes is not None:
            size = 0
            kept: list[JobEvent[Any]] = []
            for event in reversed(history):
                event_size = len(repr(event).encode("utf-8"))
                if kept and size + event_size > retention.max_bytes:
                    break
                kept.append(event)
                size += event_size
            history[:] = reversed(kept)

    async def subscribe_work(self, identity: JobIdentity) -> WorkSubscription:
        if not self.started:
            raise RuntimeError("Transport must be started before subscribing")
        queue = self._work_queues.setdefault(
            identity.canonical_name,
            asyncio.Queue(),
        )
        subscription = _InMemoryWorkSubscription(self, identity, queue)
        self._work_subscriptions[identity.canonical_name].add(subscription)
        return subscription

    async def wait_for_status(
        self,
        identity: JobIdentity,
        job_id: str,
    ) -> JobStatus:
        execution = await self.get_execution(identity, job_id)
        if execution is None:
            raise KeyError(job_id)
        terminal_states = {
            JobState.COMPLETED,
            JobState.FAILED,
            JobState.CANCELLED,
        }
        while execution.state not in terminal_states:
            event = self._status_events[job_id]
            await event.wait()
            event.clear()
        return execution.status()

    async def signal_status(self, job_id: str) -> None:
        self._status_events[job_id].set()

    async def claim_terminal_event(
        self,
        identity: JobIdentity,
        job_id: str,
    ) -> bool:
        execution = await self.get_execution(identity, job_id)
        if execution is None:
            raise KeyError(job_id)
        async with self._execution_locks[job_id]:
            if execution.state not in {
                JobState.COMPLETED,
                JobState.FAILED,
                JobState.CANCELLED,
            } or execution.terminal_event_published:
                return False
            execution.terminal_event_published = True
            return True

    async def release_terminal_event(
        self,
        identity: JobIdentity,
        job_id: str,
    ) -> None:
        execution = await self.get_execution(identity, job_id)
        if execution is None:
            raise KeyError(job_id)
        async with self._execution_locks[job_id]:
            if execution.state in {
                JobState.COMPLETED,
                JobState.FAILED,
                JobState.CANCELLED,
            }:
                execution.terminal_event_published = False

    async def mark_started(self, execution: ExecutionRecord, attempt: int) -> None:
        terminal_event: JobFailed | JobCancelled | None = None
        async with self._execution_locks[execution.job_id]:
            if execution.state in {
                JobState.COMPLETED,
                JobState.FAILED,
                JobState.CANCELLED,
            }:
                return
            if execution.cancellation_requested:
                execution.state = JobState.CANCELLED
                execution.completed_at = datetime.now(UTC)
                execution.error = JobError(
                    code="cancelled",
                    message="Cancellation requested before the Attempt started",
                )
                terminal_event = JobCancelled(reason=execution.error.message)
            elif (
                execution.deadline is not None
                and datetime.now(UTC) >= execution.deadline
            ):
                execution.state = JobState.FAILED
                execution.completed_at = datetime.now(UTC)
                execution.error = JobError(
                    code="deadline_exceeded",
                    message="The execution deadline was exceeded before starting",
                )
                terminal_event = JobFailed(error=execution.error)
            else:
                execution.state = JobState.RUNNING
                execution.attempt = attempt
                execution.started_at = execution.started_at or datetime.now(UTC)
                execution.next_attempt_at = None
            if terminal_event is None:
                self._status_events[execution.job_id].set()
        if terminal_event is not None:
            await self._append_terminal(execution, terminal_event)

    async def update_progress(
        self,
        execution: ExecutionRecord,
        progress: ProgressSnapshot,
    ) -> None:
        if execution.state in {
            JobState.COMPLETED,
            JobState.FAILED,
            JobState.CANCELLED,
        }:
            return
        execution.progress = progress
        self.progress_update_log.append((execution.job_id, progress))
        self._status_events[execution.job_id].set()

    async def mark_retry(
        self,
        execution: ExecutionRecord,
        *,
        next_attempt_at: datetime | None,
    ) -> None:
        if execution.state in {
            JobState.COMPLETED,
            JobState.FAILED,
            JobState.CANCELLED,
        }:
            return
        if execution.cancellation_requested:
            execution.state = JobState.CANCELLED
            execution.completed_at = datetime.now(UTC)
            execution.next_attempt_at = None
            execution.progress = None
            execution.error = JobError(
                code="cancelled",
                message="Cancellation requested while the Attempt was failing",
            )
        else:
            execution.state = JobState.PENDING
            execution.next_attempt_at = next_attempt_at
            execution.progress = None
            self._status_events[execution.job_id].set()
