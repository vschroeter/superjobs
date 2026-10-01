from __future__ import annotations

import asyncio
import hashlib
import logging
import uuid
from collections import defaultdict
from collections.abc import AsyncIterator
from datetime import UTC, datetime, timedelta
from typing import Any, cast

import msgpack
from faststream.nats import JStream, NatsBroker
from faststream.nats.subscriber.usecases import LogicSubscriber
from nats.js.api import KeyValueConfig
from nats.js.errors import APIError, NoKeysError, NotFoundError

from superjobs.exceptions.jobs import (
    IdempotencyConflictError,
    ObservationExpiredError,
    ResultTooLargeError,
)
from superjobs.jobs.events import (
    JobCancelled,
    JobCompleted,
    JobEvent,
    JobFailed,
    JobLog,
    JobProgress,
    JobRetryScheduled,
    JobStarted,
)
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
from superjobs.transport.implementations.nats import NatsDelivery, NatsQueueConfig
from superjobs.transport.transport import Destination, Source, Transport

logger = logging.getLogger(__name__)


class _NatsWorkSubscription(WorkSubscription):
    def __init__(
        self,
        backend: NatsJobBackend,
        identity: JobIdentity,
        subscriber: LogicSubscriber[Any],
    ):
        self._backend = backend
        self._identity = identity
        self._subscriber = subscriber
        self._iterator = cast(AsyncIterator[Any], subscriber.__aiter__())
        self._closed = False

    def __aiter__(self) -> _NatsWorkSubscription:
        return self

    async def __anext__(self) -> WorkItem:
        if self._closed:
            raise StopAsyncIteration
        while True:
            message = await anext(self._iterator)
            try:
                wire = _unpack(message.body)
                job_id = str(wire["job_id"])
            except Exception:
                await message.reject()
                continue
            try:
                execution = await self._backend.get_execution(
                    self._identity,
                    job_id,
                )
            except Exception:
                retry_delay = self._backend.queue_config.retry_delay
                if retry_delay is None:
                    await message.nack()
                else:
                    await message.nack(delay=retry_delay)
                continue
            try:
                if execution is None:
                    await message.reject()
                    continue
                work_execution = execution
                not_before_value = wire.get("not_before")
                if not_before_value:
                    not_before = datetime.fromisoformat(str(not_before_value))
                    remaining = (not_before - datetime.now(UTC)).total_seconds()
                    if remaining > 0:
                        await message.nack(delay=remaining)
                        continue
                delivery_count = getattr(
                    getattr(message, "metadata", None),
                    "num_delivered",
                    1,
                )
                attempt = max(
                    int(wire.get("attempt", 1)),
                    int(delivery_count or 1),
                )
                return WorkItem(
                    execution=execution,
                    attempt=attempt,
                    delivery=NatsDelivery(
                        message,
                        attempt=attempt,
                        retry_publisher=(
                            lambda next_attempt, delay: self._backend._publish_retry(
                                work_execution,
                                next_attempt,
                                delay,
                            )
                        ),
                    ),
                )
            except Exception:
                await message.reject()
                continue

    async def close(self) -> None:
        if not self._closed:
            self._closed = True
            await self._subscriber.stop()


class _NatsObservationStream:
    def __init__(
        self,
        backend: NatsJobBackend,
        identity: JobIdentity,
        job_id: str,
        subscriber: LogicSubscriber[Any],
        after: int,
    ):
        self._backend = backend
        self._identity = identity
        self._job_id = job_id
        self._subscriber = subscriber
        self._iterator = cast(AsyncIterator[Any], subscriber.__aiter__())
        self._after = after
        self._pending: list[JobEvent[Any]] = []
        self._terminal = False
        self._closed = False
        self._last_sequence = after

    def __aiter__(self) -> _NatsObservationStream:
        return self

    async def __anext__(self) -> JobEvent[Any]:
        if self._closed:
            raise StopAsyncIteration
        if self._pending:
            return self._pending.pop(0)
        if self._terminal:
            await self.aclose()
            raise StopAsyncIteration

        while not self._pending and not self._terminal:
            message = await anext(self._iterator)
            batch = _unpack(message.body)
            for wire_event in batch.get("events", []):
                event = self._backend._event_from_wire(
                    self._identity,
                    wire_event,
                )
                if event.sequence > self._last_sequence + 1:
                    await self.aclose()
                    raise ObservationExpiredError(
                        "The requested observation cursor has expired",
                    )
                self._last_sequence = max(self._last_sequence, event.sequence)
                is_terminal = isinstance(
                    event.data,
                    (JobCompleted, JobFailed, JobCancelled),
                )
                if event.sequence <= self._after:
                    if is_terminal:
                        self._terminal = True
                    continue
                self._pending.append(event)
                self._after = event.sequence
                if is_terminal:
                    self._terminal = True
                    break
            await message.ack_sync()

        if not self._pending:
            await self.aclose()
            raise StopAsyncIteration
        return self._pending.pop(0)

    async def aclose(self) -> None:
        if not self._closed:
            self._closed = True
            await self._subscriber.stop()


class NatsJobBackend(JobBackend):
    """JetStream-backed implementation of the public job runtime semantics."""

    def __init__(
        self,
        broker: NatsBroker,
        queue_config: NatsQueueConfig | None = None,
    ):
        self.broker = broker
        self.queue_config = queue_config or NatsQueueConfig()
        self._transport = Transport(broker, self.queue_config)
        ack_wait = self.queue_config.ack_wait or 30.0
        self.heartbeat_interval = max(0.001, ack_wait / 3)
        self._observation_lock_ttl = max(1.0, ack_wait)
        self.observation_retention = ObservationRetention(
            max_age=(timedelta(seconds=self.queue_config.observation_max_age) if self.queue_config.observation_max_age is not None else None),
            max_events=self.queue_config.observation_max_msgs,
            max_bytes=self.queue_config.observation_max_bytes,
        )
        self.result_retention = ResultRetention(
            max_age=(timedelta(seconds=self.queue_config.result_max_age) if self.queue_config.result_max_age is not None else None),
        )
        self.max_result_bytes = self.queue_config.max_result_bytes
        self.started = False
        self._jobs: dict[str, Any] = {}
        self._publishers: dict[str, Any] = {}
        self._completion_kv = None
        self._idempotency_kv = None
        self._sequence_kv = None
        self._deadline_tasks: dict[str, asyncio.Task[None]] = {}
        self._observation_locks: defaultdict[str, asyncio.Lock] = defaultdict(
            asyncio.Lock,
        )

    def register_job(self, job) -> None:
        existing = self._jobs.get(job.canonical_name)
        if existing is not None and existing.identity != job.identity:
            raise ValueError(f"Conflicting Job definition for {job}")
        self._jobs[job.canonical_name] = job

    async def start(self) -> None:
        if self.started:
            return
        await self._transport.start()
        try:
            await self._transport._ensure_stream(
                JStream(
                    name=self.queue_config.stream,
                    subjects=[f"{self.queue_config.subject_prefix}.work.>"],
                ),
            )
            await self._transport._ensure_stream(
                JStream(
                    name=self.queue_config.observation_stream,
                    subjects=[f"{self.queue_config.subject_prefix}.obs.>"],
                    max_age=(self.observation_retention.max_age.total_seconds() if self.observation_retention.max_age is not None else None),
                    max_bytes=self.observation_retention.max_bytes,
                    max_msgs_per_subject=(self.observation_retention.max_events if self.observation_retention.max_events is not None else -1),
                ),
            )
            self._completion_kv = await self._ensure_kv(
                self.queue_config.completion_bucket,
            )
            self._idempotency_kv = await self._ensure_kv(
                self.queue_config.idempotency_bucket,
            )
            self._sequence_kv = await self._ensure_kv(
                f"{self.queue_config.subject_prefix}-sequences",
            )
            self.started = True
            await self._restore_deadlines()
        except BaseException:
            self.started = False
            await self._transport.stop()
            raise

    async def _restore_deadlines(self) -> None:
        completion_kv = self._completion_kv
        if completion_kv is None:
            return
        try:
            keys = await completion_kv.keys()
        except NoKeysError:
            keys = ()
        for key in keys:
            try:
                entry = await completion_kv.get(key)
            except NotFoundError:
                continue
            if entry.value is None:
                continue
            record = _record_from_wire(_unpack(entry.value))
            if record.state is JobState.PENDING and record.deadline is not None:
                self._schedule_deadline(record)

    async def stop(self) -> None:
        if not self.started:
            return
        for publisher in self._publishers.values():
            try:
                await publisher.stop()
            except Exception:
                pass
        self._publishers.clear()
        deadline_tasks = tuple(self._deadline_tasks.values())
        for task in deadline_tasks:
            if not task.done():
                task.cancel()
        if deadline_tasks:
            await asyncio.gather(*deadline_tasks, return_exceptions=True)
        self._deadline_tasks.clear()
        await self._transport.stop()
        self.started = False

    async def _ensure_kv(self, bucket: str):
        jetstream = self.broker.connection.jetstream()
        try:
            return await jetstream.key_value(bucket)
        except NotFoundError:
            try:
                await jetstream.create_key_value(
                    KeyValueConfig(bucket=bucket, history=1),
                )
            except APIError:
                pass
            return await jetstream.key_value(bucket)

    async def _kv_get(self, bucket, key: str) -> bytes | None:
        try:
            return (await bucket.get(key)).value
        except NotFoundError:
            return None

    async def _kv_put(self, bucket, key: str, value: bytes) -> None:
        await bucket.put(key, value)

    async def _kv_create(self, bucket, key: str, value: bytes) -> bool:
        try:
            await bucket.create(key, value)
            return True
        except APIError:
            return False

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

        idempotency_key = self._idempotency_key(
            identity,
            options.caller_scope,
            options.idempotency_key,
        )
        mapping = await self._kv_get(self._idempotency_kv, idempotency_key)
        if mapping is not None:
            execution = await self.get_execution(identity, _unpack(mapping)["job_id"])
            if execution is None or execution.fingerprint != fingerprint:
                raise IdempotencyConflictError(
                    "Idempotency key was already used with a different submission",
                )
            if publish and execution.state is JobState.PENDING and not execution.published:
                await self.publish(execution)
                await self._mark_published(execution)
            self._schedule_deadline(execution)
            return execution

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
        execution_key = self._execution_key(execution.job_id)
        created = await self._kv_create(
            self._completion_kv,
            execution_key,
            _pack(_record_to_wire(execution)),
        )
        if not created:
            existing = await self.get_execution(identity, execution.job_id)
            if existing is None or existing.fingerprint != fingerprint:
                raise IdempotencyConflictError(
                    f"Job ID {execution.job_id!r} was already used for another submission",
                )
            if publish and existing.state is JobState.PENDING and not existing.published:
                await self.publish(existing)
                await self._mark_published(existing)
            return existing

        mapping_value = _pack(
            {
                "job_id": execution.job_id,
                "fingerprint": fingerprint,
            },
        )
        if not await self._kv_create(
            self._idempotency_kv,
            idempotency_key,
            mapping_value,
        ):
            existing_mapping = await self._kv_get(
                self._idempotency_kv,
                idempotency_key,
            )
            existing_id = _unpack(existing_mapping)["job_id"] if existing_mapping else ""
            existing = await self.get_execution(identity, existing_id)
            if existing is None or existing.fingerprint != fingerprint:
                raise IdempotencyConflictError(
                    "Idempotency key was already used with a different submission",
                )
            if publish and existing.state is JobState.PENDING and not existing.published:
                await self.publish(existing)
                await self._mark_published(existing)
            return existing

        if publish:
            await self.publish(execution)
            await self._mark_published(execution)
        self._schedule_deadline(execution)
        return execution

    async def publish(
        self,
        execution: ExecutionRecord,
        *,
        attempt: int = 1,
        not_before: datetime | None = None,
    ) -> None:
        subject = self._work_subject(execution.identity)
        publisher = await self._publisher(
            subject,
            self.queue_config.stream,
        )
        await publisher.publish(
            _pack(
                {
                    "job_id": execution.job_id,
                    "attempt": attempt,
                    "payload": execution.request_payload,
                    "media_type": execution.request_media_type,
                    "not_before": (not_before.isoformat() if not_before is not None else None),
                },
            ),
            headers={"content-type": "application/msgpack"},
            correlation_id=execution.job_id,
        )

    async def _mark_published(self, execution: ExecutionRecord) -> None:
        completion_kv = self._completion_kv
        if completion_kv is None:
            raise RuntimeError("NATS backend is not started")
        key = self._execution_key(execution.job_id)
        while True:
            entry = await completion_kv.get(key)
            if entry.value is None:
                raise KeyError(execution.job_id)
            current = _record_from_wire(_unpack(entry.value))
            if current.identity != execution.identity:
                raise KeyError(execution.job_id)
            if current.published:
                execution.__dict__.update(current.__dict__)
                return
            current.published = True
            try:
                await completion_kv.update(
                    key,
                    _pack(_record_to_wire(current)),
                    last=entry.revision,
                )
            except APIError:
                continue
            execution.__dict__.update(current.__dict__)
            return

    async def _publish_retry(
        self,
        execution: ExecutionRecord,
        attempt: int,
        delay: timedelta | None,
    ) -> None:
        not_before = datetime.now(UTC) + delay if delay is not None and delay.total_seconds() > 0 else None
        await self.publish(
            execution,
            attempt=attempt,
            not_before=not_before,
        )

    async def get_execution(
        self,
        identity: JobIdentity,
        job_id: str,
    ) -> ExecutionRecord | None:
        if self._completion_kv is None:
            return None
        value = await self._kv_get(self._completion_kv, self._execution_key(job_id))
        if value is None:
            return None
        record = _record_from_wire(_unpack(value))
        if record.identity != identity:
            return None
        self._schedule_deadline(record)
        if (
            not record.result_expired
            and record.completed_at is not None
            and (
                (self.result_retention.max_age is not None and datetime.now(UTC) - record.completed_at > self.result_retention.max_age)
                or (self.result_retention.max_bytes is not None and len(record.result_payload) > self.result_retention.max_bytes)
            )
        ):
            record.result_payload = b""
            record.result_expired = True
            await self._kv_put(
                self._completion_kv,
                self._execution_key(job_id),
                _pack(_record_to_wire(record)),
            )
        return record

    def _schedule_deadline(self, execution: ExecutionRecord) -> None:
        if not self.started or execution.deadline is None:
            return
        if execution.state is not JobState.PENDING:
            return
        existing = self._deadline_tasks.get(execution.job_id)
        if existing is not None and not existing.done():
            return

        async def enforce() -> None:
            try:
                deadline = execution.deadline
                if deadline is None:
                    return
                delay = (deadline - datetime.now(UTC)).total_seconds()
                if delay > 0:
                    await asyncio.sleep(delay)
                completion_kv = self._completion_kv
                if completion_kv is None:
                    return
                key = self._execution_key(execution.job_id)
                while True:
                    entry = await completion_kv.get(key)
                    if entry.value is None:
                        return
                    current = _record_from_wire(_unpack(entry.value))
                    if current.state is not JobState.PENDING or current.deadline is None:
                        return
                    if datetime.now(UTC) < current.deadline:
                        await asyncio.sleep(
                            (current.deadline - datetime.now(UTC)).total_seconds(),
                        )
                        continue
                    current.state = JobState.FAILED
                    current.completed_at = datetime.now(UTC)
                    current.error = JobError(
                        code="deadline_exceeded",
                        message="The execution deadline was exceeded before starting",
                    )
                    try:
                        await completion_kv.update(
                            key,
                            _pack(_record_to_wire(current)),
                            last=entry.revision,
                        )
                    except APIError:
                        continue
                    await self._append_terminal_event(
                        current,
                        JobFailed(error=current.error),
                    )
                    return
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception(
                    "Unable to enforce deadline for %s",
                    execution.job_id,
                )

        task = asyncio.create_task(
            enforce(),
            name=f"superjobs-deadline-{execution.job_id}",
        )
        self._deadline_tasks[execution.job_id] = task
        task.add_done_callback(
            lambda completed: self._deadline_tasks.pop(execution.job_id, None) if self._deadline_tasks.get(execution.job_id) is completed else None,
        )

    async def write_completion(
        self,
        execution: ExecutionRecord,
        *,
        state: JobState,
        result_payload: bytes = b"",
        result_media_type: str | None = None,
        error: JobError | None = None,
    ) -> ExecutionRecord:
        if state is JobState.COMPLETED and self.max_result_bytes is not None and len(result_payload) > self.max_result_bytes:
            raise ResultTooLargeError(
                f"Result is larger than the configured {self.max_result_bytes}-byte limit",
            )
        completion_kv = self._completion_kv
        if completion_kv is None:
            raise RuntimeError("NATS backend is not started")
        key = self._execution_key(execution.job_id)
        while True:
            entry = await completion_kv.get(key)
            if entry.value is None:
                raise KeyError(execution.job_id)
            current = _record_from_wire(_unpack(entry.value))
            if current.identity != execution.identity:
                raise KeyError(execution.job_id)
            if current.state in {
                JobState.COMPLETED,
                JobState.FAILED,
                JobState.CANCELLED,
            }:
                execution.__dict__.update(current.__dict__)
                return current
            current.state = state
            current.completed_at = datetime.now(UTC)
            current.result_payload = result_payload
            current.result_media_type = result_media_type
            current.error = error
            current.next_attempt_at = None
            try:
                await completion_kv.update(
                    key,
                    _pack(_record_to_wire(current)),
                    last=entry.revision,
                )
            except APIError:
                continue
            execution.__dict__.update(current.__dict__)
            return current

    async def request_cancel(self, identity: JobIdentity, job_id: str) -> JobStatus:
        completion_kv = self._completion_kv
        if completion_kv is None:
            raise RuntimeError("NATS backend is not started")
        key = self._execution_key(job_id)
        while True:
            entry = await completion_kv.get(key)
            if entry.value is None:
                raise KeyError(job_id)
            execution = _record_from_wire(_unpack(entry.value))
            if execution.identity != identity:
                raise KeyError(job_id)
            if execution.state in {
                JobState.COMPLETED,
                JobState.FAILED,
                JobState.CANCELLED,
            }:
                return execution.status()

            execution.cancellation_requested = True
            pending = execution.state is JobState.PENDING
            if pending:
                execution.state = JobState.CANCELLED
                execution.completed_at = datetime.now(UTC)
                execution.error = JobError(
                    code="cancelled",
                    message="Cancellation requested before the Attempt started",
                )
            try:
                await completion_kv.update(
                    key,
                    _pack(_record_to_wire(execution)),
                    last=entry.revision,
                )
            except APIError:
                continue
            if pending:
                error = execution.error
                if error is None:
                    raise RuntimeError("Pending cancellation has no error record")
                await self._append_terminal_event(
                    execution,
                    JobCancelled(reason=error.message),
                )
            return execution.status()

    async def _append_terminal_event(self, execution: ExecutionRecord, data: Any) -> None:
        await self.publish_terminal_observation(
            execution.identity,
            execution.job_id,
            attempt=execution.attempt,
            event=data,
        )

    async def claim_terminal_event(
        self,
        identity: JobIdentity,
        job_id: str,
    ) -> bool:
        completion_kv = self._completion_kv
        if completion_kv is None:
            raise RuntimeError("NATS backend is not started")
        key = self._execution_key(job_id)
        while True:
            entry = await completion_kv.get(key)
            if entry.value is None:
                raise KeyError(job_id)
            current = _record_from_wire(_unpack(entry.value))
            if current.identity != identity:
                raise KeyError(job_id)
            if (
                current.state
                not in {
                    JobState.COMPLETED,
                    JobState.FAILED,
                    JobState.CANCELLED,
                }
                or current.terminal_event_published
            ):
                return False
            current.terminal_event_published = True
            try:
                await completion_kv.update(
                    key,
                    _pack(_record_to_wire(current)),
                    last=entry.revision,
                )
            except APIError:
                continue
            return True

    async def release_terminal_event(
        self,
        identity: JobIdentity,
        job_id: str,
    ) -> None:
        completion_kv = self._completion_kv
        if completion_kv is None:
            raise RuntimeError("NATS backend is not started")
        key = self._execution_key(job_id)
        while True:
            entry = await completion_kv.get(key)
            if entry.value is None:
                raise KeyError(job_id)
            current = _record_from_wire(_unpack(entry.value))
            if current.identity != identity:
                raise KeyError(job_id)
            if not current.terminal_event_published:
                return
            current.terminal_event_published = False
            try:
                await completion_kv.update(
                    key,
                    _pack(_record_to_wire(current)),
                    last=entry.revision,
                )
            except APIError:
                continue
            return

    async def is_cancel_requested(self, job_id: str) -> bool:
        if self._completion_kv is None:
            return False
        value = await self._kv_get(self._completion_kv, self._execution_key(job_id))
        if value is None:
            return False
        return bool(_unpack(value).get("cancellation_requested", False))

    async def wait_for_cancellation(self, job_id: str) -> None:
        while not await self.is_cancel_requested(job_id):
            await asyncio.sleep(0.1)

    async def _acquire_observation_lock(
        self,
        identity: JobIdentity,
        job_id: str,
    ) -> str:
        completion_kv = self._completion_kv
        if completion_kv is None:
            raise RuntimeError("NATS backend is not started")
        key = self._execution_key(job_id)
        token = uuid.uuid4().hex
        while True:
            entry = await completion_kv.get(key)
            if entry.value is None:
                raise KeyError(job_id)
            current = _record_from_wire(_unpack(entry.value))
            if current.identity != identity:
                raise KeyError(job_id)
            now = datetime.now(UTC)
            if current.observation_lock_token is not None and current.observation_lock_expires_at is not None and current.observation_lock_expires_at > now:
                await asyncio.sleep(0.01)
                continue
            current.observation_lock_token = token
            current.observation_lock_expires_at = now + timedelta(
                seconds=self._observation_lock_ttl,
            )
            try:
                await completion_kv.update(
                    key,
                    _pack(_record_to_wire(current)),
                    last=entry.revision,
                )
            except APIError:
                continue
            return token

    async def _release_observation_lock(self, job_id: str, token: str) -> None:
        completion_kv = self._completion_kv
        if completion_kv is None:
            return
        key = self._execution_key(job_id)
        while True:
            try:
                entry = await completion_kv.get(key)
            except NotFoundError:
                return
            if entry.value is None:
                return
            current = _record_from_wire(_unpack(entry.value))
            if current.observation_lock_token != token:
                return
            current.observation_lock_token = None
            current.observation_lock_expires_at = None
            try:
                await completion_kv.update(
                    key,
                    _pack(_record_to_wire(current)),
                    last=entry.revision,
                )
            except APIError:
                continue
            return

    async def publish_terminal_observation(
        self,
        identity: JobIdentity,
        job_id: str,
        *,
        attempt: int,
        event: Any,
    ) -> bool:
        async with self._observation_locks[job_id]:
            token = await self._acquire_observation_lock(identity, job_id)
            try:
                execution = await self.get_execution(identity, job_id)
                if execution is None:
                    raise KeyError(job_id)
                if execution.terminal_event_published:
                    return False
                sequences = await self.allocate_observation_sequences(
                    identity,
                    job_id,
                    1,
                )
                wrapped = JobEvent(
                    job_id=job_id,
                    sequence=sequences[0],
                    timestamp=datetime.now(UTC),
                    attempt=attempt,
                    data=event,
                )
                await self.append_observations(
                    identity,
                    job_id,
                    [wrapped],
                )
                await self.claim_terminal_event(identity, job_id)
                return True
            finally:
                await self._release_observation_lock(job_id, token)

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
        async with self._observation_locks[job_id]:
            token = await self._acquire_observation_lock(identity, job_id)
            try:
                execution = await self.get_execution(identity, job_id)
                if execution is None:
                    raise KeyError(job_id)
                if execution.terminal_event_published and not any(isinstance(event, (JobCompleted, JobFailed, JobCancelled)) for event in events):
                    return
                if progress is not None and execution.state not in {
                    JobState.COMPLETED,
                    JobState.FAILED,
                    JobState.CANCELLED,
                }:
                    await self.update_progress(execution, progress)
                sequences = await self.allocate_observation_sequences(
                    identity,
                    job_id,
                    len(events),
                )
                wrapped = [
                    JobEvent(
                        job_id=job_id,
                        sequence=sequence,
                        timestamp=datetime.now(UTC),
                        attempt=attempt,
                        data=event,
                    )
                    for sequence, event in zip(sequences, events, strict=True)
                ]
                await self.append_observations(identity, job_id, wrapped)
            finally:
                await self._release_observation_lock(job_id, token)

    async def append_observations(
        self,
        identity: JobIdentity,
        job_id: str,
        events: list[JobEvent[Any]],
    ) -> None:
        if not events:
            return
        execution = await self.get_execution(identity, job_id)
        if execution is None:
            raise KeyError(job_id)
        if execution.terminal_event_published and not any(isinstance(event.data, (JobCompleted, JobFailed, JobCancelled)) for event in events):
            return
        subject = self._observation_subject(identity, job_id)
        publisher = await self._publisher(
            subject,
            self.queue_config.observation_stream,
        )
        await publisher.publish(
            _pack(
                {
                    "events": [self._event_to_wire(identity, event) for event in events],
                },
            ),
            headers={"content-type": "application/msgpack"},
            correlation_id=job_id,
        )

    async def allocate_observation_sequences(
        self,
        identity: JobIdentity,
        job_id: str,
        count: int,
    ) -> list[int]:
        if count < 0:
            raise ValueError("count must be non-negative")
        sequence_kv = self._sequence_kv
        if sequence_kv is None:
            raise RuntimeError("NATS backend is not started")
        key = self._execution_key(job_id)
        while True:
            try:
                entry = await sequence_kv.get(key)
                if entry.value is None:
                    raise ValueError("Observation sequence entry has no value")
                current = int(_unpack(entry.value))
                next_value = current + count
                await sequence_kv.update(key, _pack(next_value), last=entry.revision)
                return list(range(current + 1, next_value + 1))
            except NotFoundError:
                if await self._kv_create(sequence_kv, key, _pack(count)):
                    return list(range(1, count + 1))
            except APIError:
                continue

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
        await self._validate_observation_cursor(identity, job_id, after)
        subscriber = await self._transport.create_subscriber(
            Source(
                name=self._observation_subject(identity, job_id),
                stream=self.queue_config.observation_stream,
            ),
            start=True,
            durable=False,
        )
        return _NatsObservationStream(
            self,
            identity,
            job_id,
            subscriber,
            after,
        )

    async def _validate_observation_cursor(
        self,
        identity: JobIdentity,
        job_id: str,
        after: int,
    ) -> None:
        sequence_kv = self._sequence_kv
        if sequence_kv is None:
            raise RuntimeError("NATS backend is not started")
        value = await self._kv_get(
            sequence_kv,
            self._execution_key(job_id),
        )
        if value is None:
            return
        last_sequence = int(_unpack(value))
        if after >= last_sequence:
            return

        subject = self._observation_subject(identity, job_id)
        jetstream = self.broker.connection.jetstream()
        try:
            message = await jetstream.get_msg(
                self.queue_config.observation_stream,
                subject=subject,
                direct=True,
            )
        except NotFoundError as exception:
            raise ObservationExpiredError(
                "The requested observation cursor has expired",
            ) from exception
        except Exception:
            return
        data = message.data
        if not data:
            raise ObservationExpiredError(
                "The requested observation cursor has expired",
            )
        events = _unpack(data).get("events", [])
        if not events:
            raise ObservationExpiredError(
                "The requested observation cursor has expired",
            )

    async def subscribe_work(self, identity: JobIdentity) -> WorkSubscription:
        subscriber = await self._transport.create_subscriber(
            Source(
                name=self._work_subject(identity),
                stream=self.queue_config.stream,
            ),
            start=True,
        )
        return _NatsWorkSubscription(self, identity, subscriber)

    async def wait_for_status(
        self,
        identity: JobIdentity,
        job_id: str,
    ) -> JobStatus:
        while True:
            execution = await self.get_execution(identity, job_id)
            if execution is None:
                raise KeyError(job_id)
            if execution.state in {
                JobState.COMPLETED,
                JobState.FAILED,
                JobState.CANCELLED,
            }:
                return execution.status()
            await asyncio.sleep(0.05)

    async def signal_status(self, job_id: str) -> None:
        return

    async def mark_started(self, execution: ExecutionRecord, attempt: int) -> None:
        completion_kv = self._completion_kv
        if completion_kv is None:
            raise RuntimeError("NATS backend is not started")
        key = self._execution_key(execution.job_id)
        while True:
            entry = await completion_kv.get(key)
            if entry.value is None:
                raise KeyError(execution.job_id)
            current = _record_from_wire(_unpack(entry.value))
            if current.identity != execution.identity:
                raise KeyError(execution.job_id)
            if current.state in {
                JobState.COMPLETED,
                JobState.FAILED,
                JobState.CANCELLED,
            }:
                execution.__dict__.update(current.__dict__)
                return

            terminal_event: JobFailed | JobCancelled | None = None
            if current.cancellation_requested:
                current.state = JobState.CANCELLED
                current.completed_at = datetime.now(UTC)
                current.error = JobError(
                    code="cancelled",
                    message="Cancellation requested before the Attempt started",
                )
                terminal_event = JobCancelled(reason=current.error.message)
            elif current.deadline is not None and datetime.now(UTC) >= current.deadline:
                current.state = JobState.FAILED
                current.completed_at = datetime.now(UTC)
                current.error = JobError(
                    code="deadline_exceeded",
                    message="The execution deadline was exceeded before starting",
                )
                terminal_event = JobFailed(error=current.error)
            else:
                current.state = JobState.RUNNING
                current.attempt = attempt
                current.started_at = current.started_at or datetime.now(UTC)
                current.next_attempt_at = None
            try:
                await completion_kv.update(
                    key,
                    _pack(_record_to_wire(current)),
                    last=entry.revision,
                )
            except APIError:
                continue
            execution.__dict__.update(current.__dict__)
            if terminal_event is not None:
                await self._append_terminal_event(execution, terminal_event)
            return

    async def update_progress(
        self,
        execution: ExecutionRecord,
        progress: ProgressSnapshot,
    ) -> None:
        completion_kv = self._completion_kv
        if completion_kv is None:
            raise RuntimeError("NATS backend is not started")
        key = self._execution_key(execution.job_id)
        while True:
            entry = await completion_kv.get(key)
            if entry.value is None:
                raise KeyError(execution.job_id)
            current = _record_from_wire(_unpack(entry.value))
            if current.identity != execution.identity:
                raise KeyError(execution.job_id)
            if current.state in {
                JobState.COMPLETED,
                JobState.FAILED,
                JobState.CANCELLED,
            }:
                execution.__dict__.update(current.__dict__)
                return
            current.progress = progress
            try:
                await completion_kv.update(
                    key,
                    _pack(_record_to_wire(current)),
                    last=entry.revision,
                )
            except APIError:
                continue
            execution.__dict__.update(current.__dict__)
            return

    async def mark_retry(
        self,
        execution: ExecutionRecord,
        *,
        next_attempt_at: datetime | None,
    ) -> None:
        completion_kv = self._completion_kv
        if completion_kv is None:
            raise RuntimeError("NATS backend is not started")
        key = self._execution_key(execution.job_id)
        while True:
            entry = await completion_kv.get(key)
            if entry.value is None:
                raise KeyError(execution.job_id)
            current = _record_from_wire(_unpack(entry.value))
            if current.identity != execution.identity:
                raise KeyError(execution.job_id)
            if current.state in {
                JobState.COMPLETED,
                JobState.FAILED,
                JobState.CANCELLED,
            }:
                execution.__dict__.update(current.__dict__)
                return
            if current.cancellation_requested:
                current.state = JobState.CANCELLED
                current.completed_at = datetime.now(UTC)
                current.next_attempt_at = None
                current.progress = None
                current.error = JobError(
                    code="cancelled",
                    message="Cancellation requested while the Attempt was failing",
                )
            else:
                current.state = JobState.PENDING
                current.next_attempt_at = next_attempt_at
                current.progress = None
            try:
                await completion_kv.update(
                    key,
                    _pack(_record_to_wire(current)),
                    last=entry.revision,
                )
            except APIError:
                continue
            execution.__dict__.update(current.__dict__)
            return

    async def _publisher(self, subject: str, stream: str):
        key = f"{stream}:{subject}"
        if key not in self._publishers:
            self._publishers[key] = await self._transport.create_publisher(
                Destination(name=subject, stream=stream),
                start=True,
            )
        return self._publishers[key]

    def _work_subject(self, identity: JobIdentity) -> str:
        return f"{self.queue_config.subject_prefix}.work.{identity.canonical_name}"

    def _observation_subject(self, identity: JobIdentity, job_id: str) -> str:
        execution_token = hashlib.sha256(job_id.encode("utf-8")).hexdigest()
        return f"{self.queue_config.subject_prefix}.obs.{identity.canonical_name}.{execution_token}"

    def _execution_key(self, job_id: str) -> str:
        return hashlib.sha256(job_id.encode("utf-8")).hexdigest()

    def _idempotency_key(self, identity: JobIdentity, scope: str, key: str) -> str:
        raw = "\0".join((identity.canonical_name, scope, key))
        return hashlib.sha256(raw.encode("utf-8")).hexdigest()

    def _event_to_wire(self, identity: JobIdentity, event: JobEvent[Any]) -> dict[str, Any]:
        job = self._jobs.get(identity.canonical_name)
        return {
            "job_id": event.job_id,
            "sequence": event.sequence,
            "timestamp": event.timestamp.isoformat(),
            "attempt": event.attempt,
            "data": _data_to_wire(job, event.data),
        }

    def _event_from_wire(
        self,
        identity: JobIdentity,
        value: dict[str, Any],
    ) -> JobEvent[Any]:
        job = self._jobs.get(identity.canonical_name)
        return JobEvent(
            job_id=str(value["job_id"]),
            sequence=int(value["sequence"]),
            timestamp=datetime.fromisoformat(value["timestamp"]),
            attempt=int(value["attempt"]),
            data=_data_from_wire(job, value["data"]),
        )


def _pack(value: Any) -> bytes:
    return cast(bytes, msgpack.packb(value, use_bin_type=True))


def _unpack(value: bytes) -> Any:
    return msgpack.unpackb(value, raw=False)


def _record_to_wire(record: ExecutionRecord) -> dict[str, Any]:
    return {
        "name": record.identity.name,
        "version": record.identity.version,
        "job_id": record.job_id,
        "request_payload": record.request_payload,
        "request_media_type": record.request_media_type,
        "fingerprint": record.fingerprint,
        "idempotency_key": record.idempotency_key,
        "caller_scope": record.caller_scope,
        "timeout": record.timeout,
        "deadline": record.deadline.isoformat() if record.deadline else None,
        "created_at": record.created_at.isoformat(),
        "state": record.state.value,
        "attempt": record.attempt,
        "progress": (
            {
                "completed": record.progress.completed,
                "total": record.progress.total,
            }
            if record.progress
            else None
        ),
        "started_at": record.started_at.isoformat() if record.started_at else None,
        "completed_at": (record.completed_at.isoformat() if record.completed_at else None),
        "cancellation_requested": record.cancellation_requested,
        "next_attempt_at": (record.next_attempt_at.isoformat() if record.next_attempt_at else None),
        "result_payload": record.result_payload,
        "result_media_type": record.result_media_type,
        "error": record.error.to_wire() if record.error else None,
        "result_expired": record.result_expired,
        "terminal_event_published": record.terminal_event_published,
        "published": record.published,
        "observation_lock_token": record.observation_lock_token,
        "observation_lock_expires_at": (record.observation_lock_expires_at.isoformat() if record.observation_lock_expires_at else None),
    }


def _record_from_wire(value: dict[str, Any]) -> ExecutionRecord:
    progress = value.get("progress")
    error = value.get("error")
    return ExecutionRecord(
        identity=JobIdentity(value["name"], version=value.get("version")),
        job_id=str(value["job_id"]),
        request_payload=value["request_payload"],
        request_media_type=value.get("request_media_type"),
        fingerprint=value["fingerprint"],
        idempotency_key=value["idempotency_key"],
        caller_scope=value.get("caller_scope", "default"),
        timeout=value.get("timeout"),
        deadline=(datetime.fromisoformat(value["deadline"]) if value.get("deadline") else None),
        created_at=datetime.fromisoformat(value["created_at"]),
        state=JobState(value["state"]),
        attempt=int(value.get("attempt", 0)),
        progress=(ProgressSnapshot(progress["completed"], progress.get("total")) if progress else None),
        started_at=(datetime.fromisoformat(value["started_at"]) if value.get("started_at") else None),
        completed_at=(datetime.fromisoformat(value["completed_at"]) if value.get("completed_at") else None),
        cancellation_requested=bool(value.get("cancellation_requested", False)),
        next_attempt_at=(datetime.fromisoformat(value["next_attempt_at"]) if value.get("next_attempt_at") else None),
        result_payload=value.get("result_payload", b""),
        result_media_type=value.get("result_media_type"),
        error=JobError.from_wire(error) if error else None,
        result_expired=bool(value.get("result_expired", False)),
        terminal_event_published=bool(
            value.get("terminal_event_published", False),
        ),
        published=bool(value.get("published", False)),
        observation_lock_token=value.get("observation_lock_token"),
        observation_lock_expires_at=(datetime.fromisoformat(value["observation_lock_expires_at"]) if value.get("observation_lock_expires_at") else None),
    )


def _data_to_wire(job: Any, data: Any) -> dict[str, Any]:
    if isinstance(data, JobStarted):
        return {"kind": "started"}
    if isinstance(data, JobCompleted):
        return {"kind": "completed"}
    if isinstance(data, JobFailed):
        return {"kind": "failed", "error": data.error.to_wire()}
    if isinstance(data, JobCancelled):
        return {"kind": "cancelled", "reason": data.reason}
    if isinstance(data, JobRetryScheduled):
        return {
            "kind": "retry_scheduled",
            "attempt": data.attempt,
            "delay": data.delay,
        }
    if isinstance(data, JobProgress):
        return {
            "kind": "progress",
            "completed": data.completed,
            "total": data.total,
        }
    if isinstance(data, JobLog):
        return {
            "kind": "log",
            "message": data.message,
            "level": data.level,
            "extra": data.extra,
        }
    if job is None or job.event_codec is None:
        raise TypeError("No application event codec is registered")
    return {
        "kind": "application",
        "payload": job.encode_event(data),
    }


def _data_from_wire(job: Any, value: dict[str, Any]) -> Any:
    kind = value["kind"]
    if kind == "started":
        return JobStarted()
    if kind == "completed":
        return JobCompleted()
    if kind == "failed":
        return JobFailed(JobError.from_wire(value["error"]))
    if kind == "cancelled":
        return JobCancelled(value.get("reason"))
    if kind == "retry_scheduled":
        return JobRetryScheduled(
            attempt=int(value["attempt"]),
            delay=value.get("delay"),
        )
    if kind == "progress":
        return JobProgress(
            completed=int(value["completed"]),
            total=value.get("total"),
        )
    if kind == "log":
        return JobLog(
            message=value["message"],
            level=value.get("level", "info"),
            extra=value.get("extra", {}),
        )
    if kind == "application":
        if job is None:
            raise TypeError("No Job definition is registered")
        return job.decode_event(value["payload"])
    raise ValueError(f"Unknown observation kind: {kind}")
