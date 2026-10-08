from __future__ import annotations

import asyncio
import inspect
import logging
from collections.abc import Awaitable, Callable
from datetime import UTC, datetime, timedelta
from typing import Any

from superjobs.exceptions.jobs import (
    InvalidResultError,
    JobCancelledError,
    ResultTooLargeError,
)
from superjobs.jobs.events import (
    JobCancelled,
    JobCompleted,
    JobFailed,
    JobRetryScheduled,
    JobStarted,
)
from superjobs.jobs.execution import JobError, JobState
from superjobs.jobs.job_context import JobContext, ObservationPolicy
from superjobs.jobs.retry_policy import RetryPolicy
from superjobs.transport.backend import ExecutionRecord, JobBackend, WorkItem


logger = logging.getLogger(__name__)


class JobHandler[ReqT: Any | None, FinalT: Any | None, InterT: Any | None]:
    def __init__(
        self,
        job,
        callback: Callable[..., FinalT | Awaitable[FinalT]],
        *,
        backend: JobBackend,
        concurrency: int = 1,
        retry: RetryPolicy | None = None,
        observation_policy: ObservationPolicy | None = None,
        heartbeat_interval: float | None = None,
    ):
        if concurrency < 1:
            raise ValueError("concurrency must be at least one")
        effective_heartbeat_interval = (
            getattr(backend, "heartbeat_interval", 5.0)
            if heartbeat_interval is None
            else heartbeat_interval
        )
        if effective_heartbeat_interval <= 0:
            raise ValueError("heartbeat_interval must be positive")
        self.job = job
        self.callback = callback
        self.backend = backend
        self.concurrency = concurrency
        self.retry_policy = retry or RetryPolicy()
        self.observation_policy = observation_policy or ObservationPolicy()
        self.heartbeat_interval = effective_heartbeat_interval

        self.request_subscriber = None
        self._start_task: asyncio.Task[None] | None = None
        self._consumer_task: asyncio.Task[None] | None = None
        self._active_tasks: set[asyncio.Task[None]] = set()
        self._stop_event = asyncio.Event()
        self._ready_event = asyncio.Event()
        self._lifecycle_lock = asyncio.Lock()

    async def start(self) -> None:
        async with self._lifecycle_lock:
            if self._consumer_task is not None and not self._consumer_task.done():
                return
            self._stop_event.clear()
            self._ready_event.clear()
            self.request_subscriber = await self.backend.subscribe_work(self.job.identity)
            self._consumer_task = asyncio.create_task(
                self._consume(),
                name=f"superjobs-consumer-{self.job.canonical_name}",
            )
            self._ready_event.set()

    def schedule_start(self) -> None:
        if self._start_task is None or self._start_task.done():
            self._start_task = asyncio.create_task(
                self.start(),
                name=f"superjobs-start-{self.job.canonical_name}",
            )

    async def stop_admission(self) -> None:
        self._stop_event.set()
        subscriber = self.request_subscriber
        if subscriber is not None:
            await subscriber.close()

        if self._start_task is not None and not self._start_task.done():
            self._start_task.cancel()
        if self._consumer_task is not None and not self._consumer_task.done():
            self._consumer_task.cancel()
        await asyncio.gather(
            *(
                task
                for task in (self._start_task, self._consumer_task)
                if task is not None
            ),
            return_exceptions=True,
        )
        self.request_subscriber = None
        self._start_task = None
        self._consumer_task = None
        self._ready_event.clear()

    async def stop(
        self,
        *,
        graceful: bool = True,
        timeout: float | None = None,
    ) -> None:
        if timeout is not None and timeout <= 0:
            raise ValueError("timeout must be positive")
        await self.stop_admission()

        active = tuple(self._active_tasks)
        if active:
            if graceful:
                wait = asyncio.gather(*active, return_exceptions=True)
                if timeout is None:
                    await wait
                else:
                    try:
                        await asyncio.wait_for(wait, timeout=timeout)
                    except TimeoutError:
                        graceful = False
            if not graceful:
                for task in active:
                    if not task.done():
                        task.cancel()
                await asyncio.gather(*active, return_exceptions=True)

    async def wait_ready(self) -> None:
        if self._start_task is not None:
            await self._start_task
        if self._consumer_task is None:
            raise RuntimeError(f"Job handler for {self.job} has not been started")
        await self._ready_event.wait()

    async def _consume(self) -> None:
        if self.request_subscriber is None:
            raise RuntimeError("Job subscriber is not initialized")
        try:
            async for item in self.request_subscriber:
                if self._stop_event.is_set():
                    return
                await self._dispatch(item)
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception("Job subscriber failed for %s", self.job)

    async def _dispatch(self, item: WorkItem) -> None:
        semaphore = getattr(self, "_semaphore", None)
        if semaphore is None:
            semaphore = self._semaphore = asyncio.Semaphore(self.concurrency)
        await semaphore.acquire()
        task = asyncio.create_task(
            self._run_with_slot(item, semaphore),
            name=f"superjobs-attempt-{item.execution.job_id}",
        )
        self._active_tasks.add(task)
        task.add_done_callback(self._active_tasks.discard)
        if self.concurrency == 1:
            await asyncio.shield(task)

    async def _run_with_slot(
        self,
        item: WorkItem,
        semaphore: asyncio.Semaphore,
    ) -> None:
        try:
            await self._process_item(item)
        except asyncio.CancelledError:
            try:
                await self.backend.mark_retry(
                    item.execution,
                    next_attempt_at=None,
                )
                await item.delivery.retry(attempt=item.attempt + 1)
            except Exception:
                logger.exception("Unable to retry cancelled Attempt")
            raise
        finally:
            semaphore.release()

    async def _process_item(self, item: WorkItem) -> None:
        execution = item.execution
        attempt = item.attempt
        if execution.state in {
            JobState.COMPLETED,
            JobState.FAILED,
            JobState.CANCELLED,
        }:
            if not await self._publish_terminal_observation(execution):
                await self._retry_terminal_delivery(item)
                return
            await self.backend.signal_status(execution.job_id)
            await item.delivery.ack()
            return

        if await self.backend.is_cancel_requested(execution.job_id):
            await self._complete_cancelled(item, reason="Cancellation requested")
            return

        if execution.deadline is not None and datetime.now(UTC) >= execution.deadline:
            await self._complete_failure(
                item,
                JobError(
                    code="deadline_exceeded",
                    message="The execution deadline was exceeded before starting",
                ),
            )
            return

        try:
            request = self.job.decode_request(execution.request_payload)
        except Exception as exception:
            await item.delivery.reject(reason=str(exception))
            return

        await self.backend.mark_started(execution, attempt)
        if execution.state is not JobState.RUNNING:
            await item.delivery.ack()
            return
        context = JobContext(
            self.job,
            backend=self.backend,
            execution=execution,
            delivery=item.delivery,
            attempt=attempt,
            observation_policy=self.observation_policy,
        )
        heartbeat = asyncio.create_task(
            self._heartbeat(item),
            name=f"superjobs-heartbeat-{execution.job_id}",
        )
        try:
            try:
                await self._append_system_event(execution, attempt, JobStarted())
            except Exception:
                logger.exception("Unable to publish JobStarted for %s", execution.job_id)
            async def invoke_callback() -> Any:
                arguments = (
                    (request, context)
                    if self.job.request_type is not None
                    else (context,)
                )
                if inspect.iscoroutinefunction(self.callback):
                    result = self.callback(*arguments)
                else:
                    result = await asyncio.to_thread(self.callback, *arguments)
                if inspect.isawaitable(result):
                    return await result
                return result

            timeout = _effective_timeout(execution)
            if timeout is None:
                result = await invoke_callback()
            else:
                async with asyncio.timeout(timeout):
                    result = await invoke_callback()
            if _deadline_exceeded(execution.deadline):
                raise TimeoutError
            try:
                result_payload = self.job.encode_result(result)
            except Exception as exception:
                raise InvalidResultError(str(exception)) from exception
            await context.flush()
            completion = await self.backend.write_completion(
                execution,
                state=JobState.COMPLETED,
                result_payload=result_payload,
                result_media_type=(
                    self.job.result_codec.media_type
                    if self.job.result_codec is not None
                    else None
                ),
            )
            if not await self._publish_terminal_observation(completion):
                await self._retry_terminal_delivery(item)
                return
            await self.backend.signal_status(execution.job_id)
            await item.delivery.ack()
        except JobCancelledError as exception:
            try:
                await context.flush()
            except Exception:
                logger.exception("Unable to flush observations during cancellation")
            await self._complete_cancelled(item, reason=exception.reason)
        except asyncio.TimeoutError:
            deadline_exceeded = _deadline_exceeded(execution.deadline)
            await self._handle_failure(
                item,
                context,
                JobError(
                    code="deadline_exceeded" if deadline_exceeded else "timed_out",
                    message=(
                        "The execution deadline was exceeded"
                        if deadline_exceeded
                        else "The Attempt exceeded its timeout"
                    ),
                    type_name="TimeoutError",
                ),
                TimeoutError(),
            )
        except asyncio.CancelledError:
            raise
        except ResultTooLargeError as exception:
            await self._handle_failure(
                item,
                context,
                JobError(
                    code=exception.code,
                    message=str(exception),
                    type_name=exception.__class__.__name__,
                ),
                exception,
            )
        except InvalidResultError as exception:
            await self._handle_failure(
                item,
                context,
                JobError(
                    code=exception.code,
                    message=str(exception),
                    type_name=exception.__class__.__name__,
                ),
                exception,
            )
        except Exception as exception:
            await self._handle_failure(
                item,
                context,
                JobError.from_exception(exception),
                exception,
            )
        finally:
            heartbeat.cancel()
            await asyncio.gather(heartbeat, return_exceptions=True)
            await context.close()

    async def _handle_failure(
        self,
        item: WorkItem,
        context: JobContext[Any],
        error: JobError,
        exception: BaseException,
    ) -> None:
        execution = item.execution
        try:
            await context.flush()
        except Exception:
            logger.exception(
                "Unable to flush observations after failure of %s",
                execution.job_id,
            )

        if (
            self.retry_policy.should_retry(
                attempt=item.attempt,
                exception=exception,
            )
            and not _deadline_exceeded(execution.deadline)
        ):
            delay = self.retry_policy.delay(attempt=item.attempt)
            next_attempt_at = datetime.fromtimestamp(
                datetime.now(UTC).timestamp() + delay,
                UTC,
            )
            await self.backend.mark_retry(
                execution,
                next_attempt_at=next_attempt_at,
            )
            if execution.state is JobState.CANCELLED:
                if not await self._publish_terminal_observation(execution):
                    await self._retry_terminal_delivery(item)
                    return
                await self.backend.signal_status(execution.job_id)
                await item.delivery.ack()
                return
            try:
                await self._append_system_event(
                    execution,
                    item.attempt,
                    JobRetryScheduled(
                        attempt=item.attempt + 1,
                        delay=delay,
                    ),
                )
            except Exception:
                logger.exception(
                    "Unable to publish retry observation for %s",
                    execution.job_id,
                )
            await item.delivery.retry(
                delay=timedelta(seconds=delay),
                attempt=item.attempt + 1,
            )
            return

        await self._complete_failure(item, error)

    async def _complete_failure(self, item: WorkItem, error: JobError) -> None:
        completion = await self.backend.write_completion(
            item.execution,
            state=JobState.FAILED,
            error=error,
        )
        if not await self._publish_terminal_observation(completion):
            await self._retry_terminal_delivery(item)
            return
        await self.backend.signal_status(item.execution.job_id)
        await item.delivery.ack()

    async def _complete_cancelled(
        self,
        item: WorkItem,
        *,
        reason: str | None,
    ) -> None:
        completion = await self.backend.write_completion(
            item.execution,
            state=JobState.CANCELLED,
            error=JobError(
                code="cancelled",
                message=reason or "Job cancelled",
            ),
        )
        if not await self._publish_terminal_observation(completion):
            await self._retry_terminal_delivery(item)
            return
        await self.backend.signal_status(item.execution.job_id)
        await item.delivery.ack()

    async def _publish_terminal_observation(
        self,
        completion: ExecutionRecord,
    ) -> bool:
        try:
            if completion.state is JobState.COMPLETED:
                data: Any = JobCompleted()
            elif completion.state is JobState.FAILED:
                data = JobFailed(
                    completion.error
                    or JobError(
                        code="failed",
                        message="Job failed without an error record",
                    ),
                )
            elif completion.state is JobState.CANCELLED:
                data = JobCancelled(
                    reason=(
                        completion.error.message
                        if completion.error is not None
                        else "Job cancelled"
                    ),
                )
            else:
                return True
            await self.backend.publish_terminal_observation(
                completion.identity,
                completion.job_id,
                attempt=completion.attempt,
                event=data,
            )
        except Exception:
            logger.exception(
                "Unable to publish terminal observation for %s",
                completion.job_id,
            )
            return False
        return True

    async def _retry_terminal_delivery(self, item: WorkItem) -> None:
        configured_delay = getattr(
            getattr(self.backend, "queue_config", None),
            "retry_delay",
            None,
        )
        delay = (
            float(configured_delay)
            if configured_delay is not None and configured_delay >= 0
            else 0.1
        )
        try:
            await item.delivery.retry(
                delay=timedelta(seconds=delay),
                attempt=item.attempt,
            )
        except Exception:
            logger.exception(
                "Unable to retry terminal observation for %s",
                item.execution.job_id,
            )

    async def _append_system_event(
        self,
        execution: ExecutionRecord,
        attempt: int,
        data: Any,
    ) -> None:
        await self.backend.publish_observations(
            execution.identity,
            execution.job_id,
            attempt=attempt,
            events=[data],
        )

    async def _heartbeat(self, item: WorkItem) -> None:
        try:
            while True:
                await asyncio.sleep(self.heartbeat_interval)
                await item.delivery.extend_lease()
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception("Lease heartbeat failed for %s", item.execution.job_id)


def _deadline_exceeded(deadline: datetime | None) -> bool:
    return deadline is not None and datetime.now(UTC) >= deadline


def _effective_timeout(execution: ExecutionRecord) -> float | None:
    timeout = execution.timeout
    if execution.deadline is None:
        return timeout
    remaining = (execution.deadline - datetime.now(UTC)).total_seconds()
    if remaining <= 0:
        return 0.0
    if timeout is None:
        return remaining
    return min(timeout, remaining)
