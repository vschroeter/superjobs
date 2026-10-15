"""Admitted execution retains ownership until actual work and cleanup finish."""

from __future__ import annotations

import asyncio
import copy
import inspect
import logging
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, replace
from datetime import UTC, datetime, timedelta
from typing import Any, TypeVar
from uuid import uuid4

import nats.errors
from nats.js.errors import APIError

from superjobs.exceptions.jobs import (
    InvalidResultError,
    JobCancelledError,
    ResultTooLargeError,
)
from superjobs.jobs.events import JobRetryScheduled, JobStarted
from superjobs.jobs.execution import JobError, JobState
from superjobs.jobs.job_context import JobContext
from superjobs.jobs.resource_cleanup import (
    ResourceCleanupError,
    init_resource_cleanup_state,
    resource_cleanup_acknowledged,
    resource_cleanup_failed_reason,
)
from superjobs.jobs.resource_runtime import ResourceRuntimeCoordinator
from superjobs.resources import (
    AcquisitionOutcome,
    AdmissionAcquireIntent,
    AdmissionOwner,
    ReconcileOutcome,
    ReleaseOutcome,
    ResourceAdmission,
    ResourcePartialAcquisitionError,
    ResourceRequirement,
    ResourceStaleOperationError,
    ResourceStoreUnavailableError,
    WorkerIncarnation,
)
from superjobs.transport.backend import ExecutionRecord, WorkItem

logger = logging.getLogger(__name__)
T = TypeVar("T")
TERMINAL = frozenset({JobState.COMPLETED, JobState.FAILED, JobState.CANCELLED})
TRANSIENT = (
    ResourceStoreUnavailableError,
    ConnectionError,
    TimeoutError,
    OSError,
    nats.errors.Error,
    APIError,
)


@dataclass(frozen=True, slots=True)
class ResourceHandlerConfig:
    requirement: ResourceRequirement
    admission: ResourceAdmission
    incarnation: Callable[[], WorkerIncarnation]
    coordinator: ResourceRuntimeCoordinator | None = None
    poll_interval: float = 0.05


async def _withdraw(config: ResourceHandlerConfig) -> None:
    if config.coordinator is not None:
        await config.coordinator.withdraw_for_resource(config.requirement.resource_key)


async def _retry(
    operation: Callable[[], Awaitable[T]], config: ResourceHandlerConfig
) -> T:
    """Keep the accepted invocation/outcome while individual broker calls fail."""
    while True:
        try:
            return await operation()
        except TRANSIENT as error:
            logger.warning("Resource operation awaits broker reconciliation: %s", error)
            await _withdraw(config)
            await asyncio.sleep(config.poll_interval)


async def _shield_until_done(
    task: asyncio.Task[T], cancelled: asyncio.Event
) -> tuple[T, bool]:
    interrupted = False
    while True:
        try:
            return await asyncio.shield(task), interrupted
        except asyncio.CancelledError:
            if task.done():
                return task.result(), True
            interrupted = True
            cancelled.set()


async def _refresh(handler: Any, item: WorkItem) -> None:
    current = await handler.backend.get_execution(
        item.execution.identity, item.execution.job_id
    )
    if current is None:
        raise ResourceStoreUnavailableError(
            "Execution record unavailable during admission"
        )
    item.execution.__dict__.update(current.__dict__)


async def _terminal(handler: Any, item: WorkItem) -> None:
    if not await handler._publish_terminal_observation(item.execution):
        await handler._retry_terminal_delivery(item)
        return
    await handler.backend.signal_status(item.execution.job_id)
    await item.delivery.ack()


async def _maintain(
    handler: Any,
    item: WorkItem,
    config: ResourceHandlerConfig,
    owner: list[AdmissionOwner | None],
) -> None:
    while True:
        try:
            await item.delivery.extend_lease()
            if owner[0] is not None:
                if item.execution.state in TERMINAL:
                    owner[0] = None
                    continue
                result = await config.admission.reconcile(owner[0])
                if result.outcome not in {
                    ReconcileOutcome.RECONCILED,
                    ReconcileOutcome.UNAVAILABLE,
                }:
                    if (
                        await config.admission.execution_owner(owner[0].execution_id)
                        != owner[0]
                    ):
                        await _withdraw(config)
        except TRANSIENT:
            await _withdraw(config)
            if owner[0] is not None:
                try:
                    await config.admission.mark_uncertain(owner[0])
                except TRANSIENT:
                    pass
        await asyncio.sleep(min(1.0, handler.heartbeat_interval))


async def process_resource_bound_item(
    handler: Any, item: WorkItem, config: ResourceHandlerConfig
) -> None:
    cancelled = asyncio.Event()
    lifetime = asyncio.create_task(
        _process_invocation(handler, item, config, cancelled)
    )
    _, interrupted = await _shield_until_done(lifetime, cancelled)
    if interrupted:
        raise asyncio.CancelledError


async def _process_invocation(
    handler: Any,
    item: WorkItem,
    config: ResourceHandlerConfig,
    cancelled: asyncio.Event,
) -> None:
    # In-memory deliveries share authoritative records, but invocation tokens cannot.
    item = replace(item, execution=copy.copy(item.execution))
    owner: list[AdmissionOwner | None] = [None]
    maintenance = asyncio.create_task(_maintain(handler, item, config, owner))
    try:
        intent = AdmissionAcquireIntent(
            incarnation=config.incarnation(),
            invocation_id=uuid4(),
            execution_id=item.execution.job_id,
            resource_key=config.requirement.resource_key,
        )
        while owner[0] is None:
            await _retry(lambda: _refresh(handler, item), config)
            if item.execution.state in TERMINAL:
                await _terminal(handler, item)
                return
            if cancelled.is_set():
                await _retry(lambda: item.delivery.retry(attempt=item.attempt), config)
                return
            holder = await _retry(
                lambda: config.admission.execution_owner(intent.execution_id), config
            )
            if holder is not None and holder.invocation_id != intent.invocation_id:
                await asyncio.sleep(config.poll_interval)
                continue
            error = None
            terminal_state = JobState.CANCELLED
            if await _retry(
                lambda: handler.backend.is_cancel_requested(intent.execution_id), config
            ):
                error = JobError(
                    code="cancelled",
                    message="Cancellation requested while waiting for a resource",
                )
            elif (
                item.execution.deadline is not None
                and datetime.now(UTC) >= item.execution.deadline
            ):
                terminal_state = JobState.FAILED
                error = JobError(
                    code="deadline_exceeded",
                    message="Execution deadline exceeded while waiting for a resource",
                )
            if error is not None:
                await _retry(
                    lambda: handler.backend.write_completion(
                        item.execution,
                        state=terminal_state,
                        error=error,
                        only_if_waiting=True,
                    ),
                    config,
                )
                await _refresh(handler, item)
                if item.execution.state in TERMINAL:
                    await _terminal(handler, item)
                    return
                continue
            try:
                result = await _retry(
                    lambda: config.admission.try_acquire(intent), config
                )
            except ResourcePartialAcquisitionError:
                # Reuse the same intent to reconcile a lost write acknowledgement.
                await asyncio.sleep(config.poll_interval)
                continue
            if result.outcome in {
                AcquisitionOutcome.ACQUIRED,
                AcquisitionOutcome.ALREADY_HELD,
            }:
                owner[0] = result.owner
                assert owner[0] is not None
                break
            if result.outcome is AcquisitionOutcome.NOT_DECLARED:
                raise ValueError(
                    f"Resource capacity is not declared: {intent.resource_key}"
                )
            await _retry(
                lambda: handler.backend.mark_started(
                    item.execution,
                    item.attempt,
                    target_state=JobState.WAITING_FOR_RESOURCE,
                ),
                config,
            )
            await asyncio.sleep(config.poll_interval)
        await _run_admitted(handler, item, config, owner[0], cancelled)
    except ResourceStaleOperationError as error:
        # A fenced invocation cannot release or acknowledge a successor.
        await _withdraw(config)
        logger.warning(
            "Invocation fenced for execution %s: %s", item.execution.job_id, error
        )
    except Exception:
        await _withdraw(config)
        raise
    finally:
        maintenance.cancel()
        await asyncio.gather(maintenance, return_exceptions=True)


async def _invoke(handler: Any, request: Any, context: JobContext[Any]) -> Any:
    arguments = (
        (request, context) if handler.job.request_type is not None else (context,)
    )
    if inspect.iscoroutinefunction(handler.callback):
        result = handler.callback(*arguments)
    else:
        result = await asyncio.to_thread(handler.callback, *arguments)
    return await result if inspect.isawaitable(result) else result


async def _run_admitted(
    handler: Any,
    item: WorkItem,
    config: ResourceHandlerConfig,
    owner: AdmissionOwner,
    cancelled: asyncio.Event,
) -> None:
    await _retry(
        lambda: handler.backend.mark_started(
            item.execution, item.attempt, admission_owner=owner
        ),
        config,
    )
    await _refresh(handler, item)
    if item.execution.state in TERMINAL:
        await _retry(
            lambda: config.admission.release_resource_after_cleanup(owner), config
        )
        await _retry(lambda: config.admission.finalize_execution_claim(owner), config)
        await _terminal(handler, item)
        return
    if cancelled.is_set():
        await _retry(
            lambda: config.admission.release_resource_after_cleanup(owner), config
        )
        await _retry(
            lambda: handler.backend.mark_retry(
                item.execution, next_attempt_at=None, expected_admission_owner=owner
            ),
            config,
        )
        if item.execution.state not in TERMINAL:
            await _retry(lambda: item.delivery.retry(attempt=item.attempt), config)
        await _retry(lambda: config.admission.finalize_execution_claim(owner), config)
        return
    context = JobContext(
        handler.job,
        backend=handler.backend,
        execution=item.execution,
        delivery=item.delivery,
        attempt=item.attempt,
        observation_policy=handler.observation_policy,
    )
    init_resource_cleanup_state(context)
    cancellation_watch: asyncio.Task[bool] | None = None
    try:
        try:
            request = (
                handler.job.decode_request(item.execution.request_payload)
                if handler.job.request_type is not None
                else None
            )
        except Exception:
            await _retry(
                lambda: config.admission.release_acknowledged_cleanup(owner), config
            )
            await item.delivery.reject(reason="Invalid resource-bound request")
            return
        try:
            await handler._append_system_event(
                item.execution, item.attempt, JobStarted()
            )
        except TRANSIENT:
            pass
        callback = asyncio.create_task(_invoke(handler, request, context))
        cancellation_watch = asyncio.create_task(cancelled.wait())
        from superjobs.jobs.job_handler import _effective_timeout

        timeout = _effective_timeout(item.execution)
        done, _ = await asyncio.wait(
            {callback, cancellation_watch},
            timeout=timeout,
            return_when=asyncio.FIRST_COMPLETED,
        )
        timed_out = not done
        if timed_out or cancelled.is_set():
            context._cancelled = True
            context._cancel_reason = (
                "Execution timeout" if timed_out else "Worker stopping"
            )
        # Never cancel a wrapper awaiting a native thread; drain the actual callback.
        try:
            result = await callback
            returned = True
            exception: BaseException | None = None
        except (Exception, asyncio.CancelledError) as caught:
            result = None
            returned = False
            exception = caught
        cleanup_failed = resource_cleanup_failed_reason(context)
        cleaned = (
            (returned or resource_cleanup_acknowledged(context))
            and cleanup_failed is None
            and not isinstance(exception, ResourceCleanupError)
        )
        if not cleaned:
            await _retry(
                lambda: config.admission.retain_cleanup_failed(
                    owner, reason_detail=cleanup_failed or str(exception)
                ),
                config,
            )
            await _withdraw(config)
            await _persist(
                handler,
                item,
                config,
                owner,
                state=JobState.FAILED,
                error=JobError(
                    code="resource_cleanup_failed",
                    message=cleanup_failed or "Handler cleanup was not acknowledged",
                ),
            )
            await _terminal(handler, item)
            return
        try:
            await context.flush()
        except TRANSIENT:
            logger.warning("Observations await broker recovery for %s", context.id)
        if await _retry(
            lambda: handler.backend.is_cancel_requested(context.id), config
        ):
            context._cancelled = True
        if timed_out or (
            item.execution.deadline is not None
            and datetime.now(UTC) >= item.execution.deadline
        ):
            deadline = (
                item.execution.deadline is not None
                and datetime.now(UTC) >= item.execution.deadline
            )
            error = JobError(
                code="deadline_exceeded" if deadline else "timed_out",
                message="The execution deadline was exceeded"
                if deadline
                else "The Attempt exceeded its timeout",
            )
            exception = TimeoutError(error.message)
            state = JobState.FAILED
        elif (
            isinstance(exception, (JobCancelledError, asyncio.CancelledError))
            or context.cancelled
            or cancelled.is_set()
        ):
            state = JobState.CANCELLED
            error = JobError(
                code="cancelled",
                message=str(exception) if exception else "Cancellation requested",
            )
        elif exception is not None:
            state = JobState.FAILED
            error = JobError.from_exception(exception)
        else:
            state = JobState.COMPLETED
            error = None
        payload = b""
        if state is JobState.COMPLETED:
            try:
                payload = handler.job.encode_result(result)
                maximum = getattr(handler.backend, "max_result_bytes", None)
                if maximum is not None and len(payload) > maximum:
                    raise ResultTooLargeError(
                        "Result exceeds the configured byte limit"
                    )
            except Exception as caught:
                exception = (
                    caught
                    if isinstance(caught, ResultTooLargeError)
                    else InvalidResultError(str(caught))
                )
                error = JobError.from_exception(
                    exception, code=getattr(exception, "code", "invalid_result")
                )
                state = JobState.FAILED
        released = await _retry(
            lambda: config.admission.release_resource_after_cleanup(owner), config
        )
        if released.outcome is not ReleaseOutcome.RELEASED:
            raise ResourceStaleOperationError("Resource release token is stale")
        retry = (
            state is JobState.FAILED
            and exception is not None
            and handler.retry_policy.should_retry(
                attempt=item.attempt, exception=exception
            )
            and not (
                item.execution.deadline is not None
                and datetime.now(UTC) >= item.execution.deadline
            )
        )
        if retry:
            delay = handler.retry_policy.delay(attempt=item.attempt)
            await _retry(
                lambda: handler.backend.mark_retry(
                    item.execution,
                    next_attempt_at=datetime.now(UTC) + timedelta(seconds=delay),
                    expected_admission_owner=owner,
                ),
                config,
            )
            if item.execution.state is JobState.CANCELLED:
                await _retry(
                    lambda: config.admission.finalize_execution_claim(owner), config
                )
                await _terminal(handler, item)
                return
            try:
                await handler._append_system_event(
                    item.execution,
                    item.attempt,
                    JobRetryScheduled(attempt=item.attempt + 1, delay=delay),
                )
            except TRANSIENT:
                pass
            await _retry(
                lambda: item.delivery.retry(
                    delay=timedelta(seconds=delay), attempt=item.attempt + 1
                ),
                config,
            )
            await _retry(
                lambda: config.admission.finalize_execution_claim(owner), config
            )
            return
        await _persist(
            handler,
            item,
            config,
            owner,
            state=state,
            error=error,
            result_payload=payload,
            result_media_type=handler.job.result_codec.media_type
            if state is JobState.COMPLETED and handler.job.result_codec is not None
            else None,
        )
        await _retry(lambda: config.admission.finalize_execution_claim(owner), config)
        await _terminal(handler, item)
    finally:
        if cancellation_watch is not None:
            cancellation_watch.cancel()
            await asyncio.gather(cancellation_watch, return_exceptions=True)
        await context.close()


async def _persist(
    handler: Any,
    item: WorkItem,
    config: ResourceHandlerConfig,
    owner: AdmissionOwner,
    **outcome: Any,
) -> ExecutionRecord:
    async def persist() -> ExecutionRecord:
        await config.admission.assert_execution_owner(owner)
        return await handler.backend.write_completion(
            item.execution, expected_admission_owner=owner, **outcome
        )

    return await _retry(persist, config)
