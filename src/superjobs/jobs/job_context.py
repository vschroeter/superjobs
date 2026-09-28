from __future__ import annotations

import asyncio
import logging
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

from superjobs.exceptions.jobs import JobCancelledError
from superjobs.jobs.events import (
    JobLog,
    JobProgress,
)
from superjobs.jobs.execution import JobStatus, ProgressSnapshot
from superjobs.transport.backend import ExecutionRecord, JobBackend
from superjobs.transport.delivery import Delivery


logger = logging.getLogger(__name__)


@dataclass(frozen=True, slots=True)
class ObservationPolicy:
    batch_interval: float = 0.1
    progress_interval: float = 1.0
    max_batch_events: int = 100
    max_batch_bytes: int = 1024 * 1024
    max_queue_events: int = 1000

    def __post_init__(self) -> None:
        if self.batch_interval <= 0:
            raise ValueError("batch_interval must be positive")
        if self.progress_interval <= 0:
            raise ValueError("progress_interval must be positive")
        if self.max_batch_events < 1:
            raise ValueError("max_batch_events must be positive")
        if self.max_batch_bytes < 1:
            raise ValueError("max_batch_bytes must be positive")
        if self.max_queue_events < self.max_batch_events:
            raise ValueError("max_queue_events must be at least max_batch_events")


class _ObservationBatcher:
    def __init__(
        self,
        context: JobContext[Any],
        policy: ObservationPolicy,
    ):
        self.context = context
        self.policy = policy
        self.events: list[Any] = []
        self.progress: ProgressSnapshot | None = None
        self._last_progress_flush = 0.0
        self._wake = asyncio.Event()
        self._flush_lock = asyncio.Lock()
        self._closed = False
        self._error: BaseException | None = None
        self._task = asyncio.create_task(
            self._run(),
            name=f"superjobs-observations-{context.id}",
        )

    async def add_event(self, event: Any) -> None:
        await self._raise_if_failed()
        while len(self.events) >= self.policy.max_queue_events:
            await self.flush()
            await self._raise_if_failed()
        self.events.append(event)
        self._wake.set()
        if len(self.events) >= self.policy.max_batch_events:
            await self.flush()

    async def set_progress(self, progress: ProgressSnapshot) -> None:
        await self._raise_if_failed()
        self.progress = progress
        self._wake.set()

    async def flush(self) -> None:
        await self._raise_if_failed()
        while self.events or self.progress is not None:
            await self._flush_once(force=True)
        await self._raise_if_failed()

    async def close(self) -> None:
        if self._closed:
            return
        try:
            try:
                await self.flush()
            except Exception:
                logger.exception(
                    "Unable to flush observations for %s during close",
                    self.context.id,
                )
        finally:
            self._closed = True
            self._wake.set()
            if self._task is not asyncio.current_task() and not self._task.done():
                self._task.cancel()
            await asyncio.gather(self._task, return_exceptions=True)

    async def _run(self) -> None:
        try:
            while not self._closed:
                try:
                    await asyncio.wait_for(
                        self._wake.wait(),
                        timeout=self.policy.batch_interval,
                    )
                except TimeoutError:
                    pass
                self._wake.clear()
                if self.events:
                    await self._flush_once(force=False)
                elif self.progress is not None:
                    await self._flush_once(force=False)
        except asyncio.CancelledError:
            raise
        except BaseException as exception:
            self._error = exception

    async def _flush_once(self, *, force: bool) -> None:
        async with self._flush_lock:
            if not self.events and self.progress is None:
                return
            backend = self.context.backend
            execution = self.context.execution
            if backend is None or execution is None:
                raise RuntimeError("Observation batcher has no execution backend")

            now = asyncio.get_running_loop().time()
            progress = self.progress
            if (
                progress is not None
                and not force
                and now - self._last_progress_flush < self.policy.progress_interval
            ):
                progress = None

            event_limit = self.policy.max_batch_events
            if progress is not None:
                event_limit = max(0, event_limit - 1)
            selected_events: list[Any] = []
            selected_bytes = 0
            for candidate in self.events[:event_limit]:
                candidate_size = self._estimate_size(candidate)
                if (
                    selected_events
                    and selected_bytes + candidate_size > self.policy.max_batch_bytes
                ):
                    break
                selected_events.append(candidate)
                selected_bytes += candidate_size

            progress_included = False
            if progress is not None:
                progress_event = JobProgress(progress.completed, progress.total)
                progress_size = self._estimate_size(progress_event)
                if (
                    not selected_events
                    or selected_bytes + progress_size <= self.policy.max_batch_bytes
                ):
                    selected_events.append(progress_event)
                    progress_included = True

            if not selected_events:
                return

            self.events = self.events[:]
            del self.events[: len(selected_events) - (1 if progress_included else 0)]
            if progress_included:
                self.progress = None
                self._last_progress_flush = now

            try:
                await backend.publish_observations(
                    self.context.job.identity,
                    self.context.id,
                    attempt=self.context.attempt,
                    events=selected_events,
                    progress=progress if progress_included else None,
                )
            except BaseException as exception:
                self.events = selected_events[
                    : len(selected_events) - (1 if progress_included else 0)
                ] + self.events
                if progress_included:
                    self.progress = self.context.progress_snapshot
                    self._last_progress_flush = 0.0
                self._error = exception
                raise

    def _estimate_size(self, event: Any) -> int:
        if self.context.job.event_codec is not None:
            try:
                return len(self.context.job.event_codec.encode(event))
            except Exception:
                pass
        return len(repr(event).encode("utf-8")) + 64

    async def _raise_if_failed(self) -> None:
        if self._error is not None:
            raise RuntimeError("Observation publishing failed") from self._error


class JobContext[InterT: Any]:
    def __init__(
        self,
        job,
        *,
        backend: JobBackend | None = None,
        execution: ExecutionRecord | None = None,
        delivery: Delivery | None = None,
        attempt: int = 1,
        message: Any | None = None,
        observation_policy: ObservationPolicy | None = None,
    ):
        self.job = job
        self.backend = backend
        self.execution = execution
        self.delivery = delivery
        self.attempt = attempt
        self.message = message

        self.id = execution.job_id if execution is not None else ""
        self.created_at = (
            execution.created_at if execution is not None else datetime.now(UTC)
        )
        self.deadline = execution.deadline if execution is not None else None
        self._progress_snapshot: ProgressSnapshot | None = (
            execution.progress if execution is not None else None
        )
        self._cancelled = False
        self._cancel_reason: str | None = None
        self._cancel_watcher: asyncio.Task[None] | None = None
        self.logger = logging.getLogger(f"superjobs.job.{self.job.canonical_name}")
        self._batcher = (
            _ObservationBatcher(
                self,
                observation_policy or ObservationPolicy(),
            )
            if backend is not None and execution is not None
            else None
        )
        if backend is not None and execution is not None:
            self._cancel_watcher = asyncio.create_task(
                self._watch_cancellation(),
                name=f"superjobs-cancel-watch-{self.id}",
            )

    @property
    def cancelled(self) -> bool:
        return self._cancelled

    @property
    def progress_snapshot(self) -> ProgressSnapshot | None:
        return self._progress_snapshot

    @property
    def status(self) -> JobStatus:
        if self.execution is None:
            return JobStatus.pending(created_at=self.created_at)
        return self.execution.status()

    async def check_cancelled(self) -> None:
        if (
            self._cancelled
            or self._cancel_watcher is None
            and self.backend is not None
            and await self.backend.is_cancel_requested(self.id)
        ):
            self._cancelled = True
            raise JobCancelledError(self._cancel_reason)

    async def progress(self, completed: int, total: int | None = None) -> None:
        if not isinstance(completed, int) or completed < 0:
            raise ValueError("completed must be a non-negative integer")
        if total is not None and (not isinstance(total, int) or total < 0):
            raise ValueError("total must be a non-negative integer or None")
        if total is not None and completed > total:
            raise ValueError("completed cannot exceed total")

        snapshot = ProgressSnapshot(completed=completed, total=total)
        self._progress_snapshot = snapshot
        if self._batcher is not None:
            await self._batcher.set_progress(snapshot)

    async def emit(self, event: InterT) -> None:
        if self._batcher is None:
            raise RuntimeError("This context is not attached to a running execution")
        if self.job.event_codec is None:
            raise TypeError(f"Job {self.job} does not declare intermediate events")
        self.job.event_codec.encode(event)
        await self._batcher.add_event(event)

    async def emit_event(self, event: InterT) -> None:
        await self.emit(event)

    async def log(
        self,
        message: str,
        *,
        level: str = "info",
        extra: Mapping[str, Any] | None = None,
    ) -> None:
        if self._batcher is None:
            raise RuntimeError("This context is not attached to a running execution")
        await self._batcher.add_event(
            JobLog(message=message, level=level, extra=dict(extra or {})),
        )

    async def flush(self) -> None:
        if self._batcher is not None:
            await self._batcher.flush()

    async def close(self) -> None:
        try:
            if self._batcher is not None:
                await self._batcher.close()
        finally:
            if self._cancel_watcher is not None:
                self._cancel_watcher.cancel()
                await asyncio.gather(self._cancel_watcher, return_exceptions=True)
                self._cancel_watcher = None

    async def cancel(self) -> JobStatus:
        if self.backend is None:
            raise RuntimeError("This context is not attached to a running execution")
        return await self.backend.request_cancel(self.job.identity, self.id)

    async def _watch_cancellation(self) -> None:
        if self.backend is None:
            return
        try:
            await self.backend.wait_for_cancellation(self.id)
            self._cancelled = True
        except asyncio.CancelledError:
            raise
