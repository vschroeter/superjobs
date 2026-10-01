"""Worker-only subscribe/delivery wrapper that records replacement ack after real ack."""

from __future__ import annotations

import os
from collections.abc import AsyncIterator
from datetime import timedelta
from pathlib import Path
from typing import Any

from superjobs.jobs.job_identity import JobIdentity
from superjobs.transport.backend import JobBackend, WorkItem, WorkSubscription
from superjobs.transport.delivery import Delivery

try:
    from protocol import REPLACEMENT_ACK, write_checkpoint
except ModuleNotFoundError:  # imported from repository tests
    from tools.worker_recovery_support.protocol import REPLACEMENT_ACK, write_checkpoint


class _AckCheckpointDelivery:
    def __init__(
        self,
        inner: Delivery,
        *,
        backend: JobBackend,
        identity: JobIdentity,
        job_id: str,
        state_dir: Path,
        run_id: str,
        worker_generation: str,
        worker_pid: int,
    ) -> None:
        self._inner = inner
        self._backend = backend
        self._identity = identity
        self._job_id = job_id
        self._state_dir = state_dir
        self._run_id = run_id
        self._worker_generation = worker_generation
        self._worker_pid = worker_pid

    @property
    def attempt(self) -> int:
        return self._inner.attempt

    async def ack(self) -> None:
        await self._inner.ack()
        if self._worker_generation != "2":
            return
        execution = await self._backend.get_execution(self._identity, self._job_id)
        if execution is None:
            return
        write_checkpoint(
            self._state_dir,
            run_id=self._run_id,
            checkpoint=REPLACEMENT_ACK,
            execution_id=execution.job_id,
            pid=self._worker_pid,
            worker_generation=self._worker_generation,
            terminal_state=execution.state.name,
            terminal_event_published=execution.terminal_event_published,
        )

    async def retry(
        self,
        *,
        delay: timedelta | None = None,
        attempt: int | None = None,
    ) -> None:
        await self._inner.retry(delay=delay, attempt=attempt)

    async def reject(self, *, reason: str | None = None) -> None:
        await self._inner.reject(reason=reason)

    async def extend_lease(self) -> None:
        await self._inner.extend_lease()


class _AckCheckpointWorkSubscription:
    def __init__(
        self,
        inner: WorkSubscription,
        *,
        backend: JobBackend,
        identity: JobIdentity,
        state_dir: Path,
        run_id: str,
        worker_generation: str,
        worker_pid: int,
    ) -> None:
        self._inner = inner
        self._backend = backend
        self._identity = identity
        self._state_dir = state_dir
        self._run_id = run_id
        self._worker_generation = worker_generation
        self._worker_pid = worker_pid
        self._iterator: AsyncIterator[WorkItem] | None = None

    def __aiter__(self) -> _AckCheckpointWorkSubscription:
        self._iterator = self._inner.__aiter__()
        return self

    async def __anext__(self) -> WorkItem:
        assert self._iterator is not None
        item = await self._iterator.__anext__()
        return WorkItem(
            execution=item.execution,
            attempt=item.attempt,
            delivery=_AckCheckpointDelivery(
                item.delivery,
                backend=self._backend,
                identity=self._identity,
                job_id=item.execution.job_id,
                state_dir=self._state_dir,
                run_id=self._run_id,
                worker_generation=self._worker_generation,
                worker_pid=self._worker_pid,
            ),
        )

    async def close(self) -> None:
        await self._inner.close()


class AckCheckpointBackend:
    """Delegates to an inner backend; wraps work subscriptions for gen-2 ack evidence."""

    def __init__(self, inner: JobBackend) -> None:
        self._inner = inner

    def __getattr__(self, name: str) -> Any:
        return getattr(self._inner, name)

    async def subscribe_work(self, identity: JobIdentity) -> WorkSubscription:
        subscription = await self._inner.subscribe_work(identity)
        state_dir = Path(os.environ["SUPERJOBS_CROSS_STATE_DIR"])
        run_id = os.environ["SUPERJOBS_CROSS_RUN_ID"]
        worker_generation = os.environ.get("SUPERJOBS_WORKER_GENERATION", "1")
        return _AckCheckpointWorkSubscription(
            subscription,
            backend=self._inner,
            identity=identity,
            state_dir=state_dir,
            run_id=run_id,
            worker_generation=worker_generation,
            worker_pid=os.getpid(),
        )
