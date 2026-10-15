"""Worker-only transport wrapper that records work pulls before handler admission."""

from __future__ import annotations

import os
from collections.abc import AsyncIterator
from pathlib import Path

from superjobs.jobs.job_identity import JobIdentity
from superjobs.transport.backend import JobBackend, WorkItem, WorkSubscription
from superjobs.transport.delivery import Delivery
from dataclasses import replace

from evidence import append_event


class _ExpireDeliveryLease(Delivery):
    """Test-only fault: force actual JetStream redelivery during native work."""

    def __init__(self, inner: Delivery) -> None:
        self._inner = inner

    @property
    def attempt(self) -> int:
        return self._inner.attempt

    async def ack(self) -> None:
        await self._inner.ack()

    async def retry(self, *, delay=None, attempt=None) -> None:
        await self._inner.retry(delay=delay, attempt=attempt)

    async def reject(self, *, reason=None) -> None:
        await self._inner.reject(reason=reason)

    async def extend_lease(self) -> None:
        pass


class _PullEvidenceSubscription:
    def __init__(
        self,
        inner: WorkSubscription,
        *,
        state_dir: Path,
        run_id: str,
        worker_generation: str,
    ) -> None:
        self._inner = inner
        self._state_dir = state_dir
        self._run_id = run_id
        self._worker_generation = worker_generation
        self._iterator: AsyncIterator[WorkItem] | None = None

    def __aiter__(self) -> _PullEvidenceSubscription:
        self._iterator = self._inner.__aiter__()
        return self

    async def __anext__(self) -> WorkItem:
        assert self._iterator is not None
        item = await self._iterator.__anext__()
        append_event(
            self._state_dir,
            kind="work_pull",
            run_id=self._run_id,
            execution_id=item.execution.job_id,
            attempt=item.attempt,
            worker_generation=self._worker_generation,
        )
        return replace(item, delivery=_ExpireDeliveryLease(item.delivery))

    async def close(self) -> None:
        await self._inner.close()


class DeliveryEvidenceBackend:
    """Delegates to an inner backend and appends pull evidence outside the handler."""

    def __init__(self, inner: JobBackend) -> None:
        self._inner = inner
        self._state_dir = Path(os.environ["SUPERJOBS_CROSS_STATE_DIR"])
        self._run_id = os.environ["SUPERJOBS_CROSS_RUN_ID"]
        self._worker_generation = os.environ.get("SUPERJOBS_WORKER_GENERATION", "1")

    def __getattr__(self, name: str):
        return getattr(self._inner, name)

    async def subscribe_work(self, identity: JobIdentity) -> WorkSubscription:
        subscription = await self._inner.subscribe_work(identity)
        return _PullEvidenceSubscription(
            subscription,
            state_dir=self._state_dir,
            run_id=self._run_id,
            worker_generation=self._worker_generation,
        )
