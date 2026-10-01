"""Worker-only backends that block after real durable transport side effects."""

from __future__ import annotations

import asyncio
import os
from collections.abc import AsyncIterator
from datetime import timedelta
from pathlib import Path

from faststream.nats import NatsBroker

from superjobs.jobs.execution import JobState
from superjobs.jobs.job_identity import JobIdentity
from superjobs.transport.backend import ExecutionRecord, WorkItem, WorkSubscription
from superjobs.transport.nats_backend import NatsJobBackend

try:
    from protocol import COMPLETION_SAVED, RETRY_PUBLISHED, write_checkpoint
except ModuleNotFoundError:  # imported from repository tests
    from tools.worker_recovery_support.protocol import (
        COMPLETION_SAVED,
        RETRY_PUBLISHED,
        write_checkpoint,
    )

SCENARIO_AFTER_RETRY_PUBLICATION = "recovery_after_retry_publication"


class _DeliveryAttemptTrackingSubscription:
    def __init__(
        self,
        inner: WorkSubscription,
        backend: BlockingAfterRetryPublicationBackend,
    ) -> None:
        self._inner = inner
        self._backend = backend
        self._iterator: AsyncIterator[WorkItem] | None = None

    def __aiter__(self) -> _DeliveryAttemptTrackingSubscription:
        self._iterator = self._inner.__aiter__()
        return self

    async def __anext__(self) -> WorkItem:
        assert self._iterator is not None
        item = await self._iterator.__anext__()
        self._backend._active_delivery_attempt = item.attempt
        return item

    async def close(self) -> None:
        await self._inner.close()


class BlockingAfterCompletionBackend(NatsJobBackend):
    def __init__(self, broker: NatsBroker, queue_config=None) -> None:
        super().__init__(broker, queue_config)

    async def write_completion(
        self,
        execution: ExecutionRecord,
        *,
        state: JobState,
        result_payload: bytes = b"",
        result_media_type: str | None = None,
        error=None,
    ) -> ExecutionRecord:
        updated = await super().write_completion(
            execution,
            state=state,
            result_payload=result_payload,
            result_media_type=result_media_type,
            error=error,
        )
        if state is JobState.COMPLETED:
            state_dir = Path(os.environ["SUPERJOBS_CROSS_STATE_DIR"])
            run_id = os.environ["SUPERJOBS_CROSS_RUN_ID"]
            write_checkpoint(
                state_dir,
                run_id=run_id,
                checkpoint=COMPLETION_SAVED,
                execution_id=execution.job_id,
                pid=os.getpid(),
                worker_generation=os.environ.get("SUPERJOBS_WORKER_GENERATION", "1"),
                completion_state=updated.state.name,
                terminal_event_published=updated.terminal_event_published,
            )
            while True:
                await asyncio.sleep(3600)
        return updated


class BlockingAfterRetryPublicationBackend(NatsJobBackend):
    def __init__(self, broker: NatsBroker, queue_config=None) -> None:
        super().__init__(broker, queue_config)
        self._active_delivery_attempt: int | None = None

    async def subscribe_work(self, identity: JobIdentity) -> WorkSubscription:
        subscription = await super().subscribe_work(identity)
        return _DeliveryAttemptTrackingSubscription(subscription, self)

    async def _publish_retry(
        self,
        execution: ExecutionRecord,
        attempt: int,
        delay: timedelta | None,
    ) -> None:
        await super()._publish_retry(execution, attempt, delay)
        state_dir = Path(os.environ["SUPERJOBS_CROSS_STATE_DIR"])
        run_id = os.environ["SUPERJOBS_CROSS_RUN_ID"]
        delivery_attempt = self._active_delivery_attempt
        if delivery_attempt is None:
            raise RuntimeError(
                "retry publication checkpoint missing delivery attempt evidence",
            )
        write_checkpoint(
            state_dir,
            run_id=run_id,
            checkpoint=RETRY_PUBLISHED,
            execution_id=execution.job_id,
            pid=os.getpid(),
            worker_generation=os.environ.get("SUPERJOBS_WORKER_GENERATION", "1"),
            delivery_attempt=delivery_attempt,
            retry_target_attempt=attempt,
        )
        while True:
            await asyncio.sleep(3600)
