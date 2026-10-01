"""Worker-only backend that blocks after the real durable completion write."""

from __future__ import annotations

import asyncio
import os
from pathlib import Path

from faststream.nats import NatsBroker

from superjobs.jobs.execution import JobState
from superjobs.transport.backend import ExecutionRecord
from superjobs.transport.nats_backend import NatsJobBackend

try:
    from protocol import COMPLETION_SAVED, write_checkpoint
except ModuleNotFoundError:  # imported from repository tests
    from tools.worker_recovery_support.protocol import COMPLETION_SAVED, write_checkpoint


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
