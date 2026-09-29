"""Pyright-negative checks: deliberate type errors (not part of the passing suite)."""

from __future__ import annotations

from superjobs import InMemoryTransport, JobContext, SuperJobs
from superjobs_contract_example import (
    MANIFEST_WITH_EVENTS_JOB,
    ManifestEvent,
    ManifestRequest,
    ManifestResult,
)


async def _wrong_request_submit(jobs: SuperJobs) -> None:
    client = jobs.client(MANIFEST_WITH_EVENTS_JOB)
    await client.submit("not a manifest request")  # reportArgumentType


async def _wrong_event_emit(context: JobContext[ManifestEvent]) -> None:
    await context.emit("not an event")  # reportArgumentType


def _wrong_handler_registration(jobs: SuperJobs) -> None:
    @jobs.handler(MANIFEST_WITH_EVENTS_JOB)
    async def incompatible(
        request: str,
        context: JobContext[str],
    ) -> str:
        await context.emit("wrong")
        return "wrong"

    # Measured gap: incompatible handler is not rejected at registration time.
