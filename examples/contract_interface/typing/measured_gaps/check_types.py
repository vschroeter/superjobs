"""Documented typing gaps: compare EXPECTED targets with MEASURED Pyright output."""

from __future__ import annotations

from typing import reveal_type

from superjobs import Job, JobContext, SuperJobs
from superjobs_contract_example import (
    MANIFEST_WITH_EVENTS_JOB,
    ManifestEvent,
    ManifestRequest,
    ManifestResult,
)


def _omitted_event_parameter_inference() -> None:
    reveal_type(
        Job(
            "probe.noevent",
            request=ManifestRequest,
            result=ManifestResult,
        ),
    )


def _handler_callable_preservation(jobs: SuperJobs) -> None:
    @jobs.handler(MANIFEST_WITH_EVENTS_JOB)
    async def manifest_with_events(
        request: ManifestRequest,
        context: JobContext[ManifestEvent],
    ) -> ManifestResult:
        return ManifestResult(revision=request.device_id)

    reveal_type(manifest_with_events)
